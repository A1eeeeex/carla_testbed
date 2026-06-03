from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

from carla_testbed.analysis.traffic_light_behavior import (
    TRAFFIC_LIGHT_BEHAVIOR_REPORT_SCHEMA_VERSION,
    analyze_traffic_light_behavior_run_dir,
    write_traffic_light_behavior_report,
)

FIXTURE = Path("tests/fixtures/natural_driving/simple_suite/traffic_light_red_stop")


def _copy_run(tmp_path: Path) -> Path:
    target = tmp_path / "traffic_light_red_stop"
    shutil.copytree(FIXTURE, target)
    return target


def _drop_summary_metric(run_dir: Path, metric: str) -> None:
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"].pop(metric, None)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


def _add_timeseries_column(run_dir: Path, field: str, values: list[str]) -> None:
    timeseries_path = run_dir / "timeseries.csv"
    with timeseries_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0].keys()) if rows else []
    if field not in fieldnames:
        fieldnames.append(field)
    for index, row in enumerate(rows):
        row[field] = values[min(index, len(values) - 1)]
    with timeseries_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _set_manifest_expectation(run_dir: Path, expected_behavior: str) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_expectation"] = {
        "expected_behavior": expected_behavior,
        "expected_initial_state": "GREEN" if expected_behavior == "green_go" else "RED",
        "required_report_fields": ["green_pass_time_s"]
        if expected_behavior == "green_go"
        else ["red_stop_distance_m", "stopped_at_red"],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def _drop_manifest_expectation(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("traffic_light_expectation", None)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def _set_manifest_control(run_dir: Path, control: dict) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_control"] = control
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def _drop_manifest_control(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("traffic_light_control", None)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def test_red_stop_fixture_generates_pass_report(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)

    report = analyze_traffic_light_behavior_run_dir(
        run_dir,
        scenario_class="traffic_light_red_stop",
    )
    outputs = write_traffic_light_behavior_report(report, tmp_path / "out")

    assert report["schema_version"] == TRAFFIC_LIGHT_BEHAVIOR_REPORT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["metrics"]["red_stop_distance_m"] == 3.2
    assert report["traffic_light_expectation"]["expected_behavior"] == "red_stop"
    assert report["traffic_light_control"]["stimulus_mode"] == "deterministic_gt_control"
    assert report["traffic_light_control"]["initial_state"] == "RED"
    assert Path(outputs["traffic_light_behavior_report"]).is_file()
    assert Path(outputs["traffic_light_behavior_summary"]).is_file()


def test_claim_grade_missing_traffic_light_control_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _drop_manifest_control(run_dir)

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "traffic_light_control_missing"
    assert "traffic_light_control" in report["missing_fields"]


def test_force_green_claim_grade_traffic_light_behavior_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_control"]["traffic_light_policy"] = "force_green"
    manifest["traffic_light_control"]["mode"] = "force_green"
    manifest["traffic_light_control"]["stimulus_mode"] = "force_green"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "traffic_light_force_green_not_claim_grade"
    assert "traffic_light_control.traffic_light_policy" in report["missing_fields"]


def test_claim_grade_traffic_light_control_must_affect_actor(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _set_manifest_control(
        run_dir,
        {
            "mode": "deterministic_gt_control",
            "stimulus_mode": "deterministic_gt_control",
            "initial_state": "RED",
            "initial_affected_count": 0,
            "events": [],
        },
    )

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "fail"
    assert report["failure_reason"] == "traffic_light_control_no_actor_affected"


def test_red_to_green_claim_grade_requires_release_event(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_expectation"] = {
        "expected_behavior": "red_to_green_release",
        "expected_initial_state": "RED",
        "expected_release_state": "GREEN",
        "stimulus_mode": "deterministic_gt_control",
        "claim_grade": True,
        "required_report_fields": ["red_to_green_release_time_s"],
    }
    manifest["traffic_light_control"] = {
        "mode": "deterministic_gt_control",
        "stimulus_mode": "deterministic_gt_control",
        "traffic_light_policy": "carla_actual",
        "color_source": "carla_actor_state",
        "confidence": 1.0,
        "contain_lights": True,
        "initial_state": "RED",
        "release_state": "GREEN",
        "initial_affected_count": 1,
        "events": [{"phase": "initial", "state": "RED", "affected_count": 1}],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["red_to_green_release_time_s"] = 2.5
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = analyze_traffic_light_behavior_run_dir(
        run_dir,
        scenario_class="traffic_light_red_to_green_release",
    )

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "traffic_light_control_release_not_observed"
    assert "traffic_light_control.release_frame_id" in report["missing_fields"]

    manifest["traffic_light_control"]["events"].append(
        {"phase": "release", "state": "GREEN", "affected_count": 1, "frame_id": 42}
    )
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = analyze_traffic_light_behavior_run_dir(
        run_dir,
        scenario_class="traffic_light_red_to_green_release",
    )

    assert report["status"] == "pass"


def test_red_light_not_stopped_fails(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["stopped_at_red"] = False
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "fail"
    assert report["failure_reason"] == "red_light_not_stopped"


def test_missing_red_stop_distance_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"].pop("red_stop_distance_m")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    timeseries_path = run_dir / "timeseries.csv"
    lines = timeseries_path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    keep = [index for index, name in enumerate(header) if name != "red_stop_distance_m"]
    rewritten = [
        ",".join(header[index] for index in keep),
        *(",".join(row.split(",")[index] for index in keep) for row in lines[1:]),
    ]
    timeseries_path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")
    events_path = run_dir / "events.jsonl"
    events_path.write_text('{"event":"run_start","sim_time":0.0}\n', encoding="utf-8")

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_red_stop_distance"
    assert "red_stop_distance_m" in report["missing_fields"]


def test_missing_stopped_at_red_evidence_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _drop_summary_metric(run_dir, "stopped_at_red")

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_stopped_at_red"
    assert "stopped_at_red" in report["missing_fields"]


def test_stopped_at_red_can_be_derived_from_speed_and_distance(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _drop_summary_metric(run_dir, "stopped_at_red")
    _add_timeseries_column(run_dir, "ego_speed", ["3.0", "1.2", "0.1", "0.0"])

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "pass"
    assert report["metrics"]["stopped_at_red"] is True


def test_near_red_stop_line_without_stopping_fails(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _drop_summary_metric(run_dir, "stopped_at_red")
    _add_timeseries_column(run_dir, "ego_speed", ["3.0", "2.5", "2.0", "1.5"])

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "fail"
    assert report["failure_reason"] == "red_light_not_stopped"


def test_contract_warn_limits_behavior_report_to_warn(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    contract_path = run_dir / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["status"] = "warn"
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "warn"
    assert report["failure_reason"] == "traffic_light_contract_warn"


def test_missing_behavior_inputs_cannot_pass_even_when_summary_metrics_exist(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    (run_dir / "events.jsonl").unlink()

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_behavior_inputs"
    assert "events" in report["missing_inputs"]
    assert "source.events" in report["missing_fields"]


def test_green_go_requires_green_pass_time(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _set_manifest_expectation(run_dir, "green_go")

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_green_go")

    assert report["status"] == "insufficient_data"
    assert report["failure_reason"] == "missing_green_pass_time"
    assert "green_pass_time_s" in report["missing_fields"]


def test_missing_traffic_light_expectation_warns_for_legacy_artifacts(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _drop_manifest_expectation(run_dir)

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "pass"
    assert report["traffic_light_expectation"] is None
    assert "traffic_light_expectation_missing" in report["warnings"]


def test_traffic_light_expectation_mismatch_fails(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    _set_manifest_expectation(run_dir, "green_go")

    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class="traffic_light_red_stop")

    assert report["status"] == "fail"
    assert report["failure_reason"] == "traffic_light_expectation_mismatch"
    assert "traffic_light_expectation.expected_behavior" in report["missing_fields"]
