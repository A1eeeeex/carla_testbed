from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_control_handoff import (
    APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION,
    analyze_apollo_control_handoff,
    ensure_apollo_control_handoff_report,
    write_apollo_control_handoff_report,
)
from carla_testbed.analysis.natural_driving import analyze_natural_driving_suite
from carla_testbed.analysis.transport_ab import _compare_pair

FIXTURE = Path("tests/fixtures/apollo_control_handoff/full_pass")
SCRIPT = Path("tools/analyze_apollo_control_handoff.py")
NATURAL_FIXTURE = Path("tests/fixtures/natural_driving/simple_suite")


def _copy_case(tmp_path: Path) -> Path:
    target = tmp_path / "case"
    shutil.copytree(FIXTURE, target)
    return target


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_full_pass_writes_report(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)

    report = analyze_apollo_control_handoff(run_dir=run_dir)
    outputs = write_apollo_control_handoff_report(report, tmp_path / "out")

    assert report["schema_version"] == APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION
    assert report["verdict"] == "pass"
    assert report["failure_stage"] == "none"
    assert report["evidence_level"] == "mixed"
    assert report["control_channel"]["message_count"] == 10
    assert report["bridge_receive"]["control_rx_count"] == 10
    assert report["mapping_and_apply"]["apply_control_count"] == 10
    assert Path(outputs["apollo_control_handoff_report"]).is_file()
    assert Path(outputs["apollo_control_handoff_summary"]).is_file()


def test_route_completion_delta_counts_as_vehicle_response(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    rows = _read_csv(run_dir / "timeseries.csv")
    for index, row in enumerate(rows):
        row["ego_speed"] = ""
        row["speed_mps"] = ""
        row["v_mps"] = ""
        row["route_s"] = ""
        row["route_completion"] = str(0.1 + 0.2 * index)
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["vehicle_response"]["status"] == "pass"
    assert report["vehicle_response"]["route_completion_delta"] > 0.01
    assert report["failure_stage"] == "none"


def test_refresh_preserves_existing_report_without_raw_handoff_inputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    shutil.copytree(NATURAL_FIXTURE / "lane_keep_097", run_dir)
    expected_path = run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json"

    result = ensure_apollo_control_handoff_report(run_dir, refresh=True)
    report = _json(expected_path)

    assert result["status"] == "existing_report_copied"
    assert result["path"] == str(expected_path)
    assert result["source_report"] == str(expected_path)
    assert result["report_status"] == "pass"
    assert report["verdict"] == "pass"
    assert report["failure_stage"] == "none"


def test_refresh_regenerates_handoff_when_raw_inputs_exist(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    existing_path = run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_text(
        json.dumps(
            {
                "schema_version": APOLLO_CONTROL_HANDOFF_SCHEMA_VERSION,
                "verdict": "pass",
                "failure_stage": "none",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    stats = _json(run_dir / "artifacts" / "channel_stats.json")
    stats["channels"]["/apollo/control"]["message_count"] = 0
    _write_json(run_dir / "artifacts" / "channel_stats.json", stats)
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_rx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()

    result = ensure_apollo_control_handoff_report(run_dir, refresh=True)
    regenerated = _json(existing_path)

    assert result["status"] == "generated"
    assert result["report_status"] == "fail"
    assert result["failure_stage"] == "control_channel"
    assert regenerated["verdict"] == "fail"
    assert regenerated["failure_stage"] == "control_channel"


def test_process_crash_tcmalloc_is_process_health_failure(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "control.err.log").write_text(
        "FATAL tcmalloc invalid free detected\ncore dumped\n",
        encoding="utf-8",
    )

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "process_health"
    assert report["process_health"]["crash_reason"] == "tcmalloc_invalid_free"


def test_planning_ready_control_missing_fails_control_channel(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    stats = _json(run_dir / "artifacts" / "channel_stats.json")
    stats["channels"]["/apollo/control"]["message_count"] = 0
    _write_json(run_dir / "artifacts" / "channel_stats.json", stats)
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_rx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "control_channel"
    assert report["input_readiness"]["status"] in {"pass", "warn"}


def test_control_channel_present_but_bridge_missing_fails_bridge_receive(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_rx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "bridge_receive"


def test_bridge_receive_but_decode_missing_fails_raw_decode(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "bridge_control_decode.jsonl").unlink()
    rows = _read_csv(run_dir / "timeseries.csv")
    for row in rows:
        for field in ("apollo_steer_raw", "throttle_raw", "brake_raw"):
            row[field] = ""
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "raw_decode"


def test_mapped_control_without_applied_control_fails_mapping_and_apply(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    (run_dir / "artifacts" / "direct_bridge_control_apply.jsonl").unlink()
    bridge = _json(run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge["control_tx_count"] = 0
    _write_json(run_dir / "artifacts" / "cyber_bridge_stats.json", bridge)
    rows = _read_csv(run_dir / "timeseries.csv")
    for row in rows:
        for field in ("carla_steer_applied", "throttle_applied", "brake_applied"):
            row[field] = "0.0"
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "mapping_and_apply"
    assert report["mapping_and_apply"]["apply_control_count"] == 0


def test_applied_control_without_vehicle_response_fails_vehicle_response(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    rows = _read_csv(run_dir / "timeseries.csv")
    for row in rows:
        row["route_s"] = "0.0"
        row["ego_speed"] = "0.0"
        row["ego_yaw_rate"] = "0.0"
    _write_csv(run_dir / "timeseries.csv", rows)

    report = analyze_apollo_control_handoff(run_dir=run_dir)

    assert report["verdict"] == "fail"
    assert report["failure_stage"] == "vehicle_response"


def test_cli_generates_report(tmp_path: Path) -> None:
    run_dir = _copy_case(tmp_path)
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    assert "failure_stage" in result.stdout
    assert (out / "apollo_control_handoff_report.json").is_file()
    assert (out / "apollo_control_handoff_summary.md").is_file()


def test_natural_driving_missing_handoff_report_cannot_pass(tmp_path: Path) -> None:
    suite = tmp_path / "suite"
    shutil.copytree(NATURAL_FIXTURE, suite)
    handoff_path = (
        suite
        / "lane_keep_097"
        / "analysis"
        / "apollo_control_handoff"
        / "apollo_control_handoff_report.json"
    )
    handoff_path.unlink()

    report = analyze_natural_driving_suite(suite)
    lane = next(row for row in report["run_results"] if row["scenario_id"] == "lane_keep_097")

    assert lane["verdict"] == "insufficient_data"
    assert lane["failure_reason"] == "apollo_control_handoff_report_missing"
    assert "apollo_control_handoff_report.json" in lane["missing_fields"]


def test_natural_driving_failed_handoff_report_blocks_hard_pass(tmp_path: Path) -> None:
    suite = tmp_path / "suite"
    shutil.copytree(NATURAL_FIXTURE, suite)
    handoff_path = (
        suite
        / "lane_keep_097"
        / "analysis"
        / "apollo_control_handoff"
        / "apollo_control_handoff_report.json"
    )
    payload = _json(handoff_path)
    payload["verdict"] = "fail"
    payload["failure_stage"] = "control_channel"
    payload["blocking_reasons"] = ["control_channel_failed"]
    payload["control_channel"]["message_count"] = 0
    _write_json(handoff_path, payload)

    report = analyze_natural_driving_suite(suite)
    lane = next(row for row in report["run_results"] if row["scenario_id"] == "lane_keep_097")

    assert lane["verdict"] == "fail"
    assert lane["failure_reason"] == "apollo_control_handoff_control_channel"
    assert lane["apollo_control_handoff_status"] == "fail"
    assert lane["apollo_control_handoff_failure_stage"] == "control_channel"


def test_ab_candidate_positive_blocked_when_handoff_fails() -> None:
    baseline = {
        "run_id": "baseline_lane097",
        "route_id": "lane097",
        "backend": "ros2_gt",
        "duration_s": 30.0,
        "run_status": "success",
        "return_code": None,
        "artifact_complete": True,
        "route_completion": 0.7,
        "lateral_error_p95_m": 0.5,
        "heading_error_p95_rad": 0.02,
        "planning_hz": 10.0,
        "carla_applied_control_hz": 20.0,
        "localization_hz": 20.0,
        "chassis_hz": 20.0,
        "route_hard_gate_eligible": True,
        "failure_reason": "success",
        "apollo_control_handoff_status": "pass",
        "apollo_control_handoff_failure_stage": "none",
    }
    candidate = {
        **baseline,
        "run_id": "candidate_lane097",
        "backend": "carla_direct",
        "direct_transport_contract_status": "aligned",
        "apollo_control_handoff_status": "fail",
        "apollo_control_handoff_failure_stage": "bridge_receive",
    }

    comparison = _compare_pair(baseline, candidate)

    assert comparison["status"] == "insufficient_data"
    assert "candidate apollo_control_handoff failed: bridge_receive" in comparison["reasons"]
