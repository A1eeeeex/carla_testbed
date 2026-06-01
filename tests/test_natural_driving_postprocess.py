from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.natural_driving_postprocess import postprocess_natural_driving_runs

FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")
CHANNEL_STATS = Path("tests/fixtures/apollo/channel_stats_natural_valid.json")


def _copy_suite(tmp_path: Path) -> Path:
    target = tmp_path / "suite"
    shutil.copytree(FIXTURE_ROOT, target)
    return target


def test_postprocess_keeps_complete_fixture_pass(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    out_dir = tmp_path / "out"

    report = postprocess_natural_driving_runs(suite_root, out_dir=out_dir)

    assert report["schema_version"] == "town01_natural_driving_postprocess.v1"
    assert report["natural_driving"]["status"] == "pass"
    assert report["run_count"] == 3
    assert (out_dir / "natural_driving_postprocess.json").is_file()
    assert (out_dir / "natural_driving_postprocess.md").is_file()
    assert (out_dir / "natural_driving_report.json").is_file()
    assert all(run["artifact_completeness"]["status"] == "pass" for run in report["runs"])
    assert all(run["artifact_completeness"]["invalid_manifest_source_fields"] == [] for run in report["runs"])
    assert all(run["artifact_completeness"]["invalid_report_source_fields"] == [] for run in report["runs"])
    assert all(Path(run["artifact_completeness"]["path"]).is_file() for run in report["runs"])
    assert all(Path(run["artifact_completeness"]["summary_path"]).is_file() for run in report["runs"])
    natural_report = json.loads((out_dir / "natural_driving_report.json").read_text(encoding="utf-8"))
    assert all(run["artifacts"]["artifact_completeness"] for run in natural_report["run_results"])


def test_postprocess_writes_route_start_probe_plan_for_route_start_lane_invasion(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane_dir = suite_root / "lane_keep_097"
    summary_path = lane_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["metrics"]["lane_invasion_count"] = 1
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    failure_dir = lane_dir / "analysis" / "failure_timeline"
    failure_dir.mkdir(parents=True, exist_ok=True)
    (failure_dir / "failure_timeline_report.json").write_text(
        json.dumps(
            {
                "schema_version": "town01_failure_timeline.v1",
                "run_id": "lane_keep_097",
                "route_id": "lane097",
                "scenario_class": "lane_keep",
                "status": "pass",
                "route_start_gate": {
                    "route_s_at_anchor": -0.5,
                    "anchor_before_route_start": True,
                    "anchor_near_route_start": True,
                },
                "anchor_event": {"event_type": "lane_invasion"},
                "ordering_findings": ["safety_event_before_route_start"],
                "source": {
                    "manifest_path": "manifest.json",
                    "summary_path": "summary.json",
                    "events_path": "events.jsonl",
                    "timeseries_path": "timeseries.csv",
                    "route_health_path": "analysis/route_health/route_health.json",
                    "control_health_path": "analysis/control_health/control_health_report.json",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    alignment_dir = lane_dir / "analysis" / "route_start_alignment"
    alignment_dir.mkdir(parents=True, exist_ok=True)
    (alignment_dir / "route_start_alignment_report.json").write_text(
        json.dumps(
            {
                "schema_version": "route_start_alignment_report.v1",
                "run_id": "lane_keep_097",
                "route_id": "lane097",
                "scenario_class": "lane_keep",
                "status": "warn",
                "reason": "failure_before_route_start",
                "static_spawn_alignment": {"spawn_lateral_offset_m": -0.5},
                "initial_ego_alignment": {"rear_axle_offset_compatible": True},
                "failure_anchor_alignment": {"route_s": -0.5},
                "recommendation": {
                    "available": True,
                    "recommended_ego_offset_y_delta_m": 0.5,
                },
                "source": {
                    "manifest_path": "manifest.json",
                    "summary_path": "summary.json",
                    "timeseries_path": "timeseries.csv",
                    "failure_timeline_path": "analysis/failure_timeline/failure_timeline_report.json",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    outputs = report["route_start_probe_plan"]["outputs"]
    plan = json.loads(Path(outputs["route_start_probe_plan"]).read_text(encoding="utf-8"))
    script = Path(outputs["route_start_probe_script"]).read_text(encoding="utf-8")

    assert report["natural_driving"]["status"] == "fail"
    assert report["route_start_probe_plan"]["status"] == "ready"
    assert report["route_start_probe_plan"]["probe_count"] == 1
    assert plan["claim_boundary"]["does_not_change_steer_scale"] is True
    assert plan["probes"][0]["override"] == "scenario.route_health.ego_offset_y_m=0.5"
    assert "--override scenario.route_health.ego_offset_y_m=0.5" in script
    assert "tools/analyze_route_start_probe_result.py" in script
    assert "analysis/route_start_probe_result" in script
    assert "set -uo pipefail" in script
    assert "overall_rc=0" in script
    assert "if [ -f " in script


def test_postprocess_does_not_duplicate_matrix_paths_that_are_relative_to_cwd(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    suite_root = Path("suite")
    fixture_root = Path(__file__).resolve().parent / "fixtures" / "natural_driving" / "simple_suite"
    shutil.copytree(fixture_root, suite_root)
    with (suite_root / "run_matrix.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["actual_run_dir"])
        writer.writeheader()
        writer.writerow({"actual_run_dir": "suite/lane_keep_097"})

    report = postprocess_natural_driving_runs(suite_root, out_dir=Path("out"))
    natural = json.loads(Path("out/natural_driving_report.json").read_text(encoding="utf-8"))

    assert report["run_count"] == 3
    assert natural["run_count"] == 3
    assert all("suite/suite" not in run["run_dir"] for run in natural["run_results"])


def test_postprocess_generates_route_health_and_channel_health_when_inputs_exist(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    shutil.rmtree(lane / "analysis" / "route_health")
    shutil.rmtree(lane / "analysis" / "control_health")
    (lane / "apollo_channel_health_report.json").unlink()
    shutil.copy2(CHANNEL_STATS, lane / "channel_stats.json")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")

    assert lane_result["route_health"]["status"] == "generated"
    assert Path(lane_result["route_health"]["path"]).is_file()
    assert lane_result["route_curve_artifact_gap"]["status"] == "generated"
    assert Path(lane_result["route_curve_artifact_gap"]["path"]).is_file()
    assert lane_result["apollo_channel_health"]["status"] == "generated"
    assert Path(lane_result["apollo_channel_health"]["path"]).is_file()
    assert lane_result["control_health"]["status"] == "generated"
    assert Path(lane_result["control_health"]["path"]).is_file()
    channel = json.loads(Path(lane_result["apollo_channel_health"]["path"]).read_text(encoding="utf-8"))
    assert channel["status"] == "pass"
    assert channel["scenario_class"] == "lane_keep"


def test_postprocess_regenerates_control_health_when_context_is_stale(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    control_path = lane / "analysis" / "control_health" / "control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["scenario_class"] = "traffic_light_red_stop"
    control["route_id"] = "traffic_light_red_stop_tbd"
    control_path.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    regenerated = json.loads(Path(lane_result["control_health"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["control_health"]["status"] == "generated"
    assert regenerated["scenario_class"] == "lane_keep"
    assert regenerated["route_id"] == "lane097"
    assert report["natural_driving"]["status"] == "pass"


def test_postprocess_regenerates_failure_timeline_when_context_is_stale(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    failure_path = lane / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    failure["scenario_class"] = "traffic_light_red_stop"
    failure["route_id"] = "traffic_light_red_stop_tbd"
    failure_path.write_text(json.dumps(failure, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    regenerated = json.loads(Path(lane_result["failure_timeline"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["failure_timeline"]["status"] == "generated"
    assert regenerated["scenario_class"] == "lane_keep"
    assert regenerated["route_id"] == "lane097"
    assert report["natural_driving"]["status"] == "pass"


def test_postprocess_regenerates_route_start_alignment_when_context_is_stale(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    alignment_path = lane / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    alignment = json.loads(alignment_path.read_text(encoding="utf-8"))
    alignment["scenario_class"] = "traffic_light_red_stop"
    alignment["route_id"] = "traffic_light_red_stop_tbd"
    alignment_path.write_text(json.dumps(alignment, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    regenerated = json.loads(Path(lane_result["route_start_alignment"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["route_start_alignment"]["status"] == "generated"
    assert regenerated["scenario_class"] == "lane_keep"
    assert regenerated["route_id"] == "lane097"
    assert report["natural_driving"]["status"] == "pass"


def test_postprocess_regenerates_failure_timeline_and_route_start_alignment_when_source_is_missing(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    failure_path = lane / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    alignment_path = lane / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    for path in (failure_path, alignment_path):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.pop("source", None)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    failure = json.loads(Path(lane_result["failure_timeline"]["path"]).read_text(encoding="utf-8"))
    alignment = json.loads(Path(lane_result["route_start_alignment"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["failure_timeline"]["status"] == "generated"
    assert lane_result["route_start_alignment"]["status"] == "generated"
    assert failure["source"]["summary_path"].endswith("summary.json")
    assert failure["source"]["route_health_path"].endswith("route_health.json")
    assert alignment["source"]["failure_timeline_path"].endswith("failure_timeline_report.json")
    assert report["natural_driving"]["status"] == "pass"


def test_postprocess_regenerates_channel_health_when_scenario_context_is_stale(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    shutil.copy2(CHANNEL_STATS, lane / "channel_stats.json")
    channel_path = lane / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["scenario_class"] = "traffic_light_red_stop"
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    regenerated = json.loads(Path(lane_result["apollo_channel_health"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["apollo_channel_health"]["status"] == "generated"
    assert regenerated["scenario_class"] == "lane_keep"
    assert report["natural_driving"]["status"] == "pass"


def test_postprocess_rejects_stale_channel_health_without_raw_stats(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    channel_path = lane / "apollo_channel_health_report.json"
    channel = json.loads(channel_path.read_text(encoding="utf-8"))
    channel["scenario_class"] = "traffic_light_red_stop"
    channel_path.write_text(json.dumps(channel, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    generated = json.loads(Path(lane_result["apollo_channel_health"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["apollo_channel_health"]["status"] == "insufficient_data"
    assert "channel_stats" in generated["missing_inputs"]
    assert generated["scenario_class"] == "lane_keep"
    assert report["natural_driving"]["status"] == "insufficient_data"


def test_postprocess_regenerates_thin_route_curve_artifact_gap_report(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    gap_dir = lane / "analysis" / "route_curve_artifact_gap"
    gap_dir.mkdir(parents=True, exist_ok=True)
    (gap_dir / "route_curve_artifact_gap_report.json").write_text(
        json.dumps({"schema_version": "route_curve_artifact_gap.v1", "status": "pass"}) + "\n",
        encoding="utf-8",
    )

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    regenerated_path = Path(lane_result["route_curve_artifact_gap"]["path"])
    regenerated = json.loads(regenerated_path.read_text(encoding="utf-8"))

    assert lane_result["route_curve_artifact_gap"]["status"] == "generated"
    assert regenerated["source"]["timeseries_csv"].endswith("timeseries.csv")
    assert regenerated["source"]["summary_json"].endswith("summary.json")


def test_postprocess_rejects_stale_route_health_route_id_without_route_inputs(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    route_health_path = lane / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["route_id"] = "curve217"
    route_health_path.write_text(json.dumps(route_health, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    regenerated = json.loads(Path(lane_result["route_health"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["route_health"]["status"] == "generated"
    assert regenerated["verdict"]["status"] == "insufficient_data"
    assert "route" in regenerated["missing_inputs"]
    assert report["natural_driving"]["status"] == "insufficient_data"


def test_refresh_preserves_existing_route_and_channel_reports_when_raw_inputs_are_absent(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    route_report = lane / "analysis" / "route_health" / "route_health.json"
    analysis_report = lane / "analysis" / "apollo_channel_health" / "apollo_channel_health_report.json"
    if analysis_report.exists():
        analysis_report.unlink()

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out", refresh=True)
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    traffic_result = next(run for run in report["runs"] if run["run_id"] == "traffic_light_red_stop")
    channel_path = Path(lane_result["apollo_channel_health"]["path"])
    channel_report = json.loads(channel_path.read_text(encoding="utf-8"))
    route_report_payload = json.loads(route_report.read_text(encoding="utf-8"))

    assert lane_result["route_health"]["status"] == "existing_report_copied"
    assert lane_result["route_health"]["report_status"] == "pass"
    assert lane_result["apollo_channel_health"]["status"] == "existing_report_copied"
    assert lane_result["apollo_channel_health"]["stats_source"] == "existing_report_without_raw_stats"
    assert channel_path == analysis_report
    assert route_report_payload["verdict"]["status"] == "pass"
    assert channel_report["status"] == "pass"
    assert traffic_result["traffic_light_contract"]["status"] == "existing_report_copied"
    assert traffic_result["traffic_light_contract"]["report_status"] == "pass"
    assert traffic_result["traffic_light_behavior"]["report_status"] == "pass"
    assert report["natural_driving"]["status"] == "pass"


def test_postprocess_writes_insufficient_channel_report_when_stats_missing(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    (lane / "apollo_channel_health_report.json").unlink()

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    channel_path = Path(lane_result["apollo_channel_health"]["path"])
    channel_report = json.loads(channel_path.read_text(encoding="utf-8"))

    assert lane_result["apollo_channel_health"]["status"] == "insufficient_data"
    assert channel_report["status"] == "insufficient_data"
    assert "channel_stats" in channel_report["missing_inputs"]
    assert report["natural_driving"]["status"] == "insufficient_data"
    assert report["natural_driving"]["problem_run_count"] >= 1
    assert any(
        run["run_id"] == "lane_keep_097" and run["verdict"] == "insufficient_data"
        for run in report["natural_driving"]["problem_runs"]
    )


def test_postprocess_derives_channel_stats_from_bridge_counters(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    (lane / "apollo_channel_health_report.json").unlink()
    artifacts = lane / "artifacts"
    artifacts.mkdir()
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "loc_count": 600,
                "chassis_count": 600,
                "obstacles_count": 300,
                "control_rx_count": 300,
                "routing_response_count": 2,
                "planning": {"msg_count": 300},
                "traffic_light": {
                    "channel": "/apollo/perception/traffic_light",
                    "publish_count": 0,
                },
                "timing": {"sim_time_sec": 30.0},
            }
        ),
        encoding="utf-8",
    )

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    channel_path = Path(lane_result["apollo_channel_health"]["path"])
    channel_report = json.loads(channel_path.read_text(encoding="utf-8"))

    assert lane_result["apollo_channel_health"]["stats_source"] == "derived_from_bridge_counters"
    assert (lane / "channel_stats.json").is_file()
    assert channel_report["status"] == "warn"
    assert "control_stats_derived_from_bridge_counters_not_promotion_grade" in channel_report["warnings"]
    assert report["natural_driving"]["status"] == "warn"
    assert report["natural_driving"]["problem_run_count"] >= 1
    assert any(
        run["run_id"] == "lane_keep_097" and run["verdict"] == "warn"
        for run in report["natural_driving"]["problem_runs"]
    )


def test_refresh_regenerates_mixed_channel_stats_from_row_level_artifacts(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    lane = suite_root / "lane_keep_097"
    artifacts = lane / "artifacts"
    artifacts.mkdir(exist_ok=True)
    (lane / "channel_stats.json").write_text(
        json.dumps(
            {
                "schema_version": "channel_stats.v1",
                "source": {"type": "derived_from_bridge_counters"},
                "channels": {},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "loc_count": 600,
                "chassis_count": 600,
                "obstacles_count": 0,
                "control_rx_count": 300,
                "routing_response_count": 2,
                "planning": {"msg_count": 300},
                "traffic_light": {
                    "channel": "/apollo/perception/traffic_light",
                    "publish_count": 0,
                },
                "timing": {"sim_time_sec": 30.0},
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "planning_topic_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "planning_header_timestamp_sec": timestamp,
                    "planning_header_sequence_num": index,
                }
            )
            for index, timestamp in enumerate((1000.0, 1000.1, 1000.2), start=1)
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "control_decode_debug.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "parsed_control": {
                        "control_timestamp": timestamp,
                        "control_header_sequence_num": index,
                    }
                }
            )
            for index, timestamp in enumerate((1000.0, 1000.05, 1000.1), start=1)
        )
        + "\n",
        encoding="utf-8",
    )

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out", refresh=True)
    lane_result = next(run for run in report["runs"] if run["run_id"] == "lane_keep_097")
    stats = json.loads((lane / "channel_stats.json").read_text(encoding="utf-8"))
    channel = json.loads(Path(lane_result["apollo_channel_health"]["path"]).read_text(encoding="utf-8"))

    assert lane_result["apollo_channel_health"]["status"] == "generated"
    assert lane_result["apollo_channel_health"]["stats_source"] == "mixed_bridge_and_row_level_artifacts"
    assert stats["source"]["type"] == "mixed_bridge_and_row_level_artifacts"
    assert channel["channel_results"]["planning"]["evidence_source"] == "row_level_artifact"
    assert channel["channel_results"]["control"]["evidence_source"] == "row_level_artifact"


def test_postprocess_generates_warn_traffic_light_contract_report_from_default_configs(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    traffic = suite_root / "traffic_light_red_stop"
    (traffic / "traffic_light_contract_report.json").unlink()

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    traffic_result = next(run for run in report["runs"] if run["scenario_class"] == "traffic_light_red_stop")
    traffic_path = Path(traffic_result["traffic_light_contract"]["path"])
    traffic_report = json.loads(traffic_path.read_text(encoding="utf-8"))

    assert traffic_result["traffic_light_contract"]["status"] == "generated"
    assert traffic_result["traffic_light_contract"]["report_status"] == "warn"
    assert traffic_report["status"] == "warn"
    assert "town01_apollo_contract_warn" in traffic_report["warnings"]
    assert report["natural_driving"]["status"] == "warn"


def test_postprocess_regenerates_thin_traffic_light_contract_report(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    traffic = suite_root / "traffic_light_red_stop"
    contract_path = traffic / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract.pop("source", None)
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    traffic_result = next(run for run in report["runs"] if run["scenario_class"] == "traffic_light_red_stop")
    regenerated_path = Path(traffic_result["traffic_light_contract"]["path"])
    regenerated = json.loads(regenerated_path.read_text(encoding="utf-8"))

    assert traffic_result["traffic_light_contract"]["status"] == "generated"
    assert regenerated_path.parent.name == "traffic_light"
    assert regenerated["source"]["town01_contract_path"] == "configs/town01/apollo_contract.example.yaml"
    assert regenerated["source"]["traffic_light_mapping_path"] == "configs/town01/traffic_lights.example.yaml"
    assert report["natural_driving"]["status"] == "warn"


def test_postprocess_regenerates_traffic_light_contract_when_scenario_context_is_stale(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    traffic = suite_root / "traffic_light_red_stop"
    contract_path = traffic / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["scenario_class"] = "traffic_light_green_go"
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    traffic_result = next(run for run in report["runs"] if run["scenario_class"] == "traffic_light_red_stop")
    regenerated = json.loads(Path(traffic_result["traffic_light_contract"]["path"]).read_text(encoding="utf-8"))

    assert traffic_result["traffic_light_contract"]["status"] == "generated"
    assert regenerated["scenario_class"] == "traffic_light_red_stop"
    assert report["natural_driving"]["status"] == "warn"


def test_postprocess_regenerates_traffic_light_behavior_when_context_is_stale(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    traffic = suite_root / "traffic_light_red_stop"
    behavior_path = traffic / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["scenario_class"] = "traffic_light_green_go"
    behavior["route_id"] = "traffic_light_green_go_tbd"
    behavior_path.write_text(json.dumps(behavior, indent=2) + "\n", encoding="utf-8")

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    traffic_result = next(run for run in report["runs"] if run["scenario_class"] == "traffic_light_red_stop")
    regenerated = json.loads(Path(traffic_result["traffic_light_behavior"]["path"]).read_text(encoding="utf-8"))

    assert traffic_result["traffic_light_behavior"]["status"] == "generated"
    assert regenerated["scenario_class"] == "traffic_light_red_stop"
    assert regenerated["route_id"] == "traffic_light_red_stop_tbd"
    assert report["natural_driving"]["status"] == "pass"


def test_postprocess_writes_insufficient_traffic_light_contract_report_when_configs_missing(
    tmp_path: Path,
) -> None:
    suite_root = _copy_suite(tmp_path)
    traffic = suite_root / "traffic_light_red_stop"
    (traffic / "traffic_light_contract_report.json").unlink()

    report = postprocess_natural_driving_runs(
        suite_root,
        out_dir=tmp_path / "out",
        town01_contract_config=tmp_path / "missing_contract.yaml",
        traffic_light_mapping_config=tmp_path / "missing_mapping.yaml",
    )
    traffic_result = next(run for run in report["runs"] if run["scenario_class"] == "traffic_light_red_stop")
    traffic_path = Path(traffic_result["traffic_light_contract"]["path"])
    traffic_report = json.loads(traffic_path.read_text(encoding="utf-8"))

    assert traffic_result["traffic_light_contract"]["status"] == "insufficient_data"
    assert traffic_report["status"] == "insufficient_data"
    assert "town01_apollo_contract_config" in traffic_report["missing_inputs"]
    assert "traffic_light_mapping_config" in traffic_report["missing_inputs"]
    assert report["natural_driving"]["status"] == "insufficient_data"


def test_postprocess_cli_writes_outputs(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    out_dir = tmp_path / "cli_out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/postprocess_town01_natural_driving.py",
            "--suite-root",
            str(suite_root),
            "--out",
            str(out_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert payload["run_count"] == 3
    assert payload["artifact_index_pre_refresh"]["status"] == "no_run_matrix"
    assert payload["artifact_index_refresh"]["status"] == "no_run_matrix"
    assert Path(payload["outputs"]["natural_driving_postprocess_json"]).is_file()
    assert payload["suite_manifest_postprocess_update"]["status"] == "suite_manifest_missing"


def test_postprocess_cli_updates_suite_manifest_when_present(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    out_dir = tmp_path / "cli_out"
    suite_manifest_path = suite_root / "suite_manifest.json"
    suite_manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "natural_driving_suite_manifest.v1",
                "batch_id": "fixture_suite",
                "claim_boundary": {
                    "can_claim_natural_driving_from_manifest": False,
                    "postprocess_required_for_claim": True,
                    "postprocess_evidence_status": "not_run",
                    "required_report": "natural_driving_report.json",
                },
                "runs": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "tools/postprocess_town01_natural_driving.py",
            "--suite-root",
            str(suite_root),
            "--out",
            str(out_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    manifest = json.loads(suite_manifest_path.read_text(encoding="utf-8"))

    assert payload["status"] == "insufficient_data"
    assert payload["suite_manifest_postprocess_update"]["status"] == "updated"
    assert payload["suite_manifest_postprocess_update"]["postprocess_status"] == payload["status"]
    assert manifest["postprocess_status"] == payload["status"]
    assert manifest["postprocess_outputs"]["natural_driving_report"].endswith(
        "natural_driving_report.json"
    )
    evidence = manifest["postprocess_evidence"]
    assert evidence["schema_version"] == "natural_driving_postprocess_evidence.v1"
    assert evidence["status"] == payload["status"]
    assert evidence["outputs"]["natural_driving_postprocess_json"].endswith(
        "natural_driving_postprocess.json"
    )
    assert manifest["claim_boundary"]["can_claim_natural_driving_from_manifest"] is False
    assert manifest["claim_boundary"]["postprocess_required_for_claim"] is True
    assert manifest["claim_boundary"]["postprocess_evidence_status"] == payload["status"]


def test_postprocess_cli_full_target_coverage_gate_blocks_subset_reports(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    out_dir = tmp_path / "cli_out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/postprocess_town01_natural_driving.py",
            "--suite-root",
            str(suite_root),
            "--out",
            str(out_dir),
            "--require-full-target-coverage",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 3
    payload = json.loads(result.stdout)
    assert payload["status"] == "insufficient_data"
    assert payload["coverage_check"]["required"] is True
    assert payload["coverage_check"]["passed"] is False
    assert payload["coverage_check"]["suite_plan_missing"] == ["suite_manifest.json", "run_matrix.csv"]
    assert "curve_diagnostic" in payload["coverage_check"]["missing_required_scenario_classes"]
    assert "traffic_light_green_go" in payload["coverage_check"]["missing_required_scenario_classes"]
    assert "missing_required_scenario_ids" in payload["coverage_check"]
    assert "unproven_required_scenario_ids" in payload["coverage_check"]
    assert "scenario_identity_mismatches" in payload["coverage_check"]


def test_postprocess_cli_can_fail_on_selected_status(tmp_path: Path) -> None:
    suite_root = _copy_suite(tmp_path)
    (suite_root / "lane_keep_097" / "events.jsonl").unlink()
    out_dir = tmp_path / "cli_out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/postprocess_town01_natural_driving.py",
            "--suite-root",
            str(suite_root),
            "--out",
            str(out_dir),
            "--fail-on-status",
            "fail,insufficient_data",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "insufficient_data"
    assert payload["problem_run_count"] >= 1
    assert "lane_keep_097" in payload["insufficient_data_runs"]
    assert "lane_keep_097" in payload["artifact_incomplete_runs"]
    problem = next(run for run in payload["problem_runs"] if run["run_id"] == "lane_keep_097")
    assert problem["failure_reason"] == "missing_required_artifacts"
    assert "events.jsonl" in problem["missing_artifacts"]


def test_postprocess_includes_run_matrix_rows_without_summary(tmp_path: Path) -> None:
    suite_root = tmp_path / "suite"
    run_dir = suite_root / "lane_keep" / "lane_keep_097"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "lane_keep_097",
                "scenario_class": "lane_keep",
                "route_id": "lane097",
                "algorithm_variant_id": "apollo_10_0_carla_gt_town01_reference",
                "algorithm_variant_manifest_path": "configs/algorithms/apollo_variant.carla_gt.example.yaml",
                "map": "Town01",
                "transport_mode": "ros2_gt",
                "backend": "apollo_cyberrt",
                "truth_input": True,
                "duration_s": 70,
                "fixed_delta_seconds": 0.05,
                "ticks": 1400,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    with (suite_root / "run_matrix.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_id", "scenario_class", "run_dir"])
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "lane_keep_097",
                "scenario_class": "lane_keep",
                "run_dir": str(run_dir),
            }
        )

    report = postprocess_natural_driving_runs(suite_root, out_dir=tmp_path / "out")
    run = report["runs"][0]

    assert report["run_count"] == 1
    assert report["natural_driving"]["status"] == "insufficient_data"
    assert run["run_id"] == "lane_keep_097"
    assert run["route_health"]["status"] == "generated"
    assert run["control_health"]["status"] == "generated"
    assert run["apollo_channel_health"]["status"] == "insufficient_data"
    assert run["artifact_completeness"]["status"] == "insufficient_data"
    assert "summary.json" in run["artifact_completeness"]["missing_artifacts"]
    assert Path(run["route_health"]["path"]).is_file()
    assert Path(run["control_health"]["path"]).is_file()
