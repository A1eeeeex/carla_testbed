from __future__ import annotations

import csv
import json

from carla_testbed.analysis.v_t_gap import extract_v_t_gap, write_v_t_gap_report
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def test_v_t_gap_extracts_bumper_gap_from_timeseries_and_actor_trace(tmp_path) -> None:
    run = _write_run_fixture(tmp_path)

    report = extract_v_t_gap(run_dir=run)

    assert report["schema_version"] == "v_t_gap.v1"
    assert report["status"] == "pass"
    assert report["row_count"] == 2
    assert report["rows"][0]["gap_m"] == 16.0
    assert report["rows"][0]["gap_method"] == "bumper_to_bumper_longitudinal_projection"
    assert report["rows"][0]["gap_degraded"] is False
    assert report["gap_method_counts"]["bumper_to_bumper_longitudinal_projection"] == 2
    assert report["rows"][0]["target_actor_role"] == "lead_vehicle"


def test_v_t_gap_missing_target_contract_is_invalid(tmp_path) -> None:
    run = tmp_path / "run"
    (run / "artifacts").mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
    (run / "artifacts" / "fixed_scene_resolved.json").write_text(
        json.dumps({"target_actor_contract": {"status": "missing"}}),
        encoding="utf-8",
    )

    report = extract_v_t_gap(run_dir=run)

    assert report["schema_version"] == "v_t_gap.v1"
    assert report["status"] == "invalid"
    assert report["invalid_reason"] == "missing_target_actor"


def test_v_t_gap_prefers_actor_trace_longitudinal_gap_when_dimensions_missing(tmp_path) -> None:
    run = _write_run_fixture(tmp_path)
    trace_path = run / "artifacts" / "scenario_actor_trace.jsonl"
    rows = [
        {"sim_time_sec": 0.0, "actor_role": "lead_vehicle", "actor_id": "lead-1", "actual_speed_mps": 0.0, "x": -300.0, "y": 0.0, "yaw_rad": 3.14, "longitudinal_to_ego_m": 300.0},
    ]
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "warn"
    assert report["rows"][0]["gap_m"] == 300.0
    assert report["rows"][0]["gap_method"] == "actor_trace_longitudinal_gap_degraded"
    assert report["rows"][0]["gap_degraded"] is True
    assert report["rows"][0]["gap_degraded_reason"] == "missing_actor_bbox_or_length"
    assert report["degraded_reason_counts"]["actor_trace_longitudinal_gap_degraded"] == 2


def test_v_t_gap_writer_outputs_csv_and_report(tmp_path) -> None:
    run = _write_run_fixture(tmp_path)
    report = extract_v_t_gap(run_dir=run)

    outputs = write_v_t_gap_report(report, tmp_path / "out")

    assert outputs["report"].endswith("v_t_gap_report.json")
    assert (tmp_path / "out" / "v_t_gap.csv").exists()
    with (tmp_path / "out" / "v_t_gap.csv").open("r", encoding="utf-8") as handle:
        header = handle.readline().strip().split(",")
    assert "gap_degraded_reason" in header


def _write_run_fixture(tmp_path):
    run = tmp_path / "run"
    artifacts = run / "artifacts"
    artifacts.mkdir(parents=True)
    template = load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    storyboard = compile_fixed_scene_template(template)
    (artifacts / "fixed_scene_resolved.json").write_text(json.dumps(storyboard), encoding="utf-8")
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sim_time", "ego_speed_mps", "ego_x", "ego_y", "ego_yaw_rad", "ego_length_m", "ego_width_m"],
        )
        writer.writeheader()
        writer.writerow({"sim_time": 0.0, "ego_speed_mps": 10.0, "ego_x": 0.0, "ego_y": 0.0, "ego_yaw_rad": 0.0, "ego_length_m": 4.0, "ego_width_m": 2.0})
        writer.writerow({"sim_time": 1.0, "ego_speed_mps": 10.0, "ego_x": 10.0, "ego_y": 0.0, "ego_yaw_rad": 0.0, "ego_length_m": 4.0, "ego_width_m": 2.0})
    rows = [
        {"sim_time_sec": 0.0, "actor_role": "lead_vehicle", "actor_id": "lead-1", "actual_speed_mps": 8.0, "x": 20.0, "y": 0.0, "yaw_rad": 0.0, "length_m": 4.0, "width_m": 2.0},
        {"sim_time_sec": 1.0, "actor_role": "lead_vehicle", "actor_id": "lead-1", "actual_speed_mps": 8.0, "x": 28.0, "y": 0.0, "yaw_rad": 0.0, "length_m": 4.0, "width_m": 2.0},
    ]
    (artifacts / "scenario_actor_trace.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    return run
