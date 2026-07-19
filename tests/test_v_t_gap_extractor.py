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
    assert report["validity"] == "valid"
    assert report["source_files"]["timeseries"].endswith("timeseries.csv")
    assert report["source_files"]["actor_trace"].endswith("scenario_actor_trace.jsonl")
    assert report["rows"][0]["gap_m"] == 16.0
    assert report["rows"][0]["gap_method"] == "bumper_to_bumper_longitudinal_projection"
    assert report["rows"][0]["gap_degraded"] is False
    assert report["rows"][0]["validity"] == "valid"
    assert report["rows"][0]["invalid_reason"] is None
    assert "timeseries.csv" in report["rows"][0]["source_files"]
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
    assert report["validity"] == "invalid"
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
    assert report["validity"] == "degraded"
    assert report["rows"][0]["gap_m"] == 300.0
    assert report["rows"][0]["gap_method"] == "actor_trace_longitudinal_gap_degraded"
    assert report["rows"][0]["gap_degraded"] is True
    assert report["rows"][0]["gap_degraded_reason"] == "missing_actor_bbox_or_length"
    assert report["rows"][0]["validity"] == "degraded"
    assert report["degraded_reason_counts"]["actor_trace_longitudinal_gap_degraded"] == 1
    assert report["trace_overlap_filter"]["rows_after_filter"] == 1


def test_v_t_gap_degrades_longitudinal_projection_when_lateral_offset_is_large(tmp_path) -> None:
    run = _write_run_fixture(tmp_path)
    trace_path = run / "artifacts" / "scenario_actor_trace.jsonl"
    rows = [
        {
            "sim_time_sec": 0.0,
            "actor_role": "lead_vehicle",
            "actor_id": "lead-1",
            "actual_speed_mps": 8.0,
            "x": 20.0,
            "y": 20.0,
            "yaw_rad": 0.0,
            "length_m": 4.0,
            "width_m": 2.0,
        },
    ]
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "warn"
    assert report["validity"] == "degraded"
    assert report["rows"][0]["gap_method"] == "bumper_to_bumper_longitudinal_projection_lateral_degraded"
    assert report["rows"][0]["gap_degraded"] is True
    assert report["rows"][0]["gap_degraded_reason"] == "lateral_offset_exceeds_projection_validity"
    assert report["rows"][0]["lateral_offset_m"] == 20.0


def test_v_t_gap_prefers_route_s_gap_when_same_lane_route_s_is_available(tmp_path) -> None:
    run = _write_route_s_gap_fixture(tmp_path, ego_lane_id="15:0:1", target_lane_id="15:0:1")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "pass"
    assert report["validity"] == "valid"
    assert report["rows"][0]["gap_method"] == "route_s_bumper_gap"
    assert report["rows"][0]["gap_m"] == 16.0
    assert report["rows"][0]["longitudinal_center_gap_m"] == 20.0
    assert report["rows"][0]["gap_degraded"] is False
    assert report["rows"][0]["route_gap_unavailable_reason"] is None
    assert report["rows"][0]["route_s_direction_anchor_m"] == 20.0
    assert report["rows"][0]["route_s_direction_anchor_source"] == "ego_pose_forward_projection"
    assert report["rows"][0]["route_s_direction_corrected"] is False
    assert report["rows"][0]["ego_route_s"] == 10.0
    assert report["rows"][0]["target_route_s"] == 30.0
    assert report["rows"][0]["ego_lane_id"] == "15:0:1"
    assert report["rows"][0]["target_lane_id"] == "15:0:1"


def test_v_t_gap_route_s_gap_corrects_lane_s_direction_with_forward_anchor(tmp_path) -> None:
    run = _write_route_s_gap_fixture(
        tmp_path,
        ego_lane_id="15:0:1",
        target_lane_id="15:0:1",
        ego_route_s=30.0,
        target_route_s=10.0,
    )

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "pass"
    assert report["rows"][0]["gap_method"] == "route_s_bumper_gap"
    assert report["rows"][0]["gap_m"] == 16.0
    assert report["rows"][0]["longitudinal_center_gap_m"] == 20.0
    assert report["rows"][0]["route_s_direction_corrected"] is True


def test_v_t_gap_rejects_route_s_gap_when_anchor_conflicts_with_lane_s(tmp_path) -> None:
    run = _write_route_s_gap_fixture(
        tmp_path,
        ego_lane_id="15:0:1",
        target_lane_id="15:0:1",
        ego_route_s=106.0,
        target_route_s=105.0,
    )
    trace_path = run / "artifacts" / "scenario_actor_trace.jsonl"
    trace = json.loads(trace_path.read_text(encoding="utf-8").strip())
    trace["longitudinal_to_ego_m"] = 29.0
    trace_path.write_text(json.dumps(trace) + "\n", encoding="utf-8")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "pass"
    assert report["validity"] == "valid"
    assert report["rows"][0]["gap_method"] == "trajectory_progress_bumper_gap"
    assert report["rows"][0]["gap_m"] == 25.0
    assert report["rows"][0]["longitudinal_center_gap_m"] == 29.0
    assert report["rows"][0]["route_gap_unavailable_reason"] == "route_s_anchor_conflict"


def test_v_t_gap_uses_trajectory_progress_gap_when_lane_ids_differ(tmp_path) -> None:
    run = _write_route_s_gap_fixture(tmp_path, ego_lane_id="15:0:1", target_lane_id="16:0:1")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "pass"
    assert report["validity"] == "valid"
    assert report["rows"][0]["gap_method"] == "trajectory_progress_bumper_gap"
    assert report["rows"][0]["gap_m"] == 16.0
    assert report["rows"][0]["route_gap_unavailable_reason"] == "lane_id_mismatch"
    assert report["rows"][0]["trajectory_progress_initial_center_gap_m"] == 20.0
    assert report["rows"][0]["trajectory_progress_source"] == "ego_pose_forward_projection"
    assert report["rows"][0]["gap_degraded"] is False


def test_v_t_gap_uses_ego_heading_and_vehicle_characteristics_for_apollo_derived_trace(tmp_path) -> None:
    run = _write_run_fixture(tmp_path)
    artifacts = run / "artifacts"
    (artifacts / "carla_vehicle_characteristics.json").write_text(
        json.dumps({"length": 4.8, "width": 1.8}),
        encoding="utf-8",
    )
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sim_time", "ego_speed", "ego_x", "ego_y", "ego_heading"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time": 0.0,
                "ego_speed": 1.0,
                "ego_x": 300.0,
                "ego_y": -5.0,
                "ego_heading": 3.141592653589793,
            }
        )
    (artifacts / "scenario_actor_trace.jsonl").write_text(
        json.dumps(
            {
                "sim_time_sec": 0.0,
                "actor_role": "lead_vehicle",
                "actor_id": "lead-1",
                "actual_speed_mps": 0.0,
                "x": 0.0,
                "y": -5.0,
                "yaw_rad": 3.141592653589793,
                "length_m": 4.8,
                "width_m": 1.8,
                "source": "derived_from_apollo_obstacle_gt_contract",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "pass"
    assert report["rows"][0]["gap_method"] == "bumper_to_bumper_longitudinal_projection"
    assert report["rows"][0]["gap_degraded"] is False
    assert report["rows"][0]["longitudinal_center_gap_m"] == 300.0
    assert round(report["rows"][0]["gap_m"], 1) == 295.2


def test_v_t_gap_keeps_degraded_gap_when_trajectory_progress_lacks_lengths(tmp_path) -> None:
    run = _write_route_s_gap_fixture(tmp_path, ego_lane_id="15:0:1", target_lane_id="16:0:1")
    timeseries_path = run / "timeseries.csv"
    with timeseries_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sim_time",
                "ego_speed_mps",
                "ego_x",
                "ego_y",
                "ego_yaw_rad",
                "ego_route_s",
                "ego_lane_id",
                "ego_length_m",
                "ego_width_m",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time": 0.0,
                "ego_speed_mps": 10.0,
                "ego_x": 0.0,
                "ego_y": 0.0,
                "ego_yaw_rad": 0.0,
                "ego_route_s": 10.0,
                "ego_lane_id": "15:0:1",
                "ego_length_m": 4.0,
                "ego_width_m": 2.0,
            }
        )
    (run / "artifacts" / "scenario_actor_trace.jsonl").write_text(
        json.dumps(
            {
                "sim_time_sec": 0.0,
                "actor_role": "lead_vehicle",
                "actor_id": "lead-1",
                "actual_speed_mps": 8.0,
                "x": 20.0,
                "y": 20.0,
                "yaw_rad": 0.0,
                "route_s": 30.0,
                "lane_id": "16:0:1",
                "width_m": 2.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "warn"
    assert report["validity"] == "degraded"
    assert report["rows"][0]["gap_method"] == "center_distance_fallback"
    assert report["rows"][0]["route_gap_unavailable_reason"] == "lane_id_mismatch"
    assert report["rows"][0]["gap_degraded_reason"] == "missing_actor_bbox_or_length"


def test_v_t_gap_writer_outputs_csv_and_report(tmp_path) -> None:
    run = _write_run_fixture(tmp_path)
    report = extract_v_t_gap(run_dir=run)

    outputs = write_v_t_gap_report(report, tmp_path / "out")

    assert outputs["report"].endswith("v_t_gap_report.json")
    assert (tmp_path / "out" / "v_t_gap.csv").exists()
    with (tmp_path / "out" / "v_t_gap.csv").open("r", encoding="utf-8") as handle:
        header = handle.readline().strip().split(",")
    assert "gap_degraded_reason" in header
    assert "lateral_offset_m" in header
    assert "ego_route_s" in header
    assert "target_route_s" in header
    assert "route_s_direction_anchor_m" in header
    assert "ego_trajectory_progress_m" in header
    assert "target_trajectory_progress_m" in header
    assert "trajectory_progress_source" in header
    assert "route_gap_unavailable_reason" in header
    assert "source_files" in header
    assert "validity" in header
    assert "invalid_reason" in header


def test_v_t_gap_respects_active_after_phase_target_activation(tmp_path) -> None:
    run = _write_cut_in_run_fixture(tmp_path)

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "pass"
    assert report["target_activation_filter"]["mode"] == "active_after_phase"
    assert report["target_activation_filter"]["active_after_phase"] == "cut_in_lane_change"
    assert report["target_activation_filter"]["activation_start_s"] == 5.0
    assert report["target_activation_filter"]["rows_before_filter"] == 3
    assert report["target_activation_filter"]["rows_after_filter"] == 2
    assert report["row_count"] == 2
    assert [row["sim_time_s"] for row in report["rows"]] == [5.0, 6.0]


def test_v_t_gap_aligns_relative_actor_trace_to_absolute_apollo_timeseries(tmp_path) -> None:
    run = _write_cut_in_run_fixture(tmp_path)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "metadata": {
                    "scenario_metadata": {
                        "fixed_scene_runtime_hook": {
                            "start_sim_time_s": 100.0,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sim_time", "ego_speed_mps", "ego_x", "ego_y", "ego_yaw_rad", "ego_length_m", "ego_width_m"],
        )
        writer.writeheader()
        # The absolute timeseries can contain a long setup/warmup interval.
        # Explicit runtime-start alignment must use range overlap rather than
        # whichever trace origin happens to be closest to the first warmup row.
        for sim_time, ego_x in [(40.0, -60.0), (100.0, 0.0), (105.0, 5.0), (106.0, 6.0)]:
            writer.writerow(
                {
                    "sim_time": sim_time,
                    "ego_speed_mps": 10.0,
                    "ego_x": ego_x,
                    "ego_y": 0.0,
                    "ego_yaw_rad": 0.0,
                    "ego_length_m": 4.0,
                    "ego_width_m": 2.0,
                }
            )

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "pass"
    assert report["time_alignment"]["status"] == "applied"
    assert report["time_alignment"]["source"] == "fixed_scene_runtime_hook.start_sim_time_s"
    assert report["time_alignment"]["offset_s"] == 100.0
    assert report["trace_overlap_filter"]["status"] == "applied"
    assert report["target_activation_filter"]["activation_start_s"] == 105.0
    assert [row["sim_time_s"] for row in report["rows"]] == [105.0, 106.0]
    assert report["rows"][0]["longitudinal_center_gap_m"] == 14.0
    assert report["rows"][0]["gap_m"] == 10.0


def test_v_t_gap_excludes_warmup_rows_before_deferred_actor_trace(tmp_path) -> None:
    run = _write_cut_in_run_fixture(tmp_path)
    resolved_path = run / "artifacts" / "fixed_scene_resolved.json"
    resolved = json.loads(resolved_path.read_text(encoding="utf-8"))
    resolved["target_actor_contract"]["activation"] = {
        "activation_semantics": "active_from_scenario_start",
        "active_after_phase": None,
    }
    resolved_path.write_text(json.dumps(resolved), encoding="utf-8")
    (run / "manifest.json").write_text(
        json.dumps({"fixed_scene_runtime_hook": {"start_sim_time_s": 100.0}}),
        encoding="utf-8",
    )
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sim_time", "ego_speed_mps", "ego_x", "ego_y", "ego_yaw_rad", "ego_length_m", "ego_width_m"],
        )
        writer.writeheader()
        for sim_time, ego_x in [(80.0, 200.0), (100.0, 0.0), (105.0, 5.0), (106.0, 6.0)]:
            writer.writerow(
                {
                    "sim_time": sim_time,
                    "ego_speed_mps": 10.0,
                    "ego_x": ego_x,
                    "ego_y": 0.0,
                    "ego_yaw_rad": 0.0,
                    "ego_length_m": 4.0,
                    "ego_width_m": 2.0,
                }
            )

    report = extract_v_t_gap(run_dir=run)

    assert [row["sim_time_s"] for row in report["rows"]] == [100.0, 105.0, 106.0]
    assert report["trace_overlap_filter"]["excluded_before_trace"] == 1
    assert min(row["gap_m"] for row in report["rows"]) >= 0.0


def test_v_t_gap_invalid_when_activation_phase_has_no_timestamp(tmp_path) -> None:
    run = _write_cut_in_run_fixture(tmp_path)
    trace_path = run / "artifacts" / "scenario_actor_trace.jsonl"
    rows = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    for row in rows:
        if row.get("phase") == "cut_in_lane_change":
            row.pop("sim_time_sec", None)
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "invalid"
    assert report["invalid_reason"] == "missing_target_activation_phase"
    assert report["target_activation_filter"]["invalid_reason"] == "target_activation_phase_time_missing"


def test_route_only_target_not_required_is_not_applicable(tmp_path) -> None:
    run = tmp_path / "route_only"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "route_only",
                "scenario_case": "town01_lane_keep_097",
                "backend": "carla_builtin",
                "target_actor_contract": {
                    "schema_version": "target_actor_contract.v1",
                    "status": "not_required",
                    "required": False,
                    "scenario_class": "lane_keep",
                    "target_actor_role": None,
                    "source": "scenario_class_not_required",
                },
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "not_applicable"
    assert report["validity"] == "not_applicable"
    assert report["target_actor_contract"]["status"] == "not_required"
    assert report["missing_fields"] == []


def test_legacy_route_only_manifest_infers_target_not_required(tmp_path) -> None:
    run = tmp_path / "apollo_route_only"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "apollo_route_only",
                "scenario_case": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "backend": "apollo_cyberrt",
            }
        ),
        encoding="utf-8",
    )
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")

    report = extract_v_t_gap(run_dir=run)

    assert report["status"] == "not_applicable"
    assert report["target_actor_contract"]["status"] == "not_required"
    assert report["target_actor_contract"]["source"] == "scenario_class_not_required"


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


def _write_route_s_gap_fixture(
    tmp_path,
    *,
    ego_lane_id: str,
    target_lane_id: str,
    ego_route_s: float = 10.0,
    target_route_s: float = 30.0,
):
    run = _write_run_fixture(tmp_path)
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sim_time",
                "ego_speed_mps",
                "ego_x",
                "ego_y",
                "ego_yaw_rad",
                "ego_route_s",
                "ego_lane_id",
                "ego_length_m",
                "ego_width_m",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sim_time": 0.0,
                "ego_speed_mps": 10.0,
                "ego_x": 0.0,
                "ego_y": 0.0,
                "ego_yaw_rad": 0.0,
                "ego_route_s": ego_route_s,
                "ego_lane_id": ego_lane_id,
                "ego_length_m": 4.0,
                "ego_width_m": 2.0,
            }
        )
    rows = [
        {
            "sim_time_sec": 0.0,
            "actor_role": "lead_vehicle",
            "actor_id": "lead-1",
            "actual_speed_mps": 8.0,
            "x": 20.0,
            "y": 20.0,
            "yaw_rad": 0.0,
            "route_s": target_route_s,
            "lane_id": target_lane_id,
            "length_m": 4.0,
            "width_m": 2.0,
        }
    ]
    (run / "artifacts" / "scenario_actor_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    return run


def _write_cut_in_run_fixture(tmp_path):
    run = tmp_path / "cut_in"
    artifacts = run / "artifacts"
    artifacts.mkdir(parents=True)
    template = load_fixed_scene_template("configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml")
    storyboard = compile_fixed_scene_template(template)
    (artifacts / "fixed_scene_resolved.json").write_text(json.dumps(storyboard), encoding="utf-8")
    with (run / "timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sim_time", "ego_speed_mps", "ego_x", "ego_y", "ego_yaw_rad", "ego_length_m", "ego_width_m"],
        )
        writer.writeheader()
        for sim_time, ego_x in [(0.0, 0.0), (5.0, 5.0), (6.0, 6.0)]:
            writer.writerow(
                {
                    "sim_time": sim_time,
                    "ego_speed_mps": 10.0,
                    "ego_x": ego_x,
                    "ego_y": 0.0,
                    "ego_yaw_rad": 0.0,
                    "ego_length_m": 4.0,
                    "ego_width_m": 2.0,
                }
            )
    rows = [
        {
            "sim_time_sec": 0.0,
            "phase": "adjacent_lane_prepare",
            "actor_role": "lead_vehicle",
            "actor_id": "lead-1",
            "actual_speed_mps": 9.0,
            "x": 20.0,
            "y": -3.6,
            "yaw_rad": 0.0,
            "length_m": 4.0,
            "width_m": 2.0,
        },
        {
            "sim_time_sec": 5.0,
            "phase": "cut_in_lane_change",
            "actor_role": "lead_vehicle",
            "actor_id": "lead-1",
            "actual_speed_mps": 9.0,
            "x": 19.0,
            "y": 0.0,
            "yaw_rad": 0.0,
            "length_m": 4.0,
            "width_m": 2.0,
        },
        {
            "sim_time_sec": 6.0,
            "phase": "cut_in_lane_change",
            "actor_role": "lead_vehicle",
            "actor_id": "lead-1",
            "actual_speed_mps": 9.0,
            "x": 20.0,
            "y": 0.0,
            "yaw_rad": 0.0,
            "length_m": 4.0,
            "width_m": 2.0,
        },
    ]
    (artifacts / "scenario_actor_trace.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    return run
