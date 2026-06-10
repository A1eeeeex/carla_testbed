from __future__ import annotations

import json

from carla_testbed.analysis.scenario_actor_contract import (
    analyze_scenario_actor_contract,
    write_scenario_actor_contract_report,
)
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def test_follow_stop_actor_contract_passes_when_lead_stops(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_follow_stop_actor_fixture(tmp_path, final_speed=0.0)

    report = analyze_scenario_actor_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "pass"
    assert report["behavior"]["lead_stopped"] is True
    assert report["behavior"]["min_gap_m"] == 12.0


def test_follow_stop_actor_contract_fails_if_lead_never_stops(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_follow_stop_actor_fixture(tmp_path, final_speed=4.0)

    report = analyze_scenario_actor_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "fail"
    assert "lead_vehicle_never_observed_stopped" in report["blocking_reasons"]


def test_static_lead_actor_contract_fails_if_lead_moves(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    rows = [
        {"actor_role": "lead_vehicle", "phase": "lead_hold_stop", "actual_speed_mps": 0.0},
        {"actor_role": "lead_vehicle", "phase": "lead_hold_stop", "actual_speed_mps": 1.2},
    ]
    events = [{"phase": "lead_hold_stop", "event": "phase_started"}]
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    events_path.write_text("\n".join(json.dumps(row) for row in events) + "\n", encoding="utf-8")

    report = analyze_scenario_actor_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "fail"
    assert "static_lead_vehicle_moved" in report["blocking_reasons"]


def test_scenario_actor_contract_missing_trace_is_insufficient_data(tmp_path) -> None:
    storyboard_path, _, _ = _write_follow_stop_actor_fixture(tmp_path, final_speed=0.0)

    report = analyze_scenario_actor_contract(storyboard_path=storyboard_path, trace_path=None)

    assert report["status"] == "insufficient_data"
    assert "scenario_actor_trace.rows" in report["missing_fields"]


def test_scenario_actor_contract_writes_report_files(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_follow_stop_actor_fixture(tmp_path, final_speed=0.0)
    report = analyze_scenario_actor_contract(storyboard_path=storyboard_path, trace_path=trace_path, events_path=events_path)

    outputs = write_scenario_actor_contract_report(report, tmp_path / "analysis")

    assert (tmp_path / "analysis" / "scenario_actor_contract_report.json").exists()
    assert "scenario_actor_contract_summary.md" in outputs["summary"]


def test_cut_in_actor_contract_requires_observed_lateral_evidence_not_only_progress(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenario_templates/cut_in.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    rows = [
        {"actor_role": "lead_vehicle", "phase": "adjacent_lane_prepare", "lane_change_progress": None},
        {"actor_role": "lead_vehicle", "phase": "cut_in_lane_change", "lane_change_progress": 0.5},
        {"actor_role": "lead_vehicle", "phase": "cut_in_lane_change", "lane_change_progress": 1.0},
    ]
    events = [
        {"phase": "adjacent_lane_prepare", "event": "phase_started"},
        {"phase": "cut_in_lane_change", "event": "phase_started"},
    ]
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    events_path.write_text("\n".join(json.dumps(row) for row in events) + "\n", encoding="utf-8")

    report = analyze_scenario_actor_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "insufficient_data"
    assert report["behavior"]["lane_change_intent_completed"] is True
    assert report["metrics"]["lane_change_completed"] is False
    assert "lateral_to_ego_m" in report["missing_fields"]
    assert "longitudinal_to_ego_m" in report["missing_fields"]


def test_baguang_cut_in_actor_contract_checks_start_gap_and_lateral_shift(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    rows = [
        {
            "actor_role": "lead_vehicle",
            "phase": "cut_in_lane_change",
            "action_type": "lane_change",
            "lane_change_progress": 0.0,
            "longitudinal_to_ego_m": 9.8,
            "lateral_to_ego_m": -3.6,
            "sim_time_sec": 5.0,
            "lane_change_runtime_mode": "set_transform_interpolation",
            "physics_controlled_lane_change": False,
            "claim_grade_lane_change": False,
            "velocity_source": "carla_get_velocity",
        },
        {
            "actor_role": "lead_vehicle",
            "phase": "cut_in_lane_change",
            "action_type": "lane_change",
            "lane_change_progress": 0.25,
            "longitudinal_to_ego_m": 10.0,
            "lateral_to_ego_m": -2.7,
            "sim_time_sec": 6.0,
            "lane_change_runtime_mode": "set_transform_interpolation",
            "physics_controlled_lane_change": False,
            "claim_grade_lane_change": False,
            "velocity_source": "carla_get_velocity",
        },
        {
            "actor_role": "lead_vehicle",
            "phase": "cut_in_lane_change",
            "action_type": "lane_change",
            "lane_change_progress": 0.5,
            "longitudinal_to_ego_m": 10.2,
            "lateral_to_ego_m": -1.8,
            "sim_time_sec": 7.0,
            "lane_change_runtime_mode": "set_transform_interpolation",
            "physics_controlled_lane_change": False,
            "claim_grade_lane_change": False,
            "velocity_source": "carla_get_velocity",
        },
        {
            "actor_role": "lead_vehicle",
            "phase": "cut_in_lane_change",
            "action_type": "lane_change",
            "lane_change_progress": 0.75,
            "longitudinal_to_ego_m": 10.4,
            "lateral_to_ego_m": -0.9,
            "sim_time_sec": 8.0,
            "lane_change_runtime_mode": "set_transform_interpolation",
            "physics_controlled_lane_change": False,
            "claim_grade_lane_change": False,
            "velocity_source": "carla_get_velocity",
        },
        {
            "actor_role": "lead_vehicle",
            "phase": "cut_in_lane_change",
            "action_type": "lane_change",
            "lane_change_progress": 1.0,
            "longitudinal_to_ego_m": 10.5,
            "lateral_to_ego_m": -0.1,
            "sim_time_sec": 9.0,
            "lane_change_runtime_mode": "set_transform_interpolation",
            "physics_controlled_lane_change": False,
            "claim_grade_lane_change": False,
            "velocity_source": "carla_get_velocity",
        },
    ]
    events = [{"phase": "cut_in_lane_change", "event": "phase_started"}]
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    events_path.write_text("\n".join(json.dumps(row) for row in events) + "\n", encoding="utf-8")

    report = analyze_scenario_actor_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "pass"
    assert report["behavior"]["lane_change_start_longitudinal_gap_m"] == 9.8
    assert report["behavior"]["lane_change_lateral_shift_m"] == 3.5
    assert report["behavior"]["lane_change_runtime_modes"] == ["set_transform_interpolation"]
    assert report["behavior"]["claim_grade_lane_change"] is False
    assert report["behavior"]["lateral_dynamics"]["no_teleport_check"] is True


def test_baguang_cut_in_actor_contract_fails_when_start_gap_is_wrong(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    rows = [
        {
            "actor_role": "lead_vehicle",
            "phase": "cut_in_lane_change",
            "action_type": "lane_change",
            "lane_change_progress": 1.0,
            "longitudinal_to_ego_m": 16.0,
            "lateral_to_ego_m": -3.6,
        },
        {
            "actor_role": "lead_vehicle",
            "phase": "cut_in_lane_change",
            "action_type": "lane_change",
            "lane_change_progress": 1.0,
            "longitudinal_to_ego_m": 16.5,
            "lateral_to_ego_m": -0.1,
        },
    ]
    events = [{"phase": "cut_in_lane_change", "event": "phase_started"}]
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    events_path.write_text("\n".join(json.dumps(row) for row in events) + "\n", encoding="utf-8")

    report = analyze_scenario_actor_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "fail"
    assert "lane_change_start_gap_out_of_tolerance" in report["blocking_reasons"]


def _write_follow_stop_actor_fixture(tmp_path, *, final_speed: float):
    template = load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    rows = [
        {"actor_role": "lead_vehicle", "phase": "lead_cruise", "distance_to_ego_m": 300.0, "actual_speed_mps": 16.0},
        {"actor_role": "lead_vehicle", "phase": "lead_brake_to_stop", "distance_to_ego_m": 60.0, "actual_speed_mps": 8.0},
        {"actor_role": "lead_vehicle", "phase": "lead_hold_stop", "distance_to_ego_m": 12.0, "actual_speed_mps": final_speed},
    ]
    events = [
        {"phase": "lead_cruise", "event": "phase_started"},
        {"phase": "lead_brake_to_stop", "event": "phase_started"},
        {"phase": "lead_hold_stop", "event": "phase_started"},
    ]
    trace_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    events_path.write_text("\n".join(json.dumps(row) for row in events) + "\n", encoding="utf-8")
    return storyboard_path, trace_path, events_path
