from __future__ import annotations

import json

from carla_testbed.analysis.fixed_scene_contract import analyze_fixed_scene_contract, write_fixed_scene_contract_report
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def test_fixed_scene_contract_passes_with_storyboard_trace_and_events(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_fixed_scene_fixture(tmp_path)

    report = analyze_fixed_scene_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "pass"
    assert report["scene_id"] == "follow_stop_097"
    assert report["missing_artifacts"] == []


def test_fixed_scene_contract_missing_trace_is_insufficient_data(tmp_path) -> None:
    storyboard_path, _, _ = _write_fixed_scene_fixture(tmp_path)

    report = analyze_fixed_scene_contract(storyboard_path=storyboard_path)

    assert report["status"] == "insufficient_data"
    assert "artifacts/scenario_actor_trace.jsonl" in report["missing_artifacts"]


def test_fixed_scene_contract_fails_on_spawn_feasibility_failure(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_fixed_scene_fixture(tmp_path)
    runtime_state_path = tmp_path / "fixed_scene_runtime_state.json"
    runtime_state_path.write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_runtime_state.v1",
                "spawn_feasibility": {
                    "lead_vehicle": {
                        "status": "fail",
                        "blocking_reasons": ["waypoint_spawn_required_but_fallback_used"],
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    report = analyze_fixed_scene_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
        runtime_state_path=runtime_state_path,
    )

    assert report["status"] == "fail"
    assert "spawn_feasibility_failed:lead_vehicle" in report["blocking_reasons"]
    assert report["metrics"]["spawn_feasibility_pass"] is False


def test_fixed_scene_contract_fails_when_required_phase_does_not_complete(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_fixed_scene_fixture(tmp_path, include_completed=False)

    report = analyze_fixed_scene_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "fail"
    assert "fixed_scene_required_phase_not_completed" in report["blocking_reasons"]
    assert report["missing_completed_required_phases"] == [
        "lead_brake_to_stop",
        "lead_cruise",
        "lead_hold_stop",
    ]


def test_static_lead_hold_phase_does_not_require_completion_for_short_smoke(tmp_path) -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    trace_path.write_text(
        json.dumps({"schema_version": "scenario_actor_trace.v1", "scene_id": storyboard["scene_id"], "phase": "lead_hold_stop", "actor_role": "lead_vehicle"}) + "\n",
        encoding="utf-8",
    )
    events_path.write_text(
        json.dumps({"schema_version": "scenario_phase_event.v1", "scene_id": storyboard["scene_id"], "phase": "lead_hold_stop", "event": "phase_started"}) + "\n",
        encoding="utf-8",
    )

    report = analyze_fixed_scene_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "pass"
    assert report["required_completion_phases"] == []
    assert "fixed_scene_required_phase_not_completed" not in report["blocking_reasons"]


def test_duration_policy_allows_small_route_end_tolerance(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_duration_policy_fixture(tmp_path, route_s=299.91)

    report = analyze_fixed_scene_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "pass"
    assert report["duration_policy"]["verified"] is True
    assert report["duration_policy"]["lead_route_end_tolerance_m"] == 0.5
    assert report["duration_policy"]["lead_route_end_margin_m"] < 0.0


def test_duration_policy_still_fails_when_route_end_missed_beyond_tolerance(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_duration_policy_fixture(tmp_path, route_s=298.0)

    report = analyze_fixed_scene_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )

    assert report["status"] == "fail"
    assert "duration_policy_route_end_not_reached" in report["blocking_reasons"]
    assert report["duration_policy"]["verified"] is False


def test_fixed_scene_contract_writes_report_files(tmp_path) -> None:
    storyboard_path, trace_path, events_path = _write_fixed_scene_fixture(tmp_path)
    report = analyze_fixed_scene_contract(storyboard_path=storyboard_path, trace_path=trace_path, events_path=events_path)

    outputs = write_fixed_scene_contract_report(report, tmp_path / "analysis")

    assert (tmp_path / "analysis" / "fixed_scene_contract_report.json").exists()
    assert "fixed_scene_contract_summary.md" in outputs["summary"]


def _write_fixed_scene_fixture(tmp_path, *, include_completed: bool = True):
    template = load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    trace_rows = [
        {"schema_version": "scenario_actor_trace.v1", "scene_id": "follow_stop_097", "phase": "lead_cruise", "actor_role": "lead_vehicle"},
        {"schema_version": "scenario_actor_trace.v1", "scene_id": "follow_stop_097", "phase": "lead_brake_to_stop", "actor_role": "lead_vehicle"},
        {"schema_version": "scenario_actor_trace.v1", "scene_id": "follow_stop_097", "phase": "lead_hold_stop", "actor_role": "lead_vehicle"},
    ]
    events = [
        {"schema_version": "scenario_phase_event.v1", "scene_id": "follow_stop_097", "phase": "lead_cruise", "event": "phase_started"},
        {"schema_version": "scenario_phase_event.v1", "scene_id": "follow_stop_097", "phase": "lead_brake_to_stop", "event": "phase_started"},
        {"schema_version": "scenario_phase_event.v1", "scene_id": "follow_stop_097", "phase": "lead_hold_stop", "event": "phase_started"},
    ]
    if include_completed:
        events.extend(
            [
                {"schema_version": "scenario_phase_event.v1", "scene_id": "follow_stop_097", "phase": "lead_cruise", "event": "phase_completed"},
                {"schema_version": "scenario_phase_event.v1", "scene_id": "follow_stop_097", "phase": "lead_brake_to_stop", "event": "phase_completed"},
                {"schema_version": "scenario_phase_event.v1", "scene_id": "follow_stop_097", "phase": "lead_hold_stop", "event": "phase_completed"},
            ]
        )
    trace_path.write_text("\n".join(json.dumps(row) for row in trace_rows) + "\n", encoding="utf-8")
    events_path.write_text("\n".join(json.dumps(row) for row in events) + "\n", encoding="utf-8")
    return storyboard_path, trace_path, events_path


def _write_duration_policy_fixture(tmp_path, *, route_s: float):
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    storyboard = compile_fixed_scene_template(template)
    storyboard_path = tmp_path / "fixed_scene_resolved.json"
    trace_path = tmp_path / "scenario_actor_trace.jsonl"
    events_path = tmp_path / "scenario_phase_events.jsonl"
    storyboard_path.write_text(json.dumps(storyboard), encoding="utf-8")
    trace_path.write_text(
        json.dumps(
            {
                "schema_version": "scenario_actor_trace.v1",
                "scene_id": storyboard["scene_id"],
                "phase": "lead_speed_profile",
                "actor_role": "lead_vehicle",
                "route_s": route_s,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    events_path.write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {
                    "schema_version": "scenario_phase_event.v1",
                    "scene_id": storyboard["scene_id"],
                    "phase": "lead_speed_profile",
                    "event": "phase_started",
                },
                {
                    "schema_version": "scenario_phase_event.v1",
                    "scene_id": storyboard["scene_id"],
                    "phase": "lead_speed_profile",
                    "event": "phase_completed",
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return storyboard_path, trace_path, events_path
