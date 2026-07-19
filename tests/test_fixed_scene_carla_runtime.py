from __future__ import annotations

import json
import math

import pytest

from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime
from carla_testbed.scenario_player import carla_runtime as carla_runtime_module
from carla_testbed.scenario_player import runtime_hook as runtime_hook_module
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.runtime_hook import (
    _scene_preroll_lead_target_speed,
    _scene_preroll_speed_command,
    setup_fixed_scene_runtime_hook,
)
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def test_carla_runtime_spawns_lead_actor_and_writes_artifacts(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    storyboard = compile_fixed_scene_template(load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml"))
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    assert state.errors == []
    assert state.spawned_count == 1
    assert "lead_vehicle" in runtime.active_roles()
    assert world.spawned[0].blueprint.attributes["role_name"] == "lead_vehicle"
    assert math.isclose(world.spawned[0].transform.location.x, 300.0)
    assert (tmp_path / "fixed_scene_resolved.json").exists()
    assert (tmp_path / "fixed_scene_runtime_state.json").exists()


def test_carla_runtime_prefers_heading_continuous_waypoint_branch(tmp_path) -> None:
    world = _FakeBranchingWaypointWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    storyboard = compile_fixed_scene_template(load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml"))
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    assert state.errors == []
    assert state.spawn_feasibility["lead_vehicle"]["status"] == "pass"
    assert math.isclose(world.spawned[0].transform.location.x, 300.0)
    assert math.isclose(world.spawned[0].transform.location.y, 0.0)
    assert math.isclose(world.spawned[0].transform.rotation.yaw, 2.0)


def test_carla_runtime_allows_curved_route_ahead_spawn_without_straight_frame_lateral_failure(tmp_path) -> None:
    world = _FakeCurvedAheadWaypointWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    storyboard = compile_fixed_scene_template(load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml"))
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    feasibility = state.spawn_feasibility["lead_vehicle"]
    assert state.errors == []
    assert feasibility["status"] == "warn"
    assert feasibility["route_ahead_selection"] is True
    assert feasibility["route_distance_m"] == 300.0
    assert feasibility["actual_lateral_offset_m"] > 50.0
    assert feasibility["lane_check"]["status"] == "pass_route_continuity"
    assert feasibility["blocking_reasons"] == []
    assert "spawn_feasibility_route_ahead_skipped_straight_frame_lateral_check" in feasibility["warnings"]


def test_carla_runtime_uses_ego_frame_fallback_when_waypoint_not_preferred(tmp_path) -> None:
    world = _FakeBadShortAheadWaypointWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml")
    storyboard = compile_fixed_scene_template(template)
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    feasibility = state.spawn_feasibility["lead_vehicle"]
    assert state.errors == []
    assert feasibility["spawn_source"] == "fallback_transform_ahead"
    assert feasibility["route_ahead_selection"] is False
    assert feasibility["route_distance_m"] is None
    assert math.isclose(feasibility["actual_longitudinal_offset_m"], 20.0)
    assert math.isclose(feasibility["actual_euclidean_offset_m"], 20.0)
    assert feasibility["blocking_reasons"] == []


def test_carla_runtime_settles_ego_pose_before_spawning_target_actor(tmp_path) -> None:
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    world = _FakeMaterializingWorld(
        ego=ego,
        materialized_transform=_Transform(_Location(100.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml")
    storyboard = compile_fixed_scene_template(template)
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    feasibility = state.spawn_feasibility["lead_vehicle"]
    assert state.errors == []
    assert world.tick_count == 2
    assert math.isclose(world.spawned[0].transform.location.x, 120.0)
    assert math.isclose(feasibility["actual_longitudinal_offset_m"], 20.0)


def test_carla_runtime_verifies_declared_bumper_gap(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    storyboard = compile_fixed_scene_template(
        load_fixed_scene_template("configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml")
    )
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    feasibility = state.spawn_feasibility["lead_vehicle"]
    assert state.errors == []
    assert feasibility["gap_reference"] == "bumper_to_bumper"
    assert feasibility["expected_bumper_gap_m"] == 20.0
    assert feasibility["actual_bumper_gap_m"] == pytest.approx(20.5)
    assert feasibility["blocking_reasons"] == []


def test_carla_runtime_falls_back_when_preferred_waypoint_has_gross_distance_mismatch(tmp_path) -> None:
    world = _FakeBadShortAheadWaypointWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["prefer_waypoint"] = True
    storyboard = compile_fixed_scene_template(template)
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    feasibility = state.spawn_feasibility["lead_vehicle"]
    assert state.errors == []
    assert feasibility["spawn_source"] == "fallback_after_bad_waypoint_next"
    assert feasibility["route_distance_m"] == 20.0
    assert math.isclose(feasibility["actual_longitudinal_offset_m"], 20.0)
    assert feasibility["blocking_reasons"] == []


def test_carla_runtime_blocks_bad_waypoint_fallback_when_waypoint_required(tmp_path) -> None:
    world = _FakeBadShortAheadWaypointWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = True
    storyboard = compile_fixed_scene_template(template)
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    feasibility = state.spawn_feasibility["lead_vehicle"]
    assert feasibility["spawn_source"] == "fallback_after_bad_waypoint_next"
    assert "waypoint_spawn_required_but_fallback_used" in feasibility["blocking_reasons"]
    assert any("waypoint_spawn_required_but_fallback_used" in error for error in state.errors)


def test_carla_runtime_applies_follow_stop_controls(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    storyboard = compile_fixed_scene_template(load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml"))
    runtime = CarlaFixedSceneRuntime()
    runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)
    lead = runtime.actors["lead_vehicle"]

    result = runtime.tick({"ego_actor": ego, "sim_time_sec": 0.0, "world_frame": 1})
    applied = result["applied_controls"]["lead_vehicle"]["clamped_command"]
    assert applied["throttle"] > 0.0
    assert applied["brake"] == 0.0

    ego.transform.location.x = 230.0
    lead.velocity = _Vector(8.0, 0.0, 0.0)
    result = runtime.tick({"ego_actor": ego, "sim_time_sec": 10.0, "world_frame": 2})
    applied = result["applied_controls"]["lead_vehicle"]["clamped_command"]
    assert applied["throttle"] == 0.0
    assert applied["brake"] == 1.0
    assert applied["hand_brake"] is False

    lead.velocity = _Vector(0.0, 0.0, 0.0)
    result = runtime.tick({"ego_actor": ego, "sim_time_sec": 15.0, "world_frame": 3})
    applied = result["applied_controls"]["lead_vehicle"]["clamped_command"]
    assert applied["brake"] == 1.0
    assert applied["hand_brake"] is True

    trace_rows = [json.loads(line) for line in (tmp_path / "scenario_actor_trace.jsonl").read_text().splitlines()]
    event_rows = [json.loads(line) for line in (tmp_path / "scenario_phase_events.jsonl").read_text().splitlines()]
    assert trace_rows[-1]["actor_role"] == "lead_vehicle"
    assert trace_rows[-1]["phase"] == "lead_hold_stop"
    assert trace_rows[-1]["actor_length_m"] == 4.4
    assert trace_rows[-1]["bbox_extent_x_m"] == 2.2
    assert [row["phase"] for row in event_rows if row["event"] == "phase_started"] == [
        "lead_cruise",
        "lead_brake_to_stop",
        "lead_hold_stop",
    ]


def test_carla_runtime_static_lead_stays_held(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template = load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    storyboard = compile_fixed_scene_template(template)
    runtime = CarlaFixedSceneRuntime()
    runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    result = runtime.tick({"ego_actor": ego, "sim_time_sec": 0.0, "world_frame": 1})

    applied = result["applied_controls"]["lead_vehicle"]["clamped_command"]
    assert result["current_phase"] == "lead_hold_stop"
    assert applied["throttle"] == 0.0
    assert applied["brake"] == 1.0
    assert applied["hand_brake"] is True


def test_carla_runtime_holds_actor_after_storyboard_stop(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    storyboard = compile_fixed_scene_template(template)
    runtime = CarlaFixedSceneRuntime()
    runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)
    lead = runtime.actors["lead_vehicle"]
    lead.velocity = _Vector(12.0, 0.0, 0.0)

    result = runtime.tick({"world": world, "ego_actor": ego, "sim_time_sec": 21.0, "world_frame": 1})

    applied = result["applied_controls"]["lead_vehicle"]["clamped_command"]
    assert result["stopped"] is True
    assert applied["throttle"] == 0.0
    assert applied["brake"] == 1.0
    assert applied["hand_brake"] is True
    assert lead.target_velocity is not None
    assert math.isclose(lead.target_velocity.x, 0.0, abs_tol=0.01)
    assert result["applied_controls"]["lead_vehicle"]["requested_command"]["metadata"]["controller"] == (
        "fixed_scene_runtime_stop_hold"
    )


def test_fixed_scene_runtime_hook_ticks_and_exposes_target_actor(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template_path = tmp_path / "lead_accel.yaml"
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    template_path.write_text(json.dumps(template), encoding="utf-8")

    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=tmp_path / "artifacts",
        scenario_path=template_path,
    )
    frame_context = type(
        "FrameContext",
        (),
        {"frame_id": 1, "sim_time_s": 0.0, "step": 0, "metadata": {}},
    )()

    hook.after_world_tick(frame_context)
    target = hook.target_actor()
    payload = hook.to_dict()

    assert target is hook.runtime.actors["lead_vehicle"]
    assert hook.tick_count == 1
    assert frame_context.metadata["fixed_scene_runtime"]["active_roles"] == ["lead_vehicle"]
    assert payload["status"] == "pass"
    assert payload["target_actor_role"] == "lead_vehicle"
    assert (tmp_path / "artifacts" / "fixed_scene_resolved.json").exists()
    assert (tmp_path / "artifacts" / "scenario_actor_trace.jsonl").exists()

    hook.close()


def test_fixed_scene_runtime_hook_uses_relative_scene_time(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template_path = tmp_path / "lead_accel.yaml"
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    template_path.write_text(json.dumps(template), encoding="utf-8")
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=tmp_path / "artifacts",
        scenario_path=template_path,
    )

    first = type("FrameContext", (), {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}})()
    second = type("FrameContext", (), {"frame_id": 11, "sim_time_s": 42.5, "step": 1, "metadata": {}})()
    hook.after_world_tick(first)
    hook.after_world_tick(second)

    assert first.metadata["fixed_scene_runtime"]["scene_sim_time_s"] == 0.0
    assert second.metadata["fixed_scene_runtime"]["scene_sim_time_s"] == 2.5
    assert hook.to_dict()["start_sim_time_s"] == 40.0
    rows = [
        json.loads(line)
        for line in (tmp_path / "artifacts" / "scenario_actor_trace.jsonl").read_text().splitlines()
    ]
    assert rows[0]["sim_time_sec"] == 0.0
    assert rows[1]["sim_time_sec"] == 2.5

    hook.close()


def test_fixed_scene_runtime_hook_requests_run_termination_after_storyboard_stop(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template_path = tmp_path / "lead_accel.yaml"
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    template["params"]["duration_s"] = 1.0
    template_path.write_text(json.dumps(template), encoding="utf-8")
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=tmp_path / "artifacts",
        scenario_path=template_path,
    )

    first = type("FrameContext", (), {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}})()
    final = type("FrameContext", (), {"frame_id": 11, "sim_time_s": 41.0, "step": 1, "metadata": {}})()
    hook.after_world_tick(first)
    hook.after_world_tick(final)

    assert "run_termination_request" not in first.metadata
    assert final.metadata["fixed_scene_runtime"]["stopped"] is True
    assert final.metadata["run_termination_request"] == {
        "source": "fixed_scene_runtime",
        "reason": "fixed_scene_storyboard_completed",
        "scene_id": "baguang_lead_accel_40_to_70_20m",
        "scene_sim_time_s": 1.0,
    }

    hook.close()


def test_fixed_scene_runtime_hook_supports_explicit_warmup_preroll(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template_path = tmp_path / "lead_accel.yaml"
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    template_path.write_text(json.dumps(template), encoding="utf-8")
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=tmp_path / "artifacts",
        scenario_path=template_path,
        start_gate="apollo_warmup_delay",
        start_delay_s=12.0,
        materialize_ego_initial_speed_on_arm=True,
    )
    waiting = type("FrameContext", (), {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}})()
    armed = type("FrameContext", (), {"frame_id": 11, "sim_time_s": 52.0, "step": 1, "metadata": {}})()

    hook.after_world_tick(waiting)
    hook.after_world_tick(armed)

    assert waiting.metadata["fixed_scene_runtime"]["armed"] is False
    assert hook.armed is True
    assert hook.start_sim_time_s == 52.0
    assert armed.metadata["fixed_scene_runtime"]["scene_sim_time_s"] == 0.0
    assert armed.metadata["fixed_scene_runtime"]["readiness"]["source"] == "configured_sim_time_preroll"

    hook.close()


def test_fixed_scene_runtime_hook_defers_target_spawn_until_ego_speed_ready(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template_path = tmp_path / "lead_accel.yaml"
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    template_path.write_text(json.dumps(template), encoding="utf-8")
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=tmp_path / "artifacts",
        scenario_path=template_path,
        start_gate="ego_speed_ready",
        defer_role_spawn_until_arm=True,
        ego_speed_ready_mps=18.0,
        ego_speed_ready_hold_ticks=2,
    )
    waiting = type("FrameContext", (), {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}})()

    hook.after_world_tick(waiting)

    assert hook.runtime.actors == {}
    assert hook.tick_count == 0
    assert waiting.metadata["fixed_scene_runtime"]["armed"] is False

    ego.velocity = _Vector(18.5, 0.0, 0.0)
    first_ready = type("FrameContext", (), {"frame_id": 11, "sim_time_s": 40.05, "step": 1, "metadata": {}})()
    armed = type("FrameContext", (), {"frame_id": 12, "sim_time_s": 40.1, "step": 2, "metadata": {}})()
    hook.after_world_tick(first_ready)
    hook.after_world_tick(armed)

    assert hook.armed is True
    assert hook.tick_count == 1
    assert hook.start_sim_time_s == 40.1
    assert set(hook.runtime.actors) == {"lead_vehicle"}
    assert hook.setup_state.spawned_count == 1
    assert armed.metadata["fixed_scene_runtime"]["scene_sim_time_s"] == 0.0
    assert armed.metadata["fixed_scene_runtime"]["readiness"]["source"] == "carla_ego_state"

    hook.close()


def test_fixed_scene_runtime_hook_resets_ego_before_deferred_target_spawn(tmp_path) -> None:
    initial = _Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0))
    ego = _FakeActor(actor_id=1, transform=initial)
    world = _FakeMaterializingWorld(
        ego=ego,
        materialized_transform=_Transform(_Location(100.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    template_path = tmp_path / "lead_accel.yaml"
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    template_path.write_text(json.dumps(template), encoding="utf-8")
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=tmp_path / "artifacts",
        scenario_path=template_path,
        start_gate="ego_speed_ready",
        defer_role_spawn_until_arm=True,
        reset_ego_pose_on_arm=True,
        post_reset_ego_speed_ready_mps=18.0,
        post_reset_ego_speed_ready_hold_ticks=2,
        ego_speed_ready_mps=18.0,
        ego_speed_ready_hold_ticks=1,
    )
    ego.transform = _Transform(_Location(200.0, 0.0, 0.0), _Rotation(yaw=0.0))
    ego.velocity = _Vector(18.5, 0.0, 0.0)
    armed = type("FrameContext", (), {"frame_id": 11, "sim_time_s": 40.0, "step": 1, "metadata": {}})()

    hook.after_world_tick(armed)

    assert hook.armed is False
    assert hook.post_reset_speed_gate_pending is True
    assert math.isclose(ego.transform.location.x, 100.0)
    assert world.spawned == []
    settle = type("FrameContext", (), {"frame_id": 12, "sim_time_s": 40.05, "step": 2, "metadata": {}})()
    hook.before_tick(settle)
    ego.velocity = _Vector(19.0, 0.0, 0.0)
    hook.after_world_tick(settle)

    assert hook.armed is False
    second_settle = type(
        "FrameContext", (), {"frame_id": 13, "sim_time_s": 40.1, "step": 3, "metadata": {}}
    )()
    hook.before_tick(second_settle)
    ego.velocity = _Vector(19.0, 0.0, 0.0)
    hook.after_world_tick(second_settle)

    assert hook.armed is True
    assert hook.role_speed_gate_pending is False
    assert math.isclose(world.spawned[0].transform.location.x, 120.0)
    assert hook.role_speed_ready_count == 1
    assert hook.initial_speed_materialization_count == 2
    report = json.loads(
        (tmp_path / "artifacts" / "ego_initial_pose_materialization.json").read_text(encoding="utf-8")
    )
    assert report["status"] == "pass"
    assert report["pose_before"]["x"] == 200.0
    assert report["target_pose"]["x"] == 100.0
    assert report["pose_after"]["x"] == 100.0

    hook.close()


def test_fixed_scene_runtime_hook_arms_prestarted_scene_before_first_harness_tick(tmp_path) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    world = _FakeWorld()
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=tmp_path / "artifacts",
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="prestarted_scene_pending",
        defer_role_spawn_until_arm=False,
    )

    readiness = hook.arm_prestarted_scene()

    assert hook.armed is True
    assert hook.start_gate == "prestarted_scene"
    assert hook.tick_count == 0
    assert readiness["source"] == "pre_harness_atomic_initial_state_assignment"
    assert readiness["ego_initial_speed_assignment"]["status"] == "pass"
    assert set(hook.runtime.actors) == {"lead_vehicle"}
    assert ego.target_velocity is not None
    assert hook.runtime.actors["lead_vehicle"].target_velocity is not None

    hook.close()


def test_fixed_scene_runtime_hook_waits_for_apollo_planning_readiness(tmp_path) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    world = _FakeWorld()
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        defer_role_spawn_until_arm=False,
        materialize_ego_initial_speed_on_arm=True,
    )

    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 0,
                "planning": {
                    "nonempty_trajectory_count": 0,
                    "last_trajectory_point_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    waiting = type("FrameContext", (), {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}})()
    hook.after_world_tick(waiting)

    assert hook.armed is False
    assert hook.tick_count == 0
    assert set(hook.runtime.actors) == {"lead_vehicle"}
    assert waiting.metadata["fixed_scene_runtime"]["readiness"]["status"] == "waiting"

    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 1,
                "planning": {
                    "nonempty_trajectory_count": 1,
                    "last_trajectory_point_count": 1,
                    "last_trajectory_type": "SPEED_FALLBACK",
                },
            }
        ),
        encoding="utf-8",
    )
    fallback = type(
        "FrameContext",
        (),
        {"frame_id": 11, "sim_time_s": 40.05, "step": 1, "metadata": {}},
    )()
    hook.after_world_tick(fallback)

    assert hook.armed is False
    assert fallback.metadata["fixed_scene_runtime"]["readiness"]["fallback_trajectory"] is True

    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 1,
                "planning": {
                    "nonempty_trajectory_count": 1,
                    "last_trajectory_point_count": 12,
                    "last_trajectory_type": "NORMAL",
                },
            }
        ),
        encoding="utf-8",
    )
    ready = type("FrameContext", (), {"frame_id": 12, "sim_time_s": 40.1, "step": 2, "metadata": {}})()
    hook.after_world_tick(ready)

    assert hook.armed is True
    assert hook.tick_count == 1
    assert hook.start_sim_time_s == 40.1
    assert ready.metadata["fixed_scene_runtime"]["readiness"]["status"] == "ready"
    assert ego.target_velocity is not None
    assert hook.runtime.actors["lead_vehicle"].target_velocity is not None

    hook.close()


def test_fixed_scene_runtime_hook_rejects_stale_normal_planning_snapshot(
    tmp_path, monkeypatch
) -> None:
    now = [100.0]
    monkeypatch.setattr(runtime_hook_module.time, "time", lambda: now[0])
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        planning_ready_max_message_age_s=0.25,
    )
    stats = {
        "routing_success_count": 1,
        "planning": {
            "nonempty_trajectory_count": 1,
            "last_trajectory_point_count": 111,
            "last_trajectory_type": "NORMAL",
            "last_planning_header_sequence_num": 7,
            "last_msg_wall_time_sec": 99.0,
        },
    }
    artifacts.mkdir(parents=True, exist_ok=True)
    stats_path = artifacts / "cyber_bridge_stats.json"
    stats_path.write_text(json.dumps(stats), encoding="utf-8")

    stale = hook._apollo_planning_readiness()

    assert stale["ready"] is False
    assert stale["trajectory_ready"] is False
    assert stale["planning_message_fresh"] is False
    assert stale["planning_message_age_s"] == pytest.approx(1.0)
    assert stale["last_planning_header_sequence_num"] == 7

    stats["planning"].update(
        {
            "last_planning_header_sequence_num": 8,
            "last_msg_wall_time_sec": 99.9,
        }
    )
    stats_path.write_text(json.dumps(stats), encoding="utf-8")

    fresh = hook._apollo_planning_readiness()

    assert fresh["ready"] is True
    assert fresh["trajectory_ready"] is True
    assert fresh["planning_message_fresh"] is True
    assert fresh["planning_message_age_s"] == pytest.approx(0.1)
    assert fresh["last_planning_header_sequence_num"] == 8

    trace_rows = [
        json.loads(line)
        for line in (
            artifacts / "fixed_scene_planning_readiness_trace.jsonl"
        ).read_text(encoding="utf-8").splitlines()
    ]
    assert [row["ready"] for row in trace_rows] == [False, True]
    assert [row["planning_message_fresh"] for row in trace_rows] == [False, True]
    assert trace_rows[-1]["poll_count"] == 2
    assert trace_rows[-1]["claim_boundary"].startswith(
        "This trace records the strict Apollo Planning start-gate decision."
    )

    hook.close()


def test_fixed_scene_runtime_hook_uses_sim_time_for_planning_freshness(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(runtime_hook_module.time, "time", lambda: 100.0)
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        planning_ready_max_message_age_s=0.25,
    )
    stats = {
        "routing_success_count": 1,
        "planning": {
            "nonempty_trajectory_count": 1,
            "last_trajectory_point_count": 111,
            "last_trajectory_type": "NORMAL",
            "last_planning_header_sequence_num": 7,
            "last_msg_ts_sec": 24.95,
            "last_msg_wall_time_sec": 99.0,
        },
    }
    artifacts.mkdir(parents=True, exist_ok=True)
    stats_path = artifacts / "cyber_bridge_stats.json"
    stats_path.write_text(json.dumps(stats), encoding="utf-8")

    fresh = hook._apollo_planning_readiness(current_sim_time_s=25.0)

    assert fresh["ready"] is True
    assert fresh["planning_message_fresh"] is True
    assert fresh["planning_message_freshness_clock"] == "sim_time"
    assert fresh["planning_message_age_s"] == pytest.approx(0.05)
    assert fresh["planning_message_sim_age_s"] == pytest.approx(0.05)
    assert fresh["planning_message_wall_age_s"] == pytest.approx(1.0)

    stale = hook._apollo_planning_readiness(current_sim_time_s=25.3)

    assert stale["ready"] is False
    assert stale["planning_message_fresh"] is False
    assert stale["planning_message_age_s"] == pytest.approx(0.35)

    hook.close()


def test_fixed_scene_runtime_hook_uses_newer_per_message_planning_snapshot(
    tmp_path, monkeypatch
) -> None:
    now = [100.0]
    monkeypatch.setattr(runtime_hook_module.time, "time", lambda: now[0])
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=_FakeActor(
            actor_id=1,
            transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
        ),
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        planning_ready_max_message_age_s=0.25,
    )
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 1,
                "planning": {
                    "nonempty_trajectory_count": 72,
                    "last_trajectory_point_count": 111,
                    "last_trajectory_type": "NORMAL",
                    "last_planning_header_sequence_num": 152,
                    "last_msg_wall_time_sec": 99.80,
                },
            }
        ),
        encoding="utf-8",
    )
    summary_path = artifacts / "planning_topic_debug_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "messages_with_nonzero_trajectory_points": 74,
                "last_trajectory_point_count": 82,
                "last_trajectory_type": "SPEED_FALLBACK",
                "last_planning_header_sequence_num": 154,
                "last_msg_wall_time_sec": 99.95,
            }
        ),
        encoding="utf-8",
    )

    fallback = hook._apollo_planning_readiness()

    assert fallback["ready"] is False
    assert fallback["fallback_trajectory"] is True
    assert fallback["last_planning_header_sequence_num"] == 154
    assert fallback["stats_last_planning_header_sequence_num"] == 152
    assert fallback["planning_snapshot_source"] == "planning_topic_debug_summary"
    assert fallback["planning_snapshot_ahead_of_stats_s"] == pytest.approx(0.15)
    assert fallback["planning_message_age_s"] == pytest.approx(0.05)

    summary_path.write_text(
        json.dumps(
            {
                "messages_with_nonzero_trajectory_points": 75,
                "last_trajectory_point_count": 129,
                "last_trajectory_type": "NORMAL",
                "last_planning_header_sequence_num": 155,
                "last_msg_wall_time_sec": 99.98,
            }
        ),
        encoding="utf-8",
    )

    normal = hook._apollo_planning_readiness()

    assert normal["ready"] is True
    assert normal["fallback_trajectory"] is False
    assert normal["last_planning_header_sequence_num"] == 155
    assert normal["last_trajectory_point_count"] == 129
    assert normal["planning_message_age_s"] == pytest.approx(0.02)

    hook.close()


def test_fixed_scene_preroll_cannot_handover_on_repeated_stale_normal_snapshot(
    tmp_path, monkeypatch
) -> None:
    now = [100.0]
    monkeypatch.setattr(runtime_hook_module.time, "time", lambda: now[0])
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        planning_ready_max_message_age_s=0.25,
        scene_preroll_enabled=True,
        scene_preroll_ready_hold_ticks=10,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 1,
                "planning": {
                    "nonempty_trajectory_count": 1,
                    "last_trajectory_point_count": 111,
                    "last_trajectory_type": "NORMAL",
                    "last_planning_header_sequence_num": 7,
                    "last_msg_wall_time_sec": 100.0,
                },
            }
        ),
        encoding="utf-8",
    )
    initial = type(
        "FrameContext",
        (),
        {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}},
    )()
    hook.after_world_tick(initial)
    assert hook.scene_preroll_active is True

    for tick in range(1, 11):
        now[0] = 100.0 + tick * 0.05
        frame = type(
            "FrameContext",
            (),
            {
                "frame_id": 10 + tick,
                "sim_time_s": 40.0 + tick * 0.05,
                "step": tick,
                "metadata": {},
            },
        )()
        hook.after_world_tick(frame)

    assert hook.scene_preroll_active is True
    assert hook.scene_preroll_handover_pending is False
    assert hook.scene_preroll_ready_count == 10
    assert hook.scene_preroll_planning_compatible_count == 1
    assert hook.readiness["planning_ready"] is False
    assert hook.readiness["status"] == "waiting_for_fresh_planning_message"
    assert (
        hook.readiness["planning_compatible_message_update"]
        == "preserved_same_sequence_freshness_gap"
    )
    assert hook.readiness["planning"]["planning_message_fresh"] is False
    assert not (artifacts / "fixed_scene_ego_handover.json").exists()

    hook.close()


def test_fixed_scene_runtime_hook_physically_prerolls_before_apollo_handover(tmp_path) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    world = _FakeWorld()
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        materialize_ego_initial_speed_on_arm=True,
        scene_preroll_enabled=True,
        scene_preroll_ready_hold_ticks=2,
    )
    lead = hook.runtime.actors["lead_vehicle"]

    planning_stats = {
        "routing_success_count": 1,
        "control_tx_count": 0,
        "planning": {
            "nonempty_trajectory_count": 1,
            "last_trajectory_point_count": 12,
            "last_trajectory_type": "NORMAL",
        },
    }
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(planning_stats), encoding="utf-8"
    )
    gate_ready = type(
        "FrameContext", (), {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}}
    )()
    hook.after_world_tick(gate_ready)

    assert hook.scene_preroll_active is True
    assert hook.armed is False
    assert ego.target_velocity is None
    assert lead.target_velocity is None

    first = type(
        "FrameContext", (), {"frame_id": 11, "sim_time_s": 40.05, "step": 1, "metadata": {}}
    )()
    hook.before_tick(first)
    assert ego.last_control.throttle > 0.0
    assert lead.last_control.throttle > 0.0
    assert ego.last_control.steer == 0.0
    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)
    hook.after_world_tick(first)
    activation = json.loads(
        (artifacts / "fixed_scene_obstacle_activation.json").read_text()
    )
    assert activation["status"] == "pass"
    assert activation["speed_ready"] is True
    assert activation["gap_ready"] is True
    assert activation["official_scenario_timer_started"] is False
    assert activation["ego_speed_mps"] == pytest.approx(19.44)
    assert activation["lead_speed_mps"] == pytest.approx(19.44)

    second = type(
        "FrameContext", (), {"frame_id": 12, "sim_time_s": 40.1, "step": 2, "metadata": {}}
    )()
    hook.before_tick(second)
    hook.after_world_tick(second)

    assert hook.scene_preroll_active is False
    assert hook.scene_preroll_handover_pending is True
    assert hook.armed is False
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert marker["status"] == "ready"
    assert marker["official_scenario_timer_started"] is False
    assert marker["planning"]["last_trajectory_type"] == "NORMAL"

    (artifacts / "fixed_scene_control_handover_ack.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_control_handover_ack.v1",
                "status": "published",
                "control_tx_count": 1,
                "control_timestamp_sec": 40.12,
                "handover_world_frame": marker["world_frame"],
                "handover_world_sim_time_s": marker["world_sim_time_s"],
            }
        ),
        encoding="utf-8",
    )
    handed_over = type(
        "FrameContext", (), {"frame_id": 13, "sim_time_s": 40.15, "step": 3, "metadata": {}}
    )()
    hook.after_world_tick(handed_over)

    assert hook.armed is True
    assert hook.start_sim_time_s == pytest.approx(40.15)
    assert handed_over.metadata["fixed_scene_runtime"]["scene_sim_time_s"] == 0.0
    assert ego.target_velocity is None
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert marker["official_scenario_timer_started"] is True
    assert marker["control_tx_count_after_handover"] == 1
    assert (
        marker["apollo_control_start_observation_source"]
        == "fixed_scene_control_handover_ack"
    )
    assert marker["apollo_control_start_world_sim_time_s"] == pytest.approx(40.12)
    assert marker["apollo_control_start_observed_world_sim_time_s"] == pytest.approx(
        40.15
    )
    assert marker["apollo_control_start_observation_delay_s"] == pytest.approx(0.03)
    preroll_rows = (artifacts / "fixed_scene_setup_preroll_trace.jsonl").read_text().splitlines()
    scenario_rows = (artifacts / "scenario_actor_trace.jsonl").read_text().splitlines()
    assert len(preroll_rows) == 2
    assert json.loads(scenario_rows[0])["sim_time_sec"] == 0.0

    hook.close()


def test_planning_gate_can_materialize_authored_initial_speeds_before_short_handover(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_materialize_initial_speeds_on_gate=True,
        scene_preroll_ego_handover_mode="target_ready",
        scene_preroll_planning_speed_gate_enabled=True,
        scene_preroll_planning_compatible_min_messages=2,
        scene_preroll_planning_require_replan_anchor=True,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    assert ego.target_velocity is None
    assert lead.target_velocity is None

    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 1,
                "control_tx_count": 0,
                "planning": {
                    "nonempty_trajectory_count": 1,
                    "last_trajectory_point_count": 111,
                    "last_trajectory_type": "NORMAL",
                    "last_planning_header_sequence_num": 7,
                    "last_is_replan": True,
                    "last_trajectory_first_nonexpired_point_v": 0.0,
                    "last_trajectory_speed_at_1s_mps": 0.7,
                },
            }
        ),
        encoding="utf-8",
    )
    gate = type(
        "FrameContext",
        (),
        {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}},
    )()
    hook.after_world_tick(gate)

    assert hook.scene_preroll_active is True
    assert hook.scene_preroll_handover_pending is False
    assert hook.armed is False
    assert ego.target_velocity.x == pytest.approx(19.44)
    assert lead.target_velocity.x == pytest.approx(19.44)
    assert not (artifacts / "fixed_scene_ego_handover.json").exists()
    report = json.loads(
        (artifacts / "fixed_scene_gate_initial_state_materialization.json").read_text()
    )
    assert report["status"] == "pass"
    assert report["start_gate"] == "apollo_planning_ready"
    assert report["pre_materialization_ego_speed_mps"] == 0.0
    assert report["pre_materialization_lead_speed_mps"] == 0.0
    assert report["planning"]["routing_success_count"] == 1
    assert report["planning"]["last_trajectory_point_count"] == 111
    assert report["planning"]["last_trajectory_type"] == "NORMAL"
    assert report["official_scenario_timer_started"] is False
    assert gate.metadata["fixed_scene_runtime"]["scene_sim_time_s"] is None

    hook.close()


def test_planning_gate_keeps_initial_replan_when_post_stability_handover_does_not(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_materialize_initial_speeds_on_gate=True,
        scene_preroll_ego_handover_mode="target_ready",
        scene_preroll_ready_hold_ticks=2,
        scene_preroll_planning_speed_gate_enabled=True,
        scene_preroll_planning_compatible_min_messages=2,
        planning_ready_require_initial_replan_anchor=True,
        scene_preroll_planning_require_replan_anchor=False,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    artifacts.mkdir(parents=True, exist_ok=True)
    planning = {
        "nonempty_trajectory_count": 1,
        "last_trajectory_point_count": 176,
        "last_trajectory_type": "NORMAL",
        "last_planning_header_sequence_num": 42,
        "last_is_replan": False,
        "last_trajectory_first_nonexpired_point_v": 0.75,
        "last_trajectory_speed_at_1s_mps": 1.4,
    }
    stats = {
        "routing_success_count": 1,
        "control_tx_count": 0,
        "planning": planning,
    }

    def observe(frame_id: int, sim_time_s: float) -> None:
        planning["last_msg_ts_sec"] = sim_time_s
        (artifacts / "cyber_bridge_stats.json").write_text(
            json.dumps(stats), encoding="utf-8"
        )
        frame = type(
            "FrameContext",
            (),
            {
                "frame_id": frame_id,
                "sim_time_s": sim_time_s,
                "step": frame_id,
                "metadata": {},
            },
        )()
        hook.after_world_tick(frame)

    observe(10, 40.0)
    assert hook.scene_preroll_active is False
    assert hook.readiness["ready"] is False
    assert hook.readiness["initial_replan_anchor_required"] is True
    assert hook.readiness["initial_replan_anchor_ready"] is False
    assert ego.target_velocity is None
    assert lead.target_velocity is None
    assert not (
        artifacts / "fixed_scene_gate_initial_state_materialization.json"
    ).exists()

    planning.update(
        {
            "last_planning_header_sequence_num": 43,
            "last_is_replan": True,
            "last_replan_reason": "replan for current state",
            "last_trajectory_point_count": 111,
            "last_trajectory_first_nonexpired_point_v": 0.0,
            "last_trajectory_speed_at_1s_mps": 0.7,
        }
    )
    observe(11, 40.05)
    assert hook.scene_preroll_active is True
    assert hook.readiness["initial_state_materialization"]["status"] == "pass"
    assert ego.target_velocity.x == pytest.approx(19.44)
    assert lead.target_velocity.x == pytest.approx(19.44)

    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)
    planning.update(
        {
            "last_planning_header_sequence_num": 44,
            "last_is_replan": False,
            "last_trajectory_first_nonexpired_point_v": 19.44,
            "last_trajectory_speed_at_1s_mps": 19.6,
        }
    )
    observe(12, 40.10)
    assert hook.readiness["planning"]["initial_replan_anchor_required"] is False
    assert hook.scene_preroll_planning_compatible_count == 1

    planning.update(
        {
            "last_planning_header_sequence_num": 45,
            "last_is_replan": False,
            "last_replan_reason": "",
        }
    )
    observe(13, 40.15)
    assert hook.scene_preroll_planning_compatible_count == 2
    assert hook.scene_preroll_handover_pending is True
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert marker["planning_replan_anchor_required"] is False
    assert marker["planning_replan_anchor_sequence_num"] is None

    hook.close()


def test_short_handover_waits_for_forward_gear_and_bounded_acceleration(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_materialize_initial_speeds_on_gate=True,
        scene_preroll_ego_handover_mode="target_ready",
        scene_preroll_ready_hold_ticks=2,
        scene_preroll_drivetrain_gate_enabled=True,
        scene_preroll_drivetrain_max_abs_acceleration_mps2=2.0,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    artifacts.mkdir(parents=True, exist_ok=True)
    planning = {
        "routing_success_count": 1,
        "control_tx_count": 0,
        "planning": {
            "nonempty_trajectory_count": 1,
            "last_trajectory_point_count": 111,
            "last_trajectory_type": "NORMAL",
            "last_planning_header_sequence_num": 7,
            "last_trajectory_first_nonexpired_point_v": 19.44,
            "last_trajectory_speed_at_1s_mps": 19.44,
        },
    }
    (artifacts / "cyber_bridge_stats.json").write_text(json.dumps(planning))
    gate = type(
        "FrameContext",
        (),
        {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}},
    )()
    hook.after_world_tick(gate)
    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)

    for actor in (ego, lead):
        actor.last_control = _FakeControl(gear=0)
        actor.acceleration = _Vector(5.0, 0.0, 0.0)
    unstable = type(
        "FrameContext",
        (),
        {"frame_id": 11, "sim_time_s": 40.05, "step": 1, "metadata": {}},
    )()
    hook.after_world_tick(unstable)
    assert hook.readiness["status"] == "waiting_for_drivetrain_stability"
    assert hook.readiness["drivetrain_readiness"]["roles"]["ego"]["gear"] == 0
    assert not (artifacts / "fixed_scene_ego_handover.json").exists()

    for actor in (ego, lead):
        actor.last_control = _FakeControl(gear=1)
        actor.acceleration = _Vector(1.5, 0.0, 0.0)
    stable_first = type(
        "FrameContext",
        (),
        {"frame_id": 12, "sim_time_s": 40.1, "step": 2, "metadata": {}},
    )()
    hook.after_world_tick(stable_first)
    assert hook.readiness["ready_hold_ticks"] == 1
    assert not (artifacts / "fixed_scene_ego_handover.json").exists()

    stable_second = type(
        "FrameContext",
        (),
        {"frame_id": 13, "sim_time_s": 40.15, "step": 3, "metadata": {}},
    )()
    hook.after_world_tick(stable_second)
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert marker["drivetrain_readiness"]["ready"] is True
    assert marker["drivetrain_readiness"]["roles"]["ego"]["gear"] == 1
    assert marker["official_scenario_timer_started"] is False
    hook.close()


def test_scene_preroll_uses_authored_role_speeds_for_cut_in_readiness(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_materialize_initial_speeds_on_gate=True,
        scene_preroll_ego_handover_mode="target_ready",
        scene_preroll_speed_tolerance_mps=0.5,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 1,
                "control_tx_count": 0,
                "planning": {
                    "nonempty_trajectory_count": 1,
                    "last_trajectory_point_count": 111,
                    "last_trajectory_type": "NORMAL",
                    "last_planning_header_sequence_num": 7,
                    "last_is_replan": True,
                    "last_trajectory_first_nonexpired_point_v": 11.11,
                    "last_trajectory_speed_at_1s_mps": 11.11,
                },
            }
        ),
        encoding="utf-8",
    )
    gate = type(
        "FrameContext",
        (),
        {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}},
    )()
    hook.after_world_tick(gate)

    assert hook.target_actor() is lead
    assert ego.target_velocity.x == pytest.approx(11.11)
    assert lead.target_velocity.x == pytest.approx(9.72)

    ego.velocity = _Vector(11.11, 0.0, 0.0)
    lead.velocity = _Vector(9.72, 0.0, 0.0)
    observation = type(
        "FrameContext",
        (),
        {"frame_id": 11, "sim_time_s": 40.05, "step": 1, "metadata": {}},
    )()
    hook.after_world_tick(observation)

    assert hook.readiness["speed_ready"] is True
    assert hook.readiness["ego_target_speed_mps"] == pytest.approx(11.11)
    assert hook.readiness["target_actor_target_speed_mps"] == pytest.approx(9.72)
    assert hook.readiness["gap_ready"] is True
    assert hook.readiness["gap_reference"] == "center_to_center"
    assert hook.readiness["setup_gap_m"] == pytest.approx(10.0)
    assert hook.readiness["expected_setup_gap_m"] == pytest.approx(10.0)
    assert hook.readiness["bumper_gap_m"] == pytest.approx(5.6)

    control = type(
        "FrameContext",
        (),
        {"frame_id": 12, "sim_time_s": 40.1, "step": 2, "metadata": {}},
    )()
    hook.before_tick(control)
    assert ego.last_control.throttle == pytest.approx(0.026 * 11.11)
    assert lead.last_control.throttle == pytest.approx(0.026 * 9.72)

    hook.scene_preroll_ready_hold_ticks = 2
    hook.after_world_tick(control)
    handover = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert handover["gap_reference"] == "center_to_center"
    assert handover["setup_gap_m"] == pytest.approx(10.0)
    assert handover["bumper_gap_m"] == pytest.approx(5.6)

    hook.close()


def test_gate_initial_speed_materialization_rejects_immediate_planning_handover(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_materialize_initial_speeds_on_gate=True,
        scene_preroll_ego_handover_mode="planning_ready",
    )

    assert (
        "scene_preroll_gate_initial_speed_materialization_requires_target_ready_handover"
        in hook.setup_state.errors
    )

    hook.close()


def test_zero_speed_static_scene_allows_planning_ready_handover_setup(tmp_path) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        scenario_path="configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_ego_handover_mode="planning_ready",
    )

    assert hook.scene_preroll_target_speed_mps == 0.0
    assert hook.scene_preroll_target_actor_speed_mps == 0.0
    assert "scene_preroll_target_speed_missing" not in hook.setup_state.errors

    hook.close()


def test_scene_preroll_requires_compatible_replan_and_distinct_confirmation(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    world = _FakeWorld()
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=world,
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_ready_hold_ticks=2,
        scene_preroll_planning_speed_gate_enabled=True,
        scene_preroll_planning_current_speed_tolerance_mps=1.0,
        scene_preroll_planning_lookahead_speed_tolerance_mps=2.0,
        scene_preroll_planning_compatible_min_messages=2,
        scene_preroll_planning_require_replan_anchor=True,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)
    planning = {
        "nonempty_trajectory_count": 1,
        "last_trajectory_point_count": 12,
        "last_trajectory_type": "NORMAL",
        "last_planning_header_sequence_num": 10,
        "last_is_replan": False,
        "last_trajectory_first_nonexpired_point_v": 15.0,
        "last_trajectory_speed_at_1s_mps": 14.0,
    }
    stats = {
        "routing_success_count": 1,
        "control_tx_count": 0,
        "planning": planning,
    }
    artifacts.mkdir(parents=True, exist_ok=True)

    def observe(frame_id: int, sim_time_s: float) -> None:
        (artifacts / "cyber_bridge_stats.json").write_text(
            json.dumps(stats), encoding="utf-8"
        )
        frame = type(
            "FrameContext",
            (),
            {"frame_id": frame_id, "sim_time_s": sim_time_s, "step": frame_id, "metadata": {}},
        )()
        hook.after_world_tick(frame)

    observe(10, 40.0)
    assert hook.scene_preroll_active is True

    observe(11, 40.05)
    assert hook.scene_preroll_planning_compatible_count == 0
    assert hook.readiness["planning_speed_compatibility"]["ready"] is False

    planning.update(
        {
            "last_planning_header_sequence_num": 11,
            "last_trajectory_first_nonexpired_point_v": 19.2,
            "last_trajectory_speed_at_1s_mps": 19.0,
        }
    )
    observe(12, 40.10)
    assert hook.scene_preroll_planning_compatible_count == 0

    planning.update(
        {
            "last_planning_header_sequence_num": 12,
            "last_is_replan": True,
        }
    )
    observe(13, 40.15)
    assert hook.scene_preroll_planning_compatible_count == 1
    assert hook.scene_preroll_planning_replan_anchor_sequence_num == 12
    observe(14, 40.20)
    assert hook.scene_preroll_planning_compatible_count == 1
    assert hook.scene_preroll_active is True
    assert not (artifacts / "fixed_scene_ego_handover.json").exists()

    planning.update(
        {
            "last_planning_header_sequence_num": 13,
            "last_is_replan": False,
            "last_trajectory_type": "SPEED_FALLBACK",
        }
    )
    observe(15, 40.25)
    assert hook.scene_preroll_planning_compatible_count == 0
    assert hook.scene_preroll_planning_replan_anchor_sequence_num is None

    planning.update(
        {
            "last_planning_header_sequence_num": 14,
            "last_is_replan": True,
            "last_trajectory_type": "NORMAL",
        }
    )
    observe(16, 40.30)
    assert hook.scene_preroll_planning_compatible_count == 1
    planning.update(
        {
            "last_planning_header_sequence_num": 15,
            "last_is_replan": False,
        }
    )
    observe(17, 40.35)
    assert hook.scene_preroll_planning_compatible_count == 2
    assert hook.scene_preroll_active is False
    assert hook.scene_preroll_handover_pending is True
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert marker["planning_compatible_message_count"] == 2
    assert marker["planning_replan_anchor_required"] is True
    assert marker["planning_replan_anchor_sequence_num"] == 14
    assert marker["planning_speed_compatibility"]["ready"] is True

    hook.close()


def test_scene_preroll_can_handover_on_current_speed_without_future_speed_bias(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_ready_hold_ticks=2,
        scene_preroll_planning_speed_gate_enabled=True,
        scene_preroll_planning_current_speed_tolerance_mps=1.0,
        scene_preroll_planning_lookahead_speed_gate_enabled=False,
        scene_preroll_planning_lookahead_speed_tolerance_mps=2.0,
        scene_preroll_planning_compatible_min_messages=2,
        scene_preroll_planning_require_replan_anchor=False,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)
    planning = {
        "nonempty_trajectory_count": 1,
        "last_trajectory_point_count": 12,
        "last_trajectory_type": "NORMAL",
        "last_planning_header_sequence_num": 10,
        "last_is_replan": False,
        "last_trajectory_first_nonexpired_point_v": 0.0,
        "last_trajectory_speed_at_1s_mps": 1.0,
    }
    stats = {
        "routing_success_count": 1,
        "control_tx_count": 0,
        "planning": planning,
    }
    artifacts.mkdir(parents=True, exist_ok=True)

    def observe(frame_id: int, sim_time_s: float) -> None:
        (artifacts / "cyber_bridge_stats.json").write_text(
            json.dumps(stats), encoding="utf-8"
        )
        frame = type(
            "FrameContext",
            (),
            {"frame_id": frame_id, "sim_time_s": sim_time_s, "step": frame_id, "metadata": {}},
        )()
        hook.after_world_tick(frame)

    observe(10, 40.0)
    assert hook.scene_preroll_active is True

    planning.update(
        {
            "last_planning_header_sequence_num": 11,
            "last_trajectory_type": "SPEED_FALLBACK",
            "last_trajectory_first_nonexpired_point_v": 19.2,
            "last_trajectory_speed_at_1s_mps": 15.0,
        }
    )
    observe(11, 40.05)
    assert hook.scene_preroll_planning_compatible_count == 0
    assert not (artifacts / "fixed_scene_ego_handover.json").exists()

    planning.update(
        {
            "last_planning_header_sequence_num": 12,
            "last_trajectory_type": "NORMAL",
        }
    )
    observe(12, 40.10)
    assert hook.scene_preroll_planning_compatible_count == 1
    assert hook.readiness["planning_speed_compatibility"]["ready"] is True
    assert (
        hook.readiness["planning_speed_compatibility"]["lookahead_speed_required"]
        is False
    )

    observe(13, 40.15)
    assert hook.scene_preroll_planning_compatible_count == 1
    assert not (artifacts / "fixed_scene_ego_handover.json").exists()

    planning["last_planning_header_sequence_num"] = 13
    observe(14, 40.20)
    assert hook.scene_preroll_planning_compatible_count == 2
    assert hook.scene_preroll_active is False
    assert hook.scene_preroll_handover_pending is True
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    compatibility = marker["planning_speed_compatibility"]
    assert marker["planning"]["last_trajectory_type"] == "NORMAL"
    assert marker["planning_compatible_message_count"] == 2
    assert marker["planning_replan_anchor_required"] is False
    assert compatibility["ready"] is True
    assert compatibility["trajectory_current_speed_delta_mps"] < 1.0
    assert compatibility["trajectory_speed_at_1s_delta_mps"] > 2.0
    assert compatibility["lookahead_speed_gate_enabled"] is False
    assert compatibility["lookahead_speed_required"] is False

    hook.close()


def test_scene_preroll_preserves_distinct_compatible_messages_across_same_sequence_freshness_gap(
    tmp_path,
    monkeypatch,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        planning_ready_max_message_age_s=0.25,
        scene_preroll_enabled=True,
        scene_preroll_ready_hold_ticks=2,
        scene_preroll_planning_speed_gate_enabled=True,
        scene_preroll_planning_current_speed_tolerance_mps=1.0,
        scene_preroll_planning_lookahead_speed_gate_enabled=False,
        scene_preroll_planning_compatible_min_messages=2,
        scene_preroll_planning_require_replan_anchor=False,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)
    now_s = 1000.0
    planning = {
        "nonempty_trajectory_count": 1,
        "last_trajectory_point_count": 12,
        "last_trajectory_type": "NORMAL",
        "last_planning_header_sequence_num": 10,
        "last_is_replan": False,
        "last_trajectory_first_nonexpired_point_v": 19.44,
        "last_trajectory_speed_at_1s_mps": 19.0,
        "last_msg_wall_time_sec": now_s,
    }
    stats = {
        "routing_success_count": 1,
        "control_tx_count": 0,
        "planning": planning,
    }
    artifacts.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(runtime_hook_module.time, "time", lambda: now_s)

    def observe(frame_id: int, sim_time_s: float) -> None:
        (artifacts / "cyber_bridge_stats.json").write_text(
            json.dumps(stats), encoding="utf-8"
        )
        frame = type(
            "FrameContext",
            (),
            {
                "frame_id": frame_id,
                "sim_time_s": sim_time_s,
                "step": frame_id,
                "metadata": {},
            },
        )()
        hook.after_world_tick(frame)

    observe(10, 40.0)
    planning["last_planning_header_sequence_num"] = 11
    observe(11, 40.05)
    assert hook.scene_preroll_planning_compatible_count == 1

    now_s += 0.30
    observe(12, 40.10)
    assert hook.readiness["planning_ready"] is False
    assert hook.scene_preroll_planning_compatible_count == 1
    assert (
        hook.readiness["planning_compatible_message_update"]
        == "preserved_same_sequence_freshness_gap"
    )

    planning.update(
        {
            "last_planning_header_sequence_num": 12,
            "last_trajectory_type": "SPEED_FALLBACK",
            "last_msg_wall_time_sec": now_s,
        }
    )
    observe(13, 40.15)
    assert hook.scene_preroll_planning_compatible_count == 0
    assert hook.readiness["planning_compatible_message_update"] == "reset_ineligible_message"

    for frame_id, sequence_num in ((14, 13), (15, 14)):
        now_s += 0.05
        planning.update(
            {
                "last_planning_header_sequence_num": sequence_num,
                "last_trajectory_type": "NORMAL",
                "last_msg_wall_time_sec": now_s,
            }
        )
        observe(frame_id, 40.15 + (frame_id - 13) * 0.05)

    assert hook.scene_preroll_planning_compatible_count == 2
    assert hook.scene_preroll_active is False
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert marker["planning_compatible_message_count"] == 2
    assert marker["planning"]["last_planning_header_sequence_num"] == 14
    assert marker["planning"]["planning_message_fresh"] is True
    assert marker["planning_speed_compatibility"]["ready"] is True

    hook.close()


def test_planning_ready_handover_gives_ego_to_apollo_before_formal_scene(
    tmp_path,
) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_ego_handover_mode="planning_ready",
        scene_preroll_lead_speed_headroom_mps=1.5,
        scene_preroll_lead_acceleration_mps2=1.5,
        scene_preroll_ready_hold_ticks=2,
        scene_preroll_planning_speed_gate_enabled=True,
        scene_preroll_planning_compatible_min_messages=2,
        scene_preroll_planning_require_replan_anchor=True,
    )
    lead = hook.runtime.actors["lead_vehicle"]
    planning = {
        "nonempty_trajectory_count": 1,
        "last_trajectory_point_count": 12,
        "last_trajectory_type": "NORMAL",
        "last_planning_header_sequence_num": 10,
        "last_is_replan": False,
        "last_trajectory_first_nonexpired_point_v": 0.0,
        "last_trajectory_speed_at_1s_mps": 0.5,
    }
    stats = {
        "routing_success_count": 1,
        "control_tx_count": 0,
        "planning": planning,
    }
    artifacts.mkdir(parents=True, exist_ok=True)

    def observe(frame_id: int, sim_time_s: float, *, apply_setup: bool = True):
        (artifacts / "cyber_bridge_stats.json").write_text(
            json.dumps(stats), encoding="utf-8"
        )
        frame = type(
            "FrameContext",
            (),
            {
                "frame_id": frame_id,
                "sim_time_s": sim_time_s,
                "step": frame_id,
                "metadata": {},
            },
        )()
        if apply_setup:
            hook.before_tick(frame)
        hook.after_world_tick(frame)
        return frame

    gate = observe(10, 40.0, apply_setup=False)
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert hook.scene_preroll_active is True
    assert hook.scene_preroll_handover_pending is True
    assert hook.armed is False
    assert marker["handover_trigger"] == "planning_ready"
    assert marker["ego_speed_mps"] == 0.0
    assert marker["official_scenario_timer_started"] is False
    assert gate.metadata["fixed_scene_runtime"]["scene_sim_time_s"] is None

    unstamped_before_tick = type(
        "FrameContext",
        (),
        {"frame_id": 11, "sim_time_s": None, "step": 11, "metadata": {}},
    )()
    hook.before_tick(unstamped_before_tick)
    assert ego.last_control is None
    assert lead.last_control.throttle > 0.0

    observe(11, 40.05)
    assert ego.last_control is None
    assert lead.last_control.throttle > 0.0
    assert hook.scene_preroll_apollo_control_active is False

    stats["control_tx_count"] = 1
    observe(12, 40.10)
    assert hook.scene_preroll_apollo_control_active is True
    assert hook.scene_preroll_handover_pending is False
    assert hook.armed is False

    ego.velocity = _Vector(19.44, 0.0, 0.0)
    lead.velocity = _Vector(19.44, 0.0, 0.0)
    planning.update(
        {
            "last_planning_header_sequence_num": 20,
            "last_is_replan": True,
            "last_trajectory_first_nonexpired_point_v": 19.44,
            "last_trajectory_speed_at_1s_mps": 19.44,
        }
    )
    observe(13, 40.15)
    assert hook.scene_preroll_planning_compatible_count == 1
    assert hook.armed is False

    planning.update(
        {
            "last_planning_header_sequence_num": 21,
            "last_is_replan": False,
        }
    )
    ready = observe(14, 40.20)
    assert hook.scene_preroll_active is False
    assert hook.armed is True
    assert hook.start_sim_time_s is None
    assert ego.last_control is None
    assert ready.metadata["fixed_scene_runtime"]["scene_sim_time_s"] is None

    formal = observe(15, 40.25, apply_setup=False)
    marker = json.loads((artifacts / "fixed_scene_ego_handover.json").read_text())
    assert hook.start_sim_time_s == pytest.approx(40.25)
    assert formal.metadata["fixed_scene_runtime"]["scene_sim_time_s"] == 0.0
    assert marker["apollo_control_start_world_sim_time_s"] == pytest.approx(40.10)
    assert marker["official_scenario_start_world_sim_time_s"] == pytest.approx(40.25)
    assert marker["apollo_control_setup_duration_s"] == pytest.approx(0.15)
    assert marker["official_scenario_timer_started"] is True

    hook.close()


def test_scene_preroll_speed_control_does_not_taper_far_below_target() -> None:
    accelerating = _scene_preroll_speed_command(
        target_speed_mps=19.44, actual_speed_mps=16.46
    )
    near_target = _scene_preroll_speed_command(
        target_speed_mps=19.44, actual_speed_mps=19.44
    )
    lower_speed_target = _scene_preroll_speed_command(
        target_speed_mps=10.0, actual_speed_mps=10.0
    )

    assert accelerating.throttle == pytest.approx(0.7)
    assert accelerating.brake == 0.0
    assert near_target.throttle == pytest.approx(0.50544)
    assert near_target.brake == 0.0
    assert lower_speed_target.throttle == pytest.approx(0.26)
    assert lower_speed_target.brake == 0.0


def test_scene_preroll_lead_target_uses_time_ramp_then_closes_declared_gap() -> None:
    launch = _scene_preroll_lead_target_speed(
        target_speed_mps=19.44,
        elapsed_s=0.0,
        bumper_gap_m=20.0,
        expected_bumper_gap_m=20.0,
        speed_headroom_mps=1.5,
        acceleration_mps2=1.5,
    )
    ramp = _scene_preroll_lead_target_speed(
        target_speed_mps=19.44,
        elapsed_s=4.0,
        bumper_gap_m=20.0,
        expected_bumper_gap_m=20.0,
        speed_headroom_mps=1.5,
        acceleration_mps2=1.5,
    )
    close_large_gap = _scene_preroll_lead_target_speed(
        target_speed_mps=19.44,
        elapsed_s=12.0,
        bumper_gap_m=23.0,
        expected_bumper_gap_m=20.0,
        speed_headroom_mps=1.5,
        acceleration_mps2=1.5,
    )

    assert launch == pytest.approx(1.5)
    assert ramp == pytest.approx(7.5)
    assert close_large_gap == pytest.approx(17.94)


def test_fixed_scene_runtime_hook_classifies_preroll_timeout_as_setup_failure(tmp_path) -> None:
    ego = _FakeActor(
        actor_id=1,
        transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
    )
    artifacts = tmp_path / "artifacts"
    hook = setup_fixed_scene_runtime_hook(
        world=_FakeWorld(),
        ego_actor=ego,
        run_dir=tmp_path,
        artifact_dir=artifacts,
        scenario_path="configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml",
        start_gate="apollo_planning_ready",
        scene_preroll_enabled=True,
        scene_preroll_max_time_s=1.0,
    )
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(
            {
                "routing_success_count": 1,
                "planning": {
                    "nonempty_trajectory_count": 1,
                    "last_trajectory_point_count": 12,
                    "last_trajectory_type": "NORMAL",
                },
            }
        ),
        encoding="utf-8",
    )
    gate_ready = type(
        "FrameContext", (), {"frame_id": 10, "sim_time_s": 40.0, "step": 0, "metadata": {}}
    )()
    timed_out = type(
        "FrameContext", (), {"frame_id": 11, "sim_time_s": 41.1, "step": 1, "metadata": {}}
    )()

    hook.after_world_tick(gate_ready)
    hook.after_world_tick(timed_out)

    assert timed_out.metadata["run_termination_request"] == {
        "source": "fixed_scene_runtime",
        "reason": "scene_preroll_timeout_before_handover",
        "success": False,
        "fail_reason": "setup_failed",
        "scene_id": "baguang_lead_decel_accel_70_40_70_20m",
    }
    assert hook.to_dict()["status"] == "fail"

    hook.close()


def test_carla_runtime_spawn_feasibility_blocks_required_waypoint_fallback(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    storyboard = compile_fixed_scene_template(
        load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    )
    runtime = CarlaFixedSceneRuntime()

    state = runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)

    assert any("waypoint_spawn_required_but_fallback_used" in error for error in state.errors)
    assert state.spawn_feasibility["lead_vehicle"]["status"] == "fail"


def test_carla_runtime_applies_smooth_cut_in_transform(tmp_path) -> None:
    world = _FakeWorld()
    ego = _FakeActor(actor_id=1, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    template = load_fixed_scene_template("configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml")
    template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] = False
    storyboard = compile_fixed_scene_template(template)
    runtime = CarlaFixedSceneRuntime()
    runtime.setup({"world": world, "ego_actor": ego, "artifact_dir": tmp_path}, storyboard)
    lead = runtime.actors["lead_vehicle"]

    ego.transform.location.x = 13.0
    runtime.tick({"ego_actor": ego, "sim_time_sec": 9.0, "world_frame": 1})
    start_y = lead.transform.location.y
    runtime.tick({"ego_actor": ego, "sim_time_sec": 11.0, "world_frame": 2})
    mid_y = lead.transform.location.y
    runtime.tick({"ego_actor": ego, "sim_time_sec": 13.0, "world_frame": 3})
    final_y = lead.transform.location.y

    assert math.isclose(start_y, -3.6, abs_tol=0.01)
    assert start_y < mid_y < final_y
    assert math.isclose(final_y, 0.0, abs_tol=0.05)
    assert lead.target_velocity is not None


def test_actor_state_records_route_s_and_lane_id_from_carla_waypoint() -> None:
    world = _FakeProjectionWorld(s=123.4, road_id=7, section_id=1, lane_id=-2)
    actor = _FakeActor(actor_id=20, transform=_Transform(_Location(1.0, 2.0, 0.0), _Rotation(yaw=0.0)))

    state = carla_runtime_module._actor_state("lead_vehicle", actor, world=world)

    assert state.route_s == 123.4
    assert state.lane_id == "7:1:-2"


def test_lane_change_direction_sign_yaw_zero_left_and_right() -> None:
    right_actor = _FakeActor(actor_id=10, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))
    left_actor = _FakeActor(actor_id=11, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))

    _run_lane_change_to_completion(right_actor, direction="right")
    _run_lane_change_to_completion(left_actor, direction="left")

    assert math.isclose(right_actor.transform.location.y, 3.6, abs_tol=0.01)
    assert math.isclose(left_actor.transform.location.y, -3.6, abs_tol=0.01)


def test_lane_change_direction_sign_yaw_ninety_left_and_right() -> None:
    right_actor = _FakeActor(actor_id=12, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=90.0)))
    left_actor = _FakeActor(actor_id=13, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=90.0)))

    _run_lane_change_to_completion(right_actor, direction="right")
    _run_lane_change_to_completion(left_actor, direction="left")

    assert math.isclose(right_actor.transform.location.x, -3.6, abs_tol=0.01)
    assert math.isclose(left_actor.transform.location.x, 3.6, abs_tol=0.01)


def test_route_follow_steer_uses_waypoint_lookahead() -> None:
    world = _FakeRouteLookaheadWorld(
        current_tf=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
        lookahead_tf=_Transform(_Location(8.0, 8.0, 0.0), _Rotation(yaw=45.0)),
    )
    actor = _FakeActor(actor_id=14, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))

    steer = carla_runtime_module._route_follow_steer(actor, world)

    assert steer > 0.0
    assert steer <= 0.35


def test_target_velocity_uses_route_heading_when_available() -> None:
    world = _FakeRouteLookaheadWorld(
        current_tf=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=90.0)),
        lookahead_tf=_Transform(_Location(0.0, 8.0, 0.0), _Rotation(yaw=90.0)),
    )
    actor = _FakeActor(actor_id=15, transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)))

    carla_runtime_module._set_actor_target_velocity(actor, 10.0, world=world)

    assert actor.target_velocity is not None
    assert math.isclose(actor.target_velocity.x, 0.0, abs_tol=0.01)
    assert math.isclose(actor.target_velocity.y, 10.0, abs_tol=0.01)


def test_carla_runtime_module_does_not_import_carla_runtime() -> None:
    import carla_testbed.scenario_player.carla_runtime as module

    assert hasattr(module, "CarlaFixedSceneRuntime")


def _run_lane_change_to_completion(actor, *, direction: str) -> None:
    action = {
        "type": "lane_change",
        "direction": direction,
        "duration_s": 2.0,
        "lateral_shift_m": 3.6,
        "easing": "linear",
    }
    states: dict[str, dict] = {}
    carla_runtime_module._apply_lane_change_transform(
        actor=actor,
        action=action,
        context={"sim_time_sec": 0.0},
        states=states,
    )
    carla_runtime_module._apply_lane_change_transform(
        actor=actor,
        action=action,
        context={"sim_time_sec": 2.0},
        states=states,
    )


class _FakeWorld:
    def __init__(self) -> None:
        self.blueprints = _BlueprintLibrary()
        self.spawned: list[_FakeActor] = []

    def get_blueprint_library(self):
        return self.blueprints

    def try_spawn_actor(self, blueprint, transform):
        actor = _FakeActor(actor_id=100 + len(self.spawned), transform=transform, blueprint=blueprint)
        self.spawned.append(actor)
        return actor


class _FakeMaterializingWorld(_FakeWorld):
    def __init__(self, *, ego, materialized_transform) -> None:
        super().__init__()
        self.ego = ego
        self.materialized_transform = materialized_transform
        self.tick_count = 0

    def tick(self):
        self.tick_count += 1
        self.ego.transform = self.materialized_transform
        for actor in self.spawned:
            if actor.target_velocity is not None:
                actor.velocity = actor.target_velocity


class _FakeBranchingWaypointWorld(_FakeWorld):
    def __init__(self) -> None:
        super().__init__()
        self.map = _FakeBranchingWaypointMap()

    def get_map(self):
        return self.map


class _FakeBranchingWaypointMap:
    def __init__(self) -> None:
        self.start = _FakeWaypoint(
            s=0.0,
            road_id=1,
            section_id=0,
            lane_id=1,
            transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
        )
        self.curved_branch = _FakeWaypoint(
            s=300.0,
            road_id=2,
            section_id=0,
            lane_id=1,
            transform=_Transform(_Location(300.0, 20.0, 0.0), _Rotation(yaw=35.0)),
        )
        self.same_lane = _FakeWaypoint(
            s=300.0,
            road_id=1,
            section_id=0,
            lane_id=1,
            transform=_Transform(_Location(300.0, 0.0, 0.0), _Rotation(yaw=2.0)),
        )
        self.start._candidates = [self.curved_branch, self.same_lane]

    def get_waypoint(self, location):
        if getattr(location, "x", 0.0) <= 1.0:
            return self.start
        if abs(getattr(location, "y", 0.0)) < 1.0:
            return self.same_lane
        return self.curved_branch


class _FakeCurvedAheadWaypointWorld(_FakeWorld):
    def __init__(self) -> None:
        super().__init__()
        self.map = _FakeCurvedAheadWaypointMap()

    def get_map(self):
        return self.map


class _FakeCurvedAheadWaypointMap:
    def __init__(self) -> None:
        self.start = _FakeWaypoint(
            s=0.0,
            road_id=15,
            section_id=0,
            lane_id=1,
            transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
        )
        self.curved_ahead = _FakeWaypoint(
            s=300.0,
            road_id=3,
            section_id=0,
            lane_id=1,
            transform=_Transform(_Location(247.0, 53.0, 0.0), _Rotation(yaw=0.0)),
        )
        self.start._candidates = [self.curved_ahead]

    def get_waypoint(self, location):
        if getattr(location, "x", 0.0) <= 1.0:
            return self.start
        return self.curved_ahead


class _FakeBadShortAheadWaypointWorld(_FakeWorld):
    def __init__(self) -> None:
        super().__init__()
        self.map = _FakeBadShortAheadWaypointMap()

    def get_map(self):
        return self.map


class _FakeBadShortAheadWaypointMap:
    def __init__(self) -> None:
        self.start = _FakeWaypoint(
            s=0.0,
            road_id=4,
            section_id=0,
            lane_id=-2,
            transform=_Transform(_Location(0.0, 0.0, 0.0), _Rotation(yaw=0.0)),
        )
        self.bad_ahead = _FakeWaypoint(
            s=20.0,
            road_id=4,
            section_id=0,
            lane_id=-1,
            transform=_Transform(_Location(320.0, -3.7, 0.0), _Rotation(yaw=0.0)),
        )
        self.start._candidates = [self.bad_ahead]

    def get_waypoint(self, location):
        if getattr(location, "x", 0.0) <= 30.0:
            return self.start
        return self.bad_ahead


class _FakeProjectionWorld:
    def __init__(self, *, s, road_id, section_id, lane_id):
        self.map = _FakeProjectionMap(s=s, road_id=road_id, section_id=section_id, lane_id=lane_id)

    def get_map(self):
        return self.map


class _FakeProjectionMap:
    def __init__(self, *, s, road_id, section_id, lane_id):
        self.waypoint = _FakeWaypoint(s=s, road_id=road_id, section_id=section_id, lane_id=lane_id)

    def get_waypoint(self, location):
        return self.waypoint


class _FakeRouteLookaheadWorld(_FakeWorld):
    def __init__(self, *, current_tf, lookahead_tf) -> None:
        super().__init__()
        self.map = _FakeRouteLookaheadMap(current_tf=current_tf, lookahead_tf=lookahead_tf)

    def get_map(self):
        return self.map


class _FakeRouteLookaheadMap:
    def __init__(self, *, current_tf, lookahead_tf) -> None:
        self.lookahead = _FakeWaypoint(s=8.0, road_id=1, section_id=0, lane_id=1, transform=lookahead_tf)
        self.current = _FakeWaypoint(
            s=0.0,
            road_id=1,
            section_id=0,
            lane_id=1,
            transform=current_tf,
            candidates=[self.lookahead],
        )

    def get_waypoint(self, location):
        return self.current


class _FakeWaypoint:
    def __init__(self, *, s, road_id, section_id, lane_id, transform=None, candidates=None):
        self.s = s
        self.road_id = road_id
        self.section_id = section_id
        self.lane_id = lane_id
        self.transform = transform
        self._candidates = list(candidates or [])

    def next(self, distance_m):
        return list(self._candidates)


class _BlueprintLibrary:
    def find(self, blueprint_id):
        return _Blueprint(blueprint_id)

    def filter(self, pattern):
        return [_Blueprint(pattern.replace("*", "lincoln.mkz_2020"))]


class _Blueprint:
    def __init__(self, blueprint_id):
        self.id = blueprint_id
        self.attributes = {}

    def has_attribute(self, name):
        return name == "role_name"

    def set_attribute(self, name, value):
        self.attributes[name] = value


class _FakeActor:
    def __init__(self, *, actor_id, transform, blueprint=None):
        self.id = actor_id
        self.transform = transform
        self.blueprint = blueprint
        self.bounding_box = _BoundingBox(_Vector(2.2, 0.9, 0.8))
        self.velocity = _Vector(0.0, 0.0, 0.0)
        self.last_control = None
        self.acceleration = _Vector(0.0, 0.0, 0.0)
        self.target_velocity = None
        self.autopilot = None
        self.destroyed = False

    def get_transform(self):
        return self.transform

    def get_velocity(self):
        return self.velocity

    def apply_control(self, control):
        self.last_control = control

    def get_control(self):
        return self.last_control

    def get_acceleration(self):
        return self.acceleration

    def set_autopilot(self, enabled):
        self.autopilot = enabled

    def set_target_velocity(self, velocity):
        self.target_velocity = velocity

    def set_transform(self, transform):
        self.transform = transform

    def destroy(self):
        self.destroyed = True


class _Location:
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z


class _Rotation:
    def __init__(self, *, yaw=0.0, pitch=0.0, roll=0.0):
        self.yaw = yaw
        self.pitch = pitch
        self.roll = roll


class _Transform:
    def __init__(self, location, rotation):
        self.location = location
        self.rotation = rotation


class _Vector:
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z


class _FakeControl:
    def __init__(self, *, gear):
        self.gear = gear
        self.reverse = False
        self.manual_gear_shift = False


class _BoundingBox:
    def __init__(self, extent):
        self.extent = extent
