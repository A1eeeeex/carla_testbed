from __future__ import annotations

import json
import math

import pytest

from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime
from carla_testbed.scenario_player import carla_runtime as carla_runtime_module
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.runtime_hook import setup_fixed_scene_runtime_hook
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


class _BoundingBox:
    def __init__(self, extent):
        self.extent = extent
