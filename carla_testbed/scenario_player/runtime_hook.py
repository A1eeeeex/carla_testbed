from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.control.applicator import apply_control_to_vehicle
from carla_testbed.runner.hooks import FrameContext, RunHook
from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime, CarlaFixedSceneRuntimeState
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.initial_state import (
    materialize_ego_initial_pose,
    materialize_ego_initial_speed,
)
from carla_testbed.scenario_player.schema import load_fixed_scene_template


@dataclass
class _ScenePrerollControl:
    throttle: float
    brake: float
    steer: float = 0.0
    source: str = "fixed_scene_setup_preroll"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FixedSceneRuntimeHook(RunHook):
    """RunHook wrapper that advances fixed-scene non-ego actors once per frame."""

    runtime: CarlaFixedSceneRuntime
    world: Any
    ego_actor: Any
    run_dir: Path
    artifact_dir: Path
    storyboard: Mapping[str, Any]
    setup_state: CarlaFixedSceneRuntimeState
    name: str = "fixed_scene_runtime"
    tick_count: int = 0
    start_sim_time_s: float | None = None
    start_gate: str = "none"
    materialize_ego_initial_speed_on_arm: bool = False
    armed: bool = True
    readiness: dict[str, Any] = field(default_factory=dict)
    start_delay_s: float = 0.0
    gate_wait_start_sim_time_s: float | None = None
    defer_role_spawn_until_arm: bool = False
    reset_ego_pose_on_arm: bool = False
    initial_ego_transform: Any = None
    ego_speed_ready_mps: float = 0.0
    ego_speed_ready_hold_ticks: int = 1
    ego_speed_ready_count: int = 0
    post_reset_ego_speed_ready_mps: float = 0.0
    post_reset_ego_speed_ready_hold_ticks: int = 1
    post_reset_ego_speed_ready_count: int = 0
    post_reset_speed_gate_pending: bool = False
    planning_ready_min_nonempty_count: int = 1
    planning_ready_require_routing_success: bool = True
    planning_ready_min_trajectory_points: int = 2
    planning_ready_disallow_fallback: bool = True
    planning_ready_max_message_age_s: float | None = None
    planning_ready_require_initial_replan_anchor: bool = False
    planning_ready_poll_count: int = 0
    _last_planning_readiness_trace_signature: tuple[Any, ...] | None = None
    scene_preroll_enabled: bool = False
    scene_preroll_target_speed_mps: float = 0.0
    scene_preroll_target_actor_speed_mps: float = 0.0
    scene_preroll_materialize_initial_speeds_on_gate: bool = False
    scene_preroll_ego_handover_mode: str = "target_ready"
    scene_preroll_lead_speed_headroom_mps: float = 1.5
    scene_preroll_lead_acceleration_mps2: float = 1.5
    scene_preroll_speed_tolerance_mps: float = 0.5
    scene_preroll_gap_tolerance_m: float = 1.0
    scene_preroll_ready_hold_ticks: int = 10
    scene_preroll_planning_speed_gate_enabled: bool = False
    scene_preroll_planning_current_speed_tolerance_mps: float = 1.0
    scene_preroll_planning_lookahead_speed_gate_enabled: bool = True
    scene_preroll_planning_lookahead_speed_tolerance_mps: float = 2.0
    scene_preroll_planning_compatible_min_messages: int = 1
    scene_preroll_planning_require_replan_anchor: bool = False
    scene_preroll_drivetrain_gate_enabled: bool = False
    scene_preroll_drivetrain_max_abs_acceleration_mps2: float = 2.0
    scene_preroll_max_time_s: float = 30.0
    scene_preroll_active: bool = False
    scene_preroll_handover_pending: bool = False
    scene_preroll_apollo_control_active: bool = False
    scene_preroll_apollo_control_start_sim_time_s: float | None = None
    scene_preroll_last_observed_sim_time_s: float | None = None
    scene_preroll_start_sim_time_s: float | None = None
    scene_preroll_ready_count: int = 0
    scene_preroll_planning_compatible_count: int = 0
    scene_preroll_last_observed_planning_sequence_num: int | None = None
    scene_preroll_planning_replan_anchor_sequence_num: int | None = None
    scene_preroll_control_tx_baseline: int = 0
    scene_preroll_last_controls: dict[str, Any] = field(default_factory=dict)
    scene_preroll_handover: dict[str, Any] = field(default_factory=dict)
    scene_preroll_initial_state_materialization: dict[str, Any] = field(
        default_factory=dict
    )
    scene_preroll_obstacle_activation_written: bool = False
    role_speed_gate_pending: bool = False
    role_speed_ready_count: int = 0
    initial_speed_materialization_count: int = 0
    errors: list[str] = field(default_factory=list)
    last_tick_result: dict[str, Any] = field(default_factory=dict)
    _closed: bool = False

    def before_tick(self, frame_context: FrameContext) -> None:
        if self.scene_preroll_active:
            self._apply_scene_preroll_controls(frame_context)
            return
        if not (self.post_reset_speed_gate_pending or self.role_speed_gate_pending):
            return
        report = materialize_ego_initial_speed(
            ego_actor=self.ego_actor,
            storyboard=self.storyboard,
            artifact_dir=self.artifact_dir,
            enabled=True,
            source="runtime.fixed_scene_player.post_reset_before_tick",
        )
        self.initial_speed_materialization_count += 1
        if report.get("status") == "fail":
            self.errors.extend(str(item) for item in report.get("errors", []))
        if self.role_speed_gate_pending:
            self.runtime.materialize_role_initial_speeds()

    def after_world_tick(self, frame_context: FrameContext) -> None:
        current_sim_time = float(frame_context.sim_time_s)
        if self.scene_preroll_active:
            self._observe_scene_preroll(frame_context, current_sim_time)
            return
        if self.scene_preroll_handover_pending:
            self._wait_for_apollo_handover(frame_context, current_sim_time)
            if not self.armed:
                return
        if self.post_reset_speed_gate_pending:
            ego_speed = _actor_speed_mps(self.ego_actor)
            if ego_speed is not None and ego_speed >= self.post_reset_ego_speed_ready_mps:
                self.post_reset_ego_speed_ready_count += 1
            else:
                self.post_reset_ego_speed_ready_count = 0
            ready = self.post_reset_ego_speed_ready_count >= self.post_reset_ego_speed_ready_hold_ticks
            self.readiness = {
                "ready": ready,
                "ego_speed_mps": ego_speed,
                "required_ego_speed_mps": self.post_reset_ego_speed_ready_mps,
                "ready_hold_ticks": self.post_reset_ego_speed_ready_count,
                "required_hold_ticks": self.post_reset_ego_speed_ready_hold_ticks,
                "source": "post_reset_initial_speed_observation",
            }
            if not ready:
                self._write_waiting_metadata(frame_context, current_sim_time)
                return
            self.post_reset_speed_gate_pending = False
            self._begin_role_speed_settle()
            if not self.armed:
                self._write_waiting_metadata(frame_context, current_sim_time)
                return
        if self.role_speed_gate_pending:
            role_readiness = _role_initial_speed_readiness(self.runtime, self.storyboard)
            ego_speed = _actor_speed_mps(self.ego_actor)
            ego_ready = ego_speed is not None and ego_speed >= self.post_reset_ego_speed_ready_mps
            if role_readiness["ready"] and ego_ready:
                self.role_speed_ready_count += 1
            else:
                self.role_speed_ready_count = 0
            ready = self.role_speed_ready_count >= self.post_reset_ego_speed_ready_hold_ticks
            self.readiness = {
                "ready": ready,
                "ego_speed_mps": ego_speed,
                "ego_speed_ready": ego_ready,
                "required_ego_speed_mps": self.post_reset_ego_speed_ready_mps,
                "role_speeds": role_readiness["role_speeds"],
                "ready_hold_ticks": self.role_speed_ready_count,
                "required_hold_ticks": self.post_reset_ego_speed_ready_hold_ticks,
                "source": "post_spawn_joint_initial_speed_observation",
            }
            if not ready:
                self._write_waiting_metadata(frame_context, current_sim_time)
                return
            self.role_speed_gate_pending = False
            self.armed = True
        if not self.armed:
            if self.gate_wait_start_sim_time_s is None:
                self.gate_wait_start_sim_time_s = current_sim_time
            gate_elapsed_s = current_sim_time - self.gate_wait_start_sim_time_s
            if self.start_gate == "apollo_warmup_delay":
                self.readiness = {
                    "ready": gate_elapsed_s >= self.start_delay_s,
                    "gate_elapsed_s": gate_elapsed_s,
                    "required_delay_s": self.start_delay_s,
                    "source": "configured_sim_time_preroll",
                }
            elif self.start_gate == "ego_speed_ready":
                ego_speed = _actor_speed_mps(self.ego_actor)
                if ego_speed is not None and ego_speed >= self.ego_speed_ready_mps:
                    self.ego_speed_ready_count += 1
                else:
                    self.ego_speed_ready_count = 0
                self.readiness = {
                    "ready": self.ego_speed_ready_count >= self.ego_speed_ready_hold_ticks,
                    "ego_speed_mps": ego_speed,
                    "required_ego_speed_mps": self.ego_speed_ready_mps,
                    "ready_hold_ticks": self.ego_speed_ready_count,
                    "required_hold_ticks": self.ego_speed_ready_hold_ticks,
                    "source": "carla_ego_state",
                }
            elif self.start_gate == "apollo_planning_ready":
                self.readiness = self._apollo_planning_readiness(
                    current_sim_time_s=current_sim_time
                )
            else:
                self.readiness = {
                    "ready": False,
                    "source": "unsupported_start_gate",
                    "start_gate": self.start_gate,
                }
            frame_context.metadata["fixed_scene_runtime"] = {
                "tick_count": self.tick_count,
                "scene_sim_time_s": None,
                "world_sim_time_s": current_sim_time,
                "active_roles": self.runtime.active_roles(),
                "current_phase": None,
                "applied_roles": [],
                "stopped": False,
                "armed": False,
                "start_gate": self.start_gate,
                "readiness": dict(self.readiness),
            }
            if not self.readiness.get("ready"):
                return
            if self.scene_preroll_enabled:
                gate_planning = dict(self.readiness)
                self.scene_preroll_active = True
                self.scene_preroll_start_sim_time_s = current_sim_time
                self.scene_preroll_last_observed_sim_time_s = current_sim_time
                self.scene_preroll_ready_count = 0
                self.scene_preroll_planning_compatible_count = 0
                self.scene_preroll_last_observed_planning_sequence_num = None
                self.scene_preroll_planning_replan_anchor_sequence_num = None
                if self.scene_preroll_materialize_initial_speeds_on_gate:
                    materialization = self._materialize_scene_preroll_initial_state(
                        frame_context=frame_context,
                        current_sim_time=current_sim_time,
                        planning=gate_planning,
                    )
                    if materialization.get("status") == "fail":
                        error = "scene_preroll_initial_state_materialization_failed"
                        if error not in self.errors:
                            self.errors.append(error)
                        self.scene_preroll_active = False
                        self.readiness = {
                            **gate_planning,
                            "ready": False,
                            "source": "fixed_scene_setup_preroll",
                            "status": "failed",
                            "initial_state_materialization": materialization,
                        }
                        frame_context.metadata["run_termination_request"] = {
                            "source": self.name,
                            "reason": error,
                            "success": False,
                            "fail_reason": "setup_failed",
                            "scene_id": self.storyboard.get("scene_id"),
                        }
                        self._write_waiting_metadata(frame_context, current_sim_time)
                        return
                self.readiness = {
                    **self.readiness,
                    "ready": False,
                    "planning_ready": True,
                    "source": "fixed_scene_setup_preroll",
                    "status": "accelerating",
                    "initial_state_materialization": dict(
                        self.scene_preroll_initial_state_materialization
                    ),
                }
                if self.scene_preroll_ego_handover_mode == "planning_ready":
                    lead = self.target_actor()
                    ego_speed = _actor_speed_mps(self.ego_actor)
                    lead_speed = _actor_speed_mps(lead) if lead is not None else None
                    gap = _bumper_gap_m(self.ego_actor, lead) if lead is not None else None
                    self._begin_scene_preroll_handover(
                        frame_context=frame_context,
                        current_sim_time=current_sim_time,
                        planning=gate_planning,
                        planning_speed_compatibility=(
                            self._scene_preroll_planning_speed_compatibility(
                                gate_planning,
                                ego_speed,
                            )
                        ),
                        ego_speed=ego_speed,
                        lead_speed=lead_speed,
                        gap=gap,
                        trigger="planning_ready",
                    )
                self._write_waiting_metadata(frame_context, current_sim_time)
                return
            pose_report = materialize_ego_initial_pose(
                ego_actor=self.ego_actor,
                initial_transform=self.initial_ego_transform,
                world=self.world,
                artifact_dir=self.artifact_dir,
                enabled=self.reset_ego_pose_on_arm,
                source="runtime.fixed_scene_player.deferred_scene_arm",
            )
            if pose_report.get("status") == "fail":
                self.errors.extend(str(item) for item in pose_report.get("errors", []))
                return
            if self.reset_ego_pose_on_arm and self.post_reset_ego_speed_ready_mps > 0.0:
                self.post_reset_speed_gate_pending = True
                self._write_waiting_metadata(frame_context, current_sim_time)
                return
            self._complete_arm()
        if self.start_sim_time_s is None:
            self.start_sim_time_s = current_sim_time
            self._mark_official_scene_start(frame_context, current_sim_time)
        scene_sim_time = max(0.0, current_sim_time - float(self.start_sim_time_s))
        result = self.runtime.tick(
            {
                "world": self.world,
                "ego_actor": self.ego_actor,
                "run_dir": self.run_dir,
                "artifact_dir": self.artifact_dir,
                "sim_time_sec": scene_sim_time,
                "world_frame": frame_context.frame_id,
            }
        )
        self.tick_count += 1
        self.last_tick_result = dict(result)
        frame_context.metadata["fixed_scene_runtime"] = {
            "tick_count": self.tick_count,
            "scene_sim_time_s": scene_sim_time,
            "world_sim_time_s": current_sim_time,
            "active_roles": self.runtime.active_roles(),
            "current_phase": result.get("current_phase"),
            "applied_roles": sorted((result.get("applied_controls") or {}).keys()),
            "stopped": bool(result.get("stopped")),
            "armed": self.armed,
            "start_gate": self.start_gate,
            "readiness": dict(self.readiness),
            "defer_role_spawn_until_arm": self.defer_role_spawn_until_arm,
            "reset_ego_pose_on_arm": self.reset_ego_pose_on_arm,
            "post_reset_speed_gate_pending": self.post_reset_speed_gate_pending,
            "role_speed_gate_pending": self.role_speed_gate_pending,
            "start_delay_s": self.start_delay_s,
        }
        if result.get("stopped"):
            frame_context.metadata["run_termination_request"] = {
                "source": self.name,
                "reason": "fixed_scene_storyboard_completed",
                "scene_id": self.storyboard.get("scene_id"),
                "scene_sim_time_s": scene_sim_time,
            }

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.errors.extend(self.runtime.teardown())

    def target_actor(self) -> Any | None:
        role = target_actor_role_from_storyboard(self.storyboard)
        if not role:
            return None
        return self.runtime.actors.get(role)

    def arm_prestarted_scene(self) -> dict[str, Any]:
        """Arm a fully spawned scene immediately before the harness owns ticks."""

        if self.armed:
            return dict(self.readiness)
        self.runtime.materialize_role_initial_speeds()
        ego_report = materialize_ego_initial_speed(
            ego_actor=self.ego_actor,
            storyboard=self.storyboard,
            artifact_dir=self.artifact_dir,
            enabled=True,
            source="runtime.fixed_scene_player.pre_harness_atomic_initial_state",
        )
        role_readiness = _role_initial_speed_readiness(self.runtime, self.storyboard)
        self.readiness = {
            "ready": ego_report.get("status") != "fail",
            "ego_initial_speed_assignment": ego_report,
            "role_speeds": role_readiness["role_speeds"],
            "role_speed_observation_pending_first_world_tick": not role_readiness["ready"],
            "source": "pre_harness_atomic_initial_state_assignment",
        }
        if ego_report.get("status") == "fail":
            self.errors.extend(str(item) for item in ego_report.get("errors", []))
            return dict(self.readiness)
        self.start_gate = "prestarted_scene"
        self.reset_ego_pose_on_arm = False
        self.post_reset_speed_gate_pending = False
        self.role_speed_gate_pending = False
        self.armed = True
        return dict(self.readiness)

    def _apply_scene_preroll_controls(self, frame_context: FrameContext) -> None:
        lead = self.target_actor()
        if lead is None:
            self.errors.append("scene_preroll_target_actor_missing")
            return
        ego_target_speed = self.scene_preroll_target_speed_mps
        target_actor_speed = self.scene_preroll_target_actor_speed_mps
        gap_state = _scene_preroll_gap_state(self.storyboard, self.ego_actor, lead)
        gap = gap_state["setup_gap_m"]
        expected_gap = gap_state["expected_setup_gap_m"]
        preroll_start = self.scene_preroll_start_sim_time_s
        control_sim_time = _finite_float(getattr(frame_context, "sim_time_s", None))
        if control_sim_time is None:
            control_sim_time = self.scene_preroll_last_observed_sim_time_s
        if control_sim_time is None:
            control_sim_time = preroll_start
        elapsed_s = max(
            0.0,
            float(control_sim_time or 0.0)
            - (float(preroll_start) if preroll_start is not None else float(control_sim_time or 0.0)),
        )
        controls: dict[str, Any] = {}
        if self.scene_preroll_ego_handover_mode == "planning_ready":
            role_controls = (
                (
                    target_actor_role_from_storyboard(self.storyboard) or "target",
                    lead,
                    _scene_preroll_lead_target_speed(
                        target_speed_mps=target_actor_speed,
                        elapsed_s=elapsed_s,
                        bumper_gap_m=gap,
                        expected_bumper_gap_m=expected_gap,
                        speed_headroom_mps=self.scene_preroll_lead_speed_headroom_mps,
                        acceleration_mps2=self.scene_preroll_lead_acceleration_mps2,
                    ),
                ),
            )
        else:
            ego_target = ego_target_speed
            if gap is not None and expected_gap is not None:
                ego_target += max(-1.5, min(1.5, 0.25 * (gap - expected_gap)))
            role_controls = (
                ("ego", self.ego_actor, ego_target),
                (
                    target_actor_role_from_storyboard(self.storyboard) or "target",
                    lead,
                    target_actor_speed,
                ),
            )
        for role, actor, role_target in role_controls:
            speed = _actor_speed_mps(actor)
            command = _scene_preroll_speed_command(
                target_speed_mps=role_target,
                actual_speed_mps=0.0 if speed is None else speed,
            )
            command.metadata.update(
                {
                    "role": role,
                    "target_speed_mps": role_target,
                    "actual_speed_mps": speed,
                    "claim_boundary": "setup_only_not_backend_behavior",
                    "ego_control_owner": (
                        "apollo"
                        if self.scene_preroll_ego_handover_mode == "planning_ready"
                        else "fixed_scene_setup_preroll"
                    ),
                    "setup_speed_profile": (
                        "time_ramp_with_gap_correction"
                        if self.scene_preroll_ego_handover_mode == "planning_ready"
                        else "target_speed"
                    ),
                }
            )
            result = apply_control_to_vehicle(actor, command, stamp=frame_context)
            controls[role] = result.to_dict()
            if not result.applied_ok:
                self.errors.append(f"scene_preroll_control_apply_failed:{role}:{result.error}")
        self.scene_preroll_last_controls = controls

    def _observe_scene_preroll(
        self, frame_context: FrameContext, current_sim_time: float
    ) -> None:
        self.scene_preroll_last_observed_sim_time_s = current_sim_time
        lead = self.target_actor()
        planning = self._apollo_planning_readiness(
            current_sim_time_s=current_sim_time
        )
        ego_speed = _actor_speed_mps(self.ego_actor)
        lead_speed = _actor_speed_mps(lead) if lead is not None else None
        gap_state = _scene_preroll_gap_state(self.storyboard, self.ego_actor, lead)
        gap = gap_state["setup_gap_m"]
        expected_gap = gap_state["expected_setup_gap_m"]
        bumper_gap = gap_state["bumper_gap_m"]
        expected_bumper_gap = gap_state["expected_bumper_gap_m"]
        ego_target_speed = self.scene_preroll_target_speed_mps
        target_actor_speed = self.scene_preroll_target_actor_speed_mps
        speed_ready = (
            ego_speed is not None
            and lead_speed is not None
            and abs(ego_speed - ego_target_speed)
            <= self.scene_preroll_speed_tolerance_mps
            and abs(lead_speed - target_actor_speed)
            <= self.scene_preroll_speed_tolerance_mps
        )
        gap_ready = (
            gap is not None
            and expected_gap is not None
            and abs(gap - expected_gap) <= self.scene_preroll_gap_tolerance_m
        )
        if (
            speed_ready
            and gap_ready
            and not self.scene_preroll_obstacle_activation_written
        ):
            _write_json_atomic(
                self.artifact_dir / "fixed_scene_obstacle_activation.json",
                {
                    "schema_version": "fixed_scene_obstacle_activation.v1",
                    "status": "pass",
                    "world_frame": int(frame_context.frame_id),
                    "world_sim_time_s": current_sim_time,
                    "ego_speed_mps": ego_speed,
                    "lead_speed_mps": lead_speed,
                    "target_speed_mps": ego_target_speed,
                    "ego_target_speed_mps": ego_target_speed,
                    "target_actor_target_speed_mps": target_actor_speed,
                    "speed_tolerance_mps": self.scene_preroll_speed_tolerance_mps,
                    "speed_ready": True,
                    "gap_reference": gap_state["gap_reference"],
                    "setup_gap_m": gap,
                    "expected_setup_gap_m": expected_gap,
                    "bumper_gap_m": bumper_gap,
                    "expected_bumper_gap_m": expected_bumper_gap,
                    "gap_tolerance_m": self.scene_preroll_gap_tolerance_m,
                    "gap_ready": True,
                    "official_scenario_timer_started": False,
                    "claim_boundary": (
                        "Setup-only activation boundary for publishing the real "
                        "scenario target state; not backend behavior, assistance, "
                        "or natural-driving evidence."
                    ),
                },
            )
            self.scene_preroll_obstacle_activation_written = True
        planning_speed_compatibility = self._scene_preroll_planning_speed_compatibility(
            planning,
            ego_speed,
        )
        drivetrain_readiness = _scene_preroll_drivetrain_readiness(
            ego=self.ego_actor,
            target=lead,
            enabled=self.scene_preroll_drivetrain_gate_enabled,
            max_abs_acceleration_mps2=(
                self.scene_preroll_drivetrain_max_abs_acceleration_mps2
            ),
        )
        if self.scene_preroll_handover_pending:
            self._observe_apollo_control_start(frame_context, current_sim_time)
        apollo_control_ready = bool(
            self.scene_preroll_ego_handover_mode != "planning_ready"
            or self.scene_preroll_apollo_control_active
        )
        setup_stability_ready = bool(
            speed_ready
            and gap_ready
            and drivetrain_readiness["ready"]
            and apollo_control_ready
        )
        ready_now = bool(
            planning.get("ready")
            and planning_speed_compatibility["ready"]
            and setup_stability_ready
        )
        planning_sequence_num = _integer_or_none(
            planning.get("last_planning_header_sequence_num")
        )
        planning_is_replan = planning.get("last_is_replan") is True
        planning_sequence_advanced = bool(
            planning_sequence_num is not None
            and planning_sequence_num
            != self.scene_preroll_last_observed_planning_sequence_num
        )
        same_sequence_freshness_gap = bool(
            setup_stability_ready
            and not planning.get("ready")
            and planning.get("route_ready")
            and planning.get("planning_message_fresh") is False
            and not planning.get("fallback_trajectory")
            and _max_int(planning.get("last_trajectory_point_count"), 0)
            >= self.planning_ready_min_trajectory_points
            and planning_speed_compatibility["ready"]
            and planning_sequence_num is not None
            and planning_sequence_num
            == self.scene_preroll_last_observed_planning_sequence_num
        )
        planning_compatible_message_update = "unchanged"
        if not setup_stability_ready:
            self.scene_preroll_planning_compatible_count = 0
            self.scene_preroll_last_observed_planning_sequence_num = planning_sequence_num
            self.scene_preroll_planning_replan_anchor_sequence_num = None
            planning_compatible_message_update = "reset_setup_not_stable"
        elif ready_now and self.scene_preroll_planning_speed_gate_enabled:
            if planning_sequence_advanced:
                self.scene_preroll_last_observed_planning_sequence_num = planning_sequence_num
                if self.scene_preroll_planning_require_replan_anchor:
                    if planning_is_replan:
                        self.scene_preroll_planning_replan_anchor_sequence_num = (
                            planning_sequence_num
                        )
                        self.scene_preroll_planning_compatible_count = 1
                        planning_compatible_message_update = "accepted_replan_anchor"
                    elif self.scene_preroll_planning_replan_anchor_sequence_num is not None:
                        self.scene_preroll_planning_compatible_count += 1
                        planning_compatible_message_update = "accepted_distinct_confirmation"
                    else:
                        planning_compatible_message_update = "ignored_before_replan_anchor"
                else:
                    self.scene_preroll_planning_compatible_count += 1
                    planning_compatible_message_update = "accepted_distinct_compatible_message"
            else:
                planning_compatible_message_update = "same_eligible_sequence"
        elif ready_now:
            self.scene_preroll_planning_compatible_count = 1
            self.scene_preroll_last_observed_planning_sequence_num = (
                planning_sequence_num
            )
            planning_compatible_message_update = "speed_gate_disabled"
        elif same_sequence_freshness_gap:
            planning_compatible_message_update = (
                "preserved_same_sequence_freshness_gap"
            )
        else:
            self.scene_preroll_planning_compatible_count = 0
            self.scene_preroll_last_observed_planning_sequence_num = planning_sequence_num
            self.scene_preroll_planning_replan_anchor_sequence_num = None
            planning_compatible_message_update = "reset_ineligible_message"
        self.scene_preroll_ready_count = (
            self.scene_preroll_ready_count + 1 if setup_stability_ready else 0
        )
        planning_message_hold_ready = (
            self.scene_preroll_planning_compatible_count
            >= self.scene_preroll_planning_compatible_min_messages
        )
        preroll_start = (
            current_sim_time
            if self.scene_preroll_start_sim_time_s is None
            else self.scene_preroll_start_sim_time_s
        )
        elapsed_s = current_sim_time - float(preroll_start)
        if (
            self.scene_preroll_ego_handover_mode == "planning_ready"
            and not self.scene_preroll_apollo_control_active
        ):
            status = "waiting_for_first_apollo_control_publish"
        elif ready_now:
            status = "ready_hold"
        elif same_sequence_freshness_gap:
            status = "waiting_for_fresh_planning_message"
        elif (
            planning.get("ready")
            and speed_ready
            and gap_ready
            and planning_speed_compatibility["ready"]
            and not drivetrain_readiness["ready"]
        ):
            status = "waiting_for_drivetrain_stability"
        elif planning.get("ready") and speed_ready and gap_ready:
            status = "waiting_for_planning_speed_compatibility"
        else:
            status = "accelerating"
        self.readiness = {
            "ready": False,
            "source": "fixed_scene_setup_preroll",
            "status": status,
            "ego_handover_mode": self.scene_preroll_ego_handover_mode,
            "ego_control_owner": (
                "apollo"
                if self.scene_preroll_apollo_control_active
                else (
                    "awaiting_apollo"
                    if self.scene_preroll_ego_handover_mode == "planning_ready"
                    else "fixed_scene_setup_preroll"
                )
            ),
            "apollo_control_active": self.scene_preroll_apollo_control_active,
            "apollo_control_start_sim_time_s": (
                self.scene_preroll_apollo_control_start_sim_time_s
            ),
            "planning_ready": bool(planning.get("ready")),
            "planning": planning,
            "planning_speed_compatibility": planning_speed_compatibility,
            "drivetrain_readiness": drivetrain_readiness,
            "setup_stability_ready": setup_stability_ready,
            "ready_hold_scope": "speed_gap_drivetrain_and_control",
            "planning_sequence_advanced": planning_sequence_advanced,
            "planning_same_sequence_freshness_gap": same_sequence_freshness_gap,
            "planning_compatible_message_update": planning_compatible_message_update,
            "planning_compatible_message_count": self.scene_preroll_planning_compatible_count,
            "required_planning_compatible_message_count": (
                self.scene_preroll_planning_compatible_min_messages
            ),
            "planning_replan_anchor_required": (
                self.scene_preroll_planning_require_replan_anchor
            ),
            "planning_replan_anchor_sequence_num": (
                self.scene_preroll_planning_replan_anchor_sequence_num
            ),
            "planning_compatible_message_hold_ready": planning_message_hold_ready,
            "ego_speed_mps": ego_speed,
            "lead_speed_mps": lead_speed,
            "target_speed_mps": ego_target_speed,
            "ego_target_speed_mps": ego_target_speed,
            "target_actor_target_speed_mps": target_actor_speed,
            "speed_tolerance_mps": self.scene_preroll_speed_tolerance_mps,
            "speed_ready": speed_ready,
            "gap_reference": gap_state["gap_reference"],
            "setup_gap_m": gap,
            "expected_setup_gap_m": expected_gap,
            "bumper_gap_m": bumper_gap,
            "expected_bumper_gap_m": expected_bumper_gap,
            "gap_tolerance_m": self.scene_preroll_gap_tolerance_m,
            "gap_ready": gap_ready,
            "ready_hold_ticks": self.scene_preroll_ready_count,
            "required_hold_ticks": self.scene_preroll_ready_hold_ticks,
            "elapsed_s": elapsed_s,
            "max_time_s": self.scene_preroll_max_time_s,
            "initial_state_materialization": dict(
                self.scene_preroll_initial_state_materialization
            ),
        }
        _append_jsonl(
            self.artifact_dir / "fixed_scene_setup_preroll_trace.jsonl",
            {
                "schema_version": "fixed_scene_setup_preroll_trace.v1",
                "world_frame": int(frame_context.frame_id),
                "world_sim_time_s": current_sim_time,
                **self.readiness,
                "controls": self.scene_preroll_last_controls,
                "claim_boundary": "setup_only_not_backend_behavior",
            },
        )
        if elapsed_s > self.scene_preroll_max_time_s:
            if self.scene_preroll_ego_handover_mode != "planning_ready":
                error = "scene_preroll_timeout_before_handover"
            elif self.scene_preroll_apollo_control_active:
                error = "scene_preroll_timeout_after_apollo_handover"
            else:
                error = "scene_preroll_timeout_before_apollo_control"
            if error not in self.errors:
                self.errors.append(error)
            self.scene_preroll_active = False
            self.readiness["status"] = "failed"
            frame_context.metadata["run_termination_request"] = {
                "source": self.name,
                "reason": error,
                "success": False,
                "fail_reason": "setup_failed",
                "scene_id": self.storyboard.get("scene_id"),
            }
            self._write_waiting_metadata(frame_context, current_sim_time)
            return
        if (
            not ready_now
            or self.scene_preroll_ready_count < self.scene_preroll_ready_hold_ticks
            or not planning_message_hold_ready
        ):
            self._write_waiting_metadata(frame_context, current_sim_time)
            return

        if self.scene_preroll_ego_handover_mode == "planning_ready":
            self.scene_preroll_active = False
            self._complete_arm()
            self.readiness = {
                **self.readiness,
                "status": "ready_for_formal_scenario",
                "ready": True,
            }
            self._write_waiting_metadata(frame_context, current_sim_time)
            return

        self._begin_scene_preroll_handover(
            frame_context=frame_context,
            current_sim_time=current_sim_time,
            planning=planning,
            planning_speed_compatibility=planning_speed_compatibility,
            ego_speed=ego_speed,
            lead_speed=lead_speed,
            gap=bumper_gap,
            trigger="target_ready",
        )
        self.scene_preroll_active = False
        self._write_waiting_metadata(frame_context, current_sim_time)

    def _begin_scene_preroll_handover(
        self,
        *,
        frame_context: FrameContext,
        current_sim_time: float,
        planning: Mapping[str, Any],
        planning_speed_compatibility: Mapping[str, Any],
        ego_speed: float | None,
        lead_speed: float | None,
        gap: float | None,
        trigger: str,
    ) -> None:
        ego_target_speed = self.scene_preroll_target_speed_mps
        target_actor_speed = self.scene_preroll_target_actor_speed_mps
        lead = self.target_actor()
        gap_state = _scene_preroll_gap_state(self.storyboard, self.ego_actor, lead)
        stats = _load_json_mapping(self.artifact_dir / "cyber_bridge_stats.json")
        self.scene_preroll_control_tx_baseline = _max_int(stats.get("control_tx_count"), 0)
        handover = {
            "schema_version": "fixed_scene_ego_handover.v1",
            "status": "ready",
            "world_frame": int(frame_context.frame_id),
            "world_sim_time_s": current_sim_time,
            "ego_speed_mps": ego_speed,
            "lead_speed_mps": lead_speed,
            "target_speed_mps": ego_target_speed,
            "ego_target_speed_mps": ego_target_speed,
            "target_actor_target_speed_mps": target_actor_speed,
            "gap_reference": gap_state["gap_reference"],
            "setup_gap_m": gap_state["setup_gap_m"],
            "expected_setup_gap_m": gap_state["expected_setup_gap_m"],
            "bumper_gap_m": gap,
            "expected_bumper_gap_m": gap_state["expected_bumper_gap_m"],
            "planning": planning,
            "planning_speed_compatibility": planning_speed_compatibility,
            "planning_compatible_message_count": (
                self.scene_preroll_planning_compatible_count
            ),
            "required_planning_compatible_message_count": (
                self.scene_preroll_planning_compatible_min_messages
            ),
            "planning_replan_anchor_required": (
                self.scene_preroll_planning_require_replan_anchor
            ),
            "planning_replan_anchor_sequence_num": (
                self.scene_preroll_planning_replan_anchor_sequence_num
            ),
            "drivetrain_readiness": self.readiness.get("drivetrain_readiness"),
            "handover_trigger": trigger,
            "ego_handover_mode": self.scene_preroll_ego_handover_mode,
            "control_tx_count_before_handover": self.scene_preroll_control_tx_baseline,
            "control_owner_before": (
                "unowned_stationary_setup"
                if trigger == "planning_ready"
                else "fixed_scene_setup_preroll"
            ),
            "control_owner_after": "apollo",
            "official_scenario_timer_started": False,
            "claim_boundary": "setup_only_not_backend_behavior",
        }
        _write_json_atomic(self.artifact_dir / "fixed_scene_ego_handover.json", handover)
        self.scene_preroll_handover = handover
        self.scene_preroll_handover_pending = True
        self.readiness = {
            **self.readiness,
            "status": "waiting_for_first_apollo_control_publish",
            "ready": False,
        }

    def _observe_apollo_control_start(
        self, frame_context: FrameContext, current_sim_time: float
    ) -> bool:
        stats = _load_json_mapping(self.artifact_dir / "cyber_bridge_stats.json")
        stats_control_tx_count = _max_int(stats.get("control_tx_count"), 0)
        ack_path = self.artifact_dir / "fixed_scene_control_handover_ack.json"
        ack = _load_json_mapping(ack_path)
        ack_matches_handover = bool(
            str(ack.get("status") or "").strip().lower() == "published"
            and ack.get("handover_world_frame")
            == self.scene_preroll_handover.get("world_frame")
            and ack.get("handover_world_sim_time_s")
            == self.scene_preroll_handover.get("world_sim_time_s")
        )
        ack_control_tx_count = _max_int(ack.get("control_tx_count"), 0)
        ack_published = bool(
            ack_matches_handover
            and ack_control_tx_count > self.scene_preroll_control_tx_baseline
        )
        control_tx_count = (
            ack_control_tx_count if ack_published else stats_control_tx_count
        )
        published = bool(
            ack_published
            or stats_control_tx_count > self.scene_preroll_control_tx_baseline
        )
        if not published:
            return False
        ack_control_timestamp = (
            _finite_float(ack.get("control_timestamp_sec")) if ack_published else None
        )
        control_start_sim_time = (
            ack_control_timestamp
            if ack_control_timestamp is not None
            else current_sim_time
        )
        self.scene_preroll_handover_pending = False
        self.scene_preroll_apollo_control_active = True
        self.scene_preroll_apollo_control_start_sim_time_s = control_start_sim_time
        self.scene_preroll_handover = {
            **self.scene_preroll_handover,
            "apollo_control_started": True,
            "apollo_control_start_world_frame": int(frame_context.frame_id),
            "apollo_control_start_world_sim_time_s": control_start_sim_time,
            "apollo_control_start_observed_world_sim_time_s": current_sim_time,
            "apollo_control_start_observation_source": (
                "fixed_scene_control_handover_ack"
                if ack_published
                else "cyber_bridge_stats"
            ),
            "apollo_control_start_observation_delay_s": max(
                0.0, current_sim_time - control_start_sim_time
            ),
            "control_handover_ack_path": str(ack_path),
            "control_tx_count_after_handover": control_tx_count,
        }
        _write_json_atomic(
            self.artifact_dir / "fixed_scene_ego_handover.json",
            self.scene_preroll_handover,
        )
        return True

    def _mark_official_scene_start(
        self, frame_context: FrameContext, current_sim_time: float
    ) -> None:
        if not self.scene_preroll_handover or self.scene_preroll_handover.get(
            "official_scenario_timer_started"
        ):
            return
        control_start = self.scene_preroll_apollo_control_start_sim_time_s
        self.scene_preroll_handover = {
            **self.scene_preroll_handover,
            "official_scenario_timer_started": True,
            "official_scenario_start_world_frame": int(frame_context.frame_id),
            "official_scenario_start_world_sim_time_s": current_sim_time,
            "apollo_control_setup_duration_s": (
                current_sim_time - control_start if control_start is not None else None
            ),
        }
        _write_json_atomic(
            self.artifact_dir / "fixed_scene_ego_handover.json",
            self.scene_preroll_handover,
        )

    def _materialize_scene_preroll_initial_state(
        self,
        *,
        frame_context: FrameContext,
        current_sim_time: float,
        planning: Mapping[str, Any],
    ) -> dict[str, Any]:
        lead = self.target_actor()
        report: dict[str, Any] = {
            "schema_version": "fixed_scene_gate_initial_state_materialization.v1",
            "status": "pass",
            "world_frame": int(frame_context.frame_id),
            "world_sim_time_s": current_sim_time,
            "start_gate": self.start_gate,
            "official_scenario_timer_started": False,
            "pre_materialization_ego_speed_mps": _actor_speed_mps(self.ego_actor),
            "pre_materialization_lead_speed_mps": (
                _actor_speed_mps(lead) if lead is not None else None
            ),
            "planning": dict(planning),
            "errors": [],
            "claim_boundary": (
                "This is a one-shot authored Phase 1 initial-state assignment after "
                "Routing and fresh NORMAL Planning readiness. It is not ongoing ego "
                "control, Apollo acceleration-from-rest evidence, or natural-driving "
                "evidence."
            ),
        }
        try:
            self.runtime.materialize_role_initial_speeds()
        except Exception as exc:  # pragma: no cover - defensive CARLA runtime path
            report["status"] = "fail"
            report["errors"].append(
                f"role_initial_speed_materialization_failed:{type(exc).__name__}"
            )
        ego_report = materialize_ego_initial_speed(
            ego_actor=self.ego_actor,
            storyboard=self.storyboard,
            artifact_dir=self.artifact_dir,
            enabled=True,
            source="runtime.fixed_scene_player.apollo_planning_ready_gate",
        )
        self.initial_speed_materialization_count += 1
        report["ego_initial_state_materialization"] = ego_report
        report["role_initial_speed_observation"] = _role_initial_speed_readiness(
            self.runtime,
            self.storyboard,
        )
        if ego_report.get("status") == "fail":
            report["status"] = "fail"
            report["errors"].extend(str(item) for item in ego_report.get("errors", []))
        self.scene_preroll_initial_state_materialization = report
        _write_json_atomic(
            self.artifact_dir / "fixed_scene_gate_initial_state_materialization.json",
            report,
        )
        return report

    def _scene_preroll_planning_speed_compatibility(
        self,
        planning: Mapping[str, Any],
        ego_speed_mps: float | None,
    ) -> dict[str, Any]:
        current_speed = _finite_float(
            planning.get("last_trajectory_first_nonexpired_point_v")
        )
        lookahead_speed = _finite_float(planning.get("last_trajectory_speed_at_1s_mps"))
        current_delta = (
            abs(current_speed - ego_speed_mps)
            if current_speed is not None and ego_speed_mps is not None
            else None
        )
        lookahead_delta = (
            abs(lookahead_speed - ego_speed_mps)
            if lookahead_speed is not None and ego_speed_mps is not None
            else None
        )
        if not self.scene_preroll_planning_speed_gate_enabled:
            ready = True
            status = "disabled"
        else:
            ready = bool(
                current_delta is not None
                and current_delta
                <= self.scene_preroll_planning_current_speed_tolerance_mps
                and (
                    not self.scene_preroll_planning_lookahead_speed_gate_enabled
                    or (
                        lookahead_delta is not None
                        and lookahead_delta
                        <= self.scene_preroll_planning_lookahead_speed_tolerance_mps
                    )
                )
            )
            status = "compatible" if ready else "waiting"
        return {
            "enabled": self.scene_preroll_planning_speed_gate_enabled,
            "ready": ready,
            "status": status,
            "ego_speed_mps": ego_speed_mps,
            "trajectory_current_speed_mps": current_speed,
            "trajectory_current_speed_delta_mps": current_delta,
            "current_speed_tolerance_mps": (
                self.scene_preroll_planning_current_speed_tolerance_mps
            ),
            "trajectory_speed_at_1s_mps": lookahead_speed,
            "trajectory_speed_at_1s_delta_mps": lookahead_delta,
            "lookahead_speed_gate_enabled": (
                self.scene_preroll_planning_lookahead_speed_gate_enabled
            ),
            "lookahead_speed_required": bool(
                self.scene_preroll_planning_speed_gate_enabled
                and self.scene_preroll_planning_lookahead_speed_gate_enabled
            ),
            "lookahead_speed_tolerance_mps": (
                self.scene_preroll_planning_lookahead_speed_tolerance_mps
            ),
            "claim_boundary": "setup_handover_compatibility_only",
        }

    def _wait_for_apollo_handover(
        self, frame_context: FrameContext, current_sim_time: float
    ) -> None:
        published = self._observe_apollo_control_start(frame_context, current_sim_time)
        stats = _load_json_mapping(self.artifact_dir / "cyber_bridge_stats.json")
        control_tx_count = _max_int(stats.get("control_tx_count"), 0)
        self.readiness = {
            **self.readiness,
            "ready": published,
            "status": "ready" if published else "waiting_for_first_apollo_control_publish",
            "control_tx_count": control_tx_count,
            "control_tx_count_before_handover": self.scene_preroll_control_tx_baseline,
        }
        if not published:
            self._write_waiting_metadata(frame_context, current_sim_time)
            return
        self._complete_arm()

    def _apollo_planning_readiness(
        self, *, current_sim_time_s: float | None = None
    ) -> dict[str, Any]:
        """Read the live bridge counters without importing Apollo/CyberRT.

        Dynamic fixed-scene actors may be spawned before Apollo so the bridge
        can discover them, but their declared motion must not begin until the
        external planner has established a route and emitted a real
        trajectory.  The bridge stats file is the existing runtime-neutral
        readiness surface for that handoff.
        """

        path = self.artifact_dir / "cyber_bridge_stats.json"
        if not path.exists():
            return {
                "ready": False,
                "source": "apollo_cyber_bridge_stats",
                "path": str(path),
                "status": "missing",
                "routing_success_count": 0,
                "planning_nonempty_trajectory_count": 0,
                "required_nonempty_trajectory_count": self.planning_ready_min_nonempty_count,
                "require_routing_success": self.planning_ready_require_routing_success,
            }
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            return {
                "ready": False,
                "source": "apollo_cyber_bridge_stats",
                "path": str(path),
                "status": "unreadable",
                "error": f"{type(exc).__name__}: {exc}",
                "routing_success_count": 0,
                "planning_nonempty_trajectory_count": 0,
                "required_nonempty_trajectory_count": self.planning_ready_min_nonempty_count,
                "require_routing_success": self.planning_ready_require_routing_success,
            }
        if not isinstance(payload, Mapping):
            return {
                "ready": False,
                "source": "apollo_cyber_bridge_stats",
                "path": str(path),
                "status": "invalid_payload",
                "routing_success_count": 0,
                "planning_nonempty_trajectory_count": 0,
                "required_nonempty_trajectory_count": self.planning_ready_min_nonempty_count,
                "require_routing_success": self.planning_ready_require_routing_success,
            }

        planning = payload.get("planning")
        if not isinstance(planning, Mapping):
            planning = {}
        stats_planning = dict(planning)
        planning_snapshot: Mapping[str, Any] = stats_planning
        planning_snapshot_source = "cyber_bridge_stats.planning"
        planning_snapshot_path = path

        # cyber_bridge_stats.json is intentionally flushed periodically, while
        # this summary is replaced atomically after every Planning callback.
        # Use it only when it carries a newer message receipt timestamp; route
        # readiness and cumulative bridge counters remain sourced from stats.
        per_message_path = self.artifact_dir / "planning_topic_debug_summary.json"
        per_message_snapshot = _load_json_mapping(per_message_path)
        stats_message_wall_time_s = _finite_float(
            stats_planning.get("last_msg_wall_time_sec")
        )
        per_message_wall_time_s = _finite_float(
            per_message_snapshot.get("last_msg_wall_time_sec")
        )
        if per_message_wall_time_s is not None and (
            stats_message_wall_time_s is None
            or per_message_wall_time_s > stats_message_wall_time_s
        ):
            planning_snapshot = per_message_snapshot
            planning_snapshot_source = "planning_topic_debug_summary"
            planning_snapshot_path = per_message_path
        routing_success_count = _max_int(
            payload.get("routing_success_count"),
            stats_planning.get("routing_success_count"),
            0,
        )
        nonempty_count = _max_int(
            payload.get("planning_nonempty_trajectory_count"),
            planning_snapshot.get("nonempty_trajectory_count"),
            planning_snapshot.get("messages_with_nonzero_trajectory_points"),
            0,
        )
        latest_points = _max_int(planning_snapshot.get("last_trajectory_point_count"), 0)
        raw_trajectory_type = planning_snapshot.get("last_trajectory_type")
        if planning_snapshot_source == "cyber_bridge_stats.planning":
            latest_points = _max_int(
                payload.get("last_trajectory_point_count"),
                latest_points,
            )
            if raw_trajectory_type in (None, ""):
                raw_trajectory_type = payload.get("last_trajectory_type")
        trajectory_type = str(raw_trajectory_type).strip().upper() or None
        fallback_trajectory = trajectory_type in {"SPEED_FALLBACK", "PATH_FALLBACK", "UNKNOWN"}
        planning_is_replan = planning_snapshot.get("last_is_replan") is True
        initial_replan_anchor_required = bool(
            self.scene_preroll_enabled
            and self.scene_preroll_materialize_initial_speeds_on_gate
            and self.planning_ready_require_initial_replan_anchor
            and not self.scene_preroll_active
            and not self.scene_preroll_handover_pending
        )
        initial_replan_anchor_ready = bool(
            not initial_replan_anchor_required or planning_is_replan
        )
        message_wall_time_s = _finite_float(planning_snapshot.get("last_msg_wall_time_sec"))
        message_sim_time_s = _finite_float(planning_snapshot.get("last_msg_ts_sec"))
        message_wall_age_s = (
            max(0.0, time.time() - message_wall_time_s)
            if message_wall_time_s is not None
            else None
        )
        message_sim_age_s = (
            max(0.0, current_sim_time_s - message_sim_time_s)
            if current_sim_time_s is not None and message_sim_time_s is not None
            else None
        )
        # Synchronous CARLA can intentionally advance slower than wall time.
        # Prefer the shared simulation clock when available so readiness tests
        # message currency, not host throughput; retain wall age for diagnostics.
        freshness_clock = "sim_time" if message_sim_age_s is not None else "wall_time"
        message_age_s = (
            message_sim_age_s
            if message_sim_age_s is not None
            else message_wall_age_s
        )
        freshness_required = self.planning_ready_max_message_age_s is not None
        message_fresh = (
            not freshness_required
            or (
                message_age_s is not None
                and message_age_s <= float(self.planning_ready_max_message_age_s)
            )
        )
        planning_sequence_num = planning_snapshot.get("last_planning_header_sequence_num")
        route_ready = (
            routing_success_count >= 1
            if self.planning_ready_require_routing_success
            else True
        )
        trajectory_ready = (
            nonempty_count >= self.planning_ready_min_nonempty_count
            and latest_points >= self.planning_ready_min_trajectory_points
            and (
                not self.planning_ready_disallow_fallback
                or (trajectory_type is not None and not fallback_trajectory)
            )
            and message_fresh
            and initial_replan_anchor_ready
        )
        result = {
            "ready": bool(route_ready and trajectory_ready),
            "source": "apollo_cyber_bridge_stats",
            "path": str(path),
            "planning_snapshot_source": planning_snapshot_source,
            "planning_snapshot_path": str(planning_snapshot_path),
            "stats_last_planning_header_sequence_num": stats_planning.get(
                "last_planning_header_sequence_num"
            ),
            "stats_last_msg_wall_time_sec": stats_message_wall_time_s,
            "planning_snapshot_ahead_of_stats_s": (
                per_message_wall_time_s - stats_message_wall_time_s
                if planning_snapshot_source == "planning_topic_debug_summary"
                and per_message_wall_time_s is not None
                and stats_message_wall_time_s is not None
                else 0.0
            ),
            "status": "ready" if route_ready and trajectory_ready else "waiting",
            "routing_success_count": routing_success_count,
            "planning_nonempty_trajectory_count": nonempty_count,
            "last_trajectory_point_count": latest_points,
            "last_trajectory_type": trajectory_type,
            "fallback_trajectory": fallback_trajectory,
            "last_planning_header_sequence_num": planning_sequence_num,
            "last_is_replan": planning_is_replan,
            "last_replan_reason": planning_snapshot.get("last_replan_reason"),
            "initial_replan_anchor_required": initial_replan_anchor_required,
            "initial_replan_anchor_ready": initial_replan_anchor_ready,
            "last_trajectory_first_nonexpired_point_v": _finite_float(
                planning_snapshot.get("last_trajectory_first_nonexpired_point_v")
            ),
            "last_trajectory_speed_at_1s_mps": _finite_float(
                planning_snapshot.get("last_trajectory_speed_at_1s_mps")
            ),
            "last_trajectory_speed_at_1s_relative_time_sec": _finite_float(
                planning_snapshot.get("last_trajectory_speed_at_1s_relative_time_sec")
            ),
            "last_msg_wall_time_sec": message_wall_time_s,
            "last_msg_sim_time_sec": message_sim_time_s,
            "planning_message_age_s": message_age_s,
            "planning_message_wall_age_s": message_wall_age_s,
            "planning_message_sim_age_s": message_sim_age_s,
            "planning_message_freshness_clock": freshness_clock,
            "planning_message_fresh": message_fresh,
            "planning_message_freshness_required": freshness_required,
            "planning_ready_max_message_age_s": self.planning_ready_max_message_age_s,
            "planning_ready_require_initial_replan_anchor": (
                self.planning_ready_require_initial_replan_anchor
            ),
            "required_trajectory_point_count": self.planning_ready_min_trajectory_points,
            "disallow_fallback": self.planning_ready_disallow_fallback,
            "required_nonempty_trajectory_count": self.planning_ready_min_nonempty_count,
            "require_routing_success": self.planning_ready_require_routing_success,
            "route_ready": route_ready,
            "trajectory_ready": trajectory_ready,
        }
        self._record_planning_readiness(result)
        return result

    def _record_planning_readiness(self, readiness: Mapping[str, Any]) -> None:
        """Persist gate decisions without changing the readiness contract."""

        self.planning_ready_poll_count += 1
        signature = (
            readiness.get("status"),
            readiness.get("route_ready"),
            readiness.get("trajectory_ready"),
            readiness.get("routing_success_count"),
            readiness.get("planning_nonempty_trajectory_count"),
            readiness.get("last_trajectory_point_count"),
            readiness.get("last_trajectory_type"),
            readiness.get("fallback_trajectory"),
            readiness.get("planning_message_fresh"),
            readiness.get("last_is_replan"),
            readiness.get("initial_replan_anchor_required"),
            readiness.get("initial_replan_anchor_ready"),
            readiness.get("planning_snapshot_source"),
        )
        should_write = (
            signature != self._last_planning_readiness_trace_signature
            or self.planning_ready_poll_count % 20 == 0
        )
        self._last_planning_readiness_trace_signature = signature
        if not should_write:
            return
        _append_jsonl(
            self.artifact_dir / "fixed_scene_planning_readiness_trace.jsonl",
            {
                "schema_version": "fixed_scene_planning_readiness_trace.v1",
                "observed_at_wall_time_sec": time.time(),
                "poll_count": self.planning_ready_poll_count,
                **dict(readiness),
                "claim_boundary": (
                    "This trace records the strict Apollo Planning start-gate decision. "
                    "It does not relax freshness, trajectory-type, routing, or point-count "
                    "requirements and does not control any actor."
                ),
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "fixed_scene_runtime_hook.v1",
            "status": "fail" if self.setup_state.errors or self.errors else "pass",
            "scene_id": self.storyboard.get("scene_id"),
            "tick_count": self.tick_count,
            "start_sim_time_s": self.start_sim_time_s,
            "start_gate": self.start_gate,
            "start_delay_s": self.start_delay_s,
            "defer_role_spawn_until_arm": self.defer_role_spawn_until_arm,
            "ego_speed_ready_mps": self.ego_speed_ready_mps,
            "ego_speed_ready_hold_ticks": self.ego_speed_ready_hold_ticks,
            "post_reset_ego_speed_ready_mps": self.post_reset_ego_speed_ready_mps,
            "post_reset_ego_speed_ready_hold_ticks": self.post_reset_ego_speed_ready_hold_ticks,
            "post_reset_ego_speed_ready_count": self.post_reset_ego_speed_ready_count,
            "post_reset_speed_gate_pending": self.post_reset_speed_gate_pending,
            "planning_ready_min_nonempty_count": self.planning_ready_min_nonempty_count,
            "planning_ready_require_routing_success": self.planning_ready_require_routing_success,
            "planning_ready_min_trajectory_points": self.planning_ready_min_trajectory_points,
            "planning_ready_disallow_fallback": self.planning_ready_disallow_fallback,
            "planning_ready_max_message_age_s": self.planning_ready_max_message_age_s,
            "planning_ready_poll_count": self.planning_ready_poll_count,
            "scene_preroll_enabled": self.scene_preroll_enabled,
            "scene_preroll_target_speed_mps": self.scene_preroll_target_speed_mps,
            "scene_preroll_target_actor_speed_mps": (
                self.scene_preroll_target_actor_speed_mps
            ),
            "scene_preroll_materialize_initial_speeds_on_gate": (
                self.scene_preroll_materialize_initial_speeds_on_gate
            ),
            "scene_preroll_ego_handover_mode": self.scene_preroll_ego_handover_mode,
            "scene_preroll_lead_speed_headroom_mps": (
                self.scene_preroll_lead_speed_headroom_mps
            ),
            "scene_preroll_lead_acceleration_mps2": (
                self.scene_preroll_lead_acceleration_mps2
            ),
            "scene_preroll_speed_tolerance_mps": self.scene_preroll_speed_tolerance_mps,
            "scene_preroll_gap_tolerance_m": self.scene_preroll_gap_tolerance_m,
            "scene_preroll_ready_hold_ticks": self.scene_preroll_ready_hold_ticks,
            "scene_preroll_planning_speed_gate_enabled": (
                self.scene_preroll_planning_speed_gate_enabled
            ),
            "scene_preroll_planning_current_speed_tolerance_mps": (
                self.scene_preroll_planning_current_speed_tolerance_mps
            ),
            "scene_preroll_planning_lookahead_speed_gate_enabled": (
                self.scene_preroll_planning_lookahead_speed_gate_enabled
            ),
            "scene_preroll_planning_lookahead_speed_tolerance_mps": (
                self.scene_preroll_planning_lookahead_speed_tolerance_mps
            ),
            "scene_preroll_planning_compatible_min_messages": (
                self.scene_preroll_planning_compatible_min_messages
            ),
            "scene_preroll_planning_require_replan_anchor": (
                self.scene_preroll_planning_require_replan_anchor
            ),
            "scene_preroll_planning_compatible_count": (
                self.scene_preroll_planning_compatible_count
            ),
            "scene_preroll_planning_replan_anchor_sequence_num": (
                self.scene_preroll_planning_replan_anchor_sequence_num
            ),
            "scene_preroll_max_time_s": self.scene_preroll_max_time_s,
            "scene_preroll_active": self.scene_preroll_active,
            "scene_preroll_handover_pending": self.scene_preroll_handover_pending,
            "scene_preroll_apollo_control_active": (
                self.scene_preroll_apollo_control_active
            ),
            "scene_preroll_apollo_control_start_sim_time_s": (
                self.scene_preroll_apollo_control_start_sim_time_s
            ),
            "scene_preroll_last_observed_sim_time_s": (
                self.scene_preroll_last_observed_sim_time_s
            ),
            "scene_preroll_ready_count": self.scene_preroll_ready_count,
            "scene_preroll_handover": dict(self.scene_preroll_handover),
            "scene_preroll_initial_state_materialization": dict(
                self.scene_preroll_initial_state_materialization
            ),
            "role_speed_gate_pending": self.role_speed_gate_pending,
            "role_speed_ready_count": self.role_speed_ready_count,
            "initial_speed_materialization_count": self.initial_speed_materialization_count,
            "armed": self.armed,
            "readiness": dict(self.readiness),
            "active_roles": self.runtime.active_roles(),
            "target_actor_role": target_actor_role_from_storyboard(self.storyboard),
            "setup_state": self.setup_state.to_dict(),
            "errors": list(self.errors),
            "artifact_paths": dict(self.setup_state.artifact_paths),
            "claim_boundary": (
                "This hook may control ego only during an explicitly labeled setup "
                "pre-roll; after handover it controls fixed-scene non-ego actors only. "
                "Setup control does not prove Apollo/Autoware ego capability."
                if self.scene_preroll_enabled
                else "This hook controls non-ego fixed-scene actors only. It does not "
                "prove Apollo/Autoware ego capability."
            ),
        }

    def _complete_arm(self) -> None:
        if self.defer_role_spawn_until_arm:
            self.runtime.spawn_roles(materialize_initial_speeds=False)
        if not self.scene_preroll_enabled:
            self.runtime.materialize_role_initial_speeds()
        materialize_ego_initial_speed(
            ego_actor=self.ego_actor,
            storyboard=self.storyboard,
            artifact_dir=self.artifact_dir,
            enabled=(
                self.materialize_ego_initial_speed_on_arm and not self.scene_preroll_enabled
            ),
            source="runtime.fixed_scene_player.apollo_control_ready_arm",
        )
        self.armed = True

    def _begin_role_speed_settle(self) -> None:
        if self.defer_role_spawn_until_arm:
            # spawn_roles() advances CARLA once to materialize the actor. Give
            # moving roles their declared speed before that tick so the ego
            # cannot consume the configured initial gap while the target is
            # still stationary.
            self.runtime.spawn_roles(materialize_initial_speeds=True)
        self.runtime.materialize_role_initial_speeds()
        role_readiness = _role_initial_speed_readiness(self.runtime, self.storyboard)
        if role_readiness["ready"]:
            self.role_speed_ready_count = 1
            self.readiness = {
                "ready": True,
                "ego_speed_mps": _actor_speed_mps(self.ego_actor),
                "ego_speed_ready": True,
                "required_ego_speed_mps": self.post_reset_ego_speed_ready_mps,
                "role_speeds": role_readiness["role_speeds"],
                "ready_hold_ticks": 1,
                "required_hold_ticks": 1,
                "source": "atomic_post_spawn_initial_speed_observation",
            }
            self.armed = True
            return
        self.role_speed_gate_pending = True

    def _write_waiting_metadata(self, frame_context: FrameContext, current_sim_time: float) -> None:
        frame_context.metadata["fixed_scene_runtime"] = {
            "tick_count": self.tick_count,
            "scene_sim_time_s": None,
            "world_sim_time_s": current_sim_time,
            "active_roles": self.runtime.active_roles(),
            "current_phase": None,
            "applied_roles": [],
            "stopped": False,
            "armed": self.armed,
            "start_gate": self.start_gate,
            "readiness": dict(self.readiness),
            "post_reset_speed_gate_pending": self.post_reset_speed_gate_pending,
            "role_speed_gate_pending": self.role_speed_gate_pending,
            "scene_preroll_active": self.scene_preroll_active,
            "scene_preroll_handover_pending": self.scene_preroll_handover_pending,
            "scene_preroll_apollo_control_active": (
                self.scene_preroll_apollo_control_active
            ),
        }


def load_phase1_fixed_scene_storyboard(path: str | Path) -> dict[str, Any]:
    return compile_fixed_scene_template(load_fixed_scene_template(path))


def setup_fixed_scene_runtime_hook(
    *,
    world: Any,
    ego_actor: Any,
    run_dir: str | Path,
    scenario_path: str | Path,
    artifact_dir: str | Path | None = None,
    runtime: CarlaFixedSceneRuntime | None = None,
    start_gate: str = "none",
    materialize_ego_initial_speed_on_arm: bool = False,
    start_delay_s: float = 0.0,
    defer_role_spawn_until_arm: bool = False,
    reset_ego_pose_on_arm: bool = False,
    post_reset_ego_speed_ready_mps: float = 0.0,
    post_reset_ego_speed_ready_hold_ticks: int = 1,
    ego_speed_ready_mps: float = 0.0,
    ego_speed_ready_hold_ticks: int = 1,
    planning_ready_min_nonempty_count: int = 1,
    planning_ready_require_routing_success: bool = True,
    planning_ready_min_trajectory_points: int = 2,
    planning_ready_disallow_fallback: bool = True,
    planning_ready_max_message_age_s: float | None = None,
    planning_ready_require_initial_replan_anchor: bool | None = None,
    scene_preroll_enabled: bool = False,
    scene_preroll_target_speed_mps: float | None = None,
    scene_preroll_materialize_initial_speeds_on_gate: bool = False,
    scene_preroll_ego_handover_mode: str = "target_ready",
    scene_preroll_lead_speed_headroom_mps: float = 1.5,
    scene_preroll_lead_acceleration_mps2: float = 1.5,
    scene_preroll_speed_tolerance_mps: float = 0.5,
    scene_preroll_gap_tolerance_m: float = 1.0,
    scene_preroll_ready_hold_ticks: int = 10,
    scene_preroll_planning_speed_gate_enabled: bool = False,
    scene_preroll_planning_current_speed_tolerance_mps: float = 1.0,
    scene_preroll_planning_lookahead_speed_gate_enabled: bool = True,
    scene_preroll_planning_lookahead_speed_tolerance_mps: float = 2.0,
    scene_preroll_planning_compatible_min_messages: int = 1,
    scene_preroll_planning_require_replan_anchor: bool = False,
    scene_preroll_drivetrain_gate_enabled: bool = False,
    scene_preroll_drivetrain_max_abs_acceleration_mps2: float = 2.0,
    scene_preroll_max_time_s: float = 30.0,
) -> FixedSceneRuntimeHook:
    root = Path(run_dir).expanduser()
    artifacts = Path(artifact_dir).expanduser() if artifact_dir is not None else root / "artifacts"
    storyboard = load_phase1_fixed_scene_storyboard(scenario_path)
    configured_preroll_target = (
        _ego_initial_speed_mps(storyboard)
        if scene_preroll_target_speed_mps is None
        else float(scene_preroll_target_speed_mps)
    )
    configured_preroll_target_actor = _target_actor_initial_speed_mps(storyboard)
    if configured_preroll_target_actor is None:
        configured_preroll_target_actor = configured_preroll_target
    fixed_runtime = runtime or CarlaFixedSceneRuntime()
    normalized_gate = str(start_gate or "none").strip().lower()
    normalized_handover_mode = str(scene_preroll_ego_handover_mode or "target_ready").strip().lower()
    armed = normalized_gate in {"", "none"}
    if scene_preroll_enabled:
        artifacts.mkdir(parents=True, exist_ok=True)
        for stale_path in (
            artifacts / "fixed_scene_setup_preroll_trace.jsonl",
            artifacts / "fixed_scene_ego_handover.json",
            artifacts / "fixed_scene_control_handover_ack.json",
            artifacts / "fixed_scene_gate_initial_state_materialization.json",
            artifacts / "fixed_scene_obstacle_activation.json",
        ):
            try:
                stale_path.unlink()
            except FileNotFoundError:
                pass
    state = fixed_runtime.setup(
        {
            "world": world,
            "ego_actor": ego_actor,
            "run_dir": root,
            "artifact_dir": artifacts,
            "materialize_role_initial_speeds": armed,
            "defer_role_spawn": bool(defer_role_spawn_until_arm and not armed),
        },
        storyboard,
    )
    if scene_preroll_enabled and normalized_gate != "apollo_planning_ready":
        state.errors.append("scene_preroll_requires_apollo_planning_ready_start_gate")
    if scene_preroll_enabled and configured_preroll_target is None:
        state.errors.append("scene_preroll_target_speed_missing")
    if normalized_handover_mode not in {"target_ready", "planning_ready"}:
        state.errors.append(f"scene_preroll_ego_handover_mode_invalid:{normalized_handover_mode}")
    if (
        scene_preroll_materialize_initial_speeds_on_gate
        and normalized_handover_mode != "target_ready"
    ):
        state.errors.append(
            "scene_preroll_gate_initial_speed_materialization_requires_target_ready_handover"
        )
    # setup() advances CARLA once so the actor's true spawn pose is
    # materialized. Capture only after that tick; pre-setup transforms may be
    # placeholder values from the freshly spawned actor.
    initial_ego_transform = _safe_actor_transform(ego_actor)
    return FixedSceneRuntimeHook(
        runtime=fixed_runtime,
        world=world,
        ego_actor=ego_actor,
        run_dir=root,
        artifact_dir=artifacts,
        storyboard=storyboard,
        setup_state=state,
        start_gate=normalized_gate or "none",
        materialize_ego_initial_speed_on_arm=bool(materialize_ego_initial_speed_on_arm),
        armed=armed,
        start_delay_s=max(0.0, float(start_delay_s)),
        defer_role_spawn_until_arm=bool(defer_role_spawn_until_arm),
        reset_ego_pose_on_arm=bool(reset_ego_pose_on_arm),
        initial_ego_transform=initial_ego_transform,
        post_reset_ego_speed_ready_mps=max(0.0, float(post_reset_ego_speed_ready_mps)),
        post_reset_ego_speed_ready_hold_ticks=max(1, int(post_reset_ego_speed_ready_hold_ticks)),
        ego_speed_ready_mps=max(0.0, float(ego_speed_ready_mps)),
        ego_speed_ready_hold_ticks=max(1, int(ego_speed_ready_hold_ticks)),
        planning_ready_min_nonempty_count=max(1, int(planning_ready_min_nonempty_count)),
        planning_ready_require_routing_success=bool(planning_ready_require_routing_success),
        planning_ready_min_trajectory_points=max(1, int(planning_ready_min_trajectory_points)),
        planning_ready_disallow_fallback=bool(planning_ready_disallow_fallback),
        planning_ready_max_message_age_s=(
            None
            if planning_ready_max_message_age_s is None
            else max(0.0, float(planning_ready_max_message_age_s))
        ),
        planning_ready_require_initial_replan_anchor=bool(
            scene_preroll_planning_require_replan_anchor
            if planning_ready_require_initial_replan_anchor is None
            else planning_ready_require_initial_replan_anchor
        ),
        scene_preroll_enabled=bool(scene_preroll_enabled),
        scene_preroll_target_speed_mps=max(0.0, float(configured_preroll_target or 0.0)),
        scene_preroll_target_actor_speed_mps=max(
            0.0, float(configured_preroll_target_actor or 0.0)
        ),
        scene_preroll_materialize_initial_speeds_on_gate=bool(
            scene_preroll_materialize_initial_speeds_on_gate
        ),
        scene_preroll_ego_handover_mode=normalized_handover_mode,
        scene_preroll_lead_speed_headroom_mps=max(
            0.0, float(scene_preroll_lead_speed_headroom_mps)
        ),
        scene_preroll_lead_acceleration_mps2=max(
            0.0, float(scene_preroll_lead_acceleration_mps2)
        ),
        scene_preroll_speed_tolerance_mps=max(
            0.0, float(scene_preroll_speed_tolerance_mps)
        ),
        scene_preroll_gap_tolerance_m=max(0.0, float(scene_preroll_gap_tolerance_m)),
        scene_preroll_ready_hold_ticks=max(1, int(scene_preroll_ready_hold_ticks)),
        scene_preroll_planning_speed_gate_enabled=bool(
            scene_preroll_planning_speed_gate_enabled
        ),
        scene_preroll_planning_current_speed_tolerance_mps=max(
            0.0, float(scene_preroll_planning_current_speed_tolerance_mps)
        ),
        scene_preroll_planning_lookahead_speed_gate_enabled=bool(
            scene_preroll_planning_lookahead_speed_gate_enabled
        ),
        scene_preroll_planning_lookahead_speed_tolerance_mps=max(
            0.0, float(scene_preroll_planning_lookahead_speed_tolerance_mps)
        ),
        scene_preroll_planning_compatible_min_messages=max(
            1, int(scene_preroll_planning_compatible_min_messages)
        ),
        scene_preroll_planning_require_replan_anchor=bool(
            scene_preroll_planning_require_replan_anchor
        ),
        scene_preroll_drivetrain_gate_enabled=bool(
            scene_preroll_drivetrain_gate_enabled
        ),
        scene_preroll_drivetrain_max_abs_acceleration_mps2=max(
            0.0, float(scene_preroll_drivetrain_max_abs_acceleration_mps2)
        ),
        scene_preroll_max_time_s=max(1.0, float(scene_preroll_max_time_s)),
    )


def _safe_actor_transform(actor: Any) -> Any | None:
    getter = getattr(actor, "get_transform", None)
    if not callable(getter):
        return None
    try:
        return getter()
    except Exception:
        return None


def _scene_preroll_drivetrain_readiness(
    *,
    ego: Any,
    target: Any,
    enabled: bool,
    max_abs_acceleration_mps2: float,
) -> dict[str, Any]:
    roles = {
        "ego": _actor_drivetrain_state(
            ego, max_abs_acceleration_mps2=max_abs_acceleration_mps2
        ),
        "target": _actor_drivetrain_state(
            target, max_abs_acceleration_mps2=max_abs_acceleration_mps2
        ),
    }
    return {
        "enabled": bool(enabled),
        "ready": (not enabled) or all(item["ready"] for item in roles.values()),
        "max_abs_acceleration_mps2": float(max_abs_acceleration_mps2),
        "roles": roles,
        "claim_boundary": "setup_only_not_backend_behavior",
    }


def _actor_drivetrain_state(
    actor: Any, *, max_abs_acceleration_mps2: float
) -> dict[str, Any]:
    control = None
    getter = getattr(actor, "get_control", None)
    if callable(getter):
        try:
            control = getter()
        except Exception:
            control = None
    gear = _integer_or_none(getattr(control, "gear", None))
    reverse = bool(getattr(control, "reverse", False)) if control is not None else None
    manual_gear_shift = (
        bool(getattr(control, "manual_gear_shift", False))
        if control is not None
        else None
    )
    acceleration = _actor_longitudinal_acceleration_mps2(actor)
    forward_gear_ready = bool(
        gear is not None and gear > 0 and reverse is False and manual_gear_shift is False
    )
    acceleration_ready = bool(
        acceleration is not None
        and abs(acceleration) <= float(max_abs_acceleration_mps2)
    )
    return {
        "ready": forward_gear_ready and acceleration_ready,
        "gear": gear,
        "reverse": reverse,
        "manual_gear_shift": manual_gear_shift,
        "forward_gear_ready": forward_gear_ready,
        "longitudinal_acceleration_mps2": acceleration,
        "acceleration_ready": acceleration_ready,
    }


def _actor_longitudinal_acceleration_mps2(actor: Any) -> float | None:
    acceleration_getter = getattr(actor, "get_acceleration", None)
    transform_getter = getattr(actor, "get_transform", None)
    if not callable(acceleration_getter) or not callable(transform_getter):
        return None
    try:
        acceleration = acceleration_getter()
        transform = transform_getter()
        yaw_rad = math.radians(float(transform.rotation.yaw))
        ax = float(acceleration.x)
        ay = float(acceleration.y)
    except (AttributeError, TypeError, ValueError):
        return None
    value = ax * math.cos(yaw_rad) + ay * math.sin(yaw_rad)
    return value if math.isfinite(value) else None


def _actor_speed_mps(actor: Any) -> float | None:
    try:
        velocity = actor.get_velocity()
    except Exception:
        return None
    length = getattr(velocity, "length", None)
    if callable(length):
        try:
            return float(length())
        except Exception:
            return None
    try:
        x, y, z = float(velocity.x), float(velocity.y), float(velocity.z)
    except (AttributeError, TypeError, ValueError):
        return None
    return (x * x + y * y + z * z) ** 0.5


def _max_int(*values: Any) -> int:
    numbers: list[int] = []
    for value in values:
        try:
            if value is None or value == "":
                continue
            numbers.append(max(0, int(value)))
        except (TypeError, ValueError):
            continue
    return max(numbers, default=0)


def _integer_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _scene_preroll_speed_command(
    *, target_speed_mps: float, actual_speed_mps: float
) -> _ScenePrerollControl:
    error = float(target_speed_mps) - float(actual_speed_mps)
    if error < -0.25:
        throttle = 0.0
        brake = min(0.7, 0.18 * -error)
    elif error > 1.0:
        # Stay at the setup controller's bounded acceleration command until
        # close to target. Tapering several m/s early can settle below the
        # declared speed and consume the short Baguang road before handover.
        throttle = 0.7
        brake = 0.0
    else:
        brake = 0.0
        # CARLA road load requires a target-speed-dependent feed-forward term.
        # A fixed low base command formed a stable equilibrium below 70kph and
        # consumed the finite setup road before the handover gate could open.
        feedforward = min(0.7, 0.026 * max(0.0, float(target_speed_mps)))
        throttle = min(0.7, max(0.0, feedforward + 0.30 * error))
    return _ScenePrerollControl(throttle=throttle, brake=brake)


def _scene_preroll_lead_target_speed(
    *,
    target_speed_mps: float,
    elapsed_s: float,
    bumper_gap_m: float | None,
    expected_bumper_gap_m: float | None,
    speed_headroom_mps: float,
    acceleration_mps2: float,
) -> float:
    target_speed = max(0.0, float(target_speed_mps))
    if target_speed <= 0.0:
        return 0.0
    ramp_speed = min(
        target_speed,
        max(0.0, float(speed_headroom_mps))
        + max(0.0, float(acceleration_mps2)) * max(0.0, float(elapsed_s)),
    )
    gap_correction = 0.0
    if bumper_gap_m is not None and expected_bumper_gap_m is not None:
        # Keep the setup actor's launch profile independent from Apollo ego
        # speed. Directly following ego creates a circular follower loop because
        # Apollo simultaneously plans against this lead actor.
        gap_correction = 0.5 * (float(expected_bumper_gap_m) - float(bumper_gap_m))
    return min(target_speed, max(0.0, ramp_speed + gap_correction))


def _ego_initial_speed_mps(storyboard: Mapping[str, Any]) -> float | None:
    roles = storyboard.get("roles")
    ego = roles.get("ego") if isinstance(roles, Mapping) else None
    if not isinstance(ego, Mapping) or ego.get("initial_speed_mps") is None:
        return None
    try:
        return max(0.0, float(ego["initial_speed_mps"]))
    except (TypeError, ValueError):
        return None


def _target_actor_initial_speed_mps(storyboard: Mapping[str, Any]) -> float | None:
    roles = storyboard.get("roles")
    if not isinstance(roles, Mapping):
        return None
    role = target_actor_role_from_storyboard(storyboard)
    target = roles.get(role) if role else None
    if not isinstance(target, Mapping) or target.get("initial_speed_mps") is None:
        return None
    try:
        return max(0.0, float(target["initial_speed_mps"]))
    except (TypeError, ValueError):
        return None


def _expected_bumper_gap_m(storyboard: Mapping[str, Any]) -> float | None:
    roles = storyboard.get("roles")
    if not isinstance(roles, Mapping):
        return None
    role = target_actor_role_from_storyboard(storyboard)
    target = roles.get(role) if role else None
    spawn = target.get("spawn") if isinstance(target, Mapping) else None
    if not isinstance(spawn, Mapping) or spawn.get("expected_bumper_gap_m") is None:
        return None
    try:
        return float(spawn["expected_bumper_gap_m"])
    except (TypeError, ValueError):
        return None


def _scene_preroll_gap_state(
    storyboard: Mapping[str, Any], ego: Any, target: Any
) -> dict[str, Any]:
    roles = storyboard.get("roles")
    role = target_actor_role_from_storyboard(storyboard)
    role_config = roles.get(role) if isinstance(roles, Mapping) and role else None
    spawn = role_config.get("spawn") if isinstance(role_config, Mapping) else None
    if not isinstance(spawn, Mapping):
        spawn = {}
    gap_reference = str(spawn.get("gap_reference") or "center_to_center")
    bumper_gap = _bumper_gap_m(ego, target) if target is not None else None
    expected_bumper_gap = _expected_bumper_gap_m(storyboard)
    center_gap = _longitudinal_center_offset_m(ego, target) if target is not None else None
    expected_center_gap = _finite_float(
        spawn.get("s_offset_m", spawn.get("distance_m"))
    )
    if gap_reference == "bumper_to_bumper":
        setup_gap = bumper_gap
        expected_setup_gap = expected_bumper_gap
    else:
        setup_gap = center_gap
        expected_setup_gap = expected_center_gap
    return {
        "gap_reference": gap_reference,
        "setup_gap_m": setup_gap,
        "expected_setup_gap_m": expected_setup_gap,
        "center_gap_m": center_gap,
        "expected_center_gap_m": expected_center_gap,
        "bumper_gap_m": bumper_gap,
        "expected_bumper_gap_m": expected_bumper_gap,
    }


def _longitudinal_center_offset_m(ego: Any, target: Any) -> float | None:
    ego_tf = _safe_actor_transform(ego)
    target_tf = _safe_actor_transform(target)
    if ego_tf is None or target_tf is None:
        return None
    ego_location = getattr(ego_tf, "location", None)
    target_location = getattr(target_tf, "location", None)
    rotation = getattr(ego_tf, "rotation", None)
    if ego_location is None or target_location is None or rotation is None:
        return None
    try:
        yaw = math.radians(float(rotation.yaw))
        dx = float(target_location.x) - float(ego_location.x)
        dy = float(target_location.y) - float(ego_location.y)
    except (AttributeError, TypeError, ValueError):
        return None
    return dx * math.cos(yaw) + dy * math.sin(yaw)


def _bumper_gap_m(ego: Any, target: Any) -> float | None:
    longitudinal = _longitudinal_center_offset_m(ego, target)
    ego_tf = _safe_actor_transform(ego)
    rotation = getattr(ego_tf, "rotation", None) if ego_tf is not None else None
    if longitudinal is None or rotation is None:
        return None
    try:
        yaw = math.radians(float(rotation.yaw))
    except (AttributeError, TypeError, ValueError):
        return None
    ego_extent = _longitudinal_half_extent(ego, yaw)
    target_extent = _longitudinal_half_extent(target, yaw)
    if ego_extent is None or target_extent is None:
        return None
    return longitudinal - ego_extent - target_extent


def _longitudinal_half_extent(actor: Any, reference_yaw_rad: float) -> float | None:
    extent = getattr(getattr(actor, "bounding_box", None), "extent", None)
    transform = _safe_actor_transform(actor)
    rotation = getattr(transform, "rotation", None) if transform is not None else None
    if extent is None or rotation is None:
        return None
    try:
        relative_yaw = math.radians(float(rotation.yaw)) - reference_yaw_rad
        return abs(math.cos(relative_yaw)) * float(extent.x) + abs(math.sin(relative_yaw)) * float(
            extent.y
        )
    except (AttributeError, TypeError, ValueError):
        return None


def _load_json_mapping(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(dict(payload), sort_keys=True, default=str) + "\n")


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _role_initial_speed_readiness(
    runtime: CarlaFixedSceneRuntime,
    storyboard: Mapping[str, Any],
    *,
    tolerance_mps: float = 0.5,
) -> dict[str, Any]:
    role_configs = storyboard.get("roles")
    if not isinstance(role_configs, Mapping):
        role_configs = {}
    role_speeds: dict[str, dict[str, Any]] = {}
    ready = True
    for role, actor in runtime.actors.items():
        role_cfg = role_configs.get(role)
        if not isinstance(role_cfg, Mapping):
            continue
        spawn_cfg = role_cfg.get("spawn")
        if not isinstance(spawn_cfg, Mapping):
            spawn_cfg = {}
        expected = role_cfg.get("initial_speed_mps", spawn_cfg.get("initial_speed_mps"))
        if expected is None:
            continue
        expected_speed = max(0.0, float(expected))
        observed_speed = _actor_speed_mps(actor)
        role_ready = observed_speed is not None and abs(observed_speed - expected_speed) <= tolerance_mps
        ready = ready and role_ready
        role_speeds[str(role)] = {
            "expected_speed_mps": expected_speed,
            "observed_speed_mps": observed_speed,
            "ready": role_ready,
        }
    return {"ready": ready, "role_speeds": role_speeds}


def target_actor_role_from_storyboard(storyboard: Mapping[str, Any]) -> str | None:
    contract = storyboard.get("target_actor_contract")
    if isinstance(contract, Mapping):
        role = contract.get("target_actor_role") or contract.get("role")
        if role:
            return str(role)
    target = storyboard.get("target_actor")
    if isinstance(target, Mapping) and target.get("role"):
        return str(target["role"])
    return None
