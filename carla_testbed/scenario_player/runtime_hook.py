from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.runner.hooks import FrameContext, RunHook
from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime, CarlaFixedSceneRuntimeState
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.initial_state import (
    materialize_ego_initial_pose,
    materialize_ego_initial_speed,
)
from carla_testbed.scenario_player.schema import load_fixed_scene_template


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
    role_speed_gate_pending: bool = False
    role_speed_ready_count: int = 0
    initial_speed_materialization_count: int = 0
    errors: list[str] = field(default_factory=list)
    last_tick_result: dict[str, Any] = field(default_factory=dict)
    _closed: bool = False

    def before_tick(self, frame_context: FrameContext) -> None:
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
                "This hook controls non-ego fixed-scene actors only. It does not "
                "prove Apollo/Autoware ego capability."
            ),
        }

    def _complete_arm(self) -> None:
        if self.defer_role_spawn_until_arm:
            self.runtime.spawn_roles(materialize_initial_speeds=False)
        self.runtime.materialize_role_initial_speeds()
        materialize_ego_initial_speed(
            ego_actor=self.ego_actor,
            storyboard=self.storyboard,
            artifact_dir=self.artifact_dir,
            enabled=self.materialize_ego_initial_speed_on_arm,
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
            "armed": False,
            "start_gate": self.start_gate,
            "readiness": dict(self.readiness),
            "post_reset_speed_gate_pending": self.post_reset_speed_gate_pending,
            "role_speed_gate_pending": self.role_speed_gate_pending,
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
) -> FixedSceneRuntimeHook:
    root = Path(run_dir).expanduser()
    artifacts = Path(artifact_dir).expanduser() if artifact_dir is not None else root / "artifacts"
    storyboard = load_phase1_fixed_scene_storyboard(scenario_path)
    fixed_runtime = runtime or CarlaFixedSceneRuntime()
    normalized_gate = str(start_gate or "none").strip().lower()
    armed = normalized_gate in {"", "none"}
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
    )


def _safe_actor_transform(actor: Any) -> Any | None:
    getter = getattr(actor, "get_transform", None)
    if not callable(getter):
        return None
    try:
        return getter()
    except Exception:
        return None


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
