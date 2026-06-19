from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.runner.hooks import FrameContext, RunHook
from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime, CarlaFixedSceneRuntimeState
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
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
    errors: list[str] = field(default_factory=list)
    last_tick_result: dict[str, Any] = field(default_factory=dict)
    _closed: bool = False

    def after_world_tick(self, frame_context: FrameContext) -> None:
        result = self.runtime.tick(
            {
                "world": self.world,
                "ego_actor": self.ego_actor,
                "run_dir": self.run_dir,
                "artifact_dir": self.artifact_dir,
                "sim_time_sec": frame_context.sim_time_s,
                "world_frame": frame_context.frame_id,
            }
        )
        self.tick_count += 1
        self.last_tick_result = dict(result)
        frame_context.metadata["fixed_scene_runtime"] = {
            "tick_count": self.tick_count,
            "active_roles": self.runtime.active_roles(),
            "current_phase": result.get("current_phase"),
            "applied_roles": sorted((result.get("applied_controls") or {}).keys()),
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "fixed_scene_runtime_hook.v1",
            "status": "fail" if self.setup_state.errors or self.errors else "pass",
            "scene_id": self.storyboard.get("scene_id"),
            "tick_count": self.tick_count,
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
) -> FixedSceneRuntimeHook:
    root = Path(run_dir).expanduser()
    artifacts = Path(artifact_dir).expanduser() if artifact_dir is not None else root / "artifacts"
    storyboard = load_phase1_fixed_scene_storyboard(scenario_path)
    fixed_runtime = runtime or CarlaFixedSceneRuntime()
    state = fixed_runtime.setup(
        {
            "world": world,
            "ego_actor": ego_actor,
            "run_dir": root,
            "artifact_dir": artifacts,
        },
        storyboard,
    )
    return FixedSceneRuntimeHook(
        runtime=fixed_runtime,
        world=world,
        ego_actor=ego_actor,
        run_dir=root,
        artifact_dir=artifacts,
        storyboard=storyboard,
        setup_state=state,
    )


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
