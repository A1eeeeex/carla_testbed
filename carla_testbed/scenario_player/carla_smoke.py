from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.fixed_scene_contract import (
    analyze_fixed_scene_contract_run_dir,
    write_fixed_scene_contract_report,
)
from carla_testbed.analysis.scenario_actor_contract import (
    analyze_scenario_actor_contract_run_dir,
    write_scenario_actor_contract_report,
)
from carla_testbed.control.applicator import apply_control_to_vehicle
from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


class _EgoCommand:
    source = "carla_builtin_smoke"

    def __init__(self, *, throttle: float, brake: float = 0.0, steer: float = 0.0) -> None:
        self.throttle = throttle
        self.brake = brake
        self.steer = steer
        self.reverse = False
        self.hand_brake = False
        self.manual_gear_shift = False
        self.gear = 0
        self.metadata = {"runtime_boundary": "carla_builtin_ego_smoke_not_autonomy_claim"}


def run_fixed_scene_carla_smoke(
    *,
    template_path: str | Path,
    run_dir: str | Path,
    host: str = "localhost",
    port: int = 2000,
    town: str | None = "Town01",
    duration_s: float | None = None,
    fixed_dt_s: float = 0.05,
    ego_spawn_index: int | None = None,
    ego_throttle: float = 0.35,
    ego_brake_after_s: float | None = None,
) -> dict[str, Any]:
    """Run a CARLA-only fixed-scene smoke.

    This function is local-runtime only. It validates scene playback and trace
    production, not Apollo/Autoware autonomy.
    """

    carla = _import_carla()
    run_root = Path(run_dir).expanduser()
    artifacts = run_root / "artifacts"
    analysis = run_root / "analysis"
    artifacts.mkdir(parents=True, exist_ok=True)
    template = load_fixed_scene_template(template_path)
    storyboard = compile_fixed_scene_template(template)
    duration = float(duration_s or storyboard.get("params", {}).get("duration_s", 30.0) or 30.0)
    spawn_index = ego_spawn_index if ego_spawn_index is not None else spawn_index_from_ref(
        _ego_spawn_ref(storyboard)
    )

    client = carla.Client(host, int(port))
    client.set_timeout(30.0)
    world = client.load_world(town) if town else client.get_world()
    original_settings = world.get_settings()
    sync_settings = world.get_settings()
    sync_settings.synchronous_mode = True
    sync_settings.fixed_delta_seconds = float(fixed_dt_s)
    world.apply_settings(sync_settings)

    ego = None
    runtime = CarlaFixedSceneRuntime()
    cleanup_errors: list[str] = []
    start_wall = time.time()
    ticks = 0
    try:
        ego = _spawn_ego(world, spawn_index=spawn_index)
        state = runtime.setup(
            {
                "world": world,
                "ego_actor": ego,
                "artifact_dir": artifacts,
                "run_dir": run_root,
            },
            storyboard,
        )
        sim_time = 0.0
        while sim_time < duration:
            frame = world.tick()
            ticks += 1
            sim_time = ticks * float(fixed_dt_s)
            runtime.tick(
                {
                    "world": world,
                    "ego_actor": ego,
                    "artifact_dir": artifacts,
                    "run_dir": run_root,
                    "sim_time_sec": sim_time,
                    "world_frame": int(frame) if frame is not None else ticks,
                }
            )
            brake = 1.0 if ego_brake_after_s is not None and sim_time >= float(ego_brake_after_s) else 0.0
            throttle = 0.0 if brake > 0.0 else float(ego_throttle)
            apply_control_to_vehicle(ego, _EgoCommand(throttle=throttle, brake=brake))
        status = "pass" if not state.errors else "warn"
    finally:
        cleanup_errors.extend(runtime.teardown())
        if ego is not None:
            try:
                ego.destroy()
            except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
                cleanup_errors.append(f"ego_destroy_failed:{type(exc).__name__}: {exc}")
        try:
            world.apply_settings(original_settings)
        except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
            cleanup_errors.append(f"restore_world_settings_failed:{type(exc).__name__}: {exc}")

    end_wall = time.time()
    manifest = {
        "schema_version": "fixed_scene_carla_smoke_manifest.v1",
        "run_id": run_root.name,
        "scenario_id": storyboard.get("scene_id"),
        "scenario_class": storyboard.get("scenario_class"),
        "map": town,
        "backend": "carla_builtin",
        "ego_control_source": "carla_builtin_smoke",
        "scenario_actor_control_source": "fixed_scene_player",
        "background_traffic_control_source": "none",
        "background_walker_control_source": "none",
        "needs_local_carla": True,
        "starts_apollo": False,
        "starts_autoware": False,
        "interpretation_boundary": "CARLA-only fixed-scene smoke, not an autonomy claim.",
    }
    summary = {
        "schema_version": "fixed_scene_carla_smoke_summary.v1",
        "run_id": run_root.name,
        "status": status,
        "success": status in {"pass", "warn"},
        "ticks": ticks,
        "sim_duration_s": ticks * float(fixed_dt_s),
        "wall_duration_s": end_wall - start_wall,
        "cleanup_errors": cleanup_errors,
        "claim_boundary": "not_natural_driving_evidence",
    }
    (run_root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    fixed_report = analyze_fixed_scene_contract_run_dir(run_root)
    actor_report = analyze_scenario_actor_contract_run_dir(run_root)
    fixed_paths = write_fixed_scene_contract_report(fixed_report, analysis / "fixed_scene_contract")
    actor_paths = write_scenario_actor_contract_report(actor_report, analysis / "scenario_actor_contract")
    return {
        "schema_version": "fixed_scene_carla_smoke_result.v1",
        "run_dir": str(run_root),
        "status": status,
        "manifest": str(run_root / "manifest.json"),
        "summary": str(run_root / "summary.json"),
        "fixed_scene_contract": fixed_paths,
        "scenario_actor_contract": actor_paths,
    }


def spawn_index_from_ref(spawn_ref: str | None) -> int:
    if not spawn_ref:
        return 0
    digits = "".join(ch for ch in str(spawn_ref) if ch.isdigit())
    return int(digits) if digits else 0


def _ego_spawn_ref(storyboard: Mapping[str, Any]) -> str | None:
    ego = (storyboard.get("roles") or {}).get("ego") if isinstance(storyboard.get("roles"), Mapping) else {}
    return str(ego.get("spawn_ref")) if isinstance(ego, Mapping) and ego.get("spawn_ref") else None


def _spawn_ego(world: Any, *, spawn_index: int) -> Any:
    blueprint = _find_ego_blueprint(world)
    spawns = list(world.get_map().get_spawn_points())
    if not spawns:
        raise RuntimeError("CARLA map has no spawn points")
    transform = spawns[max(0, min(int(spawn_index), len(spawns) - 1))]
    actor = world.try_spawn_actor(blueprint, transform)
    if actor is None:
        actor = world.spawn_actor(blueprint, transform)
    if hasattr(actor, "set_autopilot"):
        actor.set_autopilot(False)
    return actor


def _find_ego_blueprint(world: Any) -> Any:
    library = world.get_blueprint_library()
    for blueprint_id in ("vehicle.lincoln.mkz_2020", "vehicle.lincoln.mkz_2017", "vehicle.tesla.model3"):
        try:
            return library.find(blueprint_id)
        except Exception:
            continue
    matches = list(library.filter("vehicle.*"))
    if not matches:
        raise RuntimeError("no vehicle blueprint available")
    return matches[0]


def _import_carla() -> Any:
    try:
        import carla  # type: ignore

        return carla
    except Exception as exc:  # pragma: no cover - local runtime dependency
        raise RuntimeError("CARLA Python API is required for fixed-scene CARLA smoke") from exc
