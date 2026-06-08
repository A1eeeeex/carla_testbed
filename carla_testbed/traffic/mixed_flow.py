from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Mapping

from .base import TrafficFlowState
from .carla_tm_flow import CarlaTrafficManagerFlow
from .carla_walker_flow import CarlaWalkerFlow


class MixedCarlaFlow:
    name = "mixed_carla_flow"

    def __init__(self) -> None:
        self._vehicle_flow = CarlaTrafficManagerFlow()
        self._walker_flow = CarlaWalkerFlow()
        self._vehicle_state: TrafficFlowState | None = None
        self._walker_state: TrafficFlowState | None = None

    def setup(self, context: Any, config: dict[str, Any]) -> TrafficFlowState:
        flow_cfg = dict(config.get("traffic_flow") or config)
        vehicle_cfg = flow_cfg.get("vehicles") if isinstance(flow_cfg.get("vehicles"), Mapping) else {}
        walker_cfg = flow_cfg.get("walkers") if isinstance(flow_cfg.get("walkers"), Mapping) else {}
        vehicle_enabled = bool(vehicle_cfg.get("enabled", True)) and int(vehicle_cfg.get("count", 0) or 0) > 0
        walker_enabled = bool(walker_cfg.get("enabled", True)) and int(walker_cfg.get("count", 0) or 0) > 0

        if vehicle_enabled:
            self._vehicle_state = self._vehicle_flow.setup(context, {"traffic_flow": {**flow_cfg, "provider": "carla_traffic_manager"}})
        else:
            self._vehicle_state = TrafficFlowState(
                provider="carla_traffic_manager",
                enabled=False,
                seed=_int_or_none(flow_cfg.get("seed")),
                requested_count=0,
                spawned_count=0,
            )
        if walker_enabled:
            self._walker_state = self._walker_flow.setup(
                context,
                {"traffic_flow": {**flow_cfg, "provider": "carla_walker_ai_controller"}},
            )
        else:
            self._walker_state = TrafficFlowState(
                provider="carla_walker_ai_controller",
                enabled=False,
                seed=_int_or_none(flow_cfg.get("seed")),
                requested_count=0,
                spawned_count=0,
            )

        state = TrafficFlowState(
            provider=self.name,
            enabled=bool(flow_cfg.get("enabled", False)),
            seed=_int_or_none(flow_cfg.get("seed")),
            requested_count=(self._vehicle_state.requested_count + self._walker_state.requested_count),
            spawned_count=(self._vehicle_state.spawned_count + self._walker_state.spawned_count),
            actors=[*self._vehicle_state.actors, *self._walker_state.actors],
            warnings=[*self._vehicle_state.warnings, *self._walker_state.warnings],
            errors=[*self._vehicle_state.errors, *self._walker_state.errors],
        )
        _write_combined_manifest(context, flow_cfg=flow_cfg, state=state, vehicle_state=self._vehicle_state, walker_state=self._walker_state)
        return state

    def tick(self, context: Any) -> None:
        self._vehicle_flow.tick(context)
        self._walker_flow.tick(context)

    def teardown(self, context: Any) -> None:
        self._walker_flow.teardown(context)
        self._vehicle_flow.teardown(context)


def _write_combined_manifest(
    context: Any,
    *,
    flow_cfg: Mapping[str, Any],
    state: TrafficFlowState,
    vehicle_state: TrafficFlowState,
    walker_state: TrafficFlowState,
) -> None:
    root = _artifact_root(context)
    if root is None:
        return
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    existing = _read_json(artifacts / "traffic_flow_manifest.json")
    vehicles = [_actor_info_to_dict(actor) for actor in vehicle_state.actors]
    walkers = [_actor_info_to_dict(actor) for actor in walker_state.actors]
    walker_cfg = flow_cfg.get("walkers") if isinstance(flow_cfg.get("walkers"), Mapping) else {}
    manifest = {
        **existing,
        "schema_version": "traffic_flow_manifest.v1",
        "provider": "mixed_carla_flow",
        "enabled": state.enabled,
        "seed": state.seed,
        "background_traffic_control_source": "carla_traffic_manager" if vehicle_state.enabled else "none",
        "background_walker_control_source": "carla_walker_ai_controller" if walker_state.enabled else "none",
        "requested_vehicle_count": vehicle_state.requested_count,
        "spawned_vehicle_count": vehicle_state.spawned_count,
        "requested_walker_count": walker_state.requested_count,
        "spawned_walker_count": walker_state.spawned_count,
        "controller_count": _count_walker_controllers(walkers),
        "controller_started_count": _count_started_walker_controllers(walkers),
        "world_pedestrians_seed": state.seed,
        "walker_cross_factor": float(walker_cfg.get("cross_factor", 0.0) or 0.0),
        "vehicles": vehicles,
        "walkers": walkers,
        "actors": [*vehicles, *walkers],
        "warnings": list(state.warnings),
        "errors": list(state.errors),
        "combined_wall_time_sec": time.time(),
    }
    (artifacts / "traffic_flow_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _actor_info_to_dict(actor: Any) -> dict[str, Any]:
    payload = dict(actor.__dict__)
    payload.setdefault("control_source", actor.control_source or actor.provider)
    return payload


def _count_walker_controllers(walkers: list[dict[str, Any]]) -> int:
    return sum(1 for walker in walkers if (walker.get("behavior") or {}).get("controller_id") not in {None, -1})


def _count_started_walker_controllers(walkers: list[dict[str, Any]]) -> int:
    return sum(1 for walker in walkers if bool((walker.get("behavior") or {}).get("controller_started")))


def _artifact_root(context: Any) -> Path | None:
    for name in ("run_dir", "artifact_root", "output_dir"):
        value = getattr(context, name, None)
        if value:
            return Path(value).expanduser()
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
