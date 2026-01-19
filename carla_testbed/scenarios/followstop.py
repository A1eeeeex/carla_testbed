from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import carla

from carla_testbed.sim import spawn_with_retry
from .base import ActorRefs, Scenario


@dataclass
class FollowStopConfig:
    front_idx: int = 210
    ego_idx: int = 120
    stop_brake: float = 1.0


class FollowStopScenario(Scenario):
    """Minimal follow-stop scene: spawn ego + front; front is held with brake."""

    def __init__(self, cfg: FollowStopConfig):
        self.cfg = cfg
        self.actors: Optional[ActorRefs] = None

    def build(self, world: carla.World, carla_map: carla.Map, bp_lib: carla.BlueprintLibrary):
        spawns = carla_map.get_spawn_points()
        veh_bp = bp_lib.filter("vehicle.tesla.model3")[0]

        def _safe_idx(idx: int) -> int:
            if not spawns:
                return 0
            if idx < 0 or idx >= len(spawns):
                print(f"[WARN] preferred spawn idx {idx} out of range (0-{len(spawns)-1}), clamping.")
                return max(0, min(idx, len(spawns) - 1))
            return idx

        self.cfg.front_idx = _safe_idx(self.cfg.front_idx)
        self.cfg.ego_idx = _safe_idx(self.cfg.ego_idx)

        front, front_idx = spawn_with_retry(world, veh_bp, spawns, preferred_idx=self.cfg.front_idx)
        if front is None:
            raise RuntimeError("Failed to spawn front vehicle")
        front.set_simulate_physics(True)
        front.apply_control(carla.VehicleControl(throttle=0.0, brake=self.cfg.stop_brake, hand_brake=True))

        ego, ego_idx = spawn_with_retry(world, veh_bp, spawns, preferred_idx=self.cfg.ego_idx)
        if ego is None:
            front.destroy()
            raise RuntimeError("Failed to spawn ego vehicle")
        ego.set_simulate_physics(True)

        self.cfg.front_idx = front_idx
        self.cfg.ego_idx = ego_idx
        self.actors = ActorRefs(ego=ego, front=front)
        return self.actors

    def reset(self):
        # placeholder for future use (traffic reset, etc.)
        pass

    def destroy(self):
        if self.actors:
            for a in [self.actors.ego, self.actors.front]:
                try:
                    a.destroy()
                except Exception:
                    pass
            self.actors = None
