from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from carla_testbed.schemas import ControlCommand


@dataclass
class LegacyControllerConfig:
    lateral_mode: str = "dummy"
    policy_mode: str = "acc"
    controller_mode: str = "composite"  # or hybrid_agent_acc
    agent_type: str = "basic"
    takeover_dist_m: float = 200.0
    blend_time_s: float = 1.5


class LegacyFollowStopController:
    """Adapter around existing code/followstop/controllers.py build_default_controller."""

    def __init__(
        self,
        cfg: LegacyControllerConfig,
        root: Optional[Path] = None,
        world: Optional[Any] = None,
        carla_map: Optional[Any] = None,
        ego: Optional[Any] = None,
        front: Optional[Any] = None,
    ):
        try:
            from algo.controllers.legacy_followstop import controllers as legacy_ctrl
        except (AttributeError, ImportError, ModuleNotFoundError) as exc:
            if any(value is not None for value in (world, carla_map, ego, front)):
                raise
            self._legacy = None
            self.cfg = cfg
            self.ctrl = None
            self.name = "legacy_controller_unavailable"
            self.import_error = exc
            return

        # controllers 已迁移到 algo.controllers.legacy_followstop.controllers
        self._legacy = legacy_ctrl
        self.cfg = cfg
        self.ctrl = legacy_ctrl.build_default_controller(
            lateral_mode=cfg.lateral_mode,
            policy_mode=cfg.policy_mode,
            controller_mode=cfg.controller_mode,
            agent_type=cfg.agent_type,
            takeover_dist_m=cfg.takeover_dist_m,
            blend_time_s=cfg.blend_time_s,
            carla_world=world,
            carla_map=carla_map,
            ego_vehicle=ego,
            front_vehicle=front,
        )
        self.name = getattr(self.ctrl, "name", "legacy_controller")

    def reset(self):
        if hasattr(self.ctrl, "reset"):
            self.ctrl.reset()

    def step(
        self,
        t: float,
        dt: float,
        world: Any,
        carla_map: Any,
        ego: Any,
        front: Any,
    ) -> ControlCommand:
        legacy = self._legacy
        if legacy is None or self.ctrl is None:
            raise RuntimeError("LegacyFollowStopController requires a full CARLA PythonAPI runtime")
        v = legacy.speed_mps(ego)
        state = legacy.ControlState(
            t=t,
            dt=dt,
            ego=ego,
            front=front,
            world=world,
            carla_map=carla_map,
            v=v,
            d=0.0,
        )
        veh_ctrl = self.ctrl.step(state)
        cmd = ControlCommand(
            throttle=veh_ctrl.throttle,
            brake=veh_ctrl.brake,
            steer=veh_ctrl.steer,
            reverse=veh_ctrl.reverse,
            hand_brake=veh_ctrl.hand_brake,
            manual_gear_shift=veh_ctrl.manual_gear_shift,
            gear=veh_ctrl.gear if hasattr(veh_ctrl, "gear") else 0,
            meta={"last_debug": getattr(self.ctrl, "last_debug", {}), "name": self.name},
        )
        return cmd


class CarlaLegacyControllerAdapter(LegacyFollowStopController):
    """Compatibility name for CARLA-object based legacy controller path."""
