from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import carla

from carla_testbed.schemas import ControlCommand
from .base import Controller
from algo.controllers.legacy_followstop import controllers as legacy_ctrl


@dataclass
class LegacyControllerConfig:
    lateral_mode: str = "dummy"
    policy_mode: str = "acc"
    controller_mode: str = "composite"  # or hybrid_agent_acc
    agent_type: str = "basic"
    takeover_dist_m: float = 200.0
    blend_time_s: float = 1.5


class LegacyFollowStopController(Controller):
    """Adapter around existing code/followstop/controllers.py build_default_controller."""

    def __init__(
        self,
        cfg: LegacyControllerConfig,
        root: Optional[Path] = None,
        world: Optional[carla.World] = None,
        carla_map: Optional[carla.Map] = None,
        ego: Optional[carla.Vehicle] = None,
        front: Optional[carla.Vehicle] = None,
    ):
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
        world: carla.World,
        carla_map: carla.Map,
        ego: carla.Vehicle,
        front: carla.Vehicle,
    ) -> ControlCommand:
        legacy = self._legacy
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
        veh_ctrl: carla.VehicleControl = self.ctrl.step(state)
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
