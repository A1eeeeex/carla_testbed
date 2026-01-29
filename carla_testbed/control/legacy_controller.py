from __future__ import annotations

import sys
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import carla

from carla_testbed.schemas import ControlCommand
from .base import Controller


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
        # Locate CARLA root containing code/followstop/controllers.py
        root_path = root
        env_root = os.environ.get("CARLA_ROOT")
        candidates = []
        if env_root:
            candidates.append(Path(env_root))
        if root_path:
            candidates.append(root_path)
        candidates.append(Path("/home/ubuntu/CARLA_0.9.16"))
        candidates.extend(Path(__file__).resolve().parents)
        candidates.append(Path.cwd())

        root_path_resolved = None
        for p in candidates:
            if p and (p / "code" / "followstop" / "controllers.py").exists():
                root_path_resolved = p
                break
        if root_path_resolved is None:
            raise RuntimeError("Cannot locate repository root containing code/followstop/controllers.py")

        # controllers 已搬迁到 algo/controllers/legacy_followstop；兼容旧路径 code/followstop
        new_ctrl_path = Path(__file__).resolve().parents[2] / "algo" / "controllers" / "legacy_followstop"
        if new_ctrl_path.exists() and str(new_ctrl_path) not in sys.path:
            sys.path.insert(0, str(new_ctrl_path))
        followstop_dir = root_path_resolved / "code" / "followstop"
        if followstop_dir.exists() and str(followstop_dir) not in sys.path:
            sys.path.insert(0, str(followstop_dir))
        try:
            import controllers as legacy_ctrl  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Failed to import legacy controllers (checked {new_ctrl_path} and {followstop_dir}): {e}") from e

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
