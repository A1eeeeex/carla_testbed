#!/usr/bin/env python3
"""
Step0+Step1 demo: connect to CARLA, enable synchronous mode, tick a few frames.
This is a placeholder; follow-stop scenario/control will be migrated in later steps.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


def _resolve_carla_root(cli_root: Path) -> Path:
    env_root = os.environ.get("CARLA_ROOT")
    candidates = []
    if env_root:
        candidates.append(Path(env_root))
    if cli_root:
        candidates.append(cli_root)
    candidates.append(Path("/home/ubuntu/CARLA_0.9.16"))
    for c in candidates:
        if c and (c / "code" / "followstop" / "controllers.py").exists():
            return c
    for p in Path(__file__).resolve().parents:
        if (p / "code" / "followstop" / "controllers.py").exists():
            return p
    return cli_root


def _inject_paths(testbed_root: Path, carla_root: Path):
    for p in [
        testbed_root,
        testbed_root / "carla_testbed",
        carla_root / "code" / "followstop",
        carla_root / "PythonAPI",
        carla_root / "PythonAPI" / "carla",
    ]:
        if p and p.exists() and str(p) not in sys.path:
            sys.path.insert(0, str(p))


TESTBED_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CARLA_ROOT = _resolve_carla_root(Path(os.environ.get("CARLA_ROOT", "/home/ubuntu/CARLA_0.9.16")))
_inject_paths(TESTBED_ROOT, DEFAULT_CARLA_ROOT)

import carla

from carla_testbed.config import HarnessConfig
from carla_testbed.control import LegacyControllerConfig
from carla_testbed.config.rig_loader import apply_overrides, load_rig_file, load_rig_preset, rig_to_specs
from carla_testbed.runner import TestHarness
from carla_testbed.scenarios import FollowStopConfig, FollowStopScenario
from carla_testbed.sim import CarlaClientManager, configure_synchronous_mode, restore_settings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--town", default="Town01")
    ap.add_argument("--ticks", type=int, default=10)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=2000)
    ap.add_argument("--carla-root", type=Path, default=DEFAULT_CARLA_ROOT, help="CARLA根目录（含 code/followstop/controllers.py）")
    ap.add_argument("--front-idx", type=int, default=210)
    ap.add_argument("--ego-idx", type=int, default=120)
    ap.add_argument("--controller", default="composite")
    ap.add_argument("--lateral-mode", default="pure_pursuit")
    ap.add_argument("--policy-mode", default="acc")
    ap.add_argument("--agent-type", default="basic")
    ap.add_argument("--takeover-dist", type=float, default=200.0)
    ap.add_argument("--blend-time", type=float, default=1.5)
    ap.add_argument("--rig", default="minimal", choices=["minimal", "apollo_like", "perception_lidar", "perception_camera", "fullstack"])
    ap.add_argument("--rig-file", type=str, default=None, help="自定义 rig yaml/json 路径")
    ap.add_argument("--rig-override", action="append", default=[], help="rig 覆盖，格式 key=value，支持点路径")
    ap.add_argument("--enable-fail-capture", action="store_true", help="失败时抓取失败窗口 HUD 视频")
    ap.add_argument("--record-demo", action="store_true", help="录制双相机 demo（png 序列 + 可选 mp4）")
    ap.add_argument("--make-hud", action="store_true", help="生成 HUD overlay mp4（依赖 pillow+ffmpeg）")
    ap.add_argument("--enable-ros2-bridge", action="store_true", help="启用 ROS2 在线桥接（发布传感器数据）")
    ap.add_argument("--ros2-contract", type=Path, default=None, help="自定义 io_contract_ros2.yaml 路径（默认使用 run_dir/config 下生成的文件）")
    args = ap.parse_args()

    default_out = Path(__file__).resolve().parents[1] / "runs"
    cfg = HarnessConfig(
        town=args.town,
        max_steps=args.ticks,
        out_dir=default_out,
        enable_ros2_bridge=args.enable_ros2_bridge,
        ros2_contract_path=args.ros2_contract,
    )
    harness = TestHarness(cfg)

    client_mgr = CarlaClientManager(host=args.host, port=args.port, timeout=30.0, root=args.carla_root)
    client = client_mgr.create_client()

    world = client.get_world()
    try:
        current_town = world.get_map().name.split("/")[-1]
    except Exception:
        current_town = ""
    if current_town != args.town:
        world = client.load_world(args.town)
        time.sleep(1.0)

    original_settings = configure_synchronous_mode(world, cfg.dt)
    bp_lib = world.get_blueprint_library()

    scenario = FollowStopScenario(FollowStopConfig(front_idx=args.front_idx, ego_idx=args.ego_idx))
    actors = None

    try:
        actors = scenario.build(world, world.get_map(), bp_lib)
        ego = actors.ego
        front = actors.front
        print(f"Spawned ego at idx={scenario.cfg.ego_idx}, front at idx={scenario.cfg.front_idx}")

        ctrl_cfg = LegacyControllerConfig(
            lateral_mode=args.lateral_mode,
            policy_mode=args.policy_mode,
            controller_mode=args.controller,
            agent_type=args.agent_type,
            takeover_dist_m=args.takeover_dist,
            blend_time_s=args.blend_time,
        )
        out_run_dir = default_out / f"followstop_{int(time.time())}"
        # rig loading
        preset_dir = Path(__file__).resolve().parents[1] / "configs" / "rigs"
        rig_raw = load_rig_preset(args.rig, preset_dir)
        if args.rig_file:
            rig_raw = load_rig_file(args.rig_file)
        rig_final = apply_overrides(rig_raw, args.rig_override)
        sensor_specs, events_cfg = rig_to_specs(rig_final)
        state, summary = harness.run(
            world=world,
            carla_map=world.get_map(),
            ego=ego,
            front=front,
            controller_cfg=ctrl_cfg,
            out_dir=out_run_dir,
            sensor_specs=sensor_specs,
            rig_raw=rig_raw,
            rig_final=rig_final,
            rig_name=args.rig if not args.rig_file else Path(args.rig_file).name,
            enable_fail_capture=args.enable_fail_capture,
            enable_demo=args.record_demo,
            make_hud=args.make_hud,
        )
        print(f"Run finished: success={summary['success']} fail_reason={summary['fail_reason']} collisions={summary['collision_count']}")
    finally:
        restore_settings(world, original_settings)
        if actors is not None:
            scenario.destroy()
        print("Settings restored, exiting.")


if __name__ == "__main__":
    main()
