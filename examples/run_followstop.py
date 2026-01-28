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
from typing import List, Optional
import yaml


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
from carla_testbed.record import RecordManager, RecordOptions
from carla_testbed.record.rviz.launcher import RvizLauncher
from carla_testbed.record.ros2_bag import Ros2BagRecorder


def _dedup(seq):
    seen = set()
    out = []
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def build_ros2_bag_topics(
    rig_final: dict,
    ego_id: str,
    camera_suffix: str = "image",
    lidar_suffix: str = "point_cloud",
    radar_suffix: str = "point_cloud",
    include_tf: bool = True,
    include_clock: bool = True,
    auto_topics: bool = True,
    explicit_topics: Optional[List[str]] = None,
    extra_topics: Optional[List[str]] = None,
) -> List[str]:
    topics = []
    if auto_topics and rig_final:
        for sensor in rig_final.get("sensors", []) or []:
            if not sensor.get("enabled", True):
                continue
            sid = sensor.get("id")
            bp = sensor.get("blueprint", "")
            prefix = f"/carla/{ego_id}/{sid}"
            if bp.startswith("sensor.camera"):
                topics.append(f"{prefix}/{camera_suffix}")
                topics.append(f"{prefix}/camera_info")
            elif bp.startswith("sensor.lidar"):
                topics.append(f"{prefix}/{lidar_suffix}")
            elif "sensor.other.imu" in bp:
                topics.append(f"{prefix}/imu")
            elif "sensor.other.gnss" in bp:
                topics.append(f"{prefix}/gnss")
            elif "sensor.other.radar" in bp:
                topics.append(f"{prefix}/{radar_suffix}")
        # 安全兜底：即便 rig 未声明，也默认尝试录 imu/gnss 话题（原生接口常见命名）
        topics.append(f"/carla/{ego_id}/imu")
        topics.append(f"/carla/{ego_id}/gnss")
    if explicit_topics:
        topics.extend([t.strip() for t in explicit_topics if t.strip()])
    if extra_topics:
        topics.extend([t.strip() for t in extra_topics if t.strip()])
    # include_tf/clock handled by recorder flags; no need to insert here unless user specified explicitly
    return _dedup(topics)


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
    ap.add_argument("--lateral-mode", default="dummy")
    ap.add_argument("--policy-mode", default="acc")
    ap.add_argument("--agent-type", default="basic")
    ap.add_argument("--takeover-dist", type=float, default=200.0)
    ap.add_argument("--blend-time", type=float, default=1.5)
    ap.add_argument("--rig", default="minimal", choices=["minimal", "apollo_like", "perception_lidar", "perception_camera", "fullstack"])
    ap.add_argument("--rig-file", type=str, default=None, help="自定义 rig yaml/json 路径")
    ap.add_argument("--rig-override", action="append", default=[], help="rig 覆盖，格式 key=value，支持点路径")
    ap.add_argument("--enable-fail-capture", action="store_true", help="失败时抓取失败窗口 HUD 视频")
    ap.add_argument("--enable-ros2-native", action="store_true", help="启用 CARLA 原生 ROS2 发布（/carla/<ego>/<sensor>/...）")
    ap.add_argument("--ros-invert-tf", dest="ros_invert_tf", action="store_true", default=True, help="ROS2 模式下对 y/pitch/yaw 取反（默认开启以兼容 CARLA 示例）")
    ap.add_argument("--ros-keep-tf", dest="ros_invert_tf", action="store_false", help="禁用取反，直接使用 rig 坐标")
    ap.add_argument("--ego-id", default="hero", help="ego role_name/ros_name（默认 hero）")
    ap.add_argument("--enable-rviz", action="store_true", help="在 ROS2 原生模式下启动 RViz 可视化（默认 docker）")
    ap.add_argument("--rviz-mode", choices=["docker", "local"], default="docker", help="RViz 运行方式，默认 docker（local 暂作占位）")
    ap.add_argument("--rviz-domain", type=int, default=0, help="ROS_DOMAIN_ID（默认 0）")
    ap.add_argument("--rviz-ego", type=str, default=None, help="RViz 订阅使用的 ego 名称（默认跟随 --ego-id）")
    ap.add_argument("--rviz-camera-image-suffix", type=str, default="image", help="相机话题后缀（默认 image）")
    ap.add_argument("--rviz-lidar-cloud-suffix", type=str, default="point_cloud", help="激光雷达话题后缀（默认 point_cloud）")
    ap.add_argument("--rviz-docker-image", type=str, default="carla_testbed_rviz:humble", help="RViz docker 镜像名（默认 carla_testbed_rviz:humble）")
    # ROS2 bag
    ap.add_argument("--enable-ros2-bag", action="store_true", help="在 ROS2 原生模式下录制 rosbag2")
    ap.add_argument("--ros2-bag-out", type=Path, default=None, help="rosbag 输出目录/前缀（默认 runs/<run>/ros2_bag/bag）")
    ap.add_argument("--ros2-bag-storage", choices=["sqlite3", "mcap"], default="sqlite3", help="rosbag2 存储后端")
    ap.add_argument("--ros2-bag-compress", choices=["none", "zstd"], default="none", help="压缩方式（需相关插件支持）")
    ap.add_argument("--ros2-bag-max-size-mb", type=int, default=None, help="单 bag 最大尺寸（MB），超出则切分")
    ap.add_argument("--ros2-bag-max-duration-s", type=int, default=None, help="单 bag 最长时长（秒），超出则切分")
    ap.add_argument("--ros2-bag-include-tf", action="store_true", default=True, help="录制 /tf /tf_static")
    ap.add_argument("--ros2-bag-no-tf", dest="ros2_bag_include_tf", action="store_false", help="不录 /tf /tf_static")
    ap.add_argument("--ros2-bag-include-clock", action="store_true", default=True, help="录制 /clock")
    ap.add_argument("--ros2-bag-no-clock", dest="ros2_bag_include_clock", action="store_false", help="不录 /clock")
    ap.add_argument("--ros2-bag-topics", type=str, default=None, help="显式 topic 列表，逗号分隔")
    ap.add_argument("--ros2-bag-auto-topics", action="store_true", default=True, help="根据 rig 自动推导 topic")
    ap.add_argument("--ros2-bag-no-auto-topics", dest="ros2_bag_auto_topics", action="store_false", help="禁用自动推导 topic")
    ap.add_argument("--ros2-bag-extra-topics", type=str, default=None, help="追加录制的话题，逗号分隔")
    ap.add_argument("--ros2-bag-camera-image-suffix", type=str, default="image", help="自动 camera topic 后缀（默认 image）")
    ap.add_argument("--ros2-bag-lidar-cloud-suffix", type=str, default="point_cloud", help="自动 lidar topic 后缀（默认 point_cloud）")
    ap.add_argument("--ros2-bag-radar-cloud-suffix", type=str, default="point_cloud", help="自动 radar topic 后缀（默认 point_cloud）")
    # Recording modes (new)
    ap.add_argument("--record", action="append", choices=["dual_cam", "hud", "sensor_demo"], help="录制/渲染模式（可多次传递）")
    ap.add_argument("--record-output", type=Path, default=None, help="录制输出目录（默认 run_dir/video）")
    ap.add_argument("--record-fps", type=float, default=None, help="录制输出 fps（默认取仿真 1/dt）")
    ap.add_argument("--record-resolution", type=str, default="1920x1080", help="录制分辨率，如 1920x1080")
    ap.add_argument("--record-chase-distance", type=float, default=8.0)
    ap.add_argument("--record-chase-height", type=float, default=3.0)
    ap.add_argument("--record-chase-pitch", type=float, default=-15.0)
    ap.add_argument("--record-max-lidar-points", type=int, default=10000)
    ap.add_argument("--record-keep-frames", action="store_true", help="保留中间帧 png")
    ap.add_argument("--record-no-lidar", action="store_true", help="sensor_demo 渲染时跳过 lidar overlay")
    ap.add_argument("--record-no-radar", action="store_true", help="sensor_demo 渲染时跳过 radar overlay")
    ap.add_argument("--record-no-hud", action="store_true", help="跳过 HUD 叠加（sensor_demo/hud 模式）")
    # Deprecated
    ap.add_argument("--record-demo", action="store_true", help="(deprecated) 录制双相机 demo")
    ap.add_argument("--make-hud", action="store_true", help="(deprecated) 生成 HUD overlay")
    args = ap.parse_args()

    if args.enable_rviz and not args.enable_ros2_native:
        print("[ERROR] --enable-rviz 仅在 --enable-ros2-native 模式下可用")
        sys.exit(1)
    if args.enable_ros2_bag and not args.enable_ros2_native:
        print("[ERROR] --enable-ros2-bag 仅支持原生 ROS2 发布模式，请先加 --enable-ros2-native")
        sys.exit(1)
    rviz_ego = args.rviz_ego or args.ego_id

    default_out = Path(__file__).resolve().parents[1] / "runs"
    # Parse resolution
    def _parse_res(text: str):
        if isinstance(text, (list, tuple)) and len(text) == 2:
            return int(text[0]), int(text[1])
        if "x" in text:
            w, h = text.lower().split("x", 1)
            return int(w), int(h)
        return 1920, 1080

    record_modes: List[str] = []
    if args.record:
        record_modes.extend(args.record)
    if args.record_demo:
        print("[WARN] --record-demo 已弃用，请改用 --record dual_cam")
        record_modes.append("dual_cam")
    if args.make_hud:
        print("[WARN] --make-hud 已弃用，请改用 --record hud")
        record_modes.append("hud")
    # deduplicate
    record_modes = list(dict.fromkeys(record_modes))
    record_resolution = _parse_res(args.record_resolution)

    cfg = HarnessConfig(
        town=args.town,
        max_steps=args.ticks,
        out_dir=default_out,
        enable_ros2_native=args.enable_ros2_native,
        ros_invert_tf=args.ros_invert_tf,
        ego_id=args.ego_id,
        record_modes=record_modes,
        record_output=args.record_output,
        record_fps=args.record_fps,
        record_resolution=record_resolution,
        record_chase_distance=args.record_chase_distance,
        record_chase_height=args.record_chase_height,
        record_chase_pitch=args.record_chase_pitch,
        record_max_lidar_points=args.record_max_lidar_points,
        record_keep_frames=args.record_keep_frames,
        record_no_lidar=args.record_no_lidar,
        record_no_radar=args.record_no_radar,
        record_no_hud=args.record_no_hud,
        enable_ros2_bag=args.enable_ros2_bag,
        ros2_bag_out=args.ros2_bag_out,
        ros2_bag_storage=args.ros2_bag_storage,
        ros2_bag_compress=args.ros2_bag_compress,
        ros2_bag_max_size_mb=args.ros2_bag_max_size_mb,
        ros2_bag_max_duration_s=args.ros2_bag_max_duration_s,
        ros2_bag_include_tf=args.ros2_bag_include_tf,
        ros2_bag_include_clock=args.ros2_bag_include_clock,
        ros2_bag_topics=args.ros2_bag_topics.split(",") if args.ros2_bag_topics else None,
        ros2_bag_extra_topics=args.ros2_bag_extra_topics.split(",") if args.ros2_bag_extra_topics else None,
        ros2_bag_auto_topics=args.ros2_bag_auto_topics,
        ros2_bag_camera_image_suffix=args.ros2_bag_camera_image_suffix,
        ros2_bag_lidar_cloud_suffix=args.ros2_bag_lidar_cloud_suffix,
        ros2_bag_radar_cloud_suffix=args.ros2_bag_radar_cloud_suffix,
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

    original_settings = world.get_settings()
    if cfg.synchronous_mode:
        original_settings = configure_synchronous_mode(world, cfg.dt)
    bp_lib = world.get_blueprint_library()

    scenario = FollowStopScenario(FollowStopConfig(front_idx=args.front_idx, ego_idx=args.ego_idx, ego_id=args.ego_id))
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
        rig_label = args.rig if not args.rig_file else Path(args.rig_file).stem
        sensor_specs, events_cfg = rig_to_specs(rig_final)
        rviz_launcher = None
        if args.enable_ros2_native and args.enable_rviz:
            rviz_rig_path = out_run_dir / "config" / f"{rig_label}_rviz.yaml"
            rviz_rig_path.parent.mkdir(parents=True, exist_ok=True)
            rviz_rig_path.write_text(yaml.safe_dump(rig_final))
            rviz_launcher = RvizLauncher(
                rig_path=rviz_rig_path,
                ego_id=rviz_ego,
                domain_id=args.rviz_domain,
                mode=args.rviz_mode,
                docker_image=args.rviz_docker_image,
                camera_suffix=args.rviz_camera_image_suffix,
                lidar_suffix=args.rviz_lidar_cloud_suffix,
            )
        bag_cfg = None
        if args.enable_ros2_native and args.enable_ros2_bag:
            bag_out = args.ros2_bag_out or (out_run_dir / "ros2_bag" / "bag")
            topics = []
            explicit = args.ros2_bag_topics.split(",") if args.ros2_bag_topics else None
            extras = args.ros2_bag_extra_topics.split(",") if args.ros2_bag_extra_topics else None
            topics = build_ros2_bag_topics(
                rig_final=rig_final,
                ego_id=args.ego_id,
                camera_suffix=args.ros2_bag_camera_image_suffix,
                lidar_suffix=args.ros2_bag_lidar_cloud_suffix,
                radar_suffix=args.ros2_bag_radar_cloud_suffix,
                include_tf=args.ros2_bag_include_tf,
                include_clock=args.ros2_bag_include_clock,
                auto_topics=args.ros2_bag_auto_topics,
                explicit_topics=explicit,
                extra_topics=extras,
            )
            print(f"[ROS2 bag] topics: {topics}")
            bag_cfg = {
                "out_path": bag_out,
                "topics": topics,
                "storage": args.ros2_bag_storage,
                "compress": args.ros2_bag_compress,
                "include_tf": args.ros2_bag_include_tf,
                "include_clock": args.ros2_bag_include_clock,
                "max_size_mb": args.ros2_bag_max_size_mb,
                "max_duration_s": args.ros2_bag_max_duration_s,
                "env": None,
                "log_path": out_run_dir / "logs" / "ros2_bag.log",
            }
        # Prepare record manager (run_dir not created until run)
        record_opts = RecordOptions(
            modes=record_modes,
            output_dir=args.record_output or None,
            fps=args.record_fps,
            resolution=record_resolution,
            chase_distance=args.record_chase_distance,
            chase_height=args.record_chase_height,
            chase_pitch=args.record_chase_pitch,
            max_lidar_points=args.record_max_lidar_points,
            keep_frames=args.record_keep_frames,
            skip_lidar=args.record_no_lidar,
            skip_radar=args.record_no_radar,
            skip_hud=args.record_no_hud,
        )
        record_mgr = RecordManager(
            run_dir=out_run_dir,
            rig_resolved=rig_final,
            config_paths={},  # filled inside harness via cfg outputs
            opts=record_opts,
        ) if record_modes else None

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
            events_cfg=events_cfg,
            enable_fail_capture=args.enable_fail_capture,
            record_manager=record_mgr,
            client=client,
            rviz_launcher=rviz_launcher,
            bag_recorder_cfg=bag_cfg,
        )
        print(f"Run finished: success={summary['success']} fail_reason={summary['fail_reason']} collisions={summary['collision_count']}")
    finally:
        restore_settings(world, original_settings)
        if actors is not None:
            scenario.destroy()
        print("Settings restored, exiting.")


if __name__ == "__main__":
    main()
