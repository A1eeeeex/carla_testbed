from __future__ import annotations

import json
import signal
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from tbio.ros2.goal_engage import ComposeRos2Runner, Ros2CommandRunner, ros_env_setup_snippet


def _script_cmd(runner: Ros2CommandRunner, local_script: Path, container_script: str) -> str:
    if isinstance(runner, ComposeRos2Runner):
        return f"python3 {shlex.quote(container_script)}"
    return f"{shlex.quote(sys.executable)} {shlex.quote(str(local_script.resolve()))}"


def start_control_logger(
    runner: Ros2CommandRunner,
    repo_root: Path,
    *,
    topic: str,
    out_jsonl: Path,
    out_log: Path,
    max_msgs: Optional[int],
    force_anymsg: bool = True,
    reliability: str = "best_effort",
) -> subprocess.Popen:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    out_log.parent.mkdir(parents=True, exist_ok=True)
    runner_out = runner.runtime_path(out_jsonl)
    script_cmd = _script_cmd(runner, repo_root / "tbio/ros2/tools/control_logger.py", "/work/tbio/ros2/tools/control_logger.py")
    cmd = f"{script_cmd} --topic {shlex.quote(topic)} --out {shlex.quote(runner_out)}"
    if max_msgs is not None:
        cmd += f" --max-msgs {int(max_msgs)}"
    if force_anymsg:
        cmd += " --force-anymsg"
    if reliability in {"best_effort", "reliable"}:
        cmd += f" --reliability {reliability}"
    cmd = f"{ros_env_setup_snippet()} {cmd}"
    log_fp = out_log.open("w")
    return runner.popen_bash(cmd, stdout=log_fp, stderr=subprocess.STDOUT)


def start_topic_probe(
    runner: Ros2CommandRunner,
    repo_root: Path,
    *,
    topics: Iterable[str],
    out_json: Path,
    out_log: Path,
    max_msgs: int = 5,
    discover_prefixes: Optional[Iterable[str]] = None,
    typed_topics: Optional[Dict[str, str]] = None,
) -> subprocess.Popen:
    topics = [t for t in topics if t]
    if not topics:
        raise ValueError("topic probe requires at least one topic")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_log.parent.mkdir(parents=True, exist_ok=True)
    runner_out = runner.runtime_path(out_json)
    script_cmd = _script_cmd(runner, repo_root / "tbio/ros2/tools/topic_probe.py", "/work/tbio/ros2/tools/topic_probe.py")
    topics_arg = " ".join(shlex.quote(t) for t in topics)
    cmd = f"{script_cmd} --topics {topics_arg} --max-msgs {int(max_msgs)} --out {shlex.quote(runner_out)}"
    prefixes = [p for p in (discover_prefixes or []) if p]
    if prefixes:
        cmd += " --discover-prefixes " + " ".join(shlex.quote(p) for p in prefixes)
    for topic, msg_type in (typed_topics or {}).items():
        if topic and msg_type:
            cmd += f" --typed-topic {shlex.quote(f'{topic}::{msg_type}')}"
    cmd = f"{ros_env_setup_snippet()} {cmd}"
    log_fp = out_log.open("w")
    return runner.popen_bash(cmd, stdout=log_fp, stderr=subprocess.STDOUT)


def stop_process(proc: Optional[subprocess.Popen], timeout_s: float = 5.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=min(2.0, timeout_s))
        return
    except Exception:
        pass
    proc.terminate()
    try:
        proc.wait(timeout=timeout_s)
    except Exception:
        proc.kill()


def infer_ros2_sensor_topics_from_rig(
    rig_final: Optional[Dict],
    ego_id: str,
    *,
    namespace: str = "/carla",
    camera_suffix: str = "image",
    lidar_suffix: str = "point_cloud",
    radar_suffix: str = "point_cloud",
) -> List[str]:
    ns = (namespace or "/carla").strip()
    if not ns.startswith("/"):
        ns = "/" + ns
    ns = ns.rstrip("/") or "/carla"
    topics: List[str] = []
    rig = rig_final or {}
    for sensor in rig.get("sensors", []) or []:
        if not sensor.get("enabled", True):
            continue
        sid = sensor.get("id")
        if not sid:
            continue
        bp = sensor.get("blueprint", "")
        prefixes = [f"{ns}/{ego_id}/{sid}", f"{ns}/{sid}"]
        if bp.startswith("sensor.camera"):
            for prefix in prefixes:
                topics.extend([f"{prefix}/{camera_suffix}", f"{prefix}/camera_info"])
        elif bp.startswith("sensor.lidar"):
            for prefix in prefixes:
                topics.append(f"{prefix}/{lidar_suffix}")
        elif "sensor.other.imu" in bp:
            for prefix in prefixes:
                topics.append(f"{prefix}/imu")
        elif "sensor.other.gnss" in bp:
            for prefix in prefixes:
                topics.append(f"{prefix}/gnss")
        elif "sensor.other.radar" in bp:
            for prefix in prefixes:
                topics.append(f"{prefix}/{radar_suffix}")
    topics.extend(
        [
            f"{ns}/{ego_id}/imu",
            f"{ns}/{ego_id}/gnss",
            f"{ns}/{ego_id}/odom",
            f"{ns}/{ego_id}/objects3d",
            f"{ns}/{ego_id}/objects_markers",
            f"{ns}/{ego_id}/objects_gt_json",
            f"{ns}/imu",
            f"{ns}/gnss",
        ]
    )
    # de-dup while preserving order
    out: List[str] = []
    seen = set()
    for t in topics:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def infer_topic_types(topics: Iterable[str]) -> Dict[str, str]:
    typed: Dict[str, str] = {}
    for topic in topics:
        if not topic:
            continue
        if topic.endswith("/image"):
            typed[topic] = "sensor_msgs/msg/Image"
        elif topic.endswith("/camera_info"):
            typed[topic] = "sensor_msgs/msg/CameraInfo"
        elif topic.endswith("/imu"):
            typed[topic] = "sensor_msgs/msg/Imu"
        elif topic.endswith("/gnss"):
            typed[topic] = "sensor_msgs/msg/NavSatFix"
        elif topic.endswith("/points") or topic.endswith("/point_cloud"):
            typed[topic] = "sensor_msgs/msg/PointCloud2"
        elif topic.endswith("/odom"):
            typed[topic] = "nav_msgs/msg/Odometry"
        elif topic.endswith("/objects3d"):
            typed[topic] = "vision_msgs/msg/Detection3DArray"
        elif topic.endswith("/objects_markers"):
            typed[topic] = "visualization_msgs/msg/MarkerArray"
        elif topic.endswith("/objects_gt_json"):
            typed[topic] = "std_msgs/msg/String"
        elif topic == "/clock":
            typed[topic] = "rosgraph_msgs/msg/Clock"
        elif topic == "/tf":
            typed[topic] = "tf2_msgs/msg/TFMessage"
    return typed


def default_probe_topics(
    *,
    stack: Optional[str],
    ego_id: str,
    rig_final: Optional[Dict],
    control_topic: str,
    namespace: str = "/carla",
    max_sensor_topics: int = 8,
) -> List[str]:
    base = ["/clock", "/tf", control_topic]
    if stack == "autoware":
        return base
    ns = (namespace or "/carla").strip()
    if not ns.startswith("/"):
        ns = "/" + ns
    ns = ns.rstrip("/") or "/carla"
    sensor_topics = infer_ros2_sensor_topics_from_rig(rig_final, ego_id, namespace=ns)
    picked = [t for t in sensor_topics if t.startswith(f"{ns}/")][:max_sensor_topics]
    out: List[str] = []
    seen = set()
    for t in base + picked:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


@dataclass
class ProbeAssessment:
    sensor_probe_ok: bool
    clock_count: int
    tf_count: Optional[int]
    ros2_sensor_topic_count: int
    ros2_sensor_msgs: int
    ros2_sensor_ok: bool


def assess_probe_results(
    probe_path: Path,
    probe_topics: List[str],
    ego_id: str,
    namespace: str = "/carla",
) -> ProbeAssessment:
    sensor_probe_ok = False
    clock_count = 0
    tf_count: Optional[int] = None
    ros2_sensor_topic_count = 0
    ros2_sensor_msgs = 0
    ros2_sensor_ok = False

    if probe_path.exists():
        probe_data = json.loads(probe_path.read_text())
        clock_count = int((probe_data.get("/clock") or {}).get("count", 0))
        tf_entry = probe_data.get("/tf")
        tf_count = None if tf_entry is None else int(tf_entry.get("count", 0))
        sensor_probe_ok = clock_count >= 2 and (tf_count is None or tf_count >= 1)
        ns = (namespace or "/carla").strip()
        if not ns.startswith("/"):
            ns = "/" + ns
        ns = ns.rstrip("/") or "/carla"
        sensor_topics = sorted({t for t in probe_data.keys() if t.startswith(f"{ns}/") and "//" not in t})
        if not sensor_topics:
            sensor_topics = [t for t in probe_topics if t.startswith(f"{ns}/") and "//" not in t]
        ros2_sensor_topic_count = len(sensor_topics)
        ros2_sensor_msgs = sum(int((probe_data.get(t) or {}).get("count", 0)) for t in sensor_topics)
        ros2_sensor_ok = (ros2_sensor_topic_count == 0) or ros2_sensor_msgs > 0

    return ProbeAssessment(
        sensor_probe_ok=sensor_probe_ok,
        clock_count=clock_count,
        tf_count=tf_count,
        ros2_sensor_topic_count=ros2_sensor_topic_count,
        ros2_sensor_msgs=ros2_sensor_msgs,
        ros2_sensor_ok=ros2_sensor_ok,
    )
