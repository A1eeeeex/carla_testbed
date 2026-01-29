from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Dict

import yaml


def load_yaml(path: Path) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def generate_mapping_and_calib(rig_path: Path, contract_path: Path, out_dir: Path):
    rig = load_yaml(rig_path)
    contract = load_yaml(contract_path)
    slots = contract.get("slots", {}) or {}
    mapping = {}
    calib = {}
    for sensor in rig.get("sensors", []):
        sid = sensor.get("id") or sensor.get("name")
        if not sid:
            continue
        stype = sensor.get("blueprint") or sensor.get("type")
        mapping[sid] = {
            "topic": slots.get(sid, {}).get("topic", f"/carla/ego/{sid}"),
            "carla_sensor": stype,
            "id": sid,
        }
        pose = sensor.get("transform", sensor.get("pose", {})) or {}
        loc = pose if isinstance(pose, dict) else {}
        rot = pose if isinstance(pose, dict) else {}
        if not loc and isinstance(pose, dict):
            loc = pose.get("location", {}) or {}
            rot = pose.get("rotation", {}) or {}
        calib[sid] = {
            "translation": {"x": float(loc.get("x", 0.0)), "y": float(loc.get("y", 0.0)), "z": float(loc.get("z", 0.0))},
            "rotation": {
                "roll": math.radians(float(rot.get("roll", 0.0))),
                "pitch": math.radians(float(rot.get("pitch", 0.0))),
                "yaw": math.radians(float(rot.get("yaw", 0.0))),
            },
        }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "sensor_mapping.yaml").write_text(yaml.safe_dump(mapping, sort_keys=False))
    (out_dir / "sensor_kit_calibration.yaml").write_text(yaml.safe_dump(calib, sort_keys=False))
    return mapping, calib


def generate_qos(contract_path: Path, out_dir: Path):
    contract = load_yaml(contract_path)
    qos = {}
    sensor_slots = ["front_camera", "top_lidar", "imu", "gnss", "odometry"]
    for slot in sensor_slots:
        spec = contract.get("slots", {}).get(slot, {})
        topic = spec.get("topic")
        if not topic:
            continue
        qos[topic] = {
            "reliability": "best_effort" if slot in ["front_camera", "top_lidar"] else "reliable",
            "durability": "volatile",
            "history": "keep_last",
            "depth": 5,
        }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "qos_overrides.yaml").write_text(yaml.safe_dump(qos, sort_keys=False))
    return qos


def generate_frames(contract_path: Path, out_dir: Path):
    frames = load_yaml(contract_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    frames_out = out_dir / "frames.yaml"
    frames_out.write_text(yaml.safe_dump(frames, sort_keys=False))
    static_tf = out_dir / "static_tf.py"
    static_tf.write_text(
        """from __future__ import annotations
import rclpy
from rclpy.node import Node
from tf2_ros.static_transform_broadcaster import StaticTransformBroadcaster
from geometry_msgs.msg import TransformStamped
from pathlib import Path
import yaml

def load_frames(path: Path):
    with open(path, 'r') as f:
        return yaml.safe_load(f) or {}

def main():
    rclpy.init()
    node = Node('static_tf_loader')
    br = StaticTransformBroadcaster(node)
    data = load_frames(Path(__file__).parent / 'frames.yaml')
    transforms = []  # fill with numeric transforms if provided; frames.yaml currently captures topology only.
    br.sendTransform(transforms)
    rclpy.spin(node)

if __name__ == '__main__':
    main()
"""
    )
    return frames


def generate_all(rig_path: Path, contract_path: Path, frames_path: Path, out_dir: Path):
    mapping, calib = generate_mapping_and_calib(rig_path, contract_path, out_dir)
    qos = generate_qos(contract_path, out_dir)
    frames = generate_frames(frames_path, out_dir)
    return {"mapping": mapping, "calibration": calib, "qos": qos, "frames": frames}


def main():
    ap = argparse.ArgumentParser(description="Generate artifacts for autoware mode")
    ap.add_argument("--rig", required=True, type=Path)
    ap.add_argument("--contract", required=True, type=Path)
    ap.add_argument("--frames", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()
    generate_all(args.rig, args.contract, args.frames, args.out)


if __name__ == "__main__":
    main()
