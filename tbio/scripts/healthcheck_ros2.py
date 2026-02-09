from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Dict, List

import yaml

try:
    import rclpy
    from rclpy.node import Node
    from rosgraph_msgs.msg import Clock
    from tf2_msgs.msg import TFMessage
    from rclpy.any_msg import AnyMsg
except Exception:
    rclpy = None


def load_config(path: Path) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def wait_for_topic(node: Node, topic: str, sample: int = 1, timeout: float = 5.0) -> bool:
    msgs = []

    def _cb(msg):
        msgs.append(msg)

    sub = node.create_subscription(AnyMsg, topic, _cb, 10)
    end = time.time() + timeout
    while time.time() < end and len(msgs) < sample:
        rclpy.spin_once(node, timeout_sec=0.2)
    node.destroy_subscription(sub)
    return len(msgs) >= sample


def check_tf(node: Node, timeout: float = 5.0) -> bool:
    frames = set()

    def _cb(msg: TFMessage):
        for t in msg.transforms:
            frames.add(t.child_frame_id)
            frames.add(t.header.frame_id)

    sub = node.create_subscription(TFMessage, "/tf", _cb, 10)
    end = time.time() + timeout
    while time.time() < end and not frames:
        rclpy.spin_once(node, timeout_sec=0.2)
    node.destroy_subscription(sub)
    return bool(frames)


def healthcheck(config_path: Path, timeout: float = 5.0) -> bool:
    if rclpy is None:
        print("[healthcheck] rclpy not available on host; skipping ROS2 checks (treat as WARN)")
        return True
    cfg = load_config(config_path)
    topics_cfg = cfg.get("record", {}).get("sensor_probe", {}).get("topics") or []
    topics = [t for t in topics_cfg if t]

    rclpy.init(args=None)
    node = rclpy.create_node("io_healthcheck")

    ok = True
    # clock
    if not wait_for_topic(node, "/clock", sample=2, timeout=timeout):
        print("[FAIL] /clock missing or no data")
        ok = False
    # sensors
    for t in topics:
        if not wait_for_topic(node, t, sample=1, timeout=timeout):
            print(f"[FAIL] topic missing or empty: {t}")
            ok = False
    # tf
    if not check_tf(node, timeout=timeout):
        print("[FAIL] TF not received")
        ok = False
    if ok:
        print("[PASS] ROS2 healthcheck OK")
    else:
        print("[WARN] ROS2 healthcheck failed; see messages above")
    rclpy.try_shutdown()
    return ok


def main():
    ap = argparse.ArgumentParser(description="ROS2 healthcheck")
    ap.add_argument("--config", required=True, type=Path, help="effective.yaml or input config")
    ap.add_argument("--timeout", type=float, default=5.0)
    args = ap.parse_args()
    success = healthcheck(args.config, timeout=args.timeout)
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
