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
except Exception:
    rclpy = None
    Node = None
    Clock = None


def load_contract(path: Path) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def check_topics(node: Node, contract: Dict) -> List[str]:
    existing = {name for name, _ in node.get_topic_names_and_types()}
    missing = []
    for slot, spec in (contract.get("slots", {}) or {}).items():
        topic = spec.get("topic")
        if topic and topic not in existing:
            missing.append(f"{slot}:{topic}")
    return missing


def check_clock(node: Node, timeout: float = 3.0) -> bool:
    if Clock is None:
        return False
    stamps: List[float] = []

    def _cb(msg):
        ts = msg.clock.sec + msg.clock.nanosec * 1e-9
        stamps.append(ts)

    sub = node.create_subscription(Clock, "/clock", _cb, 10)
    end = time.time() + timeout
    while time.time() < end and len(stamps) < 2:
        rclpy.spin_once(node, timeout_sec=0.2)
    node.destroy_subscription(sub)
    return len(stamps) >= 2 and stamps[-1] > stamps[0]


def main():
    ap = argparse.ArgumentParser(description="Check ROS2 topics against contract and /clock health")
    ap.add_argument("--contract", type=Path, default=Path("io/contract/canon_ros2.yaml"))
    ap.add_argument("--timeout", type=float, default=3.0)
    args = ap.parse_args()

    if rclpy is None:
        raise SystemExit("rclpy not available")
    contract = load_contract(args.contract)
    rclpy.init(args=None)
    node = rclpy.create_node("io_topic_inspector")

    missing = check_topics(node, contract)
    clock_ok = check_clock(node, timeout=args.timeout)

    if missing:
        print("[inspect] missing topics: " + ", ".join(missing))
    if not clock_ok:
        print("[inspect] /clock missing or not moving")
    if not missing and clock_ok:
        print("[inspect] all required topics present")
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
