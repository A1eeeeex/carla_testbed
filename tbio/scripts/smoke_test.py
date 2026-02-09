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


def _load_contract(path: Path) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def _check_clock(node: Node, timeout: float = 5.0) -> bool:
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
    if len(stamps) < 2:
        print("[smoke] /clock missing or not updating")
        return False
    if stamps[-1] <= stamps[0]:
        print("[smoke] /clock not increasing")
        return False
    return True


def _check_topics(node: Node, contract: Dict) -> List[str]:
    missing = []
    topics = {name for name, _ in node.get_topic_names_and_types()}
    for slot, spec in (contract.get("slots", {}) or {}).items():
        topic = spec.get("topic")
        direction = spec.get("direction", "publish")
        if not topic or direction == "internal":
            continue
        if topic not in topics:
            missing.append(f"{slot}:{topic}")
    return missing


def run_smoke_test(contract_path: Path, timeout: float = 5.0) -> bool:
    if rclpy is None:
        print("[smoke] rclpy not available; skip")
        return False
    contract = _load_contract(contract_path)
    rclpy.init(args=None)
    node = rclpy.create_node("io_smoke_test")
    ok = _check_clock(node, timeout=timeout)
    missing = _check_topics(node, contract)
    if missing:
        print("[smoke] missing topics: " + ", ".join(missing))
        ok = False
    if ok:
        print("[smoke] OK")
    rclpy.try_shutdown()
    return ok


def main():
    ap = argparse.ArgumentParser(description="Minimal ROS2 smoke test against contract")
    ap.add_argument("--contract", type=Path, required=True, help="Path to canon_ros2.yaml")
    ap.add_argument("--timeout", type=float, default=5.0)
    args = ap.parse_args()
    run_smoke_test(args.contract, timeout=args.timeout)


if __name__ == "__main__":
    main()
