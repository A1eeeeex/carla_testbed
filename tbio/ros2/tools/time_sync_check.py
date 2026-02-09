from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Dict, List

import yaml

try:
    import rclpy
    from rclpy.node import Node
    from rclpy.any_msg import AnyMsg
    from rosgraph_msgs.msg import Clock
except Exception:
    rclpy = None
    Node = None
    Clock = None
    AnyMsg = None


def load_contract(path: Path) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def main():
    ap = argparse.ArgumentParser(description="Check sim_time alignment between /clock and headers")
    ap.add_argument("--contract", type=Path, default=Path("io/contract/canon_ros2.yaml"))
    ap.add_argument("--sample", type=int, default=5, help="number of messages per topic")
    args = ap.parse_args()

    if rclpy is None:
        raise SystemExit("rclpy not available")

    contract = load_contract(args.contract)
    slots = contract.get("slots", {}) or {}
    topics = [spec.get("topic") for spec in slots.values() if spec.get("topic")]
    if not topics:
        raise SystemExit("no topics in contract")

    rclpy.init(args=None)
    node = rclpy.create_node("io_time_sync_check")

    clock_stamps: List[float] = []
    if Clock:
        def _clock_cb(msg):
            clock_stamps.append(msg.clock.sec + msg.clock.nanosec * 1e-9)
        node.create_subscription(Clock, "/clock", _clock_cb, 10)

    samples: Dict[str, List[float]] = {t: [] for t in topics}

    def _make_cb(topic_name: str):
        def _cb(msg: AnyMsg):
            hdr = getattr(msg, "header", None) or getattr(msg, "hdr", None)
            if hdr:
                stamp = hdr.stamp.sec + hdr.stamp.nanosec * 1e-9
                samples[topic_name].append(stamp)
        return _cb

    subs = []
    for t in topics:
        subs.append(node.create_subscription(AnyMsg, t, _make_cb(t), 10))

    end = time.time() + 5.0
    while time.time() < end and any(len(v) < args.sample for v in samples.values()):
        rclpy.spin_once(node, timeout_sec=0.2)

    for sub in subs:
        node.destroy_subscription(sub)
    rclpy.try_shutdown()

    if not clock_stamps:
        print("[time_sync] no /clock samples")
        return
    clock_latest = clock_stamps[-1]
    for topic, vals in samples.items():
        if not vals:
            print(f"[time_sync] {topic}: no samples")
            continue
        drift = max(abs(clock_latest - v) for v in vals)
        print(f"[time_sync] {topic}: max drift to /clock = {drift:.3f}s")


if __name__ == "__main__":
    main()
