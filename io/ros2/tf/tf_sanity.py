from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import yaml

try:
    import rclpy
    from rclpy.node import Node
    from tf2_msgs.msg import TFMessage
except Exception:
    rclpy = None
    Node = None
    TFMessage = None


def load_required(path: Path) -> List[str]:
    with open(path, "r") as f:
        data: Dict = yaml.safe_load(f) or {}
    return data.get("required_tf", [])


def main():
    ap = argparse.ArgumentParser(description="Check TF frames existence against contract frames.yaml")
    ap.add_argument("--frames", type=Path, default=Path("io/contract/frames.yaml"))
    ap.add_argument("--timeout", type=float, default=3.0)
    args = ap.parse_args()

    if rclpy is None:
        raise SystemExit("rclpy not available")
    required = set(load_required(args.frames))
    if not required:
        raise SystemExit("no required_tf in frames.yaml")

    rclpy.init(args=None)
    node = rclpy.create_node("io_tf_sanity")
    seen: set[str] = set()

    def _cb(msg: TFMessage):
        for t in msg.transforms:
            seen.add(t.header.frame_id)
            seen.add(t.child_frame_id)

    sub = node.create_subscription(TFMessage, "/tf", _cb, 10)
    end = node.get_clock().now().nanoseconds / 1e9 + args.timeout
    while node.get_clock().now().nanoseconds / 1e9 < end:
        rclpy.spin_once(node, timeout_sec=0.2)
    node.destroy_subscription(sub)
    rclpy.try_shutdown()

    missing = required - seen
    if missing:
        print("[tf] missing frames: " + ", ".join(sorted(missing)))
    else:
        print("[tf] all required frames observed")


if __name__ == "__main__":
    main()
