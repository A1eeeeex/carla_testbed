from __future__ import annotations

import argparse
import json
from pathlib import Path

import rclpy
from rclpy.node import Node
from rclpy.any_msg import AnyMsg
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from rosidl_runtime_py import message_to_ordereddict


class TopicProbe(Node):
    def __init__(self, topics, max_msgs, out_path: Path):
        super().__init__('topic_probe')
        self.targets = topics
        self.max_msgs = max_msgs
        self.out_path = out_path
        self.stats = {t: {"count": 0} for t in topics}
        qos = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=5,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )
        for t in topics:
            self.create_subscription(AnyMsg, t, self._cb_factory(t), qos)
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self.get_logger().info(f"Probing topics: {topics}")

    def _cb_factory(self, topic):
        def _cb(msg: AnyMsg):
            st = self.stats[topic]
            st["count"] += 1
            if "first" not in st:
                st["first"] = self.get_clock().now().nanoseconds / 1e9
            st["last"] = self.get_clock().now().nanoseconds / 1e9
            if st["count"] <= 3:
                st.setdefault("samples", []).append(message_to_ordereddict(msg))
            if self.max_msgs and st["count"] >= self.max_msgs:
                # check if all reached max
                if all((self.max_msgs is None) or (v["count"] >= self.max_msgs) for v in self.stats.values()):
                    self._finish()
        return _cb

    def _finish(self):
        with self.out_path.open('w') as f:
            json.dump(self.stats, f, indent=2)
        rclpy.shutdown()


def main():
    ap = argparse.ArgumentParser(description="Probe topics for data presence")
    ap.add_argument('--topics', nargs='+', required=True)
    ap.add_argument('--max-msgs', type=int, default=5)
    ap.add_argument('--out', type=Path, required=True)
    args = ap.parse_args()
    rclpy.init()
    node = TopicProbe(args.topics, args.max_msgs, args.out)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node._finish()


if __name__ == '__main__':
    main()
