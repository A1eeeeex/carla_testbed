from __future__ import annotations

import argparse
import json
import signal
from pathlib import Path

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from rosidl_runtime_py.utilities import get_message


class TopicProbe(Node):
    def __init__(self, topics, max_msgs, out_path: Path, discover_prefixes=None, typed_topics=None):
        super().__init__('topic_probe')
        self.targets = topics
        self.required = set(topics)
        self.discover_prefixes = [p for p in (discover_prefixes or []) if p]
        self.typed_topics = dict(typed_topics or {})
        self.max_msgs = max_msgs
        self.out_path = out_path
        self.stats = {t: {"count": 0} for t in topics}
        self._subs = {}
        self._pending = set(topics)
        self._finished = False
        self._invalid_discovered = set()
        self._unsupported_types = {}
        self._qos = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=5,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )
        self._bind_timer = self.create_timer(0.5, self._bind_pending_topics)
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self._flush_timer = self.create_timer(1.0, self._flush)
        self.get_logger().info(f"Probing topics: {topics}")
        self._subscribe_typed_topics()

    def _subscribe_typed_topics(self):
        for topic, msg_type in self.typed_topics.items():
            if topic in self._subs:
                continue
            try:
                msg_cls = get_message(msg_type)
                self._subs[topic] = self.create_subscription(msg_cls, topic, self._cb_factory(topic), self._qos)
                self.stats.setdefault(topic, {"count": 0})
                self.stats[topic]["type"] = msg_type
                if topic in self._pending:
                    self._pending.remove(topic)
                self.get_logger().info(f"Pre-subscribed {topic} as {msg_type}")
            except Exception as exc:
                self.get_logger().warn(f"failed pre-subscribe {topic} type={msg_type}: {exc}")

    def _bind_pending_topics(self):
        graph = dict(self.get_topic_names_and_types())
        if self.discover_prefixes:
            for topic in graph.keys():
                if any(topic.startswith(prefix) for prefix in self.discover_prefixes):
                    if "//" in topic:
                        self._invalid_discovered.add(topic)
                        continue
                    if topic not in self.stats and topic not in self._subs:
                        self.stats[topic] = {"count": 0, "discovered": True}
                        self._pending.add(topic)

        if not self._pending:
            return
        for topic in list(self._pending):
            types = graph.get(topic) or []
            if not types:
                continue
            msg_type = types[0]
            try:
                msg_cls = get_message(msg_type)
                self._subs[topic] = self.create_subscription(msg_cls, topic, self._cb_factory(topic), self._qos)
                self.stats[topic]["type"] = msg_type
                self._pending.remove(topic)
                self.get_logger().info(f"Subscribed {topic} as {msg_type}")
            except Exception as exc:
                self.get_logger().warn(f"failed to subscribe {topic} type={msg_type}: {exc}")
                if "Invalid topic name" in str(exc):
                    self._pending.remove(topic)
                    self._invalid_discovered.add(topic)
                    continue
                if isinstance(exc, ModuleNotFoundError) or "No module named" in str(exc):
                    self._pending.remove(topic)
                    self._unsupported_types[topic] = msg_type

    def _cb_factory(self, topic):
        def _cb(msg):
            st = self.stats[topic]
            st["count"] += 1
            if "first" not in st:
                st["first"] = self.get_clock().now().nanoseconds / 1e9
            st["last"] = self.get_clock().now().nanoseconds / 1e9
            if st["count"] <= 3:
                sample = {
                    "repr": repr(msg)[:300],
                }
                st.setdefault("samples", []).append(sample)
            if self.max_msgs and st["count"] >= self.max_msgs:
                # stop when all required topics have at least max_msgs
                required_stats = [self.stats[t] for t in self.required if t in self.stats]
                if required_stats and all(v["count"] >= self.max_msgs for v in required_stats):
                    self._finish()
        return _cb

    def _flush(self):
        payload = dict(self.stats)
        meta = {}
        if self._invalid_discovered:
            meta["invalid_discovered_topics"] = sorted(self._invalid_discovered)
        if self._unsupported_types:
            meta["unsupported_type_topics"] = dict(sorted(self._unsupported_types.items()))
        if meta:
            payload["_meta"] = meta
        with self.out_path.open('w') as f:
            json.dump(payload, f, indent=2)

    def _finish(self):
        if self._finished:
            return
        self._finished = True
        try:
            self._flush()
        except Exception as exc:
            self.get_logger().error(f"flush failed: {exc}")
        if rclpy.ok():
            rclpy.shutdown()


def main():
    ap = argparse.ArgumentParser(description="Probe topics for data presence")
    ap.add_argument('--topics', nargs='+', required=True)
    ap.add_argument('--max-msgs', type=int, default=5)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--discover-prefixes', nargs='*', default=[])
    ap.add_argument('--typed-topic', action='append', default=[], help="topic::pkg/msg/Type")
    args = ap.parse_args()
    typed_topics = {}
    for entry in args.typed_topic:
        if "::" not in entry:
            continue
        topic, msg_type = entry.split("::", 1)
        topic = topic.strip()
        msg_type = msg_type.strip()
        if topic and msg_type:
            typed_topics[topic] = msg_type
    rclpy.init()
    node = TopicProbe(
        args.topics,
        args.max_msgs,
        args.out,
        discover_prefixes=args.discover_prefixes,
        typed_topics=typed_topics,
    )
    def _handle_signal(signum, _frame):
        node.get_logger().info(f"signal {signum}, finishing probe")
        node._finish()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node._finish()


if __name__ == '__main__':
    main()
