#!/usr/bin/env python3
"""Publish an Autoware external velocity limit for diagnostic probes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy


def _write_report(path: str | None, payload: dict) -> None:
    if not path:
        return
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="/planning/scenario_planning/max_velocity_default")
    parser.add_argument("--max-velocity-mps", type=float, required=True)
    parser.add_argument("--publish-hz", type=float, default=5.0)
    parser.add_argument("--duration-s", type=float, default=30.0)
    parser.add_argument("--start-delay-s", type=float, default=0.0)
    parser.add_argument("--sender", default="api")
    parser.add_argument("--use-constraints", action="store_true")
    parser.add_argument("--max-acceleration", type=float, default=1.0)
    parser.add_argument("--min-acceleration", type=float, default=-2.5)
    parser.add_argument("--max-jerk", type=float, default=1.5)
    parser.add_argument("--min-jerk", type=float, default=-1.5)
    parser.add_argument("--report", default=None)
    args = parser.parse_args(argv)

    if args.max_velocity_mps <= 0:
        raise SystemExit("--max-velocity-mps must be > 0")
    if args.publish_hz <= 0:
        raise SystemExit("--publish-hz must be > 0")
    if args.duration_s <= 0:
        raise SystemExit("--duration-s must be > 0")

    report = {
        "schema_version": "autoware_velocity_limit_probe.v1",
        "topic": args.topic,
        "max_velocity_mps": args.max_velocity_mps,
        "publish_hz": args.publish_hz,
        "duration_s": args.duration_s,
        "start_delay_s": args.start_delay_s,
        "sender": args.sender,
        "published_count": 0,
        "status": "started",
    }

    try:
        from autoware_internal_planning_msgs.msg import VelocityLimit, VelocityLimitConstraints
    except Exception as exc:  # pragma: no cover - runtime environment dependent
        report.update({"status": "failed", "error": f"import_failed:{exc}"})
        _write_report(args.report, report)
        raise

    rclpy.init(args=None)
    qos = QoSProfile(
        depth=1,
        history=HistoryPolicy.KEEP_LAST,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )
    report["qos"] = {
        "depth": 1,
        "history": "keep_last",
        "reliability": "reliable",
        "durability": "transient_local",
    }

    node = Node("carla_testbed_velocity_limit_probe")
    publisher = node.create_publisher(VelocityLimit, args.topic, qos)
    try:
        if args.start_delay_s > 0:
            time.sleep(args.start_delay_s)
        period_s = 1.0 / args.publish_hz
        deadline = time.monotonic() + args.duration_s
        while rclpy.ok() and time.monotonic() < deadline:
            msg = VelocityLimit()
            msg.stamp = node.get_clock().now().to_msg()
            msg.max_velocity = float(args.max_velocity_mps)
            msg.use_constraints = bool(args.use_constraints)
            msg.sender = str(args.sender)
            msg.constraints = VelocityLimitConstraints()
            msg.constraints.max_acceleration = float(args.max_acceleration)
            msg.constraints.min_acceleration = float(args.min_acceleration)
            msg.constraints.max_jerk = float(args.max_jerk)
            msg.constraints.min_jerk = float(args.min_jerk)
            publisher.publish(msg)
            report["published_count"] += 1
            rclpy.spin_once(node, timeout_sec=0.0)
            time.sleep(period_s)
        report["status"] = "completed"
        return 0
    finally:
        _write_report(args.report, report)
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
