"""Pure helpers for Autoware external velocity-limit probes."""

from __future__ import annotations

import json
from typing import Any


DEFAULT_TOPIC = "/planning/scenario_planning/max_velocity_default"
DEFAULT_SENDER = "api"


def build_velocity_limit_message_dict(
    max_velocity_mps: float,
    *,
    sender: str = DEFAULT_SENDER,
    use_constraints: bool = False,
    max_acceleration: float = 1.0,
    min_acceleration: float = -2.5,
    max_jerk: float = 1.5,
    min_jerk: float = -1.5,
    stamp_sec: int = 0,
    stamp_nanosec: int = 0,
) -> dict[str, Any]:
    """Build a dict matching autoware_internal_planning_msgs/msg/VelocityLimit."""

    velocity = float(max_velocity_mps)
    if velocity <= 0:
        raise ValueError(f"max_velocity_mps must be > 0, got {velocity}")
    return {
        "stamp": {"sec": int(stamp_sec), "nanosec": int(stamp_nanosec)},
        "max_velocity": velocity,
        "use_constraints": bool(use_constraints),
        "constraints": {
            "max_acceleration": float(max_acceleration),
            "min_acceleration": float(min_acceleration),
            "max_jerk": float(max_jerk),
            "min_jerk": float(min_jerk),
        },
        "sender": str(sender or DEFAULT_SENDER),
    }


def build_ros2_topic_pub_command(
    *,
    topic: str = DEFAULT_TOPIC,
    max_velocity_mps: float,
    once: bool = False,
    rate_hz: float = 1.0,
    sender: str = DEFAULT_SENDER,
) -> list[str]:
    """Build an equivalent ros2 CLI command for manual reproduction."""

    message = build_velocity_limit_message_dict(max_velocity_mps, sender=sender)
    cmd = [
        "ros2",
        "topic",
        "pub",
        str(topic),
        "autoware_internal_planning_msgs/msg/VelocityLimit",
        json.dumps(message, sort_keys=True),
    ]
    if once:
        cmd.insert(3, "--once")
    else:
        cmd[3:3] = ["--rate", str(float(rate_hz))]
    return cmd
