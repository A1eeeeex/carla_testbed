from __future__ import annotations

from typing import Any, Dict, Optional

try:
    from rclpy.qos import (
        QoSHistoryPolicy,
        QoSReliabilityPolicy,
        QoSDurabilityPolicy,
        QoSProfile,
    )
except ImportError as exc:  # pragma: no cover - requires ROS2 runtime
    raise RuntimeError("ROS2 QoS utilities require rclpy; please source your ROS2 environment.") from exc


def _policy_from_str(name: str, policy_cls):
    normalized = (name or "").lower()
    for key, val in policy_cls.__dict__.items():
        if key.lower() == normalized:
            return val
    # fallback aliases
    if normalized in ["best_effort", "besteffort"]:
        return QoSReliabilityPolicy.BEST_EFFORT
    if normalized in ["reliable"]:
        return QoSReliabilityPolicy.RELIABLE
    if normalized in ["transient_local", "transientlocal"]:
        return QoSDurabilityPolicy.TRANSIENT_LOCAL
    if normalized in ["volatile"]:
        return QoSDurabilityPolicy.VOLATILE
    if normalized in ["keep_all", "keepall"]:
        return QoSHistoryPolicy.KEEP_ALL
    if normalized in ["keep_last", "keeplast"]:
        return QoSHistoryPolicy.KEEP_LAST
    return None


def qos_from_contract(qos_entry: Any, default_profile: Optional[QoSProfile] = None) -> QoSProfile:
    """
    Parse QoS description from contract (dict or string) into QoSProfile.
    Falls back to ROS2 SENSOR_DATA semantics if not provided.
    """
    if isinstance(qos_entry, QoSProfile):
        return qos_entry
    if qos_entry is None:
        return default_profile or QoSProfile(depth=5, reliability=QoSReliabilityPolicy.BEST_EFFORT)
    if isinstance(qos_entry, str):
        name = qos_entry.upper()
        if name == "SENSOR_DATA":
            return QoSProfile(depth=5, reliability=QoSReliabilityPolicy.BEST_EFFORT, durability=QoSDurabilityPolicy.VOLATILE)
        if name == "SYSTEM_DEFAULT":
            return QoSProfile(depth=10)
    if isinstance(qos_entry, dict):
        history = qos_entry.get("history")
        reliability = qos_entry.get("reliability")
        durability = qos_entry.get("durability")
        depth = qos_entry.get("depth", 10)
        prof = QoSProfile(depth=depth)
        if history:
            pol = _policy_from_str(history, QoSHistoryPolicy)
            if pol is not None:
                prof.history = pol
        if reliability:
            pol = _policy_from_str(reliability, QoSReliabilityPolicy)
            if pol is not None:
                prof.reliability = pol
        if durability:
            pol = _policy_from_str(durability, QoSDurabilityPolicy)
            if pol is not None:
                prof.durability = pol
        return prof
    return default_profile or QoSProfile(depth=5, reliability=QoSReliabilityPolicy.BEST_EFFORT)
