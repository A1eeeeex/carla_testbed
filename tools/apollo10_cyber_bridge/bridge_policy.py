from __future__ import annotations

from typing import Any, Dict, MutableMapping


StatsDict = MutableMapping[str, Any]


def increment_policy_counter(stats: StatsDict, key: str, amount: int = 1) -> int:
    next_value = int(stats.get(key, 0) or 0) + int(amount)
    stats[key] = next_value
    return next_value


def increment_policy_reason_count(
    stats: StatsDict,
    *,
    reason: str,
    key: str = "lateral_guard_reason_counts",
    amount: int = 1,
) -> Dict[str, int]:
    reason_text = str(reason or "").strip() or "unspecified"
    reason_counts = dict(stats.get(key, {}) or {})
    reason_counts[reason_text] = int(reason_counts.get(reason_text, 0) or 0) + int(amount)
    stats[key] = reason_counts
    return reason_counts


def record_lateral_guard_trigger(stats: StatsDict, *, reason: str) -> None:
    increment_policy_counter(stats, "lateral_guard_trigger_count")
    increment_policy_reason_count(stats, reason=reason)


def build_bridge_policy_summary(*, goal_validity_last: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source": "bridge_policy",
        "goal_fallback": {
            "source": "bridge_policy",
            "reason": str(goal_validity_last.get("fallback_reason") or ""),
            "fallback_applied": bool(goal_validity_last.get("fallback_applied", False)),
        },
        "lateral_guard": {
            "source": "bridge_policy",
            "reason_counts": dict(stats.get("lateral_guard_reason_counts", {}) or {}),
            "trigger_count": int(stats.get("lateral_guard_trigger_count", 0) or 0),
            "apply_count": int(stats.get("lateral_guard_apply_count", 0) or 0),
            "fallback_applied": bool((stats.get("lateral_guard_apply_count", 0) or 0) > 0),
        },
        "trajectory_contract_guard": {
            "source": "bridge_policy",
            "reason": "trajectory_contract_lateral_guard",
            "apply_count": int(stats.get("trajectory_contract_lateral_guard_apply_count", 0) or 0),
            "fallback_applied": bool((stats.get("trajectory_contract_lateral_guard_apply_count", 0) or 0) > 0),
        },
    }
