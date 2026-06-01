from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


SAFETY_EXIT_REASONS = frozenset({"COLLISION", "LANE_INVASION", "LANE_INVASION_HARD"})
CONTROL_HEALTH_APPLIED_OSCILLATION = "CONTROL_HEALTH_APPLIED_OSCILLATION"
CONTROL_HEALTH_MISSING_TIMESERIES = "CONTROL_HEALTH_MISSING_TIMESERIES"
CONTROL_HEALTH_BRIDGE_OVERSPEED_THROTTLE = "CONTROL_HEALTH_BRIDGE_OVERSPEED_THROTTLE"
CONTROL_HEALTH_MISSING_BRIDGE_LOG = "CONTROL_HEALTH_MISSING_BRIDGE_LOG"


@dataclass(frozen=True)
class AcceptanceFailure:
    code: str
    scope: str
    detail: dict[str, Any]


def _count_from_summary(summary: Mapping[str, Any], key: str) -> int:
    value = summary.get(key)
    if value is None and isinstance(summary.get("metrics"), Mapping):
        value = summary["metrics"].get(key)
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _float_from_row(row: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def safety_failures_from_summary(summary: Mapping[str, Any]) -> list[AcceptanceFailure]:
    """Return safety-critical failures that must dominate acceptance status."""

    failures: list[AcceptanceFailure] = []
    seen: set[str] = set()

    raw_exit_reason = summary.get("exit_reason")
    exit_reason = str(raw_exit_reason or "").strip().upper()
    collision_count = _count_from_summary(summary, "collision_count")
    lane_invasion_count = _count_from_summary(summary, "lane_invasion_count")

    if exit_reason in SAFETY_EXIT_REASONS:
        detail = {
            "code": exit_reason,
            "scope": "safety",
            "source": "exit_reason",
            "exit_reason": raw_exit_reason,
            "collision_count": collision_count,
            "lane_invasion_count": lane_invasion_count,
        }
        failures.append(AcceptanceFailure(code=exit_reason, scope="safety", detail=detail))
        seen.add(exit_reason)

    if collision_count > 0 and "COLLISION" not in seen:
        detail = {
            "code": "COLLISION",
            "scope": "safety",
            "source": "collision_count",
            "collision_count": collision_count,
            "exit_reason": raw_exit_reason,
        }
        failures.append(AcceptanceFailure(code="COLLISION", scope="safety", detail=detail))
        seen.add("COLLISION")

    if lane_invasion_count > 0 and not ({"LANE_INVASION", "LANE_INVASION_HARD"} & seen):
        detail = {
            "code": "LANE_INVASION",
            "scope": "safety",
            "source": "lane_invasion_count",
            "lane_invasion_count": lane_invasion_count,
            "exit_reason": raw_exit_reason,
        }
        failures.append(AcceptanceFailure(code="LANE_INVASION", scope="safety", detail=detail))

    return failures


def applied_control_health_from_rows(
    rows: list[Mapping[str, Any]],
    *,
    max_throttle_brake_switch_count: int,
) -> dict[str, Any]:
    states: list[str] = []
    throttle_values: list[float] = []
    brake_values: list[float] = []
    saw_applied_control = False

    for row in rows:
        throttle = _float_from_row(row, "throttle_applied", "applied_throttle", "carla_throttle_applied")
        brake = _float_from_row(row, "brake_applied", "applied_brake", "carla_brake_applied")
        if throttle is None and brake is None:
            continue
        saw_applied_control = True
        throttle_f = float(throttle or 0.0)
        brake_f = float(brake or 0.0)
        throttle_values.append(throttle_f)
        brake_values.append(brake_f)
        if throttle_f > 0.01 and brake_f > 0.01:
            states.append("conflict")
        elif throttle_f > 0.01:
            states.append("throttle")
        elif brake_f > 0.01:
            states.append("brake")
        else:
            states.append("coast")

    compact = [state for state in states if state in {"throttle", "brake"}]
    switch_count = sum(1 for prev, cur in zip(compact, compact[1:]) if prev != cur)
    state_counts = {state: states.count(state) for state in ("throttle", "brake", "conflict", "coast")}
    return {
        "available": bool(rows),
        "applied_control_available": saw_applied_control,
        "sample_count": len(rows),
        "state_counts": state_counts,
        "throttle_max": max(throttle_values) if throttle_values else None,
        "brake_max": max(brake_values) if brake_values else None,
        "throttle_brake_switch_count": switch_count,
        "max_throttle_brake_switch_count": int(max_throttle_brake_switch_count),
        "ok": saw_applied_control and switch_count <= int(max_throttle_brake_switch_count),
    }


def applied_control_health_from_timeseries(
    path: str | Path,
    *,
    max_throttle_brake_switch_count: int,
) -> dict[str, Any]:
    csv_path = Path(path)
    if not csv_path.exists():
        return {
            "available": False,
            "applied_control_available": False,
            "path": str(csv_path),
            "sample_count": 0,
            "throttle_brake_switch_count": None,
            "max_throttle_brake_switch_count": int(max_throttle_brake_switch_count),
            "ok": False,
        }
    try:
        with csv_path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
    except Exception as exc:
        return {
            "available": False,
            "applied_control_available": False,
            "path": str(csv_path),
            "sample_count": 0,
            "read_error": str(exc),
            "throttle_brake_switch_count": None,
            "max_throttle_brake_switch_count": int(max_throttle_brake_switch_count),
            "ok": False,
        }
    check = applied_control_health_from_rows(
        rows,
        max_throttle_brake_switch_count=max_throttle_brake_switch_count,
    )
    check["path"] = str(csv_path)
    return check


def bridge_target_speed_health_from_log(
    path: str | Path,
    *,
    overspeed_threshold_mps: float,
    max_throttle_while_overspeed_rows: int,
) -> dict[str, Any]:
    log_path = Path(path)
    if not log_path.exists():
        return {
            "available": False,
            "path": str(log_path),
            "apply_log_rows": 0,
            "overspeed_threshold_mps": float(overspeed_threshold_mps),
            "max_throttle_while_overspeed_rows": int(max_throttle_while_overspeed_rows),
            "throttle_while_overspeed_rows": None,
            "ok": False,
        }
    try:
        text = log_path.read_text(errors="replace")
    except Exception as exc:
        return {
            "available": False,
            "path": str(log_path),
            "apply_log_rows": 0,
            "read_error": str(exc),
            "overspeed_threshold_mps": float(overspeed_threshold_mps),
            "max_throttle_while_overspeed_rows": int(max_throttle_while_overspeed_rows),
            "throttle_while_overspeed_rows": None,
            "ok": False,
        }

    apply_log_rows = 0
    overspeed_rows = 0
    throttle_while_overspeed_rows = 0
    max_current_minus_target_speed_mps: float | None = None
    for line in text.splitlines():
        if "apply frame=" not in line:
            continue
        target_match = re.search(r"(?:^|\s)target_speed=(-?\d+(?:\.\d+)?|null)", line)
        current_match = re.search(r"(?:^|\s)current_speed=(-?\d+(?:\.\d+)?|null)", line)
        throttle_match = re.search(r"(?:^|\s)throttle=(-?\d+(?:\.\d+)?|null)", line)
        if (
            not target_match
            or not current_match
            or target_match.group(1) == "null"
            or current_match.group(1) == "null"
        ):
            continue
        apply_log_rows += 1
        current_minus_target = float(current_match.group(1)) - float(target_match.group(1))
        max_current_minus_target_speed_mps = (
            current_minus_target
            if max_current_minus_target_speed_mps is None
            else max(max_current_minus_target_speed_mps, current_minus_target)
        )
        if current_minus_target <= float(overspeed_threshold_mps):
            continue
        overspeed_rows += 1
        throttle = 0.0
        if throttle_match and throttle_match.group(1) != "null":
            throttle = float(throttle_match.group(1))
        if throttle > 0.01:
            throttle_while_overspeed_rows += 1

    return {
        "available": True,
        "path": str(log_path),
        "apply_log_rows": apply_log_rows,
        "overspeed_threshold_mps": float(overspeed_threshold_mps),
        "max_throttle_while_overspeed_rows": int(max_throttle_while_overspeed_rows),
        "max_current_minus_target_speed_mps": max_current_minus_target_speed_mps,
        "overspeed_rows": overspeed_rows,
        "throttle_while_overspeed_rows": throttle_while_overspeed_rows,
        "ok": throttle_while_overspeed_rows <= int(max_throttle_while_overspeed_rows),
    }


def bridge_target_speed_failures_from_check(
    check: Mapping[str, Any],
    *,
    enabled: bool,
    fail_on_missing: bool,
) -> list[AcceptanceFailure]:
    if not enabled:
        return []
    if not check.get("available"):
        if not fail_on_missing:
            return []
        detail = {
            "code": CONTROL_HEALTH_MISSING_BRIDGE_LOG,
            "scope": "control_health",
            "source": "bridge_log",
            **dict(check),
        }
        return [
            AcceptanceFailure(
                code=CONTROL_HEALTH_MISSING_BRIDGE_LOG,
                scope="control_health",
                detail=detail,
            )
        ]
    if not check.get("ok"):
        detail = {
            "code": CONTROL_HEALTH_BRIDGE_OVERSPEED_THROTTLE,
            "scope": "control_health",
            "source": "bridge_log",
            **dict(check),
        }
        return [
            AcceptanceFailure(
                code=CONTROL_HEALTH_BRIDGE_OVERSPEED_THROTTLE,
                scope="control_health",
                detail=detail,
            )
        ]
    return []


def control_health_failures_from_check(
    check: Mapping[str, Any],
    *,
    enabled: bool,
    fail_on_missing: bool,
) -> list[AcceptanceFailure]:
    if not enabled:
        return []
    if not check.get("available") or not check.get("applied_control_available"):
        if not fail_on_missing:
            return []
        detail = {
            "code": CONTROL_HEALTH_MISSING_TIMESERIES,
            "scope": "control_health",
            "source": "timeseries",
            **dict(check),
        }
        return [
            AcceptanceFailure(
                code=CONTROL_HEALTH_MISSING_TIMESERIES,
                scope="control_health",
                detail=detail,
            )
        ]
    switch_count = check.get("throttle_brake_switch_count")
    max_switch_count = check.get("max_throttle_brake_switch_count")
    if switch_count is not None and max_switch_count is not None and switch_count > max_switch_count:
        detail = {
            "code": CONTROL_HEALTH_APPLIED_OSCILLATION,
            "scope": "control_health",
            "source": "timeseries",
            **dict(check),
        }
        return [
            AcceptanceFailure(
                code=CONTROL_HEALTH_APPLIED_OSCILLATION,
                scope="control_health",
                detail=detail,
            )
        ]
    return []
