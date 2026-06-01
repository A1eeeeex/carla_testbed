from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

CURVE_PAIR_SCHEMA_VERSION = "curve_pair_semantics.v1"


def _read_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _first_seq(values: Sequence[Any]) -> int | None:
    cleaned: list[int] = []
    for value in values:
        num = _num(value)
        if num is not None:
            cleaned.append(int(num))
    return min(cleaned) if cleaned else None


def _guard_count(control: Mapping[str, Any], key: str) -> int:
    counts = control.get("guard_apply_counts") or {}
    value = counts.get(key)
    num = _num(value)
    return 0 if num is None else int(num)


def _first_high_steer_seq(apollo: Mapping[str, Any]) -> int | None:
    first = apollo.get("first_high_steer")
    if isinstance(first, Mapping):
        return int(_num(first.get("seq")) or _num(first.get("frame_id")) or -1) if first else None
    return None


def summarize_curve_route(report: Mapping[str, Any]) -> dict[str, Any]:
    metrics = report.get("run_metrics") or {}
    apollo = report.get("apollo_semantics") or {}
    control = report.get("control_semantics") or {}
    geometry = report.get("route_geometry") or {}
    guard_counts = {
        "lateral_guard": _guard_count(control, "lateral_guard"),
        "trajectory_contract_lateral_guard": _guard_count(control, "trajectory_contract_lateral_guard"),
        "low_speed_steer_guard": _guard_count(control, "low_speed_steer_guard"),
    }
    first_high_steer = _first_high_steer_seq(apollo)
    first_matched = _first_seq(apollo.get("matched_point_anomaly_locations") or [])
    first_target = _first_seq(apollo.get("target_point_anomaly_locations") or [])
    onset_candidates = [
        value
        for value in (first_high_steer, first_matched, first_target)
        if value is not None and value >= 0
    ]
    onset_seq = min(onset_candidates) if onset_candidates else None
    missing_fields = set(str(item) for item in report.get("missing_fields") or [])
    if report.get("missing_inputs"):
        family = "insufficient_data"
        reason = "missing route-health inputs"
    elif guard_counts["lateral_guard"] or guard_counts["trajectory_contract_lateral_guard"]:
        family = "bridge_policy_confounded"
        reason = "bridge policy guard applied during curve evidence"
    elif first_high_steer is not None or first_matched is not None or first_target is not None:
        family = "apollo_lateral_semantics_primary"
        reason = "Apollo high-steer/matched/target anomalies precede bridge guard evidence"
    elif {"matched_point", "target_point", "apollo_raw_steer"}.intersection(missing_fields):
        family = "semantic_fields_missing"
        reason = "Apollo semantic fields are missing"
    else:
        family = "no_semantic_blocker_detected"
        reason = "route-health did not expose high-steer or matched/target anomalies"
    return {
        "route_id": report.get("route_id"),
        "verdict_status": (report.get("verdict") or {}).get("status"),
        "failure_family": family,
        "reason": reason,
        "onset_seq": onset_seq,
        "first_high_steer_seq": first_high_steer,
        "first_matched_point_anomaly_seq": first_matched,
        "first_target_point_anomaly_seq": first_target,
        "lateral_error_p95_m": metrics.get("lateral_error_p95_m"),
        "lateral_error_max_m": metrics.get("lateral_error_max_m"),
        "heading_error_p95_rad": metrics.get("heading_error_p95_rad"),
        "heading_error_max_rad": metrics.get("heading_error_max_rad"),
        "curve_segments_count": geometry.get("curve_segments_count"),
        "guard_apply_counts": guard_counts,
        "raw_mapped_applied_steer_available": bool(control.get("raw_mapped_applied_steer_available")),
        "missing_fields": sorted(missing_fields),
        "missing_inputs": list(report.get("missing_inputs") or []),
    }


def compare_curve_pair(
    reports: Sequence[Mapping[str, Any]],
    *,
    onset_delta_threshold_seq: int = 20,
) -> dict[str, Any]:
    route_summaries = [summarize_curve_route(report) for report in reports]
    families = {summary["failure_family"] for summary in route_summaries}
    if len(route_summaries) < 2:
        status = "insufficient_data"
        reason = "need at least two curve route-health reports"
    elif "insufficient_data" in families:
        status = "insufficient_data"
        reason = "at least one curve report has missing route-health inputs"
    elif len(families) > 1:
        status = "heterogeneous_blocker"
        reason = "curve routes expose different blocker families"
    else:
        onsets = [summary.get("onset_seq") for summary in route_summaries]
        numeric_onsets = [int(value) for value in onsets if value is not None]
        if len(numeric_onsets) == len(route_summaries):
            delta = max(numeric_onsets) - min(numeric_onsets)
            if delta <= int(onset_delta_threshold_seq):
                status = "shared_family_similar_onset"
                reason = "curve routes share a blocker family and similar onset"
            else:
                status = "shared_family_different_onset"
                reason = "curve routes share a blocker family but diverge at different onsets"
        else:
            status = "shared_family_onset_incomplete"
            reason = "curve routes share a blocker family but onset evidence is incomplete"
    return {
        "schema_version": CURVE_PAIR_SCHEMA_VERSION,
        "status": status,
        "reason": reason,
        "onset_delta_threshold_seq": onset_delta_threshold_seq,
        "routes": route_summaries,
        "claim_supported": (
            "This classifies curve route-health evidence only; it does not promote carla_direct or prove curve is solved."
        ),
    }


def compare_curve_pair_files(
    route_health_paths: Sequence[str | Path],
    *,
    onset_delta_threshold_seq: int = 20,
) -> dict[str, Any]:
    return compare_curve_pair(
        [_read_json(path) for path in route_health_paths],
        onset_delta_threshold_seq=onset_delta_threshold_seq,
    )


def render_curve_pair_summary(report: Mapping[str, Any]) -> str:
    lines = [
        "# Curve Pair Semantic Comparison",
        "",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        "",
        "| route_id | family | onset_seq | high_steer | matched | target | lat_p95 | heading_p95 | guards |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for route in report.get("routes") or []:
        guards = route.get("guard_apply_counts") or {}
        guard_text = ",".join(f"{key}={value}" for key, value in sorted(guards.items()))
        lines.append(
            "| "
            f"{route.get('route_id')} | "
            f"{route.get('failure_family')} | "
            f"{route.get('onset_seq') if route.get('onset_seq') is not None else ''} | "
            f"{route.get('first_high_steer_seq') if route.get('first_high_steer_seq') is not None else ''} | "
            f"{route.get('first_matched_point_anomaly_seq') if route.get('first_matched_point_anomaly_seq') is not None else ''} | "
            f"{route.get('first_target_point_anomaly_seq') if route.get('first_target_point_anomaly_seq') is not None else ''} | "
            f"{route.get('lateral_error_p95_m') if route.get('lateral_error_p95_m') is not None else ''} | "
            f"{route.get('heading_error_p95_rad') if route.get('heading_error_p95_rad') is not None else ''} | "
            f"{guard_text} |"
        )
    lines.extend(["", str(report.get("claim_supported") or ""), ""])
    return "\n".join(lines)
