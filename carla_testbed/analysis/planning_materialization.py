from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

PLANNING_MATERIALIZATION_SCHEMA_VERSION = "planning_materialization.v1"
EMPTY_ASOF_TOLERANCE_S = 0.10


def analyze_planning_materialization_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    artifacts = root / "artifacts"
    return analyze_planning_materialization_files(
        planning_topic_debug=_find_first(
            root,
            (
                "artifacts/planning_topic_debug.jsonl",
                "planning_topic_debug.jsonl",
            ),
        ),
        planning_topic_debug_summary=_find_first(
            root,
            (
                "artifacts/planning_topic_debug_summary.json",
                "artifacts/planning_topic_debug_summary.finalized.json",
                "planning_topic_debug_summary.json",
            ),
        ),
        planning_route_segment_debug=_find_first(
            root,
            (
                "artifacts/planning_route_segment_debug.jsonl",
                "artifacts/apollo_route_segment_debug.jsonl",
                "planning_route_segment_debug.jsonl",
            ),
        ),
        reference_line_contract=_find_first(
            root,
            (
                "artifacts/apollo_reference_line_contract.jsonl",
                "apollo_reference_line_contract.jsonl",
            ),
        ),
        topic_publish_stats=_find_first(
            root,
            (
                "artifacts/topic_publish_stats.jsonl",
                "topic_publish_stats.jsonl",
            ),
        ),
        hdmap_projection=_find_first(
            root,
            (
                "artifacts/apollo_hdmap_projection.jsonl",
                "apollo_hdmap_projection.jsonl",
            ),
        ),
        summary=_find_first(root, ("summary.json",)),
        bridge_stats=_find_first(
            root,
            (
                "artifacts/cyber_bridge_stats.json",
                "cyber_bridge_stats.json",
            ),
        ),
        planning_logs=sorted(artifacts.glob("apollo_planning*")) if artifacts.exists() else (),
    )


def analyze_planning_materialization_files(
    *,
    planning_topic_debug: str | Path | None = None,
    planning_topic_debug_summary: str | Path | None = None,
    planning_route_segment_debug: str | Path | None = None,
    reference_line_contract: str | Path | None = None,
    topic_publish_stats: str | Path | None = None,
    hdmap_projection: str | Path | None = None,
    summary: str | Path | None = None,
    bridge_stats: str | Path | None = None,
    planning_logs: Sequence[str | Path] = (),
) -> dict[str, Any]:
    planning_rows = _read_jsonl(Path(planning_topic_debug).expanduser() if planning_topic_debug else None)
    planning_summary = _read_json(
        Path(planning_topic_debug_summary).expanduser() if planning_topic_debug_summary else None
    )
    route_rows = _read_jsonl(
        Path(planning_route_segment_debug).expanduser() if planning_route_segment_debug else None
    )
    reference_rows = _read_jsonl(
        Path(reference_line_contract).expanduser() if reference_line_contract else None
    )
    topic_rows = _read_jsonl(Path(topic_publish_stats).expanduser() if topic_publish_stats else None)
    projection_rows = _read_jsonl(Path(hdmap_projection).expanduser() if hdmap_projection else None)
    summary_payload = _read_json(Path(summary).expanduser() if summary else None)
    bridge_payload = _read_json(Path(bridge_stats).expanduser() if bridge_stats else None)
    log_errors = _top_log_errors(Path(path).expanduser() for path in planning_logs)

    missing_fields: list[str] = []
    warnings: list[str] = []
    if not planning_rows and not planning_summary:
        missing_fields.append("planning_topic_debug_or_summary")
    if not route_rows:
        warnings.append("planning_route_segment_debug_missing")
    if not reference_rows:
        warnings.append("apollo_reference_line_contract_missing")
    if not topic_rows:
        warnings.append("topic_publish_stats_missing")
    if not projection_rows:
        warnings.append("apollo_hdmap_projection_missing")

    planning_message_count = _int_or_none(planning_summary.get("total_messages_received"))
    nonempty_count = _int_or_none(
        planning_summary.get("messages_with_nonzero_trajectory_points")
    )
    if planning_rows:
        planning_message_count = len(planning_rows)
        nonempty_count = sum(1 for row in planning_rows if _trajectory_point_count(row) > 0)
    planning_message_count = planning_message_count or 0
    nonempty_count = nonempty_count or 0
    empty_count = max(0, planning_message_count - nonempty_count)
    nonempty_ratio = (
        float(nonempty_count) / float(planning_message_count)
        if planning_message_count > 0
        else None
    )

    routing_success_ts = _first_number(
        planning_summary,
        "routing_first_success_response_ts_sec",
        bridge_payload,
        "routing_first_success_response_ts_sec",
    )
    rows_after_routing = [
        row
        for row in planning_rows
        if routing_success_ts is not None and _row_time(row) is not None and _row_time(row) >= routing_success_ts
    ]
    after_routing_ratio = _nonempty_ratio(rows_after_routing) if rows_after_routing else None

    loc_ready, chassis_ready = _localization_chassis_ready(topic_rows)
    after_loc_chassis_ready_ratio = (
        nonempty_ratio if loc_ready is True and chassis_ready is True else None
    )

    route_by_seq = _rows_by_sequence(route_rows)
    empty_reason_histogram: Counter[str] = Counter(
        {
            "localization_stale_or_gap": 0,
            "chassis_stale_or_gap": 0,
            "routing_not_ready": 0,
            "reference_line_provider_not_ready": 0,
            "hdmap_projection_failed": 0,
            "path_fallback_or_speed_fallback": 0,
            "unknown": 0,
        }
    )
    for row in planning_rows:
        if _trajectory_point_count(row) > 0:
            continue
        reason = _classify_empty_row(row, route_by_seq.get(_sequence(row)), routing_success_ts)
        empty_reason_histogram[reason] += 1

    projection_status_counts = Counter(
        str(row.get("status") or "unknown") for row in projection_rows if isinstance(row, Mapping)
    )
    empty_asof_join = _empty_asof_join(
        planning_rows=planning_rows,
        topic_rows=topic_rows,
        reference_rows=reference_rows,
        projection_rows=projection_rows,
        tolerance_s=EMPTY_ASOF_TOLERANCE_S,
    )
    if projection_status_counts and any(
        status not in {"ok", "pass"} and count > 0
        for status, count in projection_status_counts.items()
    ):
        empty_reason_histogram["hdmap_projection_failed"] += int(
            sum(
                count
                for status, count in projection_status_counts.items()
                if status not in {"ok", "pass"}
            )
        )
    input_freshness_attribution = _input_freshness_attribution(
        empty_asof_join,
        empty_reason_histogram=empty_reason_histogram,
    )
    if input_freshness_attribution["status"] == "insufficient_data":
        warnings.append("planning_input_freshness_unverified")

    first_nonempty_ts = _first_nonempty_timestamp(planning_rows)
    first_msg_ts = _first_message_timestamp(planning_rows, planning_summary)
    first_nonempty_latency_s = (
        first_nonempty_ts - first_msg_ts
        if first_nonempty_ts is not None and first_msg_ts is not None
        else None
    )
    first_nonempty_after_routing_latency_s = (
        first_nonempty_ts - routing_success_ts
        if first_nonempty_ts is not None and routing_success_ts is not None
        else None
    )
    longest_empty_streak = _longest_empty_streak(planning_rows)
    verdict = _verdict(nonempty_ratio, missing_fields)
    blocking_reasons: list[str] = []
    if verdict == "fail":
        blocking_reasons.append("planning_trajectory_materialization_low")
    elif verdict == "insufficient_data":
        blocking_reasons.append("planning_materialization_insufficient_data")

    route_establishment = _route_establishment(
        summary_payload=summary_payload,
        bridge_payload=bridge_payload,
        nonempty_after_routing_ratio=after_routing_ratio,
        first_nonempty_after_routing_latency_s=first_nonempty_after_routing_latency_s,
    )
    time_domain = _time_domain_diagnostics(
        planning_rows=planning_rows,
        topic_rows=topic_rows,
        routing_success_ts=routing_success_ts,
        first_nonempty_after_routing_latency_s=first_nonempty_after_routing_latency_s,
        planning_summary=planning_summary,
    )
    if time_domain["status"] == "insufficient_data":
        warnings.append("planning_time_domain_mixed_or_unverified")
        route_establishment.setdefault("warnings", []).append(
            "routing_planning_latency_time_domain_unverified"
        )
        if route_establishment.get("route_established") is True:
            route_establishment["route_established"] = None
            route_establishment.setdefault("blocking_reasons", []).append(
                "route_establishment_time_domain_unverified"
            )
    if route_establishment["route_established"] is False:
        blocking_reasons.append("route_establishment_not_confirmed")

    return {
        "schema_version": PLANNING_MATERIALIZATION_SCHEMA_VERSION,
        "run_id": _text(summary_payload.get("run_id") or bridge_payload.get("run_id")),
        "route_id": _text(summary_payload.get("route_id") or bridge_payload.get("route_id")),
        "scenario_class": _text(
            summary_payload.get("scenario_class") or bridge_payload.get("scenario_class")
        ),
        "planning_message_count": planning_message_count,
        "nonempty_trajectory_count": nonempty_count,
        "empty_trajectory_count": empty_count,
        "nonempty_trajectory_ratio": nonempty_ratio,
        "after_routing_success_nonempty_ratio": after_routing_ratio,
        "after_localization_chassis_ready_nonempty_ratio": after_loc_chassis_ready_ratio,
        "metrics": {
            "planning_message_count": planning_message_count,
            "nonempty_trajectory_count": nonempty_count,
            "empty_trajectory_count": empty_count,
            "nonempty_trajectory_ratio": nonempty_ratio,
            "after_routing_success_nonempty_ratio": after_routing_ratio,
            "after_localization_chassis_ready_nonempty_ratio": after_loc_chassis_ready_ratio,
            "longest_empty_streak": longest_empty_streak,
        },
        "localization_channel_ready": loc_ready,
        "chassis_channel_ready": chassis_ready,
        "empty_reason_observed_debug": dict(sorted(empty_reason_histogram.items())),
        "empty_reason_histogram": dict(sorted(empty_reason_histogram.items())),
        "empty_asof_join": empty_asof_join,
        "input_freshness_attribution": input_freshness_attribution,
        "time_domain": time_domain,
        "first_planning_message_time_sec": first_msg_ts,
        "first_nonempty_planning_time_sec": first_nonempty_ts,
        "last_nonempty_planning_time_sec": _last_nonempty_timestamp(planning_rows),
        "first_nonempty_latency_s": first_nonempty_latency_s,
        "first_nonempty_after_routing_latency_s": first_nonempty_after_routing_latency_s,
        "longest_empty_streak": longest_empty_streak,
        "planning_debug_status_topk": _planning_status_topk(route_rows),
        "apollo_log_error_topk": log_errors,
        "hdmap_projection_status_counts": dict(sorted(projection_status_counts.items())),
        "route_establishment": route_establishment,
        "thresholds": {
            "claim_candidate_nonempty_ratio_min": 0.80,
            "warn_nonempty_ratio_min": 0.50,
            "fail_nonempty_ratio_min": 0.20,
        },
        "missing_fields": sorted(set(missing_fields)),
        "warnings": sorted(set(warnings)),
        "blocking_reasons": sorted(set(blocking_reasons)),
        "verdict": verdict,
        "interpretation_boundary": (
            "Control rx/tx evidence does not prove route establishment. Empty or sparse "
            "ADCTrajectory output must be resolved before treating control oscillation as "
            "the primary blocker."
        ),
    }


def write_planning_materialization_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "planning_materialization_report.json"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path = output_dir / "planning_materialization_summary.md"
    summary_path.write_text(planning_materialization_summary_md(report), encoding="utf-8")
    return {
        "planning_materialization_report": str(report_path),
        "planning_materialization_summary": str(summary_path),
    }


def planning_materialization_summary_md(report: Mapping[str, Any]) -> str:
    route_establishment = report.get("route_establishment")
    if not isinstance(route_establishment, Mapping):
        route_establishment = {}
    lines = [
        "# Apollo Planning Materialization",
        "",
        f"- Verdict: {report.get('verdict')}",
        f"- Planning messages: {report.get('planning_message_count')}",
        f"- Non-empty trajectories: {report.get('nonempty_trajectory_count')}",
        f"- Non-empty ratio: {_fmt_ratio(report.get('nonempty_trajectory_ratio'))}",
        f"- After routing success ratio: {_fmt_ratio(report.get('after_routing_success_nonempty_ratio'))}",
        f"- Longest empty streak: {report.get('longest_empty_streak')}",
        f"- Route established: {route_establishment.get('route_established')}",
        f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
        f"- Warnings: {', '.join(report.get('warnings') or []) or 'none'}",
        f"- Empty as-of join: {_fmt_asof(report.get('empty_asof_join'))}",
        f"- Time domain: {_fmt_time_domain(report.get('time_domain'))}",
        f"- Input freshness attribution: {_fmt_freshness(report.get('input_freshness_attribution'))}",
        "",
        "## Empty Reason Histogram",
    ]
    histogram = report.get("empty_reason_histogram")
    if isinstance(histogram, Mapping):
        for key, value in histogram.items():
            lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _input_freshness_attribution(
    empty_asof_join: Mapping[str, Any],
    *,
    empty_reason_histogram: Mapping[str, Any],
) -> dict[str, Any]:
    empty_count = _int_or_none(empty_asof_join.get("empty_row_count")) or 0
    loc_cov = _number(empty_asof_join.get("localization_join_coverage_ratio"))
    chassis_cov = _number(empty_asof_join.get("chassis_join_coverage_ratio"))
    if empty_count <= 0:
        return {
            "status": "not_applicable",
            "input_freshness_verified_empty_count": 0,
            "input_freshness_unverified_empty_count": 0,
            "localization_stale_or_gap_empty_count": 0,
            "chassis_stale_or_gap_empty_count": 0,
            "warnings": [],
        }
    warnings: list[str] = []
    loc_stale_count: int | None
    chassis_stale_count: int | None
    if loc_cov is None or loc_cov <= 0:
        loc_stale_count = None
        warnings.append("localization_freshness_join_unavailable")
    else:
        loc_stale_count = _int_or_none(empty_asof_join.get("stale_localization_empty_count"))
    if chassis_cov is None or chassis_cov <= 0:
        chassis_stale_count = None
        warnings.append("chassis_freshness_join_unavailable")
    else:
        chassis_stale_count = _int_or_none(empty_asof_join.get("stale_chassis_empty_count"))
    verified = 0
    if loc_cov is not None and chassis_cov is not None:
        verified = int(round(empty_count * min(loc_cov, chassis_cov)))
    unverified = max(0, empty_count - verified)
    status = "insufficient_data" if warnings else "pass"
    return {
        "status": status,
        "input_freshness_verified_empty_count": verified,
        "input_freshness_unverified_empty_count": unverified,
        "localization_join_coverage_ratio": loc_cov,
        "chassis_join_coverage_ratio": chassis_cov,
        "localization_stale_or_gap_empty_count": loc_stale_count,
        "chassis_stale_or_gap_empty_count": chassis_stale_count,
        "observed_debug_empty_reason_histogram": dict(empty_reason_histogram),
        "warnings": sorted(set(warnings)),
    }


def _time_domain_diagnostics(
    *,
    planning_rows: Sequence[Mapping[str, Any]],
    topic_rows: Sequence[Mapping[str, Any]],
    routing_success_ts: float | None,
    first_nonempty_after_routing_latency_s: float | None,
    planning_summary: Mapping[str, Any],
) -> dict[str, Any]:
    planning_domain = _dominant_time_domain(planning_rows)
    topic_domain = _dominant_time_domain(topic_rows)
    summary_latency = _number(planning_summary.get("routing_first_response_after_last_routing_send_sec"))
    warnings: list[str] = []
    invalid_latency_fields: list[str] = []
    if summary_latency is not None and (summary_latency < -1.0 or summary_latency > 300.0):
        invalid_latency_fields.append("routing_first_response_after_last_routing_send_sec")
        warnings.append("summary_latency_outside_reasonable_range")
    if first_nonempty_after_routing_latency_s is not None and (
        first_nonempty_after_routing_latency_s < -1.0 or first_nonempty_after_routing_latency_s > 300.0
    ):
        invalid_latency_fields.append("first_nonempty_after_routing_latency_s")
        warnings.append("derived_latency_outside_reasonable_range")
    mixed = bool(
        planning_domain.get("domain")
        and topic_domain.get("domain")
        and planning_domain.get("domain") != topic_domain.get("domain")
    )
    if mixed:
        warnings.append("planning_topic_time_domain_mismatch")
    if routing_success_ts is not None and planning_domain.get("domain") == "unknown":
        warnings.append("routing_success_time_domain_unverified")
    status = "insufficient_data" if warnings else "pass"
    return {
        "status": status,
        "planning_time_domain": planning_domain,
        "topic_time_domain": topic_domain,
        "routing_success_timestamp_sec": routing_success_ts,
        "first_nonempty_after_routing_latency_s": first_nonempty_after_routing_latency_s,
        "summary_routing_latency_s": summary_latency,
        "invalid_latency_fields": invalid_latency_fields,
        "mixed_time_domain_detected": mixed,
        "warnings": sorted(set(warnings)),
    }


def _dominant_time_domain(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        source = _time_source(row)
        if source is not None:
            counts[source] += 1
    if not counts:
        return {"domain": "unknown", "field_counts": {}}
    if counts.get("sim_time_sec", 0) >= max(counts.values()):
        return {"domain": "sim_time", "field_counts": dict(sorted(counts.items()))}
    if counts.get("wall_time_sec", 0) or counts.get("timestamp", 0):
        return {"domain": "wall_or_ambiguous", "field_counts": dict(sorted(counts.items()))}
    if counts.get("header_timestamp_sec", 0) or counts.get("planning_header_timestamp_sec", 0):
        return {"domain": "header_timestamp", "field_counts": dict(sorted(counts.items()))}
    return {"domain": "unknown", "field_counts": dict(sorted(counts.items()))}


def _empty_asof_join(
    *,
    planning_rows: Sequence[Mapping[str, Any]],
    topic_rows: Sequence[Mapping[str, Any]],
    reference_rows: Sequence[Mapping[str, Any]],
    projection_rows: Sequence[Mapping[str, Any]],
    tolerance_s: float,
) -> dict[str, Any]:
    empty_times = [
        row_time
        for row in planning_rows
        if _trajectory_point_count(row) <= 0 and (row_time := _row_time(row)) is not None
    ]
    empty_count = len(empty_times)
    localization_rows = [
        row
        for row in topic_rows
        if str(row.get("channel") or "") == "/apollo/localization/pose"
    ]
    chassis_rows = [
        row
        for row in topic_rows
        if str(row.get("channel") or "") == "/apollo/canbus/chassis"
    ]
    loc_matches = _asof_matches(empty_times, localization_rows, tolerance_s=tolerance_s)
    chassis_matches = _asof_matches(empty_times, chassis_rows, tolerance_s=tolerance_s)
    reference_matches = _asof_matches(empty_times, reference_rows, tolerance_s=tolerance_s)
    projection_matches = _asof_matches(empty_times, projection_rows, tolerance_s=tolerance_s)
    hdmap_non_ok_count = sum(
        1
        for match in projection_matches
        if isinstance(match, Mapping)
        and str(match.get("status") or "unknown") not in {"ok", "pass"}
    )
    reference_not_ok_count = sum(
        1
        for match in reference_matches
        if isinstance(match, Mapping)
        and _reference_row_not_ok(match)
    )
    return {
        "tolerance_s": tolerance_s,
        "empty_row_count": empty_count,
        "localization_join_coverage_ratio": _coverage_ratio(loc_matches, empty_count),
        "chassis_join_coverage_ratio": _coverage_ratio(chassis_matches, empty_count),
        "reference_line_join_coverage_ratio": _coverage_ratio(reference_matches, empty_count),
        "hdmap_projection_join_coverage_ratio": _coverage_ratio(projection_matches, empty_count),
        "stale_localization_empty_count": empty_count - sum(1 for match in loc_matches if match is not None),
        "stale_chassis_empty_count": empty_count - sum(1 for match in chassis_matches if match is not None),
        "reference_line_not_ok_empty_count": reference_not_ok_count,
        "hdmap_non_ok_empty_count": hdmap_non_ok_count,
    }


def _asof_matches(
    event_times: Sequence[float],
    rows: Sequence[Mapping[str, Any]],
    *,
    tolerance_s: float,
) -> list[Mapping[str, Any] | None]:
    timed_rows = [
        (row_time, row)
        for row in rows
        if isinstance(row, Mapping) and (row_time := _event_time(row)) is not None
    ]
    result: list[Mapping[str, Any] | None] = []
    for event_time in event_times:
        best_row: Mapping[str, Any] | None = None
        best_delta: float | None = None
        for row_time, row in timed_rows:
            delta = abs(row_time - event_time)
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_row = row
        result.append(best_row if best_delta is not None and best_delta <= tolerance_s else None)
    return result


def _coverage_ratio(matches: Sequence[Mapping[str, Any] | None], denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return float(sum(1 for match in matches if match is not None)) / float(denominator)


def _event_time(row: Mapping[str, Any]) -> float | None:
    source = _time_source(row)
    return _number(row.get(source)) if source else None


def _reference_row_not_ok(row: Mapping[str, Any]) -> bool:
    for key in (
        "status",
        "reference_line_status",
        "reference_line_provider_status",
        "lane_follow_map_status",
    ):
        value = str(row.get(key) or "").strip().lower()
        if value and value not in {"ok", "pass", "valid", "ready"}:
            return True
    count = _int_or_none(row.get("reference_line_count"))
    return count == 0


def _classify_empty_row(
    planning_row: Mapping[str, Any],
    route_row: Mapping[str, Any] | None,
    routing_success_ts: float | None,
) -> str:
    parse_fail = str(planning_row.get("parse_fail_reason") or "").strip()
    if parse_fail:
        return "unknown"
    row_ts = _row_time(planning_row)
    if routing_success_ts is not None and row_ts is not None and row_ts < routing_success_ts:
        return "routing_not_ready"
    if route_row:
        guess = str(route_row.get("planning_empty_reason_guess") or "").strip()
        reference_status = str(route_row.get("reference_line_provider_status") or "").strip()
        map_status = str(route_row.get("lane_follow_map_status") or "").strip()
        route_segments = _int_or_none(route_row.get("route_segment_count"))
        reference_count = _int_or_none(route_row.get("reference_line_count"))
        if "speed_fallback" in guess or "path_fallback" in guess:
            return "path_fallback_or_speed_fallback"
        if (
            guess in {"reference_line_missing", "route_segment_missing"}
            or reference_status == "failed"
            or map_status == "reference_line_missing"
            or reference_count == 0
            or route_segments == 0
        ):
            return "reference_line_provider_not_ready"
    reference_count = _int_or_none(planning_row.get("reference_line_count"))
    route_segments = _int_or_none(planning_row.get("routing_segment_count"))
    if reference_count == 0 or route_segments == 0:
        return "reference_line_provider_not_ready"
    return "unknown"


def _route_establishment(
    *,
    summary_payload: Mapping[str, Any],
    bridge_payload: Mapping[str, Any],
    nonempty_after_routing_ratio: float | None,
    first_nonempty_after_routing_latency_s: float | None,
) -> dict[str, Any]:
    routing_success_count = _int_or_none(
        summary_payload.get("routing_success_count")
        or bridge_payload.get("routing_success_count")
    )
    route_completion = _number(
        summary_payload.get("route_completion")
        or summary_payload.get("route_completion_ratio")
    )
    route_distance = _number(
        summary_payload.get("distance_traveled_m")
        or summary_payload.get("route_distance_achieved_m")
    )
    fail_reason = _text(summary_payload.get("fail_reason") or summary_payload.get("exit_reason"))
    route_established = None
    reasons: list[str] = []
    if routing_success_count is None or routing_success_count < 1:
        route_established = False
        reasons.append("routing_success_missing")
    if nonempty_after_routing_ratio is None:
        route_established = False
        reasons.append("planning_nonempty_after_routing_missing")
    elif nonempty_after_routing_ratio < 0.80:
        route_established = False
        reasons.append("planning_nonempty_after_routing_below_threshold")
    if fail_reason == "ROUTE_ESTABLISHMENT_LATENCY_SEC":
        route_established = False
        reasons.append("route_establishment_latency")
    if route_established is None:
        route_established = True
    return {
        "routing_success_count": routing_success_count,
        "planning_nonempty_after_routing_success_ratio": nonempty_after_routing_ratio,
        "first_nonempty_after_routing_latency_s": first_nonempty_after_routing_latency_s,
        "route_distance_achieved_m": route_distance,
        "route_completion_ratio": route_completion,
        "route_established": route_established,
        "blocking_reasons": sorted(set(reasons)),
    }


def _localization_chassis_ready(topic_rows: Sequence[Mapping[str, Any]]) -> tuple[bool | None, bool | None]:
    if not topic_rows:
        return None, None
    grouped: dict[str, list[float]] = {}
    for row in topic_rows:
        channel = str(row.get("channel") or "")
        if channel not in {"/apollo/localization/pose", "/apollo/canbus/chassis"}:
            continue
        ts = _number(row.get("header_timestamp_sec") or row.get("sim_time_sec"))
        if ts is not None:
            grouped.setdefault(channel, []).append(ts)
    return (
        _channel_ready_from_timestamps(grouped.get("/apollo/localization/pose", [])),
        _channel_ready_from_timestamps(grouped.get("/apollo/canbus/chassis", [])),
    )


def _channel_ready_from_timestamps(timestamps: Sequence[float]) -> bool | None:
    if not timestamps:
        return None
    gaps = [
        (right - left) * 1000.0
        for left, right in zip(timestamps, timestamps[1:])
        if right >= left
    ]
    if not gaps:
        return None
    return max(gaps) <= 250.0


def _rows_by_sequence(rows: Sequence[Mapping[str, Any]]) -> dict[int, Mapping[str, Any]]:
    result: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        seq = _sequence(row)
        if seq is not None:
            result[seq] = row
    return result


def _sequence(row: Mapping[str, Any]) -> int | None:
    return _int_or_none(row.get("planning_header_sequence_num"))


def _row_time(row: Mapping[str, Any]) -> float | None:
    source = _time_source(row)
    return _number(row.get(source)) if source else None


def _time_source(row: Mapping[str, Any]) -> str | None:
    for key in (
        "sim_time_sec",
        "sim_time",
        "planning_sim_time_sec",
        "header_timestamp_sec",
        "planning_header_timestamp_sec",
        "wall_time_sec",
        "timestamp",
    ):
        if _number(row.get(key)) is not None:
            return key
    return None


def _trajectory_point_count(row: Mapping[str, Any]) -> int:
    return _int_or_none(
        row.get("trajectory_point_count")
        or row.get("last_trajectory_point_count")
    ) or 0


def _nonempty_ratio(rows: Sequence[Mapping[str, Any]]) -> float | None:
    if not rows:
        return None
    return float(sum(1 for row in rows if _trajectory_point_count(row) > 0)) / float(len(rows))


def _first_nonempty_timestamp(rows: Sequence[Mapping[str, Any]]) -> float | None:
    for row in rows:
        if _trajectory_point_count(row) > 0:
            return _row_time(row)
    return None


def _last_nonempty_timestamp(rows: Sequence[Mapping[str, Any]]) -> float | None:
    for row in reversed(list(rows)):
        if _trajectory_point_count(row) > 0:
            return _row_time(row)
    return None


def _first_message_timestamp(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> float | None:
    if rows:
        return _row_time(rows[0])
    return _number(summary.get("first_msg_ts_sec"))


def _longest_empty_streak(rows: Sequence[Mapping[str, Any]]) -> int | None:
    if not rows:
        return None
    longest = 0
    current = 0
    for row in rows:
        if _trajectory_point_count(row) <= 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _planning_status_topk(rows: Sequence[Mapping[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in rows:
        for key in (
            "planning_empty_reason_guess",
            "reference_line_provider_status",
            "create_route_segments_status",
            "lane_follow_map_status",
        ):
            value = str(row.get(key) or "").strip()
            if value:
                counter[f"{key}:{value}"] += 1
    return [{"status": key, "count": count} for key, count in counter.most_common(limit)]


def _top_log_errors(paths: Iterable[Path], *, limit: int = 8) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        try:
            with path.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    upper = line.upper()
                    if "ERROR" not in upper and "WARNING" not in upper and "FATAL" not in upper:
                        continue
                    text = " ".join(line.strip().split())
                    if text:
                        counter[text[:220]] += 1
        except OSError:
            continue
    return [{"message": key, "count": count} for key, count in counter.most_common(limit)]


def _verdict(nonempty_ratio: float | None, missing_fields: Sequence[str]) -> str:
    if missing_fields or nonempty_ratio is None:
        return "insufficient_data"
    if nonempty_ratio >= 0.80:
        return "pass"
    if nonempty_ratio >= 0.50:
        return "warn"
    return "fail"


def _find_first(root: Path, rels: Sequence[str]) -> Path | None:
    for rel in rels:
        candidate = root / rel
        if candidate.is_file():
            return candidate
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path | None) -> list[Mapping[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[Mapping[str, Any]] = []
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, Mapping):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _int_or_none(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _first_number(*items: Any) -> float | None:
    if len(items) % 2 != 0:
        return None
    for index in range(0, len(items), 2):
        mapping, key = items[index], items[index + 1]
        if isinstance(mapping, Mapping):
            value = _number(mapping.get(key))
            if value is not None:
                return value
    return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _fmt_ratio(value: Any) -> str:
    number = _number(value)
    return "null" if number is None else f"{number:.3f}"


def _fmt_asof(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "not evaluated"
    empty_count = value.get("empty_row_count")
    loc = _fmt_ratio(value.get("localization_join_coverage_ratio"))
    chassis = _fmt_ratio(value.get("chassis_join_coverage_ratio"))
    reference = _fmt_ratio(value.get("reference_line_join_coverage_ratio"))
    hdmap = _fmt_ratio(value.get("hdmap_projection_join_coverage_ratio"))
    return (
        f"empty={empty_count}, loc={loc}, chassis={chassis}, "
        f"reference_line={reference}, hdmap={hdmap}"
    )


def _fmt_time_domain(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "not evaluated"
    return (
        f"status={value.get('status')}, "
        f"planning={_nested_domain(value.get('planning_time_domain'))}, "
        f"topic={_nested_domain(value.get('topic_time_domain'))}, "
        f"mixed={value.get('mixed_time_domain_detected')}"
    )


def _fmt_freshness(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "not evaluated"
    return (
        f"status={value.get('status')}, "
        f"verified={value.get('input_freshness_verified_empty_count')}, "
        f"unverified={value.get('input_freshness_unverified_empty_count')}"
    )


def _nested_domain(value: Any) -> str:
    return str(value.get("domain")) if isinstance(value, Mapping) else "unknown"
