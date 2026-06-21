from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

FAILURE_TIMELINE_SCHEMA_VERSION = "town01_failure_timeline.v1"
DEFAULT_WINDOW_ROWS = 60
DEFAULT_START_GATE_M = 5.0
SAFETY_EVENT_TYPES = {"lane_invasion", "collision"}


def analyze_failure_timeline_run_dir(
    run_dir: str | Path,
    *,
    window_rows: int = DEFAULT_WINDOW_ROWS,
    start_gate_m: float = DEFAULT_START_GATE_M,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    manifest_path = _find_first(root, ["manifest.json"])
    summary_path = _find_first(root, ["summary.json"])
    events_path = _find_first(root, ["events.jsonl"])
    timeseries_path = _find_first(root, ["timeseries.csv", "timeseries.jsonl"])
    route_health_path = _find_first(
        root,
        [
            "analysis/route_health/route_health.json",
            "route_health.json",
        ],
    )
    control_health_path = _find_first(
        root,
        [
            "analysis/control_health/control_health_report.json",
            "control_health_report.json",
        ],
    )

    manifest = _read_json(manifest_path)
    summary = _read_json(summary_path)
    route_health = _read_json(route_health_path)
    control_health = _read_json(control_health_path)
    rows = _read_rows(timeseries_path)
    raw_events = _read_events(events_path)

    missing_inputs = []
    if not summary_path:
        missing_inputs.append("summary")
    if not timeseries_path:
        missing_inputs.append("timeseries")
    if not route_health_path:
        missing_inputs.append("route_health")
    if not control_health_path:
        missing_inputs.append("control_health")

    key_events = _build_key_events(
        summary=summary,
        raw_events=raw_events,
        route_health=route_health,
        control_health=control_health,
        rows=rows,
    )
    event_order = _sort_events(key_events)
    anchor = _choose_anchor_event(event_order, summary)
    window_summary = _window_summary(rows, anchor, window_rows=window_rows)
    route_start_gate = _route_start_gate(anchor, window_summary, start_gate_m=start_gate_m)
    ordering_findings = _ordering_findings(event_order, anchor, route_start_gate=route_start_gate)
    missing_fields = _missing_fields(rows, route_health)
    warnings = _warnings(
        rows=rows,
        anchor=anchor,
        missing_inputs=missing_inputs,
        event_order=event_order,
    )
    status = _status(missing_inputs=missing_inputs, anchor=anchor, rows=rows)

    return {
        "schema_version": FAILURE_TIMELINE_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id", default=root.name),
        "route_id": _first_text(summary, "route_id", manifest, "route_id", route_health, "route_id"),
        "scenario_class": _first_text(summary, "scenario_class", manifest, "scenario_class"),
        "status": status,
        "primary_failure": _primary_failure(summary, event_order),
        "anchor_event": anchor,
        "key_events": key_events,
        "event_order": event_order,
        "ordering_findings": ordering_findings,
        "window_summary": window_summary,
        "route_start_gate": route_start_gate,
        "missing_inputs": sorted(set(missing_inputs)),
        "missing_fields": sorted(set(missing_fields)),
        "warnings": sorted(set(warnings)),
        "source": {
            "run_dir": str(root),
            "manifest_path": str(manifest_path) if manifest_path else None,
            "summary_path": str(summary_path) if summary_path else None,
            "events_path": str(events_path) if events_path else None,
            "timeseries_path": str(timeseries_path) if timeseries_path else None,
            "route_health_path": str(route_health_path) if route_health_path else None,
            "control_health_path": str(control_health_path) if control_health_path else None,
        },
        "interpretation_boundary": (
            "Failure timeline orders artifact evidence only. Row indices derived from summary steps "
            "or Apollo sequence numbers are approximate unless the source explicitly provides a "
            "synchronized frame id. This report does not prove full Apollo perception/localization "
            "or that a curve/lane failure root cause is solved."
        ),
    }


def write_failure_timeline_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "failure_timeline_report.json"
    md_path = output_dir / "failure_timeline_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "failure_timeline_report": str(json_path),
        "failure_timeline_summary": str(md_path),
    }


def _build_key_events(
    *,
    summary: Mapping[str, Any],
    raw_events: Sequence[Mapping[str, Any]],
    route_health: Mapping[str, Any],
    control_health: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    events.extend(_safety_events(raw_events, summary, rows))
    events.extend(_apollo_semantic_events(route_health, rows))
    control_event = _control_health_event(control_health)
    if control_event:
        events.append(control_event)
    summary_event = _summary_failure_event(summary, rows)
    if summary_event:
        events.append(summary_event)
    return events


def _safety_events(
    raw_events: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for raw in raw_events:
        event_type = str(raw.get("event_type") or raw.get("event") or "").strip()
        if event_type not in SAFETY_EVENT_TYPES:
            continue
        row_index = _int(raw.get("row_index"), raw.get("step"), raw.get("frame_id"), raw.get("seq"))
        event = _event(
            event_type=event_type,
            source=str(raw.get("source") or "events.jsonl"),
            row_index=row_index,
            timestamp_sec=_num(
                raw.get("sim_time"),
                raw.get("timestamp_sec"),
                raw.get("at"),
                raw.get("t"),
                raw.get("timestamp"),
            ),
            context_source=str(raw.get("context_source") or "events.jsonl"),
            details={key: value for key, value in raw.items() if key not in {"event", "event_type"}},
        )
        events.append(_attach_row_context(event, rows))
    if any(event["event_type"] in SAFETY_EVENT_TYPES for event in events):
        return events

    first_failure_step = _int(summary.get("first_failure_step"), summary.get("failure_step"))
    lane_count = _num(summary.get("lane_invasion_count"), _summary_metric(summary, "lane_invasion_count"))
    collision_count = _num(summary.get("collision_count"), _summary_metric(summary, "collision_count"))
    if lane_count and lane_count > 0:
        events.append(
            _attach_row_context(
                _event(
                    event_type="lane_invasion",
                    source="summary.json",
                    row_index=first_failure_step,
                    timestamp_sec=None,
                    context_source="summary.first_failure_step",
                    details={"lane_invasion_count": lane_count},
                ),
                rows,
            )
        )
    if collision_count and collision_count > 0:
        events.append(
            _attach_row_context(
                _event(
                    event_type="collision",
                    source="summary.json",
                    row_index=first_failure_step,
                    timestamp_sec=None,
                    context_source="summary.first_failure_step",
                    details={"collision_count": collision_count},
                ),
                rows,
            )
        )
    return events


def _apollo_semantic_events(
    route_health: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    semantics = route_health.get("apollo_semantics")
    if not isinstance(semantics, Mapping):
        return []
    events: list[dict[str, Any]] = []
    first_high = semantics.get("first_high_steer")
    if isinstance(first_high, Mapping):
        events.append(
            _attach_row_context(
                _event(
                    event_type="first_high_steer",
                    source="route_health.apollo_semantics",
                    row_index=_int(first_high.get("row_index"), first_high.get("seq"), first_high.get("frame_id")),
                    timestamp_sec=_num(first_high.get("sim_time"), first_high.get("at"), first_high.get("timestamp_sec")),
                    context_source="apollo_sequence_or_row_index",
                    details=dict(first_high),
                ),
                rows,
            )
        )
    for event_type, field_name in (
        ("matched_point_anomaly", "matched_point_anomaly_locations"),
        ("target_point_anomaly", "target_point_anomaly_locations"),
    ):
        for index in semantics.get(field_name) or []:
            events.append(
                _attach_row_context(
                    _event(
                        event_type=event_type,
                        source=f"route_health.apollo_semantics.{field_name}",
                        row_index=_int(index),
                        timestamp_sec=None,
                        context_source="route_health_index",
                        details={"location": index},
                    ),
                    rows,
                )
            )
    for event_type, field_name in (
        ("matched_point_too_large", "first_matched_point_too_large"),
        ("target_point_jump", "first_target_point_jump"),
        ("target_point_too_large", "first_target_point_too_large"),
    ):
        payload = semantics.get(field_name)
        if isinstance(payload, Mapping):
            events.append(
                _attach_row_context(
                    _event(
                        event_type=event_type,
                        source=f"route_health.apollo_semantics.{field_name}",
                        row_index=_int(payload.get("row_index"), payload.get("seq"), payload.get("frame_id")),
                        timestamp_sec=_num(payload.get("sim_time"), payload.get("at"), payload.get("timestamp_sec")),
                        context_source="apollo_sequence_or_row_index",
                        details=dict(payload),
                    ),
                    rows,
                )
            )
    return events


def _control_health_event(control_health: Mapping[str, Any]) -> dict[str, Any] | None:
    status = str(control_health.get("status") or "").strip()
    if not status or status == "pass":
        return None
    return _event(
        event_type="control_health",
        source="control_health_report.json",
        row_index=None,
        timestamp_sec=None,
        context_source="control_health_report",
        details={
            "status": status,
            "failure_reason": control_health.get("failure_reason"),
            "warnings": control_health.get("warnings") or [],
        },
    )


def _summary_failure_event(summary: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    reason = summary.get("exit_reason") or summary.get("failure_reason")
    first_failure_step = _int(summary.get("first_failure_step"), summary.get("failure_step"))
    if not reason and first_failure_step is None:
        return None
    return _attach_row_context(
        _event(
            event_type="summary_failure",
            source="summary.json",
            row_index=first_failure_step,
            timestamp_sec=None,
            context_source="summary.first_failure_step",
            details={"exit_reason": reason, "first_failure_step": first_failure_step},
        ),
        rows,
    )


def _event(
    *,
    event_type: str,
    source: str,
    row_index: int | None,
    timestamp_sec: float | None,
    context_source: str,
    details: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "source": source,
        "row_index": row_index,
        "timestamp_sec": timestamp_sec,
        "context_source": context_source,
        "details": dict(details),
    }


def _attach_row_context(event: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    enriched = dict(event)
    row_index = _event_row_index(enriched, rows)
    if row_index is None or row_index < 0 or row_index >= len(rows):
        return enriched
    row = rows[row_index]
    enriched["row_index"] = row_index
    enriched["row_context"] = {
        "sim_time": _num(row.get("sim_time"), row.get("timestamp_sec")),
        "frame_id": _int(row.get("frame_id")),
        "route_s": _num(row.get("route_s")),
        "nearest_route_index": _int(row.get("nearest_route_index"), row.get("route_index")),
        "cross_track_error": _num(row.get("cross_track_error"), row.get("lateral_error")),
        "heading_error": _num(row.get("heading_error")),
        "ego_speed": _num(row.get("ego_speed"), row.get("speed_mps")),
        "apollo_steer_raw": _num(row.get("apollo_steer_raw")),
        "bridge_steer_mapped": _num(row.get("bridge_steer_mapped")),
        "carla_steer_applied": _num(row.get("carla_steer_applied")),
        "matched_point_distance": _num(
            row.get("apollo_matched_point_distance"),
            row.get("matched_point_distance"),
        ),
        "target_point_distance": _num(
            row.get("apollo_target_point_distance"),
            row.get("target_point_distance"),
        ),
    }
    return enriched


def _event_row_index(event: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> int | None:
    row_index = _int(event.get("row_index"))
    if row_index is not None:
        return row_index
    timestamp = _num(event.get("timestamp_sec"))
    if timestamp is None:
        return None
    best_index = None
    best_delta = None
    for index, row in enumerate(rows):
        row_time = _num(row.get("sim_time"), row.get("timestamp_sec"))
        if row_time is None:
            continue
        delta = abs(row_time - timestamp)
        if best_delta is None or delta < best_delta:
            best_index = index
            best_delta = delta
    return best_index


def _sort_events(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    indexed = [(idx, dict(event)) for idx, event in enumerate(events)]
    indexed.sort(
        key=lambda pair: (
            _sort_number(pair[1].get("row_index")),
            _sort_number(pair[1].get("timestamp_sec")),
            pair[0],
        )
    )
    return [event for _, event in indexed]


def _choose_anchor_event(
    event_order: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> dict[str, Any] | None:
    for event in event_order:
        if event.get("event_type") in SAFETY_EVENT_TYPES:
            anchor = dict(event)
            anchor["anchor_reason"] = "first_safety_event"
            return anchor
    first_failure_step = _int(summary.get("first_failure_step"), summary.get("failure_step"))
    if first_failure_step is not None:
        return _event(
            event_type="summary_failure",
            source="summary.json",
            row_index=first_failure_step,
            timestamp_sec=None,
            context_source="summary.first_failure_step",
            details={"exit_reason": summary.get("exit_reason") or summary.get("failure_reason")},
        ) | {"anchor_reason": "summary_first_failure_step"}
    for event in event_order:
        if event.get("event_type") in {
            "first_high_steer",
            "matched_point_anomaly",
            "matched_point_too_large",
            "target_point_anomaly",
            "target_point_jump",
            "target_point_too_large",
        }:
            anchor = dict(event)
            anchor["anchor_reason"] = "first_apollo_semantic_event"
            return anchor
    return None


def _window_summary(
    rows: Sequence[Mapping[str, Any]],
    anchor: Mapping[str, Any] | None,
    *,
    window_rows: int,
) -> dict[str, Any] | None:
    if not rows or not anchor:
        return None
    anchor_index = _event_row_index(anchor, rows)
    if anchor_index is None:
        return None
    start = max(0, anchor_index - window_rows)
    end = min(len(rows) - 1, anchor_index + window_rows)
    window = rows[start : end + 1]
    return {
        "anchor_row_index": anchor_index,
        "start_row_index": start,
        "end_row_index": end,
        "row_count": len(window),
        "route_s_min": _min(_series(window, "route_s")),
        "route_s_max": _max(_series(window, "route_s")),
        "cross_track_error_abs_p95": _percentile(_series(window, "cross_track_error", "lateral_error", absolute=True), 0.95),
        "cross_track_error_abs_max": _max(_series(window, "cross_track_error", "lateral_error", absolute=True)),
        "heading_error_abs_p95": _percentile(_series(window, "heading_error", absolute=True), 0.95),
        "heading_error_abs_max": _max(_series(window, "heading_error", absolute=True)),
        "apollo_steer_raw_abs_p95": _percentile(_series(window, "apollo_steer_raw", absolute=True), 0.95),
        "apollo_steer_raw_abs_max": _max(_series(window, "apollo_steer_raw", absolute=True)),
        "bridge_steer_mapped_abs_p95": _percentile(_series(window, "bridge_steer_mapped", absolute=True), 0.95),
        "carla_steer_applied_abs_p95": _percentile(_series(window, "carla_steer_applied", absolute=True), 0.95),
        "matched_point_distance_max": _max(
            _series(window, "apollo_matched_point_distance", "matched_point_distance", absolute=True)
        ),
        "target_point_distance_max": _max(
            _series(window, "apollo_target_point_distance", "target_point_distance", absolute=True)
        ),
        "ego_speed_mean": _mean(_series(window, "ego_speed", "speed_mps")),
        "ego_speed_max": _max(_series(window, "ego_speed", "speed_mps")),
        "lateral_guard_apply_count": _bool_count(window, "lateral_guard_applied"),
        "trajectory_contract_guard_apply_count": _bool_count(
            window,
            "trajectory_contract_guard_applied",
            "trajectory_contract_lateral_guard_applied",
        ),
    }


def _route_start_gate(
    anchor: Mapping[str, Any] | None,
    window_summary: Mapping[str, Any] | None,
    *,
    start_gate_m: float,
) -> dict[str, Any]:
    anchor_context = anchor.get("row_context") if isinstance(anchor, Mapping) else None
    anchor_route_s = _num(anchor_context.get("route_s")) if isinstance(anchor_context, Mapping) else None
    window_min = _num(window_summary.get("route_s_min")) if isinstance(window_summary, Mapping) else None
    window_max = _num(window_summary.get("route_s_max")) if isinstance(window_summary, Mapping) else None
    anchor_before_start = anchor_route_s is not None and anchor_route_s < 0.0
    anchor_near_start = anchor_route_s is not None and abs(anchor_route_s) <= float(start_gate_m)
    window_crosses_start = window_min is not None and window_max is not None and window_min <= 0.0 <= window_max
    if anchor_route_s is None:
        status = "insufficient_data"
        reason = "anchor_route_s_missing"
    elif anchor_before_start:
        status = "warn"
        reason = "anchor_before_route_start"
    elif anchor_near_start or window_crosses_start:
        status = "warn"
        reason = "anchor_near_route_start"
    else:
        status = "pass"
        reason = None
    return {
        "status": status,
        "reason": reason,
        "start_gate_m": float(start_gate_m),
        "route_s_at_anchor": anchor_route_s,
        "window_route_s_min": window_min,
        "window_route_s_max": window_max,
        "anchor_before_route_start": anchor_before_start if anchor_route_s is not None else None,
        "anchor_near_route_start": anchor_near_start if anchor_route_s is not None else None,
        "window_crosses_route_start": window_crosses_start if window_min is not None and window_max is not None else None,
        "interpretation_boundary": (
            "Start-gate status is a diagnostic hint. It can suggest spawn/route-start alignment work, "
            "but it does not by itself prove the root cause of a lane, curve, or control failure."
        ),
    }


def _ordering_findings(
    event_order: Sequence[Mapping[str, Any]],
    anchor: Mapping[str, Any] | None,
    *,
    route_start_gate: Mapping[str, Any],
) -> list[str]:
    findings: list[str] = []
    anchor_index = _event_order_index(event_order, anchor)
    if anchor_index is None:
        return findings
    before = {str(event.get("event_type")) for event in event_order[:anchor_index]}
    after = {str(event.get("event_type")) for event in event_order[anchor_index + 1 :]}
    anchor_type = anchor.get("event_type")
    if anchor_type in SAFETY_EVENT_TYPES:
        if route_start_gate.get("anchor_before_route_start") is True:
            findings.append("safety_event_before_route_start")
        if route_start_gate.get("anchor_near_route_start") is True:
            findings.append("safety_event_near_route_start")
        if "first_high_steer" in before:
            findings.append("first_high_steer_before_safety_event")
        if {"matched_point_anomaly", "matched_point_too_large"} & before:
            findings.append("matched_point_anomaly_before_safety_event")
        if {"matched_point_anomaly", "matched_point_too_large"} & after:
            findings.append("safety_event_before_matched_point_anomaly")
        if {"target_point_anomaly", "target_point_jump", "target_point_too_large"} & before:
            findings.append("target_point_anomaly_before_safety_event")
        if {"target_point_anomaly", "target_point_jump", "target_point_too_large"} & after:
            findings.append("safety_event_before_target_point_anomaly")
    if any(event.get("event_type") == "control_health" for event in event_order):
        findings.append("control_health_nonpass_present")
    return findings


def _event_order_index(
    event_order: Sequence[Mapping[str, Any]],
    anchor: Mapping[str, Any] | None,
) -> int | None:
    if not anchor:
        return None
    for index, event in enumerate(event_order):
        if (
            event.get("event_type") == anchor.get("event_type")
            and event.get("row_index") == anchor.get("row_index")
            and event.get("source") == anchor.get("source")
        ):
            return index
    return None


def _primary_failure(summary: Mapping[str, Any], event_order: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    for event in event_order:
        if event.get("event_type") in SAFETY_EVENT_TYPES:
            return {
                "failure_reason": event.get("event_type"),
                "source": event.get("source"),
                "row_index": event.get("row_index"),
            }
    reason = summary.get("exit_reason") or summary.get("failure_reason")
    return {
        "failure_reason": reason,
        "source": "summary.json" if reason else None,
        "row_index": _int(summary.get("first_failure_step"), summary.get("failure_step")),
    }


def _missing_fields(rows: Sequence[Mapping[str, Any]], route_health: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    if rows:
        fields = set(rows[0].keys())
        for field in (
            "cross_track_error",
            "heading_error",
            "apollo_steer_raw",
            "bridge_steer_mapped",
            "carla_steer_applied",
            "route_s",
        ):
            if field not in fields:
                missing.append(f"timeseries.{field}")
    else:
        missing.extend(
            [
                "timeseries.cross_track_error",
                "timeseries.heading_error",
                "timeseries.apollo_steer_raw",
                "timeseries.route_s",
            ]
        )
    semantics = route_health.get("apollo_semantics")
    if not isinstance(semantics, Mapping):
        missing.append("route_health.apollo_semantics")
    return missing


def _warnings(
    *,
    rows: Sequence[Mapping[str, Any]],
    anchor: Mapping[str, Any] | None,
    missing_inputs: Sequence[str],
    event_order: Sequence[Mapping[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    if missing_inputs:
        warnings.append("timeline_has_missing_inputs")
    if anchor is None:
        warnings.append("anchor_event_missing")
    if not rows:
        warnings.append("timeseries_context_missing")
    if not any(event.get("event_type") in SAFETY_EVENT_TYPES for event in event_order):
        warnings.append("safety_event_missing")
    if any(str(event.get("context_source") or "").endswith("first_failure_step") for event in event_order):
        warnings.append("summary_step_context_is_approximate")
    if any(str(event.get("context_source") or "").startswith("apollo_sequence") for event in event_order):
        warnings.append("apollo_sequence_context_is_approximate")
    return warnings


def _status(
    *,
    missing_inputs: Sequence[str],
    anchor: Mapping[str, Any] | None,
    rows: Sequence[Mapping[str, Any]],
) -> str:
    if anchor is None:
        return "insufficient_data"
    if not rows or "timeseries" in missing_inputs:
        return "warn"
    if missing_inputs:
        return "warn"
    return "pass"


def _markdown(report: Mapping[str, Any]) -> str:
    primary = report.get("primary_failure") if isinstance(report.get("primary_failure"), Mapping) else {}
    anchor = report.get("anchor_event") if isinstance(report.get("anchor_event"), Mapping) else {}
    window = report.get("window_summary") if isinstance(report.get("window_summary"), Mapping) else {}
    route_start = report.get("route_start_gate") if isinstance(report.get("route_start_gate"), Mapping) else {}
    lines = [
        "# Town01 Failure Timeline",
        "",
        f"- status: `{report.get('status')}`",
        f"- run_id: `{report.get('run_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- scenario_class: `{report.get('scenario_class')}`",
        f"- primary_failure: `{primary.get('failure_reason')}`",
        f"- anchor_event: `{anchor.get('event_type')}` at row `{anchor.get('row_index')}`",
        "",
        "## Ordering Findings",
        "",
    ]
    findings = list(report.get("ordering_findings") or [])
    if findings:
        lines.extend(f"- `{item}`" for item in findings)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Anchor Window",
            "",
            f"- rows: `{window.get('start_row_index')}` to `{window.get('end_row_index')}`",
            f"- route_s: `{window.get('route_s_min')}` to `{window.get('route_s_max')}`",
            f"- cross_track_error_abs_p95: `{window.get('cross_track_error_abs_p95')}`",
            f"- heading_error_abs_p95: `{window.get('heading_error_abs_p95')}`",
            f"- apollo_steer_raw_abs_p95: `{window.get('apollo_steer_raw_abs_p95')}`",
            "",
            "## Route Start Gate",
            "",
            f"- status: `{route_start.get('status')}`",
            f"- reason: `{route_start.get('reason')}`",
            f"- route_s_at_anchor: `{route_start.get('route_s_at_anchor')}`",
            f"- anchor_before_route_start: `{route_start.get('anchor_before_route_start')}`",
            f"- anchor_near_route_start: `{route_start.get('anchor_near_route_start')}`",
            "",
            "## Event Order",
            "",
            "| event_type | row_index | source | context_source |",
            "| --- | --- | --- | --- |",
        ]
    )
    for event in report.get("event_order") or []:
        if not isinstance(event, Mapping):
            continue
        lines.append(
            f"| {event.get('event_type')} | {event.get('row_index')} | "
            f"{event.get('source')} | {event.get('context_source')} |"
        )
    lines.extend(
        [
            "",
            "This is an offline artifact-ordering report. It does not prove full Apollo "
            "perception/localization reproduction and does not by itself prove root cause.",
            "",
        ]
    )
    return "\n".join(lines)


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relative_paths}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_events(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                events.append(payload)
    return events


def _read_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix.lower() == ".jsonl":
        return _read_events(path)
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _series(
    rows: Sequence[Mapping[str, Any]],
    *fields: str,
    absolute: bool = False,
) -> list[float]:
    values: list[float] = []
    for row in rows:
        for field in fields:
            value = _num(row.get(field))
            if value is not None:
                values.append(abs(value) if absolute else value)
                break
    return values


def _summary_metric(summary: Mapping[str, Any], name: str) -> Any:
    metrics = summary.get("metrics")
    if isinstance(metrics, Mapping):
        return metrics.get(name)
    return None


def _first_text(*items: Any, default: str | None = None) -> str | None:
    index = 0
    while index + 1 < len(items):
        source = items[index]
        key = items[index + 1]
        if isinstance(source, Mapping):
            value = source.get(str(key))
            if value not in {None, ""}:
                return str(value)
        index += 2
    return default


def _num(*values: Any) -> float | None:
    for value in values:
        if value in {None, ""}:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number != number:
            continue
        return number
    return None


def _int(*values: Any) -> int | None:
    value = _num(*values)
    if value is None:
        return None
    return int(value)


def _mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _min(values: Sequence[float]) -> float | None:
    return min(values) if values else None


def _max(values: Sequence[float]) -> float | None:
    return max(values) if values else None


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = percentile * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    ratio = position - lower
    return ordered[lower] * (1.0 - ratio) + ordered[upper] * ratio


def _bool_count(rows: Sequence[Mapping[str, Any]], *fields: str) -> int:
    count = 0
    for row in rows:
        for field in fields:
            if _truthy(row.get(field)):
                count += 1
                break
    return count


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in {None, ""}:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _sort_number(value: Any) -> float:
    number = _num(value)
    return number if number is not None else float("inf")
