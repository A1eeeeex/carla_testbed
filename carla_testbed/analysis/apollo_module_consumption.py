from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

APOLLO_MODULE_CONSUMPTION_SCHEMA_VERSION = "apollo_module_consumption.v1"

TIMEOUT_PATTERNS = {
    "localization_timeout": re.compile(r"locali[sz]ation.*(timeout|time out|not ready)", re.I),
    "chassis_timeout": re.compile(r"chassis.*(timeout|time out|not ready)", re.I),
    "control_timeout": re.compile(r"control.*(timeout|time out|not ready)", re.I),
    "planning_timeout": re.compile(r"planning.*(timeout|time out|not ready)", re.I),
    "reference_line_provider_failure": re.compile(r"reference.?line.*(fail|error|not ready|empty)", re.I),
    "route_or_reference_line_failure": re.compile(
        r"(adc_route_index error|can not get distance|planning failed:planning_error|"
        r"planner failed|lane_follow_map\.cc|on_lane_planning\.cc|"
        r"create_route_segments_status failed|reference_line_provider_status failed)",
        re.I,
    ),
    "prediction_not_ready": re.compile(r"prediction.*(not ready|timeout|empty|missing)", re.I),
    "obstacle_invalid": re.compile(r"obstacle.*(invalid|missing|empty|error)", re.I),
}


def analyze_apollo_module_consumption_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    inputs = {
        "summary": _find_first(root, ["summary.json"]),
        "planning_materialization": _find_first(
            root,
            [
                "analysis/planning_materialization/planning_materialization_report.json",
                "planning_materialization_report.json",
            ],
        ),
        "planning_topic_debug_summary": _find_first(
            root,
            ["artifacts/planning_topic_debug_summary.json", "planning_topic_debug_summary.json"],
        ),
        "planning_topic_debug": _find_first(
            root,
            ["artifacts/planning_topic_debug.jsonl", "planning_topic_debug.jsonl"],
        ),
        "control_decode_debug": _find_first(
            root,
            ["artifacts/control_decode_debug.jsonl", "artifacts/bridge_control_decode.jsonl", "control_decode_debug.jsonl"],
        ),
        "routing_event_debug": _find_first(
            root,
            ["artifacts/routing_event_debug.jsonl", "routing_event_debug.jsonl"],
        ),
        "topic_publish_stats": _find_first(
            root,
            ["artifacts/topic_publish_stats.jsonl", "topic_publish_stats.jsonl"],
        ),
        "apollo_log": _find_first(
            root,
            ["artifacts/apollo.log", "apollo.log", "logs/apollo.log"],
        ),
        "prediction_evidence": _find_first(
            root,
            ["analysis/prediction_evidence/prediction_evidence_report.json", "prediction_evidence_report.json"],
        ),
    }
    return analyze_apollo_module_consumption(inputs, run_dir=root)


def analyze_apollo_module_consumption(
    inputs: Mapping[str, Path | None],
    *,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    summary = _read_json(inputs.get("summary"))
    planning_materialization = _read_json(inputs.get("planning_materialization"))
    planning_summary = _read_json(inputs.get("planning_topic_debug_summary"))
    prediction_evidence = _read_json(inputs.get("prediction_evidence"))
    planning_rows = _read_jsonl(inputs.get("planning_topic_debug"))
    control_rows = _read_jsonl(inputs.get("control_decode_debug"))
    routing_rows = _read_jsonl(inputs.get("routing_event_debug"))
    topic_rows = _read_jsonl(inputs.get("topic_publish_stats"))
    log_lines = _read_lines(inputs.get("apollo_log"))

    text_events = _text_events(
        planning_rows,
        control_rows,
        routing_rows,
        log_lines,
        _planning_materialization_log_errors(planning_materialization),
    )
    pattern_counts = _pattern_counts(text_events)
    empty_reason_histogram = _empty_reason_histogram(planning_materialization, planning_rows)
    age_metrics = _planning_age_metrics(planning_rows)
    routing_consumed = _routing_consumed(
        summary=summary,
        planning_materialization=planning_materialization,
        planning_summary=planning_summary,
        planning_rows=planning_rows,
        routing_rows=routing_rows,
    )
    publish_coverage = _topic_publish_coverage(topic_rows)
    prediction_mode = prediction_evidence.get("prediction_mode")

    missing = [
        name
        for name in ("planning_materialization", "planning_topic_debug_summary")
        if inputs.get(name) is None
    ]
    warnings: list[str] = []
    blocking: list[str] = []
    if missing:
        blocking.extend(f"{name}_missing" for name in missing)
    if routing_consumed is False:
        blocking.append("routing_response_not_consumed_by_planning")
    if routing_consumed is None and _route_establishment_false(planning_materialization):
        blocking.append("routing_response_consumption_unconfirmed")
    if int(empty_reason_histogram.get("reference_line_provider_not_ready", 0) or 0) > 0:
        blocking.append("reference_line_provider_not_ready_empty_planning")
    if pattern_counts["localization_timeout"] or pattern_counts["chassis_timeout"]:
        blocking.append("planning_input_timeout_logs_present")
    if pattern_counts["reference_line_provider_failure"]:
        blocking.append("reference_line_provider_failure_logs_present")
    if pattern_counts["route_or_reference_line_failure"]:
        blocking.append("route_or_reference_line_failure_logs_present")
    freshness = planning_materialization.get("input_freshness_attribution")
    if isinstance(freshness, Mapping) and freshness.get("status") == "insufficient_data":
        warnings.append("planning_input_freshness_unverified")
    if pattern_counts["prediction_not_ready"] and prediction_mode not in {"not_required_for_case"}:
        warnings.append("prediction_not_ready_logs_present")
    if not topic_rows:
        warnings.append("topic_publish_stats_missing")

    if blocking:
        status = "insufficient_data" if any(item.endswith("_missing") for item in blocking) else "fail"
    elif warnings:
        status = "warn"
    else:
        status = "pass"

    return {
        "schema_version": APOLLO_MODULE_CONSUMPTION_SCHEMA_VERSION,
        "run_id": summary.get("run_id"),
        "route_id": summary.get("route_id"),
        "scenario_id": summary.get("scenario_id"),
        "status": status,
        "routing_response_consumed_by_planning": routing_consumed,
        "planning_requires_prediction": prediction_evidence.get("planning_requires_prediction"),
        "prediction_mode": prediction_mode,
        "pattern_counts": dict(pattern_counts),
        "empty_reason_histogram": empty_reason_histogram,
        "planning_input_age": age_metrics,
        "input_freshness_attribution": freshness if isinstance(freshness, Mapping) else {},
        "topic_publish_coverage": publish_coverage,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_inputs": sorted(set(missing)),
        "source": {
            "run_dir": str(Path(run_dir).expanduser()) if run_dir else None,
            **{name: str(path) if path else None for name, path in inputs.items()},
        },
        "interpretation_boundary": (
            "This report checks Apollo module-consumption evidence from logs/debug artifacts. "
            "Bridge-side publish stats alone are not proof that Planning or Control consumed inputs."
        ),
    }


def write_apollo_module_consumption_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "apollo_module_consumption_report.json"
    summary_path = output / "apollo_module_consumption_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_module_consumption_summary_md(report), encoding="utf-8")
    return {
        "apollo_module_consumption_report": str(json_path),
        "apollo_module_consumption_summary": str(summary_path),
    }


def apollo_module_consumption_summary_md(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Apollo Module Consumption Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Routing response consumed by planning: `{report.get('routing_response_consumed_by_planning')}`",
            f"- Prediction mode: `{report.get('prediction_mode')}`",
            f"- Pattern counts: `{json.dumps(report.get('pattern_counts') or {}, sort_keys=True)}`",
            f"- Empty reason histogram: `{json.dumps(report.get('empty_reason_histogram') or {}, sort_keys=True)}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )


def _routing_consumed(
    *,
    summary: Mapping[str, Any],
    planning_materialization: Mapping[str, Any],
    planning_summary: Mapping[str, Any],
    planning_rows: Sequence[Mapping[str, Any]],
    routing_rows: Sequence[Mapping[str, Any]],
) -> bool | None:
    if _num(planning_materialization.get("first_nonempty_after_routing_latency_s")) is not None:
        return True
    route_establishment = planning_materialization.get("route_establishment")
    if isinstance(route_establishment, Mapping):
        after_routing_ratio = _num(
            route_establishment.get("planning_nonempty_after_routing_success_ratio")
        )
        if after_routing_ratio is not None and after_routing_ratio > 0:
            return True
    if _num(summary.get("routing_success_count")) and _num(planning_summary.get("messages_with_nonzero_trajectory_points")):
        return True
    route_establishment = planning_materialization.get("route_establishment")
    if isinstance(route_establishment, Mapping) and route_establishment.get("route_established") is True:
        return True
    if isinstance(route_establishment, Mapping) and route_establishment.get("route_established") is False:
        return False
    if any(_row_truthy(row, ("routing_header_present", "routing_response_consumed", "route_segment_available")) for row in planning_rows):
        return True
    if routing_rows and planning_rows:
        return False
    return None


def _route_establishment_false(planning_materialization: Mapping[str, Any]) -> bool:
    route_establishment = planning_materialization.get("route_establishment")
    if isinstance(route_establishment, Mapping):
        return route_establishment.get("route_established") is False
    if planning_materialization.get("verdict") == "fail":
        reasons = planning_materialization.get("blocking_reasons")
        if isinstance(reasons, list):
            return "route_establishment_not_confirmed" in {str(item) for item in reasons}
    return False


def _empty_reason_histogram(
    planning_materialization: Mapping[str, Any],
    planning_rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    existing = planning_materialization.get("empty_reason_histogram")
    if isinstance(existing, Mapping) and existing:
        return {str(key): int(value) for key, value in existing.items() if _num(value) is not None}
    histogram: Counter[str] = Counter()
    for row in planning_rows:
        point_count = _num(row.get("trajectory_point_count") or row.get("trajectory_points"))
        if point_count is not None and point_count > 0:
            continue
        reason = str(row.get("empty_reason") or row.get("planning_empty_reason") or "unknown")
        histogram[reason] += 1
    return dict(histogram)


def _planning_age_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    localization_ages = [_num(row.get("localization_age_ms")) for row in rows]
    chassis_ages = [_num(row.get("chassis_age_ms")) for row in rows]
    routing_ages = [_num(row.get("routing_response_age_ms")) for row in rows]
    return {
        "localization_age_ms_p95": _p95(localization_ages),
        "chassis_age_ms_p95": _p95(chassis_ages),
        "routing_response_age_ms_p95": _p95(routing_ages),
        "sample_count": len(rows),
    }


def _topic_publish_coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    topics = {str(row.get("topic") or row.get("channel") or "") for row in rows}
    return {
        "row_count": len(rows),
        "has_localization": "/apollo/localization/pose" in topics,
        "has_chassis": "/apollo/canbus/chassis" in topics,
        "has_planning": "/apollo/planning" in topics,
        "has_control": "/apollo/control" in topics,
    }


def _planning_materialization_log_errors(report: Mapping[str, Any]) -> list[str]:
    rows = report.get("apollo_log_error_topk")
    if not isinstance(rows, list):
        return []
    events: list[str] = []
    for row in rows:
        if isinstance(row, Mapping):
            text = row.get("message") or row.get("error") or row.get("status")
            if text:
                events.append(str(text))
        elif row:
            events.append(str(row))
    return events


def _text_events(*sources: Any) -> list[str]:
    events: list[str] = []
    for source in sources:
        if isinstance(source, list):
            for row in source:
                if isinstance(row, Mapping):
                    events.extend(str(row.get(key) or "") for key in ("message", "error", "warning", "status", "reason"))
                else:
                    events.append(str(row))
        elif isinstance(source, str):
            events.append(source)
    return [event for event in events if event.strip()]


def _pattern_counts(events: Sequence[str]) -> Counter[str]:
    counts: Counter[str] = Counter({key: 0 for key in TIMEOUT_PATTERNS})
    for event in events:
        for key, pattern in TIMEOUT_PATTERNS.items():
            if pattern.search(event):
                counts[key] += 1
    return counts


def _row_truthy(row: Mapping[str, Any], keys: Sequence[str]) -> bool:
    return any(_bool_or_none(row.get(key)) is True for key in keys)


def _find_first(root: Path, rels: Sequence[str]) -> Path | None:
    for rel in rels:
        path = root / rel
        if path.exists():
            return path
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, Mapping):
                rows.append(dict(payload))
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _read_lines(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _p95(values: Sequence[float | None]) -> float | None:
    finite = sorted(float(value) for value in values if value is not None)
    if not finite:
        return None
    index = int(round((len(finite) - 1) * 0.95))
    return finite[index]


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None
