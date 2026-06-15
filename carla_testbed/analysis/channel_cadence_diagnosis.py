from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.apollo_channel_health import (
    load_channel_health_config,
    load_channel_stats,
)

CHANNEL_CADENCE_DIAGNOSIS_SCHEMA_VERSION = "channel_cadence_diagnosis.v1"
DEFAULT_CHANNEL_CONFIG = Path("configs/algorithms/apollo_natural_driving_channels.yaml")
GT_INPUT_CHANNELS = {"localization", "chassis", "obstacles", "traffic_light"}
APOLLO_OUTPUT_CHANNELS = {"planning", "control"}


def analyze_channel_cadence_diagnosis_run_dir(
    run_dir: str | Path,
    *,
    config_path: str | Path = DEFAULT_CHANNEL_CONFIG,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    return analyze_channel_cadence_diagnosis_files(
        channel_stats_path=_find_first(root, ["channel_stats.json", "artifacts/channel_stats.json"]),
        channel_health_path=_find_first(
            root,
            [
                "analysis/apollo_channel_health/apollo_channel_health_report.json",
                "apollo_channel_health_report.json",
                "artifacts/apollo_channel_health_report.json",
            ],
        ),
        carla_tick_health_summary_path=_find_first(
            root,
            ["artifacts/carla_tick_health_summary.json", "carla_tick_health_summary.json"],
        ),
        carla_tick_health_log_path=_find_first(
            root,
            ["artifacts/carla_tick_health.jsonl", "carla_tick_health.jsonl"],
        ),
        topic_publish_stats_path=_find_first(
            root,
            ["artifacts/topic_publish_stats.jsonl", "topic_publish_stats.jsonl"],
        ),
        publish_gap_trace_path=_find_first(
            root,
            ["artifacts/publish_gap_trace.jsonl", "publish_gap_trace.jsonl"],
        ),
        summary_path=_find_first(root, ["summary.json"]),
        manifest_path=_find_first(root, ["manifest.json"]),
        config_path=config_path,
        run_dir=root,
    )


def analyze_channel_cadence_diagnosis_files(
    *,
    channel_stats_path: str | Path | None,
    channel_health_path: str | Path | None = None,
    carla_tick_health_summary_path: str | Path | None = None,
    carla_tick_health_log_path: str | Path | None = None,
    topic_publish_stats_path: str | Path | None = None,
    publish_gap_trace_path: str | Path | None = None,
    summary_path: str | Path | None = None,
    manifest_path: str | Path | None = None,
    config_path: str | Path = DEFAULT_CHANNEL_CONFIG,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    stats = load_channel_stats(channel_stats_path) if channel_stats_path else {}
    channel_health = _read_json(channel_health_path)
    carla_tick = _read_json(carla_tick_health_summary_path)
    carla_tick_rows = _read_jsonl(carla_tick_health_log_path)
    topic_publish_rows = _read_jsonl(topic_publish_stats_path)
    publish_gap_rows = _read_jsonl(publish_gap_trace_path)
    publish_gap = _summarize_publish_gap_rows(publish_gap_rows)
    summary = _read_json(summary_path)
    manifest = _read_json(manifest_path)
    return analyze_channel_cadence_diagnosis(
        config=config,
        stats=stats,
        channel_health=channel_health,
        carla_tick_health_summary=carla_tick,
        carla_tick_health_rows=carla_tick_rows,
        topic_publish_rows=topic_publish_rows,
        publish_gap_rows=publish_gap_rows,
        publish_gap_summary=publish_gap,
        source_paths={
            "run_dir": str(Path(run_dir).expanduser()) if run_dir is not None else None,
            "config": str(config_path) if config_path else None,
            "channel_stats": str(channel_stats_path) if channel_stats_path else None,
            "channel_health": str(channel_health_path) if channel_health_path else None,
            "carla_tick_health_summary": str(carla_tick_health_summary_path)
            if carla_tick_health_summary_path
            else None,
            "carla_tick_health_log": str(carla_tick_health_log_path) if carla_tick_health_log_path else None,
            "topic_publish_stats": str(topic_publish_stats_path) if topic_publish_stats_path else None,
            "publish_gap_trace": str(publish_gap_trace_path) if publish_gap_trace_path else None,
            "summary": str(summary_path) if summary_path else None,
            "manifest": str(manifest_path) if manifest_path else None,
        },
        summary=summary,
        manifest=manifest,
    )


def analyze_channel_cadence_diagnosis(
    *,
    config: Mapping[str, Any],
    stats: Mapping[str, Any],
    channel_health: Mapping[str, Any] | None = None,
    carla_tick_health_summary: Mapping[str, Any] | None = None,
    carla_tick_health_rows: Sequence[Mapping[str, Any]] | None = None,
    topic_publish_rows: Sequence[Mapping[str, Any]] | None = None,
    publish_gap_rows: Sequence[Mapping[str, Any]] | None = None,
    publish_gap_summary: Mapping[str, Any] | None = None,
    source_paths: Mapping[str, str | None] | None = None,
    summary: Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    channel_health = channel_health or {}
    carla_tick_health_summary = carla_tick_health_summary or {}
    carla_tick_health_rows = carla_tick_health_rows or []
    topic_publish_rows = topic_publish_rows or []
    publish_gap_rows = publish_gap_rows or []
    publish_gap_summary = publish_gap_summary or {}
    summary = summary or {}
    manifest = manifest or {}
    source_paths = source_paths or {}
    stats_channels = stats.get("channels") if isinstance(stats.get("channels"), Mapping) else {}

    channel_reports: dict[str, dict[str, Any]] = {}
    blocking_reasons: list[str] = []
    warnings: list[str] = []
    missing_artifacts: list[str] = []
    if not stats_channels:
        missing_artifacts.append("channel_stats")
        blocking_reasons.append("channel_stats_missing")

    health_results = (
        channel_health.get("channel_results")
        if isinstance(channel_health.get("channel_results"), Mapping)
        else {}
    )
    for channel_cfg in _configured_channels(config):
        diagnosis = _diagnose_channel(
            channel_cfg,
            stats_channels,
            health_results,
            carla_tick_health_summary=carla_tick_health_summary,
            carla_tick_health_rows=carla_tick_health_rows,
            topic_publish_rows=topic_publish_rows,
            publish_gap_rows=publish_gap_rows,
            publish_gap_summary=publish_gap_summary,
        )
        channel_reports[diagnosis["name"]] = diagnosis
        if diagnosis["status"] == "fail":
            blocking_reasons.extend(f"{diagnosis['name']}:{reason}" for reason in diagnosis["blocking_reasons"])
        elif diagnosis["status"] == "insufficient_data":
            blocking_reasons.extend(f"{diagnosis['name']}:{reason}" for reason in diagnosis["blocking_reasons"])
        warnings.extend(f"{diagnosis['name']}:{warning}" for warning in diagnosis["warnings"])

    primary = _primary_issue(channel_reports, blocking_reasons)
    status = _report_status(channel_reports, missing_artifacts)
    if not carla_tick_health_summary:
        warnings.append("carla_tick_health_summary_missing")
    if not publish_gap_summary:
        warnings.append("publish_gap_trace_missing")

    return {
        "schema_version": CHANNEL_CADENCE_DIAGNOSIS_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id"),
        "route_id": _first_text(summary, "route_id", manifest, "route_id"),
        "scenario_id": _first_text(summary, "scenario_id", manifest, "scenario_id"),
        "scenario_class": _first_text(summary, "scenario_class", manifest, "scenario_class"),
        "backend": _first_text(summary, "backend", manifest, "backend"),
        "status": status,
        "channel_health_status": channel_health.get("status"),
        "channels": channel_reports,
        "top_gap_windows": _top_gap_windows(channel_reports),
        "primary_cadence_issue": primary,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "warnings": sorted(set(warnings)),
        "missing_artifacts": sorted(set(missing_artifacts)),
        "carla_tick_cadence": _tick_cadence_summary(carla_tick_health_summary),
        "publish_gap_summary": dict(publish_gap_summary),
        "next_action": _next_action(primary, channel_reports, carla_tick_health_summary),
        "source": dict(source_paths),
        "interpretation_boundary": (
            "Channel cadence diagnosis explains transport/time-axis evidence. It does not prove "
            "Apollo planning behavior, route tracking, or natural-driving success, and it must not "
            "be used to bypass apollo_channel_health_report gates."
        ),
    }


def write_channel_cadence_diagnosis_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "channel_cadence_diagnosis_report.json"
    summary_path = output_dir / "channel_cadence_diagnosis_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(channel_cadence_diagnosis_summary_md(report), encoding="utf-8")
    return {
        "channel_cadence_diagnosis_report": str(json_path),
        "channel_cadence_diagnosis_summary": str(summary_path),
    }


def channel_cadence_diagnosis_summary_md(report: Mapping[str, Any]) -> str:
    lines = [
        "# Channel Cadence Diagnosis Summary",
        "",
        f"- Run ID: `{report.get('run_id')}`",
        f"- Route ID: `{report.get('route_id')}`",
        f"- Scenario: `{report.get('scenario_id')}` / `{report.get('scenario_class')}`",
        f"- Backend: `{report.get('backend')}`",
        f"- Status: `{report.get('status')}`",
        f"- Channel-health status: `{report.get('channel_health_status')}`",
        f"- Primary cadence issue: `{report.get('primary_cadence_issue')}`",
        f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
        f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
        f"- Next action: `{report.get('next_action')}`",
        "",
        "## Channels",
        "",
    ]
    channels = report.get("channels") if isinstance(report.get("channels"), Mapping) else {}
    for name, payload in channels.items():
        if not isinstance(payload, Mapping):
            continue
        lines.extend(
            [
                f"### {name}",
                f"- status: `{payload.get('status')}`",
                f"- cadence_class: `{payload.get('cadence_class')}`",
                f"- suspected_layer: `{payload.get('suspected_layer')}`",
                f"- header_sim_hz: `{payload.get('header_sim_hz')}`",
                f"- delivery_wall_hz: `{payload.get('delivery_wall_hz')}`",
                f"- max_gap_ms: `{payload.get('max_gap_ms')}`",
                f"- gap_p95_ms: `{payload.get('gap_p95_ms')}`",
                f"- gap_count_over_250ms: `{payload.get('gap_count_over_250ms')}`",
                f"- blocking_reasons: `{', '.join(payload.get('blocking_reasons') or []) or 'none'}`",
                f"- warnings: `{', '.join(payload.get('warnings') or []) or 'none'}`",
                f"- first_gap_window: `{_format_gap_window(payload)}`",
                "",
            ]
        )
    lines.extend([str(report.get("interpretation_boundary") or ""), ""])
    return "\n".join(lines)


def _diagnose_channel(
    channel_cfg: Mapping[str, Any],
    stats_channels: Mapping[str, Any],
    health_results: Mapping[str, Any],
    *,
    carla_tick_health_summary: Mapping[str, Any],
    carla_tick_health_rows: Sequence[Mapping[str, Any]],
    topic_publish_rows: Sequence[Mapping[str, Any]],
    publish_gap_rows: Sequence[Mapping[str, Any]],
    publish_gap_summary: Mapping[str, Any],
) -> dict[str, Any]:
    name = str(channel_cfg.get("name") or "")
    channel = str(channel_cfg.get("channel") or "")
    stats_entry = _find_stats(stats_channels, name, channel)
    health_entry = _find_stats(health_results, name, channel)
    min_hz = _num(channel_cfg.get("min_hz"))
    max_gap_ms_allowed = _num(channel_cfg.get("max_gap_ms"))
    required = bool(channel_cfg.get("required"))

    if stats_entry is None:
        return {
            "name": name,
            "channel": channel,
            "required": required,
            "status": "insufficient_data" if required else "warn",
            "cadence_class": "missing_stats",
            "suspected_layer": "evidence_gap",
            "blocking_reasons": ["channel_stats_missing"] if required else [],
            "warnings": ["optional_channel_stats_missing"] if not required else [],
            "min_hz": min_hz,
            "max_gap_ms_allowed": max_gap_ms_allowed,
        }

    header_sim_hz = _first_num(stats_entry, "header_sim_hz", "fresh_world_frame_hz", "sim_time_hz", "hz")
    hz = _num(stats_entry.get("hz"))
    delivery_wall_hz = _num(stats_entry.get("delivery_wall_hz"))
    max_gap_ms = _num(stats_entry.get("sim_time_max_gap_ms")) or _num(stats_entry.get("max_gap_ms"))
    raw_max_gap_ms = _num(stats_entry.get("max_gap_ms"))
    gap_p95_ms = _first_num(stats_entry, "sim_time_gap_p95_ms", "gap_p95_ms")
    gap_count_250 = _int(stats_entry.get("sim_time_gap_count_over_250ms"))
    if gap_count_250 is None:
        gap_count_250 = _int(stats_entry.get("gap_count_over_250ms"))
    gap_count_1000 = _int(stats_entry.get("sim_time_gap_count_over_1000ms"))
    if gap_count_1000 is None:
        gap_count_1000 = _int(stats_entry.get("gap_count_over_1000ms"))
    duplicate_count = _int(stats_entry.get("duplicate_timestamp_count")) or 0
    stale_count = _int(stats_entry.get("stale_count")) or 0
    message_count = _int(stats_entry.get("message_count"))
    promotion_grade = stats_entry.get("promotion_grade_evidence")
    issues = list(health_entry.get("issues") or []) if isinstance(health_entry, Mapping) else []
    warnings: list[str] = []
    blocking: list[str] = []
    cadence_class = "healthy_header_sim_cadence"
    suspected_layer = _default_suspected_layer(name)

    if message_count is None or message_count <= 0:
        cadence_class = "no_messages"
        blocking.append("no_messages")
    if min_hz is not None and (header_sim_hz is None or header_sim_hz < min_hz):
        cadence_class = "low_header_sim_rate"
        blocking.append("header_sim_rate_below_contract")
    if max_gap_ms_allowed is not None and max_gap_ms is not None and max_gap_ms > max_gap_ms_allowed:
        if _is_isolated_gap(max_gap_ms, gap_p95_ms, gap_count_250, gap_count_1000, max_gap_ms_allowed):
            cadence_class = "isolated_header_sim_gap"
            blocking.append("isolated_header_sim_gap_over_contract")
            warnings.append("gap_p95_ok_but_max_gap_exceeds_contract")
        else:
            cadence_class = "sustained_header_sim_gap"
            blocking.append("header_sim_gap_over_contract")
    if "message_gap_time_axis_warn" in issues:
        cadence_class = "wall_or_header_axis_gap_with_sim_time_context"
        warnings.append("channel_health_detected_time_axis_gap_warning")
        suspected_layer = "time_axis_or_artifact_delivery"
    if (
        delivery_wall_hz is not None
        and min_hz is not None
        and delivery_wall_hz < min_hz
        and header_sim_hz is not None
        and header_sim_hz >= min_hz
    ):
        warnings.append("delivery_wall_hz_below_contract_but_header_sim_hz_ok")
    if stale_count > 0 or duplicate_count > 0:
        cadence_class = "stale_or_duplicate_timestamps"
        blocking.append("stale_or_duplicate_timestamps")
    if stats_entry.get("derived_from_bridge_counters") is True or stats_entry.get("max_gap_ms_estimated") is True:
        warnings.append("derived_or_estimated_stats_not_claim_grade")
    if promotion_grade is False:
        warnings.append("promotion_grade_evidence_false")

    if blocking and not required:
        warnings.extend(f"optional_{reason}" for reason in blocking)
        blocking = []

    if not blocking and warnings:
        status = "warn"
    elif blocking:
        status = "fail" if required else "warn"
    else:
        status = "pass"

    publish_gap_counts = publish_gap_summary.get("by_channel") if isinstance(publish_gap_summary.get("by_channel"), Mapping) else {}
    channel_gap_counts = publish_gap_counts.get(name) or publish_gap_counts.get(channel) or {}
    gap_windows = _channel_gap_windows(
        channel=channel,
        channel_name=name,
        topic_publish_rows=topic_publish_rows,
        publish_gap_rows=publish_gap_rows,
        carla_tick_health_rows=carla_tick_health_rows,
        max_gap_ms_allowed=max_gap_ms_allowed,
    )
    return {
        "name": name,
        "channel": channel,
        "required": required,
        "status": status,
        "cadence_class": cadence_class,
        "suspected_layer": suspected_layer,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "message_count": message_count,
        "fresh_message_count": _int(stats_entry.get("fresh_message_count")),
        "hz": hz,
        "header_sim_hz": header_sim_hz,
        "fresh_world_frame_hz": _num(stats_entry.get("fresh_world_frame_hz")),
        "delivery_wall_hz": delivery_wall_hz,
        "max_gap_ms": max_gap_ms,
        "raw_max_gap_ms": raw_max_gap_ms,
        "gap_p95_ms": gap_p95_ms,
        "gap_count_over_250ms": gap_count_250,
        "gap_count_over_1000ms": gap_count_1000,
        "duplicate_timestamp_count": duplicate_count,
        "stale_count": stale_count,
        "timestamp_monotonic": stats_entry.get("timestamp_monotonic"),
        "sequence_monotonic": stats_entry.get("sequence_monotonic"),
        "promotion_grade_evidence": promotion_grade,
        "evidence_source": stats_entry.get("evidence_source"),
        "source": stats_entry.get("source"),
        "min_hz": min_hz,
        "max_gap_ms_allowed": max_gap_ms_allowed,
        "channel_health_issues": issues,
        "gap_windows": gap_windows,
        "publish_gap_counts": dict(channel_gap_counts) if isinstance(channel_gap_counts, Mapping) else {},
        "carla_tick_cadence": _tick_cadence_summary(carla_tick_health_summary),
    }


def _primary_issue(channel_reports: Mapping[str, Mapping[str, Any]], blocking_reasons: Sequence[str]) -> str | None:
    priority = ("localization", "chassis", "planning", "control", "obstacles", "traffic_light")
    for name in priority:
        report = channel_reports.get(name)
        if not isinstance(report, Mapping):
            continue
        reasons = list(report.get("blocking_reasons") or [])
        if reasons:
            return f"{name}:{reasons[0]}"
    return blocking_reasons[0] if blocking_reasons else None


def _channel_gap_windows(
    *,
    channel: str,
    channel_name: str,
    topic_publish_rows: Sequence[Mapping[str, Any]],
    publish_gap_rows: Sequence[Mapping[str, Any]],
    carla_tick_health_rows: Sequence[Mapping[str, Any]],
    max_gap_ms_allowed: float | None,
) -> list[dict[str, Any]]:
    if max_gap_ms_allowed is None:
        return []
    rows = [
        row
        for row in topic_publish_rows
        if str(row.get("channel") or "") == channel and _row_time_axis(row) is not None
    ]
    rows.sort(key=lambda row: (_row_time_axis(row) or 0.0, _num(row.get("wall_time_sec")) or 0.0))
    windows: list[dict[str, Any]] = []
    for previous, current in zip(rows, rows[1:]):
        prev_ts = _row_time_axis(previous)
        curr_ts = _row_time_axis(current)
        if prev_ts is None or curr_ts is None:
            continue
        gap_ms = (curr_ts - prev_ts) * 1000.0
        if gap_ms <= max_gap_ms_allowed:
            continue
        start_wall = _num(previous.get("wall_time_sec"))
        end_wall = _num(current.get("wall_time_sec"))
        window = {
            "channel": channel,
            "channel_name": channel_name,
            "start_timestamp": prev_ts,
            "end_timestamp": curr_ts,
            "gap_ms": gap_ms,
            "start_wall_time_sec": start_wall,
            "end_wall_time_sec": end_wall,
            "start_sequence_num": previous.get("sequence_num"),
            "end_sequence_num": current.get("sequence_num"),
            "start_frame_id": previous.get("frame_id"),
            "end_frame_id": current.get("frame_id"),
        }
        window["publish_gap_correlation"] = _summarize_window_publish_gap(
            rows=publish_gap_rows,
            channel_name=channel_name,
            start_ts=prev_ts,
            end_ts=curr_ts,
            start_wall=start_wall,
            end_wall=end_wall,
        )
        window["carla_tick_correlation"] = _summarize_window_tick_health(
            rows=carla_tick_health_rows,
            start_ts=prev_ts,
            end_ts=curr_ts,
            start_wall=start_wall,
            end_wall=end_wall,
        )
        windows.append(window)
    windows.sort(key=lambda item: _num(item.get("gap_ms")) or 0.0, reverse=True)
    return windows[:5]


def _top_gap_windows(channel_reports: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    for report in channel_reports.values():
        if not isinstance(report, Mapping):
            continue
        for window in report.get("gap_windows") or []:
            if isinstance(window, Mapping):
                windows.append(dict(window))
    windows.sort(key=lambda item: _num(item.get("gap_ms")) or 0.0, reverse=True)
    return windows[:8]


def _summarize_window_publish_gap(
    *,
    rows: Sequence[Mapping[str, Any]],
    channel_name: str,
    start_ts: float,
    end_ts: float,
    start_wall: float | None,
    end_wall: float | None,
) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if _row_in_window(row, start_ts=start_ts, end_ts=end_ts, start_wall=start_wall, end_wall=end_wall)
    ]
    if not selected:
        return {"row_count": 0}
    field = {"localization": "published_localization", "chassis": "published_chassis"}.get(channel_name)
    skip_counts: dict[str, int] = {}
    published = 0
    skipped = 0
    max_loop_ms: float | None = None
    max_writer_ms: float | None = None
    max_queue_depth: int | None = None
    backpressure_count = 0
    for row in selected:
        skip = str(row.get("skip_reason") or "none")
        skip_counts[skip] = skip_counts.get(skip, 0) + 1
        if field:
            if row.get(field) is True:
                published += 1
            elif row.get("expected_publish") is True:
                skipped += 1
        max_loop_ms = _max_num(max_loop_ms, row.get("publish_loop_duration_ms"))
        max_writer_ms = _max_num(max_writer_ms, row.get("writer_write_duration_ms_max"), row.get("writer_write_duration_ms"))
        queue_depth = _int(row.get("async_queue_depth_max")) or _int(row.get("async_queue_depth"))
        if queue_depth is not None:
            max_queue_depth = queue_depth if max_queue_depth is None else max(max_queue_depth, queue_depth)
        if row.get("artifact_backpressure") is True:
            backpressure_count += 1
    return {
        "row_count": len(selected),
        "skip_reason_counts": skip_counts,
        "published_count": published,
        "skipped_count": skipped,
        "max_publish_loop_duration_ms": max_loop_ms,
        "max_writer_write_duration_ms": max_writer_ms,
        "max_async_queue_depth": max_queue_depth,
        "artifact_backpressure_count": backpressure_count,
    }


def _summarize_window_tick_health(
    *,
    rows: Sequence[Mapping[str, Any]],
    start_ts: float,
    end_ts: float,
    start_wall: float | None,
    end_wall: float | None,
) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if _row_in_window(row, start_ts=start_ts, end_ts=end_ts, start_wall=start_wall, end_wall=end_wall)
    ]
    if not selected:
        return {"row_count": 0}
    max_stage_name: str | None = None
    max_stage_duration_s: float | None = None
    for row in selected:
        stages = row.get("stage_durations_s")
        if isinstance(stages, Mapping):
            for name, value in stages.items():
                duration = _num(value)
                if duration is not None and (max_stage_duration_s is None or duration > max_stage_duration_s):
                    max_stage_duration_s = duration
                    max_stage_name = str(name)
    return {
        "row_count": len(selected),
        "max_frame_loop_wall_duration_s": _max_field(selected, "frame_loop_wall_duration_s"),
        "max_frame_post_tick_wall_duration_s": _max_field(selected, "frame_post_tick_wall_duration_s"),
        "max_inter_tick_wall_interval_s": _max_field(selected, "inter_tick_wall_interval_s"),
        "max_tick_wall_duration_s": _max_field(selected, "tick_wall_duration_s"),
        "max_stage_name": max_stage_name,
        "max_stage_duration_s": max_stage_duration_s,
    }


def _row_in_window(
    row: Mapping[str, Any],
    *,
    start_ts: float,
    end_ts: float,
    start_wall: float | None,
    end_wall: float | None,
) -> bool:
    timestamp = _row_time_axis(row)
    if timestamp is not None and start_ts <= timestamp <= end_ts:
        return True
    wall_time = _num(row.get("wall_time_sec")) or _num(row.get("wall_time_s"))
    if wall_time is not None and start_wall is not None and end_wall is not None:
        return start_wall <= wall_time <= end_wall
    return False


def _row_time_axis(row: Mapping[str, Any]) -> float | None:
    return _first_num(row, "header_timestamp_sec", "sim_time_sec", "sim_time", "sim_time_s", "timestamp")


def _max_field(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    out: float | None = None
    for row in rows:
        out = _max_num(out, row.get(key))
    return out


def _format_gap_window(payload: Mapping[str, Any]) -> str:
    windows = payload.get("gap_windows")
    if not isinstance(windows, list) or not windows:
        return "none"
    first = windows[0]
    if not isinstance(first, Mapping):
        return "none"
    publish = first.get("publish_gap_correlation") if isinstance(first.get("publish_gap_correlation"), Mapping) else {}
    tick = first.get("carla_tick_correlation") if isinstance(first.get("carla_tick_correlation"), Mapping) else {}
    return (
        f"{_summary_metric(first.get('start_timestamp'))}->{_summary_metric(first.get('end_timestamp'))} "
        f"gap_ms={_summary_metric(first.get('gap_ms'))}; "
        f"skip={publish.get('skip_reason_counts') or {}}; "
        f"tick_stage={tick.get('max_stage_name')}"
    )


def _report_status(channel_reports: Mapping[str, Mapping[str, Any]], missing_artifacts: Sequence[str]) -> str:
    if missing_artifacts:
        return "insufficient_data"
    statuses = {str(report.get("status")) for report in channel_reports.values() if isinstance(report, Mapping)}
    if "fail" in statuses or "insufficient_data" in statuses:
        return "fail" if "fail" in statuses else "insufficient_data"
    if "warn" in statuses:
        return "warn"
    return "pass"


def _is_isolated_gap(
    max_gap_ms: float,
    gap_p95_ms: float | None,
    gap_count_250: int | None,
    gap_count_1000: int | None,
    max_gap_ms_allowed: float,
) -> bool:
    if gap_count_1000 and gap_count_1000 > 0:
        return False
    if gap_count_250 is not None and gap_count_250 <= 5:
        if gap_p95_ms is None or gap_p95_ms <= max_gap_ms_allowed:
            return max_gap_ms <= max_gap_ms_allowed * 2.0
    return False


def _next_action(
    primary: str | None,
    channel_reports: Mapping[str, Mapping[str, Any]],
    carla_tick_health_summary: Mapping[str, Any],
) -> str:
    if primary is None:
        return "Channel cadence is non-blocking; continue with localization/reference-line/control evidence."
    channel_name = primary.split(":", 1)[0]
    report = channel_reports.get(channel_name, {})
    cadence = report.get("cadence_class") if isinstance(report, Mapping) else None
    if cadence == "isolated_header_sim_gap":
        slow_stage = carla_tick_health_summary.get("max_stage_name")
        window_hint = _gap_window_next_action_hint(report)
        if window_hint:
            return window_hint
        return (
            "Inspect publish_gap_trace and CARLA tick stage timings around the isolated header-sim gap; "
            f"latest max tick stage is {slow_stage or 'unknown'}. Do not tune control before cadence is explained."
        )
    if cadence == "sustained_header_sim_gap":
        return "Inspect GT publisher cadence and bridge publish loop; required Apollo input channel has sustained sim-time gaps."
    if cadence == "low_header_sim_rate":
        return "Inspect GT publisher target Hz and source sample freshness for the low-rate required channel."
    if cadence == "stale_or_duplicate_timestamps":
        return "Inspect stale republish or duplicate timestamp handling; duplicate/stale GT inputs cannot support claim-grade evidence."
    if cadence == "missing_stats":
        return "Generate channel_stats.json and apollo_channel_health_report.json before interpreting behavior."
    return "Inspect channel_stats, topic_publish_stats, and publish_gap_trace for the primary cadence issue."


def _gap_window_next_action_hint(report: Mapping[str, Any]) -> str | None:
    windows = report.get("gap_windows")
    if not isinstance(windows, list) or not windows:
        return None
    first = windows[0]
    if not isinstance(first, Mapping):
        return None
    publish = first.get("publish_gap_correlation") if isinstance(first.get("publish_gap_correlation"), Mapping) else {}
    tick = first.get("carla_tick_correlation") if isinstance(first.get("carla_tick_correlation"), Mapping) else {}
    skip_counts = publish.get("skip_reason_counts") if isinstance(publish.get("skip_reason_counts"), Mapping) else {}
    if _int(skip_counts.get("publish_loop_overrun")):
        return (
            "Inspect bridge publish loop overrun during the top gap window "
            f"{_summary_metric(first.get('start_timestamp'))}->{_summary_metric(first.get('end_timestamp'))}s; "
            f"CARLA tick correlation points to {tick.get('max_stage_name') or 'unknown'} and "
            f"max_async_queue_depth={publish.get('max_async_queue_depth')}. Keep channel gate failing until "
            "localization/chassis max gap is within contract."
        )
    if _int(skip_counts.get("stale_sample_skipped")):
        return (
            "Inspect stale-sample skip handling during the top gap window "
            f"{_summary_metric(first.get('start_timestamp'))}->{_summary_metric(first.get('end_timestamp'))}s; "
            "required GT input channels cannot rely on stale republish for claim-grade evidence."
        )
    return None


def _summarize_publish_gap_trace(path: str | Path | None) -> dict[str, Any]:
    return _summarize_publish_gap_rows(_read_jsonl(path))


def _summarize_publish_gap_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    by_skip: dict[str, int] = {}
    by_channel: dict[str, dict[str, int]] = {
        "localization": {"published": 0, "skipped": 0},
        "chassis": {"published": 0, "skipped": 0},
    }
    max_loop_ms: float | None = None
    max_writer_ms: float | None = None
    max_queue_depth: int | None = None
    backpressure_count = 0
    for row in rows:
        skip = str(row.get("skip_reason") or "none")
        by_skip[skip] = by_skip.get(skip, 0) + 1
        for name, field in (("localization", "published_localization"), ("chassis", "published_chassis")):
            if row.get(field) is True:
                by_channel[name]["published"] += 1
            elif row.get("expected_publish") is True:
                by_channel[name]["skipped"] += 1
        max_loop_ms = _max_num(max_loop_ms, row.get("publish_loop_duration_ms"))
        max_writer_ms = _max_num(max_writer_ms, row.get("writer_write_duration_ms_max"), row.get("writer_write_duration_ms"))
        queue_depth = _int(row.get("async_queue_depth_max")) or _int(row.get("async_queue_depth"))
        if queue_depth is not None:
            max_queue_depth = queue_depth if max_queue_depth is None else max(max_queue_depth, queue_depth)
        if row.get("artifact_backpressure") is True:
            backpressure_count += 1
    return {
        "row_count": len(rows),
        "skip_reason_counts": by_skip,
        "by_channel": by_channel,
        "max_publish_loop_duration_ms": max_loop_ms,
        "max_writer_write_duration_ms": max_writer_ms,
        "max_async_queue_depth": max_queue_depth,
        "artifact_backpressure_count": backpressure_count,
    }


def _configured_channels(config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    channels = config.get("channels")
    return [item for item in channels if isinstance(item, Mapping)] if isinstance(channels, list) else []


def _load_config(path: str | Path) -> dict[str, Any]:
    try:
        return load_channel_health_config(path)
    except Exception:
        return {"channels": []}


def _find_stats(stats_channels: Mapping[str, Any], name: str, channel: str) -> Mapping[str, Any] | None:
    for key in (channel, name):
        value = stats_channels.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _tick_cadence_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    return {
        "inter_tick_wall_interval_p95_s": payload.get("inter_tick_wall_interval_p95_s"),
        "max_inter_tick_wall_interval_s": payload.get("max_inter_tick_wall_interval_s"),
        "max_stage_name": payload.get("max_stage_name"),
        "max_stage_duration_s": payload.get("max_stage_duration_s"),
        "max_tick_wall_duration_s": payload.get("max_tick_wall_duration_s"),
        "slow_frame_count": payload.get("slow_frame_count"),
    }


def _default_suspected_layer(name: str) -> str:
    if name in GT_INPUT_CHANNELS:
        return "gt_publish_cadence"
    if name in APOLLO_OUTPUT_CHANNELS:
        return "apollo_output_cadence"
    return "channel_cadence"


def _summary_metric(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, (int, str, bool)):
        return str(value)
    return "None" if value is None else str(value)


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = (root / relative).resolve() if relative.startswith("..") else root / relative
        if path.exists():
            return path
    return None


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    p = Path(path).expanduser()
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    p = Path(path).expanduser()
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with p.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    payload = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _num(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _first_num(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _num(payload.get(key))
        if value is not None:
            return value
    return None


def _max_num(current: float | None, *values: Any) -> float | None:
    out = current
    for value in values:
        num = _num(value)
        if num is None:
            continue
        out = num if out is None else max(out, num)
    return out


def _first_text(*payloads_and_keys: Any) -> str | None:
    if len(payloads_and_keys) % 2:
        return None
    for idx in range(0, len(payloads_and_keys), 2):
        payload = payloads_and_keys[idx]
        key = payloads_and_keys[idx + 1]
        if isinstance(payload, Mapping):
            value = payload.get(key)
            if value not in {None, ""}:
                return str(value)
    return None
