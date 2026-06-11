from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

CHANNEL_STATS_SCHEMA_VERSION = "channel_stats.v1"

CHANNEL_MAP = {
    "localization": ("/apollo/localization/pose", "loc_count"),
    "chassis": ("/apollo/canbus/chassis", "chassis_count"),
    "obstacles": ("/apollo/perception/obstacles", "obstacle_message_count"),
    "planning": ("/apollo/planning", ("planning", "msg_count")),
    "control": ("/apollo/control", "control_rx_count"),
    "routing_response": ("/apollo/routing_response", "routing_response_count"),
}


def discover_bridge_stats_path(run_dir: str | Path) -> Path | None:
    root = Path(run_dir).expanduser()
    candidates = [
        root / "artifacts" / "cyber_bridge_stats.json",
        root / "cyber_bridge_stats.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    for candidate in sorted(root.rglob("cyber_bridge_stats.json")):
        if candidate.is_file():
            return candidate
    return None


def bridge_stats_to_channel_stats(
    bridge_stats: Mapping[str, Any],
    *,
    source_path: str | Path | None = None,
    duration_s: float | None = None,
    duration_source: str | None = None,
) -> dict[str, Any]:
    resolved_duration_s = duration_s
    resolved_duration_source = duration_source
    if resolved_duration_s is None:
        resolved_duration_s, resolved_duration_source = _duration_s(bridge_stats)
    channels: dict[str, dict[str, Any]] = {}
    for _name, (channel, count_path) in CHANNEL_MAP.items():
        count = _count_at(bridge_stats, count_path)
        if _name == "obstacles" and count <= 0:
            count = _int_or_zero(bridge_stats.get("obstacles_count"))
        channels[channel] = _channel_entry(
            count,
            duration_s=resolved_duration_s,
            source="cyber_bridge_stats",
        )
        if _name == "obstacles":
            channels[channel]["obstacle_count"] = _int_or_zero(
                bridge_stats.get("obstacle_object_count", bridge_stats.get("obstacles_count"))
            )
            channels[channel]["empty_message_count"] = _int_or_zero(
                bridge_stats.get("obstacle_empty_message_count")
            )

    traffic_light = bridge_stats.get("traffic_light")
    if isinstance(traffic_light, Mapping):
        count = _int_or_zero(traffic_light.get("publish_count"))
        channel = str(traffic_light.get("channel") or "/apollo/perception/traffic_light")
        channels[channel] = _channel_entry(
            count,
            duration_s=resolved_duration_s,
            source="cyber_bridge_stats.traffic_light",
        )

    return {
        "schema_version": CHANNEL_STATS_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "type": "derived_from_bridge_counters",
            "path": None if source_path is None else str(source_path),
            "duration_s": resolved_duration_s,
            "duration_source": resolved_duration_source,
            "limitations": [
                "max_gap_ms is estimated from aggregate count and duration",
                "timestamp_monotonic and sequence_monotonic are inferred from bridge counter materialization",
                "use exported cyber_monitor/cyber_record stats for promotion-grade channel evidence",
            ],
        },
        "channels": channels,
    }


def normalize_channel_stats_for_run(run_dir: str | Path, *, output_path: str | Path | None = None) -> dict[str, Any] | None:
    root = Path(run_dir).expanduser()
    bridge_stats_path = discover_bridge_stats_path(root)
    if bridge_stats_path is None:
        return None
    bridge_stats = _read_json(bridge_stats_path)
    if not bridge_stats:
        return None
    duration_s, duration_source = _duration_s_for_run(root, bridge_stats)
    stats = bridge_stats_to_channel_stats(
        bridge_stats,
        source_path=bridge_stats_path,
        duration_s=duration_s,
        duration_source=duration_source,
    )
    _merge_row_level_artifact_stats(root, stats)
    target = Path(output_path).expanduser() if output_path is not None else root / "channel_stats.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(stats, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    stats["_output_path"] = str(target)
    return stats


def _channel_entry(count: int, *, duration_s: float | None, source: str) -> dict[str, Any]:
    hz = None
    estimated_gap_ms = None
    if duration_s is not None and duration_s > 0:
        hz = float(count) / duration_s
    if hz is not None and hz > 0:
        estimated_gap_ms = 1000.0 / hz
    return {
        "message_count": int(count),
        "hz": hz,
        "first_timestamp": 0.0 if count > 0 else None,
        "last_timestamp": duration_s if count > 0 else None,
        "max_gap_ms": estimated_gap_ms,
        "max_gap_start_timestamp": None,
        "max_gap_end_timestamp": None,
        "max_gap_index": None,
        "gap_p95_ms": estimated_gap_ms,
        "gap_count_over_250ms": None,
        "gap_count_over_1000ms": None,
        "timestamp_monotonic": bool(count > 0),
        "sequence_monotonic": bool(count > 0),
        "stale_count": 0 if count > 0 else None,
        "derived_from_bridge_counters": True,
        "max_gap_ms_estimated": True,
        "source": source,
    }


def _merge_row_level_artifact_stats(run_dir: Path, stats: dict[str, Any]) -> None:
    channels = stats.setdefault("channels", {})
    if not isinstance(channels, dict):
        return
    source = stats.setdefault("source", {})
    if isinstance(source, dict):
        source.setdefault("row_level_artifacts", [])
        source.setdefault("base_type", source.get("type"))

    timeseries = _find_first(run_dir, ("timeseries.csv", "artifacts/debug_timeseries.csv"))
    if timeseries is not None:
        rows = _read_csv_rows(timeseries)
        for channel, field in (
            ("/apollo/localization/pose", "localization_timestamp"),
            ("/apollo/canbus/chassis", "chassis_timestamp"),
        ):
            entry = _channel_entry_from_rows(
                rows,
                timestamp_field=field,
                source=f"{timeseries.name}.{field}",
                sequence_field=None,
            )
            if entry is not None:
                channels[channel] = entry
                _append_row_level_artifact(source, timeseries)

    planning = _find_first(run_dir, ("artifacts/planning_topic_debug.jsonl", "planning_topic_debug.jsonl"))
    if planning is not None:
        entry = _channel_entry_from_jsonl(
            planning,
            timestamp_paths=(("planning_header_timestamp_sec",), ("timestamp",), ("wall_time_sec",)),
            sequence_paths=(("planning_header_sequence_num",),),
            source="planning_topic_debug.jsonl",
        )
        if entry is not None:
            entry["primary_time_axis"] = "planning_header_timestamp_sec"
            sim_entry = _channel_entry_from_jsonl(
                planning,
                timestamp_paths=(("sim_time_sec",),),
                sequence_paths=(("planning_header_sequence_num",),),
                source="planning_topic_debug.jsonl.sim_time_sec",
            )
            if sim_entry is not None:
                _merge_secondary_time_axis(entry, "sim_time", sim_entry)
            channels["/apollo/planning"] = entry
            _append_row_level_artifact(source, planning)

    control = _find_first(
        run_dir,
        (
            "artifacts/control_trajectory_consume_debug_live.jsonl",
            "control_trajectory_consume_debug_live.jsonl",
            "artifacts/control_decode_debug.jsonl",
            "artifacts/bridge_control_decode.jsonl",
            "control_decode_debug.jsonl",
            "bridge_control_decode.jsonl",
        ),
    )
    if control is not None:
        entry = _channel_entry_from_jsonl(
            control,
            timestamp_paths=(
                ("timestamp",),
                ("parsed_control", "control_timestamp"),
                ("raw_control_msg_dump", "control_header_timestamp_sec"),
                ("control_rx_timestamp",),
                ("ts_sec",),
            ),
            sequence_paths=(
                ("control_cycle_index",),
                ("parsed_control", "control_header_sequence_num"),
                ("raw_control_msg_dump", "control_header_sequence_num"),
            ),
            source=control.name,
        )
        if entry is not None:
            channels["/apollo/control"] = entry
            _append_row_level_artifact(source, control)

    topic_publish_stats = _find_first(
        run_dir,
        ("artifacts/topic_publish_stats.jsonl", "topic_publish_stats.jsonl"),
    )
    if topic_publish_stats is not None:
        entries = _channel_entries_from_topic_publish_stats(topic_publish_stats)
        for channel, entry in entries.items():
            channels[channel] = entry
        if entries:
            _append_row_level_artifact(source, topic_publish_stats)

    publish_gap_trace = _find_first(
        run_dir,
        ("artifacts/publish_gap_trace.jsonl", "publish_gap_trace.jsonl"),
    )
    if publish_gap_trace is not None:
        summary = _publish_gap_trace_summary(publish_gap_trace)
        if summary:
            if isinstance(source, dict):
                source["publish_gap_trace_summary"] = summary
                _append_row_level_artifact(source, publish_gap_trace)

    if isinstance(source, dict) and source.get("row_level_artifacts"):
        source["type"] = "mixed_bridge_and_row_level_artifacts"
        limitations = source.setdefault("limitations", [])
        if isinstance(limitations, list):
            note = "row-level artifacts replace aggregate bridge counters where timestamped samples are available"
            if note not in limitations:
                limitations.append(note)


def _channel_entry_from_rows(
    rows: list[dict[str, str]],
    *,
    timestamp_field: str,
    source: str,
    sequence_field: str | None,
) -> dict[str, Any] | None:
    timestamps: list[float] = []
    sequences: list[float] = []
    duplicate_timestamp_count = 0
    previous_timestamp: float | None = None
    for row in rows:
        timestamp = _float_or_none(row.get(timestamp_field))
        if timestamp is None:
            continue
        timestamps.append(timestamp)
        if previous_timestamp == timestamp:
            duplicate_timestamp_count += 1
        previous_timestamp = timestamp
        if sequence_field is not None:
            sequence = _float_or_none(row.get(sequence_field))
            if sequence is not None:
                sequences.append(sequence)
    return _channel_entry_from_series(
        timestamps,
        sequences=sequences,
        duplicate_timestamp_count=duplicate_timestamp_count,
        source=source,
        sequence_inferred=sequence_field is None,
    )


def _channel_entry_from_jsonl(
    path: Path,
    *,
    timestamp_paths: tuple[tuple[str, ...], ...],
    sequence_paths: tuple[tuple[str, ...], ...],
    source: str,
) -> dict[str, Any] | None:
    timestamps: list[float] = []
    sequences: list[float] = []
    duplicate_timestamp_count = 0
    previous_timestamp: float | None = None
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, Mapping):
                    continue
                timestamp = _nested_float(payload, timestamp_paths)
                if timestamp is None:
                    continue
                timestamps.append(timestamp)
                if previous_timestamp == timestamp:
                    duplicate_timestamp_count += 1
                previous_timestamp = timestamp
                sequence = _nested_float(payload, sequence_paths)
                if sequence is not None:
                    sequences.append(sequence)
    except OSError:
        return None
    return _channel_entry_from_series(
        timestamps,
        sequences=sequences,
        duplicate_timestamp_count=duplicate_timestamp_count,
        source=source,
        sequence_inferred=not sequences,
    )


def _channel_entry_from_series(
    timestamps: list[float],
    *,
    sequences: list[float],
    duplicate_timestamp_count: int,
    source: str,
    sequence_inferred: bool,
) -> dict[str, Any] | None:
    if not timestamps:
        return None
    first = timestamps[0]
    last = timestamps[-1]
    span = last - first if last > first else None
    gaps = [
        ((right - left) * 1000.0, index, left, right)
        for index, (left, right) in enumerate(zip(timestamps, timestamps[1:]))
        if right >= left
    ]
    gap_values = [gap for gap, _index, _left, _right in gaps]
    max_gap = max(gaps, key=lambda item: item[0]) if gaps else None
    hz = ((len(timestamps) - 1) / span) if span and span > 0 and len(timestamps) > 1 else None
    return {
        "message_count": len(timestamps),
        "hz": hz,
        "first_timestamp": first,
        "last_timestamp": last,
        "max_gap_ms": max_gap[0] if max_gap else None,
        "max_gap_start_timestamp": max_gap[2] if max_gap else None,
        "max_gap_end_timestamp": max_gap[3] if max_gap else None,
        "max_gap_index": max_gap[1] if max_gap else None,
        "gap_p95_ms": _percentile(gap_values, 0.95),
        "gap_count_over_250ms": sum(1 for gap in gap_values if gap > 250.0),
        "gap_count_over_1000ms": sum(1 for gap in gap_values if gap > 1000.0),
        "timestamp_monotonic": _monotonic(timestamps),
        "sequence_monotonic": True if sequence_inferred else _monotonic(sequences),
        "stale_count": duplicate_timestamp_count,
        "derived_from_bridge_counters": False,
        "max_gap_ms_estimated": False,
        "source": source,
        "evidence_source": "row_level_artifact",
        "sequence_inferred_from_timestamps": sequence_inferred,
    }


def _merge_secondary_time_axis(entry: dict[str, Any], prefix: str, axis_entry: Mapping[str, Any]) -> None:
    mapping = {
        "hz": f"{prefix}_hz",
        "first_timestamp": f"{prefix}_first_timestamp",
        "last_timestamp": f"{prefix}_last_timestamp",
        "max_gap_ms": f"{prefix}_max_gap_ms",
        "gap_p95_ms": f"{prefix}_gap_p95_ms",
        "gap_count_over_250ms": f"{prefix}_gap_count_over_250ms",
        "gap_count_over_1000ms": f"{prefix}_gap_count_over_1000ms",
        "timestamp_monotonic": f"{prefix}_timestamp_monotonic",
        "sequence_monotonic": f"{prefix}_sequence_monotonic",
    }
    for source_key, target_key in mapping.items():
        if source_key in axis_entry:
            entry[target_key] = axis_entry[source_key]


def _channel_entries_from_topic_publish_stats(path: Path) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, Mapping):
                    continue
                channel = str(payload.get("channel") or "").strip()
                if not channel:
                    continue
                grouped.setdefault(channel, []).append(payload)
    except OSError:
        return {}

    entries: dict[str, dict[str, Any]] = {}
    for channel, rows in grouped.items():
        timestamps: list[float] = []
        wall_times: list[float] = []
        sequences: list[float] = []
        sample_keys: list[tuple[str, Any]] = []
        duplicate_timestamp_count = 0
        previous_timestamp: float | None = None
        payload_counts: list[int] = []
        empty_message_count = 0
        world_frame_missing_count = 0
        for row in rows:
            timestamp = _float_or_none(row.get("header_timestamp_sec"))
            if timestamp is None:
                timestamp = _float_or_none(row.get("sim_time_sec"))
            if timestamp is not None:
                timestamps.append(timestamp)
                if previous_timestamp == timestamp:
                    duplicate_timestamp_count += 1
                previous_timestamp = timestamp
                world_frame = row.get("carla_world_frame")
                if world_frame is not None:
                    sample_keys.append(("world_frame", world_frame))
                else:
                    world_frame_missing_count += 1
                    sample_keys.append(("timestamp", timestamp))
            wall_time = _float_or_none(row.get("wall_time_sec"))
            if wall_time is not None:
                wall_times.append(wall_time)
            sequence = _float_or_none(row.get("sequence_num"))
            if sequence is not None:
                sequences.append(sequence)
            payload_count = _int_or_none(row.get("payload_count"))
            if payload_count is not None:
                payload_counts.append(payload_count)
            if bool(row.get("empty_message")):
                empty_message_count += 1
        entry = _channel_entry_from_series(
            timestamps,
            sequences=sequences,
            duplicate_timestamp_count=duplicate_timestamp_count,
            source="topic_publish_stats.jsonl",
            sequence_inferred=not sequences,
        )
        if entry is None:
            continue
        fresh_count = len(dict.fromkeys(sample_keys)) if sample_keys else len(timestamps)
        fresh_hz = _rate_from_span(fresh_count, timestamps)
        delivery_wall_hz = _rate_from_span(len(wall_times), wall_times)
        header_sim_hz = _rate_from_span(len(timestamps), timestamps)
        if fresh_hz is not None:
            entry["hz"] = fresh_hz
        entry.update(
            {
                "message_count": len(rows),
                "fresh_message_count": fresh_count,
                "fresh_world_frame_hz": fresh_hz,
                "delivery_wall_hz": delivery_wall_hz,
                "header_sim_hz": header_sim_hz,
                "world_frame_missing_count": world_frame_missing_count,
                "duplicate_timestamp_count": duplicate_timestamp_count,
                "stale_count": duplicate_timestamp_count,
                "derived_from_bridge_counters": False,
                "max_gap_ms_estimated": False,
                "evidence_source": "topic_publish_stats",
                "promotion_grade_evidence": True,
            }
        )
        if channel == "/apollo/perception/obstacles":
            entry["obstacle_count"] = sum(payload_counts)
            entry["empty_message_count"] = empty_message_count
        if channel == "/apollo/perception/traffic_light":
            entry["light_count"] = sum(payload_counts)
            entry["empty_message_count"] = empty_message_count
        entries[channel] = entry
    return entries


def _rate_from_span(count: int, timestamps: list[float]) -> float | None:
    if count <= 1 or len(timestamps) < 2:
        return None
    first = timestamps[0]
    last = timestamps[-1]
    if last <= first:
        return None
    return float(count - 1) / (last - first)


def _publish_gap_trace_summary(path: Path) -> dict[str, Any]:
    skip_reasons: Counter[str] = Counter()
    expected_count = 0
    published_localization_count = 0
    published_chassis_count = 0
    artifact_backpressure_count = 0
    async_queue_full_count = 0
    async_dropped_count = 0
    max_queue_depth: int | None = None
    max_writer_write_duration_ms: float | None = None
    max_publish_loop_duration_ms: float | None = None
    row_count = 0
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, Mapping):
                    continue
                row_count += 1
                if payload.get("expected_publish") is True:
                    expected_count += 1
                if payload.get("published_localization") is True:
                    published_localization_count += 1
                if payload.get("published_chassis") is True:
                    published_chassis_count += 1
                reason = str(payload.get("skip_reason") or "none")
                if reason and reason != "none":
                    skip_reasons[reason] += 1
                if payload.get("artifact_backpressure") is True:
                    artifact_backpressure_count += 1
                async_queue_full_count += _int_or_zero(payload.get("async_queue_full_count"))
                async_dropped_count += _int_or_zero(payload.get("async_dropped_count"))
                queue_depth = _int_or_none(payload.get("async_queue_depth_max") or payload.get("async_queue_depth"))
                if queue_depth is not None:
                    max_queue_depth = queue_depth if max_queue_depth is None else max(max_queue_depth, queue_depth)
                writer_ms = _float_or_none(
                    payload.get("writer_write_duration_ms_max")
                    or payload.get("writer_write_duration_ms")
                )
                if writer_ms is not None:
                    max_writer_write_duration_ms = (
                        writer_ms
                        if max_writer_write_duration_ms is None
                        else max(max_writer_write_duration_ms, writer_ms)
                    )
                loop_ms = _float_or_none(payload.get("publish_loop_duration_ms"))
                if loop_ms is not None:
                    max_publish_loop_duration_ms = (
                        loop_ms
                        if max_publish_loop_duration_ms is None
                        else max(max_publish_loop_duration_ms, loop_ms)
                    )
    except OSError:
        return {}
    if row_count <= 0:
        return {}
    return {
        "schema_version": "publish_gap_trace_summary.v1",
        "path": str(path),
        "row_count": row_count,
        "expected_publish_count": expected_count,
        "published_localization_count": published_localization_count,
        "published_chassis_count": published_chassis_count,
        "skip_reason_counts": dict(sorted(skip_reasons.items())),
        "artifact_backpressure_count": artifact_backpressure_count,
        "async_queue_full_count_total": async_queue_full_count,
        "async_dropped_count_total": async_dropped_count,
        "max_async_queue_depth": max_queue_depth,
        "max_writer_write_duration_ms": max_writer_write_duration_ms,
        "max_publish_loop_duration_ms": max_publish_loop_duration_ms,
        "interpretation": (
            "This summarizes why expected GT localization/chassis publishes were skipped or delayed. "
            "It distinguishes stale-sample skip, publish-loop overrun, artifact backpressure, writer duration, "
            "and async queue pressure; it does not make skipped samples claim-grade."
        ),
    }


def _monotonic(values: list[float]) -> bool:
    return all(right >= left for left, right in zip(values, values[1:]))


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[index]


def _nested_float(payload: Mapping[str, Any], paths: tuple[tuple[str, ...], ...]) -> float | None:
    for path in paths:
        current: Any = payload
        for key in path:
            if not isinstance(current, Mapping):
                current = None
                break
            current = current.get(key)
        value = _float_or_none(current)
        if value is not None:
            return value
    return None


def _append_row_level_artifact(source: Any, path: Path) -> None:
    if not isinstance(source, dict):
        return
    artifacts = source.setdefault("row_level_artifacts", [])
    value = str(path)
    if isinstance(artifacts, list) and value not in artifacts:
        artifacts.append(value)


def _duration_s_for_run(run_dir: Path, stats: Mapping[str, Any]) -> tuple[float | None, str | None]:
    duration, source = _duration_s(stats)
    if duration is not None and source != "timing.sim_time_sec":
        return duration, source

    timeseries_duration = _timeseries_duration_s(run_dir)
    if timeseries_duration is not None:
        return timeseries_duration, "timeseries_span"

    return duration, source


def _find_first(root: Path, relative_paths: tuple[str, ...]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relative_paths}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except OSError:
        return []


def _duration_s(stats: Mapping[str, Any]) -> tuple[float | None, str | None]:
    value = _float_or_none(stats.get("publish_elapsed_wall_sec"))
    if value and value > 0:
        return value, "publish_elapsed_wall_sec"

    first_wall = _float_or_none(stats.get("first_publish_wall_ts_sec"))
    last_wall = _float_or_none(stats.get("last_publish_wall_ts_sec"))
    if first_wall is not None and last_wall is not None and last_wall > first_wall:
        return last_wall - first_wall, "publish_wall_ts_span"

    value = _float_or_none(stats.get("publish_elapsed_sim_sec"))
    if value and value > 0:
        return value, "publish_elapsed_sim_sec"

    first_sim = _float_or_none(stats.get("first_publish_ts_sec"))
    last_sim = _float_or_none(stats.get("last_publish_ts_sec"))
    if first_sim is not None and last_sim is not None and last_sim > first_sim:
        return last_sim - first_sim, "publish_sim_ts_span"

    timing = stats.get("timing")
    if isinstance(timing, Mapping):
        value = _float_or_none(timing.get("sim_time_sec"))
        if value and value > 0:
            return value, "timing.sim_time_sec"
    health = stats.get("health")
    if isinstance(health, Mapping):
        value = _float_or_none(health.get("sim_duration_s"))
        if value and value > 0:
            return value, "health.sim_duration_s"
    return None, None


def _timeseries_duration_s(run_dir: Path) -> float | None:
    for relative in ("timeseries.csv", "artifacts/debug_timeseries.csv"):
        duration = _csv_time_span_s(run_dir / relative)
        if duration is not None:
            return duration
    for candidate in sorted(run_dir.rglob("timeseries.csv")):
        duration = _csv_time_span_s(candidate)
        if duration is not None:
            return duration
    return None


def _csv_time_span_s(path: Path) -> float | None:
    if not path.exists():
        return None
    first: float | None = None
    last: float | None = None
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return None
            time_fields = [
                field
                for field in ("ts_sec", "localization_timestamp", "sim_time", "wall_time_s")
                if field in reader.fieldnames
            ]
            if not time_fields:
                return None
            time_field = time_fields[0]
            for row in reader:
                value = _float_or_none(row.get(time_field))
                if value is None:
                    continue
                if first is None:
                    first = value
                last = value
    except OSError:
        return None
    if first is None or last is None or last <= first:
        return None
    return last - first


def _count_at(payload: Mapping[str, Any], path: str | tuple[str, str]) -> int:
    if isinstance(path, str):
        return _int_or_zero(payload.get(path))
    first, second = path
    nested = payload.get(first)
    if isinstance(nested, Mapping):
        return _int_or_zero(nested.get(second))
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _int_or_zero(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
