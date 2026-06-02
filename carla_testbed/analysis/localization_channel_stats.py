from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

LOCALIZATION_CHANNEL = "/apollo/localization/pose"
DEFAULT_MIN_FRESH_HZ = 10.0


def build_localization_channel_stats(
    *,
    channel_stats: Mapping[str, Any] | None = None,
    timeseries_rows: Sequence[Mapping[str, Any]] | None = None,
    bridge_stats: Mapping[str, Any] | None = None,
    min_fresh_hz: float = DEFAULT_MIN_FRESH_HZ,
) -> dict[str, Any]:
    """Build localization channel evidence without confusing republish count with fresh samples."""

    rows = list(timeseries_rows or [])
    result = _empty_result()
    warnings: list[str] = []
    blocking_reasons: list[str] = []

    official_entry = _official_channel_entry(channel_stats)
    if official_entry is not None:
        result.update(_stats_from_official_entry(official_entry))
        result["channel_stats_source"] = "cyber_channel_stats"

    row_stats = _stats_from_rows(rows)
    if row_stats is not None:
        source = str(result.get("channel_stats_source") or "")
        official_timestamp_non_decreasing = result.get("timestamp_non_decreasing")
        official_sequence_monotonic = result.get("sequence_monotonic")
        result.update(row_stats)
        if official_timestamp_non_decreasing is False:
            result["timestamp_monotonic"] = False
            result["timestamp_non_decreasing"] = False
            result["timestamp_strictly_increasing"] = False
        if official_sequence_monotonic is False:
            result["sequence_monotonic"] = False
        result["channel_stats_source"] = "mixed" if source else "debug_timeseries"

    if official_entry is None and row_stats is None:
        bridge_result = _stats_from_bridge(bridge_stats)
        if bridge_result is not None:
            result.update(bridge_result)
            result["status"] = "insufficient_data"
            warnings.append("localization_fresh_sample_stats_missing")
        else:
            warnings.append("channel_stats_missing")
        result["_warnings"] = warnings
        result["_blocking_reasons"] = blocking_reasons
        return result

    if result.get("timestamp_non_decreasing") is False:
        result["status"] = "fail"
        blocking_reasons.append("localization_timestamp_non_monotonic")
    elif result.get("sequence_monotonic") is False:
        result["status"] = "fail"
        blocking_reasons.append("localization_sequence_non_monotonic")
    else:
        result["status"] = "pass"

    fresh_hz = _number_or_none(result.get("fresh_sample_hz"))
    publish_hz = _number_or_none(result.get("publish_hz"))
    fresh_sample_rate_ok = bool(fresh_hz is not None and fresh_hz >= float(min_fresh_hz))
    if fresh_hz is None:
        result["status"] = "insufficient_data" if result["status"] != "fail" else "fail"
        warnings.append("fresh_sample_hz_missing")
    elif fresh_hz < float(min_fresh_hz):
        warnings.append("localization_fresh_sample_hz_low")
        if result["status"] != "fail":
            result["status"] = "warn"
    if publish_hz is not None and fresh_hz is not None and publish_hz >= float(min_fresh_hz) and fresh_hz < float(min_fresh_hz):
        warnings.append("publish_hz_high_but_fresh_sample_hz_low")
        if result["status"] != "fail":
            result["status"] = "warn"

    if result.get("timestamp_strictly_increasing") is False:
        result["stale_republish_detected"] = True
        if fresh_sample_rate_ok and result.get("timestamp_non_decreasing") is True:
            result["stale_republish_status"] = "policy_ok"
            result["republish_policy"] = "stale_republish_allowed_when_fresh_sample_rate_ok"
        else:
            warnings.append("duplicate_localization_timestamps_detected")
            result["stale_republish_status"] = "warn"
            if result["status"] != "fail":
                result["status"] = "warn"

    stale_count = _number_or_none(result.get("stale_count"))
    if stale_count is not None and stale_count > 0 and result.get("stale_republish_status") != "policy_ok":
        warnings.append("localization_channel_stale_messages")
        if result["status"] != "fail":
            result["status"] = "warn"

    result["_warnings"] = warnings
    result["_blocking_reasons"] = blocking_reasons
    return result


def _empty_result() -> dict[str, Any]:
    return {
        "name": LOCALIZATION_CHANNEL,
        "message_type": "LocalizationEstimate",
        "hz": None,
        "message_count": None,
        "max_gap_ms": None,
        "timestamp_monotonic": None,
        "timestamp_non_decreasing": None,
        "timestamp_strictly_increasing": None,
        "sequence_monotonic": None,
        "stale_count": None,
        "publish_message_count": None,
        "fresh_sample_count": None,
        "unique_timestamp_count": None,
        "duplicate_timestamp_count": None,
        "duplicate_timestamp_ratio": None,
        "publish_hz": None,
        "fresh_sample_hz": None,
        "stale_republish_detected": None,
        "stale_republish_status": None,
        "republish_policy": None,
        "channel_stats_source": None,
        "status": "insufficient_data",
    }


def _official_channel_entry(channel_stats: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if not isinstance(channel_stats, Mapping):
        return None
    channels = channel_stats.get("channels")
    if not isinstance(channels, Mapping):
        return None
    entry = channels.get(LOCALIZATION_CHANNEL) or channels.get("localization")
    return entry if isinstance(entry, Mapping) else None


def _stats_from_official_entry(entry: Mapping[str, Any]) -> dict[str, Any]:
    message_count = _number_or_none(entry.get("message_count"))
    hz = _number_or_none(entry.get("hz"))
    timestamp_monotonic = entry.get("timestamp_monotonic")
    sequence_monotonic = entry.get("sequence_monotonic")
    return {
        "hz": hz,
        "message_count": message_count,
        "publish_message_count": message_count,
        "fresh_sample_count": message_count,
        "unique_timestamp_count": message_count,
        "duplicate_timestamp_count": 0,
        "duplicate_timestamp_ratio": 0.0 if message_count else None,
        "publish_hz": hz,
        "fresh_sample_hz": hz,
        "max_gap_ms": _number_or_none(entry.get("max_gap_ms")),
        "timestamp_monotonic": timestamp_monotonic,
        "timestamp_non_decreasing": timestamp_monotonic,
        "timestamp_strictly_increasing": timestamp_monotonic,
        "sequence_monotonic": sequence_monotonic,
        "stale_count": _number_or_none(entry.get("stale_count")),
        "stale_republish_detected": False,
    }


def _stats_from_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    timestamps = [
        value
        for row in rows
        if (value := _row_timestamp(row)) is not None
    ]
    if not timestamps:
        return None
    unique_timestamps = _unique_preserving_order(timestamps)
    duplicate_count = max(0, len(timestamps) - len(unique_timestamps))
    sequence_values = [
        value
        for row in rows
        if (value := _number_or_none(_first_present(row, "localization_sequence_num", "sequence_num"))) is not None
    ]
    return {
        "hz": _rate_hz(timestamps),
        "message_count": len(timestamps),
        "publish_message_count": len(timestamps),
        "fresh_sample_count": len(unique_timestamps),
        "unique_timestamp_count": len(unique_timestamps),
        "duplicate_timestamp_count": duplicate_count,
        "duplicate_timestamp_ratio": duplicate_count / len(timestamps) if timestamps else None,
        "publish_hz": _rate_hz(timestamps),
        "fresh_sample_hz": _rate_hz(unique_timestamps),
        "max_gap_ms": _max_gap_ms(unique_timestamps),
        "timestamp_monotonic": _non_decreasing(timestamps),
        "timestamp_non_decreasing": _non_decreasing(timestamps),
        "timestamp_strictly_increasing": _strictly_increasing(timestamps),
        "sequence_monotonic": _non_decreasing(sequence_values) if sequence_values else None,
        "stale_count": duplicate_count,
        "stale_republish_detected": duplicate_count > 0,
    }


def _stats_from_bridge(bridge_stats: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(bridge_stats, Mapping):
        return None
    loc_count = _number_or_none(bridge_stats.get("loc_count"))
    if loc_count is None:
        return None
    elapsed = _number_or_none(bridge_stats.get("publish_elapsed_sim_sec")) or _number_or_none(
        bridge_stats.get("publish_elapsed_wall_sec")
    )
    publish_hz = loc_count / elapsed if elapsed and elapsed > 0 else None
    return {
        "message_count": loc_count,
        "publish_message_count": loc_count,
        "publish_hz": publish_hz,
        "hz": publish_hz,
        "channel_stats_source": "bridge_stats",
    }


def _row_timestamp(row: Mapping[str, Any]) -> float | None:
    return _number_or_none(
        _first_present(
            row,
            "localization_header_timestamp_sec",
            "localization_measurement_time",
            "measurement_time",
            "localization_timestamp",
        )
    )


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in {None, ""}:
            return value
    return None


def _unique_preserving_order(values: Sequence[float]) -> list[float]:
    out: list[float] = []
    seen: set[float] = set()
    for value in values:
        rounded = round(float(value), 9)
        if rounded in seen:
            continue
        seen.add(rounded)
        out.append(float(value))
    return out


def _rate_hz(timestamps: Sequence[float]) -> float | None:
    if len(timestamps) < 2:
        return None
    elapsed = float(timestamps[-1]) - float(timestamps[0])
    if not math.isfinite(elapsed) or elapsed <= 0.0:
        return None
    return (len(timestamps) - 1) / elapsed


def _max_gap_ms(timestamps: Sequence[float]) -> float | None:
    if len(timestamps) < 2:
        return None
    gaps = [float(curr) - float(prev) for prev, curr in zip(timestamps, timestamps[1:])]
    if not gaps:
        return None
    return max(gaps) * 1000.0


def _non_decreasing(values: Sequence[float]) -> bool | None:
    if len(values) < 2:
        return None
    return all(float(curr) >= float(prev) for prev, curr in zip(values, values[1:]))


def _strictly_increasing(values: Sequence[float]) -> bool | None:
    if len(values) < 2:
        return None
    return all(float(curr) > float(prev) for prev, curr in zip(values, values[1:]))


def _number_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
