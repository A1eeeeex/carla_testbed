from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml

CHANNEL_HEALTH_CONFIG_SCHEMA_VERSION = "apollo_natural_driving_channels.v1"
CHANNEL_HEALTH_REPORT_SCHEMA_VERSION = "apollo_channel_health_report.v1"
PLANNING_WARN_MAX_GAP_MS = 500.0
PLANNING_WARN_SIM_TIME_MAX_GAP_MS = 500.0
PLANNING_WARN_GAP_COUNT_OVER_250MS = 0
DEFAULT_TRAFFIC_LIGHT_SCENARIOS = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
REQUIRED_STAT_FIELDS = {
    "message_count",
    "hz",
    "max_gap_ms",
    "timestamp_monotonic",
    "sequence_monotonic",
    "stale_count",
}


class ApolloChannelHealthError(ValueError):
    pass


def load_channel_health_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path).expanduser()
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ApolloChannelHealthError(f"channel health config must be a mapping: {config_path}")
    errors = validate_channel_health_config(payload)
    if errors:
        raise ApolloChannelHealthError("; ".join(errors))
    payload["_source_path"] = str(config_path)
    return payload


def load_channel_stats(path: str | Path) -> dict[str, Any]:
    stats_path = Path(path).expanduser()
    payload = json.loads(stats_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ApolloChannelHealthError(f"channel stats must be a mapping: {stats_path}")
    payload["_source_path"] = str(stats_path)
    return payload


def validate_channel_health_config(config: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if config.get("schema_version") != CHANNEL_HEALTH_CONFIG_SCHEMA_VERSION:
        errors.append(f"schema_version must be {CHANNEL_HEALTH_CONFIG_SCHEMA_VERSION}")
    channels = config.get("channels")
    if not isinstance(channels, list) or not channels:
        errors.append("channels must be a non-empty list")
        return errors

    names: set[str] = set()
    channel_paths: set[str] = set()
    for entry in channels:
        if not isinstance(entry, Mapping):
            errors.append("channel entry must be a mapping")
            continue
        name = str(entry.get("name") or "")
        channel = str(entry.get("channel") or "")
        if not name:
            errors.append("channel entry missing name")
        if not channel:
            errors.append(f"{name or '<missing name>'}: missing channel")
        if name in names:
            errors.append(f"duplicate channel name: {name}")
        if channel in channel_paths:
            errors.append(f"duplicate channel path: {channel}")
        names.add(name)
        channel_paths.add(channel)

    required_names = {"localization", "chassis", "obstacles", "traffic_light", "planning", "control"}
    missing = sorted(required_names - names)
    if missing:
        errors.append(f"channels missing required entries: {', '.join(missing)}")
    return errors


def analyze_apollo_channel_health(
    config: Mapping[str, Any],
    stats: Mapping[str, Any],
    *,
    scenario_class: str | None = None,
) -> dict[str, Any]:
    errors = validate_channel_health_config(config)
    if errors:
        raise ApolloChannelHealthError("; ".join(errors))

    channels = [entry for entry in config.get("channels", []) if isinstance(entry, Mapping)]
    stats_channels = _stats_channels(stats)
    status = "pass"
    channel_results: dict[str, dict[str, Any]] = {}
    missing_required_channels: list[str] = []
    missing_optional_channels: list[str] = []
    low_rate_channels: list[str] = []
    gap_failures: list[str] = []
    planning_gap_warning_channels: list[str] = []
    timestamp_failures: list[str] = []
    sequence_failures: list[str] = []
    stale_channels: list[str] = []
    warnings: list[str] = []

    for channel_config in channels:
        result = _check_channel(channel_config, stats_channels, scenario_class=scenario_class)
        name = str(channel_config["name"])
        channel_results[name] = result
        status = _combine_status(status, result["status"])
        issues = set(result.get("issues") or [])
        if "missing_channel" in issues:
            if result.get("required"):
                missing_required_channels.append(name)
            else:
                missing_optional_channels.append(name)
        if "low_rate" in issues:
            low_rate_channels.append(name)
        if "message_gap_too_large" in issues:
            gap_failures.append(name)
        if "planning_gap_warn" in issues:
            planning_gap_warning_channels.append(name)
        if "timestamp_non_monotonic" in issues and result.get("status") == "fail":
            timestamp_failures.append(name)
        if "sequence_non_monotonic" in issues and result.get("status") == "fail":
            sequence_failures.append(name)
        if "stale_messages" in issues:
            stale_channels.append(name)
        warnings.extend(result.get("warnings") or [])

    if missing_required_channels or timestamp_failures or sequence_failures:
        status = "fail"

    return {
        "schema_version": CHANNEL_HEALTH_REPORT_SCHEMA_VERSION,
        "status": status,
        "scenario_class": scenario_class,
        "channel_results": channel_results,
        "missing_required_channels": missing_required_channels,
        "missing_optional_channels": missing_optional_channels,
        "low_rate_channels": low_rate_channels,
        "gap_failures": gap_failures,
        "planning_gap_warning_channels": planning_gap_warning_channels,
        "timestamp_failures": timestamp_failures,
        "sequence_failures": sequence_failures,
        "stale_channels": stale_channels,
        "warnings": sorted(set(warnings)),
        "source": {
            "config_schema_version": config.get("schema_version"),
            "config_path": config.get("_source_path"),
            "stats_schema_version": stats.get("schema_version"),
            "stats_created_at": stats.get("created_at"),
            "stats_path": stats.get("_source_path"),
            "stats_source": stats.get("source"),
        },
        "interpretation_boundary": (
            "Channel health checks transport/rate/timestamp/gap evidence only; "
            "it does not prove Apollo behavior correctness."
        ),
    }


def analyze_apollo_channel_health_files(
    config_path: str | Path,
    stats_path: str | Path,
    *,
    scenario_class: str | None = None,
) -> dict[str, Any]:
    return analyze_apollo_channel_health(
        load_channel_health_config(config_path),
        load_channel_stats(stats_path),
        scenario_class=scenario_class,
    )


def write_apollo_channel_health_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "apollo_channel_health_report.json"
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"apollo_channel_health_report": str(path)}


def _stats_channels(stats: Mapping[str, Any]) -> Mapping[str, Any]:
    channels = stats.get("channels")
    return channels if isinstance(channels, Mapping) else {}


def _check_channel(
    config: Mapping[str, Any],
    stats_channels: Mapping[str, Any],
    *,
    scenario_class: str | None,
) -> dict[str, Any]:
    name = str(config.get("name") or "")
    channel = str(config.get("channel") or "")
    required = _is_required(config, scenario_class)
    entry = _find_stats(stats_channels, name, channel)
    result: dict[str, Any] = {
        "name": name,
        "channel": channel,
        "required": required,
        "status": "pass",
        "message_count": None,
        "hz": None,
        "max_gap_ms": None,
        "timestamp_monotonic": None,
        "sequence_monotonic": None,
        "stale_count": None,
        "issues": [],
        "warnings": [],
    }
    if entry is None:
        result["status"] = "fail" if required else "warn"
        result["issues"].append("missing_channel")
        if not required:
            result["warnings"].append(f"{name}_missing_optional_for_{scenario_class or 'unspecified_scenario'}")
        return result

    missing_stat_fields = sorted(field for field in REQUIRED_STAT_FIELDS if field not in entry)
    if missing_stat_fields:
        result["status"] = "fail" if required else _combine_status(result["status"], "warn")
        result["issues"].extend(f"missing_stat_{field}" for field in missing_stat_fields)

    message_count = entry.get("message_count")
    hz = entry.get("hz")
    max_gap_ms = entry.get("max_gap_ms")
    stale_count = entry.get("stale_count")
    result.update(
        {
            "message_count": message_count,
            "hz": hz,
            "max_gap_ms": max_gap_ms,
            "timestamp_monotonic": entry.get("timestamp_monotonic"),
            "sequence_monotonic": entry.get("sequence_monotonic"),
            "stale_count": stale_count,
            "first_timestamp": entry.get("first_timestamp"),
            "last_timestamp": entry.get("last_timestamp"),
            "max_gap_start_timestamp": entry.get("max_gap_start_timestamp"),
            "max_gap_end_timestamp": entry.get("max_gap_end_timestamp"),
            "max_gap_index": entry.get("max_gap_index"),
            "gap_p95_ms": entry.get("gap_p95_ms"),
            "gap_count_over_250ms": entry.get("gap_count_over_250ms"),
            "gap_count_over_1000ms": entry.get("gap_count_over_1000ms"),
            "source": entry.get("source"),
            "evidence_source": entry.get("evidence_source"),
            "derived_from_bridge_counters": entry.get("derived_from_bridge_counters"),
            "max_gap_ms_estimated": entry.get("max_gap_ms_estimated"),
            "sequence_inferred_from_timestamps": entry.get("sequence_inferred_from_timestamps"),
            "fresh_message_count": entry.get("fresh_message_count"),
            "fresh_world_frame_hz": entry.get("fresh_world_frame_hz"),
            "delivery_wall_hz": entry.get("delivery_wall_hz"),
            "header_sim_hz": entry.get("header_sim_hz"),
            "duplicate_timestamp_count": entry.get("duplicate_timestamp_count"),
            "promotion_grade_evidence": entry.get("promotion_grade_evidence"),
            "obstacle_count": entry.get("obstacle_count"),
            "light_count": entry.get("light_count"),
            "empty_message_count": entry.get("empty_message_count"),
            "primary_time_axis": entry.get("primary_time_axis"),
            "sim_time_hz": entry.get("sim_time_hz"),
            "sim_time_max_gap_ms": entry.get("sim_time_max_gap_ms"),
            "sim_time_gap_p95_ms": entry.get("sim_time_gap_p95_ms"),
            "sim_time_gap_count_over_250ms": entry.get("sim_time_gap_count_over_250ms"),
            "sim_time_gap_count_over_1000ms": entry.get("sim_time_gap_count_over_1000ms"),
            "sim_time_timestamp_monotonic": entry.get("sim_time_timestamp_monotonic"),
        }
    )

    if not isinstance(message_count, (int, float)) or message_count <= 0:
        result["issues"].append("no_messages")
        result["status"] = "fail" if required else _combine_status(result["status"], "warn")
        if not required:
            result["warnings"].append(f"{name}_has_no_messages_optional_for_{scenario_class or 'unspecified_scenario'}")
            return result

    min_hz = config.get("min_hz")
    if isinstance(min_hz, (int, float)) and min_hz > 0:
        if not isinstance(hz, (int, float)) or hz < min_hz:
            result["issues"].append("low_rate")
            result["status"] = "fail" if required else _combine_status(result["status"], "warn")

    max_allowed_gap = config.get("max_gap_ms")
    if isinstance(max_allowed_gap, (int, float)) and max_allowed_gap > 0:
        if not isinstance(max_gap_ms, (int, float)):
            result["issues"].append("missing_or_invalid_max_gap_ms")
            result["status"] = "fail" if required else _combine_status(result["status"], "warn")
        elif max_gap_ms > max_allowed_gap:
            sim_gap_ms = entry.get("sim_time_max_gap_ms")
            if isinstance(sim_gap_ms, (int, float)) and sim_gap_ms <= max_allowed_gap:
                result["issues"].append("message_gap_time_axis_warn")
                result["warnings"].append(f"{name}_header_or_wall_gap_large_but_sim_time_gap_within_limit")
                result["time_axis_diagnosis"] = "header_or_wall_time_gap_large_sim_time_gap_ok"
                result["status"] = _combine_status(result["status"], "warn")
            else:
                result["issues"].append("message_gap_too_large")
                result["status"] = "fail" if required else _combine_status(result["status"], "warn")

    if name == "planning":
        planning_warn_reasons: list[str] = []
        if isinstance(max_gap_ms, (int, float)) and max_gap_ms > PLANNING_WARN_MAX_GAP_MS:
            planning_warn_reasons.append("planning_header_or_wall_gap_over_500ms")
        sim_gap_ms = entry.get("sim_time_max_gap_ms")
        if isinstance(sim_gap_ms, (int, float)) and sim_gap_ms > PLANNING_WARN_SIM_TIME_MAX_GAP_MS:
            planning_warn_reasons.append("planning_sim_time_gap_over_500ms")
        gap_count_250 = entry.get("gap_count_over_250ms")
        sim_gap_count_250 = entry.get("sim_time_gap_count_over_250ms")
        if isinstance(gap_count_250, (int, float)) and gap_count_250 > PLANNING_WARN_GAP_COUNT_OVER_250MS:
            planning_warn_reasons.append("planning_gap_count_over_250ms_nonzero")
        if isinstance(sim_gap_count_250, (int, float)) and sim_gap_count_250 > PLANNING_WARN_GAP_COUNT_OVER_250MS:
            planning_warn_reasons.append("planning_sim_time_gap_count_over_250ms_nonzero")
        if planning_warn_reasons:
            result["issues"].append("planning_gap_warn")
            result["warnings"].extend(planning_warn_reasons)
            result["status"] = _combine_status(result["status"], "warn")

    max_stale_count = config.get("max_stale_count")
    if isinstance(max_stale_count, (int, float)):
        if isinstance(stale_count, (int, float)) and stale_count > max_stale_count:
            result["issues"].append("stale_messages")
            result["status"] = _combine_status(result["status"], "warn")

    if bool(config.get("timestamp_monotonic_required")) and entry.get("timestamp_monotonic") is not True:
        result["issues"].append("timestamp_non_monotonic")
        result["status"] = "fail" if required else _combine_status(result["status"], "warn")

    if bool(config.get("sequence_monotonic_required")) and entry.get("sequence_monotonic") is not True:
        result["issues"].append("sequence_non_monotonic")
        result["status"] = "fail" if required else _combine_status(result["status"], "warn")

    if entry.get("derived_from_bridge_counters") is True or entry.get("max_gap_ms_estimated") is True:
        result["issues"].append("derived_or_estimated_channel_stats")
        result["warnings"].append(
            f"{name}_stats_derived_from_bridge_counters_not_promotion_grade"
        )
        result["status"] = _combine_status(result["status"], "warn")

    return result


def _is_required(config: Mapping[str, Any], scenario_class: str | None) -> bool:
    if bool(config.get("required")):
        return True
    required_scenarios = config.get("required_for_scenario_classes")
    if isinstance(required_scenarios, list) and scenario_class:
        return scenario_class in {str(item) for item in required_scenarios}
    if str(config.get("name") or "") == "traffic_light":
        configured = config.get("required_for_scenario_classes")
        scenario_set = (
            {str(item) for item in configured}
            if isinstance(configured, list)
            else DEFAULT_TRAFFIC_LIGHT_SCENARIOS
        )
        return bool(scenario_class and scenario_class in scenario_set)
    return False


def _find_stats(stats_channels: Mapping[str, Any], name: str, channel: str) -> Mapping[str, Any] | None:
    for key in (channel, name):
        value = stats_channels.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "fail": 2}.get(status, 2)


def _combine_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current
