from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "apollo_channel_contract.v1"
REPORT_SCHEMA_VERSION = "adapter_contract_report.v1"
CHANNEL_REQUIRED_FIELDS = {
    "name",
    "channel",
    "required_for_levels",
    "min_hz",
    "max_message_age_ms",
    "timestamp_required",
    "sequence_monotonic_required",
}
FAIL_REQUIRED_CHANNELS = {"localization", "chassis", "planning", "control"}


class ChannelContractError(ValueError):
    pass


@dataclass(frozen=True)
class ChannelContractCheck:
    report: dict[str, Any]

    @property
    def status(self) -> str:
        return str(self.report.get("status") or "fail")

    @property
    def ok(self) -> bool:
        return self.status != "fail"


def _channels(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = contract.get("channels")
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _channel_stats(stats: Mapping[str, Any]) -> Mapping[str, Any]:
    channels = stats.get("channels")
    return channels if isinstance(channels, Mapping) else {}


def _missing_required_fields(channel: Mapping[str, Any]) -> list[str]:
    return sorted(CHANNEL_REQUIRED_FIELDS - set(channel))


def validate_channel_contract(contract: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if contract.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    channels = _channels(contract)
    if not channels:
        errors.append("channels must be a non-empty list")
        return errors
    names: set[str] = set()
    for channel in channels:
        missing = _missing_required_fields(channel)
        name = str(channel.get("name") or "<missing name>")
        if missing:
            errors.append(f"{name}: missing fields {', '.join(missing)}")
        if name in names:
            errors.append(f"duplicate channel name: {name}")
        names.add(name)
        if not isinstance(channel.get("required_for_levels"), list):
            errors.append(f"{name}: required_for_levels must be a list")
    missing_core = sorted(FAIL_REQUIRED_CHANNELS - names)
    if missing_core:
        errors.append(f"channels missing core entries: {', '.join(missing_core)}")
    return errors


def load_channel_contract(path: str | Path) -> dict[str, Any]:
    contract_path = Path(path).expanduser()
    payload = yaml.safe_load(contract_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ChannelContractError(f"channel contract must be a mapping: {contract_path}")
    errors = validate_channel_contract(payload)
    if errors:
        raise ChannelContractError("; ".join(errors))
    payload["_source_path"] = str(contract_path)
    return payload


def load_channel_stats(path: str | Path) -> dict[str, Any]:
    stats_path = Path(path).expanduser()
    payload = json.loads(stats_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ChannelContractError(f"channel stats must be a mapping: {stats_path}")
    return payload


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "fail": 2}.get(status, 2)


def _combine_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current


def _channel_identifier(config: Mapping[str, Any]) -> str:
    return str(config.get("channel") or config.get("name") or "")


def _get_stats_entry(stats_channels: Mapping[str, Any], config: Mapping[str, Any]) -> Mapping[str, Any] | None:
    name = str(config.get("name") or "")
    channel = _channel_identifier(config)
    value = stats_channels.get(channel)
    if isinstance(value, Mapping):
        return value
    value = stats_channels.get(name)
    if isinstance(value, Mapping):
        return value
    return None


def _is_channel_hard_required(config: Mapping[str, Any]) -> bool:
    if bool(config.get("optional")):
        return False
    return str(config.get("name") or "") in FAIL_REQUIRED_CHANNELS


def _check_one_channel(config: Mapping[str, Any], entry: Mapping[str, Any] | None) -> dict[str, Any]:
    name = str(config.get("name"))
    channel = _channel_identifier(config)
    result: dict[str, Any] = {
        "name": name,
        "channel": channel,
        "status": "pass",
        "message_count": None,
        "hz": None,
        "max_gap_ms": None,
        "max_message_age_ms": config.get("max_message_age_ms"),
        "timestamp_monotonic": None,
        "sequence_monotonic": None,
        "stale_count": None,
        "issues": [],
    }
    hard_required = _is_channel_hard_required(config)
    if entry is None:
        result["status"] = "fail" if hard_required else "warn"
        result["issues"].append("missing_channel")
        return result

    message_count = entry.get("message_count")
    hz = entry.get("hz")
    max_gap_ms = entry.get("max_gap_ms")
    stale_count = entry.get("stale_count")
    result.update(
        {
            "message_count": message_count,
            "hz": hz,
            "first_timestamp": entry.get("first_timestamp"),
            "last_timestamp": entry.get("last_timestamp"),
            "max_gap_ms": max_gap_ms,
            "timestamp_monotonic": entry.get("timestamp_monotonic"),
            "sequence_monotonic": entry.get("sequence_monotonic"),
            "stale_count": stale_count,
        }
    )

    if not isinstance(message_count, (int, float)) or message_count <= 0:
        result["issues"].append("no_messages")
        result["status"] = "fail" if hard_required else _combine_status(result["status"], "warn")

    min_hz = config.get("min_hz")
    if isinstance(min_hz, (int, float)) and min_hz > 0:
        if not isinstance(hz, (int, float)) or hz < min_hz:
            result["issues"].append("low_rate")
            result["status"] = "fail" if hard_required else _combine_status(result["status"], "warn")

    age_limit = config.get("max_message_age_ms")
    if isinstance(age_limit, (int, float)) and age_limit > 0:
        if isinstance(max_gap_ms, (int, float)) and max_gap_ms > age_limit:
            result["issues"].append("stale")
            result["status"] = _combine_status(result["status"], "warn")
        if isinstance(stale_count, (int, float)) and stale_count > 0:
            result["issues"].append("stale")
            result["status"] = _combine_status(result["status"], "warn")

    if bool(config.get("timestamp_required")) and entry.get("timestamp_monotonic") is not True:
        result["issues"].append("timestamp_non_monotonic")
        result["status"] = "fail"

    if bool(config.get("sequence_monotonic_required")) and entry.get("sequence_monotonic") is not True:
        result["issues"].append("sequence_non_monotonic")
        result["status"] = "fail"

    return result


def generate_adapter_contract_report(
    contract: Mapping[str, Any],
    stats: Mapping[str, Any],
) -> dict[str, Any]:
    errors = validate_channel_contract(contract)
    if errors:
        raise ChannelContractError("; ".join(errors))

    stats_channels = _channel_stats(stats)
    channel_results: dict[str, dict[str, Any]] = {}
    status = "pass"
    missing_required_channels: list[str] = []
    stale_channels: list[str] = []
    low_rate_channels: list[str] = []
    timestamp_failures: list[str] = []

    for channel_config in _channels(contract):
        name = str(channel_config.get("name"))
        result = _check_one_channel(channel_config, _get_stats_entry(stats_channels, channel_config))
        channel_results[name] = result
        status = _combine_status(status, result["status"])
        issues = set(result.get("issues") or [])
        if "missing_channel" in issues and _is_channel_hard_required(channel_config):
            missing_required_channels.append(name)
        if "stale" in issues:
            stale_channels.append(name)
        if "low_rate" in issues:
            low_rate_channels.append(name)
        if "timestamp_non_monotonic" in issues:
            timestamp_failures.append(name)

    if missing_required_channels or timestamp_failures:
        status = "fail"

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "channel_results": channel_results,
        "missing_required_channels": missing_required_channels,
        "stale_channels": stale_channels,
        "low_rate_channels": low_rate_channels,
        "timestamp_failures": timestamp_failures,
        "source": {
            "contract_schema_version": contract.get("schema_version"),
            "stats_schema_version": stats.get("schema_version"),
            "stats_created_at": stats.get("created_at"),
        },
    }


def check_channel_stats(contract: Mapping[str, Any], stats: Mapping[str, Any]) -> ChannelContractCheck:
    return ChannelContractCheck(generate_adapter_contract_report(contract, stats))


def check_channel_stats_file(contract: Mapping[str, Any], stats_path: str | Path) -> ChannelContractCheck:
    return check_channel_stats(contract, load_channel_stats(stats_path))
