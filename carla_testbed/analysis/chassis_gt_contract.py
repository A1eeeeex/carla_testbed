from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.channel_stats_normalizer import (
    channel_stats_candidate_paths,
    normalize_channel_stats_for_run,
)

REPORT_SCHEMA_VERSION = "chassis_gt_contract.v1"
CHASSIS_CHANNEL = "/apollo/canbus/chassis"
DEFAULT_MESSAGE_TYPE = "Chassis"
DEFAULT_SPEED_WARN_MPS = 0.50
DEFAULT_SPEED_FAIL_MPS = 2.00
DEFAULT_GAP_WARN_MS = 250.0
DEFAULT_GAP_FAIL_MS = 1000.0

ALIASES = {
    "chassis_speed_mps": ["chassis_speed_mps", "chassis_speed", "vehicle_speed_mps"],
    "ego_speed_mps": ["ego_speed", "ego_speed_mps", "speed_mps", "localization_speed_mps"],
    "driving_mode": ["driving_mode", "driving_mode_value", "chassis_driving_mode"],
    "gear_location": ["gear_location", "gear_location_value", "chassis_gear_location"],
    "error_code": [
        "chassis_error_code",
        "chassis_error_code_value",
        "chassis_error",
        "error_code",
    ],
    "throttle_applied": ["throttle_applied", "carla_throttle_applied", "commanded_throttle"],
    "throttle_feedback": [
        "throttle_feedback",
        "measured_throttle",
        "chassis_throttle_percentage",
    ],
    "brake_applied": ["brake_applied", "carla_brake_applied", "commanded_brake"],
    "brake_feedback": [
        "brake_feedback",
        "measured_brake",
        "chassis_brake_percentage",
    ],
    "steer_applied": ["carla_steer_applied", "steer_applied", "applied_steer"],
    "steer_feedback": [
        "steer_feedback",
        "measured_steer",
        "measured_steer_pct",
        "chassis_steering_percentage",
        "steering_percentage",
    ],
}


def analyze_chassis_gt_contract_files(
    *,
    run_dir: str | Path | None = None,
    timeseries_path: str | Path | None = None,
    channel_stats_path: str | Path | None = None,
    summary_path: str | Path | None = None,
    manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser() if run_dir else None
    if root is not None:
        timeseries_path = timeseries_path or _find_first(
            root,
            (
                "artifacts/debug_timeseries.csv",
                "debug_timeseries.csv",
                "timeseries.csv",
                "timeseries.jsonl",
                "artifacts/timeseries.csv",
                "artifacts/timeseries.jsonl",
            ),
        )
        channel_stats_path = channel_stats_path or _find_first(
            root,
            tuple(str(path.relative_to(root)) for path in channel_stats_candidate_paths(root))
            + ("artifacts/cyber_bridge_stats.json",),
        )
        if channel_stats_path is None:
            generated_stats = normalize_channel_stats_for_run(root)
            if generated_stats is not None:
                channel_stats_path = (
                    generated_stats.get("_normalized_output_path")
                    or generated_stats.get("_output_path")
                )
        summary_path = summary_path or _find_first(root, ("summary.json",))
        manifest_path = manifest_path or _find_first(root, ("manifest.json",))
    return analyze_chassis_gt_contract(
        timeseries_rows=_read_timeseries(Path(timeseries_path).expanduser() if timeseries_path else None),
        channel_stats=_read_json(Path(channel_stats_path).expanduser() if channel_stats_path else None),
        summary=_read_json(Path(summary_path).expanduser() if summary_path else None),
        manifest=_read_json(Path(manifest_path).expanduser() if manifest_path else None),
        source={
            "run_dir": str(root) if root is not None else None,
            "timeseries_path": str(timeseries_path) if timeseries_path else None,
            "channel_stats_path": str(channel_stats_path) if channel_stats_path else None,
            "summary_path": str(summary_path) if summary_path else None,
            "manifest_path": str(manifest_path) if manifest_path else None,
        },
    )


def analyze_chassis_gt_contract(
    *,
    timeseries_rows: Sequence[Mapping[str, Any]] | None = None,
    channel_stats: Mapping[str, Any] | None = None,
    summary: Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = list(timeseries_rows or [])
    channel_stats = channel_stats or {}
    summary = summary or {}
    manifest = manifest or {}
    source = source or {}
    missing_fields: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []

    channel = _analyze_channel(channel_stats, missing_fields, warnings, blocking_reasons)
    speed = _speed_consistency(rows, missing_fields, warnings, blocking_reasons)
    feedback = _control_feedback(rows, warnings)
    state = _state_consistency(rows, missing_fields, warnings, blocking_reasons)
    if not rows:
        missing_fields.append("timeseries")
        warnings.append("timeseries_missing")

    status = "pass"
    if blocking_reasons:
        status = "fail"
    if _has_insufficient_channel(channel) or not rows:
        status = "insufficient_data" if status != "fail" else status
    if not blocking_reasons and warnings and status == "pass":
        status = "warn"

    claim_grade = status in {"pass", "warn"} and not blocking_reasons and not missing_fields
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": _text(summary.get("run_id") or manifest.get("run_id")),
        "route_id": _text(summary.get("route_id") or manifest.get("route_id")),
        "backend": _text(summary.get("backend") or manifest.get("backend")),
        "channel": channel,
        "speed_consistency": speed,
        "control_feedback": feedback,
        "state": state,
        "missing_fields": sorted(set(missing_fields)),
        "warnings": sorted(set(warnings)),
        "blocking_reasons": sorted(set(blocking_reasons)),
        "status": status,
        "claim_grade": claim_grade,
        "verdict": {"status": status, "blocking_reasons": sorted(set(blocking_reasons))},
        "source": dict(source),
        "interpretation_boundary": (
            "Chassis GT contract checks Apollo chassis replacement semantics. It does not "
            "prove planning/control behavior correctness by itself."
        ),
    }
    return report


def write_chassis_gt_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "chassis_gt_contract_report.json"
    summary_path = output_dir / "chassis_gt_contract_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(chassis_gt_contract_summary_md(report), encoding="utf-8")
    return {
        "chassis_gt_contract_report": str(report_path),
        "chassis_gt_contract_summary": str(summary_path),
    }


def chassis_gt_contract_summary_md(report: Mapping[str, Any]) -> str:
    speed = report.get("speed_consistency") if isinstance(report.get("speed_consistency"), Mapping) else {}
    channel = report.get("channel") if isinstance(report.get("channel"), Mapping) else {}
    lines = [
        "# Apollo Chassis GT Contract",
        "",
        f"- Status: {report.get('status')}",
        f"- Claim grade: {report.get('claim_grade')}",
        f"- Channel message count: {channel.get('message_count')}",
        f"- Channel hz: {channel.get('hz')}",
        f"- Speed delta p95 mps: {speed.get('speed_delta_p95_mps')}",
        f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
        f"- Warnings: {', '.join(report.get('warnings') or []) or 'none'}",
    ]
    return "\n".join(lines) + "\n"


def _analyze_channel(
    stats: Mapping[str, Any],
    missing_fields: list[str],
    warnings: list[str],
    blocking_reasons: list[str],
) -> dict[str, Any]:
    entry = _find_channel_entry(stats)
    channel = {
        "name": CHASSIS_CHANNEL,
        "message_type": DEFAULT_MESSAGE_TYPE,
        "message_count": None,
        "hz": None,
        "max_gap_ms": None,
        "timestamp_monotonic": None,
        "sequence_monotonic": None,
        "stale_count": None,
        "status": "insufficient_data",
    }
    if entry is None:
        missing_fields.append("channel_stats.chassis")
        warnings.append("chassis_channel_stats_missing")
        return channel
    channel.update(
        {
            "message_count": entry.get("message_count"),
            "hz": entry.get("hz"),
            "max_gap_ms": entry.get("max_gap_ms"),
            "timestamp_monotonic": entry.get("timestamp_monotonic"),
            "sequence_monotonic": entry.get("sequence_monotonic"),
            "stale_count": entry.get("stale_count"),
        }
    )
    status = "pass"
    if _number(entry.get("message_count")) is None or (_number(entry.get("message_count")) or 0.0) <= 0:
        blocking_reasons.append("chassis_channel_no_messages")
        status = "fail"
    if entry.get("timestamp_monotonic") is False:
        blocking_reasons.append("chassis_timestamp_non_monotonic")
        status = "fail"
    if entry.get("sequence_monotonic") is False:
        blocking_reasons.append("chassis_sequence_non_monotonic")
        status = "fail"
    max_gap_ms = _number(entry.get("max_gap_ms"))
    if max_gap_ms is None:
        warnings.append("chassis_max_gap_ms_missing")
    elif max_gap_ms > DEFAULT_GAP_FAIL_MS:
        blocking_reasons.append("chassis_message_gap_too_large")
        status = "fail"
    elif max_gap_ms > DEFAULT_GAP_WARN_MS:
        warnings.append("chassis_message_gap_warn")
        if status == "pass":
            status = "warn"
    channel["status"] = status
    return channel


def _speed_consistency(
    rows: Sequence[Mapping[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
    blocking_reasons: list[str],
) -> dict[str, Any]:
    deltas: list[float] = []
    for row in rows:
        chassis_speed = _alias_number(row, "chassis_speed_mps")
        ego_speed = _alias_number(row, "ego_speed_mps")
        if chassis_speed is None or ego_speed is None:
            continue
        deltas.append(abs(chassis_speed - ego_speed))
    p95 = _percentile(deltas, 95.0)
    status = "pass"
    if not deltas:
        missing_fields.append("chassis_speed_mps_or_ego_speed_mps")
        warnings.append("chassis_speed_consistency_missing")
        status = "insufficient_data"
    elif p95 is not None and p95 > DEFAULT_SPEED_FAIL_MPS:
        blocking_reasons.append("chassis_speed_mismatch_high")
        status = "fail"
    elif p95 is not None and p95 > DEFAULT_SPEED_WARN_MPS:
        warnings.append("chassis_speed_mismatch_warn")
        status = "warn"
    return {
        "status": status,
        "sample_count": len(deltas),
        "speed_delta_p95_mps": p95,
        "warn_threshold_mps": DEFAULT_SPEED_WARN_MPS,
        "fail_threshold_mps": DEFAULT_SPEED_FAIL_MPS,
    }


def _control_feedback(rows: Sequence[Mapping[str, Any]], warnings: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for axis in ("throttle", "brake", "steer"):
        applied_key = f"{axis}_applied"
        feedback_key = f"{axis}_feedback"
        deltas: list[float] = []
        for row in rows:
            applied = _alias_number(row, applied_key)
            feedback = _alias_number(row, feedback_key)
            if applied is None or feedback is None:
                continue
            deltas.append(abs(_normalize_control_value(applied) - _normalize_control_value(feedback)))
        result[f"{axis}_feedback_available"] = bool(deltas)
        result[f"{axis}_applied_feedback_delta_p95"] = _percentile(deltas, 95.0)
        if not deltas:
            warnings.append(f"{axis}_feedback_missing")
    result["status"] = "warn" if any(not result.get(f"{axis}_feedback_available") for axis in ("throttle", "brake", "steer")) else "pass"
    return result


def _state_consistency(
    rows: Sequence[Mapping[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
    blocking_reasons: list[str],
) -> dict[str, Any]:
    modes = sorted({_alias_text(row, "driving_mode") for row in rows if _alias_text(row, "driving_mode")})
    gears = sorted({_alias_text(row, "gear_location") for row in rows if _alias_text(row, "gear_location")})
    errors = sorted({_alias_text(row, "error_code") for row in rows if _alias_text(row, "error_code")})
    status = "pass"
    if not modes:
        missing_fields.append("driving_mode")
        warnings.append("driving_mode_missing")
        status = "insufficient_data"
    elif any(not _is_auto_drive(mode) for mode in modes):
        blocking_reasons.append("chassis_driving_mode_not_auto")
        status = "fail"
    if gears and any(not _is_drive_gear(gear) for gear in gears):
        blocking_reasons.append("chassis_gear_not_drive")
        status = "fail"
    if errors and any(not _is_no_error(error) for error in errors):
        blocking_reasons.append("chassis_error_code_nonzero")
        status = "fail"
    return {
        "status": status,
        "driving_modes": modes,
        "gear_locations": gears,
        "error_codes": errors,
    }


def _find_channel_entry(stats: Mapping[str, Any]) -> Mapping[str, Any] | None:
    channels = stats.get("channels")
    if isinstance(channels, Mapping):
        for key in (CHASSIS_CHANNEL, "chassis", "/apollo/canbus/chassis"):
            value = channels.get(key)
            if isinstance(value, Mapping):
                return value
    results = stats.get("channel_results")
    if isinstance(results, Mapping):
        value = results.get("chassis")
        if isinstance(value, Mapping):
            return value
    return None


def _has_insufficient_channel(channel: Mapping[str, Any]) -> bool:
    return str(channel.get("status") or "") == "insufficient_data"


def _alias_number(row: Mapping[str, Any], key: str) -> float | None:
    for alias in ALIASES.get(key, [key]):
        value = _number(row.get(alias))
        if value is not None:
            return value
    return None


def _alias_text(row: Mapping[str, Any], key: str) -> str | None:
    for alias in ALIASES.get(key, [key]):
        value = row.get(alias)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _is_auto_drive(value: str) -> bool:
    upper = value.upper()
    return upper in {"1", "AUTO", "AUTONOMOUS", "COMPLETE_AUTO_DRIVE"}


def _is_drive_gear(value: str) -> bool:
    upper = value.upper()
    return upper in {"1", "3", "D", "DRIVE", "GEAR_DRIVE"}


def _is_no_error(value: str) -> bool:
    upper = value.upper()
    return upper in {"0", "NO_ERROR", "NONE"}


def _read_timeseries(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix.lower() == ".jsonl":
        rows: list[dict[str, Any]] = []
        try:
            with path.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    if isinstance(payload, dict):
                        rows.append(payload)
        except (OSError, json.JSONDecodeError):
            return []
        return rows
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except OSError:
        return []


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_first(root: Path, rels: Sequence[str]) -> Path | None:
    for rel in rels:
        candidate = root / rel
        if candidate.is_file():
            return candidate
    return None


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile / 100.0
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    frac = position - lower
    return ordered[lower] * (1.0 - frac) + ordered[upper] * frac


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_control_value(value: float) -> float:
    # Bridge rows may contain normalized CARLA controls (0..1) or Apollo
    # chassis feedback percentages (0..100). Keep signed steering semantics.
    if abs(value) > 1.5:
        return value / 100.0
    return value
