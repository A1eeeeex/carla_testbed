from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

CONTROL_ATTRIBUTION_SCHEMA_VERSION = "control_attribution.v1"
INTERPRETATION_CAVEAT = (
    "source_control_semantics does not prove Apollo algorithm limitation; "
    "check route/reference-line/matched-target semantics."
)

DEFAULT_THRESHOLDS = {
    "max_raw_mapped_steer_error_p95": 0.08,
    "max_mapped_applied_steer_error_p95": 0.08,
    "max_longitudinal_error_p95": 0.08,
    "source_steer_abs_p95_high": 0.90,
    "source_steer_saturation_ratio_high": 0.50,
    "applied_steer_active_threshold": 0.05,
    "yaw_rate_response_min_abs_p95": 0.005,
    "brake_throttle_conflict_threshold": 0.05,
}

CONTROL_FIELD_ALIASES = {
    "apollo_steer_raw": [
        "apollo_steer_raw",
        "apollo_raw.steer",
        "parsed_control.steer",
        "apollo_desired_steer",
        "steering_target",
        "raw_control_msg_dump.steering_target",
        "source_steer",
        "control_steer_raw",
        "steering_normalized_for_mapping",
        "parsed_control.steering_normalized_for_mapping",
        "output_to_carla.steering_normalized_for_mapping",
    ],
    "bridge_steer_mapped": [
        "bridge_steer_mapped",
        "bridge_mapped.steer",
        "bridge_mapped.mapped_carla_steer_cmd",
        "mapped_carla_steer_cmd",
        "output_to_carla.mapped_carla_steer_cmd",
        "mapped_steer",
        "control_steer_mapped",
        "commanded_steer",
        "cmd_steer",
        "clamped_steer",
    ],
    "carla_steer_applied": [
        "carla_steer_applied",
        "carla_applied.steer",
        "measured_steer",
        "applied_steer",
        "vehicle_steer_applied",
    ],
    "ego_yaw_rate": [
        "ego_yaw_rate",
        "ego_yaw_rate_rad_s",
        "localization_yaw_rate_rad_s",
        "yaw_rate",
        "vehicle_yaw_rate",
    ],
    "throttle_raw": ["throttle_raw", "apollo_raw.throttle", "parsed_control.throttle", "apollo_desired_throttle"],
    "throttle_mapped": [
        "throttle_mapped",
        "bridge_mapped.throttle",
        "bridge_mapped.mapped_throttle_cmd",
        "mapped_throttle_cmd",
        "output_to_carla.mapped_throttle_cmd",
        "commanded_throttle",
        "cmd_throttle",
        "clamped_throttle",
    ],
    "throttle_applied": ["throttle_applied", "carla_applied.throttle", "measured_throttle", "applied_throttle"],
    "brake_raw": ["brake_raw", "apollo_raw.brake", "parsed_control.brake", "apollo_desired_brake"],
    "brake_mapped": [
        "brake_mapped",
        "bridge_mapped.brake",
        "bridge_mapped.mapped_brake_cmd",
        "mapped_brake_cmd",
        "output_to_carla.mapped_brake_cmd",
        "commanded_brake",
        "cmd_brake",
        "clamped_brake",
    ],
    "brake_applied": ["brake_applied", "carla_applied.brake", "measured_brake", "applied_brake"],
    "control_latency_ms": ["control_latency_ms"],
    "steer_scale": ["steer_scale"],
    "steering_sign": ["steering_sign", "steer_sign"],
}


def analyze_control_attribution_run_dir(
    run_dir: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Analyze a run directory, preferring rich debug control traces when present."""
    root = Path(run_dir).expanduser()
    control_input = _find_first(
        root,
        [
            "artifacts/debug_timeseries.csv",
            "timeseries.csv",
            "timeseries.jsonl",
            "artifacts/control_decode_debug.jsonl",
        ],
    ) or root / "timeseries.csv"
    return analyze_control_attribution(
        control_input,
        summary_json=_find_first(root, ["summary.json"]),
        manifest_json=_find_first(root, ["manifest.json"]),
        config_yaml=_find_first(root, ["config.resolved.yaml", "effective_config.yaml"]),
        control_handoff_json=_find_first(
            root,
            [
                "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
                "artifacts/control_handoff_summary.json",
            ],
        ),
        bridge_health_json=_find_first(root, ["artifacts/bridge_health_summary.json"]),
        cyber_bridge_stats_json=_find_first(root, ["artifacts/cyber_bridge_stats.json"]),
        thresholds=thresholds,
    )


def analyze_control_attribution(
    control_input: str | Path,
    *,
    summary_json: str | Path | None = None,
    manifest_json: str | Path | None = None,
    config_yaml: str | Path | None = None,
    control_handoff_json: str | Path | None = None,
    bridge_health_json: str | Path | None = None,
    cyber_bridge_stats_json: str | Path | None = None,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Attribute control-chain breakpoints from offline control trace rows."""
    input_path = Path(control_input).expanduser()
    rows = _read_rows(input_path)
    summary = _read_json(Path(summary_json).expanduser()) if summary_json else {}
    manifest = _read_json(Path(manifest_json).expanduser()) if manifest_json else {}
    config = _read_yaml(Path(config_yaml).expanduser()) if config_yaml else {}
    control_handoff = _read_json(Path(control_handoff_json).expanduser()) if control_handoff_json else {}
    bridge_health = _read_json(Path(bridge_health_json).expanduser()) if bridge_health_json else {}
    cyber_bridge_stats = _read_json(Path(cyber_bridge_stats_json).expanduser()) if cyber_bridge_stats_json else {}
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})

    missing_fields: list[str] = []
    warnings: list[str] = []
    raw_available = _has_field(rows, "apollo_steer_raw")
    mapped_available = _has_field(rows, "bridge_steer_mapped")
    applied_available = _has_field(rows, "carla_steer_applied")
    yaw_available = _has_field(rows, "ego_yaw_rate")
    throttle_available = all(_has_field(rows, field) for field in ("throttle_raw", "throttle_mapped", "throttle_applied"))
    brake_available = all(_has_field(rows, field) for field in ("brake_raw", "brake_mapped", "brake_applied"))

    if not raw_available:
        missing_fields.append("apollo_steer_raw")
    if not mapped_available:
        missing_fields.append("bridge_steer_mapped")
    if not applied_available:
        missing_fields.append("carla_steer_applied")
    if not yaw_available:
        missing_fields.append("ego_yaw_rate")

    steer_scale = _first_number_from_sources("steer_scale", rows, summary, manifest, config)
    steering_sign = _first_number_from_sources("steering_sign", rows, summary, manifest, config)
    if steering_sign is None:
        steering_sign = 1.0
        warnings.append("steering_sign_missing_assumed_positive")
    if steer_scale is None and raw_available and mapped_available:
        missing_fields.append("steer_scale")

    raw_to_mapped = _raw_to_mapped_steer_consistency(
        rows,
        steer_scale=steer_scale,
        steering_sign=steering_sign,
        thresholds=active_thresholds,
    )
    mapped_to_applied = _pair_consistency(
        rows,
        "bridge_steer_mapped",
        "carla_steer_applied",
        max_error_p95=active_thresholds["max_mapped_applied_steer_error_p95"],
    )
    throttle_consistency = _triple_consistency(
        rows,
        "throttle_raw",
        "throttle_mapped",
        "throttle_applied",
        max_error_p95=active_thresholds["max_longitudinal_error_p95"],
    )
    brake_consistency = _triple_consistency(
        rows,
        "brake_raw",
        "brake_mapped",
        "brake_applied",
        max_error_p95=active_thresholds["max_longitudinal_error_p95"],
    )
    vehicle_response = _applied_steer_to_yaw_response(rows, thresholds=active_thresholds)
    source_semantics = _source_control_semantics(rows, thresholds=active_thresholds)
    latency_p95 = _percentile(_series(rows, "control_latency_ms"), 0.95)
    if latency_p95 is None:
        warnings.append("control_latency_missing")

    control_source = _first_text_from_sources(
        "applied_control_source",
        rows,
        summary,
        manifest,
    ) or _first_text_from_sources(
        "control_source",
        rows,
        summary,
        manifest,
    ) or _first_text_from_sources(
        "source_control_channel",
        rows,
        summary,
        manifest,
    ) or _first_text_from_sources(
        "control_channel_name",
        rows,
        summary,
        manifest,
    )
    control_source_evidence = _derive_apollo_control_source_evidence(
        control_handoff=control_handoff,
        bridge_health=bridge_health,
        cyber_bridge_stats=cyber_bridge_stats,
    )
    if _can_promote_generic_external_source(control_source) and control_source_evidence.get("status") == "pass":
        control_source = str(control_source_evidence["control_source"])
    applied_control_source = _normalize_applied_control_source(control_source)
    control_chain_status = _control_chain_status(
        raw_available=raw_available,
        mapped_available=mapped_available,
        applied_available=applied_available,
        applied_control_source=applied_control_source,
    )

    dominant = _dominant_breakpoint(
        raw_available=raw_available,
        mapped_available=mapped_available,
        applied_available=applied_available,
        raw_to_mapped=raw_to_mapped,
        mapped_to_applied=mapped_to_applied,
        throttle_consistency=throttle_consistency,
        brake_consistency=brake_consistency,
        thresholds=active_thresholds,
        vehicle_response=vehicle_response,
        source_semantics=source_semantics,
    )
    attribution = {
        "raw_to_mapped_steer_consistency": raw_to_mapped,
        "mapped_to_applied_steer_consistency": mapped_to_applied,
        "applied_steer_to_yaw_rate_response": vehicle_response,
        "throttle_raw_mapped_applied_consistency": throttle_consistency,
        "brake_raw_mapped_applied_consistency": brake_consistency,
        "brake_throttle_conflict_frames": _brake_throttle_conflicts(rows, active_thresholds),
        "control_latency_p95_ms": latency_p95,
        "source_control_semantics": source_semantics,
        "dominant_breakpoint": dominant,
        "control_chain_status": control_chain_status,
    }
    verdict_status = "pass"
    failure_reason = None
    if dominant == "insufficient_data":
        verdict_status = "insufficient_data"
        failure_reason = "missing_control_chain_fields"
    elif dominant in {"source_control_semantics", "bridge_mapping", "carla_apply", "vehicle_response"}:
        verdict_status = "fail"
        failure_reason = dominant
    elif control_chain_status == "applied_not_apollo":
        verdict_status = "fail"
        failure_reason = control_chain_status
    elif warnings:
        verdict_status = "warn"
        failure_reason = "control_attribution_warn"

    return {
        "schema_version": CONTROL_ATTRIBUTION_SCHEMA_VERSION,
        "run_id": _first_text_from_sources("run_id", rows, summary, manifest, default=input_path.parent.name),
        "route_id": _first_text_from_sources("route_id", rows, summary, manifest),
        "backend": _first_text_from_sources("backend", rows, summary, manifest)
        or _first_text_from_sources("backend_name", rows, summary, manifest),
        "actuator_mapping_mode": _first_text_from_sources("actuator_mapping_mode", rows, summary, manifest, config),
        "steer_scale": steer_scale,
        "steering_sign": steering_sign,
        "calibration_profile_id": _first_text_from_sources("calibration_profile_id", rows, summary, manifest),
        "control_source": control_source,
        "applied_control_source": applied_control_source,
        "control_source_evidence": control_source_evidence,
        "control_chain_status": control_chain_status,
        "resolved_fields": _resolved_fields(rows),
        "raw_control_available": raw_available,
        "mapped_control_available": mapped_available,
        "applied_control_available": applied_available,
        "vehicle_response_available": yaw_available,
        "attribution": attribution,
        "missing_fields": sorted(set(missing_fields)),
        "warnings": sorted(set(warnings)),
        "verdict": {
            "status": verdict_status,
            "failure_reason": failure_reason,
            "dominant_breakpoint": dominant,
        },
        "interpretation_caveat": INTERPRETATION_CAVEAT,
        "source": {
            "control_input_path": str(input_path),
            "summary_path": str(summary_json) if summary_json else None,
            "manifest_path": str(manifest_json) if manifest_json else None,
            "config_path": str(config_yaml) if config_yaml else None,
            "control_handoff_path": str(control_handoff_json) if control_handoff_json else None,
            "bridge_health_path": str(bridge_health_json) if bridge_health_json else None,
            "cyber_bridge_stats_path": str(cyber_bridge_stats_json) if cyber_bridge_stats_json else None,
        },
    }


def write_control_attribution_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "control_attribution_report.json"
    md_path = output_dir / "control_attribution_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "control_attribution_report": str(json_path),
        "control_attribution_summary": str(md_path),
    }


def _control_chain_status(
    *,
    raw_available: bool,
    mapped_available: bool,
    applied_available: bool,
    applied_control_source: str | None,
) -> str:
    if not raw_available:
        return "control_missing"
    if not mapped_available:
        return "raw_present_not_mapped"
    if not applied_available:
        return "mapped_not_applied"
    if applied_control_source and applied_control_source != "apollo_control":
        return "applied_not_apollo"
    return "apollo_control_attributed"


def _derive_apollo_control_source_evidence(
    *,
    control_handoff: Mapping[str, Any],
    bridge_health: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
) -> dict[str, Any]:
    flat_handoff = _flatten_mapping(control_handoff) if control_handoff else {}
    flat_bridge = _flatten_mapping(bridge_health) if bridge_health else {}
    flat_stats = _flatten_mapping(cyber_bridge_stats) if cyber_bridge_stats else {}
    control_channel = (
        _text_from_flat(flat_handoff, "control_channel.name")
        or _text_from_flat(flat_bridge, "ingress_egress.cyber.control_channel")
        or _text_from_flat(flat_bridge, "bridge_transport.cyber.control_channel")
    )
    handoff_status = _text_from_flat(flat_handoff, "status") or _text_from_flat(flat_handoff, "verdict")
    channel_status = _text_from_flat(flat_handoff, "control_channel.status")
    apply_count = _num(
        _value_from_flat(flat_handoff, "mapping_and_apply.apply_control_count")
        or _value_from_flat(flat_bridge, "command_materialization.observed_counters.control_tx_count")
        or _value_from_flat(flat_stats, "control_tx_count")
    )
    rx_count = _num(
        _value_from_flat(flat_handoff, "bridge_receive.control_rx_count")
        or _value_from_flat(flat_stats, "control_rx_count")
    )
    tx_count = _num(
        _value_from_flat(flat_stats, "control_tx_count")
        or _value_from_flat(flat_bridge, "command_materialization.observed_counters.control_tx_count")
    )
    control_apply_path = (
        _text_from_flat(flat_bridge, "control_apply_path")
        or _text_from_flat(flat_bridge, "bridge_transport.control_apply_path")
        or _text_from_flat(flat_bridge, "ingress_egress.control_apply_path")
    )
    reasons: list[str] = []
    if control_channel != "/apollo/control":
        reasons.append("apollo_control_channel_not_verified")
    if handoff_status not in {"pass", "warn", None}:
        reasons.append("control_handoff_status_blocking")
    if channel_status not in {"pass", "warn", None}:
        reasons.append("control_channel_status_blocking")
    if (apply_count or 0.0) <= 0.0 and (tx_count or 0.0) <= 0.0:
        reasons.append("control_apply_or_tx_count_missing")
    if (rx_count or 0.0) <= 0.0:
        reasons.append("control_rx_count_missing")
    status = "insufficient_data" if reasons else "pass"
    return {
        "status": status,
        "control_source": "/apollo/control" if status == "pass" else None,
        "control_channel": control_channel,
        "control_handoff_status": handoff_status,
        "control_channel_status": channel_status,
        "apply_control_count": apply_count,
        "control_rx_count": rx_count,
        "control_tx_count": tx_count,
        "control_apply_path": control_apply_path,
        "blocking_reasons": reasons,
        "interpretation": (
            "Generic harness metadata such as external_stack is not sufficient. "
            "Apollo control attribution requires /apollo/control channel evidence plus rx/tx/apply counters."
        ),
    }


def _dominant_breakpoint(
    *,
    raw_available: bool,
    mapped_available: bool,
    applied_available: bool,
    raw_to_mapped: Mapping[str, Any],
    mapped_to_applied: Mapping[str, Any],
    throttle_consistency: Mapping[str, Any],
    brake_consistency: Mapping[str, Any],
    thresholds: Mapping[str, float],
    vehicle_response: Mapping[str, Any],
    source_semantics: Mapping[str, Any],
) -> str:
    if not raw_available:
        return "insufficient_data"
    if not mapped_available:
        return "insufficient_data"
    if raw_to_mapped.get("status") == "fail":
        return "bridge_mapping"
    if not applied_available:
        return "insufficient_data"
    if mapped_to_applied.get("status") == "fail":
        return "carla_apply"
    longitudinal_breakpoint = _longitudinal_dominant_breakpoint(
        throttle_consistency,
        brake_consistency,
        thresholds=thresholds,
    )
    if longitudinal_breakpoint is not None:
        return longitudinal_breakpoint
    if vehicle_response.get("status") == "fail":
        return "vehicle_response"
    if source_semantics.get("status") == "fail":
        return "source_control_semantics"
    return "none"


def _longitudinal_dominant_breakpoint(
    throttle_consistency: Mapping[str, Any],
    brake_consistency: Mapping[str, Any],
    *,
    thresholds: Mapping[str, float],
) -> str | None:
    threshold = float(thresholds["max_longitudinal_error_p95"])
    for consistency in (throttle_consistency, brake_consistency):
        if consistency.get("status") != "fail":
            continue
        raw_to_mapped = _num(consistency.get("raw_to_mapped_error_p95"))
        mapped_to_applied = _num(consistency.get("mapped_to_applied_error_p95"))
        if raw_to_mapped is not None and raw_to_mapped > threshold:
            return "bridge_mapping"
        if mapped_to_applied is not None and mapped_to_applied > threshold:
            return "carla_apply"
    return None


def _raw_to_mapped_steer_consistency(
    rows: Sequence[Mapping[str, Any]],
    *,
    steer_scale: float | None,
    steering_sign: float,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    if steer_scale is None:
        return {
            "status": "insufficient_data",
            "expected_mapping": None,
            "error_p95": None,
            "sample_count": 0,
        }
    values: list[float] = []
    for row in rows:
        raw = _value_for_field(row, "apollo_steer_raw")
        mapped = _value_for_field(row, "bridge_steer_mapped")
        if raw is None or mapped is None:
            continue
        expected = _clamp(raw * steer_scale * steering_sign, -1.0, 1.0)
        values.append(abs(expected - mapped))
    error = _percentile(values, 0.95)
    status = _error_status(error, thresholds["max_raw_mapped_steer_error_p95"])
    return {
        "status": status,
        "expected_mapping": "bridge_steer_mapped ~= clamp(apollo_steer_raw * steer_scale * steering_sign, -1, 1)",
        "error_p95": error,
        "sample_count": len(values),
    }


def _pair_consistency(
    rows: Sequence[Mapping[str, Any]],
    left_field: str,
    right_field: str,
    *,
    max_error_p95: float,
) -> dict[str, Any]:
    values: list[float] = []
    for row in rows:
        left = _value_for_field(row, left_field)
        right = _value_for_field(row, right_field)
        if left is not None and right is not None:
            values.append(abs(left - right))
    error = _percentile(values, 0.95)
    return {
        "status": _error_status(error, max_error_p95),
        "error_p95": error,
        "sample_count": len(values),
    }


def _triple_consistency(
    rows: Sequence[Mapping[str, Any]],
    raw_field: str,
    mapped_field: str,
    applied_field: str,
    *,
    max_error_p95: float,
) -> dict[str, Any]:
    raw_mapped = _pair_consistency(rows, raw_field, mapped_field, max_error_p95=max_error_p95)
    mapped_applied = _pair_consistency(rows, mapped_field, applied_field, max_error_p95=max_error_p95)
    statuses = {raw_mapped["status"], mapped_applied["status"]}
    if "fail" in statuses:
        status = "fail"
    elif "insufficient_data" in statuses:
        status = "insufficient_data"
    elif "warn" in statuses:
        status = "warn"
    else:
        status = "pass"
    return {
        "status": status,
        "raw_to_mapped_error_p95": raw_mapped["error_p95"],
        "mapped_to_applied_error_p95": mapped_applied["error_p95"],
        "sample_count": min(int(raw_mapped["sample_count"]), int(mapped_applied["sample_count"])),
    }


def _applied_steer_to_yaw_response(
    rows: Sequence[Mapping[str, Any]],
    *,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    active_yaw: list[float] = []
    active_steer: list[float] = []
    sign_matches = 0
    sign_count = 0
    for row in rows:
        steer = _value_for_field(row, "carla_steer_applied")
        yaw = _value_for_field(row, "ego_yaw_rate")
        if steer is None or yaw is None:
            continue
        if abs(steer) <= thresholds["applied_steer_active_threshold"]:
            continue
        active_steer.append(abs(steer))
        active_yaw.append(abs(yaw))
        if abs(yaw) > thresholds["yaw_rate_response_min_abs_p95"]:
            sign_count += 1
            if _sign(steer) == _sign(yaw):
                sign_matches += 1
    steer_p95 = _percentile(active_steer, 0.95)
    yaw_p95 = _percentile(active_yaw, 0.95)
    if steer_p95 is None:
        status = "insufficient_data"
        reason = "no_active_applied_steer"
    elif yaw_p95 is None:
        status = "insufficient_data"
        reason = "yaw_rate_missing"
    elif yaw_p95 < thresholds["yaw_rate_response_min_abs_p95"]:
        status = "fail"
        reason = "applied_steer_has_no_yaw_rate_response"
    else:
        status = "pass"
        reason = None
    return {
        "status": status,
        "reason": reason,
        "applied_steer_abs_p95": steer_p95,
        "yaw_rate_abs_p95": yaw_p95,
        "sample_count": len(active_yaw),
        "steer_yaw_sign_match_ratio": None if sign_count == 0 else sign_matches / sign_count,
    }


def _source_control_semantics(rows: Sequence[Mapping[str, Any]], *, thresholds: Mapping[str, float]) -> dict[str, Any]:
    raw = _series(rows, "apollo_steer_raw")
    raw_abs = [abs(value) for value in raw]
    raw_abs_p95 = _percentile(raw_abs, 0.95)
    high_count = sum(1 for value in raw_abs if value >= thresholds["source_steer_abs_p95_high"])
    saturation_ratio = None if not raw_abs else high_count / len(raw_abs)
    status = "pass"
    reason = None
    if raw_abs_p95 is None:
        status = "insufficient_data"
        reason = "apollo_steer_raw_missing"
    elif (
        raw_abs_p95 >= thresholds["source_steer_abs_p95_high"]
        and (saturation_ratio or 0.0) >= thresholds["source_steer_saturation_ratio_high"]
    ):
        status = "fail"
        reason = "source_steer_high_or_saturated"
    return {
        "status": status,
        "reason": reason,
        "apollo_steer_raw_abs_p95": raw_abs_p95,
        "apollo_steer_raw_saturation_ratio": saturation_ratio,
        "sample_count": len(raw),
    }


def _brake_throttle_conflicts(rows: Sequence[Mapping[str, Any]], thresholds: Mapping[str, float]) -> int | None:
    seen = False
    conflicts = 0
    threshold = thresholds["brake_throttle_conflict_threshold"]
    for row in rows:
        throttle = _value_for_field(row, "throttle_applied")
        brake = _value_for_field(row, "brake_applied")
        if throttle is None or brake is None:
            continue
        seen = True
        if throttle > threshold and brake > threshold:
            conflicts += 1
    return conflicts if seen else None


def _error_status(error: float | None, max_error_p95: float) -> str:
    if error is None:
        return "insufficient_data"
    if error > max_error_p95:
        return "fail"
    return "pass"


def _read_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(_flatten_row(payload))
        return rows
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _flatten_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    flattened = dict(payload)
    flattened.update(_flatten_mapping(payload))
    return flattened


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        import yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _has_field(rows: Sequence[Mapping[str, Any]], field: str) -> bool:
    return any(_value_for_field(row, field) is not None for row in rows)


def _series(rows: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    return [value for row in rows if (value := _value_for_field(row, field)) is not None]


def _percentile(values: Sequence[float], q: float) -> float | None:
    cleaned = sorted(value for value in values if math.isfinite(float(value)))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (len(cleaned) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return cleaned[lower]
    weight = rank - lower
    return cleaned[lower] * (1.0 - weight) + cleaned[upper] * weight


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _first_number_from_sources(
    field: str,
    rows: Sequence[Mapping[str, Any]],
    *sources: Mapping[str, Any],
) -> float | None:
    for source in sources:
        value = _num(source.get(field))
        if value is not None:
            return value
        metadata = source.get("metadata")
        if isinstance(metadata, Mapping):
            value = _num(metadata.get(field))
            if value is not None:
                return value
        flat = _flatten_mapping(source)
        for alias in CONTROL_FIELD_ALIASES.get(field, [field]):
            for key, nested_value in flat.items():
                if key.endswith(alias):
                    value = _num(nested_value)
                    if value is not None:
                        return value
    for row in rows:
        value = _value_for_field(row, field)
        if value is not None:
            return value
    return None


def _first_text_from_sources(
    field: str,
    rows: Sequence[Mapping[str, Any]],
    *sources: Mapping[str, Any],
    default: str | None = None,
) -> str | None:
    for source in sources:
        value = source.get(field)
        if value not in {None, ""}:
            return str(value)
        metadata = source.get("metadata")
        if isinstance(metadata, Mapping):
            value = metadata.get(field)
            if value not in {None, ""}:
                return str(value)
        flat = _flatten_mapping(source)
        for key, nested_value in flat.items():
            if key.endswith(field) and nested_value not in {None, ""}:
                return str(nested_value)
    for row in rows:
        value = row.get(field)
        if value not in {None, ""}:
            return str(value)
    return default


def _value_for_field(row: Mapping[str, Any], field: str) -> float | None:
    for alias in CONTROL_FIELD_ALIASES.get(field, [field]):
        value = _num(row.get(alias))
        if value is not None:
            return value
    return None


def _resolved_fields(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for field, aliases in CONTROL_FIELD_ALIASES.items():
        for alias in aliases:
            if any(_num(row.get(alias)) is not None for row in rows):
                resolved[field] = alias
                break
    return resolved


def _flatten_mapping(payload: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in payload.items():
        current = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            flat.update(_flatten_mapping(value, current))
        else:
            flat[current] = value
    return flat


def _value_from_flat(payload: Mapping[str, Any], suffix: str) -> Any:
    for key, value in payload.items():
        if key == suffix or key.endswith(f".{suffix}"):
            return value
    return None


def _text_from_flat(payload: Mapping[str, Any], suffix: str) -> str | None:
    value = _value_from_flat(payload, suffix)
    if value in {None, ""}:
        return None
    return str(value)


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    return None


def _can_promote_generic_external_source(control_source: str | None) -> bool:
    if control_source in {None, ""}:
        return True
    normalized = str(control_source).strip().lower()
    return normalized in {"external_stack", "external"}


def _normalize_applied_control_source(control_source: str | None) -> str | None:
    if control_source is None:
        return None
    normalized = str(control_source).strip().lower()
    if normalized in {"/apollo/control", "apollo", "apollo_control", "apollo_cyberrt"}:
        return "apollo_control"
    if normalized in {"external_stack", "external", "autoware", "autoware_ros2"}:
        return "external_stack"
    if normalized in {"carla_builtin", "carla_testbed_builtin_controller", "builtin"}:
        return "carla_builtin"
    if normalized in {"manual", "human", "operator"}:
        return "manual"
    if normalized in {"route_follower", "legacy_followstop", "dummy_lateral", "direct_autopilot"}:
        return normalized
    return normalized or None


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _sign(value: float) -> int:
    return 1 if value >= 0.0 else -1


def _markdown(report: Mapping[str, Any]) -> str:
    attribution = report.get("attribution") if isinstance(report.get("attribution"), Mapping) else {}
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    source_evidence = (
        report.get("control_source_evidence") if isinstance(report.get("control_source_evidence"), Mapping) else {}
    )
    lines = [
        "# Control Attribution Report",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- run_id: `{report.get('run_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- backend: `{report.get('backend')}`",
        f"- actuator_mapping_mode: `{report.get('actuator_mapping_mode')}`",
        f"- control_source: `{report.get('control_source')}`",
        f"- applied_control_source: `{report.get('applied_control_source')}`",
        f"- steer_scale: `{report.get('steer_scale')}`",
        f"- steering_sign: `{report.get('steering_sign')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- dominant_breakpoint: `{attribution.get('dominant_breakpoint')}`",
        f"- control_source_evidence_status: `{source_evidence.get('status')}`",
        f"- control_source_evidence_channel: `{source_evidence.get('control_channel')}`",
        f"- control_source_evidence_apply_count: `{source_evidence.get('apply_control_count')}`",
        "",
        "## Chain Availability",
        "",
        f"- raw_control_available: `{report.get('raw_control_available')}`",
        f"- mapped_control_available: `{report.get('mapped_control_available')}`",
        f"- applied_control_available: `{report.get('applied_control_available')}`",
        f"- vehicle_response_available: `{report.get('vehicle_response_available')}`",
        "",
        "## Attribution",
        "",
    ]
    for key in (
        "raw_to_mapped_steer_consistency",
        "mapped_to_applied_steer_consistency",
        "applied_steer_to_yaw_rate_response",
        "throttle_raw_mapped_applied_consistency",
        "brake_raw_mapped_applied_consistency",
    ):
        value = attribution.get(key) if isinstance(attribution.get(key), Mapping) else {}
        lines.append(f"- {key}: status=`{value.get('status')}` details=`{json.dumps(value, sort_keys=True)}`")
    lines.extend(
        [
            f"- brake_throttle_conflict_frames: `{attribution.get('brake_throttle_conflict_frames')}`",
            f"- control_latency_p95_ms: `{attribution.get('control_latency_p95_ms')}`",
            "",
            "## Missing / Warnings",
            "",
            f"- missing_fields: `{', '.join(report.get('missing_fields') or []) or 'none'}`",
            f"- warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            "",
            "## Interpretation Caveat",
            "",
            str(report.get("interpretation_caveat") or INTERPRETATION_CAVEAT),
            "",
        ]
    )
    return "\n".join(lines)
