from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

APOLLO_LATERAL_SEMANTICS_SCHEMA_VERSION = "apollo_lateral_semantics.v1"

FIELD_ALIASES = {
    "apollo_steer_raw": ["apollo_steer_raw", "steering_target", "source_steer", "control_steer_raw"],
    "bridge_steer_mapped": ["bridge_steer_mapped", "mapped_steer", "control_steer_mapped"],
    "carla_steer_applied": ["carla_steer_applied", "applied_steer", "vehicle_steer_applied"],
    "apollo_target_point_kappa": ["apollo_target_point_kappa", "target_point_kappa"],
    "apollo_planning_first_kappa": [
        "apollo_planning_first_kappa",
        "planning_first_kappa",
        "first_point_kappa",
        "first_trajectory_point_kappa",
    ],
    "reference_lane_curvature": [
        "reference_lane_curvature",
        "reference_line_curvature",
        "apollo_reference_curvature",
    ],
    "route_curvature": ["route_curvature", "curvature_at_nearest"],
    "matched_point_distance": ["apollo_matched_point_distance", "matched_point_distance"],
    "target_point_distance": ["apollo_target_point_distance", "target_point_distance"],
    "ego_yaw_rate": ["ego_yaw_rate", "yaw_rate", "vehicle_yaw_rate"],
    "cross_track_error": ["cross_track_error", "lateral_error"],
    "heading_error": ["heading_error"],
    "steer_scale": ["steer_scale"],
    "steering_sign": ["steering_sign"],
}

REQUIRED_SEMANTIC_FIELDS = (
    "route_curvature",
    "reference_lane_curvature",
    "apollo_planning_first_kappa",
    "apollo_target_point_kappa",
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "ego_yaw_rate",
    "cross_track_error",
    "heading_error",
    "matched_point_distance",
    "target_point_distance",
)

DEFAULT_THRESHOLDS = {
    "straight_curvature_abs_max": 0.005,
    "high_kappa_abs": 0.05,
    "target_kappa_spike_abs": 0.05,
    "high_source_steer_abs": 0.85,
    "small_lateral_error_abs_p95_m": 0.20,
    "small_heading_error_abs_p95_rad": 0.10,
    "matched_point_distance_max_m": 5.0,
    "target_point_jump_m": 3.0,
    "mapped_applied_steer_error_p95": 0.10,
    "raw_mapped_expected_error_p95": 0.10,
    "applied_steer_active_abs": 0.05,
    "yaw_response_min_abs_p95": 0.005,
}


def analyze_apollo_lateral_semantics_run_dir(
    run_dir: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    return analyze_apollo_lateral_semantics(
        timeseries=_find_first(root, ["timeseries.csv", "timeseries.jsonl"]),
        route_health=_find_first(root, ["analysis/route_health/route_health.json", "route_health.json"]),
        planning_debug=_find_first(
            root,
            [
                "artifacts/planning_topic_debug.jsonl",
                "planning_topic_debug.jsonl",
                "planning_debug.csv",
                "planning_debug.json",
            ],
        ),
        source_steer_summary=_find_first(
            root,
            ["artifacts/source_steer_summary.json", "source_steer_summary.json"],
        ),
        kappa_audit_summary=_find_first(
            root,
            ["artifacts/kappa_audit_summary.json", "kappa_audit_summary.json"],
        ),
        run_dir=root,
        thresholds=thresholds,
    )


def analyze_apollo_lateral_semantics(
    *,
    timeseries: str | Path | None = None,
    route_health: str | Path | None = None,
    planning_debug: str | Path | None = None,
    source_steer_summary: str | Path | None = None,
    kappa_audit_summary: str | Path | None = None,
    run_dir: str | Path | None = None,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})
    timeseries_rows = _read_rows(timeseries)
    planning_rows = _read_rows(planning_debug)
    route_health_payload = _read_json(route_health)
    source_summary = _read_json(source_steer_summary)
    kappa_summary = _read_json(kappa_audit_summary)
    rows = [*timeseries_rows, *planning_rows]
    supplemental = _supplemental_values(route_health_payload, source_summary, kappa_summary)

    resolved_fields = {
        field: _resolved_field(rows, aliases)
        for field, aliases in FIELD_ALIASES.items()
    }
    values = {
        field: _values_for_field(rows, field, supplemental= supplemental)
        for field in FIELD_ALIASES
    }
    route_curvature = values["route_curvature"] or _route_health_curvature_values(route_health_payload)
    if route_curvature:
        values["route_curvature"] = route_curvature
        if resolved_fields.get("route_curvature") is None and route_health_payload:
            resolved_fields["route_curvature"] = "route_health.route_geometry.curvature"
    missing_fields = [
        field
        for field in REQUIRED_SEMANTIC_FIELDS
        if not values.get(field)
    ]

    stats = {
        "route_curvature_abs": _stats_abs(values["route_curvature"]),
        "reference_lane_curvature_abs": _stats_abs(values["reference_lane_curvature"]),
        "apollo_planning_first_kappa_abs": _stats_abs(values["apollo_planning_first_kappa"]),
        "apollo_target_point_kappa_abs": _stats_abs(values["apollo_target_point_kappa"]),
        "apollo_steer_raw_abs": _stats_abs(values["apollo_steer_raw"]),
        "bridge_steer_mapped_abs": _stats_abs(values["bridge_steer_mapped"]),
        "carla_steer_applied_abs": _stats_abs(values["carla_steer_applied"]),
        "ego_yaw_rate_abs": _stats_abs(values["ego_yaw_rate"]),
        "cross_track_error_abs": _stats_abs(values["cross_track_error"]),
        "heading_error_abs": _stats_abs(values["heading_error"]),
        "matched_point_distance_abs": _stats_abs(values["matched_point_distance"]),
        "target_point_distance_abs": _stats_abs(values["target_point_distance"]),
        "route_vs_planning_kappa_correlation": _pearson_pair(
            values["route_curvature"],
            values["apollo_planning_first_kappa"],
        ),
        "route_vs_target_kappa_correlation": _pearson_pair(
            values["route_curvature"],
            values["apollo_target_point_kappa"],
        ),
        "target_kappa_vs_source_steer_correlation": _pearson_pair(
            values["apollo_target_point_kappa"],
            values["apollo_steer_raw"],
        ),
    }
    anomalies = _anomalies(values, stats, active_thresholds)
    if not rows and not route_health_payload:
        anomalies.append(_anomaly("insufficient_data", "missing_timeseries_and_route_health", "insufficient_data"))
    elif missing_fields and not anomalies:
        anomalies.append(
            _anomaly(
                "insufficient_data",
                "missing_semantic_fields",
                "insufficient_data",
                fields=missing_fields,
            )
        )
    suspected_layer = _suspected_layer(anomalies)
    confidence = _confidence(anomalies, missing_fields)
    status = "pass"
    failure_reason = None
    if suspected_layer == "insufficient_data":
        status = "insufficient_data"
        failure_reason = "missing_lateral_semantics_fields"
    elif anomalies:
        status = "warn"
        failure_reason = "lateral_semantics_anomaly"

    return {
        "schema_version": APOLLO_LATERAL_SEMANTICS_SCHEMA_VERSION,
        "run_id": _first_text(rows, "run_id", default=Path(run_dir).name if run_dir else None),
        "route_id": _first_text(rows, "route_id") or _route_health_route_id(route_health_payload),
        "backend": _first_text(rows, "backend") or _first_text(rows, "backend_name"),
        "route_curvature_available": bool(values["route_curvature"]),
        "reference_line_curvature_available": bool(values["reference_lane_curvature"]),
        "planning_kappa_available": bool(values["apollo_planning_first_kappa"]),
        "target_point_available": bool(values["apollo_target_point_kappa"] or values["target_point_distance"]),
        "source_steer_available": bool(values["apollo_steer_raw"]),
        "matched_point_available": bool(values["matched_point_distance"]),
        "anomalies": anomalies,
        "correlation_summary": stats,
        "suspected_layer": suspected_layer,
        "confidence": confidence,
        "missing_fields": sorted(set(missing_fields)),
        "resolved_fields": {key: value for key, value in resolved_fields.items() if value},
        "verdict": {
            "status": status,
            "failure_reason": failure_reason,
            "suspected_layer": suspected_layer,
            "confidence": confidence,
        },
        "thresholds": active_thresholds,
        "source": {
            "run_dir": None if run_dir is None else str(Path(run_dir)),
            "timeseries": None if timeseries is None else str(Path(timeseries)),
            "route_health": None if route_health is None else str(Path(route_health)),
            "planning_debug": None if planning_debug is None else str(Path(planning_debug)),
            "source_steer_summary": None if source_steer_summary is None else str(Path(source_steer_summary)),
            "kappa_audit_summary": None if kappa_audit_summary is None else str(Path(kappa_audit_summary)),
        },
        "interpretation_boundary": (
            "This report identifies a suspected lateral-semantics layer with confidence; "
            "it is not a definitive root-cause proof, does not change steer_scale, "
            "and does not prove the bridge irrelevant when lateral guards are inactive."
        ),
    }


def write_apollo_lateral_semantics_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "apollo_lateral_semantics_report.json"
    summary_path = output_dir / "apollo_lateral_semantics_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "apollo_lateral_semantics_report": str(report_path),
        "apollo_lateral_semantics_summary": str(summary_path),
    }


def _anomalies(
    values: Mapping[str, list[float]],
    stats: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    route_max = _stat(stats, "route_curvature_abs", "max")
    ref_max = _stat(stats, "reference_lane_curvature_abs", "max")
    planning_max = _stat(stats, "apollo_planning_first_kappa_abs", "max")
    target_max = _stat(stats, "apollo_target_point_kappa_abs", "max")
    source_p95 = _stat(stats, "apollo_steer_raw_abs", "p95")
    cte_p95 = _stat(stats, "cross_track_error_abs", "p95")
    heading_p95 = _stat(stats, "heading_error_abs", "p95")
    matched_max = _stat(stats, "matched_point_distance_abs", "max")
    straight_route = route_max is not None and route_max <= thresholds["straight_curvature_abs_max"]
    straight_ref = ref_max is not None and ref_max <= thresholds["straight_curvature_abs_max"]
    if (straight_route or straight_ref) and planning_max is not None and planning_max >= thresholds["high_kappa_abs"]:
        anomalies.append(
            _anomaly(
                "route_straight_but_planning_kappa_high",
                "planning kappa is high while route/reference curvature is near zero",
                "reference_line_semantics",
                route_curvature_max_abs=route_max,
                reference_curvature_max_abs=ref_max,
                planning_first_kappa_max_abs=planning_max,
            )
        )
    if target_max is not None and target_max >= thresholds["target_kappa_spike_abs"]:
        anomalies.append(
            _anomaly(
                "target_kappa_spike",
                "target point kappa exceeds threshold",
                "target_point_semantics",
                target_point_kappa_max_abs=target_max,
            )
        )
    if (
        source_p95 is not None
        and source_p95 >= thresholds["high_source_steer_abs"]
        and cte_p95 is not None
        and cte_p95 <= thresholds["small_lateral_error_abs_p95_m"]
        and heading_p95 is not None
        and heading_p95 <= thresholds["small_heading_error_abs_p95_rad"]
    ):
        anomalies.append(
            _anomaly(
                "source_steer_high_with_small_lateral_error",
                "raw source steer is high before large lateral/heading error appears",
                "target_point_semantics",
                source_steer_abs_p95=source_p95,
                cross_track_error_abs_p95=cte_p95,
                heading_error_abs_p95=heading_p95,
            )
        )
    if matched_max is not None and matched_max > thresholds["matched_point_distance_max_m"]:
        anomalies.append(
            _anomaly(
                "matched_point_too_large",
                "matched point distance exceeds threshold",
                "reference_line_semantics",
                matched_point_distance_max_abs=matched_max,
            )
        )
    target_jump = _max_abs_delta(values["target_point_distance"])
    if target_jump is not None and target_jump > thresholds["target_point_jump_m"]:
        anomalies.append(
            _anomaly(
                "target_point_jump",
                "target point distance jumps between adjacent samples",
                "target_point_semantics",
                target_point_distance_max_jump=target_jump,
            )
        )
    raw_mapping_error = _raw_mapped_expected_error(values, thresholds)
    mapped_applied_error = _pair_error(values["bridge_steer_mapped"], values["carla_steer_applied"])
    if (
        raw_mapping_error is not None
        and raw_mapping_error > thresholds["raw_mapped_expected_error_p95"]
    ) or (
        mapped_applied_error is not None
        and mapped_applied_error > thresholds["mapped_applied_steer_error_p95"]
    ):
        anomalies.append(
            _anomaly(
                "raw_mapped_applied_mismatch",
                "raw/mapped/applied steer fields are inconsistent",
                "control_mapping",
                raw_to_mapped_expected_error_p95=raw_mapping_error,
                mapped_to_applied_error_p95=mapped_applied_error,
            )
        )
    applied_p95 = _stat(stats, "carla_steer_applied_abs", "p95")
    yaw_p95 = _stat(stats, "ego_yaw_rate_abs", "p95")
    if (
        applied_p95 is not None
        and applied_p95 >= thresholds["applied_steer_active_abs"]
        and yaw_p95 is not None
        and yaw_p95 < thresholds["yaw_response_min_abs_p95"]
    ):
        anomalies.append(
            _anomaly(
                "applied_steer_no_yaw_response",
                "CARLA-applied steer is active but yaw-rate response is near zero",
                "vehicle_response",
                applied_steer_abs_p95=applied_p95,
                ego_yaw_rate_abs_p95=yaw_p95,
            )
        )
    return anomalies


def _suspected_layer(anomalies: Sequence[Mapping[str, Any]]) -> str:
    if not anomalies:
        return "insufficient_data"
    layers = [str(item.get("suspected_layer")) for item in anomalies if item.get("type") != "insufficient_data"]
    if not layers:
        return "insufficient_data"
    for layer in (
        "control_mapping",
        "vehicle_response",
        "target_point_semantics",
        "reference_line_semantics",
        "bridge_assist",
    ):
        if layer in layers:
            return layer
    return layers[0]


def _confidence(anomalies: Sequence[Mapping[str, Any]], missing_fields: Sequence[str]) -> str:
    semantic_anomalies = [item for item in anomalies if item.get("type") != "insufficient_data"]
    if not semantic_anomalies:
        return "low"
    if len(missing_fields) <= 2 and len(semantic_anomalies) >= 2:
        return "high"
    if len(missing_fields) <= 5:
        return "medium"
    return "low"


def _anomaly(kind: str, reason: str, layer: str, **evidence: Any) -> dict[str, Any]:
    return {
        "type": kind,
        "reason": reason,
        "suspected_layer": layer,
        "evidence": evidence,
    }


def _values_for_field(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    supplemental: Mapping[str, list[float]],
) -> list[float]:
    values: list[float] = []
    for alias in FIELD_ALIASES.get(field, [field]):
        alias_values = [_num(row.get(alias)) for row in rows]
        values.extend(value for value in alias_values if value is not None)
        if values:
            break
    if not values:
        values.extend(supplemental.get(field, []))
    return values


def _resolved_field(rows: Sequence[Mapping[str, Any]], aliases: Sequence[str]) -> str | None:
    for alias in aliases:
        if any(_num(row.get(alias)) is not None for row in rows):
            return alias
    return None


def _supplemental_values(*payloads: Mapping[str, Any]) -> dict[str, list[float]]:
    result: dict[str, list[float]] = {field: [] for field in FIELD_ALIASES}
    for payload in payloads:
        if not payload:
            continue
        flat = _flatten_mapping(payload)
        for field, aliases in FIELD_ALIASES.items():
            for alias in aliases:
                for key, value in flat.items():
                    if key.endswith(alias):
                        number = _num(value)
                        if number is not None:
                            result[field].append(number)
    return result


def _flatten_mapping(payload: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in payload.items():
        current = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            flat.update(_flatten_mapping(value, current))
        else:
            flat[current] = value
    return flat


def _route_health_curvature_values(payload: Mapping[str, Any]) -> list[float]:
    geometry = payload.get("route_geometry") if isinstance(payload.get("route_geometry"), Mapping) else {}
    curvature = geometry.get("curvature") if isinstance(geometry.get("curvature"), Mapping) else {}
    values: list[float] = []
    for key in ("mean_abs", "p95_abs", "max_abs"):
        value = _num(curvature.get(key) or geometry.get(f"curvature.{key}"))
        if value is not None:
            values.append(value)
    return values


def _route_health_route_id(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("route_id") if isinstance(payload, Mapping) else None
    return None if value in {None, ""} else str(value)


def _raw_mapped_expected_error(values: Mapping[str, list[float]], thresholds: Mapping[str, float]) -> float | None:
    raw = values["apollo_steer_raw"]
    mapped = values["bridge_steer_mapped"]
    scale_values = values.get("steer_scale") or []
    sign_values = values.get("steering_sign") or [1.0]
    if not raw or not mapped or not scale_values:
        return None
    scale = scale_values[0]
    sign = sign_values[0] if sign_values else 1.0
    count = min(len(raw), len(mapped))
    errors = [abs(_clamp(raw[index] * scale * sign, -1.0, 1.0) - mapped[index]) for index in range(count)]
    return _percentile(errors, 0.95)


def _pair_error(left: Sequence[float], right: Sequence[float]) -> float | None:
    count = min(len(left), len(right))
    if count <= 0:
        return None
    return _percentile([abs(left[index] - right[index]) for index in range(count)], 0.95)


def _max_abs_delta(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    return max(abs(cur - prev) for prev, cur in zip(values, values[1:]))


def _pearson_pair(left: Sequence[float], right: Sequence[float]) -> float | None:
    count = min(len(left), len(right))
    if count < 2:
        return None
    xs = list(left[:count])
    ys = list(right[:count])
    mean_x = sum(xs) / count
    mean_y = sum(ys) / count
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x <= 0.0 or den_y <= 0.0:
        return None
    return num / (den_x * den_y)


def _stats_abs(values: Sequence[float]) -> dict[str, Any]:
    absolute = [abs(value) for value in values if math.isfinite(float(value))]
    if not absolute:
        return {"count": 0, "mean": None, "p95": None, "max": None}
    return {
        "count": len(absolute),
        "mean": sum(absolute) / len(absolute),
        "p95": _percentile(absolute, 0.95),
        "max": max(absolute),
    }


def _stat(stats: Mapping[str, Any], group: str, key: str) -> float | None:
    value = stats.get(group)
    if not isinstance(value, Mapping):
        return None
    return _num(value.get(key))


def _read_rows(path: str | Path | None) -> list[dict[str, Any]]:
    if path in (None, ""):
        return []
    resolved = Path(str(path)).expanduser()
    if not resolved.exists():
        return []
    if resolved.suffix == ".jsonl":
        return _read_jsonl(resolved)
    if resolved.suffix == ".json":
        payload = _read_json(resolved)
        rows = payload.get("rows") or payload.get("data") or payload.get("messages")
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, Mapping)]
        return [payload] if payload else []
    with resolved.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path in (None, ""):
        return {}
    resolved = Path(str(path)).expanduser()
    if not resolved.exists():
        return {}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    return None


def _first_text(rows: Sequence[Mapping[str, Any]], field: str, *, default: str | None = None) -> str | None:
    for row in rows:
        value = row.get(field)
        if value not in {None, ""}:
            return str(value)
    return default


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


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _markdown(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    lines = [
        "# Apollo Lateral Semantics Report",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- run_id: `{report.get('run_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- backend: `{report.get('backend')}`",
        f"- status: `{verdict.get('status')}`",
        f"- suspected_layer: `{report.get('suspected_layer')}`",
        f"- confidence: `{report.get('confidence')}`",
        "",
        "## Availability",
        "",
        f"- route_curvature_available: `{report.get('route_curvature_available')}`",
        f"- reference_line_curvature_available: `{report.get('reference_line_curvature_available')}`",
        f"- planning_kappa_available: `{report.get('planning_kappa_available')}`",
        f"- target_point_available: `{report.get('target_point_available')}`",
        f"- source_steer_available: `{report.get('source_steer_available')}`",
        f"- matched_point_available: `{report.get('matched_point_available')}`",
        "",
        "## Anomalies",
        "",
    ]
    anomalies = report.get("anomalies") if isinstance(report.get("anomalies"), list) else []
    if anomalies:
        for item in anomalies:
            if isinstance(item, Mapping):
                lines.append(f"- `{item.get('type')}` -> `{item.get('suspected_layer')}`: {item.get('reason')}")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Missing Fields",
            "",
            f"`{', '.join(report.get('missing_fields') or []) or 'none'}`",
            "",
            "## Interpretation Boundary",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )
    return "\n".join(lines)
