from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.gap import ActorKinematics2D, bumper_to_bumper_gap

VT_GAP_SCHEMA_VERSION = "v_t_gap.v1"


def extract_v_t_gap(
    *,
    run_dir: str | Path | None = None,
    timeseries: str | Path | None = None,
    actor_trace: str | Path | None = None,
    fixed_scene_resolved: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser() if run_dir else None
    timeseries_path = _first_existing(
        timeseries,
        *((
            root / "timeseries.csv",
            root / "timeseries.jsonl",
        ) if root else ()),
    )
    trace_path = _first_existing(
        actor_trace,
        *((root / "artifacts" / "scenario_actor_trace.jsonl",) if root else ()),
    )
    resolved_path = _first_existing(
        fixed_scene_resolved,
        *((root / "artifacts" / "fixed_scene_resolved.json",) if root else ()),
    )
    missing: list[str] = []
    if timeseries_path is None:
        missing.append("timeseries.csv_or_jsonl")
    if resolved_path is None:
        missing.append("fixed_scene_resolved.json")
    resolved = _read_json(resolved_path) if resolved_path else {}
    target_contract = resolved.get("target_actor_contract") if isinstance(resolved.get("target_actor_contract"), Mapping) else {}
    target_role = target_contract.get("target_actor_role")
    if target_contract.get("status") == "not_required":
        return _report(
            status="not_applicable",
            timeseries_path=timeseries_path,
            actor_trace_path=trace_path,
            fixed_scene_resolved_path=resolved_path,
            target_contract=target_contract,
            rows=[],
            missing_fields=[],
            warnings=["target actor is not required for this scenario class"],
        )
    if not target_role:
        return _report(
            status="invalid",
            timeseries_path=timeseries_path,
            actor_trace_path=trace_path,
            fixed_scene_resolved_path=resolved_path,
            target_contract=target_contract,
            rows=[],
            missing_fields=missing + ["target_actor_contract.target_actor_role"],
            warnings=[],
            invalid_reason="missing_target_actor",
        )
    if missing:
        return _report(
            status="invalid",
            timeseries_path=timeseries_path,
            actor_trace_path=trace_path,
            fixed_scene_resolved_path=resolved_path,
            target_contract=target_contract,
            rows=[],
            missing_fields=missing,
            warnings=[],
            invalid_reason="missing_required_artifact",
        )
    ts_rows = _read_timeseries(timeseries_path)
    trace_rows = _read_jsonl(trace_path) if trace_path else []
    target_rows = [row for row in trace_rows if str(row.get("actor_role")) == str(target_role)]
    warnings: list[str] = []
    if trace_path is None:
        warnings.append("scenario_actor_trace_missing; using timeseries lead_gap fallback if available")
    elif not target_rows:
        return _report(
            status="invalid",
            timeseries_path=timeseries_path,
            actor_trace_path=trace_path,
            fixed_scene_resolved_path=resolved_path,
            target_contract=target_contract,
            rows=[],
            missing_fields=["target_actor_trace_rows"],
            warnings=[],
            invalid_reason="missing_target_actor_trace",
        )
    extracted = _extract_rows(ts_rows, target_rows, target_role=str(target_role))
    status = "pass"
    if not extracted:
        status = "invalid"
    elif any(row.get("gap_method") == "center_distance_fallback" for row in extracted):
        status = "warn"
        warnings.append("gap_t uses center_distance_fallback for at least one row")
    elif any(row.get("gap_method") == "actor_trace_longitudinal_gap_degraded" for row in extracted):
        status = "warn"
        warnings.append("gap_t uses actor_trace longitudinal fallback; bumper geometry was unavailable")
    elif any(row.get("gap_method") == "existing_lead_gap_m_degraded" for row in extracted):
        status = "warn"
        warnings.append("gap_t uses existing lead_gap_m fallback; bumper geometry was unavailable")
    return _report(
        status=status,
        timeseries_path=timeseries_path,
        actor_trace_path=trace_path,
        fixed_scene_resolved_path=resolved_path,
        target_contract=target_contract,
        rows=extracted,
        missing_fields=[] if extracted else ["v_t_gap_rows"],
        warnings=warnings,
        invalid_reason=None if extracted else "no_v_t_gap_rows",
    )


def write_v_t_gap_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "v_t_gap_report.json"
    csv_path = output / "v_t_gap.csv"
    md_path = output / "v_t_gap_summary.md"
    rows = list(report.get("rows") or [])
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "sim_time_s",
            "ego_speed_mps",
            "target_speed_mps",
            "gap_m",
            "relative_speed_mps",
            "target_actor_id",
            "target_actor_role",
            "gap_method",
            "gap_degraded",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "csv": str(csv_path), "summary": str(md_path)}


def _extract_rows(
    timeseries_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    *,
    target_role: str,
) -> list[dict[str, Any]]:
    trace_by_time = {_time_key(row): row for row in target_rows}
    extracted: list[dict[str, Any]] = []
    for ts in timeseries_rows:
        sim_time = _float_value(ts, "sim_time", "sim_time_s", "time")
        if sim_time is None:
            continue
        target = trace_by_time.get(round(sim_time, 3)) or _nearest_trace(target_rows, sim_time)
        ego_speed = _float_value(ts, "ego_speed_mps", "ego_speed", "v_mps")
        target_speed = _float_value(target or {}, "actual_speed_mps", "target_speed_mps", "lead_speed_mps")
        row: dict[str, Any]
        trace_longitudinal_gap = _float_value(target or {}, "longitudinal_to_ego_m")
        if trace_longitudinal_gap is not None and (
            _float_value(ts, "ego_length_m", "actor_length_m", "length_m") is None
            or _float_value(target or {}, "length_m", "actor_length_m", "vehicle_length_m") is None
        ):
            row = {
                "gap_m": trace_longitudinal_gap,
                "relative_speed_mps": (
                    (target_speed - ego_speed)
                    if target_speed is not None and ego_speed is not None
                    else None
                ),
                "gap_method": "actor_trace_longitudinal_gap_degraded",
                "gap_degraded": True,
            }
        elif target and _has_pose(ts) and _has_pose(target):
            gap = bumper_to_bumper_gap(
                ActorKinematics2D(
                    x=float(_float_value(ts, "ego_x", "x") or 0.0),
                    y=float(_float_value(ts, "ego_y", "y") or 0.0),
                    yaw_rad=float(_float_value(ts, "ego_yaw_rad", "ego_yaw", "yaw_rad", "yaw") or 0.0),
                    speed_mps=ego_speed,
                    length_m=_float_value(ts, "ego_length_m", "actor_length_m", "length_m"),
                    width_m=_float_value(ts, "ego_width_m", "actor_width_m", "width_m"),
                ),
                ActorKinematics2D(
                    x=float(_float_value(target, "x") or 0.0),
                    y=float(_float_value(target, "y") or 0.0),
                    yaw_rad=float(_float_value(target, "yaw_rad", "yaw") or 0.0),
                    speed_mps=target_speed,
                    length_m=_float_value(target, "length_m", "actor_length_m", "vehicle_length_m"),
                    width_m=_float_value(target, "width_m", "actor_width_m", "vehicle_width_m"),
                ),
            )
            row = {
                "gap_m": gap.get("gap_m"),
                "relative_speed_mps": gap.get("relative_speed_mps"),
                "gap_method": gap.get("gap_method"),
                "gap_degraded": bool(gap.get("degraded")),
            }
        elif _float_value(ts, "lead_gap_m") is not None:
            row = {
                "gap_m": _float_value(ts, "lead_gap_m"),
                "relative_speed_mps": (
                    (target_speed - ego_speed)
                    if target_speed is not None and ego_speed is not None
                    else None
                ),
                "gap_method": "existing_lead_gap_m_degraded",
                "gap_degraded": True,
            }
        else:
            continue
        row.update(
            {
                "schema_version": VT_GAP_SCHEMA_VERSION,
                "sim_time_s": sim_time,
                "ego_speed_mps": ego_speed,
                "target_speed_mps": target_speed,
                "target_actor_id": (target or {}).get("actor_id"),
                "target_actor_role": target_role,
            }
        )
        extracted.append(row)
    return extracted


def _report(
    *,
    status: str,
    timeseries_path: Path | None,
    actor_trace_path: Path | None,
    fixed_scene_resolved_path: Path | None,
    target_contract: Mapping[str, Any],
    rows: list[dict[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
    invalid_reason: str | None = None,
) -> dict[str, Any]:
    gap_method_counts = _counts(str(row.get("gap_method")) for row in rows if row.get("gap_method"))
    degraded_reason_counts = _counts(
        str(row.get("gap_method"))
        for row in rows
        if row.get("gap_degraded") or str(row.get("gap_method") or "").endswith("_degraded")
    )
    target_ids = [row.get("target_actor_id") for row in rows if row.get("target_actor_id") not in {None, ""}]
    return {
        "schema_version": VT_GAP_SCHEMA_VERSION,
        "status": status,
        "invalid_reason": invalid_reason,
        "failure_reason": invalid_reason,
        "timeseries_format": timeseries_path.suffix.lstrip(".") if timeseries_path else None,
        "timeseries_path": str(timeseries_path) if timeseries_path else None,
        "actor_trace_path": str(actor_trace_path) if actor_trace_path else None,
        "fixed_scene_resolved_path": str(fixed_scene_resolved_path) if fixed_scene_resolved_path else None,
        "target_actor_contract": dict(target_contract),
        "target_actor_role": target_contract.get("target_actor_role"),
        "target_actor_id": target_ids[0] if target_ids else target_contract.get("target_actor_id"),
        "row_count": len(rows),
        "rows_count": len(rows),
        "gap_method_counts": gap_method_counts,
        "degraded_reason_counts": degraded_reason_counts,
        "gap_status": status,
        "rows": rows,
        "missing_fields": missing_fields,
        "warnings": warnings,
    }


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 v-t-gap Summary",
        "",
        f"Status: `{report.get('status')}`",
        f"Rows: `{report.get('row_count')}`",
        f"Target role: `{(report.get('target_actor_contract') or {}).get('target_actor_role')}`",
        f"Invalid reason: `{report.get('invalid_reason')}`",
        "",
    ]
    warnings = report.get("warnings") or []
    if warnings:
        lines.append("Warnings:")
        for warning in warnings:
            lines.append(f"- {warning}")
    return "\n".join(lines) + "\n"


def _read_timeseries(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if path.suffix.lower() == ".jsonl":
        return _read_jsonl(path)
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, Mapping):
            rows.append(dict(data))
    return rows


def _first_existing(*paths: str | Path | None) -> Path | None:
    for path in paths:
        if path is None:
            continue
        candidate = Path(path)
        if candidate.exists():
            return candidate
    return None


def _float_value(row: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if value in {None, ""}:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _has_pose(row: Mapping[str, Any]) -> bool:
    return _float_value(row, "x", "ego_x") is not None and _float_value(row, "y", "ego_y") is not None


def _time_key(row: Mapping[str, Any]) -> float:
    return round(float(_float_value(row, "sim_time_sec", "sim_time_s", "sim_time", "time") or 0.0), 3)


def _nearest_trace(rows: list[dict[str, Any]], sim_time: float) -> dict[str, Any] | None:
    if not rows:
        return None
    return min(rows, key=lambda row: abs(_time_key(row) - round(sim_time, 3)))


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        result[str(value)] = result.get(str(value), 0) + 1
    return result
