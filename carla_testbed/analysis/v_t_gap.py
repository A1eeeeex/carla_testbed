from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.gap import ActorKinematics2D, bumper_to_bumper_gap
from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract

VT_GAP_SCHEMA_VERSION = "v_t_gap.v1"
MAX_VALID_LONGITUDINAL_GAP_LATERAL_M = 6.0
ROUTE_S_ANCHOR_CONFLICT_ABS_TOL_M = 5.0
ROUTE_S_ANCHOR_CONFLICT_REL_TOL = 0.35


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
    manifest = _read_json(root / "manifest.json") if root else {}
    resolved = _read_json(resolved_path) if resolved_path else {}
    target_contract: dict[str, Any] = {}
    if isinstance(manifest.get("target_actor_contract"), Mapping):
        target_contract.update(dict(manifest["target_actor_contract"]))
    if isinstance(resolved.get("target_actor_contract"), Mapping):
        target_contract.update(dict(resolved["target_actor_contract"]))
    if not target_contract and manifest:
        target_contract.update(resolve_target_actor_contract(manifest))
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
    ts_rows = _enrich_ego_dimensions_from_run(ts_rows, root)
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
    target_rows, time_alignment = _align_actor_trace_timebase(
        timeseries_rows=ts_rows,
        target_rows=target_rows,
        manifest=manifest,
        root=root,
        warnings=warnings,
    )
    target_rows, activation_filter = _apply_target_activation_filter(target_rows, target_contract)
    if activation_filter["status"] == "invalid":
        return _report(
            status="invalid",
            timeseries_path=timeseries_path,
            actor_trace_path=trace_path,
            fixed_scene_resolved_path=resolved_path,
            target_contract=target_contract,
            rows=[],
            missing_fields=["target_activation_phase_rows"],
            warnings=[],
            invalid_reason="missing_target_activation_phase",
            target_activation_filter=activation_filter,
            time_alignment=time_alignment,
        )
    activation_start = _float_value(activation_filter, "activation_start_s")
    if activation_start is not None:
        ts_rows = [
            row
            for row in ts_rows
            if (_float_value(row, "sim_time", "sim_time_s", "time") or 0.0) >= activation_start
        ]
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
    elif any(row.get("gap_degraded") for row in extracted):
        status = "warn"
        warnings.append("gap_t contains degraded rows")
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
        target_activation_filter=activation_filter,
        time_alignment=time_alignment,
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
            "gap_degraded_reason",
            "lateral_offset_m",
            "longitudinal_center_gap_m",
            "ego_route_s",
            "target_route_s",
            "ego_lane_id",
            "target_lane_id",
            "route_s_direction_anchor_m",
            "route_s_direction_anchor_source",
            "route_s_direction_corrected",
            "ego_trajectory_progress_m",
            "target_trajectory_progress_m",
            "trajectory_progress_initial_center_gap_m",
            "trajectory_progress_source",
            "route_gap_unavailable_reason",
            "source_files",
            "validity",
            "invalid_reason",
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
    progress_context = _trajectory_progress_context(timeseries_rows, target_rows)
    extracted: list[dict[str, Any]] = []
    for ts in timeseries_rows:
        sim_time = _float_value(ts, "sim_time", "sim_time_s", "time")
        if sim_time is None:
            continue
        target = trace_by_time.get(round(sim_time, 3)) or _nearest_trace(target_rows, sim_time)
        ego_speed = _float_value(ts, "ego_speed_mps", "ego_speed", "v_mps")
        target_speed = _float_value(target or {}, "actual_speed_mps", "target_speed_mps", "lead_speed_mps")
        row: dict[str, Any]
        route_gap_row, route_gap_unavailable_reason = _route_s_gap_row(ts, target or {}, ego_speed, target_speed)
        trajectory_gap_row, trajectory_gap_unavailable_reason = _trajectory_progress_gap_row(
            sim_time,
            ts,
            target or {},
            ego_speed,
            target_speed,
            progress_context,
        )
        trace_longitudinal_gap = _float_value(target or {}, "longitudinal_to_ego_m")
        if route_gap_row is not None:
            row = route_gap_row
        elif trajectory_gap_row is not None and route_gap_unavailable_reason in {
            "lane_id_mismatch",
            "route_s_anchor_conflict",
        }:
            row = trajectory_gap_row
            row["route_gap_unavailable_reason"] = route_gap_unavailable_reason
        elif trace_longitudinal_gap is not None and (
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
                "gap_degraded_reason": "missing_actor_bbox_or_length",
                "route_gap_unavailable_reason": route_gap_unavailable_reason or trajectory_gap_unavailable_reason,
            }
        elif target and _has_pose(ts) and _has_pose(target):
            gap = bumper_to_bumper_gap(
                ActorKinematics2D(
                    x=float(_float_value(ts, "ego_x", "x") or 0.0),
                    y=float(_float_value(ts, "ego_y", "y") or 0.0),
                    yaw_rad=float(
                        _float_value(ts, "ego_yaw_rad", "ego_yaw", "ego_heading", "heading", "yaw_rad", "yaw")
                        or 0.0
                    ),
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
            lateral_offset = _float_value(gap, "lateral_offset_m")
            degraded = bool(gap.get("degraded"))
            degraded_reason = "missing_actor_bbox_or_length" if degraded else None
            gap_method = str(gap.get("gap_method"))
            if (
                not degraded
                and lateral_offset is not None
                and abs(lateral_offset) > MAX_VALID_LONGITUDINAL_GAP_LATERAL_M
            ):
                degraded = True
                degraded_reason = "lateral_offset_exceeds_projection_validity"
                gap_method = "bumper_to_bumper_longitudinal_projection_lateral_degraded"
            row = {
                "gap_m": gap.get("gap_m"),
                "relative_speed_mps": gap.get("relative_speed_mps"),
                "gap_method": gap_method,
                "gap_degraded": degraded,
                "gap_degraded_reason": degraded_reason,
                "lateral_offset_m": lateral_offset,
                "longitudinal_center_gap_m": gap.get("longitudinal_center_gap_m"),
                "route_gap_unavailable_reason": route_gap_unavailable_reason or trajectory_gap_unavailable_reason,
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
                "gap_degraded_reason": "missing_actor_trace_or_bbox_using_existing_lead_gap",
                "route_gap_unavailable_reason": route_gap_unavailable_reason or trajectory_gap_unavailable_reason,
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
                "ego_route_s": _float_value(ts, "ego_route_s", "route_s"),
                "target_route_s": _float_value(target or {}, "route_s", "target_route_s"),
                "ego_lane_id": _string_value(ts, "ego_lane_id", "lane_id"),
                "target_lane_id": _string_value(target or {}, "lane_id", "current_lane_id", "target_lane_id"),
            }
        )
        extracted.append(row)
    return extracted


def _trajectory_progress_context(
    timeseries_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    ego_progress = _cumulative_progress_by_time(timeseries_rows, x_keys=("ego_x", "x"), y_keys=("ego_y", "y"))
    target_progress = _cumulative_progress_by_time(target_rows, x_keys=("x",), y_keys=("y",))
    trace_by_time = {_time_key(row): row for row in target_rows}
    initial_center_gap = None
    initial_source = None
    for ts in sorted(timeseries_rows, key=_time_key):
        sim_time = _float_value(ts, "sim_time", "sim_time_s", "time")
        if sim_time is None:
            continue
        target = trace_by_time.get(round(sim_time, 3)) or _nearest_trace(target_rows, sim_time)
        if not target:
            continue
        initial_center_gap, initial_source = _route_s_direction_anchor(ts, target)
        if initial_center_gap is not None:
            break
    return {
        "ego_progress_by_time": ego_progress,
        "target_progress_by_time": target_progress,
        "initial_center_gap_m": initial_center_gap,
        "initial_center_gap_source": initial_source,
    }


def _trajectory_progress_gap_row(
    sim_time: float,
    ts: Mapping[str, Any],
    target: Mapping[str, Any],
    ego_speed: float | None,
    target_speed: float | None,
    context: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    initial_center_gap = _float_value(context, "initial_center_gap_m")
    if initial_center_gap is None:
        return None, "missing_trajectory_progress_initial_gap"
    ego_progress = (context.get("ego_progress_by_time") or {}).get(round(sim_time, 3))
    target_progress = (context.get("target_progress_by_time") or {}).get(_time_key(target))
    if ego_progress is None or target_progress is None:
        return None, "missing_ego_or_target_trajectory_progress"
    ego_length = _float_value(ts, "ego_length_m", "actor_length_m", "length_m")
    target_length = _float_value(target, "length_m", "actor_length_m", "vehicle_length_m")
    if ego_length is None or target_length is None:
        return None, "missing_actor_bbox_or_length"
    center_gap = initial_center_gap + target_progress - ego_progress
    return (
        {
            "gap_m": center_gap - 0.5 * ego_length - 0.5 * target_length,
            "relative_speed_mps": (
                (target_speed - ego_speed) if target_speed is not None and ego_speed is not None else None
            ),
            "gap_method": "trajectory_progress_bumper_gap",
            "gap_degraded": False,
            "gap_degraded_reason": None,
            "longitudinal_center_gap_m": center_gap,
            "ego_trajectory_progress_m": ego_progress,
            "target_trajectory_progress_m": target_progress,
            "trajectory_progress_initial_center_gap_m": initial_center_gap,
            "trajectory_progress_source": context.get("initial_center_gap_source"),
        },
        None,
    )


def _route_s_gap_row(
    ts: Mapping[str, Any],
    target: Mapping[str, Any],
    ego_speed: float | None,
    target_speed: float | None,
) -> tuple[dict[str, Any] | None, str | None]:
    ego_route_s = _float_value(ts, "ego_route_s", "route_s")
    target_route_s = _float_value(target, "route_s", "target_route_s")
    if ego_route_s is None or target_route_s is None:
        return None, "missing_ego_or_target_route_s"
    ego_lane_id = _string_value(ts, "ego_lane_id", "lane_id")
    target_lane_id = _string_value(target, "lane_id", "current_lane_id", "target_lane_id")
    if not ego_lane_id or not target_lane_id:
        return None, "missing_ego_or_target_lane_id"
    if ego_lane_id != target_lane_id:
        return None, "lane_id_mismatch"
    ego_length = _float_value(ts, "ego_length_m", "actor_length_m", "length_m")
    target_length = _float_value(target, "length_m", "actor_length_m", "vehicle_length_m")
    if ego_length is None or target_length is None:
        return None, "missing_actor_bbox_or_length"
    anchor, anchor_source = _route_s_direction_anchor(ts, target)
    if anchor is None:
        return None, "missing_route_s_direction_anchor"
    raw_center_gap = target_route_s - ego_route_s
    anchor_conflict_tolerance = max(
        ROUTE_S_ANCHOR_CONFLICT_ABS_TOL_M,
        ROUTE_S_ANCHOR_CONFLICT_REL_TOL * abs(anchor),
    )
    if abs(abs(raw_center_gap) - abs(anchor)) > anchor_conflict_tolerance:
        return None, "route_s_anchor_conflict"
    center_gap = math.copysign(abs(raw_center_gap), anchor if anchor != 0.0 else raw_center_gap)
    return (
        {
            "gap_m": center_gap - 0.5 * ego_length - 0.5 * target_length,
            "relative_speed_mps": (
                (target_speed - ego_speed) if target_speed is not None and ego_speed is not None else None
            ),
            "gap_method": "route_s_bumper_gap",
            "gap_degraded": False,
            "gap_degraded_reason": None,
            "longitudinal_center_gap_m": center_gap,
            "route_s_direction_anchor_m": anchor,
            "route_s_direction_anchor_source": anchor_source,
            "route_s_direction_corrected": (raw_center_gap < 0 < anchor) or (raw_center_gap > 0 > anchor),
            "route_gap_unavailable_reason": None,
        },
        None,
    )


def _route_s_direction_anchor(ts: Mapping[str, Any], target: Mapping[str, Any]) -> tuple[float | None, str | None]:
    longitudinal = _float_value(target, "longitudinal_to_ego_m")
    if longitudinal is not None:
        return longitudinal, "actor_trace_longitudinal_to_ego_m"
    pose_longitudinal = _pose_longitudinal_center_gap(ts, target)
    if pose_longitudinal is not None:
        return pose_longitudinal, "ego_pose_forward_projection"
    return None, None


def _pose_longitudinal_center_gap(ts: Mapping[str, Any], target: Mapping[str, Any]) -> float | None:
    ego_x = _float_value(ts, "ego_x", "x")
    ego_y = _float_value(ts, "ego_y", "y")
    target_x = _float_value(target, "x")
    target_y = _float_value(target, "y")
    ego_yaw = _float_value(ts, "ego_yaw_rad", "ego_yaw", "ego_heading", "heading", "yaw_rad", "yaw")
    if ego_x is None or ego_y is None or target_x is None or target_y is None or ego_yaw is None:
        return None
    return (target_x - ego_x) * math.cos(ego_yaw) + (target_y - ego_y) * math.sin(ego_yaw)


def _cumulative_progress_by_time(
    rows: list[dict[str, Any]],
    *,
    x_keys: tuple[str, ...],
    y_keys: tuple[str, ...],
) -> dict[float, float]:
    progress: dict[float, float] = {}
    previous: tuple[float, float] | None = None
    cumulative = 0.0
    for row in sorted(rows, key=_time_key):
        time_key = _time_key(row)
        x = _float_value(row, *x_keys)
        y = _float_value(row, *y_keys)
        if x is None or y is None:
            continue
        if previous is not None:
            cumulative += math.hypot(x - previous[0], y - previous[1])
        progress[time_key] = cumulative
        previous = (x, y)
    return progress


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
    target_activation_filter: Mapping[str, Any] | None = None,
    time_alignment: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source_files = {
        "timeseries": str(timeseries_path) if timeseries_path else None,
        "actor_trace": str(actor_trace_path) if actor_trace_path else None,
        "fixed_scene_resolved": str(fixed_scene_resolved_path) if fixed_scene_resolved_path else None,
    }
    row_source_files = ";".join(value for value in source_files.values() if value)
    normalized_rows = []
    for row in rows:
        item = dict(row)
        item.setdefault("source_files", row_source_files)
        item.setdefault("validity", "degraded" if item.get("gap_degraded") else "valid")
        item.setdefault("invalid_reason", None)
        normalized_rows.append(item)
    gap_method_counts = _counts(str(row.get("gap_method")) for row in rows if row.get("gap_method"))
    degraded_reason_counts = _counts(
        str(row.get("gap_method"))
        for row in rows
        if row.get("gap_degraded") or str(row.get("gap_method") or "").endswith("_degraded")
    )
    target_ids = [row.get("target_actor_id") for row in normalized_rows if row.get("target_actor_id") not in {None, ""}]
    return {
        "schema_version": VT_GAP_SCHEMA_VERSION,
        "status": status,
        "validity": _validity_from_status(status),
        "invalid_reason": invalid_reason,
        "failure_reason": invalid_reason,
        "source_files": source_files,
        "timeseries_format": timeseries_path.suffix.lstrip(".") if timeseries_path else None,
        "timeseries_path": str(timeseries_path) if timeseries_path else None,
        "actor_trace_path": str(actor_trace_path) if actor_trace_path else None,
        "fixed_scene_resolved_path": str(fixed_scene_resolved_path) if fixed_scene_resolved_path else None,
        "target_actor_contract": dict(target_contract),
        "target_activation_filter": dict(target_activation_filter or _default_activation_filter(target_contract)),
        "time_alignment": dict(time_alignment or {"status": "not_evaluated"}),
        "target_actor_role": target_contract.get("target_actor_role"),
        "target_actor_id": target_ids[0] if target_ids else target_contract.get("target_actor_id"),
        "row_count": len(rows),
        "rows_count": len(rows),
        "gap_method_counts": gap_method_counts,
        "degraded_reason_counts": degraded_reason_counts,
        "gap_status": status,
        "rows": normalized_rows,
        "missing_fields": missing_fields,
        "warnings": warnings,
    }


def _apply_target_activation_filter(
    target_rows: list[dict[str, Any]],
    target_contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    activation = target_contract.get("activation") if isinstance(target_contract.get("activation"), Mapping) else {}
    active_after_phase = activation.get("active_after_phase") if isinstance(activation, Mapping) else None
    if not active_after_phase:
        return target_rows, _default_activation_filter(target_contract)
    phase_rows = [row for row in target_rows if str(row.get("phase") or "") == str(active_after_phase)]
    if not phase_rows:
        return [], {
            "status": "invalid",
            "mode": "active_after_phase",
            "active_after_phase": str(active_after_phase),
            "activation_start_s": None,
            "rows_before_filter": len(target_rows),
            "rows_after_filter": 0,
        }
    phase_times = [
        value
        for value in (_float_value(row, "sim_time_sec", "sim_time_s", "sim_time") for row in phase_rows)
        if value is not None
    ]
    if not phase_times:
        return [], {
            "status": "invalid",
            "mode": "active_after_phase",
            "active_after_phase": str(active_after_phase),
            "activation_start_s": None,
            "rows_before_filter": len(target_rows),
            "rows_after_filter": 0,
            "invalid_reason": "target_activation_phase_time_missing",
        }
    activation_start = min(phase_times)
    filtered = [
        row
        for row in target_rows
        if (_float_value(row, "sim_time_sec", "sim_time_s", "sim_time") or 0.0) >= activation_start
    ]
    return filtered, {
        "status": "pass",
        "mode": "active_after_phase",
        "active_after_phase": str(active_after_phase),
        "activation_start_s": activation_start,
        "rows_before_filter": len(target_rows),
        "rows_after_filter": len(filtered),
    }


def _default_activation_filter(target_contract: Mapping[str, Any]) -> dict[str, Any]:
    activation = target_contract.get("activation") if isinstance(target_contract.get("activation"), Mapping) else {}
    return {
        "status": "not_applied",
        "mode": str(activation.get("activation_semantics") or "active_from_scenario_start"),
        "active_after_phase": activation.get("active_after_phase"),
        "activation_start_s": None,
        "rows_before_filter": None,
        "rows_after_filter": None,
    }


def _align_actor_trace_timebase(
    *,
    timeseries_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    manifest: Mapping[str, Any],
    root: Path | None,
    warnings: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ts_min, ts_max = _time_range(timeseries_rows)
    trace_min, trace_max = _time_range(target_rows)
    start_sim_time = _fixed_scene_runtime_start_sim_time_s(manifest, root)
    report: dict[str, Any] = {
        "status": "not_applied",
        "source": None,
        "offset_s": 0.0,
        "timeseries_min_s": ts_min,
        "timeseries_max_s": ts_max,
        "actor_trace_min_s": trace_min,
        "actor_trace_max_s": trace_max,
    }
    if ts_min is None or trace_min is None:
        report["status"] = "insufficient_data"
        return target_rows, report
    current_delta = abs(trace_min - ts_min)
    if start_sim_time is not None:
        shifted_delta = abs((trace_min + start_sim_time) - ts_min)
        if current_delta > 1.0 and shifted_delta < current_delta:
            aligned = _shift_trace_times(
                target_rows,
                offset_s=start_sim_time,
                source="fixed_scene_runtime_hook.start_sim_time_s",
            )
            report.update(
                {
                    "status": "applied",
                    "source": "fixed_scene_runtime_hook.start_sim_time_s",
                    "offset_s": start_sim_time,
                    "aligned_actor_trace_min_s": trace_min + start_sim_time,
                    "aligned_actor_trace_max_s": (trace_max + start_sim_time) if trace_max is not None else None,
                }
            )
            return aligned, report
    if current_delta > 60.0 and abs(trace_min) < 60.0:
        offset = ts_min - trace_min
        warnings.append("actor_trace_timebase_offset_inferred_from_timeseries_min")
        aligned = _shift_trace_times(target_rows, offset_s=offset, source="timeseries_min_inferred")
        report.update(
            {
                "status": "applied",
                "source": "timeseries_min_inferred",
                "offset_s": offset,
                "aligned_actor_trace_min_s": trace_min + offset,
                "aligned_actor_trace_max_s": (trace_max + offset) if trace_max is not None else None,
            }
        )
        return aligned, report
    if current_delta > 1.0:
        warnings.append("actor_trace_timeseries_timebase_mismatch_unresolved")
        report["status"] = "unresolved"
    return target_rows, report


def _fixed_scene_runtime_start_sim_time_s(manifest: Mapping[str, Any], root: Path | None) -> float | None:
    candidates: list[Mapping[str, Any]] = []
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), Mapping) else {}
    scenario_metadata = (
        metadata.get("scenario_metadata") if isinstance(metadata.get("scenario_metadata"), Mapping) else {}
    )
    for payload in (
        scenario_metadata.get("fixed_scene_runtime_hook"),
        metadata.get("fixed_scene_runtime_hook"),
        manifest.get("fixed_scene_runtime_hook"),
    ):
        if isinstance(payload, Mapping):
            candidates.append(payload)
    if root is not None:
        hook_path = root / "artifacts" / "fixed_scene_runtime_hook.json"
        if hook_path.exists():
            payload = _read_json(hook_path)
            if payload:
                candidates.append(payload)
    for candidate in candidates:
        value = _float_value(candidate, "start_sim_time_s", "start_sim_time", "runtime_start_sim_time_s")
        if value is not None:
            return value
    return None


def _shift_trace_times(rows: list[dict[str, Any]], *, offset_s: float, source: str) -> list[dict[str, Any]]:
    shifted: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        original = _float_value(item, "sim_time_sec", "sim_time_s", "sim_time", "time")
        if original is not None:
            item.setdefault("original_sim_time_sec", original)
            item["sim_time_sec"] = original + float(offset_s)
            item["timebase_alignment_offset_s"] = float(offset_s)
            item["timebase_alignment_source"] = source
        shifted.append(item)
    return shifted


def _time_range(rows: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    values = [
        value
        for value in (_float_value(row, "sim_time_sec", "sim_time_s", "sim_time", "time") for row in rows)
        if value is not None
    ]
    if not values:
        return None, None
    return min(values), max(values)


def _validity_from_status(status: str) -> str:
    if status == "pass":
        return "valid"
    if status == "warn":
        return "degraded"
    if status == "not_applicable":
        return "not_applicable"
    return "invalid"


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


def _enrich_ego_dimensions_from_run(rows: list[dict[str, Any]], root: Path | None) -> list[dict[str, Any]]:
    if root is None or not rows:
        return rows
    dimensions = _read_ego_dimensions(root / "artifacts" / "carla_vehicle_characteristics.json")
    if not dimensions:
        return rows
    enriched: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if _float_value(item, "ego_length_m", "actor_length_m", "length_m") is None and dimensions.get("length_m") is not None:
            item["ego_length_m"] = dimensions["length_m"]
        if _float_value(item, "ego_width_m", "actor_width_m", "width_m") is None and dimensions.get("width_m") is not None:
            item["ego_width_m"] = dimensions["width_m"]
        enriched.append(item)
    return enriched


def _read_ego_dimensions(path: Path) -> dict[str, float]:
    payload = _read_json(path)
    length = _float_value(payload, "length", "length_m", "vehicle_length_m")
    width = _float_value(payload, "width", "width_m", "vehicle_width_m")
    result: dict[str, float] = {}
    if length is not None:
        result["length_m"] = length
    if width is not None:
        result["width_m"] = width
    return result


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


def _string_value(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value in {None, ""}:
            continue
        return str(value)
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
