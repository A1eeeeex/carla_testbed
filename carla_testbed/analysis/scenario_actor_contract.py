from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

SCENARIO_ACTOR_CONTRACT_SCHEMA_VERSION = "scenario_actor_contract.v1"


def analyze_scenario_actor_contract_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    storyboard_path = _find_first(root, ["artifacts/fixed_scene_resolved.json", "fixed_scene_resolved.json"])
    trace_path = _find_first(root, ["artifacts/scenario_actor_trace.jsonl", "scenario_actor_trace.jsonl"])
    events_path = _find_first(root, ["artifacts/scenario_phase_events.jsonl", "scenario_phase_events.jsonl"])
    return analyze_scenario_actor_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
    )


def analyze_scenario_actor_contract(
    *,
    storyboard_path: str | Path | None,
    trace_path: str | Path | None,
    events_path: str | Path | None = None,
) -> dict[str, Any]:
    storyboard = _read_json(storyboard_path) if storyboard_path else {}
    rows = _read_jsonl(trace_path)
    events = _read_jsonl(events_path)
    missing_artifacts: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    missing_fields: list[str] = []
    if not storyboard_path:
        missing_artifacts.append("artifacts/fixed_scene_resolved.json")
    if not trace_path:
        missing_artifacts.append("artifacts/scenario_actor_trace.jsonl")
    if not rows:
        missing_fields.append("scenario_actor_trace.rows")

    template = str(storyboard.get("template") or "")
    if template == "follow_stop":
        behavior = _analyze_follow_stop(rows)
    elif template == "static_lead_stop":
        behavior = _analyze_static_lead_stop(rows)
    elif template == "lead_vehicle_accel_decel":
        behavior = _analyze_speed_profile(rows, storyboard)
    elif template in {"cut_in", "cut_out"}:
        behavior = _analyze_lane_change(rows, template=template, storyboard=storyboard)
    else:
        behavior = _analyze_generic(rows, events)
    missing_fields.extend(behavior["missing_fields"])
    warnings.extend(behavior["warnings"])
    blocking_reasons.extend(behavior["blocking_reasons"])

    if blocking_reasons:
        status = "fail"
    elif missing_artifacts or missing_fields:
        status = "insufficient_data"
    elif warnings:
        status = "warn"
    else:
        status = "pass"

    return {
        "schema_version": SCENARIO_ACTOR_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "scene_id": storyboard.get("scene_id"),
        "template": template or None,
        "scenario_class": storyboard.get("scenario_class"),
        "trace_row_count": len(rows),
        "phase_event_count": len(events),
        "behavior": behavior,
        "metrics": _metrics_from_behavior(behavior, rows, events),
        "missing_artifacts": missing_artifacts,
        "missing_fields": sorted(set(missing_fields)),
        "warnings": sorted(set(warnings)),
        "blocking_reasons": sorted(set(blocking_reasons)),
        "artifact_paths": {
            "storyboard": str(storyboard_path) if storyboard_path else None,
            "trace": str(trace_path) if trace_path else None,
            "phase_events": str(events_path) if events_path else None,
        },
        "interpretation_boundary": (
            "This report checks scripted scenario actor evidence. "
            "It is not an ego autonomy success verdict."
        ),
    }


def write_scenario_actor_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "scenario_actor_contract_report.json"
    summary_path = output_dir / "scenario_actor_contract_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_summary(report), encoding="utf-8")
    return {"report": str(report_path), "summary": str(summary_path)}


def _analyze_static_lead_stop(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lead_rows = [row for row in rows if str(row.get("actor_role")) == "lead_vehicle"]
    missing_fields: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    if not lead_rows:
        missing_fields.append("lead_vehicle_trace")
        return {
            "type": "static_lead_stop",
            "lead_trace_rows": 0,
            "missing_fields": missing_fields,
            "warnings": warnings,
            "blocking_reasons": blocking_reasons,
        }
    speeds = [_num(row.get("actual_speed_mps")) for row in lead_rows]
    speeds = [value for value in speeds if value is not None]
    if not speeds:
        missing_fields.append("actual_speed_mps")
    max_speed = max(speeds) if speeds else None
    final_speed = speeds[-1] if speeds else None
    max_horizontal_displacement = _max_horizontal_displacement(lead_rows)
    hold_rows = [row for row in lead_rows if row.get("phase") == "lead_hold_stop"]
    if max_horizontal_displacement is None:
        if speeds and max_speed is not None and max_speed > 0.5:
            blocking_reasons.append("static_lead_vehicle_moved")
        else:
            missing_fields.extend(["x", "y"])
    elif max_horizontal_displacement > 0.5:
        blocking_reasons.append("static_lead_vehicle_moved")
    if not hold_rows:
        blocking_reasons.append("lead_hold_stop_phase_not_observed")
    lead_stayed_stopped = bool(max_horizontal_displacement is not None and max_horizontal_displacement <= 0.5)
    return {
        "type": "static_lead_stop",
        "lead_trace_rows": len(lead_rows),
        "max_lead_speed_mps": max_speed,
        "final_lead_speed_mps": final_speed,
        "max_horizontal_displacement_m": max_horizontal_displacement,
        "hold_phase_rows": len(hold_rows),
        "lead_stayed_stopped": lead_stayed_stopped,
        "missing_fields": missing_fields,
        "warnings": warnings,
        "blocking_reasons": blocking_reasons,
    }


def _analyze_follow_stop(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lead_rows = [row for row in rows if str(row.get("actor_role")) == "lead_vehicle"]
    missing_fields: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    if not lead_rows:
        missing_fields.append("lead_vehicle_trace")
        return {
            "type": "follow_stop",
            "lead_trace_rows": 0,
            "missing_fields": missing_fields,
            "warnings": warnings,
            "blocking_reasons": blocking_reasons,
        }
    distances = [_num(row.get("distance_to_ego_m")) for row in lead_rows]
    speeds = [_num(row.get("actual_speed_mps")) for row in lead_rows]
    distances = [value for value in distances if value is not None]
    speeds = [value for value in speeds if value is not None]
    if not distances:
        missing_fields.append("distance_to_ego_m")
    if not speeds:
        missing_fields.append("actual_speed_mps")
    min_speed = min(speeds) if speeds else None
    final_speed = speeds[-1] if speeds else None
    min_gap = min(distances) if distances else None
    initial_gap = distances[0] if distances else None
    hold_rows = [row for row in lead_rows if row.get("phase") == "lead_hold_stop"]
    lead_stopped = bool(min_speed is not None and min_speed <= 0.3)
    if speeds and not lead_stopped:
        blocking_reasons.append("lead_vehicle_never_observed_stopped")
    if hold_rows and final_speed is not None and final_speed > 0.5:
        blocking_reasons.append("lead_vehicle_not_stationary_at_end")
    if not hold_rows:
        warnings.append("lead_hold_stop_phase_not_observed")
    return {
        "type": "follow_stop",
        "lead_trace_rows": len(lead_rows),
        "initial_gap_m": initial_gap,
        "min_gap_m": min_gap,
        "min_lead_speed_mps": min_speed,
        "final_lead_speed_mps": final_speed,
        "lead_stopped": lead_stopped,
        "hold_phase_rows": len(hold_rows),
        "missing_fields": missing_fields,
        "warnings": warnings,
        "blocking_reasons": blocking_reasons,
    }


def _analyze_speed_profile(rows: Sequence[Mapping[str, Any]], storyboard: Mapping[str, Any]) -> dict[str, Any]:
    lead_rows = [row for row in rows if str(row.get("actor_role")) == "lead_vehicle"]
    missing_fields: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    if not lead_rows:
        missing_fields.append("lead_vehicle_trace")
        return {
            "type": "lead_vehicle_accel_decel",
            "lead_trace_rows": 0,
            "speed_profile_error_p95_mps": None,
            "missing_fields": missing_fields,
            "warnings": warnings,
            "blocking_reasons": blocking_reasons,
        }
    errors: list[float] = []
    for row in lead_rows:
        target = _num(row.get("target_speed_mps"))
        actual = _num(row.get("actual_speed_mps"))
        if target is not None and actual is not None:
            errors.append(abs(actual - target))
    if not errors:
        missing_fields.append("target_speed_mps_or_actual_speed_mps")
    p95 = _percentile(errors, 95.0) if errors else None
    threshold = _num(_success_criteria(storyboard).get("speed_profile_error_p95_mps", 2.0)) or 2.0
    if p95 is not None and p95 > threshold:
        blocking_reasons.append("speed_profile_error_too_high")
    return {
        "type": "lead_vehicle_accel_decel",
        "lead_trace_rows": len(lead_rows),
        "speed_profile_error_p95_mps": p95,
        "speed_profile_error_threshold_mps": threshold,
        "missing_fields": missing_fields,
        "warnings": warnings,
        "blocking_reasons": blocking_reasons,
    }


def _analyze_lane_change(
    rows: Sequence[Mapping[str, Any]],
    *,
    template: str,
    storyboard: Mapping[str, Any],
) -> dict[str, Any]:
    actor_rows = [
        row
        for row in rows
        if str(row.get("actor_role")) in {"lead_vehicle", "cutin_vehicle", "cutout_vehicle"}
    ]
    missing_fields: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    if not actor_rows:
        missing_fields.append("lane_change_actor_trace")
    lane_rows = [row for row in actor_rows if str(row.get("action_type")) == "lane_change"]
    if not lane_rows:
        missing_fields.append("lane_change_action_rows")
        if actor_rows:
            missing_fields.extend(["longitudinal_to_ego_m", "lateral_to_ego_m"])
    progress_values = [
        value
        for value in (_num(row.get("lane_change_progress")) for row in actor_rows)
        if value is not None
    ]
    if not progress_values:
        missing_fields.append("lane_change_progress")
    max_progress = max(progress_values) if progress_values else None
    intent_completed = bool(max_progress is not None and max_progress >= 0.95)
    if progress_values and not intent_completed:
        blocking_reasons.append("lane_change_not_completed")
    start_longitudinal = _num(lane_rows[0].get("longitudinal_to_ego_m")) if lane_rows else None
    start_lateral = _num(lane_rows[0].get("lateral_to_ego_m")) if lane_rows else None
    final_lateral = _num(lane_rows[-1].get("lateral_to_ego_m")) if lane_rows else None
    lateral_shift = (
        abs(float(final_lateral) - float(start_lateral))
        if final_lateral is not None and start_lateral is not None
        else None
    )
    criteria = _success_criteria(storyboard)
    params = storyboard.get("params") if isinstance(storyboard.get("params"), Mapping) else {}
    expected_start_gap = _num(criteria.get("lane_change_start_gap_m"))
    gap_tolerance = _num(criteria.get("lane_change_start_gap_tolerance_m")) or 2.0
    if start_longitudinal is None and lane_rows:
        missing_fields.append("longitudinal_to_ego_m")
    if start_lateral is None or final_lateral is None:
        if lane_rows:
            missing_fields.append("lateral_to_ego_m")
    if expected_start_gap is not None:
        if start_longitudinal is None:
            missing_fields.append("longitudinal_to_ego_m")
        elif abs(start_longitudinal - expected_start_gap) > gap_tolerance:
            blocking_reasons.append("lane_change_start_gap_out_of_tolerance")
    min_lateral_shift = _lane_change_min_lateral_shift(criteria, params, lane_rows)
    if lateral_shift is None:
        if lane_rows:
            missing_fields.append("lane_change_lateral_shift_m")
    elif lateral_shift < min_lateral_shift:
        blocking_reasons.append("lane_change_lateral_shift_too_small")
    lateral_dynamics = _lane_change_lateral_dynamics(lane_rows)
    if lateral_dynamics["no_teleport_check"] is False:
        blocking_reasons.append("lane_change_teleport_detected")
    observed_completed = bool(intent_completed and lateral_shift is not None and lateral_shift >= min_lateral_shift)
    runtime_modes = sorted(
        {
            str(row.get("lane_change_runtime_mode"))
            for row in lane_rows
            if row.get("lane_change_runtime_mode") not in {None, ""}
        }
    )
    return {
        "type": template,
        "actor_trace_rows": len(actor_rows),
        "lane_change_completed": observed_completed,
        "lane_change_intent_completed": intent_completed,
        "lane_change_progress_max": max_progress,
        "lane_change_start_longitudinal_gap_m": start_longitudinal,
        "lane_change_start_lateral_m": start_lateral,
        "lane_change_final_lateral_m": final_lateral,
        "lane_change_lateral_shift_m": lateral_shift,
        "lane_change_min_lateral_shift_m": min_lateral_shift,
        "lane_change_runtime_modes": runtime_modes,
        "physics_controlled_lane_change": _all_true(row.get("physics_controlled_lane_change") for row in lane_rows),
        "claim_grade_lane_change": _all_true(row.get("claim_grade_lane_change") for row in lane_rows),
        "velocity_sources": sorted(
            {
                str(row.get("velocity_source"))
                for row in lane_rows
                if row.get("velocity_source") not in {None, ""}
            }
        ),
        "lateral_dynamics": lateral_dynamics,
        "missing_fields": missing_fields,
        "warnings": warnings,
        "blocking_reasons": blocking_reasons,
    }


def _analyze_generic(rows: Sequence[Mapping[str, Any]], events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    warnings: list[str] = []
    missing_fields: list[str] = []
    if not rows:
        missing_fields.append("scenario_actor_trace.rows")
    if not events:
        warnings.append("scenario_phase_events_not_available")
    return {
        "type": "generic",
        "actor_roles": sorted({str(row.get("actor_role")) for row in rows if row.get("actor_role")}),
        "started_phases": sorted(
            {str(row.get("phase")) for row in events if row.get("event") == "phase_started" and row.get("phase")}
        ),
        "missing_fields": missing_fields,
        "warnings": warnings,
        "blocking_reasons": [],
    }


def _metrics_from_behavior(
    behavior: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    started = {str(row.get("phase")) for row in events if row.get("event") == "phase_started" and row.get("phase")}
    completed = {str(row.get("phase")) for row in events if row.get("event") == "phase_completed" and row.get("phase")}
    phases = started | completed | {str(row.get("phase")) for row in rows if row.get("phase")}
    return {
        "required_roles_spawned": bool(rows),
        "phase_completion_ratio": _ratio(len(started), len(phases)),
        "speed_profile_error_p95_mps": behavior.get("speed_profile_error_p95_mps"),
        "lane_change_completed": behavior.get("lane_change_completed"),
        "lane_change_start_longitudinal_gap_m": behavior.get("lane_change_start_longitudinal_gap_m"),
        "lane_change_lateral_shift_m": behavior.get("lane_change_lateral_shift_m"),
        "lane_change_no_teleport_check": (behavior.get("lateral_dynamics") or {}).get("no_teleport_check")
        if isinstance(behavior.get("lateral_dynamics"), Mapping)
        else None,
        "initial_gap_m": behavior.get("initial_gap_m"),
        "min_gap_m": behavior.get("min_gap_m"),
        "lead_stopped": behavior.get("lead_stopped"),
        "lead_stayed_stopped": behavior.get("lead_stayed_stopped"),
        "initial_overlap_count": 0,
        "stuck_actor_count": 0,
    }


def _success_criteria(storyboard: Mapping[str, Any]) -> Mapping[str, Any]:
    criteria = storyboard.get("success_criteria")
    return criteria if isinstance(criteria, Mapping) else {}


def _lane_change_min_lateral_shift(
    criteria: Mapping[str, Any],
    params: Mapping[str, Any],
    lane_rows: Sequence[Mapping[str, Any]],
) -> float:
    configured = _num(criteria.get("min_lateral_shift_m"))
    if configured is not None:
        return configured
    lane_width = _num(params.get("lane_width_m"))
    if lane_width is None:
        lane_width = _num(params.get("lane_change_lateral_shift_m"))
    if lane_width is None and lane_rows:
        lane_width = _num(lane_rows[0].get("lateral_shift_m"))
    if lane_width is None:
        lane_width = 3.6
    return 0.8 * abs(lane_width)


def _lane_change_lateral_dynamics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    samples: list[tuple[float, float]] = []
    for row in rows:
        t = _num(row.get("sim_time_sec"))
        lateral = _num(row.get("lateral_to_ego_m"))
        if t is not None and lateral is not None:
            samples.append((t, lateral))
    samples = sorted(samples, key=lambda item: item[0])
    lateral_speeds: list[float] = []
    lateral_accels: list[float] = []
    previous_speed: tuple[float, float] | None = None
    max_step = 0.0
    for (t0, l0), (t1, l1) in zip(samples, samples[1:]):
        dt = t1 - t0
        if dt <= 0:
            continue
        dl = l1 - l0
        max_step = max(max_step, abs(dl))
        speed = dl / dt
        lateral_speeds.append(speed)
        if previous_speed is not None:
            prev_t, prev_speed = previous_speed
            accel_dt = t1 - prev_t
            if accel_dt > 0:
                lateral_accels.append((speed - prev_speed) / accel_dt)
        previous_speed = (t1, speed)
    max_lateral_speed = max((abs(value) for value in lateral_speeds), default=None)
    max_lateral_accel = max((abs(value) for value in lateral_accels), default=None)
    no_teleport = None if len(samples) < 2 else max_step <= 1.0
    return {
        "sample_count": len(samples),
        "max_lateral_step_m": max_step if samples else None,
        "max_lateral_speed_mps": max_lateral_speed,
        "max_lateral_accel_mps2": max_lateral_accel,
        "no_teleport_check": no_teleport,
    }


def _all_true(values: Any) -> bool | None:
    normalized = [value for value in values if value is not None]
    if not normalized:
        return None
    return all(bool(value) for value in normalized)


def _summary(report: Mapping[str, Any]) -> str:
    behavior = report.get("behavior") or {}
    lines = [
        "# Scenario Actor Contract",
        "",
        f"- Status: {report.get('status')}",
        f"- Scene id: {report.get('scene_id')}",
        f"- Template: {report.get('template')}",
        f"- Trace rows: {report.get('trace_row_count')}",
        f"- Behavior type: {behavior.get('type')}",
        f"- Missing artifacts: {', '.join(report.get('missing_artifacts') or []) or 'none'}",
        f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
        f"- Warnings: {', '.join(report.get('warnings') or []) or 'none'}",
        "",
        str(report.get("interpretation_boundary", "")),
    ]
    return "\n".join(lines) + "\n"


def _find_first(root: Path, candidates: Sequence[str]) -> Path | None:
    for candidate in candidates:
        path = root / candidate
        if path.exists():
            return path
    return None


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None or not Path(path).exists():
        return []
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            if isinstance(data, dict):
                rows.append(data)
    return rows


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (float(percentile) / 100.0) * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _max_horizontal_displacement(rows: Sequence[Mapping[str, Any]]) -> float | None:
    points: list[tuple[float, float]] = []
    for row in rows:
        x = _num(row.get("x"))
        y = _num(row.get("y"))
        if x is None or y is None:
            continue
        points.append((x, y))
    if not points:
        return None
    start_x, start_y = points[0]
    return max(((x - start_x) ** 2 + (y - start_y) ** 2) ** 0.5 for x, y in points)


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)
