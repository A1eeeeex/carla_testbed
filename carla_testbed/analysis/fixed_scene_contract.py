from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.scenario_player.schema import validate_fixed_scene_storyboard
from carla_testbed.scenario_player.storyboard import phase_action_types, storyboard_phases, storyboard_roles

FIXED_SCENE_CONTRACT_SCHEMA_VERSION = "fixed_scene_contract.v1"


def analyze_fixed_scene_contract_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    storyboard_path = _find_first(
        root,
        [
            "artifacts/fixed_scene_resolved.json",
            "fixed_scene_resolved.json",
            "analysis/fixed_scene/fixed_scene_resolved.json",
        ],
    )
    trace_path = _find_first(
        root,
        [
            "artifacts/scenario_actor_trace.jsonl",
            "scenario_actor_trace.jsonl",
            "analysis/fixed_scene/scenario_actor_trace.jsonl",
        ],
    )
    events_path = _find_first(
        root,
        [
            "artifacts/scenario_phase_events.jsonl",
            "scenario_phase_events.jsonl",
            "analysis/fixed_scene/scenario_phase_events.jsonl",
        ],
    )
    runtime_state_path = _find_first(
        root,
        [
            "artifacts/fixed_scene_runtime_state.json",
            "fixed_scene_runtime_state.json",
            "analysis/fixed_scene/fixed_scene_runtime_state.json",
        ],
    )
    return analyze_fixed_scene_contract(
        storyboard_path=storyboard_path,
        trace_path=trace_path,
        events_path=events_path,
        runtime_state_path=runtime_state_path,
    )


def analyze_fixed_scene_contract(
    *,
    storyboard_path: str | Path | None,
    trace_path: str | Path | None = None,
    events_path: str | Path | None = None,
    runtime_state_path: str | Path | None = None,
) -> dict[str, Any]:
    missing_artifacts: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    storyboard: dict[str, Any] | None = None
    if storyboard_path is None:
        missing_artifacts.append("artifacts/fixed_scene_resolved.json")
    else:
        try:
            storyboard = _read_json(storyboard_path)
            validate_fixed_scene_storyboard(storyboard)
        except Exception as exc:  # noqa: BLE001 - preserve validation error in report
            storyboard = None
            blocking_reasons.append(f"invalid_storyboard:{exc}")

    trace_rows = _read_jsonl(trace_path) if trace_path else []
    event_rows = _read_jsonl(events_path) if events_path else []
    runtime_state = _read_json(runtime_state_path) if runtime_state_path else {}
    if trace_path is None:
        missing_artifacts.append("artifacts/scenario_actor_trace.jsonl")
    if events_path is None:
        missing_artifacts.append("artifacts/scenario_phase_events.jsonl")
    spawn_feasibility = runtime_state.get("spawn_feasibility") if isinstance(runtime_state, Mapping) else {}
    if isinstance(spawn_feasibility, Mapping):
        for role, feasibility in spawn_feasibility.items():
            if not isinstance(feasibility, Mapping):
                continue
            if feasibility.get("status") == "fail":
                blocking_reasons.append(f"spawn_feasibility_failed:{role}")
            if feasibility.get("status") == "warn":
                warnings.append(f"spawn_feasibility_warn:{role}")

    roles = storyboard_roles(storyboard) if storyboard else []
    non_ego_roles = [role for role in roles if role != "ego"]
    traced_roles = sorted({str(row.get("actor_role")) for row in trace_rows if row.get("actor_role")})
    missing_trace_roles = sorted(set(non_ego_roles) - set(traced_roles))
    if missing_trace_roles and trace_rows:
        blocking_reasons.append("scenario_actor_trace_missing_roles")
    elif non_ego_roles and not trace_rows:
        warnings.append("scenario_actor_trace_not_available")

    phases = storyboard_phases(storyboard or {})
    phase_ids = [str(phase.get("id")) for phase in phases]
    required_phases = [
        str(phase.get("id"))
        for phase in phases
        if phase.get("id") is not None and bool(phase.get("required", True))
    ]
    required_completion_phases = [
        str(phase.get("id"))
        for phase in phases
        if phase.get("id") is not None and bool(phase.get("required", True)) and _phase_requires_completion(phase)
    ]
    started_phases = sorted(
        {
            str(row.get("phase"))
            for row in event_rows
            if row.get("event") == "phase_started" and row.get("phase") is not None
        }
    )
    completed_phases = sorted(
        {
            str(row.get("phase"))
            for row in event_rows
            if row.get("event") == "phase_completed" and row.get("phase") is not None
        }
    )
    missing_started_phases = sorted(set(phase_ids) - set(started_phases))
    missing_required_phases = sorted(set(required_phases) - set(started_phases))
    missing_completed_required_phases = sorted(set(required_completion_phases) - set(completed_phases))
    stop_before_required_phase_completed = bool(
        event_rows
        and trace_rows
        and missing_completed_required_phases
        and _trace_reaches_stop_condition(storyboard or {}, trace_rows)
    )
    if event_rows and missing_started_phases:
        warnings.append("not_all_phases_observed_started")
    if missing_required_phases and event_rows:
        blocking_reasons.append("fixed_scene_required_phase_not_started")
    if missing_completed_required_phases and event_rows:
        blocking_reasons.append("fixed_scene_required_phase_not_completed")
    if stop_before_required_phase_completed:
        blocking_reasons.append("stop_before_required_phase_completed")
    duration_policy = _duration_policy_check(storyboard or {}, trace_rows)
    warnings.extend(duration_policy["warnings"])
    blocking_reasons.extend(duration_policy["blocking_reasons"])

    if blocking_reasons:
        status = "fail"
    elif missing_artifacts or not storyboard:
        status = "insufficient_data"
    elif warnings:
        status = "warn"
    else:
        status = "pass"

    return {
        "schema_version": FIXED_SCENE_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "scene_id": storyboard.get("scene_id") if storyboard else None,
        "template": storyboard.get("template") if storyboard else None,
        "roles": roles,
        "phase_ids": phase_ids,
        "required_phases": required_phases,
        "required_completion_phases": required_completion_phases,
        "action_types": phase_action_types(storyboard or {}),
        "trace_row_count": len(trace_rows),
        "phase_event_count": len(event_rows),
        "traced_roles": traced_roles,
        "started_phases": started_phases,
        "completed_phases": completed_phases,
        "missing_trace_roles": missing_trace_roles,
        "missing_started_phases": missing_started_phases,
        "missing_required_phases": missing_required_phases,
        "missing_completed_required_phases": missing_completed_required_phases,
        "stop_before_required_phase_completed": stop_before_required_phase_completed,
        "metrics": {
            "required_roles_spawned": bool(non_ego_roles and not missing_trace_roles) if trace_rows else False,
            "phase_completion_ratio": _ratio(len(started_phases), len(phase_ids)),
            "required_phase_start_ratio": _ratio(
                len(set(required_phases) & set(started_phases)),
                len(required_phases),
            ),
            "required_phase_completion_ratio": _ratio(
                len(set(required_completion_phases) & set(completed_phases)),
                len(required_completion_phases),
            ),
            "trace_row_count": len(trace_rows),
            "phase_event_count": len(event_rows),
            "spawn_feasibility_pass": _spawn_feasibility_pass(spawn_feasibility),
            "duration_policy_verified": duration_policy["verified"],
            "lead_route_s_max_m": duration_policy["lead_route_s_max_m"],
        },
        "duration_policy": duration_policy,
        "spawn_feasibility": spawn_feasibility if isinstance(spawn_feasibility, Mapping) else {},
        "missing_artifacts": missing_artifacts,
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "artifact_paths": {
            "storyboard": str(storyboard_path) if storyboard_path else None,
            "trace": str(trace_path) if trace_path else None,
            "phase_events": str(events_path) if events_path else None,
            "runtime_state": str(runtime_state_path) if runtime_state_path else None,
        },
        "interpretation_boundary": (
            "This report validates fixed-scene playback artifacts only. "
            "It does not prove ego natural-driving capability."
        ),
    }


def write_fixed_scene_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "fixed_scene_contract_report.json"
    summary_path = output_dir / "fixed_scene_contract_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_summary(report), encoding="utf-8")
    return {"report": str(report_path), "summary": str(summary_path)}


def _summary(report: Mapping[str, Any]) -> str:
    lines = [
        "# Fixed Scene Contract",
        "",
        f"- Status: {report.get('status')}",
        f"- Scene id: {report.get('scene_id')}",
        f"- Template: {report.get('template')}",
        f"- Roles: {', '.join(report.get('roles') or [])}",
        f"- Trace rows: {report.get('trace_row_count')}",
        f"- Phase events: {report.get('phase_event_count')}",
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


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("JSON payload must be an object")
    return data


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


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def _spawn_feasibility_pass(spawn_feasibility: Any) -> bool | None:
    if not isinstance(spawn_feasibility, Mapping) or not spawn_feasibility:
        return None
    return all(
        isinstance(item, Mapping) and item.get("status") in {"pass", "not_applicable"}
        for item in spawn_feasibility.values()
    )


def _phase_requires_completion(phase: Mapping[str, Any]) -> bool:
    if phase.get("completion_required") is False:
        return False
    return True


def _duration_policy_check(storyboard: Mapping[str, Any], trace_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    params = storyboard.get("params") if isinstance(storyboard.get("params"), Mapping) else {}
    policy = params.get("duration_policy")
    lead_role = str(params.get("lead_role", "lead_vehicle"))
    route_end = _num(params.get("lead_route_end_s_m"))
    route_end_tolerance = _num(params.get("lead_route_end_tolerance_m"))
    if route_end_tolerance is None:
        route_end_tolerance = 0.5
    warnings: list[str] = []
    blocking: list[str] = []
    if policy != "lead_reaches_road_end":
        return {
            "policy": policy,
            "required": False,
            "lead_role": lead_role,
            "lead_route_end_s_m": route_end,
            "lead_route_end_tolerance_m": route_end_tolerance,
            "lead_route_s_max_m": None,
            "lead_route_end_margin_m": None,
            "actor_route_s_available": False,
            "verified": None,
            "warnings": warnings,
            "blocking_reasons": blocking,
        }
    route_values = [
        value
        for value in (
            _num(row.get("route_s"))
            for row in trace_rows
            if str(row.get("actor_role")) == lead_role
        )
        if value is not None
    ]
    route_s_max = max(route_values) if route_values else None
    if route_end is None:
        warnings.append("duration_policy_route_end_missing")
        verified = False
    elif route_s_max is None:
        warnings.append("duration_policy_not_verified_by_actor_route_s")
        verified = False
    margin = (route_s_max - route_end) if route_s_max is not None and route_end is not None else None
    if route_end is None:
        pass
    elif route_s_max is None:
        pass
    elif route_s_max + route_end_tolerance < route_end:
        blocking.append("duration_policy_route_end_not_reached")
        verified = False
    else:
        verified = True
    return {
        "policy": policy,
        "required": True,
        "lead_role": lead_role,
        "lead_route_end_s_m": route_end,
        "lead_route_end_tolerance_m": route_end_tolerance,
        "lead_route_s_max_m": route_s_max,
        "lead_route_end_margin_m": margin,
        "actor_route_s_available": bool(route_values),
        "verified": verified,
        "warnings": warnings,
        "blocking_reasons": blocking,
    }


def _trace_reaches_stop_condition(storyboard: Mapping[str, Any], trace_rows: Sequence[Mapping[str, Any]]) -> bool:
    stop = (storyboard.get("storyboard") or {}).get("stop") if isinstance(storyboard.get("storyboard"), Mapping) else None
    if not trace_rows:
        return False
    time_values = [value for value in (_num(row.get("sim_time_sec")) for row in trace_rows) if value is not None]
    frame_values = [value for value in (_num(row.get("world_frame")) for row in trace_rows) if value is not None]
    route_s_by_role: dict[str, float] = {}
    for row in trace_rows:
        role = str(row.get("actor_role") or "")
        route_s = _num(row.get("route_s"))
        if not role or route_s is None:
            continue
        route_s_by_role[role] = max(route_s_by_role.get(role, route_s), route_s)
    max_time = max(time_values) if time_values else None
    max_frame = max(frame_values) if frame_values else None
    return _stop_condition_reached(
        stop,
        max_time=max_time,
        max_frame=max_frame,
        route_s_by_role=route_s_by_role,
    )


def _stop_condition_reached(
    stop: Any,
    *,
    max_time: float | None,
    max_frame: float | None,
    route_s_by_role: Mapping[str, float] | None = None,
) -> bool:
    if not isinstance(stop, Mapping):
        return False
    if "all" in stop:
        conditions = stop.get("all")
        return bool(isinstance(conditions, list) and conditions and all(
            _stop_condition_reached(
                condition,
                max_time=max_time,
                max_frame=max_frame,
                route_s_by_role=route_s_by_role,
            )
            for condition in conditions
        ))
    if "any" in stop:
        conditions = stop.get("any")
        return bool(isinstance(conditions, list) and any(
            _stop_condition_reached(
                condition,
                max_time=max_time,
                max_frame=max_frame,
                route_s_by_role=route_s_by_role,
            )
            for condition in conditions
        ))
    if stop.get("type") == "all":
        conditions = stop.get("conditions")
        return bool(isinstance(conditions, list) and conditions and all(
            _stop_condition_reached(
                condition,
                max_time=max_time,
                max_frame=max_frame,
                route_s_by_role=route_s_by_role,
            )
            for condition in conditions
        ))
    if stop.get("type") == "any":
        conditions = stop.get("conditions")
        return bool(isinstance(conditions, list) and any(
            _stop_condition_reached(
                condition,
                max_time=max_time,
                max_frame=max_frame,
                route_s_by_role=route_s_by_role,
            )
            for condition in conditions
        ))
    if stop.get("type") == "simulation_time":
        value = _num(stop.get("value", stop.get("gte_s", stop.get("value_s"))))
        return bool(max_time is not None and value is not None and max_time >= value)
    if stop.get("type") == "world_frame":
        value = _num(stop.get("value", stop.get("gte")))
        return bool(max_frame is not None and value is not None and max_frame >= value)
    if stop.get("type") in {"actor_route_s", "ego_route_s", "route_s"}:
        role = "ego" if stop.get("type") == "ego_route_s" else str(stop.get("role", stop.get("actor", "ego")))
        route_s = (route_s_by_role or {}).get(role)
        value = _num(stop.get("value_m", stop.get("value", stop.get("gte_m"))))
        return bool(route_s is not None and value is not None and route_s >= value)
    return False


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
