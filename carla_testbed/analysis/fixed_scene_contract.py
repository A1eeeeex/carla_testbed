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

    phase_ids = [str(phase.get("id")) for phase in storyboard_phases(storyboard or {})]
    started_phases = sorted(
        {
            str(row.get("phase"))
            for row in event_rows
            if row.get("event") == "phase_started" and row.get("phase") is not None
        }
    )
    missing_started_phases = sorted(set(phase_ids) - set(started_phases))
    if event_rows and missing_started_phases:
        warnings.append("not_all_phases_observed_started")

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
        "action_types": phase_action_types(storyboard or {}),
        "trace_row_count": len(trace_rows),
        "phase_event_count": len(event_rows),
        "traced_roles": traced_roles,
        "started_phases": started_phases,
        "missing_trace_roles": missing_trace_roles,
        "missing_started_phases": missing_started_phases,
        "metrics": {
            "required_roles_spawned": bool(non_ego_roles and not missing_trace_roles) if trace_rows else False,
            "phase_completion_ratio": _ratio(len(started_phases), len(phase_ids)),
            "trace_row_count": len(trace_rows),
            "phase_event_count": len(event_rows),
            "spawn_feasibility_pass": _spawn_feasibility_pass(spawn_feasibility),
        },
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
