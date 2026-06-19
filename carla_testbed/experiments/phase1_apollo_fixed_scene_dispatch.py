from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


PHASE1_APOLLO_FIXED_SCENE_DISPATCH_SCHEMA_VERSION = "phase1_apollo_fixed_scene_dispatch.v1"

_ROW_LEVEL_EXPECTED_ARTIFACTS = {
    "artifacts/fixed_scene_runtime_state.json",
    "artifacts/scenario_actor_trace.jsonl",
    "artifacts/scenario_phase_events.jsonl",
    "artifacts/obstacle_gt_contract.jsonl",
    "analysis/fixed_scene_contract/fixed_scene_contract_report.json",
    "analysis/scenario_actor_contract/scenario_actor_contract_report.json",
    "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
}

_POSTPROCESS_EXPECTED_ARTIFACTS = {
    "analysis/v_t_gap/v_t_gap_report.json",
    "analysis/phase1_status/phase1_status.json",
}


def analyze_apollo_fixed_scene_dispatch(
    *,
    backend: str | None = None,
    backend_type: str | None = None,
    scenario_id: str | None = None,
    scenario_class: str | None = None,
    target_actor_contract: Mapping[str, Any] | None = None,
    launch_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Check whether an Apollo fixed-scene RunPlan has a real dispatch contract.

    This is deliberately narrower than online evidence. A pass means the
    LaunchPlan exposes a guarded command and declares the row-level fixed-scene
    artifacts needed after execution. It does not mean the command has run.
    """

    target_contract = dict(target_actor_contract or {})
    expected_artifacts = _list_strings((launch_plan or {}).get("expected_artifacts"))
    commands = (launch_plan or {}).get("commands")
    commands_present = bool(commands)
    starts_runtime = bool((launch_plan or {}).get("starts_runtime"))
    compatibility_source = (launch_plan or {}).get("compatibility_source")
    fixed_scene_required = _target_required(target_contract)
    effective_backend_type = backend_type or ("apollo_reference_backend" if backend == "apollo_cyberrt" else None)
    missing_row_level = sorted(_ROW_LEVEL_EXPECTED_ARTIFACTS.difference(expected_artifacts))
    missing_postprocess = sorted(_POSTPROCESS_EXPECTED_ARTIFACTS.difference(expected_artifacts))
    blocking_reasons: list[str] = []
    warnings: list[str] = []
    next_action: list[str] = []
    status = "not_applicable"
    dispatch_mode = "not_applicable"

    if effective_backend_type != "apollo_reference_backend":
        warnings.append("not_apollo_reference_backend")
    elif not fixed_scene_required:
        warnings.append("fixed_scene_target_not_required")
    elif launch_plan is None:
        status = "insufficient_data"
        dispatch_mode = "launch_plan_missing"
        blocking_reasons.append("launch_plan_missing")
        next_action.append("compile a RunPlan and include its LaunchPlan before fixed-scene Apollo dispatch review")
    else:
        if starts_runtime and commands_present:
            status = "pass"
            dispatch_mode = (
                "guarded_legacy_transition_available"
                if compatibility_source == "phase1_static_follow_stop_legacy_transition"
                else "runtime_command_available"
            )
        else:
            status = "partial"
            dispatch_mode = "runtime_migration_required"
            blocking_reasons.append("apollo_fixed_scene_runtime_migration_required")
            if not starts_runtime:
                blocking_reasons.append("launch_plan_starts_runtime_false")
            if not commands_present:
                blocking_reasons.append("launch_plan_has_no_runtime_command")
            next_action.append(
                "migrate this fixed-scene ScenarioCase to an Apollo runtime path that spawns/records target actors"
            )
        if missing_row_level:
            blocking_reasons.append("launch_plan_missing_row_level_fixed_scene_artifacts")
            next_action.append("declare row-level fixed-scene, obstacle GT, and actor-trace artifacts in LaunchPlan")
        if missing_postprocess:
            warnings.append("launch_plan_missing_phase1_postprocess_artifacts")
        if compatibility_source == "phase1_static_follow_stop_legacy_transition":
            warnings.append("guarded_static_follow_stop_transition_not_generic_fixed_scene_runtime")
        if compatibility_source == "fixed-scene Apollo runtime migration required":
            warnings.append("dynamic_fixed_scene_runtime_not_migrated")
        if status == "pass" and missing_row_level:
            status = "partial"

    return {
        "schema_version": PHASE1_APOLLO_FIXED_SCENE_DISPATCH_SCHEMA_VERSION,
        "backend": backend,
        "backend_type": effective_backend_type,
        "scenario_id": scenario_id,
        "scenario_class": scenario_class,
        "target_actor_contract": target_contract,
        "status": status,
        "dispatch_mode": dispatch_mode,
        "starts_runtime": starts_runtime,
        "commands_present": commands_present,
        "compatibility_source": compatibility_source,
        "expected_artifact_count": len(expected_artifacts),
        "required_row_level_artifacts": sorted(_ROW_LEVEL_EXPECTED_ARTIFACTS),
        "required_postprocess_artifacts": sorted(_POSTPROCESS_EXPECTED_ARTIFACTS),
        "missing_row_level_expected_artifacts": missing_row_level,
        "missing_postprocess_expected_artifacts": missing_postprocess,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "warnings": sorted(set(warnings)),
        "next_action": _unique(next_action),
        "claim_boundary": (
            "Dispatch contract evidence is not online behavior evidence. "
            "Apollo fixed-scene rows still need an executed run with target actor, "
            "obstacle GT, v-t-gap, phase1_status, and comparison artifacts."
        ),
    }


def write_apollo_fixed_scene_dispatch_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "phase1_apollo_fixed_scene_dispatch_report.json"
    summary_path = output / "phase1_apollo_fixed_scene_dispatch_summary.md"
    payload = dict(report)
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_summary_markdown(payload), encoding="utf-8")
    return {
        "phase1_apollo_fixed_scene_dispatch_report": str(report_path),
        "phase1_apollo_fixed_scene_dispatch_summary": str(summary_path),
    }


def write_apollo_fixed_scene_dispatch_for_plan(
    plan: Any,
    launch_plan: Mapping[str, Any],
    run_dir: str | Path,
    *,
    target_actor_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    backend = getattr(getattr(plan, "platform", None), "name", None)
    report = analyze_apollo_fixed_scene_dispatch(
        backend=str(backend) if backend else None,
        backend_type="apollo_reference_backend" if backend == "apollo_cyberrt" else None,
        scenario_id=getattr(getattr(plan, "scenario", None), "scenario_id", None),
        scenario_class=getattr(getattr(plan, "scenario", None), "scenario_class", None),
        target_actor_contract=target_actor_contract,
        launch_plan=launch_plan,
    )
    paths = write_apollo_fixed_scene_dispatch_report(
        report,
        Path(run_dir).expanduser() / "analysis" / "phase1_apollo_fixed_scene_dispatch",
    )
    return {"status": report.get("status"), "paths": paths, "report": report}


def _target_required(contract: Mapping[str, Any]) -> bool:
    if contract.get("required") is False or contract.get("status") == "not_required":
        return False
    return bool(contract.get("target_actor_role") or contract.get("role"))


def _list_strings(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if str(item)]


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Apollo Fixed-Scene Dispatch Contract",
        "",
        f"- Status: {report.get('status')}",
        f"- Dispatch mode: {report.get('dispatch_mode')}",
        f"- Starts runtime: {report.get('starts_runtime')}",
        f"- Commands present: {report.get('commands_present')}",
        f"- Compatibility source: {report.get('compatibility_source')}",
        f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
        f"- Warnings: {', '.join(report.get('warnings') or []) or 'none'}",
        "",
        "## Missing Expected Artifacts",
        "",
        f"- Row-level: {', '.join(report.get('missing_row_level_expected_artifacts') or []) or 'none'}",
        f"- Postprocess: {', '.join(report.get('missing_postprocess_expected_artifacts') or []) or 'none'}",
        "",
        "## Next Action",
        "",
    ]
    actions = report.get("next_action") or []
    if actions:
        lines.extend(f"- {item}" for item in actions)
    else:
        lines.append("- Execute the guarded runtime path and collect online fixed-scene evidence before comparison.")
    lines.extend(["", str(report.get("claim_boundary") or "")])
    return "\n".join(lines).rstrip() + "\n"
