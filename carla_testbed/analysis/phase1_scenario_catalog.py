from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

import yaml

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import extract_template_config
from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract

PHASE1_SCENARIO_CATALOG_SCHEMA_VERSION = "phase1_scenario_catalog.v1"
STATUS_DONE = "DONE"
STATUS_PARTIAL = "PARTIAL"
STATUS_NOT_YET = "NOT_YET"
STATUS_UNKNOWN = "UNKNOWN"

P0_SCENARIOS = [
    "follow_stop_static",
    "lead_decel_accel",
    "cut_in_simple",
    "lane_keep_straight",
    "lane_keep_curve",
]
P1_SCENARIOS = [
    "cut_out_simple",
    "junction_turn_no_signal",
    "lead_hard_brake",
]

SCENARIO_CLASS_ALIASES = {
    "follow_stop_static": {"follow_stop_static", "static_lead_stop", "follow_stop_097"},
    "lead_decel_accel": {
        "lead_accel",
        "lead_decel",
        "lead_decel_accel",
        "lead_vehicle_accel",
        "lead_vehicle_decel",
        "lead_vehicle_accel_decel",
    },
    "cut_in_simple": {"cut_in", "cut_in_simple"},
    "lane_keep_straight": {"lane_keep", "lane_keep_straight"},
    "lane_keep_curve": {"curve_diagnostic", "curve217_diagnostic", "curve213_diagnostic", "lane_keep_curve"},
    "cut_out_simple": {"cut_out", "cut_out_simple"},
    "junction_turn_no_signal": {"junction", "junction_turn", "junction_turn_no_signal"},
    "lead_hard_brake": {"lead_hard_brake"},
}


def analyze_phase1_scenario_catalog(
    repo_root: str | Path = ".",
    *,
    evidence_root: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    evidence_base = Path(evidence_root).expanduser().resolve() if evidence_root else None
    scenario_files = sorted((root / "configs" / "scenarios").glob("**/*.yaml"))
    scenario_payloads = [_load_yaml(path) for path in scenario_files]
    entries = [
        _catalog_entry(root, scenario_id, "P0", scenario_files, scenario_payloads, evidence_root=evidence_base)
        for scenario_id in P0_SCENARIOS
    ]
    entries.extend(
        _catalog_entry(root, scenario_id, "P1", scenario_files, scenario_payloads, evidence_root=evidence_base)
        for scenario_id in P1_SCENARIOS
    )
    return {
        "schema_version": PHASE1_SCENARIO_CATALOG_SCHEMA_VERSION,
        "repo_root": str(root),
        "evidence_root": str(evidence_base) if evidence_base else str(root / "runs"),
        "p0_scenarios": list(P0_SCENARIOS),
        "p1_scenarios": list(P1_SCENARIOS),
        "scenarios": entries,
        "summary": {
            "total": len(entries),
            "done": sum(1 for item in entries if item["overall_status"] == STATUS_DONE),
            "partial": sum(1 for item in entries if item["overall_status"] == STATUS_PARTIAL),
            "not_yet": sum(1 for item in entries if item["overall_status"] == STATUS_NOT_YET),
            "unknown": sum(1 for item in entries if item["overall_status"] == STATUS_UNKNOWN),
        },
    }


def write_phase1_scenario_catalog(report: Mapping[str, Any], out: str | Path) -> dict[str, str]:
    out_path = Path(out).expanduser()
    if out_path.suffix.lower() == ".json":
        json_path = out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_path.mkdir(parents=True, exist_ok=True)
        json_path = out_path / "phase1_scenario_catalog.json"
    md_path = json_path.with_suffix(".md")
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_catalog_markdown(report), encoding="utf-8")
    return {"json": str(json_path), "summary": str(md_path)}


def _catalog_entry(
    root: Path,
    canonical_id: str,
    priority: str,
    scenario_files: list[Path],
    scenario_payloads: list[dict[str, Any]],
    *,
    evidence_root: Path | None = None,
) -> dict[str, Any]:
    aliases = SCENARIO_CLASS_ALIASES[canonical_id]
    matched: list[tuple[Path, dict[str, Any]]] = []
    for path, payload in zip(scenario_files, scenario_payloads):
        scenario_class = str(payload.get("scenario_class") or "")
        scenario_id = str(payload.get("scenario_id") or "")
        if scenario_class in aliases or any(alias in scenario_id for alias in aliases):
            matched.append((path, payload))

    evidence_paths = [str(path.relative_to(root)) for path, _ in matched]
    scenario_case_ids = [str(payload.get("scenario_id") or path.stem) for path, payload in matched]
    evidence = [_evidence(path, root, "case_yaml") for path, _ in matched]
    online = _discover_online_evidence(root, aliases, evidence_root=evidence_root)
    evidence.extend(online["evidence"])
    missing_items: list[str] = []
    if not matched:
        return {
            "scenario": canonical_id,
            "priority": priority,
            "status": STATUS_NOT_YET,
            "overall_status": STATUS_NOT_YET,
            "case_yaml_status": STATUS_NOT_YET,
            "template_status": STATUS_NOT_YET,
            "fixed_scene_compile_status": STATUS_NOT_YET,
            "target_actor_status": "missing",
            "carla_online_status": online["carla_online_status"],
            "apollo_online_status": online["apollo_online_status"],
            "apollo_fixed_scene_readiness_status": online["apollo_fixed_scene_readiness_status"],
            "apollo_fixed_scene_readiness": online["apollo_fixed_scene_readiness"],
            "apollo_fixed_scene_dispatch_contract_status": online["apollo_fixed_scene_dispatch_contract_status"],
            "apollo_fixed_scene_dispatch_contract": online["apollo_fixed_scene_dispatch_contract"],
            "apollo_fixed_scene_dispatch_mode": online["apollo_fixed_scene_dispatch_mode"],
            "apollo_fixed_scene_runtime_dispatch_status": STATUS_NOT_YET,
            "apollo_fixed_scene_runtime_dispatch_reason": None,
            "v_t_gap_status": online["v_t_gap_status"],
            "v_t_gap_readiness": STATUS_NOT_YET,
            "comparison_status": online["comparison_status"],
            "comparison_target_status": online["comparison_target_status"],
            "comparison_readiness": STATUS_NOT_YET,
            "evidence": _sort_evidence(online["evidence"]),
            "representative_evidence": _representative_evidence(online["evidence"]),
            "scenario_case_ids": [],
            "case_files": [],
            "target_actor_contract": None,
            "evidence_paths": [],
            "missing": ["scenario_case_yaml"],
            "missing_items": ["scenario_case_yaml"],
            "next_action": _catalog_next_actions(
                canonical_id,
                ["scenario_case_yaml"],
                online=online,
                has_fixed_scene=False,
                evidence=online["evidence"],
            ),
            "notes": ["YAML existence is required but not sufficient for Phase 1 readiness."],
        }

    compile_status = STATUS_NOT_YET
    target_status = "not_required"
    target_contract: dict[str, Any] | None = None
    compile_errors: list[str] = []
    has_fixed_scene = any(isinstance(payload.get("fixed_scene"), Mapping) for _, payload in matched)
    for _, payload in matched:
        if isinstance(payload.get("fixed_scene"), Mapping):
            try:
                template = extract_template_config(payload)
                storyboard = compile_fixed_scene_template(template)
                compile_status = STATUS_DONE
                raw_target = storyboard.get("target_actor_contract")
                target_contract = dict(raw_target) if isinstance(raw_target, Mapping) else None
                target_status = str((target_contract or {}).get("status") or "missing")
                evidence.append(_evidence(matched[0][0], root, "fixed_scene_compile"))
                break
            except Exception as exc:
                compile_status = STATUS_UNKNOWN
                compile_errors.append(f"{type(exc).__name__}: {exc}")
        else:
            target_contract = resolve_target_actor_contract(payload)
            target_status = str(target_contract.get("status") or "not_required")

    if compile_status == STATUS_NOT_YET and has_fixed_scene:
        compile_status = STATUS_UNKNOWN
    if compile_status == STATUS_NOT_YET and _route_only_scenario(canonical_id):
        compile_status = STATUS_DONE
    if compile_status != STATUS_DONE and not _route_only_scenario(canonical_id):
        missing_items.append("fixed_scene_compile")
    if target_status == "missing":
        missing_items.append("target_actor")
    v_t_gap_readiness = _phase1_readiness_from_status(online["v_t_gap_status"])
    apollo_readiness = online["apollo_fixed_scene_readiness"]
    apollo_dispatch_contract = (
        online["apollo_fixed_scene_dispatch_contract"] if has_fixed_scene else "not_applicable"
    )
    apollo_dispatch_contract_status = (
        online["apollo_fixed_scene_dispatch_contract_status"] if has_fixed_scene else "not_applicable"
    )
    apollo_dispatch_mode = online["apollo_fixed_scene_dispatch_mode"] if has_fixed_scene else None
    apollo_runtime_dispatch = (
        online["apollo_fixed_scene_runtime_dispatch_status"] if has_fixed_scene else "not_applicable"
    )
    apollo_runtime_dispatch_reason = (
        online["apollo_fixed_scene_runtime_dispatch_reason"] if has_fixed_scene else None
    )
    comparison_readiness = _comparison_readiness(online)
    if online["carla_online_status"] != STATUS_DONE:
        missing_items.append("carla_online_evidence")
    if online["apollo_online_status"] != STATUS_DONE:
        missing_items.append("apollo_online_evidence")
    if has_fixed_scene and apollo_readiness == STATUS_NOT_YET:
        missing_items.append("apollo_fixed_scene_readiness_report")
    elif has_fixed_scene and apollo_readiness != STATUS_DONE:
        missing_items.append("apollo_fixed_scene_not_ready")
    if has_fixed_scene and apollo_runtime_dispatch != STATUS_DONE:
        missing_items.append("apollo_fixed_scene_runtime_dispatch")
    if v_t_gap_readiness == STATUS_NOT_YET:
        missing_items.append("v_t_gap_extractor_output")
    if comparison_readiness == STATUS_NOT_YET:
        missing_items.append("scenario_comparison_report")
    elif comparison_readiness != STATUS_DONE:
        missing_items.append("cross_backend_scenario_comparison")
    overall = STATUS_PARTIAL
    if (
        compile_status == STATUS_DONE
        and target_status in {"resolved", "not_required"}
        and online["carla_online_status"] == STATUS_DONE
        and online["apollo_online_status"] == STATUS_DONE
        and (not has_fixed_scene or apollo_readiness == STATUS_DONE)
        and (not has_fixed_scene or apollo_runtime_dispatch == STATUS_DONE)
        and v_t_gap_readiness == STATUS_DONE
        and comparison_readiness == STATUS_DONE
    ):
        overall = STATUS_DONE
    elif compile_status == STATUS_DONE and target_status in {"resolved", "not_required"}:
        overall = STATUS_PARTIAL

    missing = sorted(set(missing_items))
    evidence = _sort_evidence(evidence)
    next_action = _catalog_next_actions(
        canonical_id,
        missing,
        online=online,
        has_fixed_scene=has_fixed_scene,
        evidence=evidence,
    )
    return {
        "scenario": canonical_id,
        "priority": priority,
        "status": overall,
        "overall_status": overall,
        "case_yaml_status": STATUS_DONE,
        "template_status": compile_status,
        "fixed_scene_compile_status": compile_status,
        "target_actor_status": target_status,
        "carla_online_status": online["carla_online_status"],
        "apollo_online_status": online["apollo_online_status"],
        "apollo_fixed_scene_readiness_status": online["apollo_fixed_scene_readiness_status"],
        "apollo_fixed_scene_readiness": apollo_readiness,
        "apollo_fixed_scene_dispatch_contract_status": apollo_dispatch_contract_status,
        "apollo_fixed_scene_dispatch_contract": apollo_dispatch_contract,
        "apollo_fixed_scene_dispatch_mode": apollo_dispatch_mode,
        "apollo_fixed_scene_runtime_dispatch_status": apollo_runtime_dispatch,
        "apollo_fixed_scene_runtime_dispatch_reason": apollo_runtime_dispatch_reason,
        "v_t_gap_status": online["v_t_gap_status"],
        "v_t_gap_readiness": v_t_gap_readiness,
        "comparison_status": online["comparison_status"],
        "comparison_target_status": online["comparison_target_status"],
        "comparison_readiness": comparison_readiness,
        "scenario_case_ids": scenario_case_ids,
        "case_files": evidence_paths,
        "target_actor_contract": target_contract,
        "evidence": evidence,
        "representative_evidence": _representative_evidence(evidence),
        "evidence_paths": evidence_paths,
        "missing": missing,
        "missing_items": missing,
        "next_action": next_action,
        "compile_errors": compile_errors,
        "notes": _catalog_notes(canonical_id, has_fixed_scene),
    }


def _catalog_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Scenario Catalog",
        "",
        f"Schema: `{report.get('schema_version')}`",
        "",
        "| Scenario | Case IDs | Priority | Status | Case YAML | Template | Target | Target role | CARLA online | Apollo online | Apollo readiness | Apollo dispatch contract | Apollo runtime dispatch | v-t-gap | Comparison | Comparison target | Missing | Next action |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report.get("scenarios") or []:
        if not isinstance(item, Mapping):
            continue
        missing = ", ".join(str(value) for value in item.get("missing_items") or []) or "-"
        next_action = "; ".join(str(value) for value in item.get("next_action") or []) or "-"
        case_ids = ", ".join(str(value) for value in item.get("scenario_case_ids") or []) or "-"
        target_contract = item.get("target_actor_contract") if isinstance(item.get("target_actor_contract"), Mapping) else {}
        lines.append(
            "| {scenario} | {case_ids} | {priority} | {overall} | {case} | {template} | {target} | {target_role} | {carla} | {apollo} | {apollo_readiness} | {apollo_dispatch_contract} | {apollo_runtime} | {vtgap} | {comparison} | {comparison_target} | {missing} | {next_action} |".format(
                scenario=item.get("scenario"),
                case_ids=case_ids,
                priority=item.get("priority"),
                overall=item.get("overall_status"),
                case=item.get("case_yaml_status"),
                template=item.get("template_status") or item.get("fixed_scene_compile_status"),
                target=item.get("target_actor_status"),
                target_role=target_contract.get("target_actor_role") or "-",
                carla=item.get("carla_online_status"),
                apollo=item.get("apollo_online_status"),
                apollo_readiness=item.get("apollo_fixed_scene_readiness"),
                apollo_dispatch_contract=_apollo_dispatch_contract_markdown(item),
                apollo_runtime=_apollo_runtime_markdown(item),
                vtgap=item.get("v_t_gap_status"),
                comparison=item.get("comparison_status"),
                comparison_target=item.get("comparison_target_status"),
                missing=missing,
                next_action=next_action,
            )
        )
    lines.append("")
    lines.append("YAML presence is evidence for case definition only; it is not online playback or backend comparison evidence.")
    lines.append("")
    lines.append("## Representative Evidence")
    lines.append("")
    lines.append(
        "Representative evidence is selected per evidence type by readiness first and newest timestamp second. "
        "It is for review navigation only; scenario status still follows the gate columns above."
    )
    lines.append("")
    for item in report.get("scenarios") or []:
        if not isinstance(item, Mapping):
            continue
        representatives = item.get("representative_evidence")
        if not isinstance(representatives, Mapping) or not representatives:
            continue
        lines.append(f"### {item.get('scenario')}")
        lines.append("")
        for evidence_type, evidence_item in representatives.items():
            if not isinstance(evidence_item, Mapping):
                continue
            note = evidence_item.get("note")
            note_text = f" - {note}" if note else ""
            lines.append(
                f"- `{evidence_type}`: `{evidence_item.get('status')}` "
                f"`{evidence_item.get('path')}`{note_text}"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def _catalog_next_actions(
    canonical_id: str,
    missing: list[str],
    *,
    online: Mapping[str, Any],
    has_fixed_scene: bool,
    evidence: list[Mapping[str, Any]],
) -> list[str]:
    actions: list[str] = []
    missing_set = set(missing)
    if "scenario_case_yaml" in missing_set:
        actions.append("add a scenario_case YAML before collecting online evidence")
    if "fixed_scene_compile" in missing_set:
        actions.append("fix fixed_scene template compilation before runtime validation")
    if "target_actor" in missing_set:
        actions.append("declare the Phase 1 target actor explicitly or via fixed_scene role")
    if "carla_online_evidence" in missing_set:
        actions.append("run the CARLA builtin PlanningControlBackend sample and postprocess it")
    if "v_t_gap_extractor_output" in missing_set:
        actions.append("generate v-t-gap from timeseries and target actor trace")
    if "apollo_fixed_scene_readiness_report" in missing_set:
        actions.append("generate Apollo fixed-scene readiness evidence for the target role")
    if "apollo_fixed_scene_not_ready" in missing_set:
        actions.append("fix Apollo fixed-scene target-obstacle/readiness blockers before online dispatch")
    if "apollo_fixed_scene_runtime_dispatch" in missing_set:
        actions.append(
            "produce an evaluable Apollo fixed-scene runtime dispatch; "
            "backend_not_ready remains invalid, not a backend loss"
        )
    if "apollo_online_evidence" in missing_set:
        if has_fixed_scene:
            actions.append(
                "collect an evaluable ApolloBackend fixed-scene run with target actor, "
                "obstacle GT, and Phase 1 postprocess artifacts"
            )
        else:
            actions.append("collect an evaluable ApolloBackend route-only run with Phase 1 postprocess artifacts")
    if "scenario_comparison_report" in missing_set:
        actions.append("write a ScenarioComparison report after at least two backend runs exist")
    if "cross_backend_scenario_comparison" in missing_set:
        actions.append("compare only evaluable ApolloBackend and PlanningControlBackend runs for the same ScenarioCase")
    primary_blocker = _representative_apollo_blocker(evidence)
    if primary_blocker:
        actions.append(f"current representative Apollo blocker: {primary_blocker}")
    if not actions:
        actions.append("keep representative evidence current; DONE does not imply every backend behavior passed")
    return list(dict.fromkeys(actions))


def _representative_apollo_blocker(evidence: list[Mapping[str, Any]]) -> str | None:
    for item in _sort_evidence([dict(value) for value in evidence]):
        if item.get("evidence_type") != "comparison_online":
            continue
        for row in item.get("backend_results") or []:
            if not isinstance(row, Mapping):
                continue
            if row.get("backend") != "apollo_cyberrt":
                continue
            blocker = row.get("apollo_link_primary_blocker")
            if blocker:
                return str(blocker)
            failure = row.get("failure_reason")
            if failure:
                return str(failure)
    for item in _sort_evidence([dict(value) for value in evidence]):
        if item.get("evidence_type") != "Apollo_online":
            continue
        failure = item.get("failure_reason")
        if failure:
            return str(failure)
    return None


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _discover_online_evidence(
    root: Path,
    aliases: set[str],
    *,
    evidence_root: Path | None = None,
) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    carla_online_status = STATUS_NOT_YET
    apollo_online_status = STATUS_NOT_YET
    apollo_fixed_scene_readiness_status: str = STATUS_NOT_YET
    apollo_fixed_scene_readiness = STATUS_NOT_YET
    apollo_fixed_scene_dispatch_contract_status: str = STATUS_NOT_YET
    apollo_fixed_scene_dispatch_contract = STATUS_NOT_YET
    apollo_fixed_scene_dispatch_mode: str | None = None
    apollo_fixed_scene_runtime_dispatch_status = STATUS_NOT_YET
    apollo_fixed_scene_runtime_dispatch_reason: str | None = None
    v_t_gap_status: str = STATUS_NOT_YET
    comparison_status: str = STATUS_NOT_YET
    comparison_target_status: str = STATUS_NOT_YET
    comparison_backend_count = 0
    best_comparison_rank = (-1, -1, -1, 0)

    runs_dir = evidence_root or (root / "runs")
    if runs_dir.exists():
        for manifest_path in _safe_find_files(runs_dir, "manifest.json"):
            if "comparisons" in manifest_path.parts:
                continue
            run_dir = manifest_path.parent
            manifest = _read_json(manifest_path)
            if not _matches_scenario(manifest, aliases, run_dir.name):
                continue
            backend = str(manifest.get("backend") or manifest.get("backend_name") or "")
            phase1_status = _read_json(run_dir / "analysis" / "phase1_status" / "phase1_status.json")
            phase1_state = str(phase1_status.get("status") or "")
            phase1_evaluable = phase1_status.get("evaluable")
            evidence_status = (
                STATUS_DONE
                if phase1_status and phase1_state != "invalid" and phase1_evaluable is not False
                else STATUS_PARTIAL
            )
            evidence_type = "online_run"
            if backend == "carla_builtin":
                evidence_type = "CARLA_online"
                carla_online_status = _best_readiness(carla_online_status, evidence_status)
            elif backend == "apollo_cyberrt":
                evidence_type = "Apollo_online"
                apollo_online_status = _best_readiness(apollo_online_status, evidence_status)
                dispatch_status, dispatch_reason = _apollo_fixed_scene_runtime_dispatch_from_run(
                    run_dir,
                    phase1_status,
                )
                if _readiness_better(apollo_fixed_scene_runtime_dispatch_status, dispatch_status):
                    apollo_fixed_scene_runtime_dispatch_status = dispatch_status
                    apollo_fixed_scene_runtime_dispatch_reason = dispatch_reason
                elif (
                    dispatch_status == apollo_fixed_scene_runtime_dispatch_status
                    and apollo_fixed_scene_runtime_dispatch_reason is None
                    and dispatch_reason
                ):
                    apollo_fixed_scene_runtime_dispatch_reason = dispatch_reason
            evidence.append(
                _evidence(
                    run_dir,
                    root,
                    evidence_type,
                    status=evidence_status,
                    note=_run_evidence_note(backend, phase1_status, run_dir),
                    details=_run_evidence_details(phase1_status),
                )
            )
            readiness_report = _read_json(
                run_dir
                / "analysis"
                / "phase1_apollo_fixed_scene_readiness"
                / "phase1_apollo_fixed_scene_readiness_report.json"
            )
            if readiness_report:
                raw_readiness_status = str(readiness_report.get("status") or "unknown")
                readiness = _phase1_readiness_from_status(raw_readiness_status)
                apollo_fixed_scene_readiness_status = _best_raw_readiness_status(
                    apollo_fixed_scene_readiness_status,
                    raw_readiness_status,
                )
                apollo_fixed_scene_readiness = _best_readiness(apollo_fixed_scene_readiness, readiness)
                evidence.append(
                    _evidence(
                        run_dir
                        / "analysis"
                        / "phase1_apollo_fixed_scene_readiness"
                        / "phase1_apollo_fixed_scene_readiness_report.json",
                        root,
                        "apollo_fixed_scene_readiness",
                        status=readiness,
                        note=_readiness_evidence_note(readiness_report),
                        details=_readiness_evidence_details(readiness_report),
                    )
                )
            dispatch_report = _read_json(
                run_dir
                / "analysis"
                / "phase1_apollo_fixed_scene_dispatch"
                / "phase1_apollo_fixed_scene_dispatch_report.json"
            )
            if dispatch_report:
                raw_dispatch_status = str(dispatch_report.get("status") or "unknown")
                dispatch_contract = _phase1_readiness_from_status(raw_dispatch_status)
                dispatch_better = _readiness_better(
                    apollo_fixed_scene_dispatch_contract,
                    dispatch_contract,
                )
                apollo_fixed_scene_dispatch_contract_status = _best_raw_readiness_status(
                    apollo_fixed_scene_dispatch_contract_status,
                    raw_dispatch_status,
                )
                apollo_fixed_scene_dispatch_contract = _best_readiness(
                    apollo_fixed_scene_dispatch_contract,
                    dispatch_contract,
                )
                if apollo_fixed_scene_dispatch_mode is None or dispatch_better:
                    apollo_fixed_scene_dispatch_mode = str(dispatch_report.get("dispatch_mode") or "")
                evidence.append(
                    _evidence(
                        run_dir
                        / "analysis"
                        / "phase1_apollo_fixed_scene_dispatch"
                        / "phase1_apollo_fixed_scene_dispatch_report.json",
                        root,
                        "apollo_fixed_scene_dispatch_contract",
                        status=dispatch_contract,
                        note=_dispatch_contract_evidence_note(dispatch_report),
                        details=_dispatch_contract_evidence_details(dispatch_report),
                    )
                )
            gap_report = _read_json(run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json")
            if gap_report:
                raw_gap_status = str(gap_report.get("status") or "unknown")
                v_t_gap_status = _best_raw_readiness_status(v_t_gap_status, raw_gap_status)
                evidence.append(
                    _evidence(
                        run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json",
                        root,
                        "v_t_gap",
                        status=_phase1_readiness_from_status(raw_gap_status),
                    )
                )

    if runs_dir.exists():
        for summary_path in _safe_find_files(runs_dir, "comparison_summary.json"):
            summary = _read_json(summary_path)
            if not _matches_scenario(summary, aliases, summary_path.parent.name):
                continue
            raw_comparison_status = str(summary.get("comparison_status") or "unknown")
            raw_comparison_target_status = str(summary.get("comparison_target_status") or "unknown_backend_coverage")
            safety_issue = _comparison_safety_event_issue(summary)
            safety_context_issue = _comparison_safety_context_issue(summary)
            comparison_status_for_catalog = raw_comparison_status
            comparison_target_for_catalog = raw_comparison_target_status
            if safety_issue and raw_comparison_status == "comparable":
                comparison_status_for_catalog = "partially_evaluable"
                comparison_target_for_catalog = "safety_event_evidence_mismatch"
            if safety_context_issue and raw_comparison_status == "comparable":
                comparison_status_for_catalog = "partially_evaluable"
                comparison_target_for_catalog = "shared_safety_event_context_issue"
            backends = {
                str(run.get("backend"))
                for run in summary.get("participating_runs") or []
                if isinstance(run, Mapping) and run.get("backend")
            }
            missing_hint = _comparison_missing_hint(summary)
            if safety_issue:
                missing_hint = ",".join(filter(None, [missing_hint, safety_issue]))
            if safety_context_issue:
                missing_hint = ",".join(filter(None, [missing_hint, safety_context_issue]))
            comparison_note = _comparison_evidence_note(
                comparison_status_for_catalog,
                comparison_target_for_catalog,
                backend_count=len(backends),
                missing_hint=missing_hint,
                summary=summary,
            )
            comparison_backend_count = max(comparison_backend_count, len(backends))
            candidate_rank = _comparison_summary_rank(
                comparison_status_for_catalog,
                comparison_target_for_catalog,
                backend_count=len(backends),
                path_text=str(summary_path),
            )
            if candidate_rank > best_comparison_rank:
                best_comparison_rank = candidate_rank
                comparison_status = comparison_status_for_catalog
                comparison_target_status = comparison_target_for_catalog
            evidence.append(
                _evidence(
                    summary_path,
                    root,
                    "comparison_online",
                    status=(
                        STATUS_DONE
                        if comparison_status_for_catalog == "comparable"
                        and comparison_target_for_catalog == "apollo_vs_planning_control_evaluable"
                        else STATUS_PARTIAL
                    ),
                    note=comparison_note,
                    details=_comparison_evidence_details(summary),
                )
            )

    return {
        "evidence": evidence,
        "carla_online_status": carla_online_status,
        "apollo_online_status": apollo_online_status,
        "apollo_fixed_scene_readiness_status": apollo_fixed_scene_readiness_status,
        "apollo_fixed_scene_readiness": apollo_fixed_scene_readiness,
        "apollo_fixed_scene_dispatch_contract_status": apollo_fixed_scene_dispatch_contract_status,
        "apollo_fixed_scene_dispatch_contract": apollo_fixed_scene_dispatch_contract,
        "apollo_fixed_scene_dispatch_mode": apollo_fixed_scene_dispatch_mode,
        "apollo_fixed_scene_runtime_dispatch_status": apollo_fixed_scene_runtime_dispatch_status,
        "apollo_fixed_scene_runtime_dispatch_reason": apollo_fixed_scene_runtime_dispatch_reason,
        "v_t_gap_status": v_t_gap_status,
        "comparison_status": comparison_status,
        "comparison_target_status": comparison_target_status,
        "comparison_backend_count": comparison_backend_count,
    }


def _evidence(
    path: Path,
    root: Path,
    evidence_type: str,
    *,
    status: str = STATUS_DONE,
    note: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        path_text = str(path.relative_to(root))
    except ValueError:
        path_text = str(path)
    item: dict[str, Any] = {"path": path_text, "evidence_type": evidence_type, "status": status}
    if note:
        item["note"] = note
    if details:
        item.update(dict(details))
    return item


def _sort_evidence(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(evidence, key=_evidence_sort_key)


def _representative_evidence(evidence: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    representatives: dict[str, dict[str, Any]] = {}
    for item in _sort_evidence(evidence):
        evidence_type = str(item.get("evidence_type") or "unknown")
        if evidence_type not in representatives:
            representatives[evidence_type] = dict(item)
    return representatives


def _evidence_sort_key(item: Mapping[str, Any]) -> tuple[int, int, int, int, str]:
    type_order = {
        "case_yaml": 0,
        "fixed_scene_compile": 1,
        "CARLA_online": 2,
        "Apollo_online": 3,
        "apollo_fixed_scene_readiness": 4,
        "apollo_fixed_scene_dispatch_contract": 5,
        "v_t_gap": 6,
        "comparison_online": 7,
    }
    status_order = {
        STATUS_DONE: 0,
        "pass": 0,
        "warn": 0,
        "not_applicable": 0,
        STATUS_PARTIAL: 1,
        STATUS_UNKNOWN: 2,
        STATUS_NOT_YET: 3,
    }
    path_text = str(item.get("path") or "")
    return (
        type_order.get(str(item.get("evidence_type") or ""), 99),
        status_order.get(str(item.get("status") or ""), 4),
        _phase1_run_status_rank(str(item.get("phase1_run_status") or "")),
        -_timestamp_rank(path_text),
        path_text,
    )


def _timestamp_rank(path_text: str) -> int:
    matches = re.findall(r"20\d{6}_\d{6}", path_text)
    if not matches:
        return 0
    return max(int(match.replace("_", "")) for match in matches)


def _phase1_run_status_rank(status: str) -> int:
    return {
        "success": 0,
        "degraded": 1,
        "failed": 2,
        "invalid": 3,
        "missing": 4,
        "": 5,
    }.get(status, 5)


def _run_evidence_note(backend: str, phase1_status: Mapping[str, Any], run_dir: Path) -> str:
    parts = [
        f"backend={backend or 'unknown'}",
        f"phase1_status={phase1_status.get('status') or 'missing'}",
    ]
    failure_reason = phase1_status.get("failure_reason")
    if failure_reason:
        parts.append(f"failure_reason={failure_reason}")
    invalid_reasons = _short_list(phase1_status.get("invalid_reasons"))
    if invalid_reasons:
        parts.append(f"invalid_reasons={','.join(invalid_reasons)}")
    preflight_reasons = _short_list(phase1_status.get("preflight_reasons"))
    if preflight_reasons:
        parts.append(f"preflight_reasons={','.join(preflight_reasons)}")
    missing_expected = _short_list(phase1_status.get("missing_expected_artifacts"))
    if missing_expected:
        parts.append(f"missing_expected_artifacts={','.join(missing_expected)}")
    else:
        preflight = _read_json(run_dir / "preflight.json")
        preflight_missing = _short_list(preflight.get("missing_expected_artifacts"))
        if preflight_missing:
            parts.append(f"preflight_missing_expected_artifacts={','.join(preflight_missing)}")
    return "; ".join(parts)


def _run_evidence_details(phase1_status: Mapping[str, Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    details["phase1_run_status"] = str(phase1_status.get("status") or "missing")
    for key in (
        "failure_reason",
        "invalid_reasons",
        "degraded_reasons",
        "failed_reasons",
        "missing_expected_artifacts",
        "expected_artifacts",
        "preflight_reasons",
    ):
        value = phase1_status.get(key)
        if value:
            details[key] = value
    if "evaluable" in phase1_status:
        details["evaluable"] = phase1_status.get("evaluable")
    return details


def _readiness_evidence_note(report: Mapping[str, Any]) -> str:
    parts = [f"readiness_status={report.get('status') or 'missing'}"]
    blocking = _short_list(report.get("blocking_reasons"))
    if blocking:
        parts.append(f"blocking_reasons={','.join(blocking)}")
    warnings = _short_list(report.get("warnings"), limit=3)
    if warnings:
        parts.append(f"warnings={','.join(warnings)}")
    bridge_roles = report.get("front_obstacle_behavior", {}).get("role_names") if isinstance(report.get("front_obstacle_behavior"), Mapping) else None
    if isinstance(bridge_roles, list):
        parts.append(f"bridge_roles={','.join(str(item) for item in bridge_roles)}")
    target_role = report.get("target_actor_role")
    if target_role:
        parts.append(f"target_role={target_role}")
    return "; ".join(parts)


def _readiness_evidence_details(report: Mapping[str, Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    for key in (
        "blocking_reasons",
        "warnings",
        "target_actor_role",
        "target_role_covered_by_bridge_roles",
        "actor_probe_enabled_effective",
        "bridge_config_source",
        "bridge_config_path",
        "next_action",
    ):
        value = report.get(key)
        if value not in (None, [], {}):
            details[key] = value
    return details


def _dispatch_contract_evidence_note(report: Mapping[str, Any]) -> str:
    parts = [
        f"dispatch_status={report.get('status') or 'missing'}",
        f"dispatch_mode={report.get('dispatch_mode') or 'missing'}",
    ]
    blocking = _short_list(report.get("blocking_reasons"))
    if blocking:
        parts.append(f"blocking_reasons={','.join(blocking)}")
    warnings = _short_list(report.get("warnings"), limit=3)
    if warnings:
        parts.append(f"warnings={','.join(warnings)}")
    return "; ".join(parts)


def _dispatch_contract_evidence_details(report: Mapping[str, Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    for key in (
        "dispatch_mode",
        "starts_runtime",
        "commands_present",
        "compatibility_source",
        "blocking_reasons",
        "warnings",
        "next_action",
        "missing_row_level_expected_artifacts",
        "missing_postprocess_expected_artifacts",
    ):
        value = report.get(key)
        if value not in (None, [], {}):
            details[key] = value
    return details


def _apollo_dispatch_contract_markdown(item: Mapping[str, Any]) -> str:
    status = str(item.get("apollo_fixed_scene_dispatch_contract") or STATUS_NOT_YET)
    mode = item.get("apollo_fixed_scene_dispatch_mode")
    raw = item.get("apollo_fixed_scene_dispatch_contract_status")
    suffix = mode or raw
    return f"{status} ({suffix})" if suffix else status


def _apollo_runtime_markdown(item: Mapping[str, Any]) -> str:
    status = str(item.get("apollo_fixed_scene_runtime_dispatch_status") or STATUS_NOT_YET)
    reason = item.get("apollo_fixed_scene_runtime_dispatch_reason")
    return f"{status} ({reason})" if reason else status


def _short_list(value: Any, limit: int = 5) -> list[str]:
    if not isinstance(value, list):
        return []
    items = [str(item) for item in value if item]
    priority_terms = (
        "obstacle_gt_contract",
        "fixed_scene_runtime_state",
        "fixed_scene_resolved",
        "scenario_actor_trace",
        "apollo_link_health",
    )
    prioritized = [item for item in items if any(term in item for term in priority_terms)]
    remainder = [item for item in items if item not in prioritized]
    return list(dict.fromkeys([*prioritized, *remainder]))[:limit]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_find_files(root: Path, filename: str) -> list[Path]:
    matches: list[Path] = []
    if not root.exists():
        return matches
    for dirpath, _, filenames in os.walk(root, onerror=lambda _err: None):
        if filename in filenames:
            matches.append(Path(dirpath) / filename)
    return sorted(matches)


def _comparison_missing_hint(summary: Mapping[str, Any]) -> str:
    missing: list[str] = []
    for run in summary.get("participating_runs") or []:
        if not isinstance(run, Mapping):
            continue
        backend = str(run.get("backend") or "unknown")
        for artifact in run.get("missing_expected_artifacts") or []:
            missing.append(f"{backend}:{artifact}")
    return ",".join(sorted(dict.fromkeys(missing))[:5])


def _comparison_evidence_note(
    comparison_status: str,
    comparison_target_status: str,
    *,
    backend_count: int,
    missing_hint: str,
    summary: Mapping[str, Any],
) -> str:
    parts = [
        f"comparison_status={comparison_status}",
        f"comparison_target_status={comparison_target_status}",
        f"backend_count={backend_count}",
    ]
    backend_notes = _comparison_backend_note_fragments(summary)
    if backend_notes:
        parts.append(f"backend_results={','.join(backend_notes)}")
    if missing_hint:
        parts.append(f"missing_expected_artifacts={missing_hint}")
    return "; ".join(parts)


def _comparison_backend_note_fragments(summary: Mapping[str, Any]) -> list[str]:
    fragments: list[str] = []
    rows = summary.get("backend_results") or summary.get("participating_runs") or []
    if not isinstance(rows, list):
        return fragments
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        backend = str(row.get("backend") or "unknown")
        status = str(row.get("phase1_status") or "unknown")
        failure = row.get("failure_reason")
        handoff_stage = row.get("apollo_control_handoff_failure_stage")
        handoff_reason = row.get("apollo_control_handoff_failure_reason")
        handoff_reasons = [str(item) for item in row.get("apollo_control_handoff_failure_reasons") or [] if item]
        fragment = f"{backend}:{status}"
        if failure:
            fragment += f"/{failure}"
        if handoff_stage:
            fragment += f"@{handoff_stage}"
        if handoff_reason:
            fragment += f"[{handoff_reason}]"
        elif handoff_reasons:
            fragment += f"[{'+'.join(handoff_reasons[:2])}]"
        derived = _comparison_backend_derived_details(row)
        derived_bits = []
        if derived.get("apollo_link_primary_blocker"):
            derived_bits.append(f"link={derived['apollo_link_primary_blocker']}")
        if derived.get("control_health_failure_reason"):
            derived_bits.append(f"control={derived['control_health_failure_reason']}")
        if derived.get("lane_event_departure_classification"):
            derived_bits.append(f"lane_event={derived['lane_event_departure_classification']}")
        if derived_bits:
            fragment += "{" + ",".join(derived_bits) + "}"
        fragments.append(fragment)
    return fragments[:4]


def _comparison_evidence_details(summary: Mapping[str, Any]) -> dict[str, Any]:
    rows = summary.get("backend_results") or []
    if not isinstance(rows, list):
        rows = []
    compact_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        compact: dict[str, Any] = {}
        for key in (
            "backend",
            "backend_type",
            "phase1_status",
            "failure_reason",
            "apollo_control_handoff_status",
            "apollo_control_handoff_failure_stage",
            "apollo_control_handoff_failure_reason",
            "apollo_control_handoff_failure_reasons",
            "counts_as_backend_loss",
        ):
            value = row.get(key)
            if value not in (None, [], {}):
                compact[key] = value
        compact.update(_comparison_backend_derived_details(row))
        if compact:
            compact_rows.append(compact)
    details: dict[str, Any] = {"backend_results": compact_rows} if compact_rows else {}
    safety_issue = _comparison_safety_event_issue(summary)
    if safety_issue:
        details["safety_event_evidence_issue"] = safety_issue
    safety_event_evidence = summary.get("safety_event_evidence")
    if isinstance(safety_event_evidence, Mapping):
        details["safety_event_evidence"] = dict(safety_event_evidence)
    safety_event_context = summary.get("safety_event_context")
    if isinstance(safety_event_context, Mapping):
        details["safety_event_context"] = dict(safety_event_context)
    return details


def _comparison_backend_derived_details(row: Mapping[str, Any]) -> dict[str, Any]:
    metrics = row.get("phase1_metrics") if isinstance(row.get("phase1_metrics"), Mapping) else {}
    derived = (
        metrics.get("derived_blocker_evidence")
        if isinstance(metrics.get("derived_blocker_evidence"), Mapping)
        else {}
    )
    if not derived.get("available"):
        return {}
    control_health = (
        derived.get("control_health")
        if isinstance(derived.get("control_health"), Mapping)
        else {}
    )
    link_health = (
        derived.get("apollo_link_health")
        if isinstance(derived.get("apollo_link_health"), Mapping)
        else {}
    )
    lane_event = (
        derived.get("baguang_lane_event_contract")
        if isinstance(derived.get("baguang_lane_event_contract"), Mapping)
        else {}
    )
    details: dict[str, Any] = {}
    if link_health.get("primary_blocker"):
        details["apollo_link_primary_blocker"] = link_health.get("primary_blocker")
    if control_health.get("failure_reason"):
        details["control_health_failure_reason"] = control_health.get("failure_reason")
    if lane_event.get("departure_classification"):
        details["lane_event_departure_classification"] = lane_event.get("departure_classification")
    return details


def _comparison_safety_event_issue(summary: Mapping[str, Any]) -> str | None:
    safety_event_evidence = summary.get("safety_event_evidence")
    if isinstance(safety_event_evidence, Mapping):
        if safety_event_evidence.get("comparison_blocking"):
            return str(safety_event_evidence.get("reason") or "safety_event_evidence_mismatch")
        return None
    rows = summary.get("backend_results") or summary.get("participating_runs") or []
    if not isinstance(rows, list):
        return None
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("failure_reason") or "") in {"lane_invasion", "collision"}:
            return "safety_event_evidence_missing_for_safety_failure"
    return None


def _comparison_safety_context_issue(summary: Mapping[str, Any]) -> str | None:
    safety_event_context = summary.get("safety_event_context")
    if isinstance(safety_event_context, Mapping):
        if safety_event_context.get("comparison_blocking"):
            return str(safety_event_context.get("reason") or "shared_safety_event_context_issue")
        return None
    rows = summary.get("backend_results") or summary.get("participating_runs") or []
    if not isinstance(rows, list):
        return None
    evaluable_rows = [
        row
        for row in rows
        if isinstance(row, Mapping) and row.get("phase1_status") != "invalid" and row.get("evaluable") is not False
    ]
    if len(evaluable_rows) < 2:
        return None
    if not all(str(row.get("failure_reason") or "") == "lane_invasion" for row in evaluable_rows):
        return None
    contexts = []
    for row in evaluable_rows:
        metrics = row.get("phase1_metrics") if isinstance(row.get("phase1_metrics"), Mapping) else {}
        context = metrics.get("lane_invasion_context") if isinstance(metrics.get("lane_invasion_context"), Mapping) else {}
        if not _near_start_low_error_lane_context(context):
            return None
        contexts.append(context)
    return "shared_near_start_lane_invasion_context_issue" if contexts else None


def _near_start_low_error_lane_context(context: Mapping[str, Any]) -> bool:
    if not context.get("available"):
        return False
    displacement = _float_or_none(context.get("ego_displacement_from_start_m"))
    cte = _float_or_none(context.get("cross_track_error_m"))
    heading = _float_or_none(context.get("heading_error_rad"))
    near_start = bool(context.get("near_start")) or (displacement is not None and displacement <= 5.0)
    low_cte = cte is not None and abs(cte) <= 0.05
    low_heading = heading is None or abs(heading) <= 0.02
    early_displacement = displacement is None or displacement <= 5.0
    return near_start and low_cte and low_heading and early_displacement


def _apollo_fixed_scene_runtime_dispatch_from_run(
    run_dir: Path,
    phase1_status: Mapping[str, Any],
) -> tuple[str, str | None]:
    """Separate Apollo fixed-scene runtime dispatch from config readiness.

    A readiness report can prove that the bridge config is prepared to emit
    target-obstacle evidence, but it does not prove an online fixed-scene run
    actually executed. The catalog keeps this split explicit so readiness-only
    scaffolds cannot be mistaken for Apollo behavior evidence.
    """

    phase1_state = str(phase1_status.get("status") or "")
    if phase1_status and phase1_state != "invalid" and phase1_status.get("evaluable") is not False:
        return STATUS_DONE, None
    preflight = _read_json(run_dir / "preflight.json")
    reasons = {
        str(item)
        for item in [
            *(phase1_status.get("preflight_reasons") or []),
            *(preflight.get("reasons") or []),
        ]
        if item
    }
    if "apollo_fixed_scene_runtime_not_migrated" in reasons:
        return STATUS_PARTIAL, "apollo_fixed_scene_runtime_not_migrated"
    failure_reason = str(phase1_status.get("failure_reason") or preflight.get("status") or "")
    if failure_reason == "backend_not_ready":
        return STATUS_PARTIAL, "backend_not_ready"
    if failure_reason:
        return STATUS_PARTIAL, failure_reason
    return STATUS_NOT_YET, None


def _comparison_summary_rank(
    comparison_status: str,
    comparison_target_status: str,
    *,
    backend_count: int,
    path_text: str,
) -> tuple[int, int, int, int]:
    target_rank = {
        "apollo_vs_planning_control_evaluable": 6,
        "partially_evaluable_apollo_vs_planning_control": 5,
        "safety_event_evidence_mismatch": 5,
        "shared_safety_event_context_issue": 5,
        "missing_evaluable_planning_control_backend": 4,
        "missing_evaluable_apollo_reference_backend": 3,
        "same_backend_regression": 2,
        "unknown_backend_coverage": 1,
    }.get(str(comparison_target_status or ""), 0)
    status_rank = {
        "comparable": 4,
        "partially_evaluable": 3,
        "invalid": 2,
        "unknown": 1,
    }.get(str(comparison_status or ""), 0)
    return (target_rank, status_rank, int(backend_count), _timestamp_rank(path_text))


def _matches_scenario(payload: Mapping[str, Any], aliases: set[str], path_hint: str = "") -> bool:
    values = [
        str(payload.get("scenario_case") or ""),
        str(payload.get("scenario") or ""),
        str(payload.get("scenario_id") or ""),
        str(payload.get("scenario_class") or ""),
        str(payload.get("template") or ""),
        path_hint,
    ]
    if isinstance(payload.get("fixed_scene"), Mapping):
        values.append(str(payload["fixed_scene"].get("template") or ""))
    return any(_value_matches_alias(value, aliases) for value in values if value)


def _value_matches_alias(value: str, aliases: set[str]) -> bool:
    return any(value == alias or alias in value for alias in aliases)


def _phase1_readiness_from_status(status: str) -> str:
    if status in {STATUS_DONE, "pass", "warn", "not_applicable"}:
        return STATUS_DONE
    if status in {"invalid", "fail", "failed", "partial", "unknown", "insufficient_data", STATUS_UNKNOWN}:
        return STATUS_PARTIAL
    return STATUS_NOT_YET


def _route_only_scenario(canonical_id: str) -> bool:
    return canonical_id in {"lane_keep_straight", "lane_keep_curve", "junction_turn_no_signal"}


def _catalog_notes(canonical_id: str, has_fixed_scene: bool) -> list[str]:
    notes = ["YAML existence is not treated as ready without target/gap/comparison evidence."]
    if _route_only_scenario(canonical_id) and not has_fixed_scene:
        notes.append("route_only_scenario_has_no_fixed_scene_compile_requirement")
    return notes


def _comparison_readiness(online: Mapping[str, Any]) -> str:
    status = str(online.get("comparison_status") or STATUS_NOT_YET)
    target_status = str(online.get("comparison_target_status") or "")
    if status == "comparable" and target_status == "apollo_vs_planning_control_evaluable":
        return STATUS_DONE
    if status in {"comparable", "partially_evaluable", "invalid", "unknown"}:
        return STATUS_PARTIAL
    return STATUS_NOT_YET


def _best_readiness(current: str, candidate: str) -> str:
    order = {STATUS_NOT_YET: 0, STATUS_UNKNOWN: 1, STATUS_PARTIAL: 2, STATUS_DONE: 3}
    return candidate if order.get(candidate, 0) > order.get(current, 0) else current


def _readiness_better(current: str, candidate: str) -> bool:
    order = {STATUS_NOT_YET: 0, STATUS_UNKNOWN: 1, STATUS_PARTIAL: 2, STATUS_DONE: 3}
    return order.get(candidate, 0) > order.get(current, 0)


def _best_raw_readiness_status(current: str, candidate: str) -> str:
    order = {
        STATUS_NOT_YET: 0,
        "missing": 0,
        "unknown": 1,
        "insufficient_data": 2,
        "fail": 2,
        "partial": 2,
        "warn": 3,
        "pass": 4,
        "not_applicable": 4,
    }
    return candidate if order.get(candidate, 0) > order.get(current, 0) else current
