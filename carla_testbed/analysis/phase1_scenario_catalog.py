from __future__ import annotations

import json
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
    "follow_stop_static": {"follow_stop_static", "static_lead_stop"},
    "lead_decel_accel": {"lead_decel_accel", "lead_vehicle_accel", "lead_vehicle_decel", "lead_vehicle_accel_decel"},
    "cut_in_simple": {"cut_in", "cut_in_simple"},
    "lane_keep_straight": {"lane_keep", "lane_keep_straight"},
    "lane_keep_curve": {"curve_diagnostic", "lane_keep_curve"},
    "cut_out_simple": {"cut_out", "cut_out_simple"},
    "junction_turn_no_signal": {"junction", "junction_turn", "junction_turn_no_signal"},
    "lead_hard_brake": {"lead_hard_brake"},
}


def analyze_phase1_scenario_catalog(repo_root: str | Path = ".") -> dict[str, Any]:
    root = Path(repo_root).resolve()
    scenario_files = sorted((root / "configs" / "scenarios").glob("**/*.yaml"))
    scenario_payloads = [_load_yaml(path) for path in scenario_files]
    entries = [
        _catalog_entry(root, scenario_id, "P0", scenario_files, scenario_payloads)
        for scenario_id in P0_SCENARIOS
    ]
    entries.extend(
        _catalog_entry(root, scenario_id, "P1", scenario_files, scenario_payloads)
        for scenario_id in P1_SCENARIOS
    )
    return {
        "schema_version": PHASE1_SCENARIO_CATALOG_SCHEMA_VERSION,
        "repo_root": str(root),
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
) -> dict[str, Any]:
    aliases = SCENARIO_CLASS_ALIASES[canonical_id]
    matched: list[tuple[Path, dict[str, Any]]] = []
    for path, payload in zip(scenario_files, scenario_payloads):
        scenario_class = str(payload.get("scenario_class") or "")
        scenario_id = str(payload.get("scenario_id") or "")
        if scenario_class in aliases or any(alias in scenario_id for alias in aliases):
            matched.append((path, payload))

    evidence_paths = [str(path.relative_to(root)) for path, _ in matched]
    missing_items: list[str] = []
    if not matched:
        return {
            "scenario": canonical_id,
            "priority": priority,
            "overall_status": STATUS_NOT_YET,
            "case_yaml_status": STATUS_NOT_YET,
            "fixed_scene_compile_status": STATUS_NOT_YET,
            "target_actor_status": "missing",
            "v_t_gap_readiness": STATUS_NOT_YET,
            "comparison_readiness": STATUS_NOT_YET,
            "evidence_paths": [],
            "missing_items": ["scenario_case_yaml"],
            "notes": ["YAML existence is required but not sufficient for Phase 1 readiness."],
        }

    compile_status = STATUS_NOT_YET
    target_status = "not_required"
    compile_errors: list[str] = []
    for _, payload in matched:
        if isinstance(payload.get("fixed_scene"), Mapping):
            try:
                template = extract_template_config(payload)
                storyboard = compile_fixed_scene_template(template)
                compile_status = STATUS_DONE
                target_status = str(storyboard.get("target_actor_contract", {}).get("status") or "missing")
                break
            except Exception as exc:
                compile_status = STATUS_UNKNOWN
                compile_errors.append(f"{type(exc).__name__}: {exc}")
        else:
            target_status = str(resolve_target_actor_contract(payload).get("status") or "not_required")

    if compile_status == STATUS_NOT_YET and any(isinstance(payload.get("fixed_scene"), Mapping) for _, payload in matched):
        compile_status = STATUS_UNKNOWN
    if compile_status != STATUS_DONE and canonical_id not in {"lane_keep_straight", "lane_keep_curve", "junction_turn_no_signal"}:
        missing_items.append("fixed_scene_compile")
    if target_status == "missing":
        missing_items.append("target_actor")
    missing_items.extend(["v_t_gap_extractor_output", "scenario_comparison_report"])
    overall = STATUS_PARTIAL
    if canonical_id == "lead_hard_brake":
        overall = STATUS_NOT_YET
    elif compile_status == STATUS_DONE and target_status in {"resolved", "not_required"}:
        overall = STATUS_PARTIAL

    return {
        "scenario": canonical_id,
        "priority": priority,
        "overall_status": overall,
        "case_yaml_status": STATUS_DONE,
        "fixed_scene_compile_status": compile_status,
        "target_actor_status": target_status,
        "v_t_gap_readiness": STATUS_NOT_YET,
        "comparison_readiness": STATUS_NOT_YET,
        "evidence_paths": evidence_paths,
        "missing_items": sorted(set(missing_items)),
        "compile_errors": compile_errors,
        "notes": ["YAML existence is not treated as ready without target/gap/comparison evidence."],
    }


def _catalog_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Scenario Catalog",
        "",
        f"Schema: `{report.get('schema_version')}`",
        "",
        "| Scenario | Priority | Status | Case YAML | Fixed Scene | Target | Missing |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report.get("scenarios") or []:
        if not isinstance(item, Mapping):
            continue
        missing = ", ".join(str(value) for value in item.get("missing_items") or []) or "-"
        lines.append(
            "| {scenario} | {priority} | {overall} | {case} | {fixed} | {target} | {missing} |".format(
                scenario=item.get("scenario"),
                priority=item.get("priority"),
                overall=item.get("overall_status"),
                case=item.get("case_yaml_status"),
                fixed=item.get("fixed_scene_compile_status"),
                target=item.get("target_actor_status"),
                missing=missing,
            )
        )
    lines.append("")
    lines.append("YAML presence is evidence for case definition only; it is not online playback or backend comparison evidence.")
    return "\n".join(lines) + "\n"


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}
