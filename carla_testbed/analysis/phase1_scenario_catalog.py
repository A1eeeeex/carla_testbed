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
    evidence = [_evidence(path, root, "case_yaml") for path, _ in matched]
    online = _discover_online_evidence(root, aliases)
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
            "v_t_gap_status": online["v_t_gap_status"],
            "v_t_gap_readiness": STATUS_NOT_YET,
            "comparison_status": online["comparison_status"],
            "comparison_readiness": STATUS_NOT_YET,
            "evidence": online["evidence"],
            "evidence_paths": [],
            "missing": ["scenario_case_yaml"],
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
                evidence.append(_evidence(matched[0][0], root, "fixed_scene_compile"))
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
    v_t_gap_readiness = _phase1_readiness_from_status(online["v_t_gap_status"])
    comparison_readiness = _comparison_readiness(online)
    if online["carla_online_status"] != STATUS_DONE:
        missing_items.append("carla_online_evidence")
    if online["apollo_online_status"] != STATUS_DONE:
        missing_items.append("apollo_online_evidence")
    if v_t_gap_readiness == STATUS_NOT_YET:
        missing_items.append("v_t_gap_extractor_output")
    if comparison_readiness == STATUS_NOT_YET:
        missing_items.append("scenario_comparison_report")
    elif comparison_readiness != STATUS_DONE:
        missing_items.append("cross_backend_scenario_comparison")
    overall = STATUS_PARTIAL
    if canonical_id == "lead_hard_brake":
        overall = STATUS_NOT_YET
    elif (
        compile_status == STATUS_DONE
        and target_status in {"resolved", "not_required"}
        and online["carla_online_status"] == STATUS_DONE
        and online["apollo_online_status"] == STATUS_DONE
        and v_t_gap_readiness == STATUS_DONE
        and comparison_readiness == STATUS_DONE
    ):
        overall = STATUS_DONE
    elif compile_status == STATUS_DONE and target_status in {"resolved", "not_required"}:
        overall = STATUS_PARTIAL

    missing = sorted(set(missing_items))
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
        "v_t_gap_status": online["v_t_gap_status"],
        "v_t_gap_readiness": v_t_gap_readiness,
        "comparison_status": online["comparison_status"],
        "comparison_readiness": comparison_readiness,
        "evidence": evidence,
        "evidence_paths": evidence_paths,
        "missing": missing,
        "missing_items": missing,
        "compile_errors": compile_errors,
        "notes": ["YAML existence is not treated as ready without target/gap/comparison evidence."],
    }


def _catalog_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Scenario Catalog",
        "",
        f"Schema: `{report.get('schema_version')}`",
        "",
        "| Scenario | Priority | Status | Case YAML | Template | Target | CARLA online | Apollo online | v-t-gap | Comparison | Missing |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report.get("scenarios") or []:
        if not isinstance(item, Mapping):
            continue
        missing = ", ".join(str(value) for value in item.get("missing_items") or []) or "-"
        lines.append(
            "| {scenario} | {priority} | {overall} | {case} | {template} | {target} | {carla} | {apollo} | {vtgap} | {comparison} | {missing} |".format(
                scenario=item.get("scenario"),
                priority=item.get("priority"),
                overall=item.get("overall_status"),
                case=item.get("case_yaml_status"),
                template=item.get("template_status") or item.get("fixed_scene_compile_status"),
                target=item.get("target_actor_status"),
                carla=item.get("carla_online_status"),
                apollo=item.get("apollo_online_status"),
                vtgap=item.get("v_t_gap_status"),
                comparison=item.get("comparison_status"),
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


def _discover_online_evidence(root: Path, aliases: set[str]) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    carla_online_status = STATUS_NOT_YET
    apollo_online_status = STATUS_NOT_YET
    v_t_gap_status: str = STATUS_NOT_YET
    comparison_status: str = STATUS_NOT_YET
    comparison_backend_count = 0

    runs_dir = root / "runs"
    if runs_dir.exists():
        for manifest_path in sorted(runs_dir.rglob("manifest.json")):
            if "comparisons" in manifest_path.parts:
                continue
            run_dir = manifest_path.parent
            manifest = _read_json(manifest_path)
            if not _matches_scenario(manifest, aliases, run_dir.name):
                continue
            backend = str(manifest.get("backend_name") or manifest.get("backend") or "")
            phase1_status = _read_json(run_dir / "analysis" / "phase1_status" / "phase1_status.json")
            evidence_status = STATUS_DONE if phase1_status else STATUS_PARTIAL
            evidence_type = "online_run"
            if backend == "carla_builtin":
                evidence_type = "CARLA_online"
                carla_online_status = _best_readiness(carla_online_status, evidence_status)
            elif backend == "apollo_cyberrt":
                evidence_type = "Apollo_online"
                apollo_online_status = _best_readiness(apollo_online_status, evidence_status)
            evidence.append(
                _evidence(
                    run_dir,
                    root,
                    evidence_type,
                    status=evidence_status,
                    note=f"backend={backend or 'unknown'}; phase1_status={phase1_status.get('status') or 'missing'}",
                )
            )
            gap_report = _read_json(run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json")
            if gap_report:
                v_t_gap_status = str(gap_report.get("status") or "unknown")
                evidence.append(
                    _evidence(
                        run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json",
                        root,
                        "v_t_gap",
                        status=_phase1_readiness_from_status(v_t_gap_status),
                    )
                )

    comparison_dir = runs_dir / "comparisons"
    if comparison_dir.exists():
        for summary_path in sorted(comparison_dir.rglob("comparison_summary.json")):
            summary = _read_json(summary_path)
            if not _matches_scenario(summary, aliases, summary_path.parent.name):
                continue
            comparison_status = str(summary.get("comparison_status") or "unknown")
            backends = {
                str(run.get("backend"))
                for run in summary.get("participating_runs") or []
                if isinstance(run, Mapping) and run.get("backend")
            }
            comparison_backend_count = max(comparison_backend_count, len(backends))
            evidence.append(
                _evidence(
                    summary_path,
                    root,
                    "comparison_online",
                    status=STATUS_DONE if comparison_status == "comparable" else STATUS_PARTIAL,
                    note=f"comparison_status={comparison_status}; backend_count={len(backends)}",
                )
            )

    return {
        "evidence": evidence,
        "carla_online_status": carla_online_status,
        "apollo_online_status": apollo_online_status,
        "v_t_gap_status": v_t_gap_status,
        "comparison_status": comparison_status,
        "comparison_backend_count": comparison_backend_count,
    }


def _evidence(
    path: Path,
    root: Path,
    evidence_type: str,
    *,
    status: str = STATUS_DONE,
    note: str | None = None,
) -> dict[str, Any]:
    try:
        path_text = str(path.relative_to(root))
    except ValueError:
        path_text = str(path)
    item: dict[str, Any] = {"path": path_text, "evidence_type": evidence_type, "status": status}
    if note:
        item["note"] = note
    return item


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _matches_scenario(payload: Mapping[str, Any], aliases: set[str], path_hint: str = "") -> bool:
    values = [
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
    if status in {"invalid", "fail", "failed", "unknown", STATUS_UNKNOWN}:
        return STATUS_PARTIAL
    return STATUS_NOT_YET


def _comparison_readiness(online: Mapping[str, Any]) -> str:
    status = str(online.get("comparison_status") or STATUS_NOT_YET)
    backend_count = int(online.get("comparison_backend_count") or 0)
    if status == "comparable" and backend_count >= 2:
        return STATUS_DONE
    if status in {"comparable", "partially_evaluable", "invalid", "unknown"}:
        return STATUS_PARTIAL
    return STATUS_NOT_YET


def _best_readiness(current: str, candidate: str) -> str:
    order = {STATUS_NOT_YET: 0, STATUS_UNKNOWN: 1, STATUS_PARTIAL: 2, STATUS_DONE: 3}
    return candidate if order.get(candidate, 0) > order.get(current, 0) else current
