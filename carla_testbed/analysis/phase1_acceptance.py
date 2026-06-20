from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Mapping

PHASE1_ACCEPTANCE_SCHEMA_VERSION = "phase1_acceptance.v1"
STATUS_DONE = "DONE"
STATUS_PARTIAL = "PARTIAL"


def analyze_phase1_acceptance(comparison_dir: str | Path) -> dict[str, Any]:
    root = Path(comparison_dir).expanduser()
    summary_path = root / "comparison_summary.json"
    manifest_path = root / "comparison_manifest.json"
    summary = _read_json(summary_path)
    manifest = _read_json(manifest_path)
    participating_runs = [
        dict(item) for item in summary.get("participating_runs") or [] if isinstance(item, Mapping)
    ]
    backend_types = {str(run.get("backend_type") or "") for run in participating_runs}
    apollo_runs = [run for run in participating_runs if run.get("backend_type") == "apollo_reference_backend"]
    planning_runs = [
        run for run in participating_runs if run.get("backend_type") == "planning_control_backend"
    ]
    scenario_cases = {str(run.get("scenario_case") or "") for run in participating_runs if run.get("scenario_case")}
    comparison_case = str(summary.get("scenario_case") or manifest.get("scenario_case") or "")
    if comparison_case:
        scenario_cases.add(comparison_case)

    gates = {
        "same_scenario_case": len(scenario_cases) == 1 and bool(scenario_cases),
        "backend_coverage": len(apollo_runs) == 1 and len(planning_runs) == 1,
        "comparison_valid": (
            summary.get("comparison_status") == "comparable"
            and summary.get("comparison_target_status") == "apollo_vs_planning_control_evaluable"
        ),
        "blocking_assists_absent": all(not run.get("blocking_assists") for run in participating_runs),
        "scenario_interaction_evaluable": all(
            run.get("scenario_interaction_evaluable") is True for run in participating_runs
        ),
        "target_metric_evaluable": all(
            run.get("target_metric_evaluable") is True for run in participating_runs
        ),
        "artifact_complete": all(_artifact_complete(run) for run in participating_runs),
        "comparison_curves_present": (
            not _comparison_curves_required(participating_runs)
            or (root / "comparison_curves" / "v_t_gap_combined.csv").exists()
        ),
    }
    blocking_reasons = [name for name, passed in gates.items() if not passed]
    status = STATUS_DONE if not blocking_reasons else STATUS_PARTIAL
    artifact_hashes = _artifact_hashes(root, summary_path, manifest_path, participating_runs)
    run_inputs = [_run_input(run) for run in participating_runs]
    apollo_run = apollo_runs[0] if len(apollo_runs) == 1 else {}
    planning_run = planning_runs[0] if len(planning_runs) == 1 else {}
    return {
        "schema_version": PHASE1_ACCEPTANCE_SCHEMA_VERSION,
        "status": status,
        "scenario_case": next(iter(scenario_cases)) if len(scenario_cases) == 1 else comparison_case or None,
        "comparison_id": summary.get("comparison_id") or manifest.get("comparison_id") or root.name,
        "comparison_dir": str(root),
        "comparison_status": summary.get("comparison_status"),
        "comparison_target_status": summary.get("comparison_target_status"),
        "apollo_run_id": apollo_run.get("run_id"),
        "apollo_run_dir": apollo_run.get("run_dir"),
        "planning_control_run_id": planning_run.get("run_id"),
        "planning_control_run_dir": planning_run.get("run_dir"),
        "backend_types": sorted(item for item in backend_types if item),
        "run_inputs": run_inputs,
        "gates": gates,
        "blocking_reasons": blocking_reasons,
        "artifact_hashes": artifact_hashes,
        "claim_boundary": (
            "Phase1AcceptanceBundle proves an atomic Phase 1 scenario/backend comparison surface. "
            "It is not Apollo natural-driving success evidence."
        ),
    }


def write_phase1_acceptance(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    payload = dict(report)
    materialization = _materialize_acceptance_bundle(payload, output)
    _apply_materialization_gate(payload, materialization)
    json_path = output / "phase1_acceptance_report.json"
    md_path = output / "phase1_acceptance_summary.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _run_input(run: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "run_id",
        "run_dir",
        "backend",
        "backend_type",
        "scenario_case",
        "target_metric_status",
        "target_metric_evaluable",
        "scenario_interaction_evaluable",
    )
    return {key: run.get(key) for key in keys if key in run}


def _materialize_acceptance_bundle(report: Mapping[str, Any], output: Path) -> dict[str, Any]:
    evidence_root = output / "evidence"
    copied: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    optional_missing: list[dict[str, Any]] = []

    def copy_required(src: Path, dest: Path, role: str) -> None:
        if not src.exists() or not src.is_file():
            missing.append({"role": role, "source": str(src), "destination": str(dest)})
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
        copied.append(_materialized_file(role, src, dest))

    def copy_optional(src: Path, dest: Path, role: str) -> None:
        if not src.exists() or not src.is_file():
            optional_missing.append({"role": role, "source": str(src), "destination": str(dest)})
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
        copied.append(_materialized_file(role, src, dest))

    comparison_dir = Path(str(report.get("comparison_dir") or ""))
    copy_required(
        comparison_dir / "comparison_summary.json",
        evidence_root / "comparison" / "comparison_summary.json",
        "comparison_summary",
    )
    copy_required(
        comparison_dir / "comparison_manifest.json",
        evidence_root / "comparison" / "comparison_manifest.json",
        "comparison_manifest",
    )
    copy_optional(
        comparison_dir / "comparison_curves" / "v_t_gap_combined.csv",
        evidence_root / "comparison" / "comparison_curves" / "v_t_gap_combined.csv",
        "comparison_v_t_gap_combined",
    )

    for run in report.get("run_inputs") or []:
        if not isinstance(run, Mapping):
            continue
        run_dir = Path(str(run.get("run_dir") or ""))
        run_name = _safe_name(str(run.get("run_id") or run_dir.name or "run"))
        dest_root = evidence_root / "runs" / run_name
        copy_required(run_dir / "manifest.json", dest_root / "manifest.json", "run_manifest")
        copy_required(run_dir / "summary.json", dest_root / "summary.json", "run_summary")
        copy_required(
            run_dir / "analysis" / "phase1_status" / "phase1_status.json",
            dest_root / "analysis" / "phase1_status" / "phase1_status.json",
            "phase1_status",
        )
        copy_required(
            run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json",
            dest_root / "analysis" / "v_t_gap" / "v_t_gap_report.json",
            "v_t_gap_report",
        )
        _copy_one_of(
            [
                (run_dir / "timeseries.csv", dest_root / "timeseries.csv", "timeseries_csv"),
                (run_dir / "timeseries.jsonl", dest_root / "timeseries.jsonl", "timeseries_jsonl"),
            ],
            copied=copied,
            missing=missing,
            role="timeseries_surface",
        )
        copy_required(run_dir / "events.jsonl", dest_root / "events.jsonl", "events_jsonl")
        _copy_one_of(
            [
                (
                    run_dir / "artifacts" / "control_apply_trace.jsonl",
                    dest_root / "artifacts" / "control_apply_trace.jsonl",
                    "control_apply_trace",
                ),
                (
                    run_dir / "artifacts" / "ego_control_trace.jsonl",
                    dest_root / "artifacts" / "ego_control_trace.jsonl",
                    "ego_control_trace",
                ),
                (
                    run_dir / "artifacts" / "apollo_control_raw.jsonl",
                    dest_root / "artifacts" / "apollo_control_raw.jsonl",
                    "apollo_control_raw",
                ),
            ],
            copied=copied,
            missing=missing,
            role="control_trace_surface",
        )
        copy_optional(
            run_dir / "analysis" / "v_t_gap" / "v_t_gap.csv",
            dest_root / "analysis" / "v_t_gap" / "v_t_gap.csv",
            "v_t_gap_csv",
        )
        copy_optional(
            run_dir / "artifacts" / "safety_event_trace.jsonl",
            dest_root / "artifacts" / "safety_event_trace.jsonl",
            "safety_event_trace",
        )
        if _target_artifacts_required(run):
            copy_required(
                run_dir / "artifacts" / "scenario_actor_trace.jsonl",
                dest_root / "artifacts" / "scenario_actor_trace.jsonl",
                "scenario_actor_trace",
            )
            copy_required(
                run_dir / "artifacts" / "scenario_phase_events.jsonl",
                dest_root / "artifacts" / "scenario_phase_events.jsonl",
                "scenario_phase_events",
            )
        else:
            copy_optional(
                run_dir / "artifacts" / "scenario_actor_trace.jsonl",
                dest_root / "artifacts" / "scenario_actor_trace.jsonl",
                "scenario_actor_trace",
            )
            copy_optional(
                run_dir / "artifacts" / "scenario_phase_events.jsonl",
                dest_root / "artifacts" / "scenario_phase_events.jsonl",
                "scenario_phase_events",
            )

    return {
        "schema_version": "phase1_acceptance_materialization.v1",
        "bundle_root": str(evidence_root),
        "self_contained": not missing,
        "copied_files": copied,
        "missing_required_files": missing,
        "missing_optional_files": optional_missing,
    }


def _copy_one_of(
    candidates: list[tuple[Path, Path, str]],
    *,
    copied: list[dict[str, Any]],
    missing: list[dict[str, Any]],
    role: str,
) -> None:
    candidate_sources = []
    for src, dest, candidate_role in candidates:
        candidate_sources.append(str(src))
        if src.exists() and src.is_file():
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dest)
            copied.append(_materialized_file(candidate_role, src, dest))
            return
    missing.append({"role": role, "candidates": candidate_sources})


def _materialized_file(role: str, src: Path, dest: Path) -> dict[str, Any]:
    data = dest.read_bytes()
    return {
        "role": role,
        "source": str(src),
        "path": str(dest),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
    }


def _target_artifacts_required(run: Mapping[str, Any]) -> bool:
    return str(run.get("target_metric_status") or "") != "not_applicable"


def _safe_name(value: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)
    return clean[:160] or "run"


def _apply_materialization_gate(report: dict[str, Any], materialization: Mapping[str, Any]) -> None:
    gates = dict(report.get("gates") if isinstance(report.get("gates"), Mapping) else {})
    gates["bundle_self_contained"] = bool(materialization.get("self_contained"))
    report["gates"] = gates
    report["bundle_materialization"] = dict(materialization)
    blockers = list(report.get("blocking_reasons") or [])
    if not gates["bundle_self_contained"] and "bundle_self_contained" not in blockers:
        blockers.append("bundle_self_contained")
    report["blocking_reasons"] = blockers
    report["status"] = STATUS_DONE if not blockers else STATUS_PARTIAL


def _artifact_complete(run: Mapping[str, Any]) -> bool:
    if run.get("artifact_complete") is not True:
        return False
    if run.get("artifact_complete") is False:
        return False
    status = run.get("artifact_completeness_status")
    if status in {"fail", "insufficient_data"}:
        return False
    missing = run.get("missing_artifacts")
    return not bool(missing)


def _comparison_curves_required(runs: list[dict[str, Any]]) -> bool:
    if not runs:
        return True
    statuses = {str(run.get("target_metric_status") or "") for run in runs}
    return not statuses or any(status != "not_applicable" for status in statuses)


def _artifact_hashes(root: Path, *items: Any) -> list[dict[str, Any]]:
    paths: list[Path] = []
    for item in items:
        if isinstance(item, Path):
            paths.append(item)
        elif isinstance(item, list):
            for run in item:
                if not isinstance(run, Mapping):
                    continue
                for key in ("run_dir",):
                    if run.get(key):
                        run_dir = Path(str(run[key]))
                        paths.extend(
                            [
                                run_dir / "manifest.json",
                                run_dir / "summary.json",
                                run_dir / "analysis" / "phase1_status" / "phase1_status.json",
                                run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json",
                            ]
                        )
    hashes = []
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        hashes.append(
            {
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size": path.stat().st_size,
            }
        )
    return hashes


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Acceptance",
        "",
        f"Status: `{report.get('status')}`",
        f"Scenario case: `{report.get('scenario_case')}`",
        f"Comparison: `{report.get('comparison_id')}`",
        f"Apollo run: `{report.get('apollo_run_id')}`",
        f"PlanningControl run: `{report.get('planning_control_run_id')}`",
        "",
        "## Gates",
        "",
    ]
    gates = report.get("gates") if isinstance(report.get("gates"), Mapping) else {}
    for key, value in gates.items():
        lines.append(f"- {key}: `{value}`")
    blockers = report.get("blocking_reasons") or []
    if blockers:
        lines.extend(["", "## Blocking Reasons", ""])
        for item in blockers:
            lines.append(f"- `{item}`")
    lines.extend(["", str(report.get("claim_boundary") or "")])
    return "\n".join(lines) + "\n"
