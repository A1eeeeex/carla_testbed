from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.assist_ledger import read_assist_ledger_from_run_dir
from carla_testbed.analysis.phase1_status import (
    collect_derived_blocker_evidence,
    classify_phase1_run,
)

SCENARIO_COMPARISON_SCHEMA_VERSION = "phase1_comparison.v1"
NEAR_START_LANE_INVASION_MAX_DISPLACEMENT_M = 5.0
NEAR_START_LANE_INVASION_MAX_ABS_CTE_M = 0.05
NEAR_START_LANE_INVASION_MAX_ABS_HEADING_RAD = 0.02


def compare_scenario_runs(run_dirs: Sequence[str | Path]) -> dict[str, Any]:
    runs = [_run_entry(Path(path).expanduser()) for path in run_dirs]
    scenario_ids = sorted({str(run.get("scenario_id")) for run in runs if run.get("scenario_id")})
    scenario_cases = sorted({str(run.get("scenario_case")) for run in runs if run.get("scenario_case")})
    invalid_runs = [run for run in runs if not _run_evaluable(run)]
    evaluable_runs = [run for run in runs if _run_evaluable(run)]
    if not runs:
        comparison_status = "invalid"
        reason = "no_runs_provided"
    elif len(scenario_cases) > 1:
        comparison_status = "invalid"
        reason = "scenario_case_mismatch"
    elif not scenario_cases and len(scenario_ids) > 1:
        comparison_status = "invalid"
        reason = "scenario_id_mismatch"
    elif invalid_runs and evaluable_runs:
        comparison_status = "partially_evaluable"
        reason = "some_runs_invalid"
    elif invalid_runs:
        comparison_status = "invalid"
        reason = "all_runs_invalid"
    else:
        comparison_status = "comparable"
        reason = None
    safety_event_evidence = _safety_event_evidence_comparison(runs)
    safety_event_context = _safety_event_context_comparison(runs)
    backend_coverage = _backend_coverage(runs)
    validity_gates = _comparison_validity_gates(
        runs=runs,
        backend_coverage=backend_coverage,
        comparison_status=comparison_status,
    )
    if comparison_status == "comparable" and safety_event_evidence.get("comparison_blocking"):
        comparison_status = "partially_evaluable"
        reason = str(safety_event_evidence.get("reason") or "safety_event_evidence_mismatch")
    elif comparison_status == "comparable" and safety_event_context.get("comparison_blocking"):
        comparison_status = "partially_evaluable"
        reason = str(safety_event_context.get("reason") or "shared_safety_event_context_issue")
    elif comparison_status == "comparable" and validity_gates.get("comparison_blocking"):
        comparison_status = "partially_evaluable"
        reason = str(validity_gates.get("reason") or "phase1_validity_gate_failed")
    comparison_target_status = _comparison_target_status(
        comparison_status=comparison_status,
        backend_coverage=backend_coverage,
        runs=runs,
        safety_event_evidence=safety_event_evidence,
        safety_event_context=safety_event_context,
        validity_gates=validity_gates,
    )
    backends = sorted({str(run.get("backend")) for run in runs if run.get("backend")})
    comparison_id = _comparison_id(
        scenario_case=scenario_cases[0] if len(scenario_cases) == 1 else (scenario_ids[0] if len(scenario_ids) == 1 else None),
        backends=backends,
    )
    return {
        "schema_version": SCENARIO_COMPARISON_SCHEMA_VERSION,
        "comparison_id": comparison_id,
        "scenario_id": scenario_ids[0] if len(scenario_ids) == 1 else None,
        "scenario_ids": scenario_ids,
        "scenario_case": scenario_cases[0] if len(scenario_cases) == 1 else (scenario_ids[0] if len(scenario_ids) == 1 else None),
        "backends": backends,
        "comparison_status": comparison_status,
        "comparison_target_status": comparison_target_status,
        "backend_coverage": backend_coverage,
        "validity_gates": validity_gates,
        "safety_event_evidence": safety_event_evidence,
        "safety_event_context": safety_event_context,
        "reason": reason,
        "participating_runs": runs,
        "invalid_runs": [run for run in runs if not _run_evaluable(run)],
        "evaluable_runs": [run for run in runs if _run_evaluable(run)],
        "backend_results": _backend_results(
            runs,
            comparison_status=comparison_status,
            comparison_target_status=comparison_target_status,
        ),
        "v_t_gap_files": _v_t_gap_files(runs),
        "evidence_files": _evidence_files(runs),
        "summary_notes": _summary_notes(comparison_status, reason, comparison_target_status),
        "claim_boundary": (
            "ScenarioComparison compares evaluable Phase 1 runs only; invalid runs are setup/artifact issues "
            "and are not backend losses."
        ),
    }


def write_scenario_comparison(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "comparison_manifest.json"
    summary_path = output / "comparison_summary.json"
    md_path = output / "comparison_summary.md"
    curves_dir = output / "comparison_curves"
    curves_path = curves_dir / "v_t_gap_combined.csv"
    manifest = {
        "schema_version": "phase1_scenario_comparison_manifest.v1",
        "comparison_id": report.get("comparison_id"),
        "generated_at_wall_s": time.time(),
        "scenario_id": report.get("scenario_id"),
        "scenario_case": report.get("scenario_case"),
        "backends": report.get("backends") or [],
        "comparison_status": report.get("comparison_status"),
        "comparison_target_status": report.get("comparison_target_status"),
        "backend_coverage": report.get("backend_coverage"),
        "validity_gates": report.get("validity_gates") or {},
        "participating_run_dirs": [run.get("run_dir") for run in report.get("participating_runs") or []],
        "evaluable_run_dirs": [run.get("run_dir") for run in report.get("evaluable_runs") or []],
        "invalid_run_dirs": [run.get("run_dir") for run in report.get("invalid_runs") or []],
        "v_t_gap_files": report.get("v_t_gap_files") or [],
        "evidence_files": report.get("evidence_files") or [],
        "claim_boundary": report.get("claim_boundary"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    curve_written = _write_combined_v_t_gap(report, curves_path)
    outputs = {"manifest": str(manifest_path), "summary": str(summary_path), "markdown": str(md_path)}
    if curve_written:
        outputs["v_t_gap_csv"] = str(curves_path)
    return outputs


def _run_entry(run_dir: Path) -> dict[str, Any]:
    manifest = _read_json(run_dir / "manifest.json")
    phase1_status = _read_json(run_dir / "analysis" / "phase1_status" / "phase1_status.json")
    if not phase1_status:
        phase1_status = classify_phase1_run(run_dir)
    else:
        phase1_status = _status_with_safety_event_evidence(run_dir, phase1_status)
    v_t_gap = _read_json(run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json")
    if _phase1_status_missing_evaluability_fields(phase1_status):
        phase1_status = _backfill_phase1_status_evaluability_fields(phase1_status, v_t_gap)
    apollo_control_handoff = _read_json(
        run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json"
    )
    apollo_handoff_section = (
        apollo_control_handoff.get("planning_control_handoff")
        if isinstance(apollo_control_handoff.get("planning_control_handoff"), Mapping)
        else {}
    )
    apollo_process_health = (
        apollo_control_handoff.get("process_health")
        if isinstance(apollo_control_handoff.get("process_health"), Mapping)
        else {}
    )
    assist_ledger = read_assist_ledger_from_run_dir(run_dir)
    artifact_completeness = _read_json(
        run_dir / "analysis" / "artifact_completeness" / "artifact_completeness_report.json"
    )
    phase1_artifact_completeness = _read_json(
        run_dir / "analysis" / "phase1_status" / "artifact_completeness.json"
    )
    comparison_artifact_completeness = _comparison_artifact_completeness(
        phase1_status=phase1_status,
        phase1_artifact_completeness=phase1_artifact_completeness,
        run_artifact_completeness=artifact_completeness,
    )
    artifact_paths = _run_artifact_paths(run_dir)
    phase1_metrics = _phase1_metrics_with_current_derived_blockers(run_dir, phase1_status)
    run_evaluable = bool(phase1_status.get("run_evaluable", phase1_status.get("evaluable")))
    return {
        "run_dir": str(run_dir),
        "run_id": manifest.get("run_id") or phase1_status.get("run_id") or run_dir.name,
        "scenario_id": manifest.get("scenario_id") or phase1_status.get("scenario_id"),
        "scenario_case": (
            manifest.get("scenario_case")
            or manifest.get("fixed_scene_case")
            or phase1_status.get("scenario_case")
            or manifest.get("scenario_id")
            or phase1_status.get("scenario_id")
        ),
        "backend": manifest.get("backend") or phase1_status.get("backend"),
        "backend_type": manifest.get("backend_type") or phase1_status.get("backend_type"),
        "phase1_status": phase1_status.get("status"),
        "failure_reason": phase1_status.get("failure_reason"),
        "evaluable": run_evaluable,
        "run_evaluable": run_evaluable,
        "scenario_interaction_evaluable": phase1_status.get("scenario_interaction_evaluable"),
        "scenario_interaction_reason": phase1_status.get("scenario_interaction_reason"),
        "target_metric_evaluable": phase1_status.get("target_metric_evaluable"),
        "target_metric_status": phase1_status.get("target_metric_status"),
        "target_metric_reason": phase1_status.get("target_metric_reason"),
        "counts_as_backend_loss_for_target_scenario": phase1_status.get(
            "counts_as_backend_loss_for_target_scenario"
        ),
        "phase1_metrics": phase1_metrics,
        "invalid_reasons": list(phase1_status.get("invalid_reasons") or []),
        "missing_artifacts": list(phase1_status.get("missing_artifacts") or []),
        "expected_artifacts": list(phase1_status.get("expected_artifacts") or manifest.get("expected_artifacts") or []),
        "missing_expected_artifacts": list(phase1_status.get("missing_expected_artifacts") or []),
        "target_actor_contract": dict(phase1_status.get("target_actor_contract") or manifest.get("target_actor_contract") or {}),
        "active_assists": list(assist_ledger.get("active_assists") or []),
        "blocking_assists": list(assist_ledger.get("blocking_assists") or []),
        "assist_confidence": assist_ledger.get("assist_confidence"),
        "artifact_completeness_status": comparison_artifact_completeness.get("status"),
        "artifact_complete": comparison_artifact_completeness.get("artifact_complete"),
        "artifact_completeness_source": comparison_artifact_completeness.get("source"),
        "artifact_completeness_warnings": list(comparison_artifact_completeness.get("warnings") or []),
        "v_t_gap_status": v_t_gap.get("status") if v_t_gap else phase1_status.get("v_t_gap_status"),
        "apollo_control_handoff_status": apollo_control_handoff.get("verdict")
        or apollo_control_handoff.get("status"),
        "apollo_control_handoff_failure_stage": apollo_control_handoff.get("failure_stage"),
        "apollo_control_handoff_blocking_reasons": list(apollo_control_handoff.get("blocking_reasons") or []),
        "apollo_control_handoff_failure_reason": _apollo_handoff_primary_reason(
            apollo_control_handoff=apollo_control_handoff,
            process_health=apollo_process_health,
            planning_control_handoff=apollo_handoff_section,
        ),
        "apollo_control_handoff_failure_reasons": list(apollo_handoff_section.get("failure_reasons") or []),
        "artifact_paths": artifact_paths,
    }


def _phase1_metrics_with_current_derived_blockers(
    run_dir: Path,
    phase1_status: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = dict(phase1_status.get("phase1_metrics") or {})
    current = collect_derived_blocker_evidence(run_dir)
    if current.get("available"):
        metrics["derived_blocker_evidence"] = current
    return metrics


def _comparison_artifact_completeness(
    *,
    phase1_status: Mapping[str, Any],
    phase1_artifact_completeness: Mapping[str, Any],
    run_artifact_completeness: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve artifact completeness for Phase 1 ScenarioComparison.

    ScenarioComparison uses the Phase 1 scenario-run surface. A generic
    run-artifact report may also check claim-grade raw evidence, so it can be
    stricter than the Phase 1 comparison contract. Keep those stricter findings
    visible as warnings instead of turning an otherwise evaluable Phase 1 run
    into a setup/artifact failure.
    """

    phase1_missing = list(phase1_status.get("missing_artifacts") or []) + list(
        phase1_status.get("missing_expected_artifacts") or []
    )
    if phase1_missing:
        return {
            "status": "insufficient_data",
            "artifact_complete": False,
            "source": "phase1_status_missing_artifacts",
            "missing_artifacts": phase1_missing,
            "warnings": [],
        }

    if _is_legacy_phase1_artifact_completeness(phase1_artifact_completeness):
        return _normalize_phase1_artifact_completeness(
            phase1_artifact_completeness,
            source="phase1_status_artifact_completeness",
            run_artifact_completeness=run_artifact_completeness,
        )

    if _is_run_artifact_phase1_profile(run_artifact_completeness):
        return _normalize_phase1_artifact_completeness(
            run_artifact_completeness,
            source="run_artifact_completeness_phase1_profile",
            run_artifact_completeness=run_artifact_completeness,
        )

    if run_artifact_completeness:
        return {
            "status": run_artifact_completeness.get("status"),
            "artifact_complete": run_artifact_completeness.get("artifact_complete"),
            "source": "run_artifact_completeness_generic_profile",
            "missing_artifacts": list(run_artifact_completeness.get("missing_artifacts") or []),
            "warnings": list(run_artifact_completeness.get("warnings") or []),
        }

    return {
        "status": None,
        "artifact_complete": None,
        "source": "missing_artifact_completeness_report",
        "missing_artifacts": [],
        "warnings": [],
    }


def _is_legacy_phase1_artifact_completeness(report: Mapping[str, Any]) -> bool:
    return str(report.get("schema_version") or "") == "phase1_artifact_completeness.v1"


def _is_run_artifact_phase1_profile(report: Mapping[str, Any]) -> bool:
    return (
        str(report.get("schema_version") or "") == "run_artifact_completeness.v1"
        and str(report.get("profile") or "") == "phase1"
    )


def _normalize_phase1_artifact_completeness(
    report: Mapping[str, Any],
    *,
    source: str,
    run_artifact_completeness: Mapping[str, Any],
) -> dict[str, Any]:
    missing = list(report.get("missing_artifacts") or [])
    status = str(report.get("status") or ("pass" if not missing else "insufficient_data"))
    if "artifact_complete" in report:
        artifact_complete = bool(report.get("artifact_complete"))
    else:
        artifact_complete = status == "pass" and not missing
    warnings = list(report.get("warnings") or [])
    if (
        run_artifact_completeness
        and not _is_run_artifact_phase1_profile(run_artifact_completeness)
        and run_artifact_completeness.get("artifact_complete") is False
    ):
        generic_missing = list(run_artifact_completeness.get("missing_artifacts") or [])
        generic_raw_missing = list(run_artifact_completeness.get("missing_raw_evidence_artifacts") or [])
        if generic_missing or generic_raw_missing:
            warnings.append(
                "generic_artifact_completeness_not_phase1_complete:"
                f"status={run_artifact_completeness.get('status')}"
            )
    return {
        "status": status,
        "artifact_complete": artifact_complete,
        "source": source,
        "missing_artifacts": missing,
        "warnings": warnings,
    }


def _run_artifact_paths(run_dir: Path) -> dict[str, str]:
    candidates = {
        "manifest": run_dir / "manifest.json",
        "summary": run_dir / "summary.json",
        "phase1_status": run_dir / "analysis" / "phase1_status" / "phase1_status.json",
        "phase1_artifact_completeness": run_dir
        / "analysis"
        / "phase1_status"
        / "artifact_completeness.json",
        "run_artifact_completeness": run_dir
        / "analysis"
        / "artifact_completeness"
        / "artifact_completeness_report.json",
        "v_t_gap": run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json",
        "apollo_control_handoff": run_dir
        / "analysis"
        / "apollo_control_handoff"
        / "apollo_control_handoff_report.json",
    }
    return {key: str(path) for key, path in candidates.items() if path.exists()}


def _backend_results(
    runs: list[dict[str, Any]],
    *,
    comparison_status: str,
    comparison_target_status: str,
) -> list[dict[str, Any]]:
    results = []
    for run in runs:
        evaluable = _run_evaluable(run)
        results.append(
            {
                "backend": run.get("backend"),
                "backend_type": run.get("backend_type"),
                "run_id": run.get("run_id"),
                "phase1_status": run.get("phase1_status"),
                "failure_reason": run.get("failure_reason"),
                "invalid_reasons": list(run.get("invalid_reasons") or []),
                "run_evaluable": run.get("run_evaluable", run.get("evaluable")),
                "scenario_interaction_evaluable": run.get("scenario_interaction_evaluable"),
                "scenario_interaction_reason": run.get("scenario_interaction_reason"),
                "target_metric_evaluable": run.get("target_metric_evaluable"),
                "target_metric_status": run.get("target_metric_status"),
                "target_metric_reason": run.get("target_metric_reason"),
                "artifact_completeness_status": run.get("artifact_completeness_status"),
                "artifact_complete": run.get("artifact_complete"),
                "artifact_completeness_source": run.get("artifact_completeness_source"),
                "artifact_completeness_warnings": list(run.get("artifact_completeness_warnings") or []),
                "missing_artifacts": list(run.get("missing_artifacts") or []),
                "missing_expected_artifacts": list(run.get("missing_expected_artifacts") or []),
                "phase1_metrics": dict(run.get("phase1_metrics") or {}),
                "safety_event_evidence": _run_safety_event_evidence(run),
                "apollo_control_handoff_status": run.get("apollo_control_handoff_status"),
                "apollo_control_handoff_failure_stage": run.get("apollo_control_handoff_failure_stage"),
                "apollo_control_handoff_blocking_reasons": list(
                    run.get("apollo_control_handoff_blocking_reasons") or []
                ),
                "apollo_control_handoff_failure_reason": run.get("apollo_control_handoff_failure_reason"),
                "apollo_control_handoff_failure_reasons": list(
                    run.get("apollo_control_handoff_failure_reasons") or []
                ),
                "active_assists": list(run.get("active_assists") or []),
                "blocking_assists": list(run.get("blocking_assists") or []),
                "counts_as_backend_loss": (
                    comparison_status == "comparable"
                    and comparison_target_status == "apollo_vs_planning_control_evaluable"
                    and evaluable
                    and run.get("counts_as_backend_loss_for_target_scenario") is not False
                    and run.get("phase1_status") == "failed"
                ),
            }
        )
    return results


def _comparison_id(*, scenario_case: str | None, backends: Sequence[str]) -> str:
    scenario = scenario_case or "unknown_scenario"
    backend_part = "_vs_".join(backends) if backends else "no_backends"
    return f"{scenario}__{backend_part}"


def _v_t_gap_files(runs: Sequence[Mapping[str, Any]]) -> list[str]:
    files: list[str] = []
    for run in runs:
        paths = run.get("artifact_paths") if isinstance(run.get("artifact_paths"), Mapping) else {}
        report = paths.get("v_t_gap")
        if report:
            files.append(str(report))
            csv_path = str(Path(str(report)).with_name("v_t_gap.csv"))
            files.append(csv_path)
    return sorted(dict.fromkeys(files))


def _evidence_files(runs: Sequence[Mapping[str, Any]]) -> list[str]:
    files: list[str] = []
    for run in runs:
        paths = run.get("artifact_paths") if isinstance(run.get("artifact_paths"), Mapping) else {}
        for value in paths.values():
            if value:
                files.append(str(value))
        run_dir = run.get("run_dir")
        if run_dir:
            files.append(str(Path(str(run_dir)) / "analysis" / "v_t_gap" / "v_t_gap.csv"))
    return sorted(dict.fromkeys(files))


def _summary_notes(comparison_status: str, reason: str | None, comparison_target_status: str) -> list[str]:
    notes = ["phase1_comparison_not_natural_driving_claim"]
    if comparison_status in {"invalid", "partially_evaluable"}:
        notes.append("invalid_runs_do_not_count_as_backend_losses")
    if reason:
        notes.append(f"reason={reason}")
    if comparison_target_status:
        notes.append(f"comparison_target_status={comparison_target_status}")
    return notes


def _run_evaluable(run: Mapping[str, Any]) -> bool:
    return bool(run.get("run_evaluable", run.get("evaluable", run.get("phase1_status") != "invalid")))


def _phase1_status_missing_evaluability_fields(phase1_status: Mapping[str, Any]) -> bool:
    return any(
        key not in phase1_status
        for key in (
            "run_evaluable",
            "scenario_interaction_evaluable",
            "target_metric_evaluable",
            "counts_as_backend_loss_for_target_scenario",
        )
    )


def _backfill_phase1_status_evaluability_fields(
    phase1_status: Mapping[str, Any],
    v_t_gap: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(phase1_status.get("status") or "")
    run_evaluable = bool(phase1_status.get("evaluable", status != "invalid"))
    v_t_gap_status = str(v_t_gap.get("status") or phase1_status.get("v_t_gap_status") or "")
    target_metric_evaluable = v_t_gap_status in {"pass", "warn", "fail", "not_applicable"}
    target_metric_status = v_t_gap_status or (
        "not_evaluable" if not run_evaluable else "unknown"
    )
    scenario_interaction_evaluable = bool(
        phase1_status.get("scenario_interaction_evaluable", run_evaluable)
    )
    updated = dict(phase1_status)
    updated.setdefault("run_evaluable", run_evaluable)
    updated.setdefault("scenario_interaction_evaluable", scenario_interaction_evaluable)
    updated.setdefault("target_metric_evaluable", target_metric_evaluable)
    updated.setdefault("target_metric_status", target_metric_status)
    updated.setdefault(
        "counts_as_backend_loss_for_target_scenario",
        bool(
            status == "failed"
            and run_evaluable
            and scenario_interaction_evaluable
            and target_metric_evaluable
        ),
    )
    return updated


def _comparison_validity_gates(
    *,
    runs: Sequence[Mapping[str, Any]],
    backend_coverage: Mapping[str, Any],
    comparison_status: str,
) -> dict[str, Any]:
    if comparison_status != "comparable":
        return {
            "comparison_blocking": False,
            "reason": None,
            "same_case": comparison_status != "invalid",
            "backend_coverage": False,
            "interaction_complete": False,
            "target_metric_valid": False,
            "artifact_complete": False,
            "blocking_assists_absent": not _blocking_assists_present(runs),
        }
    interaction_complete = all(_run_interaction_evaluable(run) for run in runs)
    target_metric_valid = all(_run_target_metric_evaluable(run) for run in runs)
    artifact_complete = all(_run_artifact_complete_for_comparison(run) for run in runs)
    blocking_assists_absent = not _blocking_assists_present(runs)
    backend_ok = bool(
        int(backend_coverage.get("evaluable_apollo_reference_backend") or 0) > 0
        and int(backend_coverage.get("evaluable_planning_control_backend") or 0) > 0
    )
    reason = None
    if not interaction_complete:
        reason = "target_interaction_not_exercised"
    elif not target_metric_valid:
        reason = "target_metric_not_evaluable"
    elif not artifact_complete:
        reason = "artifact_completeness_failed"
    elif not blocking_assists_absent:
        reason = "blocking_assists_present_not_phase1_reference"
    return {
        "comparison_blocking": reason is not None,
        "reason": reason,
        "same_case": True,
        "backend_coverage": backend_ok,
        "interaction_complete": interaction_complete,
        "target_metric_valid": target_metric_valid,
        "artifact_complete": artifact_complete,
        "blocking_assists_absent": blocking_assists_absent,
        "run_gates": [
            {
                "run_id": run.get("run_id"),
                "backend": run.get("backend"),
                "run_evaluable": _run_evaluable(run),
                "scenario_interaction_evaluable": run.get("scenario_interaction_evaluable"),
                "scenario_interaction_reason": run.get("scenario_interaction_reason"),
                "target_metric_evaluable": run.get("target_metric_evaluable"),
                "target_metric_status": run.get("target_metric_status"),
                "target_metric_reason": run.get("target_metric_reason"),
                "artifact_complete": run.get("artifact_complete"),
                "artifact_completeness_status": run.get("artifact_completeness_status"),
                "artifact_completeness_source": run.get("artifact_completeness_source"),
                "artifact_completeness_warnings": list(run.get("artifact_completeness_warnings") or []),
                "blocking_assists": list(run.get("blocking_assists") or []),
            }
            for run in runs
        ],
    }


def _run_interaction_evaluable(run: Mapping[str, Any]) -> bool:
    value = run.get("scenario_interaction_evaluable")
    return True if value is None else bool(value)


def _run_target_metric_evaluable(run: Mapping[str, Any]) -> bool:
    value = run.get("target_metric_evaluable")
    return True if value is None else bool(value)


def _run_artifact_complete_for_comparison(run: Mapping[str, Any]) -> bool:
    artifact_complete = run.get("artifact_complete")
    if artifact_complete is False:
        return False
    return True


def _backend_coverage(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    evaluable_by_type: dict[str, int] = {}
    by_backend: dict[str, int] = {}
    evaluable_by_backend: dict[str, int] = {}
    unknown_backend_type_count = 0
    for run in runs:
        backend_type = str(run.get("backend_type") or "unknown")
        backend = str(run.get("backend") or "unknown")
        if backend_type == "unknown":
            unknown_backend_type_count += 1
        by_type[backend_type] = by_type.get(backend_type, 0) + 1
        by_backend[backend] = by_backend.get(backend, 0) + 1
        if _run_evaluable(run):
            evaluable_by_type[backend_type] = evaluable_by_type.get(backend_type, 0) + 1
            evaluable_by_backend[backend] = evaluable_by_backend.get(backend, 0) + 1
    return {
        "backend_types": sorted(by_type),
        "evaluable_backend_types": sorted(evaluable_by_type),
        "by_backend_type": by_type,
        "evaluable_by_backend_type": evaluable_by_type,
        "by_backend": by_backend,
        "evaluable_by_backend": evaluable_by_backend,
        "apollo_reference_backend": by_type.get("apollo_reference_backend", 0),
        "planning_control_backend": by_type.get("planning_control_backend", 0),
        "evaluable_apollo_reference_backend": evaluable_by_type.get("apollo_reference_backend", 0),
        "evaluable_planning_control_backend": evaluable_by_type.get("planning_control_backend", 0),
        "unknown_backend_type_count": unknown_backend_type_count,
    }


def _comparison_target_status(
    *,
    comparison_status: str,
    backend_coverage: Mapping[str, Any],
    runs: Sequence[Mapping[str, Any]],
    safety_event_evidence: Mapping[str, Any],
    safety_event_context: Mapping[str, Any],
    validity_gates: Mapping[str, Any],
) -> str:
    if safety_event_evidence.get("comparison_blocking"):
        return "safety_event_evidence_mismatch"
    if safety_event_context.get("comparison_blocking"):
        return "shared_safety_event_context_issue"
    validity_reason = str(validity_gates.get("reason") or "")
    if validity_reason:
        return validity_reason
    if int(backend_coverage.get("unknown_backend_type_count") or 0) > 0:
        return "unknown_backend_coverage"
    apollo = int(backend_coverage.get("evaluable_apollo_reference_backend") or 0)
    planning = int(backend_coverage.get("evaluable_planning_control_backend") or 0)
    all_types = set(str(item) for item in backend_coverage.get("backend_types") or [])
    if comparison_status == "comparable" and apollo > 0 and planning > 0:
        return "apollo_vs_planning_control_evaluable"
    if comparison_status == "comparable" and len(set(backend_coverage.get("evaluable_backend_types") or [])) <= 1:
        return "same_backend_regression"
    if apollo == 0 and "apollo_reference_backend" in all_types:
        return "missing_evaluable_apollo_reference_backend"
    if planning == 0 and "planning_control_backend" in all_types:
        return "missing_evaluable_planning_control_backend"
    if apollo == 0:
        return "missing_evaluable_apollo_reference_backend"
    if planning == 0:
        return "missing_evaluable_planning_control_backend"
    if comparison_status == "partially_evaluable":
        return "partially_evaluable_apollo_vs_planning_control"
    return "not_phase1_target_comparison"


def _blocking_assists_present(runs: Sequence[Mapping[str, Any]]) -> bool:
    for run in runs:
        if run.get("blocking_assists"):
            return True
    return False


def _status_with_safety_event_evidence(run_dir: Path, phase1_status: Mapping[str, Any]) -> dict[str, Any]:
    status = dict(phase1_status)
    metrics = dict(status.get("phase1_metrics") or {})
    existing = metrics.get("safety_event_evidence")
    if isinstance(existing, Mapping):
        return status
    refreshed = classify_phase1_run(run_dir)
    refreshed_metrics = refreshed.get("phase1_metrics")
    if isinstance(refreshed_metrics, Mapping) and isinstance(refreshed_metrics.get("safety_event_evidence"), Mapping):
        metrics["safety_event_evidence"] = dict(refreshed_metrics["safety_event_evidence"])
        status["phase1_metrics"] = metrics
    return status


def _run_safety_event_evidence(run: Mapping[str, Any]) -> dict[str, Any]:
    metrics = run.get("phase1_metrics") if isinstance(run.get("phase1_metrics"), Mapping) else {}
    evidence = metrics.get("safety_event_evidence") if isinstance(metrics.get("safety_event_evidence"), Mapping) else {}
    return dict(evidence)


def _safety_event_evidence_comparison(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    evaluable_runs = [run for run in runs if _run_evaluable(run)]
    event_checks = {
        "lane_invasion": _safety_event_check(evaluable_runs, "lane_invasion"),
        "collision": _safety_event_check(evaluable_runs, "collision"),
    }
    blocking_events = [
        event_name
        for event_name, check in event_checks.items()
        if isinstance(check, Mapping) and check.get("comparison_blocking")
    ]
    reason = None
    if blocking_events:
        reason = "safety_event_evidence_mismatch"
    return {
        "schema_version": "phase1_safety_event_evidence_comparison.v1",
        "comparison_blocking": bool(blocking_events),
        "reason": reason,
        "blocking_events": blocking_events,
        "event_checks": event_checks,
        "claim_boundary": (
            "Safety-event failures such as lane_invasion/collision can be compared across backends only "
            "when participating evaluable runs expose the corresponding event counters."
        ),
    }


def _safety_event_context_comparison(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    evaluable_runs = [run for run in runs if _run_evaluable(run)]
    lane_check = _shared_near_start_lane_invasion_check(evaluable_runs)
    blocking_events = ["lane_invasion"] if lane_check.get("comparison_blocking") else []
    reason = "shared_near_start_lane_invasion_context_issue" if blocking_events else None
    return {
        "schema_version": "phase1_safety_event_context_comparison.v1",
        "comparison_blocking": bool(blocking_events),
        "reason": reason,
        "blocking_events": blocking_events,
        "event_checks": {"lane_invasion": lane_check},
        "claim_boundary": (
            "A lane-invasion failure shared by all evaluable backends within the first few meters, "
            "with low cross-track and heading error, is treated as a scenario/sensor contract issue "
            "until the lane-event contract is repaired or explained. It remains a non-pass outcome, "
            "but it is not counted as a backend behavior loss."
        ),
    }


def _shared_near_start_lane_invasion_check(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    triggered = [run for run in runs if str(run.get("failure_reason") or "") == "lane_invasion"]
    all_evaluable_runs_triggered = bool(runs) and len(triggered) == len(runs)
    run_contexts = [_lane_invasion_context_for_run(run) for run in runs]
    all_contexts_support_contract_issue = bool(run_contexts) and all(
        context.get("supports_near_start_contract_issue") for context in run_contexts
    )
    comparison_blocking = len(runs) >= 2 and all_evaluable_runs_triggered and all_contexts_support_contract_issue
    return {
        "event_name": "lane_invasion",
        "all_evaluable_runs_triggered": all_evaluable_runs_triggered,
        "comparison_blocking": comparison_blocking,
        "triggered_run_ids": [run.get("run_id") for run in triggered],
        "run_contexts": run_contexts,
        "thresholds": {
            "max_displacement_m": NEAR_START_LANE_INVASION_MAX_DISPLACEMENT_M,
            "max_abs_cross_track_error_m": NEAR_START_LANE_INVASION_MAX_ABS_CTE_M,
            "max_abs_heading_error_rad": NEAR_START_LANE_INVASION_MAX_ABS_HEADING_RAD,
        },
        "reason": "shared_near_start_low_error_lane_invasion" if comparison_blocking else None,
    }


def _lane_invasion_context_for_run(run: Mapping[str, Any]) -> dict[str, Any]:
    metrics = run.get("phase1_metrics") if isinstance(run.get("phase1_metrics"), Mapping) else {}
    context = metrics.get("lane_invasion_context") if isinstance(metrics.get("lane_invasion_context"), Mapping) else {}
    if not context:
        return {
            "run_id": run.get("run_id"),
            "backend": run.get("backend"),
            "available": False,
            "supports_near_start_contract_issue": False,
            "missing_reason": "lane_invasion_context_missing",
        }
    cte = _float_or_none(context.get("cross_track_error_m"))
    heading = _float_or_none(context.get("heading_error_rad"))
    displacement = _float_or_none(context.get("ego_displacement_from_start_m"))
    near_start = bool(context.get("near_start")) or (
        displacement is not None and displacement <= NEAR_START_LANE_INVASION_MAX_DISPLACEMENT_M
    )
    low_cte = cte is not None and abs(cte) <= NEAR_START_LANE_INVASION_MAX_ABS_CTE_M
    low_heading = heading is None or abs(heading) <= NEAR_START_LANE_INVASION_MAX_ABS_HEADING_RAD
    early_displacement = displacement is None or displacement <= NEAR_START_LANE_INVASION_MAX_DISPLACEMENT_M
    supports = bool(context.get("available")) and near_start and low_cte and low_heading and early_displacement
    return {
        "run_id": run.get("run_id"),
        "backend": run.get("backend"),
        "available": bool(context.get("available")),
        "supports_near_start_contract_issue": supports,
        "near_start": near_start,
        "first_row_index": context.get("first_row_index"),
        "first_sim_time_s": context.get("first_sim_time_s"),
        "ego_displacement_from_start_m": displacement,
        "cross_track_error_m": cte,
        "heading_error_rad": heading,
        "control_source": context.get("control_source"),
    }


def _safety_event_check(runs: Sequence[Mapping[str, Any]], event_name: str) -> dict[str, Any]:
    triggered = [run for run in runs if str(run.get("failure_reason") or "") == event_name]
    missing = [run for run in runs if triggered and not _run_has_safety_event_evidence(run, event_name)]
    return {
        "event_name": event_name,
        "required_for_comparison": bool(triggered),
        "comparison_blocking": bool(missing),
        "triggered_run_ids": [run.get("run_id") for run in triggered],
        "missing_evidence_run_ids": [run.get("run_id") for run in missing],
        "run_evidence": [
            {
                "run_id": run.get("run_id"),
                "backend": run.get("backend"),
                "phase1_status": run.get("phase1_status"),
                "failure_reason": run.get("failure_reason"),
                "available": _run_has_safety_event_evidence(run, event_name),
                "evidence": _event_evidence_for_run(run, event_name),
            }
            for run in runs
        ],
    }


def _run_has_safety_event_evidence(run: Mapping[str, Any], event_name: str) -> bool:
    evidence = _event_evidence_for_run(run, event_name)
    return bool(evidence.get("available"))


def _event_evidence_for_run(run: Mapping[str, Any], event_name: str) -> dict[str, Any]:
    safety_evidence = _run_safety_event_evidence(run)
    event = safety_evidence.get(event_name)
    return dict(event) if isinstance(event, Mapping) else {"available": False, "missing_reason": "not_reported"}


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Scenario Comparison",
        "",
        f"Scenario: `{report.get('scenario_id')}`",
        f"Status: `{report.get('comparison_status')}`",
        f"Comparison id: `{report.get('comparison_id')}`",
        f"Scenario case: `{report.get('scenario_case')}`",
        f"Target status: `{report.get('comparison_target_status')}`",
        f"Reason: `{report.get('reason')}`",
        "",
        "Backend coverage:",
        "",
        "```json",
        json.dumps(report.get("backend_coverage") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "| Run | Backend | Status | Reason | Handoff stage | Handoff reason | Control signature | Control diagnosis | Link blocker | Control-health reason | Lane-event class | Backend loss? |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report.get("backend_results") or []:
        if isinstance(item, Mapping):
            phase1_metrics = item.get("phase1_metrics") if isinstance(item.get("phase1_metrics"), Mapping) else {}
            control_motion = (
                phase1_metrics.get("control_motion") if isinstance(phase1_metrics.get("control_motion"), Mapping) else {}
            )
            derived = (
                phase1_metrics.get("derived_blocker_evidence")
                if isinstance(phase1_metrics.get("derived_blocker_evidence"), Mapping)
                else {}
            )
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
            lines.append(
                f"| {item.get('run_id')} | {item.get('backend')} | {item.get('phase1_status')} | "
                f"{item.get('failure_reason')} | {item.get('apollo_control_handoff_failure_stage')} | "
                f"{item.get('apollo_control_handoff_failure_reason') or ','.join(item.get('apollo_control_handoff_failure_reasons') or []) or None} | "
                f"{control_motion.get('stuck_control_signature')} | "
                f"{control_motion.get('source_brake_interpretation')} | "
                f"{link_health.get('primary_blocker')} | "
                f"{control_health.get('failure_reason')} | "
                f"{lane_event.get('departure_classification')} | "
                f"{item.get('counts_as_backend_loss')} |"
            )
    lines.append("")
    safety_context = report.get("safety_event_context")
    if isinstance(safety_context, Mapping) and safety_context.get("comparison_blocking"):
        lines.extend(
            [
                "Safety-event context:",
                "",
                "```json",
                json.dumps(safety_context, indent=2, sort_keys=True),
                "```",
                "",
            ]
        )
    lines.append(str(report.get("claim_boundary") or ""))
    return "\n".join(lines) + "\n"


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _apollo_handoff_primary_reason(
    *,
    apollo_control_handoff: Mapping[str, Any],
    process_health: Mapping[str, Any],
    planning_control_handoff: Mapping[str, Any],
) -> str | None:
    stage = apollo_control_handoff.get("failure_stage")
    if stage == "process_health":
        reason = process_health.get("crash_reason")
        if reason:
            return str(reason)
    if stage == "planning_control_handoff":
        reasons = planning_control_handoff.get("failure_reasons")
        if isinstance(reasons, list) and reasons:
            return str(reasons[0])
    blocking = apollo_control_handoff.get("blocking_reasons")
    if isinstance(blocking, list) and blocking:
        return str(blocking[0])
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _write_combined_v_t_gap(report: Mapping[str, Any], output_path: Path) -> bool:
    rows: list[dict[str, Any]] = []
    for run in report.get("participating_runs") or []:
        if not isinstance(run, Mapping):
            continue
        if not _run_evaluable(run) or not _run_target_metric_evaluable(run):
            continue
        csv_path = Path(str((run.get("artifact_paths") or {}).get("v_t_gap", ""))).with_name("v_t_gap.csv")
        if not csv_path.exists():
            continue
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                enriched = dict(row)
                enriched["run_id"] = run.get("run_id")
                enriched["backend"] = run.get("backend")
                enriched["backend_type"] = run.get("backend_type")
                rows.append(enriched)
    if not rows:
        return False
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "backend",
        "backend_type",
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
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    return True
