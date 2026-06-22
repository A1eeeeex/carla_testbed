from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs

PHASE1_ACCEPTANCE_VERIFICATION_SCHEMA_VERSION = "phase1_acceptance_verification.v1"
STATUS_DONE = "DONE"
STATUS_INVALID = "INVALID"
STATUS_PARTIAL = "PARTIAL"


def verify_phase1_acceptance(
    comparison_dir: str | Path,
    *,
    declared_summary: Mapping[str, Any] | None = None,
    declared_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Recompute Phase 1 acceptance inputs from actual run artifacts.

    `comparison_summary.json` is treated as a declaration. The trust root is
    the run directories referenced by the comparison manifest/summary plus the
    artifacts inside those run directories.
    """

    root = Path(comparison_dir).expanduser()
    summary = dict(declared_summary or _read_json(root / "comparison_summary.json"))
    manifest = dict(declared_manifest or _read_json(root / "comparison_manifest.json"))
    declared_runs = [
        dict(item) for item in summary.get("participating_runs") or [] if isinstance(item, Mapping)
    ]
    resolved = _resolve_run_dirs(root, summary=summary, manifest=manifest)
    path_errors = [item for item in resolved if item.get("error")]
    run_dirs = [Path(str(item["resolved_path"])) for item in resolved if not item.get("error")]

    if path_errors or len(run_dirs) < 2:
        return _verification_payload(
            root=root,
            summary=summary,
            manifest=manifest,
            declared_runs=declared_runs,
            resolved_runs=resolved,
            recomputed={},
            recomputed_gates={},
            mismatches=[
                {
                    "field": "participating_run_dirs",
                    "declared": [item.get("raw_path") for item in resolved],
                    "actual": "invalid",
                    "reason": item.get("error") or "insufficient_run_dirs",
                }
                for item in (path_errors or resolved)
            ],
            verification_status="failed",
            acceptance_status=STATUS_INVALID,
            blocking_reasons=["participating_run_dirs_invalid"],
        )

    recomputed = compare_scenario_runs(run_dirs)
    recomputed_gates = _recomputed_acceptance_gates(recomputed, root)
    mismatches = _declared_actual_mismatches(summary, recomputed)
    mismatches.extend(_declared_hash_mismatches(root, summary=summary, manifest=manifest))
    blocking_reasons = [name for name, passed in recomputed_gates.items() if not passed]
    if mismatches:
        blocking_reasons.append("declared_actual_mismatch")
    verification_status = "passed" if not blocking_reasons else "failed"
    acceptance_status = STATUS_DONE if verification_status == "passed" else STATUS_PARTIAL
    if recomputed.get("comparison_status") == "invalid":
        acceptance_status = STATUS_INVALID
    return _verification_payload(
        root=root,
        summary=summary,
        manifest=manifest,
        declared_runs=declared_runs,
        resolved_runs=resolved,
        recomputed=recomputed,
        recomputed_gates=recomputed_gates,
        mismatches=mismatches,
        verification_status=verification_status,
        acceptance_status=acceptance_status,
        blocking_reasons=blocking_reasons,
    )


def _verification_payload(
    *,
    root: Path,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    declared_runs: Sequence[Mapping[str, Any]],
    resolved_runs: Sequence[Mapping[str, Any]],
    recomputed: Mapping[str, Any],
    recomputed_gates: Mapping[str, Any],
    mismatches: list[dict[str, Any]],
    verification_status: str,
    acceptance_status: str,
    blocking_reasons: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": PHASE1_ACCEPTANCE_VERIFICATION_SCHEMA_VERSION,
        "comparison_dir": str(root),
        "verification_status": verification_status,
        "acceptance_status": acceptance_status,
        "declared_comparison_status": summary.get("comparison_status"),
        "declared_comparison_target_status": summary.get("comparison_target_status"),
        "recomputed_comparison_status": recomputed.get("comparison_status"),
        "recomputed_comparison_target_status": recomputed.get("comparison_target_status"),
        "declared_comparison_id": summary.get("comparison_id") or manifest.get("comparison_id"),
        "recomputed_comparison_id": recomputed.get("comparison_id"),
        "declared_scenario_case": summary.get("scenario_case") or manifest.get("scenario_case"),
        "recomputed_scenario_case": recomputed.get("scenario_case"),
        "declared_participating_runs": [_compact_declared_run(item) for item in declared_runs],
        "recomputed_participating_runs": [
            _compact_recomputed_run(item) for item in recomputed.get("participating_runs") or []
        ],
        "resolved_run_dirs": [dict(item) for item in resolved_runs],
        "recomputed_gates": dict(recomputed_gates),
        "mismatches": mismatches,
        "blocking_reasons": blocking_reasons,
        "verified_run_ids": [
            str(run.get("run_id"))
            for run in recomputed.get("participating_runs") or []
            if run.get("run_id")
        ],
        "verified_hashes": _verified_hashes(recomputed.get("participating_runs") or []),
        "claim_boundary": (
            "Phase 1 acceptance verification recomputes comparison inputs from actual run artifacts. "
            "Declared comparison summaries are not a trust root."
        ),
    }


def _resolve_run_dirs(
    comparison_dir: Path,
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    raw_paths: list[str] = []
    for item in manifest.get("participating_run_dirs") or []:
        if item:
            raw_paths.append(str(item))
    if not raw_paths:
        for run in summary.get("participating_runs") or []:
            if isinstance(run, Mapping) and run.get("run_dir"):
                raw_paths.append(str(run["run_dir"]))
    resolved: list[dict[str, Any]] = []
    for raw in raw_paths:
        path = Path(raw).expanduser()
        if not path.is_absolute() and ".." in path.parts:
            resolved.append({"raw_path": raw, "error": "path_traversal_rejected"})
            continue
        candidates = [path]
        if not path.is_absolute():
            candidates.extend([comparison_dir / path, comparison_dir.parent / path])
        selected = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
        resolved.append(
            {
                "raw_path": raw,
                "resolved_path": str(selected.resolve() if selected.exists() else selected),
                "exists": selected.exists(),
                "error": None if selected.exists() else "run_dir_missing",
            }
        )
    return resolved


def _recomputed_acceptance_gates(recomputed: Mapping[str, Any], comparison_dir: Path) -> dict[str, Any]:
    runs = [dict(item) for item in recomputed.get("participating_runs") or [] if isinstance(item, Mapping)]
    scenario_cases = {str(run.get("scenario_case") or "") for run in runs if run.get("scenario_case")}
    apollo_runs = [run for run in runs if run.get("backend_type") == "apollo_reference_backend"]
    planning_runs = [run for run in runs if run.get("backend_type") == "planning_control_backend"]
    validity = (
        recomputed.get("validity_gates")
        if isinstance(recomputed.get("validity_gates"), Mapping)
        else {}
    )
    return {
        "same_scenario_case": len(scenario_cases) == 1 and bool(scenario_cases),
        "backend_coverage": len(apollo_runs) == 1 and len(planning_runs) == 1,
        "comparison_valid": (
            recomputed.get("comparison_status") == "comparable"
            and recomputed.get("comparison_target_status") == "apollo_vs_planning_control_evaluable"
        ),
        "blocking_assists_absent": validity.get("blocking_assists_absent") is True,
        "scenario_interaction_evaluable": validity.get("interaction_complete") is True,
        "target_metric_evaluable": validity.get("target_metric_valid") is True,
        "artifact_complete": validity.get("artifact_complete") is True,
        "comparison_curves_present": (
            not _comparison_curves_required(runs)
            or (comparison_dir / "comparison_curves" / "v_t_gap_combined.csv").exists()
        ),
    }


def _declared_actual_mismatches(
    summary: Mapping[str, Any],
    recomputed: Mapping[str, Any],
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    _append_mismatch(
        mismatches,
        "comparison_status",
        summary.get("comparison_status"),
        recomputed.get("comparison_status"),
    )
    _append_mismatch(
        mismatches,
        "comparison_target_status",
        summary.get("comparison_target_status"),
        recomputed.get("comparison_target_status"),
    )
    declared_runs = [
        dict(item) for item in summary.get("participating_runs") or [] if isinstance(item, Mapping)
    ]
    actual_by_id = {
        str(run.get("run_id")): run
        for run in recomputed.get("participating_runs") or []
        if isinstance(run, Mapping) and run.get("run_id")
    }
    for declared in declared_runs:
        run_id = str(declared.get("run_id") or "")
        actual = actual_by_id.get(run_id)
        if not actual:
            mismatches.append(
                {"field": f"run[{run_id or '?'}]", "declared": run_id or None, "actual": None}
            )
            continue
        for field in (
            "scenario_case",
            "backend_type",
            "run_evaluable",
            "scenario_interaction_evaluable",
            "target_metric_evaluable",
            "target_metric_status",
            "artifact_complete",
            "phase1_status",
        ):
            if field in declared:
                _append_mismatch(
                    mismatches,
                    f"run[{run_id}].{field}",
                    declared.get(field),
                    actual.get(field),
                )
        if "blocking_assists" in declared:
            _append_mismatch(
                mismatches,
                f"run[{run_id}].blocking_assists",
                sorted(str(item) for item in declared.get("blocking_assists") or []),
                sorted(str(item) for item in actual.get("blocking_assists") or []),
            )
    return mismatches


def _declared_hash_mismatches(
    comparison_dir: Path,
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    declared = []
    for source in (summary.get("artifact_hashes"), manifest.get("artifact_hashes")):
        if isinstance(source, list):
            declared.extend(item for item in source if isinstance(item, Mapping))
    for item in declared:
        raw = str(item.get("path") or "")
        expected = item.get("sha256")
        if not raw or not expected:
            continue
        path = Path(raw).expanduser()
        if not path.is_absolute() and ".." in path.parts:
            mismatches.append({"field": f"hash[{raw}]", "declared": expected, "actual": "path_traversal_rejected"})
            continue
        if not path.is_absolute():
            path = comparison_dir / path
        if not path.exists() or not path.is_file():
            mismatches.append({"field": f"hash[{raw}]", "declared": expected, "actual": "missing"})
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        _append_mismatch(mismatches, f"hash[{raw}]", expected, actual)
    return mismatches


def _append_mismatch(
    mismatches: list[dict[str, Any]],
    field: str,
    declared: Any,
    actual: Any,
) -> None:
    if declared != actual:
        mismatches.append({"field": field, "declared": declared, "actual": actual})


def _compact_declared_run(run: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: run.get(key)
        for key in (
            "run_id",
            "run_dir",
            "scenario_case",
            "backend",
            "backend_type",
            "run_evaluable",
            "scenario_interaction_evaluable",
            "target_metric_evaluable",
            "target_metric_status",
            "artifact_complete",
            "blocking_assists",
        )
        if key in run
    }


def _compact_recomputed_run(run: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: run.get(key)
        for key in (
            "run_id",
            "run_dir",
            "scenario_case",
            "backend",
            "backend_type",
            "phase1_status",
            "failure_reason",
            "run_evaluable",
            "scenario_interaction_evaluable",
            "scenario_interaction_reason",
            "target_metric_evaluable",
            "target_metric_status",
            "target_metric_reason",
            "artifact_complete",
            "artifact_completeness_status",
            "artifact_completeness_source",
            "blocking_assists",
        )
        if key in run
    }


def _verified_hashes(runs: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for run in runs:
        run_dir = Path(str(run.get("run_dir") or ""))
        for rel in (
            "manifest.json",
            "summary.json",
            "analysis/phase1_status/phase1_status.json",
            "analysis/v_t_gap/v_t_gap_report.json",
        ):
            path = run_dir / rel
            if path.exists() and path.is_file():
                hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def _comparison_curves_required(runs: Sequence[Mapping[str, Any]]) -> bool:
    if not runs:
        return True
    statuses = {str(run.get("target_metric_status") or "") for run in runs}
    return not statuses or any(status != "not_applicable" for status in statuses)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}
