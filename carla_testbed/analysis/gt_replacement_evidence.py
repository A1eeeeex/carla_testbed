from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.apollo_chain_completion import analyze_apollo_chain_completion_run_dir

GT_REPLACEMENT_EVIDENCE_SCHEMA_VERSION = "gt_replacement_evidence.v1"


def analyze_gt_replacement_evidence_run_dir(
    run_dir: str | Path,
    *,
    reference_path: str | Path = "configs/reference/apollo_reference_chain.yaml",
    replacement_path: str | Path = "configs/reference/apollo_gt_replacement_matrix.yaml",
) -> dict[str, Any]:
    """Build a runtime-facing view of the Apollo GT replacement matrix.

    This intentionally delegates module status computation to
    apollo_chain_completion so the replacement evidence view cannot drift from
    the chain gate used by natural-driving claim checks.
    """

    root = Path(run_dir).expanduser()
    chain = analyze_apollo_chain_completion_run_dir(
        root,
        reference_path=reference_path,
        replacement_path=replacement_path,
    )
    module_statuses = (
        chain.get("module_statuses") if isinstance(chain.get("module_statuses"), Mapping) else {}
    )
    modules = _replacement_modules(module_statuses)
    replacement_modules = [
        module
        for module in modules
        if module.get("replacement_status") in {"gt_replaced", "carla_replaced", "bypassed"}
    ]
    missing_required_evidence = list(chain.get("missing_required_evidence") or [])
    blocking_replacements = [
        module["name"]
        for module in replacement_modules
        if module.get("effective_status") in {"fail", "insufficient_data", "missing"}
        or module.get("hard_gate_eligible") is not True
        or module.get("run_claim_grade") is not True
        or module.get("missing_evidence")
    ]
    gt_replacements_claim_grade = bool(replacement_modules) and not blocking_replacements
    warnings = list(chain.get("warnings") or [])
    if not gt_replacements_claim_grade:
        warnings.append("gt_replacement_evidence_not_claim_grade")

    return {
        "schema_version": GT_REPLACEMENT_EVIDENCE_SCHEMA_VERSION,
        "run_id": chain.get("run_id") or root.name,
        "scenario_id": chain.get("scenario_id"),
        "route_id": chain.get("route_id"),
        "target_capability": chain.get("target_capability"),
        "reference_chain_path": str(Path(reference_path)),
        "replacement_matrix_path": str(Path(replacement_path)),
        "source_chain_completion": {
            "schema_version": chain.get("schema_version"),
            "verdict": chain.get("verdict"),
            "failure_stage": chain.get("failure_stage"),
            "can_claim_truth_input_closed_loop": chain.get("can_claim_truth_input_closed_loop"),
            "can_claim_unassisted_natural_driving": chain.get(
                "can_claim_unassisted_natural_driving"
            ),
        },
        "modules": modules,
        "replacement_modules": replacement_modules,
        "missing_required_evidence": missing_required_evidence,
        "blocking_replacements": sorted(set(blocking_replacements)),
        "gt_replacements_claim_grade": gt_replacements_claim_grade,
        "can_claim_truth_input_closed_loop": bool(chain.get("can_claim_truth_input_closed_loop"))
        and gt_replacements_claim_grade,
        "can_claim_unassisted_natural_driving": bool(
            chain.get("can_claim_unassisted_natural_driving")
        )
        and gt_replacements_claim_grade,
        "failure_stage": chain.get("failure_stage"),
        "warnings": sorted(set(str(item) for item in warnings if item)),
        "verdict": "pass"
        if gt_replacements_claim_grade
        else "fail"
        if any(module.get("effective_status") == "fail" for module in replacement_modules)
        else "insufficient_data",
        "interpretation_boundary": (
            "GT replacement evidence is a runtime artifact audit. It does not create new "
            "module evidence and cannot turn diagnostic or sample-only reports into "
            "claim-grade Apollo natural-driving proof."
        ),
    }


def write_gt_replacement_evidence_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "gt_replacement_evidence_report.json"
    summary_path = output_dir / "gt_replacement_evidence_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(gt_replacement_evidence_summary_md(report), encoding="utf-8")
    return {
        "gt_replacement_evidence_report": str(report_path),
        "gt_replacement_evidence_summary": str(summary_path),
    }


def gt_replacement_evidence_summary_md(report: Mapping[str, Any]) -> str:
    lines = [
        "# Apollo GT Replacement Evidence Summary",
        "",
        f"- Run ID: `{report.get('run_id')}`",
        f"- Scenario: `{report.get('scenario_id')}`",
        f"- Route ID: `{report.get('route_id')}`",
        f"- Target capability: `{report.get('target_capability')}`",
        f"- Verdict: `{report.get('verdict')}`",
        f"- Failure stage: `{report.get('failure_stage')}`",
        f"- GT replacements claim-grade: `{report.get('gt_replacements_claim_grade')}`",
        f"- Can claim truth-input closed loop: `{report.get('can_claim_truth_input_closed_loop')}`",
        f"- Can claim unassisted natural driving: `{report.get('can_claim_unassisted_natural_driving')}`",
        f"- Blocking replacements: `{', '.join(report.get('blocking_replacements') or []) or 'none'}`",
        "",
        "## Replacement Modules",
        "",
    ]
    for module in report.get("replacement_modules") or []:
        if not isinstance(module, Mapping):
            continue
        lines.extend(
            [
                f"### {module.get('name')}",
                f"- replacement_status: `{module.get('replacement_status')}`",
                f"- evidence_status: `{module.get('evidence_status')}`",
                f"- effective_status: `{module.get('effective_status')}`",
                f"- run_claim_grade: `{module.get('run_claim_grade')}`",
                f"- hard_gate_eligible: `{module.get('hard_gate_eligible')}`",
                f"- missing_evidence: `{', '.join(module.get('missing_evidence') or []) or 'none'}`",
                "",
            ]
        )
    lines.extend([str(report.get("interpretation_boundary") or ""), ""])
    return "\n".join(lines)


def _replacement_modules(module_statuses: Mapping[str, Any]) -> list[dict[str, Any]]:
    modules: list[dict[str, Any]] = []
    for name in sorted(module_statuses):
        module = module_statuses.get(name)
        if not isinstance(module, Mapping):
            continue
        observed = module.get("observed_evidence") if isinstance(module.get("observed_evidence"), Mapping) else {}
        missing = [str(key) for key, value in observed.items() if not value]
        modules.append(
            {
                "name": str(name),
                "reference_module": module.get("reference_module"),
                "replacement_status": module.get("replacement_status"),
                "project_matrix_status": module.get("project_matrix_status"),
                "evidence_status": module.get("evidence_status"),
                "run_evidence_status": module.get("run_evidence_status"),
                "effective_status": module.get("effective_status"),
                "run_claim_grade": bool(module.get("run_claim_grade")),
                "hard_gate_eligible": bool(module.get("hard_gate_eligible")),
                "blocking_capabilities": list(module.get("blocking_capabilities") or []),
                "required_evidence": list(module.get("required_evidence") or []),
                "observed_evidence": dict(observed),
                "missing_evidence": sorted(missing),
                "notes": module.get("notes"),
            }
        )
    return modules
