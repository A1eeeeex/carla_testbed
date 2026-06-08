from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import GatePlan, RunPlan

from .bundle import build_evidence_bundle
from .rules import evaluate_rules

GATE_REPORT_SCHEMA_VERSION = "platform_gate_report.v1"


def run_gate(
    run_dir: str | Path,
    *,
    plan: RunPlan | Mapping[str, Any] | None = None,
    gate: GatePlan | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    bundle = build_evidence_bundle(run_dir, plan=plan)
    plan_payload = plan.to_dict() if isinstance(plan, RunPlan) else dict(plan or {})
    gate_payload = _gate_payload(gate, plan_payload)
    fail_on = {str(item) for item in gate_payload.get("fail_on_status") or []}
    can_claim_profile = bool(gate_payload.get("can_claim_natural_driving", False))
    required = list(gate_payload.get("claim_requires") or [])

    checks: list[dict[str, Any]] = []
    for artifact_name, artifact in (bundle.get("artifacts") or {}).items():
        if not isinstance(artifact, Mapping):
            continue
        status = str(artifact.get("status") or "missing")
        if status in fail_on:
            checks.append(
                {
                    "name": artifact_name,
                    "status": "fail",
                    "reason": f"artifact status {status!r} is configured as fail_on_status",
                    "path": artifact.get("path"),
                }
            )

    missing_required = list(bundle.get("missing_required_evidence") or [])
    for name in missing_required:
        checks.append(
            {
                "name": name,
                "status": "insufficient_data",
                "reason": "required evidence missing from bundle",
                "path": None,
            }
        )
    rules = list(gate_payload.get("rules") or [])
    rule_results = evaluate_rules(rules, bundle=bundle) if rules else []
    for rule_result in rule_results:
        if rule_result.get("status") in {"fail", "insufficient_data"}:
            checks.append(
                {
                    "name": f"rule:{rule_result.get('id')}",
                    "status": rule_result.get("status"),
                    "reason": rule_result.get("blocking_reason") or "rule did not pass",
                    "path": f"{rule_result.get('report')}:{rule_result.get('path')}",
                    "actual": rule_result.get("actual"),
                    "expected": rule_result.get("expected"),
                }
            )

    status = "pass"
    if checks:
        status = "fail" if any(check["status"] == "fail" for check in checks) else "insufficient_data"
    if can_claim_profile and missing_required and status != "fail":
        status = "insufficient_data"

    can_claim = can_claim_profile and status == "pass" and not missing_required
    why_not = []
    if not can_claim_profile:
        why_not.append("gate_profile_not_claim_grade")
    if status != "pass":
        why_not.append(f"gate_status_{status}")
    if missing_required:
        why_not.append("missing_required_evidence")

    return {
        "schema_version": GATE_REPORT_SCHEMA_VERSION,
        "run_dir": str(Path(run_dir).expanduser()),
        "run_id": bundle.get("run_id"),
        "scenario_id": bundle.get("scenario_id"),
        "scenario_class": bundle.get("scenario_class"),
        "route_id": bundle.get("route_id"),
        "backend": bundle.get("backend"),
        "ego_control_source": bundle.get("ego_control_source"),
        "scenario_actor_control_source": bundle.get("scenario_actor_control_source"),
        "background_traffic_control_source": bundle.get("background_traffic_control_source"),
        "background_walker_control_source": bundle.get("background_walker_control_source"),
        "control_source_boundary": {
            "ego_control_source": bundle.get("ego_control_source"),
            "scenario_actor_control_source": bundle.get("scenario_actor_control_source"),
            "background_traffic_control_source": bundle.get("background_traffic_control_source"),
            "background_walker_control_source": bundle.get("background_walker_control_source"),
        },
        "gate_profile": gate_payload.get("profile") or gate_payload.get("name") or "unknown",
        "status": status,
        "can_claim_unassisted_natural_driving": can_claim,
        "why_not_claimable": why_not,
        "claim_requires": required,
        "fail_on_status": sorted(fail_on),
        "checks": checks,
        "rules": rule_results,
        "evidence_bundle_status": bundle.get("status"),
        "missing_required_evidence": missing_required,
        "interpretation_boundary": (
            "Gate report can only summarize available evidence. Missing artifacts are "
            "insufficient_data, not proof that Apollo/Autoware behavior failed."
        ),
    }


def write_gate_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "gate_report.json"
    summary_path = output / "gate_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(gate_summary_md(report), encoding="utf-8")
    return {"gate_report": str(json_path), "gate_summary": str(summary_path)}


def run_and_write_gate(
    run_dir: str | Path,
    *,
    out_dir: str | Path | None = None,
    plan: RunPlan | Mapping[str, Any] | None = None,
    gate: GatePlan | Mapping[str, Any] | None = None,
) -> dict[str, str]:
    root = Path(run_dir).expanduser()
    report = run_gate(root, plan=plan, gate=gate)
    output = Path(out_dir).expanduser() if out_dir is not None else root / "analysis" / "gate"
    return write_gate_report(report, output)


def gate_summary_md(report: Mapping[str, Any]) -> str:
    lines = [
        "# Platform Gate Summary",
        "",
        f"- Run ID: `{report.get('run_id')}`",
        f"- Scenario: `{report.get('scenario_id')}` / `{report.get('scenario_class')}`",
        f"- Gate profile: `{report.get('gate_profile')}`",
        f"- Status: `{report.get('status')}`",
        f"- Control sources: ego=`{report.get('ego_control_source')}`, "
        f"scenario=`{report.get('scenario_actor_control_source')}`, "
        f"background_vehicles=`{report.get('background_traffic_control_source')}`, "
        f"background_walkers=`{report.get('background_walker_control_source')}`",
        f"- Can claim unassisted natural driving: `{report.get('can_claim_unassisted_natural_driving')}`",
        f"- Why not claimable: `{', '.join(report.get('why_not_claimable') or []) or 'none'}`",
        "",
        "## Checks",
        "",
    ]
    checks = report.get("checks") or []
    if not checks:
        lines.append("- No blocking checks emitted.")
    else:
        for check in checks:
            lines.append(
                f"- `{check.get('name')}`: `{check.get('status')}` - {check.get('reason')}"
            )
    rules = report.get("rules") or []
    if rules:
        lines.extend(["", "## Rules", ""])
        for rule in rules:
            lines.append(
                f"- `{rule.get('id')}`: `{rule.get('status')}` actual=`{rule.get('actual')}` "
                f"expected `{rule.get('op')}` `{rule.get('expected')}`"
            )
    lines.append("")
    lines.append(str(report.get("interpretation_boundary") or ""))
    lines.append("")
    return "\n".join(lines)


def _gate_payload(gate: GatePlan | Mapping[str, Any] | None, plan_payload: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(gate, GatePlan):
        return {
            "profile": gate.profile,
            "can_claim_natural_driving": gate.can_claim_natural_driving,
            "claim_requires": list(gate.claim_requires),
            "fail_on_status": list(gate.fail_on_status),
            "rules": [dict(rule) for rule in gate.rules],
        }
    if isinstance(gate, Mapping):
        return dict(gate)
    plan_gate = plan_payload.get("gate")
    return dict(plan_gate) if isinstance(plan_gate, Mapping) else {}
