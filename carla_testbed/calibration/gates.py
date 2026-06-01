from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

CALIBRATION_GATE_RESULTS_SCHEMA_VERSION = "calibration_gate_results.v1"

HARD_GATE_ALIASES = {
    "lane097": "097",
    "lane217": "217",
    "junction031": "031",
}
DIRECT_BRIDGE_CADENCE_RATIO_MIN = 0.8


def _read_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _run_results_by_id(report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("run_id")): dict(row)
        for row in (report.get("run_results") or [])
        if row.get("run_id")
    }


def _artifact_path(candidate: Mapping[str, Any], relative: str) -> str | None:
    artifact_dir = candidate.get("artifact_dir")
    if not artifact_dir:
        return None
    return str(Path(str(artifact_dir)) / relative)


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _gate_status(comparison_status: str | None) -> str:
    if comparison_status == "candidate_positive":
        return "pass"
    if comparison_status == "candidate_degraded":
        return "fail"
    if comparison_status:
        return str(comparison_status)
    return "missing"


def _direct_evidence_issues(candidate: Mapping[str, Any], comparison: Mapping[str, Any]) -> list[str]:
    if candidate.get("backend") != "carla_direct":
        return []
    issues: list[str] = []
    contract_status = candidate.get("direct_transport_contract_status")
    if contract_status != "aligned":
        issues.append(f"direct_transport_contract_status={contract_status or 'missing'}")
    cadence = comparison.get("cadence_comparison") or {}
    loc_ratio = _num(cadence.get("bridge_loc_hz_ratio"))
    chassis_ratio = _num(cadence.get("bridge_chassis_hz_ratio"))
    if loc_ratio is None or chassis_ratio is None:
        issues.append("direct_bridge_cadence_ratio=missing")
    elif loc_ratio < DIRECT_BRIDGE_CADENCE_RATIO_MIN or chassis_ratio < DIRECT_BRIDGE_CADENCE_RATIO_MIN:
        issues.append(
            "direct_bridge_cadence_ratio below threshold "
            f"(loc={loc_ratio}, chassis={chassis_ratio}, min={DIRECT_BRIDGE_CADENCE_RATIO_MIN})"
        )
    return issues


def build_gate_results_from_ab_report(report: Mapping[str, Any]) -> dict[str, Any]:
    by_run_id = _run_results_by_id(report)
    gates: dict[str, dict[str, Any]] = {
        gate_id: {
            "status": "missing",
            "reason": "route not found in A/B report",
            "required": True,
            "summary_path": None,
            "route_health_path": None,
        }
        for gate_id in ("097", "217", "031")
    }
    for comparison in report.get("comparisons") or []:
        route_id = str(comparison.get("route_id") or "")
        gate_id = HARD_GATE_ALIASES.get(route_id)
        if gate_id is None:
            continue
        candidate = by_run_id.get(str(comparison.get("candidate_run_id") or ""), {})
        reasons = comparison.get("reasons") or []
        status = _gate_status(str(comparison.get("status") or ""))
        direct_issues = _direct_evidence_issues(candidate, comparison)
        if status == "pass" and direct_issues:
            status = "insufficient_data"
            reasons = [*reasons, *direct_issues]
        gates[gate_id] = {
            "status": status,
            "reason": "; ".join(str(item) for item in reasons) or str(comparison.get("status") or "missing"),
            "required": True,
            "route_id": route_id,
            "candidate_run_id": comparison.get("candidate_run_id"),
            "baseline_run_id": comparison.get("baseline_run_id"),
            "summary_path": _artifact_path(candidate, "summary.json"),
            "route_health_path": _artifact_path(candidate, "analysis/route_health/route_health.json"),
            "failure_reason": candidate.get("failure_reason"),
            "artifact_complete": candidate.get("artifact_complete"),
            "direct_transport_contract_status": candidate.get("direct_transport_contract_status"),
            "direct_stale_world_frame_policy": candidate.get("direct_stale_world_frame_policy"),
            "direct_control_apply_mode": candidate.get("direct_control_apply_mode"),
            "cadence_comparison": comparison.get("cadence_comparison") or {},
            "direct_evidence_issues": direct_issues,
        }
    return {
        "schema_version": CALIBRATION_GATE_RESULTS_SCHEMA_VERSION,
        "source_type": "ab_report",
        "source_batch_id": report.get("batch_id"),
        "gates": gates,
        "promotion_allowed": all(gate.get("status") == "pass" for gate in gates.values()),
        "claim_supported": "No-regression gate status only; this is not control-actuation calibration evidence.",
    }


def build_gate_results_from_ab_report_file(path: str | Path) -> dict[str, Any]:
    return build_gate_results_from_ab_report(_read_json(path))


def write_gate_results(path: str | Path, gate_results: Mapping[str, Any]) -> str:
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(gate_results, indent=2, sort_keys=True), encoding="utf-8")
    return str(output)
