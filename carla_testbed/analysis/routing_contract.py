from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.apollo_route_contract import analyze_apollo_route_contract_run_dir

ROUTING_CONTRACT_SCHEMA_VERSION = "routing_contract.v1"


def analyze_routing_contract_from_route_contract(route_contract: Mapping[str, Any]) -> dict[str, Any]:
    request = route_contract.get("last_routing_request")
    if not isinstance(request, Mapping):
        request = {}
    response = route_contract.get("last_routing_response")
    if not isinstance(response, Mapping):
        response = {}
    claim_route = route_contract.get("claim_route_contract")
    if not isinstance(claim_route, Mapping):
        claim_route = {}
    startup_route = route_contract.get("startup_route_contract")
    if not isinstance(startup_route, Mapping):
        startup_route = {}

    blocking = list(route_contract.get("blocking_reasons") or [])
    warnings = list(route_contract.get("warnings") or [])
    missing = list(route_contract.get("missing_fields") or [])
    if not request:
        warnings.append("routing_request_evidence_missing")
    if not response:
        warnings.append("routing_response_evidence_missing")
    if claim_route.get("materialized") is not True:
        if "claim_route_not_materialized" not in blocking:
            blocking.append("claim_route_not_materialized")

    if blocking:
        status = "fail"
    elif missing:
        status = "insufficient_data"
    elif warnings:
        status = "warn"
    else:
        status = "pass"

    return {
        "schema_version": ROUTING_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "run_id": route_contract.get("run_id"),
        "scenario_id": route_contract.get("scenario_id"),
        "route_id": route_contract.get("route_id"),
        "routing_phase": route_contract.get("routing_phase"),
        "raw_routing_phase": route_contract.get("raw_routing_phase"),
        "routing_phase_reason": route_contract.get("routing_phase_reason"),
        "last_routing_request": dict(request),
        "last_routing_response": dict(response),
        "startup_route_contract": dict(startup_route),
        "claim_route_contract": dict(claim_route),
        "apollo_routing_total_length_m": route_contract.get("apollo_routing_total_length_m"),
        "scenario_route_length_m": route_contract.get("scenario_route_length_m"),
        "scenario_route_length_source": route_contract.get("scenario_route_length_source"),
        "scenario_route_declared_length_m": route_contract.get("scenario_route_declared_length_m"),
        "scenario_route_claim_length_m": route_contract.get("scenario_route_claim_length_m"),
        "scenario_route_claim_length_source": route_contract.get("scenario_route_claim_length_source"),
        "scenario_route_legacy_length_m": route_contract.get("scenario_route_legacy_length_m"),
        "scenario_route_legacy_length_role": route_contract.get("scenario_route_legacy_length_role"),
        "scenario_route_trace_length_m": route_contract.get("scenario_route_trace_length_m"),
        "scenario_route_trace_length_source": route_contract.get("scenario_route_trace_length_source"),
        "scenario_route_length_consistency_status": route_contract.get(
            "scenario_route_length_consistency_status"
        ),
        "scenario_route_length_consistency_reason": route_contract.get(
            "scenario_route_length_consistency_reason"
        ),
        "routing_length_ratio": route_contract.get("routing_length_ratio"),
        "goal_xy_error_m": route_contract.get("goal_xy_error_m"),
        "start_xy_error_m": route_contract.get("start_xy_error_m"),
        "route_identity_status": route_contract.get("route_identity_status"),
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing)),
        "source": {
            "apollo_route_contract_status": route_contract.get("status"),
        },
        "interpretation_boundary": (
            "Routing contract verifies Apollo routing request/response compatibility "
            "with the configured scenario route. It does not prove Planning consumed "
            "the route or that Control drove the vehicle."
        ),
    }


def analyze_routing_contract_run_dir(
    run_dir: str | Path,
    *,
    frame_transform: str | Path | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    route_contract = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=frame_transform)
    report = analyze_routing_contract_from_route_contract(route_contract)
    report["source"]["run_dir"] = str(Path(run_dir).expanduser())
    return report


def write_routing_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "routing_contract_report.json"
    summary_path = output / "routing_contract_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(routing_contract_summary_md(report), encoding="utf-8")
    return {
        "routing_contract_report": str(json_path),
        "routing_contract_summary": str(summary_path),
    }


def routing_contract_summary_md(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Routing Contract Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Route ID: `{report.get('route_id')}`",
            f"- Routing phase: `{report.get('routing_phase')}`",
            f"- Raw routing phase: `{report.get('raw_routing_phase')}`",
            f"- Route identity status: `{report.get('route_identity_status')}`",
            f"- Scenario length m: `{report.get('scenario_route_length_m')}`",
            f"- Scenario length source: `{report.get('scenario_route_length_source')}`",
            f"- Scenario claim length m: `{report.get('scenario_route_claim_length_m')}`",
            f"- Scenario legacy length m: `{report.get('scenario_route_legacy_length_m')}`",
            f"- Scenario legacy length role: `{report.get('scenario_route_legacy_length_role')}`",
            f"- Scenario trace length m: `{report.get('scenario_route_trace_length_m')}`",
            f"- Scenario length consistency: `{report.get('scenario_route_length_consistency_status')}`",
            f"- Apollo routing length m: `{report.get('apollo_routing_total_length_m')}`",
            f"- Routing length ratio: `{report.get('routing_length_ratio')}`",
            f"- Goal XY error m: `{report.get('goal_xy_error_m')}`",
            f"- Claim route contract: `{json.dumps(report.get('claim_route_contract') or {}, sort_keys=True)}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )
