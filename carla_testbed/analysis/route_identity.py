from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.apollo_route_contract import analyze_apollo_route_contract_run_dir

ROUTE_IDENTITY_SCHEMA_VERSION = "route_identity.v1"


def analyze_route_identity_from_route_contract(route_contract: Mapping[str, Any]) -> dict[str, Any]:
    identity = route_contract.get("route_identity")
    if not isinstance(identity, Mapping):
        identity = {}
    configured = route_contract.get("configured_scenario_route")
    if not isinstance(configured, Mapping):
        configured = {}
    planning_segment = route_contract.get("latest_planning_active_route_segment")
    if not isinstance(planning_segment, Mapping):
        planning_segment = {}
    route_source = route_contract.get("source")
    if not isinstance(route_source, Mapping):
        route_source = {}

    issues = list(identity.get("issues") or [])
    missing_fields: list[str] = []
    for key in ("route_id", "scenario_route_length_m", "apollo_routing_total_length_m"):
        if route_contract.get(key) is None:
            missing_fields.append(key)
    if not route_contract.get("scenario_route_lane_keys"):
        missing_fields.append("scenario_route_lane_keys")
    if not route_contract.get("apollo_routing_lane_keys"):
        missing_fields.append("apollo_routing_lane_keys")

    route_identity_status = str(route_contract.get("route_identity_status") or identity.get("status") or "unknown")
    route_contract_status = str(route_contract.get("status") or "unknown")
    blocking = list(route_contract.get("blocking_reasons") or [])
    if route_identity_status == "inconsistent" and "route_identity_inconsistent" not in blocking:
        blocking.append("route_identity_inconsistent")

    if blocking:
        status = "fail"
    elif missing_fields:
        status = "insufficient_data"
    elif route_contract_status in {"pass", "warn"} and route_identity_status not in {"inconsistent", "unknown"}:
        status = route_contract_status
    else:
        status = "insufficient_data"

    return {
        "schema_version": ROUTE_IDENTITY_SCHEMA_VERSION,
        "status": status,
        "run_id": route_contract.get("run_id"),
        "scenario_id": route_contract.get("scenario_id"),
        "route_id": route_contract.get("route_id"),
        "map": configured.get("map") or configured.get("map_name"),
        "comparison_frame": route_contract.get("comparison_frame"),
        "start_pose_carla": configured.get("start_pose_carla") or route_contract.get("scenario_start_xy_carla"),
        "goal_pose_carla": configured.get("goal_pose_carla") or route_contract.get("scenario_goal_xy_carla"),
        "start_pose_apollo": route_contract.get("scenario_start_xy_apollo"),
        "goal_pose_apollo": route_contract.get("scenario_goal_xy_apollo"),
        "start_lane_id": route_contract.get("scenario_start_lane"),
        "goal_lane_id": route_contract.get("scenario_goal_lane"),
        "expected_lane_sequence": list(route_contract.get("scenario_route_lane_keys") or []),
        "apollo_routing_lane_sequence": list(route_contract.get("apollo_routing_lane_keys") or []),
        "planning_reference_line_lane_sequence": list(planning_segment.get("lane_keys") or []),
        "length_expected_m": route_contract.get("scenario_route_length_m"),
        "length_apollo_routing_m": route_contract.get("apollo_routing_total_length_m"),
        "length_planning_reference_line_m": planning_segment.get("length_m"),
        "route_identity_status": route_identity_status,
        "route_identity_issues": issues,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": list(route_contract.get("warnings") or []),
        "missing_fields": sorted(set(missing_fields + list(route_contract.get("missing_fields") or []))),
        "source": {
            "apollo_route_contract_report": route_source.get("apollo_route_contract_report"),
            "apollo_route_contract_status": route_contract_status,
        },
        "interpretation_boundary": (
            "Route identity only verifies configured route, Apollo routing response, "
            "and Planning route/reference-line identity. It does not prove behavior, "
            "control quality, or natural-driving success."
        ),
    }


def analyze_route_identity_run_dir(
    run_dir: str | Path,
    *,
    frame_transform: str | Path | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    route_contract = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=frame_transform)
    report = analyze_route_identity_from_route_contract(route_contract)
    report["source"]["run_dir"] = str(Path(run_dir).expanduser())
    return report


def write_route_identity_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "route_identity_report.json"
    summary_path = output / "route_identity_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(route_identity_summary_md(report), encoding="utf-8")
    return {
        "route_identity_report": str(json_path),
        "route_identity_summary": str(summary_path),
    }


def route_identity_summary_md(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Route Identity Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Route ID: `{report.get('route_id')}`",
            f"- Scenario ID: `{report.get('scenario_id')}`",
            f"- Expected length m: `{report.get('length_expected_m')}`",
            f"- Apollo routing length m: `{report.get('length_apollo_routing_m')}`",
            f"- Planning reference-line length m: `{report.get('length_planning_reference_line_m')}`",
            f"- Expected lane sequence: `{', '.join(report.get('expected_lane_sequence') or []) or 'none'}`",
            f"- Apollo routing lane sequence: `{', '.join(report.get('apollo_routing_lane_sequence') or []) or 'none'}`",
            f"- Planning lane sequence: `{', '.join(report.get('planning_reference_line_lane_sequence') or []) or 'none'}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )
