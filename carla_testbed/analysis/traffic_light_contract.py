from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.adapters.apollo.traffic_light_gt import (
    TrafficLightMapping,
    iter_traffic_light_mappings,
    load_traffic_light_mappings,
)
from carla_testbed.apollo.town01_contract import (
    check_town01_apollo_contract,
    load_town01_apollo_contract,
)

TRAFFIC_LIGHT_CONTRACT_REPORT_SCHEMA_VERSION = "traffic_light_contract_report.v1"


def build_traffic_light_contract_report(
    town01_contract: Mapping[str, Any],
    traffic_light_mapping: Mapping[str, Any],
    *,
    scenario_class: str | None,
) -> dict[str, Any]:
    town01_report = check_town01_apollo_contract(town01_contract)
    mappings = [
        mapping
        for mapping in iter_traffic_light_mappings(traffic_light_mapping)
        if _mapping_supports_scenario(mapping, scenario_class)
    ]
    signal_by_logical_id = {
        str(item.get("logical_id")): item
        for item in town01_report.get("signal_results") or []
        if isinstance(item, Mapping) and item.get("logical_id")
    }

    status = "pass"
    warnings: list[str] = []
    errors: list[str] = []
    mapping_results: list[dict[str, Any]] = []

    if town01_report.get("status") == "fail":
        warnings.append("town01_apollo_contract_has_failures")
    elif town01_report.get("status") == "warn":
        warnings.append("town01_apollo_contract_warn")

    if not mappings:
        status = "fail"
        errors.append("no_traffic_light_mapping_for_scenario")

    for mapping in mappings:
        result = _check_mapping(mapping, signal_by_logical_id)
        mapping_results.append(result)
        status = _combine_status(status, result["status"])
        warnings.extend(result.get("warnings") or [])
        errors.extend(result.get("issues") or [])

    if not signal_by_logical_id:
        status = _combine_status(status, "warn")
        warnings.append("town01_signal_contract_missing_or_empty")

    return {
        "schema_version": TRAFFIC_LIGHT_CONTRACT_REPORT_SCHEMA_VERSION,
        "status": status,
        "scenario_class": scenario_class,
        "mapping_results": mapping_results,
        "claim_grade_ready": bool(
            status == "pass"
            and mapping_results
            and all(item.get("claim_grade_ready") is True for item in mapping_results)
        ),
        "claim_grade_requirements": {
            "traffic_light_policy": "carla_actual",
            "color_source": ["carla_actor_state", "carla_landmark_state", "carla_traffic_light_actor_state"],
            "min_confidence": 0.99,
            "requires_signal_id": True,
            "requires_stop_line_lane_overlap": True,
        },
        "town01_contract_status": town01_report.get("status"),
        "town01_signal_count": len(signal_by_logical_id),
        "missing_inputs": [],
        "warnings": sorted(set(warnings)),
        "errors": sorted(set(errors)),
        "source": {
            "town01_contract_path": town01_contract.get("_source_path"),
            "traffic_light_mapping_path": traffic_light_mapping.get("_source_path"),
            "traffic_light_mapping_schema_version": traffic_light_mapping.get("schema_version"),
        },
        "interpretation_boundary": (
            "This report checks offline id/stop-line/lane mapping consistency only. It does not prove "
            "red-light stopping, green-light passing, or full Apollo perception reproduction."
        ),
    }


def build_traffic_light_contract_report_files(
    *,
    town01_contract_path: str | Path,
    traffic_light_mapping_path: str | Path,
    scenario_class: str | None,
) -> dict[str, Any]:
    return build_traffic_light_contract_report(
        load_town01_apollo_contract(town01_contract_path),
        load_traffic_light_mappings(traffic_light_mapping_path),
        scenario_class=scenario_class,
    )


def write_traffic_light_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "traffic_light_contract_report.json"
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"traffic_light_contract_report": str(path)}


def build_insufficient_traffic_light_contract_report(
    *,
    scenario_class: str | None,
    missing_inputs: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": TRAFFIC_LIGHT_CONTRACT_REPORT_SCHEMA_VERSION,
        "status": "insufficient_data",
        "scenario_class": scenario_class,
        "mapping_results": [],
        "missing_inputs": sorted(set(missing_inputs)),
        "warnings": ["traffic_light_contract_inputs_missing"],
        "errors": [],
        "claim_grade_ready": False,
        "interpretation_boundary": (
            "Traffic-light contract inputs were missing. This is insufficient_data, not traffic-light behavior evidence."
        ),
    }


def _check_mapping(mapping: TrafficLightMapping, signal_by_logical_id: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    signal = signal_by_logical_id.get(mapping.logical_id)
    result: dict[str, Any] = {
        "logical_id": mapping.logical_id,
        "status": "pass",
        "issues": [],
        "warnings": [],
        "apollo_signal_id": mapping.apollo_signal_id,
        "stop_line_id": mapping.stop_line_id,
        "lane_ids": list(mapping.lane_ids),
        "town01_signal_found": signal is not None,
        "claim_grade_ready": False,
        "stop_line_lane_overlap_evidence": False,
    }
    if signal is None:
        result["status"] = "fail"
        result["issues"].append("town01_signal_contract_missing_for_mapping")
        return result

    _compare_field(result, "apollo_signal_id", mapping.apollo_signal_id, signal.get("apollo_signal_id"))
    _compare_field(result, "stop_line_id", mapping.stop_line_id, signal.get("stop_line_id"))
    signal_lane_ids = {str(item) for item in signal.get("lane_ids") or []}
    mapping_lane_ids = set(mapping.lane_ids)
    if not mapping_lane_ids:
        result["status"] = _combine_status(result["status"], "insufficient_data")
        result["issues"].append("missing_mapping_lane_ids")
    elif not signal_lane_ids:
        result["status"] = _combine_status(result["status"], "insufficient_data")
        result["issues"].append("missing_signal_lane_ids")
    elif not mapping_lane_ids.issubset(signal_lane_ids):
        result["status"] = "fail"
        result["issues"].append("lane_ids_not_in_signal_contract")
    else:
        result["stop_line_lane_overlap_evidence"] = True

    for field, value in (
        ("apollo_signal_id", mapping.apollo_signal_id),
        ("stop_line_id", mapping.stop_line_id),
    ):
        if value is None:
            result["status"] = _combine_status(result["status"], "insufficient_data")
            result["issues"].append(f"missing_{field}")
        elif str(value).startswith("placeholder:"):
            result["status"] = _combine_status(result["status"], "warn")
            result["warnings"].append(f"{field}_placeholder_unverified")

    if mapping.carla_actor_id is None and mapping.carla_landmark_id is None:
        result["status"] = "fail"
        result["issues"].append("missing_carla_actor_or_landmark_id")
    for field, value in (
        ("carla_actor_id", mapping.carla_actor_id),
        ("carla_landmark_id", mapping.carla_landmark_id),
    ):
        if value is not None and str(value).startswith("placeholder:"):
            result["status"] = _combine_status(result["status"], "warn")
            result["warnings"].append(f"{field}_placeholder_unverified")

    signal_status = str(signal.get("status") or "")
    if signal_status == "fail":
        signal_issues = {str(item) for item in signal.get("issues") or []}
        if signal_issues and signal_issues.issubset(
            {
                "missing_apollo_signal_id",
                "missing_stop_line_id",
                "missing_lane_ids",
                "missing_logical_id",
            }
        ):
            result["status"] = _combine_status(result["status"], "insufficient_data")
        else:
            result["status"] = "fail"
        result["issues"].append("town01_signal_contract_failed")
    elif signal_status == "warn":
        result["status"] = _combine_status(result["status"], "warn")
        result["warnings"].append("town01_signal_contract_warn")
    result["claim_grade_ready"] = bool(
        result["status"] == "pass"
        and result["town01_signal_found"]
        and result["apollo_signal_id"]
        and result["stop_line_id"]
        and result["lane_ids"]
        and result["stop_line_lane_overlap_evidence"]
    )
    return result


def _compare_field(result: dict[str, Any], name: str, mapping_value: Any, signal_value: Any) -> None:
    if mapping_value is None or signal_value is None:
        return
    if str(mapping_value) != str(signal_value):
        result["status"] = "fail"
        result["issues"].append(f"{name}_mismatch")


def _mapping_supports_scenario(mapping: TrafficLightMapping, scenario_class: str | None) -> bool:
    if not scenario_class:
        return True
    if mapping.supported_scenarios:
        return scenario_class in set(mapping.supported_scenarios)
    return True


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "insufficient_data": 2, "fail": 3}.get(status, 3)


def _combine_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current
