from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.apollo_map_identity import (
    analyze_apollo_map_identity_run_dir,
    write_apollo_map_identity_report,
)
from carla_testbed.analysis.apollo_route_contract import (
    build_lane_equivalence_town01,
)

MAP_ROUTE_ALIGNMENT_SCHEMA_VERSION = "apollo_map_route_alignment.v2"


def analyze_apollo_map_route_alignment_run_dir(
    run_dir: str | Path,
    *,
    expected_apollo_map_name: str = "carla_town01",
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    map_identity_path = _find_first(root, ["artifacts/map_identity_report.json"])
    map_identity = _read_json(map_identity_path)
    if not map_identity:
        map_identity = analyze_apollo_map_identity_run_dir(
            root,
            expected_apollo_map_name=expected_apollo_map_name,
        )
    route_contract = _read_json(
        _find_first(root, ["analysis/apollo_route_contract/apollo_route_contract_report.json"])
    )
    lane_equivalence = _read_json(_find_first(root, ["artifacts/lane_equivalence_town01.json"]))
    lane_equivalence_path = _find_first(root, ["artifacts/lane_equivalence_town01.json"])
    hdmap_projection_rows = _read_jsonl(_find_first(root, ["artifacts/apollo_hdmap_projection.jsonl"]))
    if route_contract and (
        not lane_equivalence
        or str(lane_equivalence.get("status") or "") != "pass"
    ):
        lane_equivalence = build_lane_equivalence_town01(
            route_contract,
            hdmap_projection_rows=hdmap_projection_rows,
        )
        lane_equivalence_path = root / "artifacts" / "lane_equivalence_town01.json"
    hdmap_projection = _read_json(
        _find_first(root, ["analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json"])
    )
    route_identity = _read_json(_find_first(root, ["analysis/route_identity/route_identity_report.json"]))
    reference_line = _read_json(
        _find_first(root, ["analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"])
    )
    return analyze_apollo_map_route_alignment(
        map_identity=map_identity,
        hdmap_projection=hdmap_projection,
        lane_equivalence=lane_equivalence,
        route_contract=route_contract,
        route_identity=route_identity,
        reference_line_contract=reference_line,
        source={
            "run_dir": str(root),
            "map_identity_report": str(map_identity_path) if map_identity_path else None,
            "apollo_hdmap_projection_report": _path_str(
                _find_first(root, ["analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json"])
            ),
            "lane_equivalence_town01": _path_str(lane_equivalence_path),
            "apollo_route_contract_report": _path_str(
                _find_first(root, ["analysis/apollo_route_contract/apollo_route_contract_report.json"])
            ),
            "route_identity_report": _path_str(_find_first(root, ["analysis/route_identity/route_identity_report.json"])),
            "apollo_reference_line_contract_report": _path_str(
                _find_first(root, ["analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"])
            ),
        },
    )


def analyze_apollo_map_route_alignment(
    *,
    map_identity: Mapping[str, Any] | None = None,
    hdmap_projection: Mapping[str, Any] | None = None,
    lane_equivalence: Mapping[str, Any] | None = None,
    route_contract: Mapping[str, Any] | None = None,
    route_identity: Mapping[str, Any] | None = None,
    reference_line_contract: Mapping[str, Any] | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    map_identity = map_identity or {}
    hdmap_projection = hdmap_projection or {}
    lane_equivalence = lane_equivalence or {}
    route_contract = route_contract or {}
    route_identity = route_identity or {}
    reference_line_contract = reference_line_contract or {}

    map_status = _status(map_identity)
    projection_status = _status(hdmap_projection)
    lane_status = _status(lane_equivalence)
    route_status = _status(route_contract)
    route_identity_status = _status(route_identity)
    reference_status = _status(reference_line_contract)

    blocking: list[str] = []
    warnings: list[str] = []
    missing: list[str] = []

    if not map_identity:
        missing.append("map_identity_report")
    if not hdmap_projection:
        missing.append("apollo_hdmap_projection_report")
    if not route_contract:
        missing.append("apollo_route_contract_report")
    if not lane_equivalence:
        missing.append("lane_equivalence_town01")
    if not reference_line_contract:
        missing.append("apollo_reference_line_contract_report")

    blocking.extend(str(item) for item in map_identity.get("blocking_reasons") or [])
    warnings.extend(f"map_identity:{item}" for item in map_identity.get("warnings") or [])
    blocking.extend(str(item) for item in _projection_blocking_reasons(hdmap_projection))
    warnings.extend(f"hdmap_projection:{item}" for item in _projection_warnings(hdmap_projection))
    blocking.extend(str(item) for item in lane_equivalence.get("blocking_reasons") or [])
    warnings.extend(f"lane_equivalence:{item}" for item in lane_equivalence.get("warnings") or [])
    blocking.extend(str(item) for item in route_contract.get("blocking_reasons") or [])
    warnings.extend(f"route_contract:{item}" for item in route_contract.get("warnings") or [])
    blocking.extend(str(item) for item in reference_line_contract.get("blocking_reasons") or [])
    warnings.extend(f"reference_line:{item}" for item in reference_line_contract.get("warnings") or [])

    diagnosis = _diagnosis(
        map_identity=map_identity,
        hdmap_projection=hdmap_projection,
        lane_equivalence=lane_equivalence,
        route_contract=route_contract,
        reference_line_contract=reference_line_contract,
        missing=missing,
    )
    if diagnosis in {
        "map_files_missing",
        "mixed_map_roots",
        "map_hash_mismatch",
        "projection_geometry_bad",
        "routing_not_claim_route",
        "scenario_route_definition_bad",
        "reference_line_missing",
        "boundary_transition_ambiguous",
        "heading_convention_mismatch",
    }:
        status = "fail"
    elif missing or diagnosis in {
        "map_identity_missing",
        "projection_missing",
        "insufficient_artifacts",
        "lane_equivalence_missing",
    }:
        status = "insufficient_data"
    elif any(s == "fail" for s in (map_status, projection_status, lane_status, route_status, reference_status)):
        status = "fail"
    elif any(s == "insufficient_data" for s in (map_status, projection_status, lane_status, route_status, reference_status)):
        status = "insufficient_data"
    elif warnings or any(s == "warn" for s in (map_status, projection_status, lane_status, route_status, reference_status)):
        status = "warn"
    else:
        status = "pass"

    return {
        "schema_version": MAP_ROUTE_ALIGNMENT_SCHEMA_VERSION,
        "status": status,
        "summary": {
            "carla_world": map_identity.get("carla_world"),
            "apollo_map_name": map_identity.get("expected_apollo_map_name"),
            "map_identity_status": map_status,
            "hdmap_projection_status": projection_status,
            "lane_equivalence_status": lane_status,
            "route_contract_status": route_status,
            "route_identity_status": route_identity_status,
            "reference_line_contract_status": reference_status,
        },
        "diagnosis": diagnosis,
        "static_map_identity": _static_map_identity_layer(map_identity),
        "runtime_projection": _runtime_projection_layer(hdmap_projection, lane_equivalence),
        "routing_response": _routing_response_layer(route_contract),
        "planning_reference_line": _planning_reference_line_layer(reference_line_contract, route_contract),
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_artifacts": sorted(set(missing)),
        "next_action": _next_actions(diagnosis),
        "map_identity": _compact_map_identity(map_identity),
        "hdmap_projection": _compact_projection(hdmap_projection),
        "lane_equivalence": lane_equivalence,
        "route_contract": _compact_route_contract(route_contract),
        "reference_line_contract": _compact_reference_line(reference_line_contract),
        "source": dict(source or {}),
        "interpretation_boundary": (
            "This report verifies map identity, official HDMap projection, lane namespace "
            "equivalence, Apollo routing identity, and reference-line materialization. "
            "It does not change the route, enable fallback routing, or prove closed-loop driving."
        ),
    }


def write_apollo_map_route_alignment_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
    *,
    lane_equivalence: Mapping[str, Any] | None = None,
    lane_equivalence_out_dir: str | Path | None = None,
) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "apollo_map_route_alignment_report.json"
    summary_path = output / "apollo_map_route_alignment_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_map_route_alignment_summary_md(report), encoding="utf-8")
    outputs = {
        "apollo_map_route_alignment_report": str(json_path),
        "apollo_map_route_alignment_summary": str(summary_path),
    }
    if lane_equivalence is not None and lane_equivalence_out_dir is not None:
        lane_dir = Path(lane_equivalence_out_dir).expanduser()
        lane_dir.mkdir(parents=True, exist_ok=True)
        lane_path = lane_dir / "lane_equivalence_town01.json"
        lane_path.write_text(json.dumps(dict(lane_equivalence), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        outputs["lane_equivalence_town01"] = str(lane_path)
    return outputs


def write_map_identity_for_run_if_needed(
    run_dir: str | Path,
    *,
    expected_apollo_map_name: str = "carla_town01",
) -> dict[str, str]:
    root = Path(run_dir).expanduser()
    report = analyze_apollo_map_identity_run_dir(root, expected_apollo_map_name=expected_apollo_map_name)
    return write_apollo_map_identity_report(report, root / "artifacts")


def apollo_map_route_alignment_summary_md(report: Mapping[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    return "\n".join(
        [
            "# Apollo Map-Route Alignment Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Diagnosis: `{report.get('diagnosis')}`",
            f"- CARLA world: `{summary.get('carla_world')}`",
            f"- Apollo map name: `{summary.get('apollo_map_name')}`",
            f"- Map identity: `{summary.get('map_identity_status')}`",
            f"- HDMap projection: `{summary.get('hdmap_projection_status')}`",
            f"- Lane equivalence: `{summary.get('lane_equivalence_status')}`",
            f"- Route contract: `{summary.get('route_contract_status')}`",
            f"- Reference-line contract: `{summary.get('reference_line_contract_status')}`",
            f"- Static map identity layer: `{_nested(report, 'static_map_identity.status')}`",
            f"- Runtime projection layer: `{_nested(report, 'runtime_projection.status')}`",
            f"- Routing response layer: `{_nested(report, 'routing_response.status')}`",
            f"- Planning reference-line layer: `{_nested(report, 'planning_reference_line.status')}`",
            f"- Missing artifacts: `{', '.join(report.get('missing_artifacts') or []) or 'none'}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            f"- Next action: `{'; '.join(report.get('next_action') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )


def _diagnosis(
    *,
    map_identity: Mapping[str, Any],
    hdmap_projection: Mapping[str, Any],
    lane_equivalence: Mapping[str, Any],
    route_contract: Mapping[str, Any],
    reference_line_contract: Mapping[str, Any],
    missing: Sequence[str],
) -> str:
    map_blocking = set(map_identity.get("blocking_reasons") or [])
    if "map_identity_report" in missing:
        return "map_identity_missing"
    if "map_files_missing" in map_blocking:
        return "map_files_missing"
    if "map_hash_mismatch" in map_blocking:
        return "map_hash_mismatch"
    if "mixed_map_root" in map_blocking:
        return "mixed_map_roots"
    if missing:
        if set(missing) == {"apollo_reference_line_contract_report"}:
            return "reference_line_missing"
        if "apollo_hdmap_projection_report" in missing:
            return "projection_missing"
        if "lane_equivalence_town01" in missing:
            return "lane_equivalence_missing"
        return "insufficient_artifacts"

    projection_status = _status(hdmap_projection)
    projection_blocking = set(_projection_blocking_reasons(hdmap_projection))
    projection_missing = set(_projection_missing_or_insufficient(hdmap_projection))
    if projection_status == "fail" or projection_blocking:
        return "projection_geometry_bad"
    if projection_status == "insufficient_data" or projection_missing:
        return "projection_missing"

    lane_status = _status(lane_equivalence)
    lane_blocking = set(lane_equivalence.get("blocking_reasons") or [])
    if lane_status == "insufficient_data":
        return "lane_equivalence_missing"
    if "apollo_routing_not_claim_route" in lane_blocking:
        return "routing_not_claim_route"
    if lane_status == "fail":
        return _lane_failure_diagnosis(lane_equivalence)

    route_blocking = set(route_contract.get("blocking_reasons") or [])
    if "scenario_route_length_inconsistent" in route_blocking:
        return "scenario_route_definition_bad"
    if route_blocking:
        return "routing_not_claim_route"
    if _status(route_contract) == "insufficient_data":
        return "insufficient_artifacts"

    ref_status = _status(reference_line_contract)
    ref_blocking = set(reference_line_contract.get("blocking_reasons") or [])
    ref_warnings = set(reference_line_contract.get("warnings") or [])
    if (
        ref_status == "fail"
        or "reference_line_materialization_missing" in ref_blocking
        or "planning_nonempty_but_reference_line_unproven" in ref_warnings
    ):
        return "reference_line_missing"
    if ref_status == "insufficient_data":
        return "reference_line_missing"

    return "pass"


def _next_actions(diagnosis: str) -> list[str]:
    table = {
        "map_files_missing": ["Install/export base_map.txt, routing_map.txt, and sim_map.txt from the same Apollo map root."],
        "map_hash_mismatch": ["Rebuild or restage map components so base_map, routing_map, and sim_map share the same derivation chain."],
        "mixed_map_roots": ["Align Dreamview, bridge effective map_file, projection exporter --map-dir, and Apollo runtime map root."],
        "projection_geometry_bad": ["Inspect Apollo HDMap projection heading/lateral errors before blaming Planning or Control."],
        "projection_missing": ["Run tools/export_apollo_hdmap_projection.py with Apollo map_xysl; do not substitute CARLA waypoint projection."],
        "lane_equivalence_missing": ["Create or verify lane_equivalence_town01.json using ordered lane windows and official HDMap projection evidence."],
        "boundary_transition_ambiguous": ["Inspect route sample density and Apollo lane topology around the failed lane-window boundary before changing maps or control."],
        "heading_convention_mismatch": ["Compare route trace heading, chord heading, and Apollo lane tangent heading around failed pairs; do not relax thresholds without evidence."],
        "routing_not_claim_route": ["Compare scenario route length/start/goal/lane window with the decoded Apollo RoutingResponse."],
        "scenario_route_definition_bad": ["Fix scenario route definition evidence; do not rewrite it from Apollo RoutingResponse."],
        "reference_line_missing": ["Collect planning_route_segment_debug and apollo_reference_line_debug rows to prove reference-line materialization."],
        "insufficient_artifacts": ["Regenerate map identity, HDMap projection, route contract, route identity, and reference-line reports."],
        "pass": ["Proceed to localization/control/perception/natural-driving gates; this report alone is not a driving pass."],
    }
    return table.get(diagnosis, ["Inspect missing or failing alignment artifacts."])


def _lane_failure_diagnosis(lane_equivalence: Mapping[str, Any]) -> str:
    pairs = lane_equivalence.get("pairs")
    if not isinstance(pairs, Sequence) or isinstance(pairs, (str, bytes)):
        pairs = lane_equivalence.get("equivalence")
    classifications = {
        str(pair.get("classification"))
        for pair in pairs or []
        if isinstance(pair, Mapping) and pair.get("classification")
    }
    if classifications & {"routing_not_claim_route", "topology_mismatch"}:
        return "routing_not_claim_route"
    if classifications & {"heading_convention_mismatch", "sampling_density_insufficient"}:
        return "heading_convention_mismatch"
    if "boundary_transition_ambiguous" in classifications:
        return "boundary_transition_ambiguous"
    return "routing_not_claim_route"


def _projection_blocking_reasons(report: Mapping[str, Any]) -> list[str]:
    reasons = list(report.get("blocking_reasons") or [])
    projection = report.get("projection") if isinstance(report.get("projection"), Mapping) else {}
    reasons.extend(projection.get("blocking_reasons") or [])
    return [str(item) for item in reasons if item]


def _projection_warnings(report: Mapping[str, Any]) -> list[str]:
    warnings = list(report.get("warnings") or [])
    projection = report.get("projection") if isinstance(report.get("projection"), Mapping) else {}
    warnings.extend(projection.get("warnings") or [])
    warnings.extend(projection.get("insufficient_reasons") or [])
    return [str(item) for item in warnings if item]


def _projection_missing_or_insufficient(report: Mapping[str, Any]) -> list[str]:
    projection = report.get("projection") if isinstance(report.get("projection"), Mapping) else {}
    return [
        *[str(item) for item in report.get("missing_fields") or [] if item],
        *[str(item) for item in report.get("insufficient_reasons") or [] if item],
        *[str(item) for item in projection.get("missing_fields") or [] if item],
        *[str(item) for item in projection.get("insufficient_reasons") or [] if item],
    ]


def _static_map_identity_layer(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": _status(report),
        "map_root": report.get("selected_apollo_map_root"),
        "apollo_map_root_candidates": list(report.get("apollo_map_root_candidates") or []),
        "dreamview_selected_map": report.get("dreamview_selected_map"),
        "bridge_effective_map_file": report.get("bridge_effective_map_file"),
        "projection_exporter_map_dir": report.get("projection_exporter_map_dir"),
        "base_map": {
            "path": report.get("base_map_path"),
            "exists": report.get("base_map_exists"),
            "sha256": report.get("base_map_sha256"),
        },
        "routing_map": {
            "path": report.get("routing_map_path"),
            "exists": report.get("routing_map_exists"),
            "sha256": report.get("routing_map_sha256"),
        },
        "sim_map": {
            "path": report.get("sim_map_path"),
            "exists": report.get("sim_map_exists"),
            "sha256": report.get("sim_map_sha256"),
        },
        "map_root_consistent": report.get("map_root_consistent"),
        "blocking_reasons": list(report.get("blocking_reasons") or []),
        "warnings": list(report.get("warnings") or []),
        "claim_boundary": (
            "Static files/configs can confirm map identity and hashes only; "
            "they cannot prove scenario route, routing response, or reference-line alignment."
        ),
    }


def _runtime_projection_layer(
    hdmap_projection: Mapping[str, Any],
    lane_equivalence: Mapping[str, Any],
) -> dict[str, Any]:
    projection = hdmap_projection.get("projection") if isinstance(hdmap_projection.get("projection"), Mapping) else hdmap_projection
    return {
        "status": projection.get("status") or _status(hdmap_projection),
        "source": projection.get("source")
        or ("apollo_hdmap_api" if projection.get("official_source_available") else None),
        "official_source_available": projection.get("official_source_available"),
        "row_count": projection.get("row_count"),
        "ok_row_count": projection.get("ok_row_count"),
        "ok_ratio": projection.get("ok_ratio"),
        "sample_coverage": {
            "projection_s_coverage_m": projection.get("projection_s_coverage_m"),
            "sim_time_coverage_s": projection.get("sim_time_coverage_s"),
            "route_sample_count": _nested(lane_equivalence, "source.route_sample_count"),
        },
        "error_metrics": {
            "lateral_error_p95_m": projection.get("lateral_error_p95_m"),
            "heading_error_p95_rad": projection.get("heading_error_p95_rad"),
        },
        "nearest_lane_id_topk": list(projection.get("nearest_lane_id_topk") or []),
        "projected_carla_lane_signature": list(lane_equivalence.get("projected_carla_lane_signature") or []),
        "projected_apollo_lane_signature": list(lane_equivalence.get("projected_apollo_lane_signature") or []),
        "blocking_reasons": _projection_blocking_reasons(hdmap_projection),
        "warnings": _projection_warnings(hdmap_projection),
        "claim_boundary": (
            "Runtime projection proves route/localization samples can be projected by Apollo HDMap API. "
            "It does not prove Apollo RoutingResponse chose the scenario claim route."
        ),
    }


def _routing_response_layer(route_contract: Mapping[str, Any]) -> dict[str, Any]:
    response = route_contract.get("last_routing_response")
    if not isinstance(response, Mapping):
        response = {}
    decoded = response.get("decoded") if isinstance(response.get("decoded"), Mapping) else route_contract.get("routing_response_decoded")
    if not isinstance(decoded, Mapping):
        decoded = {}
    return {
        "status": decoded.get("status") or ("pass" if response.get("available") else "insufficient_data"),
        "available": bool(response.get("available") or decoded.get("available")),
        "source": response.get("source") or decoded.get("source"),
        "total_length_m": response.get("response_total_length_m") or decoded.get("total_length_m"),
        "lane_window_count": response.get("lane_window_count") or decoded.get("lane_segment_count"),
        "lane_window_signature": response.get("lane_window_signature"),
        "unique_lane_signature": response.get("unique_lane_signature"),
        "ordered_lane_sequence": list(decoded.get("lane_sequence_signature") or []),
        "matched_request_sequence": response.get("matched_request_sequence"),
        "response_sequence": response.get("response_sequence") or decoded.get("response_sequence"),
        "route_order_boundary": (
            "RoutingResponse is Apollo's actual route choice. It must be compared with scenario route truth; "
            "it must not be written back as scenario truth."
        ),
    }


def _planning_reference_line_layer(
    reference_line_contract: Mapping[str, Any],
    route_contract: Mapping[str, Any],
) -> dict[str, Any]:
    contracts = reference_line_contract.get("contracts") if isinstance(reference_line_contract.get("contracts"), Mapping) else {}
    planning = contracts.get("planning_trajectory") if isinstance(contracts.get("planning_trajectory"), Mapping) else {}
    route_segment = route_contract.get("latest_planning_active_route_segment")
    if not isinstance(route_segment, Mapping):
        route_segment = {}
    return {
        "status": _status(reference_line_contract),
        "route_contract_status": route_contract.get("status"),
        "reference_line_count_zero_ratio": _nested(planning, "key_metrics.reference_line_count_zero_ratio"),
        "route_segment_count": route_segment.get("route_segment_count"),
        "route_segment_total_length_m": route_segment.get("route_segment_total_length_m"),
        "lane_ids": {
            "planning_lane_id_first_topk": list(_nested(reference_line_contract, "lane_ids.planning_lane_id_first_topk") or []),
            "target_lane_id_first_topk": list(_nested(reference_line_contract, "lane_ids.target_lane_id_first_topk") or []),
            "routing_unique_lane_signature_topk": list(_nested(reference_line_contract, "lane_ids.routing_unique_lane_signature_topk") or []),
        },
        "routing_lane_window_signature": route_segment.get("routing_lane_window_signature"),
        "routing_unique_lane_signature": route_segment.get("routing_unique_lane_signature"),
        "nonempty_trajectory_ratio": _nested(planning, "key_metrics.nonempty_trajectory_ratio"),
        "planning_claim_window_nonempty_trajectory_ratio": _nested(planning, "key_metrics.planning_claim_window_nonempty_trajectory_ratio"),
        "blocking_reasons": list(reference_line_contract.get("blocking_reasons") or []),
        "warnings": list(reference_line_contract.get("warnings") or []),
        "claim_boundary": (
            "Planning non-empty trajectory is not enough. Reference-line evidence must be tied to a passing route contract."
        ),
    }


def _compact_map_identity(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "selected_apollo_map_root": report.get("selected_apollo_map_root"),
        "base_map_sha256": report.get("base_map_sha256"),
        "routing_map_sha256": report.get("routing_map_sha256"),
        "sim_map_sha256": report.get("sim_map_sha256"),
        "map_root_consistent": report.get("map_root_consistent"),
        "blocking_reasons": list(report.get("blocking_reasons") or []),
    }


def _compact_projection(report: Mapping[str, Any]) -> dict[str, Any]:
    projection = report.get("projection") if isinstance(report.get("projection"), Mapping) else report
    return {
        "status": projection.get("status") or report.get("status"),
        "claim_grade": projection.get("claim_grade") or report.get("claim_grade"),
        "official_source_available": projection.get("official_source_available"),
        "heading_error_p95_rad": projection.get("heading_error_p95_rad"),
        "lateral_error_p95_m": projection.get("lateral_error_p95_m"),
        "blocking_reasons": _projection_blocking_reasons(report),
    }


def _compact_route_contract(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "route_id": report.get("route_id"),
        "scenario_route_length_m": report.get("scenario_route_length_m"),
        "apollo_routing_total_length_m": report.get("apollo_routing_total_length_m"),
        "lane_equivalence_status": report.get("lane_equivalence_status"),
        "routing_length_ratio": report.get("routing_length_ratio"),
        "blocking_reasons": list(report.get("blocking_reasons") or []),
        "missing_fields": list(report.get("missing_fields") or []),
    }


def _compact_reference_line(report: Mapping[str, Any]) -> dict[str, Any]:
    contracts = report.get("contracts") if isinstance(report.get("contracts"), Mapping) else {}
    return {
        "status": report.get("status"),
        "planning_trajectory_status": _nested(contracts, "planning_trajectory.status"),
        "control_reference_status": _nested(contracts, "control_reference.status"),
        "apollo_hdmap_projection_status": _nested(contracts, "apollo_hdmap_projection.status"),
        "blocking_reasons": list(report.get("blocking_reasons") or []),
        "warnings": list(report.get("warnings") or []),
    }


def _status(report: Mapping[str, Any]) -> str:
    if not report:
        return "insufficient_data"
    value = report.get("status")
    if isinstance(value, str) and value:
        return value
    verdict = report.get("verdict")
    if isinstance(verdict, Mapping):
        value = verdict.get("status")
    if isinstance(verdict, str):
        value = verdict
    return str(value or "insufficient_data")


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = root / relative
        if path.exists():
            return path
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _path_str(path: Path | None) -> str | None:
    return str(path) if path else None


def _nested(mapping: Mapping[str, Any], path: str) -> Any:
    cursor: Any = mapping
    for part in path.split("."):
        if not isinstance(cursor, Mapping):
            return None
        cursor = cursor.get(part)
    return cursor
