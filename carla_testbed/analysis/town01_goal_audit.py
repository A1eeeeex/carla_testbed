from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.route_curve_artifact_gap import analyze_route_curve_artifact_gap
from carla_testbed.analysis.transport_ab import analyze_ab_manifest

GOAL_AUDIT_SCHEMA_VERSION = "town01_goal_audit.v1"
HARD_GATE_ROUTE_IDS = ("lane097", "lane217", "junction031")
CURVE_DIAGNOSTIC_ROUTE_IDS = ("curve217", "curve213")
GOAL_ROUTE_IDS = (*HARD_GATE_ROUTE_IDS, *CURVE_DIAGNOSTIC_ROUTE_IDS)
NATURAL_DRIVING_REQUIRED_CLASSES = (
    "lane_keep",
    "curve_diagnostic",
    "junction_turn",
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
)
TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS = {
    "traffic_light_red_stop": "red_stop",
    "traffic_light_green_go": "green_go",
    "traffic_light_red_to_green_release": "red_to_green_release",
}


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path).expanduser()
    if not file_path.exists():
        return {}
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item not in {None, ""}]
    if value in {None, ""}:
        return []
    return [str(value)]


def find_latest_file(root: str | Path, pattern: str) -> Path | None:
    root_path = Path(root).expanduser()
    if not root_path.exists():
        return None
    candidates = [path for path in root_path.rglob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _comparison_route_ids(report: Mapping[str, Any]) -> set[str]:
    return {
        str(item.get("route_id"))
        for item in (report.get("comparisons") or [])
        if item.get("route_id")
    }


def find_goal_ab_report_paths(root: str | Path) -> list[Path]:
    root_path = Path(root).expanduser()
    if not root_path.exists():
        return []
    candidates = sorted(
        [path for path in root_path.rglob("ab_report.json") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    selected: list[Path] = []
    seen_goal_routes: set[str] = set()
    random_selected = False
    target_routes = set(GOAL_ROUTE_IDS)
    for path in candidates:
        report = _read_json(path)
        route_ids = _comparison_route_ids(report)
        contributes_goal_route = bool((route_ids & target_routes) - seen_goal_routes)
        contributes_random = any(route_id.startswith("random_") for route_id in route_ids) and not random_selected
        if not contributes_goal_route and not contributes_random:
            continue
        selected.append(path)
        seen_goal_routes.update(route_ids & target_routes)
        random_selected = random_selected or any(route_id.startswith("random_") for route_id in route_ids)
        if target_routes.issubset(seen_goal_routes) and random_selected:
            break
    return selected


def _route_comparison(report: Mapping[str, Any], route_id: str) -> dict[str, Any] | None:
    for comparison in _comparisons_by_route(report).values():
        if comparison.get("route_id") == route_id:
            return dict(comparison)
    return None


def _comparisons_by_route(report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    by_route: dict[str, dict[str, Any]] = {}
    for item in report.get("comparisons") or []:
        route_id = item.get("route_id")
        if route_id and str(route_id) not in by_route:
            by_route[str(route_id)] = dict(item)
    return by_route


def _run_results_by_id(report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("run_id")): dict(row)
        for row in (report.get("run_results") or [])
        if row.get("run_id")
    }


def merge_ab_reports(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    run_results: list[dict[str, Any]] = []
    comparisons: list[dict[str, Any]] = []
    source_reports: list[dict[str, Any]] = []
    for report in reports:
        run_results.extend(dict(row) for row in (report.get("run_results") or []))
        comparisons.extend(dict(row) for row in (report.get("comparisons") or []))
        source_reports.append(
            {
                "batch_id": report.get("batch_id"),
                "source_manifest": report.get("source_manifest"),
                "schema_version": report.get("schema_version"),
            }
        )
    return {
        "schema_version": "ab_report.merged_for_goal_audit.v1",
        "verdict": {},
        "run_results": run_results,
        "comparisons": comparisons,
        "source_reports": source_reports,
    }


def _refresh_ab_report_from_manifest(report: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = report.get("source_manifest")
    if not manifest_path:
        return dict(report)
    try:
        return analyze_ab_manifest(manifest_path)
    except Exception as exc:  # pragma: no cover - defensive evidence fallback.
        refreshed = dict(report)
        refreshed["goal_audit_refresh_error"] = str(exc)
        return refreshed


def _load_ab_report(path: str | Path, *, refresh_from_manifest: bool) -> dict[str, Any]:
    payload = _read_json(path)
    if not payload:
        return {}
    return _refresh_ab_report_from_manifest(payload) if refresh_from_manifest else payload


def _hard_gate_from_comparisons(report: Mapping[str, Any]) -> dict[str, Any]:
    verdict = report.get("verdict") or {}
    existing = verdict.get("hard_gate_summary")
    if isinstance(existing, Mapping) and existing:
        return dict(existing)
    by_route = _comparisons_by_route(report)
    missing: list[str] = []
    positive: list[str] = []
    degraded: list[str] = []
    insufficient: list[str] = []
    for route_id in HARD_GATE_ROUTE_IDS:
        comparison = by_route.get(route_id)
        if comparison is None:
            missing.append(route_id)
            continue
        status = str(comparison.get("status") or "")
        if status == "candidate_positive":
            positive.append(route_id)
        elif status == "candidate_degraded":
            degraded.append(route_id)
        else:
            insufficient.append(route_id)
    if degraded:
        status = "hard_gate_fail"
    elif insufficient:
        status = "hard_gate_insufficient_data"
    elif missing:
        status = "hard_gate_incomplete"
    else:
        status = "hard_gate_pass"
    return {
        "status": status,
        "expected_routes": list(HARD_GATE_ROUTE_IDS),
        "observed_routes": sorted(set(HARD_GATE_ROUTE_IDS) - set(missing)),
        "positive_routes": positive,
        "degraded_routes": degraded,
        "insufficient_routes": insufficient,
        "missing_routes": missing,
        "complete": not missing,
        "pass": status == "hard_gate_pass",
    }


def _direct_cadence_status(
    report: Mapping[str, Any],
    *,
    cadence_ratio_min: float,
) -> dict[str, Any]:
    by_run_id = _run_results_by_id(report)
    observed: list[dict[str, Any]] = []
    missing: list[str] = []
    low: list[dict[str, Any]] = []
    contract_not_aligned: list[dict[str, Any]] = []
    for comparison in _comparisons_by_route(report).values():
        candidate = by_run_id.get(str(comparison.get("candidate_run_id") or ""))
        if not candidate or candidate.get("backend") != "carla_direct":
            continue
        cadence = comparison.get("cadence_comparison") or {}
        loc_ratio = _num(cadence.get("bridge_loc_hz_ratio"))
        chassis_ratio = _num(cadence.get("bridge_chassis_hz_ratio"))
        row = {
            "route_id": comparison.get("route_id"),
            "duration_s": comparison.get("duration_s"),
            "run_id": candidate.get("run_id"),
            "run_status": candidate.get("run_status"),
            "return_code": candidate.get("return_code"),
            "actual_run_dir": candidate.get("actual_run_dir"),
            "contract_status": candidate.get("direct_transport_contract_status"),
            "loc_ratio": loc_ratio,
            "chassis_ratio": chassis_ratio,
            "loc_ratio_by_stats_sim_time": _num(cadence.get("bridge_loc_hz_by_stats_sim_time_ratio")),
            "chassis_ratio_by_stats_sim_time": _num(
                cadence.get("bridge_chassis_hz_by_stats_sim_time_ratio")
            ),
            "loc_hz_manifest_duration": candidate.get("bridge_loc_hz"),
            "chassis_hz_manifest_duration": candidate.get("bridge_chassis_hz"),
            "loc_hz_stats_sim_time": candidate.get("bridge_loc_hz_by_stats_sim_time"),
            "chassis_hz_stats_sim_time": candidate.get("bridge_chassis_hz_by_stats_sim_time"),
            "bridge_stats_sim_time_sec": candidate.get("bridge_stats_sim_time_sec"),
            "bridge_cadence_duration_s": candidate.get("bridge_cadence_duration_s"),
            "bridge_cadence_duration_source": candidate.get("bridge_cadence_duration_source"),
            "policy": candidate.get("direct_stale_world_frame_policy"),
            "policy_source": candidate.get("direct_stale_world_frame_policy_source"),
            "evidence_scope": "transport_materialization_cadence",
        }
        observed.append(row)
        if loc_ratio is None or chassis_ratio is None:
            missing.append(str(comparison.get("route_id")))
        elif loc_ratio < cadence_ratio_min or chassis_ratio < cadence_ratio_min:
            low.append(row)
        if row.get("contract_status") != "aligned":
            contract_not_aligned.append(row)
    if not observed:
        status = "missing"
    elif missing:
        status = "insufficient_data"
    elif contract_not_aligned:
        status = "contract_not_aligned"
    elif low:
        status = "below_threshold"
    else:
        status = "pass"
    return {
        "status": status,
        "cadence_ratio_min": cadence_ratio_min,
        "evidence_scope": "transport_materialization_cadence",
        "cadence_ratio_definition": (
            "candidate carla_direct bridge localization/chassis Hz divided by baseline "
            "ros2_gt bridge localization/chassis Hz for the same route and duration"
        ),
        "strict_gate_duration_basis": "manifest_duration_s",
        "stats_timing_context": (
            "bridge_*_hz_by_stats_sim_time uses cyber_bridge_stats.timing.sim_time_sec "
            "when available and is diagnostic context only"
        ),
        "claim_supported": (
            "carla_direct transport/materialization cadence is sufficiently evidenced only "
            "when direct contract is aligned and loc/chassis ratios meet the threshold"
        ),
        "claim_not_supported": (
            "direct cadence status does not prove curve health, lateral semantics, "
            "natural-driving behavior, or full Apollo perception/localization reproduction"
        ),
        "observed": observed,
        "missing_routes": sorted(set(missing)),
        "contract_not_aligned": contract_not_aligned,
        "below_threshold": low,
    }


def _curve_route_health_evidence(
    report: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> dict[str, Any]:
    by_run_id = _run_results_by_id(report)
    candidate = by_run_id.get(str(comparison.get("candidate_run_id") or ""))
    missing: list[str] = []
    if not candidate:
        return {
            "status": "missing",
            "missing": ["candidate_run_result"],
        }
    return_code = candidate.get("return_code")
    run_status = str(candidate.get("run_status") or "").lower()
    if return_code not in (None, "", 0, "0"):
        missing.append("return_code_zero")
    if run_status in {"failed", "timeout", "skipped", "cancelled", "canceled"}:
        missing.append("successful_run_status")
    if candidate.get("artifact_complete") is not True:
        missing.append("artifact_complete")
    if not candidate.get("route_health_source"):
        missing.append("route_health_source")
    for key in (
        "route_completion",
        "lateral_error_p95_m",
        "heading_error_p95_rad",
        "planning_hz",
        "carla_applied_control_hz",
        "localization_hz",
    ):
        if candidate.get(key) is None:
            missing.append(key)
    if candidate.get("backend") == "carla_direct" and candidate.get("direct_transport_contract_status") != "aligned":
        missing.append("direct_transport_contract_aligned")
    failure_reason = candidate.get("failure_reason")
    if failure_reason not in {None, "success"}:
        missing.append(f"failure_reason={failure_reason}")
    return {
        "status": "pass" if not missing else "insufficient_data",
        "missing": missing,
        "candidate_run_id": candidate.get("run_id"),
        "candidate_backend": candidate.get("backend"),
        "route_health_source": candidate.get("route_health_source"),
        "artifact_complete": candidate.get("artifact_complete"),
        "runtime_contract_status": candidate.get("direct_transport_contract_status"),
        "failure_reason": failure_reason,
    }


def _hard_gate_problem_details(
    report: Mapping[str, Any],
    comparisons: Mapping[str, Mapping[str, Any] | None],
) -> list[dict[str, Any]]:
    by_run_id = _run_results_by_id(report)
    details: list[dict[str, Any]] = []
    metric_keys = (
        "route_completion",
        "lateral_error_p95_m",
        "heading_error_p95_rad",
        "failure_reason",
        "collision_count",
        "lane_invasion_count",
        "planning_hz",
        "localization_hz",
        "carla_applied_control_hz",
    )
    safety_keys = (
        "first_safety_event_type",
        "first_safety_event_time_s",
        "first_safety_event_frame_id",
        "first_safety_event_route_s",
        "first_safety_event_cross_track_error_m",
        "first_safety_event_heading_error_rad",
        "first_safety_event_ego_speed_mps",
        "first_safety_event_apollo_steer_raw",
        "first_safety_event_bridge_steer_mapped",
        "first_safety_event_carla_steer_applied",
        "first_safety_event_throttle_raw",
        "first_safety_event_throttle_mapped",
        "first_safety_event_throttle_applied",
        "first_safety_event_brake_raw",
        "first_safety_event_brake_mapped",
        "first_safety_event_brake_applied",
        "first_safety_event_lateral_guard_applied",
        "first_safety_event_trajectory_contract_guard_applied",
        "first_safety_event_control_context",
        "first_safety_event_source",
        "safety_window_duration_s",
        "safety_window_sample_count",
        "safety_window_cross_track_error_start_m",
        "safety_window_cross_track_error_end_m",
        "safety_window_cross_track_error_delta_m",
        "safety_window_cross_track_error_max_m",
        "safety_window_apollo_steer_raw_abs_p95",
        "safety_window_bridge_steer_mapped_abs_p95",
        "safety_window_carla_steer_applied_abs_p95",
        "safety_window_zero_lateral_command_ratio",
        "safety_window_control_context",
    )
    for route_id in HARD_GATE_ROUTE_IDS:
        comparison = comparisons.get(route_id)
        if comparison is None:
            details.append(
                {
                    "route_id": route_id,
                    "status": "missing",
                    "reasons": ["comparison missing"],
                }
            )
            continue
        status = str(comparison.get("status") or "")
        if status == "candidate_positive":
            continue
        baseline = by_run_id.get(str(comparison.get("baseline_run_id") or ""))
        candidate = by_run_id.get(str(comparison.get("candidate_run_id") or ""))
        detail = {
            "route_id": route_id,
            "duration_s": comparison.get("duration_s"),
            "status": status,
            "reasons": list(comparison.get("reasons") or []),
            "baseline_run_id": comparison.get("baseline_run_id"),
            "candidate_run_id": comparison.get("candidate_run_id"),
            "baseline_artifacts": {
                key: (baseline or {}).get(key)
                for key in ("artifact_dir", "summary_path", "route_health_path", "timeseries_path", "events_path")
                if (baseline or {}).get(key) is not None
            },
            "candidate_artifacts": {
                key: (candidate or {}).get(key)
                for key in ("artifact_dir", "summary_path", "route_health_path", "timeseries_path", "events_path")
                if (candidate or {}).get(key) is not None
            },
            "baseline_metrics": {
                key: (baseline or {}).get(key)
                for key in metric_keys
                if (baseline or {}).get(key) is not None
            },
            "candidate_metrics": {
                key: (candidate or {}).get(key)
                for key in metric_keys
                if (candidate or {}).get(key) is not None
            },
            "baseline_first_safety_event": {
                key: (baseline or {}).get(key)
                for key in safety_keys
                if (baseline or {}).get(key) is not None
            },
            "candidate_first_safety_event": {
                key: (candidate or {}).get(key)
                for key in safety_keys
                if (candidate or {}).get(key) is not None
            },
        }
        cadence = comparison.get("cadence_comparison")
        if isinstance(cadence, Mapping):
            detail["cadence_comparison"] = dict(cadence)
        details.append(detail)
    return details


def _shadow_mode_command(timeseries_path: Any) -> str | None:
    if timeseries_path in (None, ""):
        return None
    python = "/home/ubuntu/miniconda3/envs/carla16/bin/python3"
    ts_path = Path(str(timeseries_path))
    run_dir = ts_path.parent
    return (
        f"{python} tools/analyze_apollo_shadow_mode.py "
        f"--timeseries {timeseries_path} "
        f"--out {run_dir / 'analysis' / 'apollo_shadow_mode'}"
    )


def _shadow_mode_command_with_summary(timeseries_path: Any, summary_path: Any) -> str | None:
    if timeseries_path in (None, ""):
        return None
    python = "/home/ubuntu/miniconda3/envs/carla16/bin/python3"
    ts_path = Path(str(timeseries_path))
    run_dir = ts_path.parent
    summary_arg = "" if summary_path in (None, "") else f" --summary {summary_path}"
    return (
        f"{python} tools/analyze_apollo_shadow_mode.py "
        f"--timeseries {timeseries_path}{summary_arg} "
        f"--out {run_dir / 'analysis' / 'apollo_shadow_mode'}"
    )


def _route_curve_artifact_gap_command(timeseries_path: Any, summary_path: Any) -> str | None:
    if timeseries_path in (None, ""):
        return None
    python = "/home/ubuntu/miniconda3/envs/carla16/bin/python3"
    ts_path = Path(str(timeseries_path))
    run_dir = ts_path.parent
    summary_arg = "" if summary_path in (None, "") else f" --summary {summary_path}"
    return (
        f"{python} tools/check_route_curve_artifact_gap.py "
        f"--timeseries {timeseries_path}{summary_arg} "
        f"--out {run_dir / 'analysis' / 'route_curve_artifact_gap'}"
    )


def _route_curve_artifact_gap_for_paths(timeseries_path: Any, summary_path: Any) -> dict[str, Any]:
    if timeseries_path in (None, ""):
        return {
            "status": "insufficient_data",
            "failure_reason": "missing_timeseries_path",
            "missing_p1_fields": [],
            "required_next_fields": [],
        }
    try:
        report = analyze_route_curve_artifact_gap(timeseries_path, summary_json=summary_path)
    except Exception as exc:  # pragma: no cover - defensive audit fallback.
        return {
            "status": "insufficient_data",
            "failure_reason": "route_curve_artifact_gap_error",
            "error": str(exc),
            "missing_p1_fields": [],
            "required_next_fields": [],
        }
    return {
        "status": report.get("status"),
        "failure_reason": report.get("failure_reason"),
        "per_frame_p1_complete": report.get("per_frame_p1_complete"),
        "missing_p1_fields": list(report.get("missing_p1_fields") or []),
        "required_next_fields": list(report.get("required_next_fields") or []),
        "summary_semantics_available": report.get("summary_semantics_available"),
        "summary_anchor": (report.get("summary_semantics") or {}).get("semantic_window_anchor_kind")
        if isinstance(report.get("summary_semantics"), Mapping)
        else None,
        "claim_supported": report.get("claim_supported"),
    }


def _hard_gate_control_context_blockers(details: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for detail in details:
        safety = (
            detail.get("candidate_first_safety_event")
            if isinstance(detail.get("candidate_first_safety_event"), Mapping)
            else {}
        )
        event_context = safety.get("first_safety_event_control_context")
        window_context = safety.get("safety_window_control_context")
        if event_context not in {
            "no_lateral_command_at_safety_event",
            "mapping_suppressed_lateral_command",
            "actuation_or_apply_mismatch",
        } and window_context not in {
            "sustained_no_lateral_command_before_safety_event",
            "mapping_suppressed_lateral_command_before_safety_event",
            "actuation_or_apply_mismatch_before_safety_event",
        }:
            continue
        artifacts = (
            dict(detail.get("candidate_artifacts") or {})
            if isinstance(detail.get("candidate_artifacts"), Mapping)
            else {}
        )
        timeseries_path = artifacts.get("timeseries_path")
        summary_path = artifacts.get("summary_path")
        artifact_gap = _route_curve_artifact_gap_for_paths(timeseries_path, summary_path)
        blockers.append(
            {
                "route_id": detail.get("route_id"),
                "candidate_run_id": detail.get("candidate_run_id"),
                "candidate_artifacts": artifacts,
                "failure_reason": (detail.get("candidate_metrics") or {}).get("failure_reason")
                if isinstance(detail.get("candidate_metrics"), Mapping)
                else None,
                "event_control_context": event_context,
                "window_control_context": window_context,
                "first_safety_event_route_s": safety.get("first_safety_event_route_s"),
                "first_safety_event_time_s": safety.get("first_safety_event_time_s"),
                "safety_window_cross_track_error_delta_m": safety.get(
                    "safety_window_cross_track_error_delta_m"
                ),
                "shadow_mode_command": _shadow_mode_command_with_summary(
                    timeseries_path,
                    summary_path,
                ),
                "route_curve_artifact_gap": artifact_gap,
                "route_curve_artifact_gap_command": _route_curve_artifact_gap_command(
                    timeseries_path,
                    summary_path,
                ),
                "claim_supported": (
                    "hard-gate failure has control-trace evidence that should be "
                    "followed by Apollo lateral/shadow-mode semantic inspection"
                ),
            }
        )
    return blockers


def evaluate_ab_report(report: Mapping[str, Any], *, cadence_ratio_min: float = 0.8) -> dict[str, Any]:
    verdict = report.get("verdict") or {}
    hard_gate = _hard_gate_from_comparisons(report)
    comparisons = {
        route_id: _route_comparison(report, route_id)
        for route_id in [*HARD_GATE_ROUTE_IDS, *CURVE_DIAGNOSTIC_ROUTE_IDS]
    }
    curve_status: dict[str, Any] = {}
    for route_id in CURVE_DIAGNOSTIC_ROUTE_IDS:
        comparison = comparisons.get(route_id)
        if comparison is None:
            curve_status[route_id] = {
                "status": "missing",
                "claim_supported": "no curve evidence in this A/B report",
            }
            continue
        route_health_evidence = _curve_route_health_evidence(report, comparison)
        status = comparison.get("status")
        if status == "candidate_positive" and route_health_evidence.get("status") == "pass":
            status = "aligned_route_health_evidence"
        elif status == "candidate_positive":
            status = "insufficient_route_health_evidence"
        curve_status[route_id] = {
            "status": status,
            "comparison_status": comparison.get("status"),
            "duration_s": comparison.get("duration_s"),
            "reasons": comparison.get("reasons") or [],
            "route_health_evidence": route_health_evidence,
            "claim_supported": "curve has aligned runtime evidence only when route-health fields and direct contract evidence are complete",
        }
    direct_cadence = _direct_cadence_status(report, cadence_ratio_min=cadence_ratio_min)
    hard_gate_problem_details = _hard_gate_problem_details(report, comparisons)
    if hard_gate.get("pass") and direct_cadence.get("status") == "pass":
        status = "ab_hard_gate_pass"
    elif hard_gate:
        status = str(hard_gate.get("status") or "ab_incomplete")
    else:
        status = "missing"
    return {
        "status": status,
        "report_status": verdict.get("status"),
        "hard_gate": hard_gate,
        "hard_gate_problem_details": hard_gate_problem_details,
        "hard_gate_control_context_blockers": _hard_gate_control_context_blockers(
            hard_gate_problem_details
        ),
        "direct_cadence": direct_cadence,
        "selected_comparisons": comparisons,
        "curve_diagnostics": curve_status,
    }


def evaluate_ab_evidence_coherence(report: Mapping[str, Any]) -> dict[str, Any]:
    source_reports = report.get("source_reports")
    if not isinstance(source_reports, list) or not source_reports:
        return {
            "status": "single_report",
            "report_count": 1 if report else 0,
            "batch_ids": [report.get("batch_id")] if report.get("batch_id") else [],
            "claim_supported": "A/B evidence comes from one report.",
        }
    batch_ids = sorted(
        {
            str(item.get("batch_id"))
            for item in source_reports
            if isinstance(item, Mapping) and item.get("batch_id")
        }
    )
    source_manifests = sorted(
        {
            str(item.get("source_manifest"))
            for item in source_reports
            if isinstance(item, Mapping) and item.get("source_manifest")
        }
    )
    if len(batch_ids) == 1:
        status = "single_batch"
        claim = "Multiple A/B reports come from one batch id."
    elif batch_ids:
        status = "mixed_batch"
        claim = "A/B evidence is stitched from multiple batch ids and cannot prove final acceptance."
    else:
        status = "unknown_multi_report"
        claim = "A/B evidence is stitched from multiple reports without batch ids."
    return {
        "status": status,
        "report_count": len(source_reports),
        "batch_ids": batch_ids,
        "source_manifests": source_manifests,
        "claim_supported": claim,
    }


def evaluate_calibration(report: Mapping[str, Any]) -> dict[str, Any]:
    if not report:
        return {
            "status": "missing",
            "promotion_allowed": False,
            "claim_supported": "no calibration report evidence",
        }
    no_regression = report.get("no_regression") or {}
    recommendation = report.get("recommendation") or {}
    return {
        "status": "promotion_allowed" if no_regression.get("promotion_allowed") else "not_promotable",
        "promotion_allowed": bool(no_regression.get("promotion_allowed")),
        "failed_gates": no_regression.get("failed_gates") or [],
        "missing_gates": no_regression.get("missing_gates") or [],
        "keep_legacy_steer_scale_025": recommendation.get("keep_legacy_steer_scale_025"),
        "enable_physical_mapping": recommendation.get("enable_physical_mapping"),
        "claim_supported": "control-actuation evidence only; not a curve fix by itself",
    }


def evaluate_random_regression(report: Mapping[str, Any]) -> dict[str, Any]:
    comparisons = [
        item
        for route_id, item in _comparisons_by_route(report).items()
        if route_id.startswith("random_")
    ]
    if not comparisons:
        return {
            "status": "missing",
            "route_count": 0,
            "claim_supported": "no random regression A/B evidence",
        }
    positive = [item for item in comparisons if item.get("status") == "candidate_positive"]
    degraded = [item for item in comparisons if item.get("status") == "candidate_degraded"]
    insufficient = [
        item
        for item in comparisons
        if item.get("status") not in {"candidate_positive", "candidate_degraded"}
    ]
    if degraded:
        status = "degraded"
    elif insufficient:
        status = "insufficient_data"
    else:
        status = "pass"
    return {
        "status": status,
        "route_count": len(comparisons),
        "positive_count": len(positive),
        "degraded_routes": [item.get("route_id") for item in degraded],
        "insufficient_routes": [item.get("route_id") for item in insufficient],
        "claim_supported": "random robustness screen only; not curve promotion evidence",
    }


def evaluate_demo_recording(inspection: Mapping[str, Any]) -> dict[str, Any]:
    if not inspection:
        return {
            "status": "missing",
            "claim_supported": "no CARLA/Dreamview demo recording evidence",
        }
    status = str(inspection.get("status") or "unknown")
    return {
        "status": status,
        "route_count": inspection.get("route_count"),
        "carla_ok_count": inspection.get("carla_ok_count"),
        "dreamview_ok_count": inspection.get("dreamview_ok_count"),
        "require_carla": inspection.get("require_carla"),
        "require_dreamview": inspection.get("require_dreamview"),
        "accepted_for_goal": False,
        "claim_supported": "demo recording readiness only; not route-health proof",
    }


def evaluate_natural_driving(
    report: Mapping[str, Any],
    *,
    report_path: str | Path | None = None,
) -> dict[str, Any]:
    if not report:
        return {
            "status": "missing",
            "verdict_status": None,
            "can_claim_full_natural_driving": False,
            "claim_supported": "no natural_driving_report.json evidence",
        }
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    coverage = (
        report.get("capability_coverage")
        if isinstance(report.get("capability_coverage"), Mapping)
        else {}
    )
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    run_results = [item for item in report.get("run_results") or [] if isinstance(item, Mapping)]
    observed_classes = sorted(
        {
            str(item.get("scenario_class"))
            for item in run_results
            if item.get("scenario_class")
        }
    )
    calculated_missing_classes = [
        scenario_class
        for scenario_class in NATURAL_DRIVING_REQUIRED_CLASSES
        if scenario_class not in observed_classes
    ]
    missing_classes = list(
        verdict.get("missing_required_scenario_classes")
        or coverage.get("missing_required_scenario_classes")
        or calculated_missing_classes
    )
    unproven_classes = list(
        verdict.get("unproven_required_scenario_classes")
        or coverage.get("unproven_required_scenario_classes")
        or []
    )
    missing_scenario_ids = list(
        verdict.get("missing_required_scenario_ids")
        or coverage.get("missing_required_scenario_ids")
        or []
    )
    unproven_scenario_ids = list(
        verdict.get("unproven_required_scenario_ids")
        or coverage.get("unproven_required_scenario_ids")
        or []
    )
    scenario_identity_mismatches = list(
        verdict.get("scenario_identity_mismatches")
        or coverage.get("scenario_identity_mismatches")
        or []
    )
    verdict_status = str(verdict.get("status") or "unknown")
    verdict_claim = verdict.get("can_claim_full_natural_driving")
    coverage_claim = coverage.get("can_claim_full_natural_driving")
    report_file_backed = bool(
        report_path not in {None, ""} and Path(report_path).expanduser().exists()
    )
    suite_plan_scope = (
        report.get("suite_plan_scope")
        if isinstance(report.get("suite_plan_scope"), Mapping)
        else {}
    )
    suite_plan_missing = _natural_driving_missing_suite_plan_fields(verdict, suite_plan_scope)
    verdict_filtered = verdict.get("filtered_suite_plan") is True
    scope_filtered = suite_plan_scope.get("filtered") is True
    filtered_suite_plan = bool(verdict_filtered or scope_filtered)
    scenario_class_filter = _string_list(
        verdict.get("scenario_class_filter") or suite_plan_scope.get("scenario_class_filter")
    )
    scenario_id_filter = _string_list(
        verdict.get("scenario_id_filter") or suite_plan_scope.get("scenario_id_filter")
    )
    suite_plan_files = (
        _natural_driving_suite_plan_file_status(report, report_path=report_path)
        if report_file_backed
        else {"suite_root": None, "paths": {"suite_manifest": None, "run_matrix": None}, "missing_files": []}
    )
    suite_plan_file_missing = list(suite_plan_files.get("missing_files") or [])
    suite_plan_file_mismatches = list(suite_plan_files.get("mismatches") or [])
    can_claim_full = bool(
        verdict_claim is True
        and coverage_claim is True
        and report_file_backed
        and not suite_plan_missing
        and not suite_plan_file_missing
        and not suite_plan_file_mismatches
        and not filtered_suite_plan
        and not missing_classes
        and not unproven_classes
        and not missing_scenario_ids
        and not unproven_scenario_ids
        and not scenario_identity_mismatches
    )
    traffic_light_expectation_blockers = _traffic_light_expectation_blockers(run_results)
    artifact_blockers = _natural_driving_artifact_completeness_blockers(
        run_results,
        report=report,
        report_path=report_path,
    )
    claim_blockers: list[str] = []
    if any(blocker.get("reason") == "traffic_light_expectation_mismatch" for blocker in traffic_light_expectation_blockers):
        status = "fail"
        claim_blockers.append("traffic_light_expectation_mismatch")
    elif any(blocker.get("reason") == "traffic_light_stimulus_not_claim_grade" for blocker in traffic_light_expectation_blockers):
        status = "insufficient_data"
        claim_blockers.append("traffic_light_stimulus_not_claim_grade")
    elif traffic_light_expectation_blockers:
        status = "insufficient_data"
        claim_blockers.append("traffic_light_expectation_not_manifest_backed")
    elif artifact_blockers:
        status = "insufficient_data"
        claim_blockers.append("artifact_completeness_not_proven")
    elif suite_plan_missing:
        status = "insufficient_data"
        claim_blockers.append("suite_plan_missing")
    elif filtered_suite_plan:
        status = "insufficient_data"
        claim_blockers.append("filtered_suite_plan")
    elif scenario_identity_mismatches:
        status = "insufficient_data"
        claim_blockers.append("scenario_identity_mismatch")
    elif missing_scenario_ids:
        status = "incomplete"
        claim_blockers.append("missing_required_scenario_ids")
    elif unproven_scenario_ids:
        status = "insufficient_data"
        claim_blockers.append("unproven_required_scenario_ids")
    elif missing_classes:
        status = "incomplete"
        claim_blockers.append("missing_required_scenario_classes")
    elif unproven_classes:
        status = "insufficient_data"
        claim_blockers.append("unproven_required_scenario_classes")
    elif suite_plan_file_missing and verdict_status == "pass":
        status = "insufficient_data"
        claim_blockers.append("suite_plan_files_missing")
    elif suite_plan_file_mismatches and verdict_status == "pass":
        status = "insufficient_data"
        claim_blockers.append("suite_plan_files_mismatch")
    elif verdict_status == "pass" and can_claim_full:
        status = "pass"
    elif verdict_status == "pass":
        status = "insufficient_data"
        if not report_file_backed:
            claim_blockers.append("natural_driving_report_path_missing")
        if not can_claim_full:
            claim_blockers.append("can_claim_full_natural_driving_not_true")
    elif verdict_status == "warn":
        status = "warn"
        claim_blockers.append("verdict_warn")
    elif verdict_status in {"fail", "insufficient_data"}:
        status = verdict_status
        claim_blockers.append(f"verdict_{verdict_status}")
    else:
        status = "unknown"
        claim_blockers.append("verdict_unknown")
    return {
        "status": status,
        "verdict_status": verdict_status,
        "can_claim_full_natural_driving": can_claim_full,
        "verdict_can_claim_full_natural_driving": verdict_claim,
        "coverage_can_claim_full_natural_driving": coverage_claim,
        "report_file_backed": report_file_backed,
        "natural_driving_report_path": None if report_path is None else str(report_path),
        "suite_plan_missing": suite_plan_missing,
        "suite_plan_file_missing": suite_plan_file_missing,
        "suite_plan_file_mismatches": suite_plan_file_mismatches,
        "suite_plan_files": suite_plan_files,
        "suite_plan_has_manifest": suite_plan_scope.get("has_suite_manifest"),
        "suite_plan_has_run_matrix": suite_plan_scope.get("has_run_matrix"),
        "filtered_suite_plan": filtered_suite_plan,
        "verdict_filtered_suite_plan": verdict.get("filtered_suite_plan"),
        "suite_plan_scope_filtered": suite_plan_scope.get("filtered"),
        "scenario_class_filter": scenario_class_filter,
        "scenario_id_filter": scenario_id_filter,
        "run_count": report.get("run_count"),
        "pass_count": summary.get("pass_count"),
        "warn_count": summary.get("warn_count"),
        "fail_count": summary.get("fail_count"),
        "insufficient_data_count": summary.get("insufficient_data_count"),
        "observed_scenario_classes": observed_classes,
        "missing_scenario_classes": missing_classes,
        "unproven_scenario_classes": unproven_classes,
        "missing_required_scenario_ids": missing_scenario_ids,
        "unproven_required_scenario_ids": unproven_scenario_ids,
        "scenario_identity_mismatches": scenario_identity_mismatches,
        "claim_blockers": claim_blockers,
        "failed_runs": verdict.get("failed_runs") or [],
        "insufficient_data_runs": verdict.get("insufficient_data_runs") or [],
        "warning_runs": verdict.get("warning_runs") or [],
        "problem_run_details": _natural_driving_problem_run_details(run_results),
        "traffic_light_expectation_blockers": traffic_light_expectation_blockers,
        "artifact_completeness_blockers": artifact_blockers,
        "claim_supported": (
            "Town01 truth-input natural-driving evidence only; does not prove full Apollo "
            "perception/localization reproduction."
        ),
    }


def _natural_driving_missing_suite_plan_fields(
    verdict: Mapping[str, Any],
    suite_plan_scope: Mapping[str, Any],
) -> list[str]:
    explicit_missing = verdict.get("suite_plan_missing")
    if isinstance(explicit_missing, list):
        return [str(item) for item in explicit_missing if item not in {None, ""}]
    missing: list[str] = []
    if suite_plan_scope.get("has_suite_manifest") is not True:
        missing.append("suite_manifest.json")
    if suite_plan_scope.get("has_run_matrix") is not True:
        missing.append("run_matrix.csv")
    return missing


def _natural_driving_suite_plan_file_status(
    report: Mapping[str, Any],
    *,
    report_path: str | Path | None,
) -> dict[str, Any]:
    root = _natural_driving_suite_root(report, report_path=report_path)
    paths: dict[str, str | None] = {
        "suite_manifest": None,
        "run_matrix": None,
    }
    missing: list[str] = []
    if root is None:
        return {
            "suite_root": None,
            "paths": paths,
            "missing_files": ["suite_manifest.json", "run_matrix.csv"],
        }
    expected = {
        "suite_manifest": "suite_manifest.json",
        "run_matrix": "run_matrix.csv",
    }
    for key, file_name in expected.items():
        path = root / file_name
        if path.exists():
            paths[key] = str(path)
        else:
            missing.append(file_name)
    mismatches: list[str] = []
    suite_manifest_path = root / "suite_manifest.json"
    if suite_manifest_path.exists():
        mismatches.extend(
            _natural_driving_suite_manifest_mismatches(report, manifest_path=suite_manifest_path)
        )
    run_matrix_path = root / "run_matrix.csv"
    if not missing and run_matrix_path.exists():
        mismatches = _natural_driving_run_matrix_mismatches(
            report,
            matrix_path=run_matrix_path,
            suite_root=root,
        ) + mismatches
    return {
        "suite_root": str(root),
        "paths": paths,
        "missing_files": missing,
        "mismatches": mismatches,
    }


def _natural_driving_suite_manifest_mismatches(
    report: Mapping[str, Any],
    *,
    manifest_path: Path,
) -> list[str]:
    manifest = _read_json(manifest_path)
    mismatches: list[str] = []
    if manifest.get("schema_version") != "natural_driving_suite_manifest.v1":
        mismatches.append("suite_manifest.schema_version")
    if manifest.get("dry_run") is True:
        mismatches.append("suite_manifest.dry_run")
    for field in ("scenario_class_filter", "scenario_id_filter"):
        values = _string_list(manifest.get(field))
        if values:
            mismatches.append(f"suite_manifest.{field}")
    run_results = [run for run in report.get("run_results") or [] if isinstance(run, Mapping)]
    run_count = manifest.get("run_count")
    if run_count not in {None, ""}:
        try:
            manifest_run_count = int(run_count)
        except (TypeError, ValueError):
            mismatches.append("suite_manifest.run_count")
        else:
            if manifest_run_count != len(run_results):
                mismatches.append("suite_manifest.run_count")
    runs = manifest.get("runs")
    if not isinstance(runs, list):
        mismatches.append("suite_manifest.runs")
        return mismatches
    runs_by_id: dict[str, Mapping[str, Any]] = {}
    for row in runs:
        if not isinstance(row, Mapping):
            continue
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        if run_id in runs_by_id:
            mismatches.append(f"suite_manifest.duplicate_run_id:{run_id}")
            continue
        runs_by_id[run_id] = row
    for run in run_results:
        run_id = str(run.get("run_id") or "").strip()
        if not run_id:
            continue
        row = runs_by_id.get(run_id)
        if row is None:
            mismatches.append(f"suite_manifest.missing_run_id:{run_id}")
            continue
        mismatches.extend(_suite_plan_execution_mismatches(row, prefix="suite_manifest", run_id=run_id))
        for field in ("scenario_id", "scenario_class", "route_id"):
            expected = str(run.get(field) or "").strip()
            actual = str(row.get(field) or "").strip()
            if expected and actual != expected:
                mismatches.append(f"suite_manifest.{field}:{run_id}")
    return mismatches


def _natural_driving_run_matrix_mismatches(
    report: Mapping[str, Any],
    *,
    matrix_path: Path,
    suite_root: Path,
) -> list[str]:
    rows_by_run_id: dict[str, Mapping[str, str]] = {}
    mismatches: list[str] = []
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            run_id = str(row.get("run_id") or "").strip()
            if not run_id:
                continue
            if run_id in rows_by_run_id:
                mismatches.append(f"run_matrix.duplicate_run_id:{run_id}")
                continue
            rows_by_run_id[run_id] = row
    for run in report.get("run_results") or []:
        if not isinstance(run, Mapping):
            continue
        run_id = str(run.get("run_id") or "").strip()
        if not run_id:
            mismatches.append("run_matrix.missing_report_run_id")
            continue
        row = rows_by_run_id.get(run_id)
        if row is None:
            mismatches.append(f"run_matrix.missing_run_id:{run_id}")
            continue
        mismatches.extend(_suite_plan_execution_mismatches(row, prefix="run_matrix", run_id=run_id))
        for field in ("scenario_id", "scenario_class", "route_id"):
            expected = str(run.get(field) or "").strip()
            actual = str(row.get(field) or "").strip()
            if expected and actual != expected:
                mismatches.append(f"run_matrix.{field}:{run_id}")
        expected_dir_raw = str(run.get("run_dir") or "").strip()
        actual_dir_raw = str(row.get("actual_run_dir") or row.get("run_dir") or "").strip()
        if expected_dir_raw and actual_dir_raw:
            expected_dir = _resolve_suite_plan_context_path(expected_dir_raw, suite_root=suite_root)
            actual_dir = _resolve_suite_plan_context_path(actual_dir_raw, suite_root=suite_root)
            if expected_dir.resolve() != actual_dir.resolve():
                mismatches.append(f"run_matrix.run_dir:{run_id}")
    return mismatches


def _suite_plan_execution_mismatches(
    row: Mapping[str, Any],
    *,
    prefix: str,
    run_id: str,
) -> list[str]:
    mismatches: list[str] = []
    status = str(row.get("status") or "").strip()
    if status != "success":
        mismatches.append(f"{prefix}.status:{run_id}")
    return_code = row.get("return_code")
    try:
        return_code_int = int(return_code)
    except (TypeError, ValueError):
        mismatches.append(f"{prefix}.return_code:{run_id}")
    else:
        if return_code_int != 0:
            mismatches.append(f"{prefix}.return_code:{run_id}")
    artifact_index_status = str(row.get("artifact_index_status") or "").strip()
    if artifact_index_status and artifact_index_status != "found":
        mismatches.append(f"{prefix}.artifact_index_status:{run_id}")
    return mismatches


def _resolve_suite_plan_context_path(raw_path: str, *, suite_root: Path) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    if path.exists():
        return path
    cwd_path = Path.cwd() / path
    if cwd_path.exists():
        return cwd_path
    return suite_root / path


def _natural_driving_suite_root(
    report: Mapping[str, Any],
    *,
    report_path: str | Path | None,
) -> Path | None:
    raw_root = report.get("suite_run_root")
    if raw_root not in {None, ""}:
        root = Path(str(raw_root)).expanduser()
        if root.exists():
            return root.resolve()
        return root
    if report_path in {None, ""}:
        return None
    path = Path(report_path).expanduser()
    candidates = [path.parent]
    candidates.extend(path.parents[:3])
    for candidate in candidates:
        if (candidate / "suite_manifest.json").exists() or (candidate / "run_matrix.csv").exists():
            return candidate.resolve()
    if len(path.parents) >= 3 and path.parent.name == "natural_driving":
        return path.parents[2]
    return path.parent


def _natural_driving_problem_run_details(
    run_results: Sequence[Mapping[str, Any]],
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for run in run_results:
        verdict = str(run.get("verdict") or "")
        if verdict not in {"fail", "insufficient_data", "warn"}:
            continue
        details.append(
            {
                "run_id": run.get("run_id"),
                "scenario_class": run.get("scenario_class"),
                "route_id": run.get("route_id"),
                "verdict": run.get("verdict"),
                "failure_reason": run.get("failure_reason"),
                "missing_artifacts": list(run.get("missing_artifacts") or [])[:8],
                "missing_fields": list(run.get("missing_fields") or [])[:8],
                "runtime_contract_status": run.get("runtime_contract_status"),
                "control_handoff_status": run.get("control_handoff_status"),
                "control_health_status": run.get("control_health_status"),
                "artifact_completeness_status": (
                    run.get("artifact_completeness") or {}
                ).get("status")
                if isinstance(run.get("artifact_completeness"), Mapping)
                else None,
            }
        )
        if len(details) >= limit:
            break
    return details


def _natural_driving_artifact_completeness_blockers(
    run_results: Sequence[Mapping[str, Any]],
    *,
    report: Mapping[str, Any] | None = None,
    report_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for run in run_results:
        artifacts = run.get("artifacts")
        completeness = run.get("artifact_completeness")
        path = artifacts.get("artifact_completeness") if isinstance(artifacts, Mapping) else None
        if not isinstance(completeness, Mapping):
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": run.get("scenario_class"),
                    "reason": "artifact_completeness_missing",
                    "artifact_completeness_path": path,
                }
            )
            continue
        status = completeness.get("status")
        artifact_complete = completeness.get("artifact_complete")
        missing_artifacts = list(completeness.get("missing_artifacts") or [])
        missing_manifest_fields = list(completeness.get("missing_manifest_fields") or [])
        invalid_manifest_source_fields = list(completeness.get("invalid_manifest_source_fields") or [])
        missing_control_trace_fields = list(completeness.get("missing_control_trace_fields") or [])
        invalid_report_source_fields = list(completeness.get("invalid_report_source_fields") or [])
        if (
            status != "pass"
            or artifact_complete is not True
            or missing_artifacts
            or missing_manifest_fields
            or invalid_manifest_source_fields
            or missing_control_trace_fields
            or invalid_report_source_fields
        ):
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": run.get("scenario_class"),
                    "reason": "artifact_completeness_not_pass",
                    "status": status,
                    "artifact_complete": artifact_complete,
                    "missing_artifacts": missing_artifacts[:8],
                    "missing_manifest_fields": missing_manifest_fields[:8],
                    "invalid_manifest_source_fields": invalid_manifest_source_fields[:8],
                    "missing_control_trace_fields": missing_control_trace_fields[:8],
                    "invalid_report_source_fields": invalid_report_source_fields[:8],
                    "artifact_completeness_path": path,
                }
            )
        elif path in {None, ""}:
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": run.get("scenario_class"),
                    "reason": "artifact_completeness_report_path_missing",
                }
            )
        elif report_path is not None:
            file_blocker = _artifact_completeness_file_blocker(
                path,
                run=run,
                report=report or {},
                report_path=report_path,
            )
            if file_blocker is not None:
                blockers.append(file_blocker)
    return blockers


def _artifact_completeness_file_blocker(
    raw_path: Any,
    *,
    run: Mapping[str, Any],
    report: Mapping[str, Any],
    report_path: str | Path,
) -> dict[str, Any] | None:
    resolved_path = _resolve_natural_report_artifact_path(
        raw_path,
        report=report,
        report_path=report_path,
    )
    if resolved_path is None:
        return {
            "run_id": run.get("run_id"),
            "scenario_class": run.get("scenario_class"),
            "reason": "artifact_completeness_report_path_not_found",
            "artifact_completeness_path": raw_path,
        }
    payload = _read_json(resolved_path)
    status = payload.get("status")
    artifact_complete = payload.get("artifact_complete")
    missing_artifacts = list(payload.get("missing_artifacts") or [])
    missing_manifest_fields = list(payload.get("missing_manifest_fields") or [])
    invalid_manifest_source_fields = list(payload.get("invalid_manifest_source_fields") or [])
    missing_control_trace_fields = list(payload.get("missing_control_trace_fields") or [])
    invalid_report_source_fields = list(payload.get("invalid_report_source_fields") or [])
    schema_version = payload.get("schema_version")
    context_mismatches = _artifact_completeness_context_mismatches(
        payload,
        run=run,
        report=report,
        report_path=report_path,
        artifact_path=resolved_path,
    )
    if (
        schema_version != "run_artifact_completeness.v1"
        or status != "pass"
        or artifact_complete is not True
        or missing_artifacts
        or missing_manifest_fields
        or invalid_manifest_source_fields
        or missing_control_trace_fields
        or invalid_report_source_fields
        or context_mismatches
    ):
        return {
            "run_id": run.get("run_id"),
            "scenario_class": run.get("scenario_class"),
            "reason": "artifact_completeness_report_file_not_pass",
            "status": status,
            "artifact_complete": artifact_complete,
            "schema_version": schema_version,
            "missing_artifacts": missing_artifacts[:8],
            "missing_manifest_fields": missing_manifest_fields[:8],
            "invalid_manifest_source_fields": invalid_manifest_source_fields[:8],
            "missing_control_trace_fields": missing_control_trace_fields[:8],
            "invalid_report_source_fields": invalid_report_source_fields[:8],
            "context_mismatches": context_mismatches[:8],
            "artifact_completeness_path": str(resolved_path),
        }
    return None


def _artifact_completeness_context_mismatches(
    payload: Mapping[str, Any],
    *,
    run: Mapping[str, Any],
    report: Mapping[str, Any],
    report_path: str | Path,
    artifact_path: Path,
) -> list[str]:
    mismatches: list[str] = []
    expected_run = str(run.get("run_id") or "").strip()
    actual_run = str(payload.get("run_id") or "").strip()
    if expected_run and actual_run != expected_run:
        mismatches.append("run_id")
    expected_scenario_id = str(run.get("scenario_id") or "").strip()
    actual_scenario_id = str(payload.get("scenario_id") or "").strip()
    if expected_scenario_id and actual_scenario_id != expected_scenario_id:
        mismatches.append("scenario_id")
    expected_scenario = str(run.get("scenario_class") or "").strip()
    actual_scenario = str(payload.get("scenario_class") or "").strip()
    if expected_scenario and actual_scenario != expected_scenario:
        mismatches.append("scenario_class")
    expected_route = str(run.get("route_id") or "").strip()
    actual_route = str(payload.get("route_id") or "").strip()
    if expected_route and actual_route != expected_route:
        mismatches.append("route_id")

    expected_run_dir = _resolve_natural_report_context_path(
        run.get("run_dir"),
        report=report,
        report_path=report_path,
    )
    actual_run_dir = _resolve_natural_report_context_path(
        payload.get("run_dir"),
        report=report,
        report_path=report_path,
        artifact_path=artifact_path,
    )
    if run.get("run_dir") not in {None, ""} and actual_run_dir is None:
        mismatches.append("run_dir")
    elif expected_run_dir is not None and actual_run_dir is not None:
        if expected_run_dir.resolve() != actual_run_dir.resolve():
            mismatches.append("run_dir")
    return mismatches


def _resolve_natural_report_context_path(
    raw_path: Any,
    *,
    report: Mapping[str, Any],
    report_path: str | Path,
    artifact_path: Path | None = None,
) -> Path | None:
    if raw_path in {None, ""}:
        return None
    raw = Path(str(raw_path)).expanduser()
    candidates = [raw]
    if not raw.is_absolute():
        source_path = Path(report_path).expanduser()
        candidates.extend([Path.cwd() / raw, source_path.parent / raw])
        suite_root = report.get("suite_run_root")
        if suite_root not in {None, ""}:
            candidates.append(Path(str(suite_root)).expanduser() / raw)
        if artifact_path is not None:
            candidates.append(artifact_path.parent / raw)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    if raw.is_absolute():
        return raw
    suite_root = report.get("suite_run_root")
    if suite_root not in {None, ""}:
        return Path(str(suite_root)).expanduser() / raw
    return Path(report_path).expanduser().parent / raw


def _resolve_natural_report_artifact_path(
    raw_path: Any,
    *,
    report: Mapping[str, Any],
    report_path: str | Path,
) -> Path | None:
    if raw_path in {None, ""}:
        return None
    raw = Path(str(raw_path)).expanduser()
    candidates = [raw]
    if not raw.is_absolute():
        source_path = Path(report_path).expanduser()
        candidates.extend(
            [
                Path.cwd() / raw,
                source_path.parent / raw,
            ]
        )
        suite_root = report.get("suite_run_root")
        if suite_root not in {None, ""}:
            candidates.append(Path(str(suite_root)).expanduser() / raw)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _traffic_light_expectation_blockers(run_results: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for run in run_results:
        scenario_class = str(run.get("scenario_class") or "")
        expected = TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS.get(scenario_class)
        if expected is None:
            continue
        observed = run.get("traffic_light_expected_behavior")
        source = run.get("traffic_light_expectation_source")
        if observed in {None, ""}:
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "reason": "traffic_light_expectation_missing",
                }
            )
        elif observed != expected:
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "expected_behavior": expected,
                    "observed_behavior": observed,
                    "reason": "traffic_light_expectation_mismatch",
                }
            )
        elif source != "manifest":
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "expected_behavior": expected,
                    "source": source,
                    "reason": "traffic_light_expectation_not_manifest_backed",
                }
            )
        elif run.get("traffic_light_claim_grade") is not True:
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "expected_behavior": expected,
                    "stimulus_mode": run.get("traffic_light_stimulus_mode"),
                    "reason": "traffic_light_stimulus_not_claim_grade",
                }
            )
    return blockers


def _route_evidence_ready_for_demo(sections: Mapping[str, Any]) -> bool:
    ab = sections.get("ab") or {}
    curves = ab.get("curve_diagnostics") or {}
    return (
        ab.get("status") == "ab_hard_gate_pass"
        and (sections.get("natural_driving") or {}).get("status") == "pass"
        and (sections.get("random_regression") or {}).get("status") == "pass"
        and all(
            (curves.get(route_id) or {}).get("status") == "aligned_route_health_evidence"
            for route_id in CURVE_DIAGNOSTIC_ROUTE_IDS
        )
    )


def _attach_demo_goal_acceptance(sections: dict[str, Any]) -> None:
    demo = dict(sections.get("demo_recording") or {})
    readiness_ready = demo.get("status") == "ready"
    evidence_ready = _route_evidence_ready_for_demo(sections)
    demo["accepted_for_goal"] = bool(readiness_ready and evidence_ready)
    if readiness_ready and not evidence_ready:
        demo["claim_not_supported"] = (
            "historical demo readiness does not prove the current Town01 goal until "
            "hard gates, curve route-health evidence, and random regression evidence pass"
        )
    sections["demo_recording"] = demo


def _missing_evidence(sections: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    ab = sections.get("ab") or {}
    if ab.get("status") != "ab_hard_gate_pass":
        missing.append("097/217/031 hard-gate A/B pass under current strict contract")
    direct = (ab.get("direct_cadence") or {}).get("status")
    if direct != "pass":
        missing.append("carla_direct observed transport contract and bridge cadence ratio evidence")
    curves = ab.get("curve_diagnostics") or {}
    for route_id in CURVE_DIAGNOSTIC_ROUTE_IDS:
        if (curves.get(route_id) or {}).get("status") != "aligned_route_health_evidence":
            missing.append(f"{route_id} aligned route-health evidence beyond diagnostic-only status")
    calibration = sections.get("calibration") or {}
    if calibration.get("status") == "missing":
        missing.append("control-actuation calibration report")
    random_regression = sections.get("random_regression") or {}
    if random_regression.get("status") != "pass":
        missing.append("fixed random regression pool A/B evidence")
    natural_driving = sections.get("natural_driving") or {}
    if natural_driving.get("status") != "pass":
        missing.append(
            "natural_driving_report.json pass with can_claim_full_natural_driving=true covering lane, curve, junction, and traffic-light scenarios"
        )
    coherence = sections.get("ab_evidence_coherence") or {}
    if coherence.get("status") in {"mixed_batch", "unknown_multi_report"}:
        missing.append("single coherent A/B evidence batch for final acceptance")
    demo = sections.get("demo_recording") or {}
    if demo.get("status") != "ready":
        missing.append("CARLA + Dreamview demo recording readiness")
    elif demo.get("accepted_for_goal") is not True:
        missing.append("CARLA + Dreamview demo recording after accepted Town01 evidence")
    return missing


def _next_actions(sections: Mapping[str, Any]) -> list[str]:
    actions: list[str] = []
    ab = sections.get("ab") or {}
    hard_gate = ab.get("hard_gate") if isinstance(ab.get("hard_gate"), Mapping) else {}
    direct = ab.get("direct_cadence") if isinstance(ab.get("direct_cadence"), Mapping) else {}
    curves = ab.get("curve_diagnostics") if isinstance(ab.get("curve_diagnostics"), Mapping) else {}
    natural_driving = sections.get("natural_driving") or {}
    random_regression = sections.get("random_regression") or {}
    calibration = sections.get("calibration") or {}
    demo = sections.get("demo_recording") or {}
    coherence = sections.get("ab_evidence_coherence") or {}
    control_blockers = [
        item
        for item in (ab.get("hard_gate_control_context_blockers") or [])
        if isinstance(item, Mapping)
    ]

    if ab.get("status") in {None, "missing"}:
        actions.append("run lane097 direct canary first, then 097/217/031 hard-gate A/B")
    elif ab.get("status") != "ab_hard_gate_pass":
        degraded = hard_gate.get("degraded_routes") or []
        insufficient = hard_gate.get("insufficient_routes") or []
        missing = hard_gate.get("missing_routes") or []
        details = ", ".join(str(item) for item in [*degraded, *insufficient, *missing] if item)
        suffix = f" ({details})" if details else ""
        actions.append(f"rerun or inspect 097/217/031 hard-gate A/B before promotion{suffix}")

    if control_blockers:
        routes = ", ".join(str(item.get("route_id")) for item in control_blockers if item.get("route_id"))
        suffix = f" ({routes})" if routes else ""
        actions.append(
            "inspect Apollo lateral/shadow-mode semantics for hard-gate control-context blockers"
            + suffix
        )
        p1_gap_routes = [
            str(item.get("route_id"))
            for item in control_blockers
            if (item.get("route_curve_artifact_gap") or {}).get("per_frame_p1_complete") is False
            and item.get("route_id")
        ]
        if p1_gap_routes:
            actions.append(
                "collect per-frame P1 matched/target/trajectory fields for lateral semantics gap"
                f" ({', '.join(sorted(set(p1_gap_routes)))})"
            )

    if direct.get("status") not in {None, "pass"}:
        actions.append("inspect carla_direct transport contract and bridge cadence ratio before treating direct as stronger")

    missing_curves = [
        route_id
        for route_id in CURVE_DIAGNOSTIC_ROUTE_IDS
        if (curves.get(route_id) or {}).get("status") != "aligned_route_health_evidence"
    ]
    if missing_curves:
        actions.append(
            "generate route-health evidence for curve diagnostics: " + ", ".join(missing_curves)
        )

    if random_regression.get("status") != "pass":
        actions.append("run fixed random regression pool A/B after hard gates are stable")

    if natural_driving.get("status") != "pass":
        actions.append(
            "run the Town01 natural-driving suite and strict postprocess to produce natural_driving_report.json"
        )
        actions.append("inspect natural-driving problem runs before any lane/curve/junction/traffic-light claim")

    if calibration.get("status") == "missing":
        actions.append("collect calibration report only as control-actuation evidence, not automatic promotion")
    elif calibration.get("promotion_allowed") is not True:
        actions.append("keep calibration non-promotable until 097/217/031 no-regression gates pass")

    if coherence.get("status") in {"mixed_batch", "unknown_multi_report"}:
        actions.append("rerun final A/B as a single coherent batch before final acceptance")

    if demo.get("status") != "ready":
        actions.append("record CARLA + Dreamview demo only after route evidence is stable")
    elif demo.get("accepted_for_goal") is not True:
        actions.append("do not use demo recording as goal proof until hard gates, random regression, and natural-driving evidence pass")

    # Preserve deterministic order while removing duplicates.
    deduped: list[str] = []
    seen: set[str] = set()
    for action in actions:
        if action not in seen:
            deduped.append(action)
            seen.add(action)
    return deduped


def _next_action_commands() -> dict[str, str]:
    python = "/home/ubuntu/miniconda3/envs/carla16/bin/python3"
    return {
        "prepare_validation_packet": (
            f"{python} tools/prepare_town01_goal_validation.py "
            "--out /tmp/town01_goal_validation"
        ),
        "hard_gate_ab_online": (
            f"{python} tools/run_town01_direct_ab.py "
            "--route-config configs/routes/town01/canonical_five.yaml "
            "--durations 30 --baseline ros2_gt --candidate carla_direct "
            "--routes 097,217,031 --continue-on-failure "
            "--carla-ignore-memory-preflight --analyze-after-run "
            "--require-hard-gate-pass "
            "--require-steering-normalization-mode legacy_double_percent "
            "--require-direct-control-apply-mode frame_flush_only "
            "--require-direct-stale-world-frame-policy always_republish "
            "--require-direct-transport-contract-aligned "
            "--require-direct-bridge-cadence-ratio-min 0.8 "
            "--out runs/ab/<hard_gate_batch_id>"
        ),
        "long_window_ab_online": (
            f"{python} tools/run_town01_direct_ab.py "
            "--route-config configs/routes/town01/canonical_five.yaml "
            "--durations 30,60,120 --baseline ros2_gt --candidate carla_direct "
            "--continue-on-failure --include-diagnostic-curves "
            "--carla-ignore-memory-preflight --analyze-after-run "
            "--require-steering-normalization-mode legacy_double_percent "
            "--require-direct-control-apply-mode frame_flush_only "
            "--require-direct-stale-world-frame-policy always_republish "
            "--require-direct-transport-contract-aligned "
            "--require-direct-bridge-cadence-ratio-min 0.8 "
            "--out runs/ab/<long_window_batch_id>"
        ),
        "random_regression_ab_online": (
            f"{python} tools/run_town01_direct_ab.py "
            "--route-config configs/routes/town01/random_regression_pool_20260416.yaml "
            "--durations 30 --baseline ros2_gt --candidate carla_direct "
            "--routes random_lane_183_044,random_lane_213_048,"
            "random_junction_176_063,random_junction_071_063,"
            "random_curve_219_048,random_curve_177_051 "
            "--continue-on-failure --include-informational-routes "
            "--carla-ignore-memory-preflight --analyze-after-run "
            "--require-steering-normalization-mode legacy_double_percent "
            "--require-direct-control-apply-mode frame_flush_only "
            "--require-direct-stale-world-frame-policy always_republish "
            "--require-direct-transport-contract-aligned "
            "--require-direct-bridge-cadence-ratio-min 0.8 "
            "--out runs/ab/<random_batch_id>"
        ),
        "calibration_gate_results_from_ab": (
            f"{python} tools/build_calibration_gate_results.py "
            "--ab-report runs/ab/<hard_gate_batch_id>/analysis/ab_report.json "
            "--out runs/calibration/<calibration_batch_id>/calibration_gate_results.json"
        ),
        "calibration_report_from_trials": (
            f"{python} tools/analyze_calibration_report.py "
            "--profile configs/calibration/control_actuation.yaml "
            "--trials runs/calibration/<calibration_batch_id>/calibration_trials.csv "
            "--gate-results runs/calibration/<calibration_batch_id>/calibration_gate_results.json "
            "--out runs/calibration/<calibration_batch_id>/analysis"
        ),
        "natural_driving_dry_run": (
            f"{python} tools/run_town01_natural_driving_suite.py "
            "--suite configs/scenarios/town01_natural_driving_suite.yaml "
            "--out /tmp/town01_natural_dry --dry-run --continue-on-failure"
        ),
        "natural_driving_online": (
            f"{python} tools/run_town01_natural_driving_suite.py "
            "--suite configs/scenarios/town01_natural_driving_suite.yaml "
            "--out runs/natural/<batch_id> --continue-on-failure "
            "--postprocess-after-run "
            "--fail-on-postprocess-status fail,warn,insufficient_data"
        ),
        "natural_driving_postprocess_existing": (
            f"{python} tools/run_town01_natural_driving_suite.py "
            "--suite configs/scenarios/town01_natural_driving_suite.yaml "
            "--out <suite_root> --postprocess-existing "
            "--fail-on-postprocess-status fail,warn,insufficient_data"
        ),
        "apollo_shadow_mode_from_timeseries": (
            f"{python} tools/analyze_apollo_shadow_mode.py "
            "--timeseries <run_dir>/timeseries.csv "
            "--out <run_dir>/analysis/apollo_shadow_mode"
        ),
        "route_curve_artifact_gap_from_timeseries": (
            f"{python} tools/check_route_curve_artifact_gap.py "
            "--timeseries <run_dir>/timeseries.csv "
            "--summary <run_dir>/summary.json "
            "--out <run_dir>/analysis/route_curve_artifact_gap"
        ),
        "strict_goal_audit": (
            f"{python} tools/audit_town01_goal.py "
            "--out /tmp/town01_goal_audit --fail-on-status incomplete"
        ),
    }


def build_goal_audit(
    *,
    ab_report: Mapping[str, Any] | None = None,
    ab_report_path: str | Path | None = None,
    ab_report_paths: Sequence[str | Path] | None = None,
    calibration_report: Mapping[str, Any] | None = None,
    calibration_report_path: str | Path | None = None,
    natural_driving_report: Mapping[str, Any] | None = None,
    natural_driving_report_path: str | Path | None = None,
    demo_recording: Mapping[str, Any] | None = None,
    demo_recording_path: str | Path | None = None,
    cadence_ratio_min: float = 0.8,
    refresh_ab_from_manifest: bool = False,
) -> dict[str, Any]:
    if ab_report is not None:
        ab_payload = (
            _refresh_ab_report_from_manifest(ab_report)
            if refresh_ab_from_manifest
            else ab_report
        )
    elif ab_report_paths:
        loaded_reports = []
        for path in ab_report_paths:
            payload = _load_ab_report(path, refresh_from_manifest=refresh_ab_from_manifest)
            if payload:
                loaded_reports.append(payload)
        ab_payload = merge_ab_reports(loaded_reports)
    else:
        ab_payload = (
            _load_ab_report(ab_report_path, refresh_from_manifest=refresh_ab_from_manifest)
            if ab_report_path is not None
            else {}
        )
    calibration_payload = (
        calibration_report if calibration_report is not None else _read_json(calibration_report_path)
    )
    natural_driving_payload = (
        natural_driving_report
        if natural_driving_report is not None
        else _read_json(natural_driving_report_path)
    )
    demo_payload = demo_recording if demo_recording is not None else _read_json(demo_recording_path)
    sections = {
        "ab": evaluate_ab_report(ab_payload, cadence_ratio_min=cadence_ratio_min) if ab_payload else {"status": "missing"},
        "ab_evidence_coherence": evaluate_ab_evidence_coherence(ab_payload) if ab_payload else {"status": "missing"},
        "random_regression": evaluate_random_regression(ab_payload) if ab_payload else {"status": "missing"},
        "calibration": evaluate_calibration(calibration_payload),
        "natural_driving": evaluate_natural_driving(
            natural_driving_payload,
            report_path=natural_driving_report_path,
        ),
        "demo_recording": evaluate_demo_recording(demo_payload),
    }
    _attach_demo_goal_acceptance(sections)
    missing = _missing_evidence(sections)
    status = "complete" if not missing else "incomplete"
    next_actions = _next_actions(sections)
    return {
        "schema_version": GOAL_AUDIT_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "status": status,
        "sources": {
            "ab_report": (
                [str(path) for path in ab_report_paths]
                if ab_report_paths
                else (None if ab_report_path is None else str(ab_report_path))
            ),
            "ab_refresh_from_manifest": bool(refresh_ab_from_manifest),
            "calibration_report": None if calibration_report_path is None else str(calibration_report_path),
            "natural_driving_report": None
            if natural_driving_report_path is None
            else str(natural_driving_report_path),
            "demo_recording": None if demo_recording_path is None else str(demo_recording_path),
        },
        "sections": sections,
        "missing_evidence": missing,
        "next_actions": next_actions,
        "next_action_commands": _next_action_commands() if next_actions else {},
        "guardrails": [
            "do not claim carla_direct is default",
            "do not claim curve is solved by short-window direct evidence",
            "do not modify steer_scale=0.25 or enable physical mapping without calibration plus no-regression gates",
        ],
    }


def render_goal_audit_markdown(audit: Mapping[str, Any]) -> str:
    sections = audit.get("sections") or {}
    ab = sections.get("ab") or {}
    direct = ab.get("direct_cadence") or {}
    calibration = sections.get("calibration") or {}
    natural_driving = sections.get("natural_driving") or {}
    demo = sections.get("demo_recording") or {}
    random_regression = sections.get("random_regression") or {}
    coherence = sections.get("ab_evidence_coherence") or {}
    lines = [
        "# Town01 Goal Audit",
        "",
        f"- status: `{audit.get('status')}`",
        f"- schema_version: `{audit.get('schema_version')}`",
        "",
        "## Evidence Status",
        "",
        f"- A/B hard gates: `{ab.get('status')}`",
        f"- direct cadence: `{direct.get('status')}`",
        f"- calibration: `{calibration.get('status')}`",
        f"- random regression: `{random_regression.get('status')}`",
        f"- natural driving: `{natural_driving.get('status')}`",
        f"- natural driving full claim: `{natural_driving.get('can_claim_full_natural_driving')}`",
        f"- natural driving suite plan missing: `{', '.join(natural_driving.get('suite_plan_missing') or [])}`",
        f"- natural driving suite plan files missing: `{', '.join(natural_driving.get('suite_plan_file_missing') or [])}`",
        f"- natural driving filtered suite: `{natural_driving.get('filtered_suite_plan')}`",
        f"- natural driving missing scenario ids: `{len(natural_driving.get('missing_required_scenario_ids') or [])}`",
        f"- natural driving scenario identity mismatches: `{len(natural_driving.get('scenario_identity_mismatches') or [])}`",
        f"- natural driving problem runs: `{len(natural_driving.get('problem_run_details') or [])}`",
        f"- A/B evidence coherence: `{coherence.get('status')}`",
        f"- demo recording: `{demo.get('status')}`",
        "",
        "## Missing Evidence",
        "",
    ]
    missing = audit.get("missing_evidence") or []
    if missing:
        lines.extend(f"- {item}" for item in missing)
    else:
        lines.append("- none")
    hard_gate_details = [
        item for item in ab.get("hard_gate_problem_details") or [] if isinstance(item, Mapping)
    ]
    if hard_gate_details:
        lines.extend(
            [
                "",
                "## A/B Hard-Gate Problem Details",
                "",
                "| route_id | status | reasons | baseline_p95_lat | candidate_p95_lat | baseline_p95_heading | candidate_p95_heading | candidate_failure | first_safety_event | safety_time_s | safety_route_s | safety_cte_m | safety_speed_mps | raw_steer | mapped_steer | applied_steer | control_context | window_context | window_cte_delta_m |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |",
            ]
        )
        for item in hard_gate_details:
            baseline_metrics = item.get("baseline_metrics") if isinstance(item.get("baseline_metrics"), Mapping) else {}
            candidate_metrics = item.get("candidate_metrics") if isinstance(item.get("candidate_metrics"), Mapping) else {}
            safety = (
                item.get("candidate_first_safety_event")
                if isinstance(item.get("candidate_first_safety_event"), Mapping)
                else {}
            )
            lines.append(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                    item.get("route_id"),
                    item.get("status"),
                    "; ".join(str(reason) for reason in item.get("reasons") or []),
                    baseline_metrics.get("lateral_error_p95_m"),
                    candidate_metrics.get("lateral_error_p95_m"),
                    baseline_metrics.get("heading_error_p95_rad"),
                    candidate_metrics.get("heading_error_p95_rad"),
                    candidate_metrics.get("failure_reason"),
                    safety.get("first_safety_event_type"),
                    safety.get("first_safety_event_time_s"),
                    safety.get("first_safety_event_route_s"),
                    safety.get("first_safety_event_cross_track_error_m"),
                    safety.get("first_safety_event_ego_speed_mps"),
                    safety.get("first_safety_event_apollo_steer_raw"),
                    safety.get("first_safety_event_bridge_steer_mapped"),
                    safety.get("first_safety_event_carla_steer_applied"),
                    safety.get("first_safety_event_control_context"),
                    safety.get("safety_window_control_context"),
                    safety.get("safety_window_cross_track_error_delta_m"),
                )
            )
    control_blockers = [
        item
        for item in ab.get("hard_gate_control_context_blockers") or []
        if isinstance(item, Mapping)
    ]
    if control_blockers:
        lines.extend(
            [
                "",
                "## Hard-Gate Control Context Blockers",
                "",
                "| route_id | candidate_run_id | event_context | window_context | route_s | time_s | cte_delta_m | p1_gap_status | missing_p1 | timeseries |",
                "| --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |",
            ]
        )
        for item in control_blockers:
            artifacts = (
                item.get("candidate_artifacts")
                if isinstance(item.get("candidate_artifacts"), Mapping)
                else {}
            )
            gap = (
                item.get("route_curve_artifact_gap")
                if isinstance(item.get("route_curve_artifact_gap"), Mapping)
                else {}
            )
            lines.append(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                    item.get("route_id"),
                    item.get("candidate_run_id"),
                    item.get("event_control_context"),
                    item.get("window_control_context"),
                    item.get("first_safety_event_route_s"),
                    item.get("first_safety_event_time_s"),
                    item.get("safety_window_cross_track_error_delta_m"),
                    gap.get("status"),
                    ", ".join(gap.get("missing_p1_fields") or []),
                    artifacts.get("timeseries_path"),
                )
            )
        commands = [
            str(item.get("shadow_mode_command"))
            for item in control_blockers
            if item.get("shadow_mode_command")
        ]
        if commands:
            lines.extend(["", "Suggested shadow-mode commands:", ""])
            for command in commands[:5]:
                lines.extend(["```bash", command, "```", ""])
        gap_commands = [
            str(item.get("route_curve_artifact_gap_command"))
            for item in control_blockers
            if item.get("route_curve_artifact_gap_command")
        ]
        if gap_commands:
            lines.extend(["", "Suggested route-curve artifact-gap commands:", ""])
            for command in gap_commands[:5]:
                lines.extend(["```bash", command, "```", ""])
    problem_runs = [item for item in natural_driving.get("problem_run_details") or [] if isinstance(item, Mapping)]
    if problem_runs:
        lines.extend(
            [
                "",
                "## Natural Driving Problem Runs",
                "",
                "| run_id | scenario_class | verdict | failure_reason | missing_artifacts | missing_fields |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in problem_runs[:8]:
            lines.append(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                    item.get("run_id"),
                    item.get("scenario_class"),
                    item.get("verdict"),
                    item.get("failure_reason"),
                    ", ".join(item.get("missing_artifacts") or []),
                        ", ".join(item.get("missing_fields") or []),
                )
            )
    traffic_light_blockers = [
        item
        for item in natural_driving.get("traffic_light_expectation_blockers") or []
        if isinstance(item, Mapping)
    ]
    if traffic_light_blockers:
        lines.extend(
            [
                "",
                "## Traffic-Light Expectation Blockers",
                "",
                "| run_id | scenario_class | reason | expected_behavior | observed_behavior | source |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in traffic_light_blockers[:8]:
            lines.append(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                    item.get("run_id"),
                    item.get("scenario_class"),
                    item.get("reason"),
                    item.get("expected_behavior"),
                    item.get("observed_behavior"),
                    item.get("source"),
                )
            )
    artifact_blockers = [
        item
        for item in natural_driving.get("artifact_completeness_blockers") or []
        if isinstance(item, Mapping)
    ]
    if artifact_blockers:
        lines.extend(
            [
                "",
                "## Natural Driving Artifact Completeness Blockers",
                "",
                "| run_id | scenario_class | reason | status | missing_artifacts | missing_manifest_fields | invalid_manifest_source_fields | missing_control_trace_fields | invalid_report_source_fields | report |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in artifact_blockers[:8]:
            lines.append(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                    item.get("run_id"),
                    item.get("scenario_class"),
                    item.get("reason"),
                    item.get("status"),
                    ", ".join(item.get("missing_artifacts") or []),
                    ", ".join(item.get("missing_manifest_fields") or []),
                    ", ".join(item.get("invalid_manifest_source_fields") or []),
                    ", ".join(item.get("missing_control_trace_fields") or []),
                    ", ".join(item.get("invalid_report_source_fields") or []),
                    item.get("artifact_completeness_path"),
                )
            )
    if direct:
        lines.extend(
            [
                "",
                "## Direct Cadence Evidence",
                "",
                f"- status: `{direct.get('status')}`",
                f"- evidence_scope: `{direct.get('evidence_scope')}`",
                f"- ratio_definition: {direct.get('cadence_ratio_definition')}",
                f"- strict_gate_duration_basis: `{direct.get('strict_gate_duration_basis')}`",
                f"- stats_timing_context: {direct.get('stats_timing_context')}",
                f"- claim_supported: {direct.get('claim_supported')}",
                f"- claim_not_supported: {direct.get('claim_not_supported')}",
                "",
                "| route_id | duration_s | contract_status | loc_ratio | chassis_ratio | loc_ratio_stats_time | chassis_ratio_stats_time | policy | source |",
                "| --- | ---: | --- | ---: | ---: | ---: | ---: | --- | --- |",
            ]
        )
        for item in (direct.get("observed") or [])[:12]:
            if not isinstance(item, Mapping):
                continue
            lines.append(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                    item.get("route_id"),
                    item.get("duration_s"),
                    item.get("contract_status"),
                    item.get("loc_ratio"),
                    item.get("chassis_ratio"),
                    item.get("loc_ratio_by_stats_sim_time"),
                    item.get("chassis_ratio_by_stats_sim_time"),
                    item.get("policy"),
                    item.get("policy_source"),
                )
            )
    lines.extend(["", "## Next Actions", ""])
    next_actions = audit.get("next_actions") or []
    if next_actions:
        lines.extend(f"- {item}" for item in next_actions)
    else:
        lines.append("- none")
    commands = audit.get("next_action_commands") if isinstance(audit.get("next_action_commands"), Mapping) else {}
    if commands:
        lines.extend(["", "## Recommended Commands", ""])
        for label, command in commands.items():
            lines.extend([f"### {label}", "", "```bash", str(command), "```", ""])
    lines.extend(["", "## Guardrails", ""])
    lines.extend(f"- {item}" for item in audit.get("guardrails") or [])
    lines.append("")
    return "\n".join(lines)
