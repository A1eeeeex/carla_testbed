#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import (
    default_corpus_path,
    default_corpus_report_path,
    _normalized_recommended_subsets,
    _sync_capability_first_wave_recommended_uses,
    _sync_capability_recommended_uses,
    _sync_route_geometry_metrics,
    render_corpus_report,
    safe_bool,
    safe_float,
    safe_int,
    write_json,
)


DYNAMIC_HEALTH_TAGS = {
    "unreviewed",
    "empirically_reviewed",
    "guarded_lateral_runtime_ok",
    "guarded_lateral_repeat_verified",
    "repeat_instability_risk",
    "current_time_smaller_precursor_risk",
    "reference_line_provider_bridge_risk",
    "reroute_propagation_frame_bridge_risk",
    "persistent_path_fallback_risk",
    "recoverable_fallback_pressure",
    "relapse_heavy_after_recovery",
    "deprioritized_lateral_smoke",
    "route_health_candidate",
    "route_health_pass",
    "lane_keep_semantic_history",
    "curve_lane_follow_semantic_history",
    "junction_traverse_semantic_history",
    "traffic_light_semantic_history",
}

SEMANTIC_HISTORY_KEY_BY_SUBSET = {
    "lane_keep_candidate": "lane_keep",
    "curve_lane_follow_candidate": "curve_lane_follow",
    "junction_traverse_candidate": "junction_traverse",
    "traffic_light_candidate": "traffic_light",
}


def _semantic_label_rank(label: str) -> int:
    normalized = str(label or "").strip()
    if normalized == "route_health_pass":
        return 0
    if normalized == "route_health_candidate":
        return 1
    if normalized:
        return 2
    return 3


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to parse JSON from {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return payload


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception as exc:
        raise RuntimeError(f"Failed to parse CSV from {path}: {exc}") from exc


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to parse YAML from {path}: {exc}") from exc
    return payload if isinstance(payload, dict) else {}


def _load_many_csv_rows(paths: List[Path]) -> List[Dict[str, str]]:
    merged: List[Dict[str, str]] = []
    seen_keys = set()
    for path in paths:
        for row in _load_csv_rows(path):
            key = (
                str(row.get("run_dir") or "").strip(),
                str(row.get("route_id") or "").strip(),
                str(row.get("comparison_label") or "").strip(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged.append(row)
    return merged


def _nested_get(payload: Dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _historical_semantic_match(candidate_subset: str, effective_cfg: Dict[str, Any]) -> bool:
    lane_follow_only = safe_bool(_nested_get(effective_cfg, "algo", "apollo", "planning", "lane_follow_only_scenario"))
    disable_tl_rule = safe_bool(_nested_get(effective_cfg, "algo", "apollo", "planning", "disable_traffic_light_rule"))
    traffic_light_policy = str(_nested_get(effective_cfg, "algo", "apollo", "traffic_light", "policy") or "").strip()
    traffic_light_freeze = safe_bool(_nested_get(effective_cfg, "scenario", "traffic_lights", "freeze"))
    spawn_reject_junction = safe_bool(_nested_get(effective_cfg, "scenario", "route_health", "spawn_reject_junction"))
    goal_reject_junction = safe_bool(_nested_get(effective_cfg, "scenario", "route_health", "goal_reject_junction"))
    if candidate_subset in {"lane_keep_candidate", "curve_lane_follow_candidate"}:
        return lane_follow_only is True and disable_tl_rule is True
    if candidate_subset == "junction_traverse_candidate":
        return (
            lane_follow_only is False
            and disable_tl_rule is True
            and spawn_reject_junction is False
            and goal_reject_junction is False
        )
    if candidate_subset == "traffic_light_candidate":
        return (
            lane_follow_only is False
            and disable_tl_rule is False
            and traffic_light_policy == "carla_actual"
            and traffic_light_freeze is False
        )
    return False


def _semantic_history_evidence_by_route(runs_root: Path) -> Dict[str, Dict[str, Any]]:
    evidence_by_route: Dict[str, Dict[str, Any]] = {}
    if not runs_root.exists():
        return evidence_by_route
    for summary_path in sorted(runs_root.rglob("summary.json")):
        try:
            payload = _load_json(summary_path)
        except RuntimeError:
            continue
        route_id = str(payload.get("route_id") or "").strip()
        if not route_id:
            continue
        effective_path = summary_path.with_name("effective.yaml")
        effective_cfg = _load_yaml(effective_path) if effective_path.exists() else {}
        route_evidence = evidence_by_route.setdefault(route_id, {})
        label = str(payload.get("route_health_label") or "").strip()
        completion = safe_float(payload.get("route_completion_ratio")) or 0.0
        comparison_label = str(payload.get("comparison_label") or "").strip()
        for candidate_subset, prefix in SEMANTIC_HISTORY_KEY_BY_SUBSET.items():
            if not _historical_semantic_match(candidate_subset, effective_cfg):
                continue
            run_count_key = f"{prefix}_semantic_run_count"
            route_evidence[run_count_key] = int(route_evidence.get(run_count_key) or 0) + 1
            route_evidence[f"has_{prefix}_semantic_history"] = True
            best_label_key = f"{prefix}_semantic_best_label"
            best_completion_key = f"{prefix}_semantic_best_completion"
            best_comparison_key = f"{prefix}_semantic_best_comparison_label"
            current_label = str(route_evidence.get(best_label_key) or "").strip()
            current_completion = safe_float(route_evidence.get(best_completion_key)) or 0.0
            if (
                best_label_key not in route_evidence
                or _semantic_label_rank(label) < _semantic_label_rank(current_label)
                or (
                    _semantic_label_rank(label) == _semantic_label_rank(current_label)
                    and float(completion) > float(current_completion)
                )
            ):
                route_evidence[best_label_key] = label
                route_evidence[best_completion_key] = float(completion)
                route_evidence[best_comparison_key] = comparison_label
            if label == "route_health_pass":
                route_evidence[f"has_{prefix}_semantic_pass"] = True
            elif label == "route_health_candidate":
                route_evidence[f"has_{prefix}_semantic_candidate"] = True
            elif label:
                route_evidence[f"has_{prefix}_semantic_reviewed"] = True
    return evidence_by_route


def _is_stage6_experimental_row(row: Dict[str, str]) -> bool:
    return any(
        safe_bool(row.get(key)) is True
        for key in (
            "stage6_clear_lane_follow_cache_on_new_command",
            "effective_stage6_clear_lane_follow_cache_on_new_command",
            "stage6_reference_line_generation_guard",
            "effective_stage6_reference_line_generation_guard",
        )
    )


def _aggregate_route_evidence(
    rows: List[Dict[str, str]], *, include_stage6_experimental: bool = False
) -> Dict[str, Dict[str, Any]]:
    route_evidence: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not include_stage6_experimental and _is_stage6_experimental_row(row):
            continue
        route_id = str(row.get("route_id") or "").strip()
        if not route_id:
            continue
        evidence = route_evidence.setdefault(
            route_id,
            {
                "comparison_labels": [],
                "seen_count": 0,
                "best_completion": 0.0,
                "best_distance_m": 0.0,
                "best_lateral_completion": 0.0,
                "best_lateral_distance_m": 0.0,
                "max_lateral_path_fallback_count": 0,
                "guarded_runtime_ok_labels": [],
                "guarded_repeat_ok_labels": [],
                "guarded_unstable_labels": [],
                "has_persistent_path_fallback": False,
                "has_recoverable_fallback_pressure": False,
                "has_relapse_heavy_after_recovery": False,
                "has_route_health_candidate": False,
                "has_route_health_pass": False,
                "has_guarded_lateral_runtime_ok": False,
                "has_guarded_lateral_repeat_verified": False,
                "has_repeat_instability_risk": False,
                "has_current_time_smaller_precursor_risk": False,
                "has_reference_line_provider_bridge_risk": False,
                "has_reroute_propagation_frame_bridge_risk": False,
            },
        )
        evidence["seen_count"] = int(evidence["seen_count"]) + 1
        comparison_label = str(row.get("comparison_label") or "").strip()
        if comparison_label and comparison_label not in evidence["comparison_labels"]:
            evidence["comparison_labels"].append(comparison_label)
        completion = safe_float(row.get("route_completion_ratio")) or 0.0
        distance_m = safe_float(row.get("route_distance_achieved_m")) or 0.0
        evidence["best_completion"] = max(float(evidence["best_completion"]), completion)
        evidence["best_distance_m"] = max(float(evidence["best_distance_m"]), distance_m)
        enable_lateral = safe_bool(row.get("enable_lateral")) is True
        if enable_lateral:
            evidence["best_lateral_completion"] = max(float(evidence["best_lateral_completion"]), completion)
            evidence["best_lateral_distance_m"] = max(float(evidence["best_lateral_distance_m"]), distance_m)
            evidence["max_lateral_path_fallback_count"] = max(
                int(evidence["max_lateral_path_fallback_count"]),
                safe_int(row.get("path_fallback_count")) or 0,
            )
        if safe_bool(row.get("persistent_path_fallback_at_end")) is True:
            evidence["has_persistent_path_fallback"] = True
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and safe_bool(row.get("persistent_path_fallback_at_end")) is not True
            and (safe_int(row.get("path_fallback_count")) or 0) >= 200
            and completion < 0.50
            and distance_m < 100.0
        ):
            evidence["has_recoverable_fallback_pressure"] = True
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and safe_bool(row.get("persistent_path_fallback_at_end")) is not True
            and (safe_int(row.get("path_fallback_count_after_first_recovery")) or 0) >= 100
            and (safe_int(row.get("first_relapse_cluster_length")) or 0) >= 12
            and (safe_int(row.get("final_normal_tail_length")) or 0) <= 10
            and completion < 0.50
        ):
            evidence["has_relapse_heavy_after_recovery"] = True
        label = str(row.get("route_health_label") or "").strip()
        if label == "route_health_candidate":
            evidence["has_route_health_candidate"] = True
        if label == "route_health_pass":
            evidence["has_route_health_pass"] = True
            evidence["has_route_health_candidate"] = True
        guard_trigger_ratio = safe_float(row.get("guard_trigger_ratio")) or 0.0
        raw_steer_saturated_ratio = safe_float(row.get("raw_steer_saturated_ratio")) or 0.0
        control_used_planning_ratio = safe_float(row.get("control_used_planning_ratio")) or 0.0
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and safe_bool(row.get("persistent_path_fallback_at_end")) is not True
            and control_used_planning_ratio >= 0.80
            and completion >= 0.50
            and distance_m >= 80.0
            and raw_steer_saturated_ratio <= 0.15
            and guard_trigger_ratio <= 0.05
        ):
            evidence["has_guarded_lateral_runtime_ok"] = True
            labels = list(evidence.get("guarded_runtime_ok_labels") or [])
            if comparison_label and comparison_label not in labels:
                labels.append(comparison_label)
                evidence["guarded_runtime_ok_labels"] = labels
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and safe_bool(row.get("persistent_path_fallback_at_end")) is not True
            and control_used_planning_ratio >= 0.80
            and completion >= 0.75
            and distance_m >= 140.0
            and raw_steer_saturated_ratio <= 0.15
            and guard_trigger_ratio <= 0.05
            and (safe_int(row.get("path_fallback_count_after_first_recovery")) or 0) <= 10
            and (safe_int(row.get("final_normal_tail_length")) or 0) >= 1000
        ):
            labels = list(evidence.get("guarded_repeat_ok_labels") or [])
            if comparison_label and comparison_label not in labels:
                labels.append(comparison_label)
                evidence["guarded_repeat_ok_labels"] = labels
            if len(list(evidence.get("guarded_repeat_ok_labels") or [])) >= 2:
                evidence["has_guarded_lateral_repeat_verified"] = True
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and (
                safe_bool(row.get("persistent_path_fallback_at_end")) is True
                or (
                    (safe_int(row.get("path_fallback_count_after_first_recovery")) or 0) >= 100
                    and (safe_int(row.get("final_normal_tail_length")) or 0) <= 10
                )
            )
        ):
            labels = list(evidence.get("guarded_unstable_labels") or [])
            if comparison_label and comparison_label not in labels:
                labels.append(comparison_label)
                evidence["guarded_unstable_labels"] = labels
        runtime_labels = set(evidence.get("guarded_runtime_ok_labels") or [])
        unstable_labels = set(evidence.get("guarded_unstable_labels") or [])
        if runtime_labels and unstable_labels and len(runtime_labels.union(unstable_labels)) >= 2:
            evidence["has_repeat_instability_risk"] = True
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and (safe_int(row.get("first_path_fallback_precurrent_time_smaller_normal_count")) or 0) >= 3
            and (safe_float(row.get("first_path_fallback_precurrent_time_smaller_rel_min_max")) or 0.0) > 0.0
        ):
            evidence["has_current_time_smaller_precursor_risk"] = True
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and safe_bool(row.get("first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready")) is True
            and safe_int(row.get("first_path_fallback_bridge_empty_previous_seq")) is not None
        ):
            evidence["has_reference_line_provider_bridge_risk"] = True
        if (
            enable_lateral
            and safe_bool(row.get("enable_guard")) is True
            and safe_bool(row.get("first_path_fallback_bridge_occurs_on_reroute_propagation_frame")) is True
        ):
            evidence["has_reroute_propagation_frame_bridge_risk"] = True
    return route_evidence


def _updated_route(route: Dict[str, Any], evidence: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    updated = _sync_route_geometry_metrics(dict(route))
    original_tags = [str(tag).strip() for tag in list(route.get("health_tags") or []) if str(tag).strip()]
    retained_tags = list(original_tags)
    recommended_uses = [str(entry).strip() for entry in list(route.get("recommended_uses") or []) if str(entry).strip()]
    if evidence:
        retained_tags = [tag for tag in retained_tags if tag not in DYNAMIC_HEALTH_TAGS]
        recommended_uses = [
            entry
            for entry in recommended_uses
            if entry not in {"guarded_lateral_first_wave_smoke", "guarded_lateral_repeat_verified_smoke"}
        ]
        retained_tags.append("empirically_reviewed")
        persistent_path_fallback = bool(evidence.get("has_persistent_path_fallback"))
        recoverable_fallback_pressure = bool(evidence.get("has_recoverable_fallback_pressure"))
        relapse_heavy_after_recovery = bool(evidence.get("has_relapse_heavy_after_recovery"))
        repeat_instability_risk = bool(evidence.get("has_repeat_instability_risk"))
        current_time_smaller_precursor_risk = bool(evidence.get("has_current_time_smaller_precursor_risk"))
        reference_line_provider_bridge_risk = bool(evidence.get("has_reference_line_provider_bridge_risk"))
        reroute_propagation_frame_bridge_risk = bool(evidence.get("has_reroute_propagation_frame_bridge_risk"))
        if bool(evidence.get("has_guarded_lateral_runtime_ok")) and not persistent_path_fallback:
            retained_tags.append("guarded_lateral_runtime_ok")
            if (
                not recoverable_fallback_pressure
                and not relapse_heavy_after_recovery
                and not repeat_instability_risk
                and not current_time_smaller_precursor_risk
                and not reference_line_provider_bridge_risk
                and not reroute_propagation_frame_bridge_risk
            ):
                recommended_uses.append("guarded_lateral_first_wave_smoke")
        if (
            bool(evidence.get("has_guarded_lateral_repeat_verified"))
            and not persistent_path_fallback
            and not recoverable_fallback_pressure
            and not relapse_heavy_after_recovery
            and not repeat_instability_risk
            and not current_time_smaller_precursor_risk
            and not reference_line_provider_bridge_risk
            and not reroute_propagation_frame_bridge_risk
        ):
            retained_tags.append("guarded_lateral_repeat_verified")
            recommended_uses.append("guarded_lateral_repeat_verified_smoke")
        if reroute_propagation_frame_bridge_risk:
            retained_tags.extend(["reroute_propagation_frame_bridge_risk", "deprioritized_lateral_smoke"])
            recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_first_wave_smoke"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_repeat_verified_smoke"]
        if reference_line_provider_bridge_risk:
            retained_tags.extend(["reference_line_provider_bridge_risk", "deprioritized_lateral_smoke"])
            recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_first_wave_smoke"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_repeat_verified_smoke"]
        if current_time_smaller_precursor_risk:
            retained_tags.extend(["current_time_smaller_precursor_risk", "deprioritized_lateral_smoke"])
            recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_first_wave_smoke"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_repeat_verified_smoke"]
        if repeat_instability_risk:
            retained_tags.extend(["repeat_instability_risk", "deprioritized_lateral_smoke"])
            recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_first_wave_smoke"]
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_repeat_verified_smoke"]
        if persistent_path_fallback:
            retained_tags.extend(["persistent_path_fallback_risk", "deprioritized_lateral_smoke"])
            recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
        else:
            if recoverable_fallback_pressure:
                retained_tags.extend(["recoverable_fallback_pressure", "deprioritized_lateral_smoke"])
                recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
                recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_first_wave_smoke"]
                recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_repeat_verified_smoke"]
            if relapse_heavy_after_recovery:
                retained_tags.extend(["relapse_heavy_after_recovery", "deprioritized_lateral_smoke"])
                recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
                recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_first_wave_smoke"]
                recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_repeat_verified_smoke"]
        if bool(evidence.get("has_route_health_candidate")):
            retained_tags.append("route_health_candidate")
        if bool(evidence.get("has_route_health_pass")):
            retained_tags.append("route_health_pass")
        if bool(evidence.get("has_lane_keep_semantic_history")):
            retained_tags.append("lane_keep_semantic_history")
        if bool(evidence.get("has_curve_lane_follow_semantic_history")):
            retained_tags.append("curve_lane_follow_semantic_history")
        if bool(evidence.get("has_junction_traverse_semantic_history")):
            retained_tags.append("junction_traverse_semantic_history")
        if bool(evidence.get("has_traffic_light_semantic_history")):
            retained_tags.append("traffic_light_semantic_history")
        best_lateral_completion = safe_float(evidence.get("best_lateral_completion")) or 0.0
        best_lateral_distance_m = safe_float(evidence.get("best_lateral_distance_m")) or 0.0
        if (
            best_lateral_distance_m > 0.0
            and not persistent_path_fallback
            and not bool(evidence.get("has_guarded_lateral_runtime_ok"))
            and not bool(evidence.get("has_route_health_candidate"))
            and not bool(evidence.get("has_route_health_pass"))
            and (best_lateral_completion < 0.50 or best_lateral_distance_m < 100.0)
        ):
            retained_tags.append("deprioritized_lateral_smoke")
            recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
    elif not retained_tags:
        retained_tags.append("unreviewed")
    updated["health_tags"] = sorted(set(retained_tags))
    updated["recommended_uses"] = _sync_capability_recommended_uses(updated, recommended_uses)
    return updated


def _refresh_corpus(corpus: Dict[str, Any], evidence_by_route: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    refreshed = dict(corpus)
    refreshed_routes: List[Dict[str, Any]] = []
    for route in list(corpus.get("routes") or []):
        if not isinstance(route, dict):
            continue
        route_id = str(route.get("route_id") or "").strip()
        refreshed_routes.append(_updated_route(route, evidence_by_route.get(route_id)))
    refreshed["routes"] = refreshed_routes
    refreshed["empirical_health_summary"] = {
        route_id: {
            "comparison_labels": sorted(list(evidence.get("comparison_labels") or [])),
            "best_completion": float(evidence.get("best_completion") or 0.0),
            "best_distance_m": float(evidence.get("best_distance_m") or 0.0),
            "best_lateral_completion": float(evidence.get("best_lateral_completion") or 0.0),
            "best_lateral_distance_m": float(evidence.get("best_lateral_distance_m") or 0.0),
            "max_lateral_path_fallback_count": int(evidence.get("max_lateral_path_fallback_count") or 0),
            "guarded_runtime_ok_labels": sorted(list(evidence.get("guarded_runtime_ok_labels") or [])),
            "has_persistent_path_fallback": bool(evidence.get("has_persistent_path_fallback")),
            "has_recoverable_fallback_pressure": bool(evidence.get("has_recoverable_fallback_pressure")),
            "has_relapse_heavy_after_recovery": bool(evidence.get("has_relapse_heavy_after_recovery")),
            "has_route_health_candidate": bool(evidence.get("has_route_health_candidate")),
            "has_route_health_pass": bool(evidence.get("has_route_health_pass")),
            "has_guarded_lateral_runtime_ok": bool(evidence.get("has_guarded_lateral_runtime_ok")),
            "guarded_repeat_ok_labels": sorted(list(evidence.get("guarded_repeat_ok_labels") or [])),
            "guarded_unstable_labels": sorted(list(evidence.get("guarded_unstable_labels") or [])),
            "has_guarded_lateral_repeat_verified": bool(evidence.get("has_guarded_lateral_repeat_verified")),
            "has_repeat_instability_risk": bool(evidence.get("has_repeat_instability_risk")),
            "has_current_time_smaller_precursor_risk": bool(evidence.get("has_current_time_smaller_precursor_risk")),
            "has_reference_line_provider_bridge_risk": bool(evidence.get("has_reference_line_provider_bridge_risk")),
            "has_reroute_propagation_frame_bridge_risk": bool(
                evidence.get("has_reroute_propagation_frame_bridge_risk")
            ),
            "has_lane_keep_semantic_history": bool(evidence.get("has_lane_keep_semantic_history")),
            "lane_keep_semantic_run_count": int(evidence.get("lane_keep_semantic_run_count") or 0),
            "lane_keep_semantic_best_label": str(evidence.get("lane_keep_semantic_best_label") or "").strip(),
            "lane_keep_semantic_best_completion": float(evidence.get("lane_keep_semantic_best_completion") or 0.0),
            "lane_keep_semantic_best_comparison_label": str(
                evidence.get("lane_keep_semantic_best_comparison_label") or ""
            ).strip(),
            "has_curve_lane_follow_semantic_history": bool(evidence.get("has_curve_lane_follow_semantic_history")),
            "curve_lane_follow_semantic_run_count": int(evidence.get("curve_lane_follow_semantic_run_count") or 0),
            "curve_lane_follow_semantic_best_label": str(
                evidence.get("curve_lane_follow_semantic_best_label") or ""
            ).strip(),
            "curve_lane_follow_semantic_best_completion": float(
                evidence.get("curve_lane_follow_semantic_best_completion") or 0.0
            ),
            "curve_lane_follow_semantic_best_comparison_label": str(
                evidence.get("curve_lane_follow_semantic_best_comparison_label") or ""
            ).strip(),
            "has_junction_traverse_semantic_history": bool(evidence.get("has_junction_traverse_semantic_history")),
            "junction_traverse_semantic_run_count": int(evidence.get("junction_traverse_semantic_run_count") or 0),
            "junction_traverse_semantic_best_label": str(
                evidence.get("junction_traverse_semantic_best_label") or ""
            ).strip(),
            "junction_traverse_semantic_best_completion": float(
                evidence.get("junction_traverse_semantic_best_completion") or 0.0
            ),
            "junction_traverse_semantic_best_comparison_label": str(
                evidence.get("junction_traverse_semantic_best_comparison_label") or ""
            ).strip(),
            "has_traffic_light_semantic_history": bool(evidence.get("has_traffic_light_semantic_history")),
            "traffic_light_semantic_run_count": int(evidence.get("traffic_light_semantic_run_count") or 0),
            "traffic_light_semantic_best_label": str(
                evidence.get("traffic_light_semantic_best_label") or ""
            ).strip(),
            "traffic_light_semantic_best_completion": float(
                evidence.get("traffic_light_semantic_best_completion") or 0.0
            ),
            "traffic_light_semantic_best_comparison_label": str(
                evidence.get("traffic_light_semantic_best_comparison_label") or ""
            ).strip(),
        }
        for route_id, evidence in sorted(evidence_by_route.items())
    }
    refreshed["recommended_subsets"] = _normalized_recommended_subsets(refreshed)
    refreshed["routes"] = _sync_capability_first_wave_recommended_uses(
        [item for item in list(refreshed.get("routes") or []) if isinstance(item, dict)],
        refreshed["recommended_subsets"],
    )
    refreshed["recommended_subsets"] = _normalized_recommended_subsets(refreshed)
    return refreshed


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Town01 route corpus health tags from comparison evidence.")
    parser.add_argument(
        "--corpus",
        default=str(default_corpus_path(REPO_ROOT)),
        help="Path to route corpus JSON.",
    )
    parser.add_argument(
        "--comparison-csv",
        action="append",
        dest="comparison_csvs",
        help="Comparison CSV path. Can be repeated to merge active batch-local comparison CSVs.",
    )
    parser.add_argument(
        "--report",
        default=str(default_corpus_report_path(REPO_ROOT)),
        help="Corpus report output path.",
    )
    parser.add_argument(
        "--include-stage6-experimental",
        action="store_true",
        help=(
            "Include stage6 experimental comparison rows "
            "(for example cache-clear / generation-guard probes) when refreshing corpus tags."
        ),
    )
    parser.add_argument(
        "--runs-root",
        default="runs",
        help="Runs root used to scan historical capability-semantic evidence.",
    )
    args = parser.parse_args()

    corpus_path = Path(args.corpus).expanduser().resolve()
    comparison_csvs = [
        Path(item).expanduser().resolve()
        for item in (args.comparison_csvs or ["artifacts/town01_route_health_platform_comparison.csv"])
    ]
    report_path = Path(args.report).expanduser().resolve()
    runs_root = Path(args.runs_root).expanduser().resolve()

    corpus = _load_json(corpus_path)
    comparison_rows = _load_many_csv_rows(comparison_csvs)
    skipped_stage6_rows = sum(1 for row in comparison_rows if _is_stage6_experimental_row(row))
    evidence_by_route = _aggregate_route_evidence(
        comparison_rows,
        include_stage6_experimental=args.include_stage6_experimental,
    )
    for route_id, semantic_evidence in _semantic_history_evidence_by_route(runs_root).items():
        evidence_by_route.setdefault(route_id, {}).update(semantic_evidence)
    refreshed = _refresh_corpus(corpus, evidence_by_route)

    write_json(corpus_path, refreshed)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_corpus_report(refreshed), encoding="utf-8")

    tagged_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "empirically_reviewed" in set(route.get("health_tags") or [])
    )
    persistent_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "persistent_path_fallback_risk" in set(route.get("health_tags") or [])
    )
    recoverable_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "recoverable_fallback_pressure" in set(route.get("health_tags") or [])
    )
    repeat_verified_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "guarded_lateral_repeat_verified" in set(route.get("health_tags") or [])
    )
    repeat_instability_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "repeat_instability_risk" in set(route.get("health_tags") or [])
    )
    precursor_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "current_time_smaller_precursor_risk" in set(route.get("health_tags") or [])
    )
    bridge_risk_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "reference_line_provider_bridge_risk" in set(route.get("health_tags") or [])
    )
    propagation_bridge_risk_routes = sum(
        1
        for route in list(refreshed.get("routes") or [])
        if isinstance(route, dict) and "reroute_propagation_frame_bridge_risk" in set(route.get("health_tags") or [])
    )
    print(
        "[town01-route-corpus-health] "
        f"routes={len(list(refreshed.get('routes') or []))} "
        f"empirically_reviewed={tagged_routes} "
        f"guarded_lateral_repeat_verified={repeat_verified_routes} "
        f"repeat_instability_risk={repeat_instability_routes} "
        f"current_time_smaller_precursor_risk={precursor_routes} "
        f"reference_line_provider_bridge_risk={bridge_risk_routes} "
        f"reroute_propagation_frame_bridge_risk={propagation_bridge_risk_routes} "
        f"skipped_stage6_experimental_rows="
        f"{0 if args.include_stage6_experimental else skipped_stage6_rows} "
        f"persistent_path_fallback_risk={persistent_routes} "
        f"recoverable_fallback_pressure={recoverable_routes} "
        f"comparison_csv_count={len(comparison_csvs)} "
        f"corpus={corpus_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
