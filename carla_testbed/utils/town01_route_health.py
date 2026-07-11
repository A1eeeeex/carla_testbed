from __future__ import annotations

import bisect
import csv
from datetime import datetime
import json
import math
from pathlib import Path
import random
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml


CORE_ACCEPTANCE_POLICY: Dict[str, Any] = {
    "routing_request_count_min": 1,
    "route_establishment_latency_sec_max": 45.0,
    "planning_nonzero_ratio_min": 0.80,
    "control_used_planning_ratio_min": 0.80,
    "invalid_goal_count_max": 0,
    "long_phase_invalid_goal_skip_max": 0,
    "unstable_reference_line_skip_max": 0,
    "control_no_trajectory_count_max": 0,
    "route_distance_achieved_m_min": 140.0,
    "route_completion_ratio_min": 0.55,
    "max_speed_mps_min": 5.0,
    "low_speed_creep_duration_sec_max": 2.0,
    "route_health_pass_completion_ratio_min": 0.75,
    "lateral_max_raw_steer_saturated_ratio": 0.15,
    "lateral_max_longest_saturation_sec": 15.0,
    "guard_trigger_ratio_max": 0.05,
    "completion_goal_distance_m_max": 10.0,
}

DEFAULT_FLAGS: Dict[str, Any] = {
    "enable_lateral": False,
    "enable_guard": True,
    "enable_lateral_debug": False,
    "enable_recording": False,
    "enable_strict_observation": False,
    "enable_stage6_reference_line": False,
    "stage6_clear_lane_follow_cache_on_new_command": False,
    "stage6_reference_line_generation_guard": False,
}

ALIAS_PRESETS: Dict[str, Dict[str, Any]] = {
    "minimal": {
        "enable_lateral": False,
        "enable_guard": True,
        "enable_lateral_debug": False,
        "enable_recording": False,
        "enable_strict_observation": False,
        "enable_stage6_reference_line": False,
        "stage6_clear_lane_follow_cache_on_new_command": False,
        "stage6_reference_line_generation_guard": False,
    },
    "relaxed": {
        "enable_lateral": False,
        "enable_guard": True,
        "enable_lateral_debug": False,
        "enable_recording": False,
        "enable_strict_observation": False,
        "enable_stage6_reference_line": False,
        "stage6_clear_lane_follow_cache_on_new_command": False,
        "stage6_reference_line_generation_guard": False,
    },
    "strict": {
        "enable_lateral": False,
        "enable_guard": True,
        "enable_lateral_debug": False,
        "enable_recording": True,
        "enable_strict_observation": True,
        "enable_stage6_reference_line": False,
        "stage6_clear_lane_follow_cache_on_new_command": False,
        "stage6_reference_line_generation_guard": False,
    },
    "lateral_enabled": {
        "enable_lateral": True,
        "enable_guard": True,
        "enable_lateral_debug": False,
        "enable_recording": False,
        "enable_strict_observation": False,
        "enable_stage6_reference_line": False,
        "stage6_clear_lane_follow_cache_on_new_command": False,
        "stage6_reference_line_generation_guard": False,
    },
    "lateral_enabled_raw": {
        "enable_lateral": True,
        "enable_guard": False,
        "enable_lateral_debug": False,
        "enable_recording": False,
        "enable_strict_observation": False,
        "enable_stage6_reference_line": False,
        "stage6_clear_lane_follow_cache_on_new_command": False,
        "stage6_reference_line_generation_guard": False,
    },
    "lateral_enabled_guarded": {
        "enable_lateral": True,
        "enable_guard": True,
        "enable_lateral_debug": True,
        "enable_recording": False,
        "enable_strict_observation": False,
        "enable_stage6_reference_line": False,
        "stage6_clear_lane_follow_cache_on_new_command": False,
        "stage6_reference_line_generation_guard": False,
    },
}

CAPABILITY_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_candidate",
    "curve_lane_follow_candidate",
    "junction_traverse_candidate",
    "traffic_light_candidate",
)

TRUE_LATERAL_CAPABILITY_PROFILES = {
    "curve_lane_follow",
    "junction_traverse",
    "traffic_light_actual",
}

REAR_AXLE_LOCALIZATION_BACK_OFFSET_M = 1.4235
REAR_AXLE_LOCALIZATION_BACK_OFFSET_TOLERANCE_M = 0.05
APOLLO_CONTROL_STATE_REFERENCE = "rear_axle_input_com_internal"

CAPABILITY_FIRST_WAVE_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_first_wave_smoke",
    "curve_lane_follow_first_wave_smoke",
    "junction_traverse_first_wave_smoke",
    "traffic_light_first_wave_smoke",
)

CAPABILITY_NEXT_REVIEW_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_next_review_queue",
    "curve_lane_follow_next_review_queue",
    "junction_traverse_next_review_queue",
    "traffic_light_next_review_queue",
)

CAPABILITY_CONTRAST_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_contrast_queue",
    "curve_lane_follow_contrast_queue",
    "junction_traverse_contrast_queue",
    "traffic_light_contrast_queue",
)

CAPABILITY_REVIEW_PACK_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_review_pack",
    "curve_lane_follow_review_pack",
    "junction_traverse_review_pack",
    "traffic_light_review_pack",
)

CAPABILITY_REVIEW_PRIORITY_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_review_priority_queue",
    "curve_lane_follow_review_priority_queue",
    "junction_traverse_review_priority_queue",
    "traffic_light_review_priority_queue",
)

CAPABILITY_FOCUS_PACK_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_focus_pack",
    "curve_lane_follow_focus_pack",
    "junction_traverse_focus_pack",
    "traffic_light_focus_pack",
)

CAPABILITY_PROXY_PACK_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_proxy_pack",
    "curve_lane_follow_proxy_pack",
    "junction_traverse_proxy_pack",
    "traffic_light_proxy_pack",
)

CAPABILITY_SEED_PACK_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_seed_pack",
    "curve_lane_follow_seed_pack",
    "junction_traverse_seed_pack",
    "traffic_light_seed_pack",
)

CAPABILITY_HISTORY_GAP_SUBSET_NAMES: Tuple[str, ...] = (
    "lane_keep_history_gap_queue",
    "curve_lane_follow_history_gap_queue",
    "junction_traverse_history_gap_queue",
    "traffic_light_history_gap_queue",
)

CAPABILITY_PAIR_GAP_SUBSET_NAMES: Tuple[str, ...] = (
    "curve_lane_follow_pair_gap_queue",
    "junction_traverse_pair_gap_queue",
    "traffic_light_pair_gap_queue",
)

PAIR_AWARE_CAPABILITY_SUBSET_NAMES = {
    "curve_lane_follow_candidate",
    "junction_traverse_candidate",
    "traffic_light_candidate",
}

CAPABILITY_BLOCKED_TAGS = {
    "persistent_path_fallback_risk",
    "recoverable_fallback_pressure",
    "relapse_heavy_after_recovery",
    "repeat_instability_risk",
    "current_time_smaller_precursor_risk",
    "reference_line_provider_bridge_risk",
    "reroute_propagation_frame_bridge_risk",
    "deprioritized_lateral_smoke",
}

_CANONICAL_COMPARISON_SET_CACHE: Optional[Tuple[Dict[str, Any], ...]] = None
_RANDOM_REGRESSION_POOL_CACHE: Optional[Tuple[Dict[str, Any], ...]] = None
ROUTE_HEALTH_PROMOTION_LABELS = {"route_health_candidate", "route_health_pass"}
CURVE_LANE_FOLLOW_MIN_POST_TRANSITION_LENGTH_M = 25.0


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def safe_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return None


def _is_stage6_experimental_mode_row(row: Dict[str, Any]) -> bool:
    return any(
        safe_bool(row.get(key)) is True
        for key in (
            "stage6_clear_lane_follow_cache_on_new_command",
            "effective_stage6_clear_lane_follow_cache_on_new_command",
            "stage6_reference_line_generation_guard",
            "effective_stage6_reference_line_generation_guard",
        )
    )


def extract_lon_diff(value: Any) -> Optional[float]:
    text = str(value or "")
    if not text:
        return None
    match = re.search(r"lon_diff\s*=\s*([-+]?\d+(?:\.\d+)?)", text)
    if not match:
        return None
    return safe_float(match.group(1))


def classify_replan_reason(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if "current time smaller than the previous trajectory's first time" in text:
        return "current_time_smaller"
    if "distance between matched point and actual position is too large" in text:
        return "matched_point_too_large"
    if "algorithm failure" in text:
        return "algorithm_failure"
    return "other"


def is_empty_previous_trajectory_replan(value: Any) -> bool:
    return "replan for empty previous trajectory" in str(value or "").strip().lower()


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    except Exception:
        return []
    return rows


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def canonical_flags(raw_flags: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    merged = dict(DEFAULT_FLAGS)
    for key, value in (raw_flags or {}).items():
        if key in merged:
            merged[key] = bool(value)
    if merged["stage6_clear_lane_follow_cache_on_new_command"] or merged["stage6_reference_line_generation_guard"]:
        merged["enable_stage6_reference_line"] = True
    return merged


def flags_for_alias(alias: str) -> Dict[str, Any]:
    preset = ALIAS_PRESETS.get(str(alias).strip())
    if preset is None:
        raise KeyError(alias)
    return canonical_flags(preset)


def default_corpus_path(repo_root: Path) -> Path:
    return (repo_root / "carla_testbed" / "assets" / "routes" / "town01" / "town01_route_corpus.json").resolve()


def default_corpus_report_path(repo_root: Path) -> Path:
    return (
        repo_root / "carla_testbed" / "assets" / "routes" / "town01" / "town01_route_corpus_report.md"
    ).resolve()


def default_canonical_comparison_set_path(repo_root: Path) -> Path:
    return (
        repo_root / "carla_testbed" / "assets" / "routes" / "town01" / "town01_canonical_comparison_set.json"
    ).resolve()


def default_random_regression_pool_path(repo_root: Path) -> Path:
    return (
        repo_root
        / "carla_testbed"
        / "assets"
        / "routes"
        / "town01"
        / "town01_random_regression_pool_20260416.json"
    ).resolve()


def default_mainline_config_path(repo_root: Path) -> Path:
    return (repo_root / "configs" / "io" / "examples" / "town01_apollo_route_health.yaml").resolve()


def canonical_comparison_set_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "assets"
        / "routes"
        / "town01"
        / "town01_canonical_comparison_set.json"
    ).resolve()


def random_regression_pool_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "assets"
        / "routes"
        / "town01"
        / "town01_random_regression_pool_20260416.json"
    ).resolve()


def _load_canonical_comparison_set() -> Tuple[Dict[str, Any], ...]:
    path = canonical_comparison_set_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"failed to load Town01 canonical comparison set: {path}") from exc
    routes = payload.get("routes")
    if not isinstance(routes, list) or not routes:
        raise RuntimeError(f"invalid Town01 canonical comparison set payload: {path}")
    required_keys = {
        "comparison_key",
        "display_name",
        "capability_profile",
        "candidate_subset",
        "route_id",
        "role",
    }
    normalized: List[Dict[str, Any]] = []
    for raw in routes:
        if not isinstance(raw, dict):
            raise RuntimeError(f"invalid Town01 canonical comparison row in {path}")
        row = dict(raw)
        missing = sorted(required_keys - set(row))
        if missing:
            raise RuntimeError(
                f"Town01 canonical comparison row missing keys {missing}: {row!r}"
            )
        normalized.append(row)
    return tuple(normalized)


def _load_random_regression_pool() -> Tuple[Dict[str, Any], ...]:
    path = random_regression_pool_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"failed to load Town01 random regression pool: {path}") from exc
    routes = payload.get("routes")
    if not isinstance(routes, list) or not routes:
        raise RuntimeError(f"invalid Town01 random regression pool payload: {path}")
    required_keys = {
        "comparison_key",
        "display_name",
        "capability_profile",
        "candidate_subset",
        "route_id",
        "role",
    }
    normalized: List[Dict[str, Any]] = []
    for raw in routes:
        if not isinstance(raw, dict):
            raise RuntimeError(f"invalid Town01 random regression row in {path}")
        row = dict(raw)
        missing = sorted(required_keys - set(row))
        if missing:
            raise RuntimeError(
                f"Town01 random regression row missing keys {missing}: {row!r}"
            )
        normalized.append(row)
    return tuple(normalized)


def canonical_comparison_set() -> Tuple[Dict[str, Any], ...]:
    global _CANONICAL_COMPARISON_SET_CACHE
    if _CANONICAL_COMPARISON_SET_CACHE is None:
        _CANONICAL_COMPARISON_SET_CACHE = _load_canonical_comparison_set()
    return tuple(dict(item) for item in _CANONICAL_COMPARISON_SET_CACHE)


def random_regression_pool() -> Tuple[Dict[str, Any], ...]:
    global _RANDOM_REGRESSION_POOL_CACHE
    if _RANDOM_REGRESSION_POOL_CACHE is None:
        _RANDOM_REGRESSION_POOL_CACHE = _load_random_regression_pool()
    return tuple(dict(item) for item in _RANDOM_REGRESSION_POOL_CACHE)


def canonical_comparison_route_ids() -> Tuple[str, ...]:
    return tuple(str(item.get("route_id") or "").strip() for item in canonical_comparison_set())


def random_regression_route_ids() -> Tuple[str, ...]:
    return tuple(str(item.get("route_id") or "").strip() for item in random_regression_pool())


def canonical_comparison_entry(route_id: str) -> Dict[str, Any]:
    route_id_text = str(route_id or "").strip()
    for item in canonical_comparison_set():
        if item.get("route_id") == route_id_text:
            return dict(item)
    return {}


def random_regression_entry(route_id: str) -> Dict[str, Any]:
    route_id_text = str(route_id or "").strip()
    for item in random_regression_pool():
        if item.get("route_id") == route_id_text:
            return dict(item)
    return {}


def canonical_demo_steps(*, include_curve_diagnostic: bool = False) -> Tuple[str, ...]:
    allowed_groups = {"core"}
    if include_curve_diagnostic:
        allowed_groups.add("curve_diagnostic")
    steps: List[str] = []
    for item in canonical_comparison_set():
        if str(item.get("showcase_group") or "") not in allowed_groups:
            continue
        if not bool(item.get("showcase_enabled", False)) and not include_curve_diagnostic:
            continue
        capability_profile = str(item.get("capability_profile") or "").strip()
        route_id = str(item.get("route_id") or "").strip()
        if capability_profile and route_id:
            steps.append(f"{capability_profile}:{route_id}")
    return tuple(steps)


def random_regression_steps() -> Tuple[str, ...]:
    steps: List[str] = []
    for item in random_regression_pool():
        capability_profile = str(item.get("capability_profile") or "").strip()
        route_id = str(item.get("route_id") or "").strip()
        if capability_profile and route_id:
            steps.append(f"{capability_profile}:{route_id}")
    return tuple(steps)


def capability_layer_summary(
    *,
    route_health_label: Any,
    runtime_contract_status: Any,
    control_handoff_status: Any,
) -> Dict[str, str]:
    label = str(route_health_label or "").strip()
    runtime_status = str(runtime_contract_status or "").strip()
    handoff_status = str(control_handoff_status or "").strip()
    aligned = runtime_status == "aligned"
    control_consuming = handoff_status == "control_consuming_with_nonzero_planning"

    if aligned and control_consuming:
        engineering_gate = "pass"
    elif aligned:
        engineering_gate = "aligned_runtime_only"
    elif runtime_status:
        engineering_gate = "not_aligned"
    else:
        engineering_gate = "unknown"

    if label in ROUTE_HEALTH_PROMOTION_LABELS and aligned:
        capability_evidence = "positive"
    elif label == "route_established_but_behavior_unhealthy" and aligned:
        capability_evidence = "behavior_unhealthy"
    elif label == "route_established_but_no_control_progress":
        capability_evidence = "no_control_progress"
    elif label == "route_not_established":
        capability_evidence = "route_not_established"
    elif label:
        capability_evidence = "insufficient"
    else:
        capability_evidence = "missing"

    if label in ROUTE_HEALTH_PROMOTION_LABELS and aligned:
        promotion_candidate = "ready"
    elif label in ROUTE_HEALTH_PROMOTION_LABELS:
        promotion_candidate = "blocked_by_runtime"
    elif label:
        promotion_candidate = "not_ready"
    else:
        promotion_candidate = "missing"

    return {
        "engineering_gate": engineering_gate,
        "capability_evidence": capability_evidence,
        "promotion_candidate": promotion_candidate,
    }


def load_effective_cfg(run_dir: Path) -> Dict[str, Any]:
    eff_path = run_dir / "effective.yaml"
    if not eff_path.exists():
        return {}
    try:
        payload = yaml.safe_load(eff_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _nested_get(payload: Dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _infer_runtime_flags_from_effective_cfg(effective_cfg: Dict[str, Any]) -> Dict[str, bool]:
    inferred: Dict[str, bool] = {}

    force_zero_steer_output = safe_bool(
        _nested_get(effective_cfg, "algo", "apollo", "control_mapping", "force_zero_steer_output")
    )
    acc_only_mode = safe_bool(_nested_get(effective_cfg, "algo", "apollo", "planning", "acc_only_mode"))
    longitudinal_only_pipeline = safe_bool(
        _nested_get(effective_cfg, "algo", "apollo", "planning", "longitudinal_only_pipeline")
    )
    longitudinal_only_keep_lane_follow_path = safe_bool(
        _nested_get(
            effective_cfg,
            "algo",
            "apollo",
            "planning",
            "longitudinal_only_keep_lane_follow_path",
        )
    )
    if (
        force_zero_steer_output is False
        and acc_only_mode is False
        and longitudinal_only_pipeline is False
        and longitudinal_only_keep_lane_follow_path is False
    ):
        inferred["enable_lateral"] = True
    elif (
        force_zero_steer_output is True
        and acc_only_mode is True
        and longitudinal_only_pipeline is True
        and longitudinal_only_keep_lane_follow_path is True
    ):
        inferred["enable_lateral"] = False

    guard_states = [
        safe_bool(_nested_get(effective_cfg, "algo", "apollo", "control_mapping", "low_speed_steer_guard_enabled")),
        safe_bool(
            _nested_get(
                effective_cfg,
                "algo",
                "apollo",
                "control_mapping",
                "low_speed_sustained_saturation_guard_enabled",
            )
        ),
        safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "control_mapping", "sustained_lateral_guard_enabled")
        ),
        safe_bool(
            _nested_get(
                effective_cfg,
                "algo",
                "apollo",
                "control_mapping",
                "trajectory_contract_lateral_guard_enabled",
            )
        ),
    ]
    if all(state is not None for state in guard_states) and len({bool(state) for state in guard_states}) == 1:
        inferred["enable_guard"] = bool(guard_states[0])

    recording_enabled = safe_bool(_nested_get(effective_cfg, "algo", "apollo", "dreamview", "record", "enabled"))
    if recording_enabled is not None:
        inferred["enable_recording"] = recording_enabled

    strict_observation = safe_bool(
        _nested_get(effective_cfg, "algo", "apollo", "map_contract", "fail_fast_on_high_risk_mismatch")
    )
    if strict_observation is not None:
        inferred["enable_strict_observation"] = strict_observation

    stage6_enabled = safe_bool(_nested_get(effective_cfg, "algo", "apollo", "stage6_reference_line", "enabled"))
    if stage6_enabled is not None:
        inferred["enable_stage6_reference_line"] = stage6_enabled

    stage6_clear = safe_bool(
        _nested_get(
            effective_cfg,
            "algo",
            "apollo",
            "stage6_reference_line",
            "clear_lane_follow_cache_on_new_command",
        )
    )
    if stage6_clear is not None:
        inferred["stage6_clear_lane_follow_cache_on_new_command"] = stage6_clear

    stage6_guard = safe_bool(
        _nested_get(
            effective_cfg,
            "algo",
            "apollo",
            "stage6_reference_line",
            "reference_line_generation_guard",
        )
    )
    if stage6_guard is not None:
        inferred["stage6_reference_line_generation_guard"] = stage6_guard

    return inferred


def _resolved_runtime_flags(
    raw_flags: Optional[Dict[str, Any]],
    effective_cfg: Dict[str, Any],
    scenario_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    resolved = dict(DEFAULT_FLAGS)

    meta = scenario_meta if isinstance(scenario_meta, dict) else {}
    for key in DEFAULT_FLAGS:
        meta_value = safe_bool(meta.get(key))
        if meta_value is not None:
            resolved[key] = meta_value

    for key, value in _infer_runtime_flags_from_effective_cfg(effective_cfg).items():
        resolved[key] = bool(value)

    if isinstance(raw_flags, dict):
        for key in DEFAULT_FLAGS:
            if key not in raw_flags:
                continue
            explicit_value = safe_bool(raw_flags.get(key))
            if explicit_value is None:
                explicit_value = bool(raw_flags.get(key))
            resolved[key] = explicit_value

    if resolved["stage6_clear_lane_follow_cache_on_new_command"] or resolved["stage6_reference_line_generation_guard"]:
        resolved["enable_stage6_reference_line"] = True
    return resolved


def _runtime_mode_snapshot(flags: Dict[str, Any], effective_cfg: Dict[str, Any]) -> Dict[str, Any]:
    localization_back_offset_m = safe_float(
        _nested_get(effective_cfg, "algo", "apollo", "bridge", "localization_back_offset_m")
    )
    localization_reference_mode = "unknown"
    localization_offset_error_m = None
    if localization_back_offset_m is not None:
        localization_offset_error_m = float(
            localization_back_offset_m - REAR_AXLE_LOCALIZATION_BACK_OFFSET_M
        )
        if abs(float(localization_back_offset_m) - REAR_AXLE_LOCALIZATION_BACK_OFFSET_M) <= (
            REAR_AXLE_LOCALIZATION_BACK_OFFSET_TOLERANCE_M
        ):
            localization_reference_mode = "rear_axle"
        elif abs(float(localization_back_offset_m)) <= REAR_AXLE_LOCALIZATION_BACK_OFFSET_TOLERANCE_M:
            localization_reference_mode = "vehicle_origin"
        else:
            localization_reference_mode = "custom_offset"
    return {
        "enable_lateral": bool(flags.get("enable_lateral")),
        "enable_guard": bool(flags.get("enable_guard")),
        "enable_lateral_debug": bool(flags.get("enable_lateral_debug")),
        "enable_recording": bool(flags.get("enable_recording")),
        "enable_strict_observation": bool(flags.get("enable_strict_observation")),
        "enable_stage6_reference_line": bool(flags.get("enable_stage6_reference_line")),
        "stage6_clear_lane_follow_cache_on_new_command": bool(
            flags.get("stage6_clear_lane_follow_cache_on_new_command")
        ),
        "stage6_reference_line_generation_guard": bool(flags.get("stage6_reference_line_generation_guard")),
        "force_zero_steer_output": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "control_mapping", "force_zero_steer_output")
        ),
        "straight_lane_zero_steer_enabled": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "control_mapping", "straight_lane_zero_steer_enabled")
        ),
        "low_speed_steer_guard_enabled": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "control_mapping", "low_speed_steer_guard_enabled")
        ),
        "low_speed_sustained_saturation_guard_enabled": safe_bool(
            _nested_get(
                effective_cfg,
                "algo",
                "apollo",
                "control_mapping",
                "low_speed_sustained_saturation_guard_enabled",
            )
        ),
        "sustained_lateral_guard_enabled": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "control_mapping", "sustained_lateral_guard_enabled")
        ),
        "trajectory_contract_lateral_guard_enabled": safe_bool(
            _nested_get(
                effective_cfg,
                "algo",
                "apollo",
                "control_mapping",
                "trajectory_contract_lateral_guard_enabled",
            )
        ),
        "run_ticks": safe_int(_nested_get(effective_cfg, "run", "ticks")),
        "profile_name": _nested_get(effective_cfg, "run", "profile_name"),
        "comparison_label": _nested_get(effective_cfg, "run", "comparison_label"),
        "effective_stage6_reference_line_enabled": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "stage6_reference_line", "enabled")
        ),
        "effective_stage6_clear_lane_follow_cache_on_new_command": safe_bool(
            _nested_get(
                effective_cfg,
                "algo",
                "apollo",
                "stage6_reference_line",
                "clear_lane_follow_cache_on_new_command",
            )
        ),
        "effective_stage6_reference_line_generation_guard": safe_bool(
            _nested_get(
                effective_cfg,
                "algo",
                "apollo",
                "stage6_reference_line",
                "reference_line_generation_guard",
            )
        ),
        "capability_profile": _nested_get(effective_cfg, "run", "capability_profile"),
        "effective_acc_only_mode": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "planning", "acc_only_mode")
        ),
        "effective_longitudinal_only_pipeline": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "planning", "longitudinal_only_pipeline")
        ),
        "effective_longitudinal_only_keep_lane_follow_path": safe_bool(
            _nested_get(
                effective_cfg,
                "algo",
                "apollo",
                "planning",
                "longitudinal_only_keep_lane_follow_path",
            )
        ),
        "effective_lane_follow_only_scenario": safe_bool(
            _nested_get(effective_cfg, "algo", "apollo", "planning", "lane_follow_only_scenario")
        ),
        "effective_traffic_light_policy": _nested_get(
            effective_cfg,
            "algo",
            "apollo",
            "traffic_light",
            "policy",
        ),
        "effective_localization_back_offset_m": localization_back_offset_m,
        "localization_reference_mode": localization_reference_mode,
        "localization_reference_expected_back_offset_m": REAR_AXLE_LOCALIZATION_BACK_OFFSET_M,
        "localization_reference_offset_error_m": localization_offset_error_m,
        "apollo_control_state_reference": APOLLO_CONTROL_STATE_REFERENCE,
    }


def evaluate_runtime_contract(capability_profile: str, runtime_mode: Dict[str, Any]) -> Dict[str, Any]:
    capability = str(capability_profile or runtime_mode.get("capability_profile") or "").strip()
    requires_true_lateral = capability in TRUE_LATERAL_CAPABILITY_PROFILES
    requested_true_lateral = requires_true_lateral or safe_bool(runtime_mode.get("enable_lateral")) is True
    blockers: List[str] = []

    if requested_true_lateral:
        if safe_bool(runtime_mode.get("enable_lateral")) is not True:
            blockers.append("enable_lateral=false")
        if safe_bool(runtime_mode.get("force_zero_steer_output")) is True:
            blockers.append("force_zero_steer_output=true")
        if safe_bool(runtime_mode.get("effective_acc_only_mode")) is True:
            blockers.append("acc_only_mode=true")
        if safe_bool(runtime_mode.get("effective_longitudinal_only_pipeline")) is True:
            blockers.append("longitudinal_only_pipeline=true")
        if safe_bool(runtime_mode.get("effective_longitudinal_only_keep_lane_follow_path")) is True:
            blockers.append("longitudinal_only_keep_lane_follow_path=true")
        localization_reference_mode = str(runtime_mode.get("localization_reference_mode") or "").strip()
        if localization_reference_mode != "rear_axle":
            blockers.append(f"localization_reference_mode={localization_reference_mode or 'unknown'}")

    status = "not_required"
    if requested_true_lateral:
        status = "aligned" if not blockers else "misconfigured"

    return {
        "capability_profile": capability,
        "requires_true_lateral": requires_true_lateral,
        "requested_true_lateral": requested_true_lateral,
        "status": status,
        "blockers": blockers,
    }


def _route_health_tags(route: Dict[str, Any]) -> set[str]:
    return {
        str(tag).strip()
        for tag in list(route.get("health_tags") or [])
        if str(tag).strip()
    }


def _route_spawn_idx(route: Dict[str, Any]) -> Optional[int]:
    spawn_idx = safe_int(route.get("spawn_idx"))
    if spawn_idx is not None:
        return int(spawn_idx)
    route_id = str(route.get("route_id") or "").strip()
    match = re.search(r"spawn(\d+)_goal", route_id)
    if not match:
        return None
    return safe_int(match.group(1))


def _route_is_route_health_pass(route: Dict[str, Any]) -> bool:
    tags = _route_health_tags(route)
    if "route_health_pass" in tags:
        return True
    return str(route.get("route_health_label") or "").strip() == "route_health_pass"


def _route_allowed_for_subset(route: Dict[str, Any], subset_name: str) -> bool:
    tags = _route_health_tags(route)
    normalized_subset = str(subset_name or "").strip()
    if normalized_subset in {
        "lateral_smoke_candidate",
        "guarded_lateral_first_wave_smoke",
        "guarded_lateral_repeat_verified_smoke",
        "lane_keep_first_wave_smoke",
        "curve_lane_follow_first_wave_smoke",
        "junction_traverse_first_wave_smoke",
        "traffic_light_first_wave_smoke",
        "lane_keep_next_review_queue",
        "curve_lane_follow_next_review_queue",
        "junction_traverse_next_review_queue",
        "traffic_light_next_review_queue",
        "lane_keep_contrast_queue",
        "curve_lane_follow_contrast_queue",
        "junction_traverse_contrast_queue",
        "traffic_light_contrast_queue",
        "lane_keep_review_pack",
        "curve_lane_follow_review_pack",
        "junction_traverse_review_pack",
        "traffic_light_review_pack",
        "lane_keep_review_priority_queue",
        "curve_lane_follow_review_priority_queue",
        "junction_traverse_review_priority_queue",
        "traffic_light_review_priority_queue",
        "lane_keep_focus_pack",
        "curve_lane_follow_focus_pack",
        "junction_traverse_focus_pack",
        "traffic_light_focus_pack",
        "lane_keep_proxy_pack",
        "curve_lane_follow_proxy_pack",
        "junction_traverse_proxy_pack",
        "traffic_light_proxy_pack",
        "lane_keep_seed_pack",
        "curve_lane_follow_seed_pack",
        "junction_traverse_seed_pack",
        "traffic_light_seed_pack",
        "lane_keep_history_gap_queue",
        "curve_lane_follow_history_gap_queue",
        "junction_traverse_history_gap_queue",
        "traffic_light_history_gap_queue",
        "curve_lane_follow_pair_gap_queue",
        "junction_traverse_pair_gap_queue",
        "traffic_light_pair_gap_queue",
    }:
        if tags.intersection(CAPABILITY_BLOCKED_TAGS):
            return False
    return True


def _is_lateral_smoke_candidate(route: Dict[str, Any]) -> bool:
    route_length = safe_float(route.get("route_length_m"))
    road_transition_count = safe_int(route.get("road_transition_count"))
    lane_transition_count = safe_int(route.get("lane_transition_count"))
    return bool(
        _route_allowed_for_subset(route, "lateral_smoke_candidate")
        and route_length is not None
        and road_transition_count == 0
        and lane_transition_count == 0
        and 220.0 <= float(route_length) <= 250.0
    )


def _should_autobackfill_lateral_smoke_candidate(route: Dict[str, Any]) -> bool:
    tags = _route_health_tags(route)
    return bool("empirically_reviewed" not in tags and _is_lateral_smoke_candidate(route))


def _is_guarded_lateral_first_wave_smoke(route: Dict[str, Any]) -> bool:
    tags = _route_health_tags(route)
    return bool(
        _route_allowed_for_subset(route, "guarded_lateral_first_wave_smoke")
        and "empirically_reviewed" in tags
        and "guarded_lateral_runtime_ok" in tags
    )


def _is_guarded_lateral_repeat_verified_smoke(route: Dict[str, Any]) -> bool:
    tags = _route_health_tags(route)
    return bool(
        _route_allowed_for_subset(route, "guarded_lateral_repeat_verified_smoke")
        and "empirically_reviewed" in tags
        and "guarded_lateral_runtime_ok" in tags
        and "guarded_lateral_repeat_verified" in tags
    )


def _route_heading_delta_deg(route: Dict[str, Any]) -> Optional[float]:
    spawn_pose = route.get("spawn_pose") or {}
    goal_pose = route.get("goal_pose") or {}
    spawn_yaw = safe_float(spawn_pose.get("yaw_deg"))
    goal_yaw = safe_float(goal_pose.get("yaw_deg"))
    if spawn_yaw is None or goal_yaw is None:
        return None
    return ((float(goal_yaw) - float(spawn_yaw) + 180.0) % 360.0) - 180.0


def _route_abs_heading_delta_deg(route: Dict[str, Any]) -> Optional[float]:
    delta = _route_heading_delta_deg(route)
    return abs(float(delta)) if delta is not None else None


def _capability_direction_class(route: Dict[str, Any], subset_name: str) -> str:
    signed_delta = _route_heading_delta_deg(route)
    normalized_subset = str(subset_name or "").strip()
    if normalized_subset == "lane_keep_candidate":
        return "straight"
    if signed_delta is None:
        return "unknown"
    if abs(float(signed_delta)) < 5.0:
        return "straight"
    if normalized_subset == "curve_lane_follow_candidate":
        return "left_bend" if float(signed_delta) > 0.0 else "right_bend"
    if normalized_subset == "junction_traverse_candidate":
        return "left_turn" if float(signed_delta) > 0.0 else "right_turn"
    if normalized_subset == "traffic_light_candidate":
        return "signalized_left_turn" if float(signed_delta) > 0.0 else "signalized_right_turn"
    return "positive_delta" if float(signed_delta) > 0.0 else "negative_delta"


def _capability_geometry_class(route: Dict[str, Any], subset_name: str) -> str:
    route_length = safe_float(route.get("route_length_m")) or 0.0
    road_transition_count = safe_int(route.get("road_transition_count")) or 0
    abs_heading_delta = _route_abs_heading_delta_deg(route) or 0.0
    subset_name = str(subset_name or "").strip()
    if subset_name == "lane_keep_candidate":
        if route_length < 220.0:
            return "short_straight"
        if route_length <= 240.0:
            return "nominal_straight"
        return "long_straight"
    if subset_name == "curve_lane_follow_candidate":
        if abs_heading_delta < 35.0:
            return "gentle_bend"
        if abs_heading_delta < 55.0:
            return "standard_bend"
        return "sharp_bend"
    if subset_name == "junction_traverse_candidate":
        if road_transition_count >= 4:
            return "cross_junction_turn"
        if road_transition_count == 3:
            return "multi_stage_turn"
        return "simple_turn"
    if subset_name == "traffic_light_candidate":
        if road_transition_count >= 4 and route_length <= 250.0:
            return "signalized_cross_compact"
        if road_transition_count >= 4:
            return "signalized_cross_long"
        return "signalized_turn"
    return "generic"


def _capability_supports_pairs(candidate_subset_name: str) -> bool:
    return str(candidate_subset_name or "").strip() in PAIR_AWARE_CAPABILITY_SUBSET_NAMES


def _capability_pair_signature(geometry_class: str, directions: Sequence[str]) -> str:
    ordered_directions = sorted(
        str(item).strip()
        for item in list(directions or [])
        if str(item).strip()
    )
    if not str(geometry_class or "").strip() or len(ordered_directions) < 2:
        return ""
    return f"{str(geometry_class).strip()}:{' vs '.join(ordered_directions)}"


def _capability_pair_coverage(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[str]:
    if not _capability_supports_pairs(candidate_subset_name):
        return []
    grouped: Dict[str, set[str]] = {}
    for route in list(routes or []):
        if not isinstance(route, dict):
            continue
        geometry_class = _capability_geometry_class(route, candidate_subset_name)
        direction_class = _capability_direction_class(route, candidate_subset_name)
        if not str(geometry_class or "").strip() or not str(direction_class or "").strip():
            continue
        grouped.setdefault(str(geometry_class).strip(), set()).add(str(direction_class).strip())
    signatures = [
        _capability_pair_signature(geometry_class, sorted(directions))
        for geometry_class, directions in grouped.items()
        if len(directions) >= 2
    ]
    return sorted(item for item in signatures if item)


def _sync_route_geometry_metrics(route: Dict[str, Any]) -> Dict[str, Any]:
    updated = dict(route)
    signed_delta = _route_heading_delta_deg(updated)
    abs_delta = abs(float(signed_delta)) if signed_delta is not None else None
    updated["goal_heading_delta_deg"] = signed_delta
    updated["goal_abs_heading_delta_deg"] = abs_delta
    updated["lane_keep_geometry_class"] = _capability_geometry_class(updated, "lane_keep_candidate")
    updated["curve_lane_follow_geometry_class"] = _capability_geometry_class(updated, "curve_lane_follow_candidate")
    updated["junction_traverse_geometry_class"] = _capability_geometry_class(updated, "junction_traverse_candidate")
    updated["traffic_light_geometry_class"] = _capability_geometry_class(updated, "traffic_light_candidate")
    updated["lane_keep_direction_class"] = _capability_direction_class(updated, "lane_keep_candidate")
    updated["curve_lane_follow_direction_class"] = _capability_direction_class(updated, "curve_lane_follow_candidate")
    updated["junction_traverse_direction_class"] = _capability_direction_class(updated, "junction_traverse_candidate")
    updated["traffic_light_direction_class"] = _capability_direction_class(updated, "traffic_light_candidate")
    return updated


def _is_lane_keep_candidate(route: Dict[str, Any]) -> bool:
    route_length = safe_float(route.get("route_length_m"))
    road_transition_count = safe_int(route.get("road_transition_count"))
    lane_transition_count = safe_int(route.get("lane_transition_count"))
    return bool(
        route_length is not None
        and road_transition_count == 0
        and lane_transition_count == 0
        and 180.0 <= float(route_length) <= 260.0
    )


def _curve_lane_follow_post_transition_length_m(route: Dict[str, Any]) -> Optional[float]:
    values = [
        safe_float(route.get("post_last_lane_transition_length_m")),
        safe_float(route.get("post_last_road_transition_length_m")),
        safe_float(route.get("post_first_lane_transition_length_m")),
        safe_float(route.get("post_first_road_transition_length_m")),
    ]
    available = [float(value) for value in values if value is not None]
    return min(available) if available else None


def _is_terminal_short_turn_curve(route: Dict[str, Any]) -> bool:
    explicit = safe_bool(route.get("terminal_short_turn_like"))
    if explicit is True:
        return True
    post_transition_length = _curve_lane_follow_post_transition_length_m(route)
    return bool(
        post_transition_length is not None
        and post_transition_length < CURVE_LANE_FOLLOW_MIN_POST_TRANSITION_LENGTH_M
    )


def _is_curve_lane_follow_candidate(route: Dict[str, Any]) -> bool:
    route_length = safe_float(route.get("route_length_m"))
    road_transition_count = safe_int(route.get("road_transition_count"))
    lane_transition_count = safe_int(route.get("lane_transition_count"))
    abs_heading_delta = _route_abs_heading_delta_deg(route)
    post_transition_length = _curve_lane_follow_post_transition_length_m(route)
    return bool(
        route_length is not None
        and abs_heading_delta is not None
        and road_transition_count == 1
        and lane_transition_count == 1
        and 220.0 <= float(route_length) <= 320.0
        and 20.0 <= float(abs_heading_delta) < 80.0
        and not _is_terminal_short_turn_curve(route)
        and (
            post_transition_length is None
            or post_transition_length >= CURVE_LANE_FOLLOW_MIN_POST_TRANSITION_LENGTH_M
        )
    )


def _is_junction_traverse_candidate(route: Dict[str, Any]) -> bool:
    route_length = safe_float(route.get("route_length_m"))
    road_transition_count = safe_int(route.get("road_transition_count"))
    lane_transition_count = safe_int(route.get("lane_transition_count"))
    abs_heading_delta = _route_abs_heading_delta_deg(route)
    return bool(
        route_length is not None
        and abs_heading_delta is not None
        and road_transition_count is not None
        and lane_transition_count is not None
        and int(road_transition_count) >= 2
        and int(lane_transition_count) >= 1
        and 220.0 <= float(route_length) <= 320.0
        and float(abs_heading_delta) >= 45.0
    )


def _is_traffic_light_candidate(route: Dict[str, Any]) -> bool:
    route_length = safe_float(route.get("route_length_m"))
    road_transition_count = safe_int(route.get("road_transition_count"))
    abs_heading_delta = _route_abs_heading_delta_deg(route)
    return bool(
        _is_junction_traverse_candidate(route)
        and route_length is not None
        and road_transition_count is not None
        and 220.0 <= float(route_length) <= 280.0
        and 2 <= int(road_transition_count) <= 4
        and abs_heading_delta is not None
        and 70.0 <= float(abs_heading_delta) <= 110.0
    )


def _capability_candidate_score(route: Dict[str, Any], subset_name: str) -> Tuple[float, float, str]:
    route_length = safe_float(route.get("route_length_m")) or 999.0
    abs_heading_delta = _route_abs_heading_delta_deg(route) or 0.0
    route_id = str(route.get("route_id") or "")
    if subset_name == "lane_keep_candidate":
        return (abs(float(route_length) - 230.0), 0.0, route_id)
    if subset_name == "curve_lane_follow_candidate":
        return (abs(float(abs_heading_delta) - 45.0), abs(float(route_length) - 240.0), route_id)
    if subset_name == "junction_traverse_candidate":
        return (abs(float(abs_heading_delta) - 90.0), abs(float(route_length) - 250.0), route_id)
    if subset_name == "traffic_light_candidate":
        return (abs(float(abs_heading_delta) - 90.0), abs(float(route_length) - 240.0), route_id)
    return (abs(float(route_length) - 230.0), abs(float(abs_heading_delta)), route_id)


def _spawn_health_bucket(routes: Sequence[Dict[str, Any]]) -> Dict[int, int]:
    risky_tags = {
        "persistent_path_fallback_risk",
        "recoverable_fallback_pressure",
        "relapse_heavy_after_recovery",
        "repeat_instability_risk",
        "current_time_smaller_precursor_risk",
        "reference_line_provider_bridge_risk",
        "reroute_propagation_frame_bridge_risk",
        "deprioritized_lateral_smoke",
    }
    state: Dict[int, Dict[str, bool]] = {}
    for route in routes:
        spawn_idx = _route_spawn_idx(route)
        if spawn_idx is None:
            continue
        tags = _route_health_tags(route)
        item = state.setdefault(spawn_idx, {"has_verified": False, "has_risky": False})
        if "guarded_lateral_repeat_verified" in tags or "route_health_pass" in tags:
            item["has_verified"] = True
        if tags.intersection(risky_tags):
            item["has_risky"] = True
    buckets: Dict[int, int] = {}
    for spawn_idx, item in state.items():
        if item.get("has_risky"):
            buckets[spawn_idx] = 2
        elif item.get("has_verified"):
            buckets[spawn_idx] = 0
        else:
            buckets[spawn_idx] = 1
    return buckets


def _capability_first_wave_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_first_wave_smoke",
        "curve_lane_follow_candidate": "curve_lane_follow_first_wave_smoke",
        "junction_traverse_candidate": "junction_traverse_first_wave_smoke",
        "traffic_light_candidate": "traffic_light_first_wave_smoke",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_next_review_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_next_review_queue",
        "curve_lane_follow_candidate": "curve_lane_follow_next_review_queue",
        "junction_traverse_candidate": "junction_traverse_next_review_queue",
        "traffic_light_candidate": "traffic_light_next_review_queue",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_contrast_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_contrast_queue",
        "curve_lane_follow_candidate": "curve_lane_follow_contrast_queue",
        "junction_traverse_candidate": "junction_traverse_contrast_queue",
        "traffic_light_candidate": "traffic_light_contrast_queue",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_pair_gap_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "curve_lane_follow_candidate": "curve_lane_follow_pair_gap_queue",
        "junction_traverse_candidate": "junction_traverse_pair_gap_queue",
        "traffic_light_candidate": "traffic_light_pair_gap_queue",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_review_pack_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_review_pack",
        "curve_lane_follow_candidate": "curve_lane_follow_review_pack",
        "junction_traverse_candidate": "junction_traverse_review_pack",
        "traffic_light_candidate": "traffic_light_review_pack",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_review_priority_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_review_priority_queue",
        "curve_lane_follow_candidate": "curve_lane_follow_review_priority_queue",
        "junction_traverse_candidate": "junction_traverse_review_priority_queue",
        "traffic_light_candidate": "traffic_light_review_priority_queue",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_focus_pack_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_focus_pack",
        "curve_lane_follow_candidate": "curve_lane_follow_focus_pack",
        "junction_traverse_candidate": "junction_traverse_focus_pack",
        "traffic_light_candidate": "traffic_light_focus_pack",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_history_gap_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_history_gap_queue",
        "curve_lane_follow_candidate": "curve_lane_follow_history_gap_queue",
        "junction_traverse_candidate": "junction_traverse_history_gap_queue",
        "traffic_light_candidate": "traffic_light_history_gap_queue",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_proxy_pack_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_proxy_pack",
        "curve_lane_follow_candidate": "curve_lane_follow_proxy_pack",
        "junction_traverse_candidate": "junction_traverse_proxy_pack",
        "traffic_light_candidate": "traffic_light_proxy_pack",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_seed_pack_subset_name(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_seed_pack",
        "curve_lane_follow_candidate": "curve_lane_follow_seed_pack",
        "junction_traverse_candidate": "junction_traverse_seed_pack",
        "traffic_light_candidate": "traffic_light_seed_pack",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_first_wave_limit(candidate_subset_name: str) -> int:
    return 4


def _capability_candidate_limit(candidate_subset_name: str) -> int:
    return 8


def _capability_next_review_limit(candidate_subset_name: str) -> int:
    return 4


def _capability_contrast_limit(candidate_subset_name: str) -> int:
    return 4


def _capability_pair_gap_limit(candidate_subset_name: str) -> int:
    return 4


def _capability_review_pack_limit(candidate_subset_name: str) -> int:
    if _capability_supports_pairs(candidate_subset_name):
        return 6
    return 4


def _capability_review_priority_limit(candidate_subset_name: str) -> int:
    return _capability_review_pack_limit(candidate_subset_name)


def _capability_focus_pack_limit(candidate_subset_name: str) -> int:
    if _capability_supports_pairs(candidate_subset_name):
        return 2
    return 3


def _capability_history_gap_limit(candidate_subset_name: str) -> int:
    if _capability_supports_pairs(candidate_subset_name):
        return 2
    return 2


def _capability_proxy_pack_limit(candidate_subset_name: str) -> int:
    return 2


def _capability_seed_pack_limit(candidate_subset_name: str) -> int:
    return 1


def _capability_shape_key(route: Dict[str, Any], candidate_subset_name: str) -> str:
    return "{}:{}".format(
        _capability_geometry_class(route, candidate_subset_name),
        _capability_direction_class(route, candidate_subset_name),
    )


def _capability_direction_side(direction_class: str) -> str:
    normalized = str(direction_class or "").strip().lower()
    if "left" in normalized:
        return "left"
    if "right" in normalized:
        return "right"
    if "straight" in normalized:
        return "straight"
    return normalized or "none"


def _capability_review_priority(route: Dict[str, Any]) -> int:
    tags = _route_health_tags(route)
    if tags.intersection(CAPABILITY_BLOCKED_TAGS):
        return 5
    if _route_is_route_health_pass(route):
        return 4
    if "route_health_candidate" in tags or "guarded_lateral_runtime_ok" in tags:
        return 3
    if "empirically_reviewed" in tags:
        return 2
    return 1


def _capability_semantic_history_tag(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep_semantic_history",
        "curve_lane_follow_candidate": "curve_lane_follow_semantic_history",
        "junction_traverse_candidate": "junction_traverse_semantic_history",
        "traffic_light_candidate": "traffic_light_semantic_history",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_semantic_history_prefix(candidate_subset_name: str) -> str:
    mapping = {
        "lane_keep_candidate": "lane_keep",
        "curve_lane_follow_candidate": "curve_lane_follow",
        "junction_traverse_candidate": "junction_traverse",
        "traffic_light_candidate": "traffic_light",
    }
    return mapping.get(str(candidate_subset_name or "").strip(), "")


def _capability_semantic_best_label(route: Dict[str, Any], candidate_subset_name: str) -> str:
    prefix = _capability_semantic_history_prefix(candidate_subset_name)
    if not prefix:
        return ""
    return str(route.get(f"{prefix}_semantic_best_label") or "").strip()


def _capability_semantic_best_completion(route: Dict[str, Any], candidate_subset_name: str) -> float:
    prefix = _capability_semantic_history_prefix(candidate_subset_name)
    if not prefix:
        return 0.0
    return safe_float(route.get(f"{prefix}_semantic_best_completion")) or 0.0


def _capability_semantic_anchor_sort_key(
    route: Dict[str, Any],
    candidate_subset_name: str,
) -> Tuple[int, float, float, float, str]:
    best_label = _capability_semantic_best_label(route, candidate_subset_name)
    if best_label == "route_health_pass":
        label_rank = 0
    elif best_label == "route_health_candidate":
        label_rank = 1
    elif best_label:
        label_rank = 2
    else:
        label_rank = 3
    best_completion = _capability_semantic_best_completion(route, candidate_subset_name)
    candidate_primary, candidate_secondary, route_id = _capability_candidate_score(route, candidate_subset_name)
    return (
        int(label_rank),
        -float(best_completion),
        float(candidate_primary),
        float(candidate_secondary),
        str(route_id or ""),
    )


def _capability_proxy_evidence_rank(route: Dict[str, Any]) -> int:
    tags = _route_health_tags(route)
    if "route_health_pass" in tags:
        return 0
    if "route_health_candidate" in tags:
        return 1
    if "empirically_reviewed" in tags:
        return 2
    if any(tag.endswith("_semantic_history") for tag in tags):
        return 3
    if "guarded_lateral_runtime_ok" in tags:
        return 4
    if tags and tags != {"unreviewed"}:
        return 5
    return 6


def _capability_proxy_similarity_rank(
    seed_route: Dict[str, Any],
    proxy_route: Dict[str, Any],
    candidate_subset_name: str,
) -> int:
    seed_geometry = _capability_geometry_class(seed_route, candidate_subset_name)
    seed_direction = _capability_direction_class(seed_route, candidate_subset_name)
    proxy_geometry = _capability_geometry_class(proxy_route, candidate_subset_name)
    proxy_direction = _capability_direction_class(proxy_route, candidate_subset_name)
    if seed_geometry == proxy_geometry and seed_direction == proxy_direction:
        return 0
    if seed_direction == proxy_direction:
        return 1
    if _capability_direction_side(seed_direction) == _capability_direction_side(proxy_direction):
        return 2
    if seed_geometry == proxy_geometry:
        return 3
    return 4


def _capability_semantic_anchor_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
    subset_name: str = "",
) -> List[Dict[str, Any]]:
    history_tag = _capability_semantic_history_tag(candidate_subset_name)
    normalized_subset = str(subset_name or "").strip()
    if not history_tag:
        return []

    ordered: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in (
        list(_capability_focus_pack_candidates(routes, candidate_subset_name))
        + list(_capability_review_priority_candidates(routes, candidate_subset_name))
        + list(_capability_review_pack_candidates(routes, candidate_subset_name))
        + list(_capability_contrast_candidates(routes, candidate_subset_name))
        + list(_capability_next_review_candidates(routes, candidate_subset_name))
        + list(_capability_first_wave_candidates(routes, candidate_subset_name))
        + list(_capability_candidate_candidates(routes, candidate_subset_name))
    ):
        route_id = str(item.get("route_id") or "").strip()
        if not route_id or route_id in seen_ids:
            continue
        if normalized_subset and not _route_allowed_for_subset(item, normalized_subset):
            continue
        if history_tag not in _route_health_tags(item):
            continue
        ordered.append(item)
        seen_ids.add(route_id)
    return sorted(
        ordered,
        key=lambda route: _capability_semantic_anchor_sort_key(route, candidate_subset_name),
    )


def _candidate_routes_for_capability(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    return [
        route
        for route in routes
        if candidate_subset_name in list(route.get("recommended_uses") or [])
        or (
            candidate_subset_name == "lane_keep_candidate"
            and _is_lane_keep_candidate(route)
        )
        or (
            candidate_subset_name == "curve_lane_follow_candidate"
            and _is_curve_lane_follow_candidate(route)
        )
        or (
            candidate_subset_name == "junction_traverse_candidate"
            and _is_junction_traverse_candidate(route)
        )
        or (
            candidate_subset_name == "traffic_light_candidate"
            and _is_traffic_light_candidate(route)
        )
    ]


def _capability_candidate_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    filtered = [
        route
        for route in _candidate_routes_for_capability(routes, candidate_subset_name)
        if _route_allowed_for_subset(route, candidate_subset_name)
    ]
    scored = sorted(filtered, key=lambda route: _capability_candidate_score(route, candidate_subset_name))
    limit = _capability_candidate_limit(candidate_subset_name)
    ordered: List[Dict[str, Any]] = []
    seen_route_ids: set[str] = set()
    seen_shapes: set[str] = set()

    for route in scored:
        route_id = str(route.get("route_id") or "").strip()
        shape_key = _capability_shape_key(route, candidate_subset_name)
        if not route_id or route_id in seen_route_ids or shape_key in seen_shapes:
            continue
        ordered.append(route)
        seen_route_ids.add(route_id)
        seen_shapes.add(shape_key)
        if len(ordered) >= limit:
            return ordered

    for route in scored:
        route_id = str(route.get("route_id") or "").strip()
        if not route_id or route_id in seen_route_ids:
            continue
        ordered.append(route)
        seen_route_ids.add(route_id)
        if len(ordered) >= limit:
            break
    return ordered


def _capability_first_wave_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    spawn_buckets = _spawn_health_bucket(routes)
    candidate_routes = _capability_candidate_candidates(routes, candidate_subset_name)
    first_wave_subset = _capability_first_wave_subset_name(candidate_subset_name)
    filtered = [
        route
        for route in candidate_routes
        if _route_allowed_for_subset(route, first_wave_subset)
    ]
    scored = sorted(
        filtered,
        key=lambda route: (
            int(spawn_buckets.get(_route_spawn_idx(route) or -1, 1)),
            *_capability_candidate_score(route, candidate_subset_name),
        ),
    )
    limit = _capability_first_wave_limit(candidate_subset_name)
    ordered: List[Dict[str, Any]] = []
    seen_route_ids: set[str] = set()
    seen_spawns: set[int] = set()
    seen_classes: set[str] = set()
    seen_directions: set[str] = set()
    seen_shapes: set[str] = set()
    prefer_geometry_diversity = candidate_subset_name in {
        "curve_lane_follow_candidate",
        "junction_traverse_candidate",
        "traffic_light_candidate",
    }
    prefer_direction_diversity = prefer_geometry_diversity
    if prefer_geometry_diversity:
        for route in scored:
            route_id = str(route.get("route_id") or "")
            spawn_idx = _route_spawn_idx(route)
            geometry_class = _capability_geometry_class(route, candidate_subset_name)
            if (
                not route_id
                or route_id in seen_route_ids
                or spawn_idx is None
                or spawn_idx in seen_spawns
                or geometry_class in seen_classes
            ):
                continue
            ordered.append(route)
            seen_route_ids.add(route_id)
            seen_spawns.add(spawn_idx)
            seen_classes.add(geometry_class)
            direction_class = _capability_direction_class(route, candidate_subset_name)
            seen_directions.add(direction_class)
            seen_shapes.add(f"{geometry_class}:{direction_class}")
            if len(ordered) >= limit:
                return ordered
    if prefer_geometry_diversity:
        for require_new_direction in (True, False):
            for route in scored:
                route_id = str(route.get("route_id") or "")
                spawn_idx = _route_spawn_idx(route)
                geometry_class = _capability_geometry_class(route, candidate_subset_name)
                direction_class = _capability_direction_class(route, candidate_subset_name)
                shape_key = f"{geometry_class}:{direction_class}"
                if (
                    not route_id
                    or route_id in seen_route_ids
                    or spawn_idx is None
                    or spawn_idx in seen_spawns
                    or shape_key in seen_shapes
                    or (require_new_direction and direction_class in seen_directions)
                ):
                    continue
                ordered.append(route)
                seen_route_ids.add(route_id)
                seen_spawns.add(spawn_idx)
                seen_classes.add(geometry_class)
                seen_directions.add(direction_class)
                seen_shapes.add(shape_key)
                if len(ordered) >= limit:
                    return ordered
    if prefer_direction_diversity:
        for route in scored:
            route_id = str(route.get("route_id") or "")
            spawn_idx = _route_spawn_idx(route)
            direction_class = _capability_direction_class(route, candidate_subset_name)
            if (
                not route_id
                or route_id in seen_route_ids
                or spawn_idx is None
                or spawn_idx in seen_spawns
                or direction_class in seen_directions
            ):
                continue
            ordered.append(route)
            seen_route_ids.add(route_id)
            seen_spawns.add(spawn_idx)
            geometry_class = _capability_geometry_class(route, candidate_subset_name)
            seen_classes.add(geometry_class)
            seen_directions.add(direction_class)
            seen_shapes.add(f"{geometry_class}:{direction_class}")
            if len(ordered) >= limit:
                return ordered
    for route in scored:
        route_id = str(route.get("route_id") or "")
        spawn_idx = _route_spawn_idx(route)
        if not route_id or route_id in seen_route_ids or spawn_idx is None or spawn_idx in seen_spawns:
            continue
        ordered.append(route)
        seen_route_ids.add(route_id)
        seen_spawns.add(spawn_idx)
        geometry_class = _capability_geometry_class(route, candidate_subset_name)
        direction_class = _capability_direction_class(route, candidate_subset_name)
        seen_classes.add(geometry_class)
        seen_directions.add(direction_class)
        seen_shapes.add(f"{geometry_class}:{direction_class}")
        if len(ordered) >= limit:
            return ordered
    for route in scored:
        route_id = str(route.get("route_id") or "")
        if not route_id or route_id in seen_route_ids:
            continue
        ordered.append(route)
        seen_route_ids.add(route_id)
        if len(ordered) >= limit:
            break
    return ordered


def _capability_next_review_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    next_review_subset = _capability_next_review_subset_name(candidate_subset_name)
    filtered = [
        route
        for route in _capability_candidate_candidates(routes, candidate_subset_name)
        if _route_allowed_for_subset(route, next_review_subset)
    ]
    first_wave_routes = _capability_first_wave_candidates(routes, candidate_subset_name)
    first_wave_ids = {
        str(route.get("route_id") or "").strip()
        for route in first_wave_routes
        if str(route.get("route_id") or "").strip()
    }
    first_wave_shapes = {
        _capability_shape_key(route, candidate_subset_name)
        for route in first_wave_routes
        if str(route.get("route_id") or "").strip()
    }
    scored = sorted(
        filtered,
        key=lambda route: (
            int(_capability_review_priority(route)),
            *_capability_candidate_score(route, candidate_subset_name),
        ),
    )
    limit = _capability_next_review_limit(candidate_subset_name)
    ordered: List[Dict[str, Any]] = []
    seen_route_ids: set[str] = set()
    seen_shapes: set[str] = set()

    for route in scored:
        route_id = str(route.get("route_id") or "").strip()
        shape_key = _capability_shape_key(route, candidate_subset_name)
        if (
            not route_id
            or route_id in seen_route_ids
            or route_id in first_wave_ids
            or shape_key in first_wave_shapes
            or shape_key in seen_shapes
        ):
            continue
        ordered.append(route)
        seen_route_ids.add(route_id)
        seen_shapes.add(shape_key)
        if len(ordered) >= limit:
            return ordered

    for route in scored:
        route_id = str(route.get("route_id") or "").strip()
        if (
            not route_id
            or route_id in seen_route_ids
            or route_id not in first_wave_ids
            or _capability_review_priority(route) >= 4
        ):
            continue
        ordered.append(route)
        seen_route_ids.add(route_id)
        if len(ordered) >= limit:
            return ordered

    for route in scored:
        route_id = str(route.get("route_id") or "").strip()
        if (
            not route_id
            or route_id in seen_route_ids
            or _capability_review_priority(route) >= 4
        ):
            continue
        ordered.append(route)
        seen_route_ids.add(route_id)
        if len(ordered) >= limit:
            return ordered

    return ordered


def _capability_contrast_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    contrast_subset = _capability_contrast_subset_name(candidate_subset_name)
    filtered = [
        route
        for route in _capability_candidate_candidates(routes, candidate_subset_name)
        if _route_allowed_for_subset(route, contrast_subset)
    ]
    first_wave_routes = _capability_first_wave_candidates(routes, candidate_subset_name)
    next_review_routes = _capability_next_review_candidates(routes, candidate_subset_name)
    first_wave_ids = {
        str(route.get("route_id") or "").strip()
        for route in first_wave_routes
        if str(route.get("route_id") or "").strip()
    }
    next_review_ids = {
        str(route.get("route_id") or "").strip()
        for route in next_review_routes
        if str(route.get("route_id") or "").strip()
    }
    first_wave_shapes = {
        _capability_shape_key(route, candidate_subset_name)
        for route in first_wave_routes
        if str(route.get("route_id") or "").strip()
    }

    def _route_rank(route: Dict[str, Any]) -> tuple[int, float, float, str]:
        route_id = str(route.get("route_id") or "").strip()
        shape_key = _capability_shape_key(route, candidate_subset_name)
        if shape_key not in first_wave_shapes and route_id in next_review_ids:
            bucket = 0
        elif route_id in first_wave_ids:
            bucket = 1
        elif route_id in next_review_ids:
            bucket = 2
        else:
            bucket = 3
        score_a, score_b, score_id = _capability_candidate_score(route, candidate_subset_name)
        return (int(bucket), float(score_a), float(score_b), str(score_id))

    if candidate_subset_name == "lane_keep_candidate":
        geometry_order = {
            "nominal_straight": 0,
            "long_straight": 1,
            "short_straight": 2,
        }
        by_geometry: Dict[str, List[Dict[str, Any]]] = {}
        for route in filtered:
            by_geometry.setdefault(_capability_geometry_class(route, candidate_subset_name), []).append(route)
        ordered: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        for geometry_class in sorted(
            by_geometry,
            key=lambda item: (
                int(geometry_order.get(item, 99)),
                min(_route_rank(route) for route in by_geometry.get(item) or [{}]),
            ),
        ):
            best_route = sorted(by_geometry.get(geometry_class) or [], key=_route_rank)[0]
            route_id = str(best_route.get("route_id") or "").strip()
            if not route_id or route_id in seen_ids:
                continue
            ordered.append(best_route)
            seen_ids.add(route_id)
            if len(ordered) >= _capability_contrast_limit(candidate_subset_name):
                return ordered
        for route in sorted(filtered, key=_route_rank):
            route_id = str(route.get("route_id") or "").strip()
            if not route_id or route_id in seen_ids:
                continue
            ordered.append(route)
            seen_ids.add(route_id)
            if len(ordered) >= _capability_contrast_limit(candidate_subset_name):
                break
        return ordered

    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for route in filtered:
        geometry_class = _capability_geometry_class(route, candidate_subset_name)
        direction_class = _capability_direction_class(route, candidate_subset_name)
        grouped.setdefault(geometry_class, {}).setdefault(direction_class, []).append(route)

    def _missing_shape_count(geometry_class: str) -> int:
        count = 0
        for direction_class in grouped.get(geometry_class, {}):
            shape_key = _shape_key(geometry_class, direction_class)
            if shape_key not in first_wave_shapes:
                count += 1
        return count

    def _shape_key(geometry_class: str, direction_class: str) -> str:
        return f"{geometry_class}:{direction_class}"

    geometry_order: List[str] = sorted(
        grouped,
        key=lambda geometry_class: (
            -_missing_shape_count(geometry_class),
            -len(grouped.get(geometry_class, {})),
            min(_route_rank(route) for routes_for_direction in (grouped.get(geometry_class) or {}).values() for route in routes_for_direction),
            geometry_class,
        ),
    )
    ordered: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for geometry_class in geometry_order:
        directions = sorted(grouped.get(geometry_class, {}))
        if len(directions) < 2:
            continue
        for direction_class in directions[:2]:
            best_route = sorted(grouped.get(geometry_class, {}).get(direction_class) or [], key=_route_rank)[0]
            route_id = str(best_route.get("route_id") or "").strip()
            if not route_id or route_id in seen_ids:
                continue
            ordered.append(best_route)
            seen_ids.add(route_id)
            if len(ordered) >= _capability_contrast_limit(candidate_subset_name):
                return ordered
    for route in sorted(filtered, key=_route_rank):
        route_id = str(route.get("route_id") or "").strip()
        if not route_id or route_id in seen_ids:
            continue
        ordered.append(route)
        seen_ids.add(route_id)
        if len(ordered) >= _capability_contrast_limit(candidate_subset_name):
            break
    return ordered


def _capability_pair_gap_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    if not _capability_supports_pairs(candidate_subset_name):
        return []
    pair_gap_subset = _capability_pair_gap_subset_name(candidate_subset_name)
    filtered = [
        route
        for route in _capability_candidate_candidates(routes, candidate_subset_name)
        if _route_allowed_for_subset(route, pair_gap_subset)
    ]
    first_wave_routes = _capability_first_wave_candidates(routes, candidate_subset_name)
    next_review_routes = _capability_next_review_candidates(routes, candidate_subset_name)
    contrast_routes = _capability_contrast_candidates(routes, candidate_subset_name)
    contrast_pairs = set(_capability_pair_coverage(contrast_routes, candidate_subset_name))
    first_wave_ids = {
        str(route.get("route_id") or "").strip()
        for route in first_wave_routes
        if str(route.get("route_id") or "").strip()
    }
    next_review_ids = {
        str(route.get("route_id") or "").strip()
        for route in next_review_routes
        if str(route.get("route_id") or "").strip()
    }
    contrast_ids = {
        str(route.get("route_id") or "").strip()
        for route in contrast_routes
        if str(route.get("route_id") or "").strip()
    }

    def _route_rank(route: Dict[str, Any]) -> tuple[int, float, float, str]:
        route_id = str(route.get("route_id") or "").strip()
        if route_id in next_review_ids:
            bucket = 0
        elif route_id in first_wave_ids:
            bucket = 1
        elif route_id in contrast_ids:
            bucket = 2
        else:
            bucket = 3
        score_a, score_b, score_id = _capability_candidate_score(route, candidate_subset_name)
        return (bucket, float(score_a), float(score_b), str(score_id))

    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for route in filtered:
        geometry_class = _capability_geometry_class(route, candidate_subset_name)
        direction_class = _capability_direction_class(route, candidate_subset_name)
        grouped.setdefault(geometry_class, {}).setdefault(direction_class, []).append(route)

    missing_geometry_classes: List[str] = []
    for geometry_class, by_direction in grouped.items():
        if len(by_direction) < 2:
            continue
        pair_signature = _capability_pair_signature(geometry_class, sorted(by_direction))
        if pair_signature and pair_signature not in contrast_pairs:
            missing_geometry_classes.append(geometry_class)
    missing_geometry_classes = sorted(
        missing_geometry_classes,
        key=lambda geometry_class: (
            min(
                _route_rank(route)
                for routes_for_direction in (grouped.get(geometry_class) or {}).values()
                for route in routes_for_direction
            ),
            geometry_class,
        ),
    )

    ordered: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for geometry_class in missing_geometry_classes:
        for direction_class in sorted(grouped.get(geometry_class) or {}):
            best_route = sorted(grouped.get(geometry_class, {}).get(direction_class) or [], key=_route_rank)[0]
            route_id = str(best_route.get("route_id") or "").strip()
            if not route_id or route_id in seen_ids:
                continue
            ordered.append(best_route)
            seen_ids.add(route_id)
            if len(ordered) >= _capability_pair_gap_limit(candidate_subset_name):
                return ordered
    return ordered


def _capability_review_pack_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    ordered: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    def _extend(items: Sequence[Dict[str, Any]]) -> None:
        for item in items:
            route_id = str(item.get("route_id") or "").strip()
            if not route_id or route_id in seen_ids:
                continue
            ordered.append(item)
            seen_ids.add(route_id)
            if len(ordered) >= _capability_review_pack_limit(candidate_subset_name):
                return

    _extend(_capability_contrast_candidates(routes, candidate_subset_name))
    if len(ordered) >= _capability_review_pack_limit(candidate_subset_name):
        return ordered
    if _capability_supports_pairs(candidate_subset_name):
        _extend(_capability_pair_gap_candidates(routes, candidate_subset_name))
    else:
        _extend(_capability_next_review_candidates(routes, candidate_subset_name))
    return ordered


def _capability_review_priority_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    limit = _capability_review_priority_limit(candidate_subset_name)
    ordered: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    def _ordered_source(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            items,
            key=lambda route: (
                int(_capability_review_priority(route)),
                *_capability_candidate_score(route, candidate_subset_name),
            ),
        )

    def _extend(items: Sequence[Dict[str, Any]]) -> None:
        for item in _ordered_source(items):
            route_id = str(item.get("route_id") or "").strip()
            if not route_id or route_id in seen_ids:
                continue
            ordered.append(item)
            seen_ids.add(route_id)
            if len(ordered) >= limit:
                return

    if _capability_supports_pairs(candidate_subset_name):
        _extend(_capability_pair_gap_candidates(routes, candidate_subset_name))
    _extend(_capability_next_review_candidates(routes, candidate_subset_name))
    _extend(_capability_contrast_candidates(routes, candidate_subset_name))
    _extend(_capability_review_pack_candidates(routes, candidate_subset_name))
    return ordered


def _capability_focus_pack_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    focus_pack_subset = _capability_focus_pack_subset_name(candidate_subset_name)
    if not focus_pack_subset:
        return []

    def _ordered_unique(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ordered: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item in items:
            route_id = str(item.get("route_id") or "").strip()
            if (
                not route_id
                or route_id in seen_ids
                or not _route_allowed_for_subset(item, focus_pack_subset)
            ):
                continue
            ordered.append(item)
            seen_ids.add(route_id)
        return ordered

    ordered_sources = _ordered_unique(
        list(_capability_review_priority_candidates(routes, candidate_subset_name))
        + list(_capability_review_pack_candidates(routes, candidate_subset_name))
        + list(_capability_contrast_candidates(routes, candidate_subset_name))
        + list(_capability_next_review_candidates(routes, candidate_subset_name))
    )
    if not ordered_sources:
        return []

    limit = _capability_focus_pack_limit(candidate_subset_name)
    if _capability_supports_pairs(candidate_subset_name):
        by_geometry: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for route in ordered_sources:
            geometry_class = _capability_geometry_class(route, candidate_subset_name)
            direction_class = _capability_direction_class(route, candidate_subset_name)
            per_geometry = by_geometry.setdefault(geometry_class, {})
            if direction_class in per_geometry:
                continue
            per_geometry[direction_class] = route
            if len(per_geometry) >= 2:
                return [
                    per_geometry[direction_class]
                    for direction_class in sorted(per_geometry)[:limit]
                ]
        return ordered_sources[:limit]

    selected: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    pass_anchor = next(
        (route for route in ordered_sources if _route_is_route_health_pass(route)),
        None,
    )
    target_nonpass = max(1, limit - 1)
    for route in ordered_sources:
        route_id = str(route.get("route_id") or "").strip()
        if not route_id or route_id in seen_ids:
            continue
        if pass_anchor is not None and route_id == str(pass_anchor.get("route_id") or "").strip():
            continue
        selected.append(route)
        seen_ids.add(route_id)
        if len(selected) >= target_nonpass:
            break
    if pass_anchor is not None:
        pass_anchor_id = str(pass_anchor.get("route_id") or "").strip()
        if pass_anchor_id and pass_anchor_id not in seen_ids:
            selected.append(pass_anchor)
            seen_ids.add(pass_anchor_id)
    for route in ordered_sources:
        route_id = str(route.get("route_id") or "").strip()
        if not route_id or route_id in seen_ids:
            continue
        selected.append(route)
        seen_ids.add(route_id)
        if len(selected) >= limit:
            break
    return selected[:limit]


def _capability_history_gap_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    history_gap_subset = _capability_history_gap_subset_name(candidate_subset_name)
    history_tag = _capability_semantic_history_tag(candidate_subset_name)
    if not history_gap_subset or not history_tag:
        return []

    def _ordered_unique(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ordered: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item in items:
            route_id = str(item.get("route_id") or "").strip()
            if (
                not route_id
                or route_id in seen_ids
                or not _route_allowed_for_subset(item, history_gap_subset)
            ):
                continue
            ordered.append(item)
            seen_ids.add(route_id)
        return ordered

    ordered_sources = _ordered_unique(
        list(_capability_focus_pack_candidates(routes, candidate_subset_name))
        + list(_capability_review_priority_candidates(routes, candidate_subset_name))
        + list(_capability_review_pack_candidates(routes, candidate_subset_name))
        + list(_capability_contrast_candidates(routes, candidate_subset_name))
        + list(_capability_next_review_candidates(routes, candidate_subset_name))
    )
    if not ordered_sources:
        return []

    limit = _capability_history_gap_limit(candidate_subset_name)
    selected: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for route in ordered_sources:
        route_id = str(route.get("route_id") or "").strip()
        if not route_id or route_id in seen_ids:
            continue
        if history_tag in _route_health_tags(route):
            continue
        selected.append(route)
        seen_ids.add(route_id)
        if len(selected) >= limit:
            break
    return selected


def _capability_seed_pack_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    seed_pack_subset = _capability_seed_pack_subset_name(candidate_subset_name)
    if not seed_pack_subset:
        return []

    def _ordered_unique(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ordered: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item in items:
            route_id = str(item.get("route_id") or "").strip()
            if (
                not route_id
                or route_id in seen_ids
                or not _route_allowed_for_subset(item, seed_pack_subset)
            ):
                continue
            ordered.append(item)
            seen_ids.add(route_id)
        return ordered

    ordered_sources = _ordered_unique(
        list(_capability_semantic_anchor_candidates(routes, candidate_subset_name, seed_pack_subset))
        + list(_capability_history_gap_candidates(routes, candidate_subset_name))
        + list(_capability_focus_pack_candidates(routes, candidate_subset_name))
        + list(_capability_review_priority_candidates(routes, candidate_subset_name))
        + list(_capability_review_pack_candidates(routes, candidate_subset_name))
        + list(_capability_contrast_candidates(routes, candidate_subset_name))
        + list(_capability_next_review_candidates(routes, candidate_subset_name))
        + list(_capability_first_wave_candidates(routes, candidate_subset_name))
    )
    if not ordered_sources:
        return []
    return ordered_sources[: _capability_seed_pack_limit(candidate_subset_name)]


def _capability_proxy_pack_candidates(
    routes: Sequence[Dict[str, Any]],
    candidate_subset_name: str,
) -> List[Dict[str, Any]]:
    proxy_pack_subset = _capability_proxy_pack_subset_name(candidate_subset_name)
    if not proxy_pack_subset:
        return []

    def _ordered_unique(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ordered: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item in items:
            route_id = str(item.get("route_id") or "").strip()
            if (
                not route_id
                or route_id in seen_ids
                or not _route_allowed_for_subset(item, proxy_pack_subset)
            ):
                continue
            ordered.append(item)
            seen_ids.add(route_id)
        return ordered

    seed_routes = _ordered_unique(_capability_seed_pack_candidates(routes, candidate_subset_name))
    if not seed_routes:
        return []

    ordered: List[Dict[str, Any]] = list(seed_routes[:1])
    seen_ids = {str(item.get("route_id") or "").strip() for item in ordered}
    seed_route = ordered[0]
    seed_length = safe_float(seed_route.get("route_length_m")) or 0.0
    capability_pool = _ordered_unique(
        list(_capability_semantic_anchor_candidates(routes, candidate_subset_name, proxy_pack_subset))
        + list(_capability_focus_pack_candidates(routes, candidate_subset_name))
        + list(_capability_review_priority_candidates(routes, candidate_subset_name))
        + list(_capability_review_pack_candidates(routes, candidate_subset_name))
        + list(_capability_contrast_candidates(routes, candidate_subset_name))
        + list(_capability_next_review_candidates(routes, candidate_subset_name))
        + list(_capability_first_wave_candidates(routes, candidate_subset_name))
        + list(_capability_candidate_candidates(routes, candidate_subset_name))
    )
    proxy_candidates = [
        route
        for route in capability_pool
        if str(route.get("route_id") or "").strip()
        and str(route.get("route_id") or "").strip() not in seen_ids
        and _capability_proxy_evidence_rank(route) < 6
    ]
    proxy_candidates = sorted(
        proxy_candidates,
        key=lambda route: (
            int(_capability_proxy_similarity_rank(seed_route, route, candidate_subset_name)),
            int(_capability_proxy_evidence_rank(route)),
            abs((safe_float(route.get("route_length_m")) or 0.0) - seed_length),
            str(route.get("route_id") or ""),
        ),
    )
    for route in proxy_candidates:
        route_id = str(route.get("route_id") or "").strip()
        if not route_id or route_id in seen_ids:
            continue
        ordered.append(route)
        seen_ids.add(route_id)
        if len(ordered) >= _capability_proxy_pack_limit(candidate_subset_name):
            break
    if len(ordered) >= _capability_proxy_pack_limit(candidate_subset_name):
        return ordered
    for route in _ordered_unique(
        list(_capability_focus_pack_candidates(routes, candidate_subset_name))
        + list(_capability_review_priority_candidates(routes, candidate_subset_name))
        + list(_capability_review_pack_candidates(routes, candidate_subset_name))
        + list(_capability_contrast_candidates(routes, candidate_subset_name))
        + list(_capability_next_review_candidates(routes, candidate_subset_name))
    ):
        route_id = str(route.get("route_id") or "").strip()
        if not route_id or route_id in seen_ids:
            continue
        ordered.append(route)
        seen_ids.add(route_id)
        if len(ordered) >= _capability_proxy_pack_limit(candidate_subset_name):
            break
    return ordered


def _sync_capability_recommended_uses(route: Dict[str, Any], recommended_uses: Sequence[str]) -> List[str]:
    uses = [
        str(entry).strip()
        for entry in list(recommended_uses or [])
        if str(entry).strip() and str(entry).strip() not in CAPABILITY_SUBSET_NAMES
    ]
    if _is_lane_keep_candidate(route):
        uses.append("lane_keep_candidate")
    if _is_curve_lane_follow_candidate(route):
        uses.append("curve_lane_follow_candidate")
    if _is_junction_traverse_candidate(route):
        uses.append("junction_traverse_candidate")
    if _is_traffic_light_candidate(route):
        uses.append("traffic_light_candidate")
    return list(dict.fromkeys(uses))


def _sync_capability_first_wave_recommended_uses(
    routes: Sequence[Dict[str, Any]],
    subsets: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    dynamic_subset_names = (
        CAPABILITY_FIRST_WAVE_SUBSET_NAMES
        + CAPABILITY_NEXT_REVIEW_SUBSET_NAMES
        + CAPABILITY_CONTRAST_SUBSET_NAMES
        + CAPABILITY_REVIEW_PACK_SUBSET_NAMES
        + CAPABILITY_REVIEW_PRIORITY_SUBSET_NAMES
        + CAPABILITY_FOCUS_PACK_SUBSET_NAMES
        + CAPABILITY_PROXY_PACK_SUBSET_NAMES
        + CAPABILITY_SEED_PACK_SUBSET_NAMES
        + CAPABILITY_HISTORY_GAP_SUBSET_NAMES
        + CAPABILITY_PAIR_GAP_SUBSET_NAMES
    )
    subset_membership = {
        subset_name: {
            str(route_id).strip()
            for route_id in list(subsets.get(subset_name) or [])
            if str(route_id).strip()
        }
        for subset_name in dynamic_subset_names
    }
    synced_routes: List[Dict[str, Any]] = []
    for route in routes:
        synced = dict(route)
        route_id = str(synced.get("route_id") or "").strip()
        recommended_uses = [
            str(entry).strip()
            for entry in list(synced.get("recommended_uses") or [])
            if str(entry).strip() and str(entry).strip() not in dynamic_subset_names
        ]
        for subset_name in dynamic_subset_names:
            if route_id and route_id in subset_membership.get(subset_name, set()):
                recommended_uses.append(subset_name)
        synced["recommended_uses"] = _sync_capability_recommended_uses(synced, recommended_uses)
        synced_routes.append(synced)
    return synced_routes


def _lateral_smoke_candidate_score(route: Dict[str, Any]) -> tuple[float, int, int, str]:
    tags = _route_health_tags(route)
    empirical_rank = 3
    if "route_health_pass" in tags:
        empirical_rank = 0
    elif "route_health_candidate" in tags or "guarded_lateral_runtime_ok" in tags:
        empirical_rank = 1
    elif "empirically_reviewed" in tags:
        empirical_rank = 2
    route_length = safe_float(route.get("route_length_m")) or 999.0
    spawn_lane = route.get("spawn_lane") or {}
    spawn_lane_id = abs(safe_int(spawn_lane.get("lane_id")) or 0)
    spawn_road_id = safe_int(spawn_lane.get("road_id")) or 0
    return (
        int(empirical_rank),
        abs(float(route_length) - 230.0),
        int(spawn_lane_id),
        int(spawn_road_id),
        str(route.get("route_id") or ""),
    )


def _normalized_recommended_subsets(corpus: Dict[str, Any]) -> Dict[str, List[str]]:
    empirical_summary = corpus.get("empirical_health_summary") or {}
    routes: List[Dict[str, Any]] = []
    for item in (corpus.get("routes") or []):
        if not isinstance(item, dict):
            continue
        route = dict(item)
        route_id = str(route.get("route_id") or "").strip()
        summary = empirical_summary.get(route_id) if isinstance(empirical_summary, dict) else None
        if isinstance(summary, dict):
            for prefix in ("lane_keep", "curve_lane_follow", "junction_traverse", "traffic_light"):
                for suffix in ("semantic_best_label", "semantic_best_completion", "semantic_best_comparison_label"):
                    key = f"{prefix}_{suffix}"
                    if key in summary:
                        route[key] = summary.get(key)
        routes.append(_sync_route_geometry_metrics(route))
    route_by_id = {
        str(item.get("route_id") or ""): item
        for item in routes
        if str(item.get("route_id") or "")
    }
    subsets = corpus.get("recommended_subsets") or {}

    def _sanitize_subset(entries: Any, subset_name: str) -> List[str]:
        ordered: List[str] = []
        for entry in list(entries or []):
            route_id = str(entry or "").strip()
            if (
                not route_id
                or route_id not in route_by_id
                or route_id in ordered
                or not _route_allowed_for_subset(route_by_id[route_id], subset_name)
            ):
                continue
            ordered.append(route_id)
        return ordered

    normalized = {
        "mainline_regression": _sanitize_subset(subsets.get("mainline_regression"), "mainline_regression"),
        "demo_candidate": _sanitize_subset(subsets.get("demo_candidate"), "demo_candidate"),
        "calibration_compare_candidate": _sanitize_subset(
            subsets.get("calibration_compare_candidate"),
            "calibration_compare_candidate",
        ),
        "lane_keep_candidate": [],
        "curve_lane_follow_candidate": [],
        "junction_traverse_candidate": [],
        "traffic_light_candidate": [],
        "lane_keep_first_wave_smoke": [],
        "curve_lane_follow_first_wave_smoke": [],
        "junction_traverse_first_wave_smoke": [],
        "traffic_light_first_wave_smoke": [],
        "lane_keep_next_review_queue": [],
        "curve_lane_follow_next_review_queue": [],
        "junction_traverse_next_review_queue": [],
        "traffic_light_next_review_queue": [],
        "lane_keep_contrast_queue": [],
        "curve_lane_follow_contrast_queue": [],
        "junction_traverse_contrast_queue": [],
        "traffic_light_contrast_queue": [],
        "lane_keep_review_pack": [],
        "curve_lane_follow_review_pack": [],
        "junction_traverse_review_pack": [],
        "traffic_light_review_pack": [],
        "lane_keep_review_priority_queue": [],
        "curve_lane_follow_review_priority_queue": [],
        "junction_traverse_review_priority_queue": [],
        "traffic_light_review_priority_queue": [],
        "lane_keep_focus_pack": [],
        "curve_lane_follow_focus_pack": [],
        "junction_traverse_focus_pack": [],
        "traffic_light_focus_pack": [],
        "lane_keep_proxy_pack": [],
        "curve_lane_follow_proxy_pack": [],
        "junction_traverse_proxy_pack": [],
        "traffic_light_proxy_pack": [],
        "lane_keep_seed_pack": [],
        "curve_lane_follow_seed_pack": [],
        "junction_traverse_seed_pack": [],
        "traffic_light_seed_pack": [],
        "lane_keep_history_gap_queue": [],
        "curve_lane_follow_history_gap_queue": [],
        "junction_traverse_history_gap_queue": [],
        "traffic_light_history_gap_queue": [],
        "curve_lane_follow_pair_gap_queue": [],
        "junction_traverse_pair_gap_queue": [],
        "traffic_light_pair_gap_queue": [],
        "guarded_lateral_first_wave_smoke": [],
        "guarded_lateral_repeat_verified_smoke": [],
    }
    for capability_subset in CAPABILITY_SUBSET_NAMES:
        capability_candidates = _capability_candidate_candidates(routes, capability_subset)
        normalized[capability_subset] = [
            str(item.get("route_id") or "")
            for item in capability_candidates[: _capability_candidate_limit(capability_subset)]
            if str(item.get("route_id") or "")
        ]
        first_wave_subset = _capability_first_wave_subset_name(capability_subset)
        normalized[first_wave_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_first_wave_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        next_review_subset = _capability_next_review_subset_name(capability_subset)
        normalized[next_review_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_next_review_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        contrast_subset = _capability_contrast_subset_name(capability_subset)
        normalized[contrast_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_contrast_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        review_pack_subset = _capability_review_pack_subset_name(capability_subset)
        normalized[review_pack_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_review_pack_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        review_priority_subset = _capability_review_priority_subset_name(capability_subset)
        normalized[review_priority_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_review_priority_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        focus_pack_subset = _capability_focus_pack_subset_name(capability_subset)
        normalized[focus_pack_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_focus_pack_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        proxy_pack_subset = _capability_proxy_pack_subset_name(capability_subset)
        normalized[proxy_pack_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_proxy_pack_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        seed_pack_subset = _capability_seed_pack_subset_name(capability_subset)
        normalized[seed_pack_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_seed_pack_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        history_gap_subset = _capability_history_gap_subset_name(capability_subset)
        normalized[history_gap_subset] = [
            str(item.get("route_id") or "")
            for item in _capability_history_gap_candidates(routes, capability_subset)
            if str(item.get("route_id") or "")
        ]
        pair_gap_subset = _capability_pair_gap_subset_name(capability_subset)
        if pair_gap_subset:
            normalized[pair_gap_subset] = [
                str(item.get("route_id") or "")
                for item in _capability_pair_gap_candidates(routes, capability_subset)
                if str(item.get("route_id") or "")
            ]
    lateral_candidates = sorted(
        (
            route
            for route in routes
            if "lateral_smoke_candidate" in list(route.get("recommended_uses") or [])
            or _should_autobackfill_lateral_smoke_candidate(route)
        ),
        key=_lateral_smoke_candidate_score,
    )
    normalized["lateral_smoke_candidate"] = [
        str(item.get("route_id") or "")
        for item in lateral_candidates[:8]
        if str(item.get("route_id") or "")
    ]
    guarded_first_wave_candidates = sorted(
        (
            route
            for route in routes
            if "guarded_lateral_first_wave_smoke" in list(route.get("recommended_uses") or [])
            or _is_guarded_lateral_first_wave_smoke(route)
        ),
        key=_lateral_smoke_candidate_score,
    )
    normalized["guarded_lateral_first_wave_smoke"] = [
        str(item.get("route_id") or "")
        for item in guarded_first_wave_candidates[:4]
        if str(item.get("route_id") or "")
    ]
    guarded_repeat_verified_candidates = sorted(
        (
            route
            for route in routes
            if "guarded_lateral_repeat_verified_smoke" in list(route.get("recommended_uses") or [])
            or _is_guarded_lateral_repeat_verified_smoke(route)
        ),
        key=_lateral_smoke_candidate_score,
    )
    normalized["guarded_lateral_repeat_verified_smoke"] = [
        str(item.get("route_id") or "")
        for item in guarded_repeat_verified_candidates[:4]
        if str(item.get("route_id") or "")
    ]
    return normalized


def load_route_corpus(path: Path) -> Dict[str, Any]:
    payload = load_json(path)
    routes = payload.get("routes")
    if not isinstance(routes, list):
        raise RuntimeError(f"Invalid Town01 route corpus: {path}")
    normalized_routes: List[Dict[str, Any]] = []
    for item in routes:
        if not isinstance(item, dict):
            continue
        route = _sync_route_geometry_metrics(dict(item))
        recommended_uses = [str(entry) for entry in list(route.get("recommended_uses") or []) if str(entry)]
        if not _route_allowed_for_subset(route, "lateral_smoke_candidate"):
            recommended_uses = [entry for entry in recommended_uses if entry != "lateral_smoke_candidate"]
        if not _route_allowed_for_subset(route, "guarded_lateral_first_wave_smoke"):
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_first_wave_smoke"]
        if not _route_allowed_for_subset(route, "guarded_lateral_repeat_verified_smoke"):
            recommended_uses = [entry for entry in recommended_uses if entry != "guarded_lateral_repeat_verified_smoke"]
        if _should_autobackfill_lateral_smoke_candidate(route) and "lateral_smoke_candidate" not in recommended_uses:
            recommended_uses.append("lateral_smoke_candidate")
        if _is_guarded_lateral_first_wave_smoke(route) and "guarded_lateral_first_wave_smoke" not in recommended_uses:
            recommended_uses.append("guarded_lateral_first_wave_smoke")
        if (
            _is_guarded_lateral_repeat_verified_smoke(route)
            and "guarded_lateral_repeat_verified_smoke" not in recommended_uses
        ):
            recommended_uses.append("guarded_lateral_repeat_verified_smoke")
        route["recommended_uses"] = _sync_capability_recommended_uses(route, recommended_uses)
        normalized_routes.append(route)
    payload["routes"] = normalized_routes
    payload["recommended_subsets"] = _normalized_recommended_subsets(payload)
    payload["routes"] = _sync_capability_first_wave_recommended_uses(
        [item for item in list(payload.get("routes") or []) if isinstance(item, dict)],
        payload["recommended_subsets"],
    )
    payload["recommended_subsets"] = _normalized_recommended_subsets(payload)
    return payload


def select_route_ids(
    corpus: Dict[str, Any],
    *,
    route_id: str = "",
    sample_size: int = 1,
    sample_seed: int = 1,
    recommended_subset: str = "",
) -> List[str]:
    routes = [item for item in (corpus.get("routes") or []) if isinstance(item, dict)]
    if route_id:
        selected = [str(item.get("route_id") or "") for item in routes if str(item.get("route_id") or "") == route_id]
        if not selected:
            raise RuntimeError(f"Route id not found in corpus: {route_id}")
        return selected
    sample_size = max(1, int(sample_size))
    normalized_subsets = _normalized_recommended_subsets(corpus)
    subset_name = str(recommended_subset or "").strip()
    if subset_name:
        if subset_name not in normalized_subsets:
            raise RuntimeError(f"Recommended subset not found in corpus: {subset_name}")
        recommended = list(normalized_subsets.get(subset_name) or [])
        ordered: List[str] = []
        for item in recommended[:sample_size]:
            if item not in ordered:
                ordered.append(item)
        return ordered
    else:
        recommended = [
            str(item.get("route_id") or "")
            for item in routes
            if "mainline_regression" in list(item.get("recommended_uses") or [])
        ]
    recommended = [item for item in recommended if item]
    ordered = []
    for item in recommended[: min(8, sample_size)]:
        if item not in ordered:
            ordered.append(item)
    remaining = [
        str(item.get("route_id") or "")
        for item in routes
        if str(item.get("route_id") or "") and str(item.get("route_id") or "") not in ordered
    ]
    rng = random.Random(int(sample_seed))
    rng.shuffle(remaining)
    while len(ordered) < sample_size and remaining:
        ordered.append(remaining.pop(0))
    return ordered


def render_corpus_report(corpus: Dict[str, Any]) -> str:
    routes = [item for item in (corpus.get("routes") or []) if isinstance(item, dict)]
    route_by_id = {
        str(item.get("route_id") or "").strip(): item
        for item in routes
        if str(item.get("route_id") or "").strip()
    }
    excluded = corpus.get("excluded_routes") or {}
    subsets = _normalized_recommended_subsets(corpus)
    coverage = corpus.get("coverage") or {}
    health_tag_counts: Dict[str, int] = {}
    lateral_deprioritized_routes: List[str] = []
    for route in routes:
        route_id = str(route.get("route_id") or "").strip()
        tags = _route_health_tags(route)
        for tag in sorted(tags):
            health_tag_counts[tag] = int(health_tag_counts.get(tag, 0)) + 1
        if not _route_allowed_for_subset(route, "lateral_smoke_candidate") and route_id:
            lateral_deprioritized_routes.append(route_id)
    lines = [
        "# Town01 Route Corpus Report",
        "",
        f"- map: `{corpus.get('map') or 'Town01'}`",
        f"- total_legal_routes: `{len(routes)}`",
        "",
        "## Exclusions",
        "",
    ]
    spawn_rejections = excluded.get("spawn_rejections") or {}
    goal_rejections = excluded.get("goal_rejections") or {}
    if spawn_rejections:
        for key, value in sorted(spawn_rejections.items()):
            lines.append(f"- spawn exclude `{key}`: `{value}`")
    if goal_rejections:
        for key, value in sorted(goal_rejections.items()):
            lines.append(f"- goal exclude `{key}`: `{value}`")
    if not spawn_rejections and not goal_rejections:
        lines.append("- no explicit exclusions recorded")
    lines.extend(
        [
            "",
            "## Recommended Subsets",
            "",
            f"- mainline_regression: `{len(subsets.get('mainline_regression') or [])}` routes",
            f"- demo_candidate: `{len(subsets.get('demo_candidate') or [])}` routes",
            f"- calibration_compare_candidate: `{len(subsets.get('calibration_compare_candidate') or [])}` routes",
            f"- lane_keep_candidate: `{len(subsets.get('lane_keep_candidate') or [])}` routes",
            f"- curve_lane_follow_candidate: `{len(subsets.get('curve_lane_follow_candidate') or [])}` routes",
            f"- junction_traverse_candidate: `{len(subsets.get('junction_traverse_candidate') or [])}` routes",
            f"- traffic_light_candidate: `{len(subsets.get('traffic_light_candidate') or [])}` routes",
            f"- lane_keep_first_wave_smoke: `{len(subsets.get('lane_keep_first_wave_smoke') or [])}` routes",
            f"- curve_lane_follow_first_wave_smoke: `{len(subsets.get('curve_lane_follow_first_wave_smoke') or [])}` routes",
            f"- junction_traverse_first_wave_smoke: `{len(subsets.get('junction_traverse_first_wave_smoke') or [])}` routes",
            f"- traffic_light_first_wave_smoke: `{len(subsets.get('traffic_light_first_wave_smoke') or [])}` routes",
            f"- lane_keep_next_review_queue: `{len(subsets.get('lane_keep_next_review_queue') or [])}` routes",
            f"- curve_lane_follow_next_review_queue: `{len(subsets.get('curve_lane_follow_next_review_queue') or [])}` routes",
            f"- junction_traverse_next_review_queue: `{len(subsets.get('junction_traverse_next_review_queue') or [])}` routes",
            f"- traffic_light_next_review_queue: `{len(subsets.get('traffic_light_next_review_queue') or [])}` routes",
            f"- lane_keep_contrast_queue: `{len(subsets.get('lane_keep_contrast_queue') or [])}` routes",
            f"- curve_lane_follow_contrast_queue: `{len(subsets.get('curve_lane_follow_contrast_queue') or [])}` routes",
            f"- junction_traverse_contrast_queue: `{len(subsets.get('junction_traverse_contrast_queue') or [])}` routes",
            f"- traffic_light_contrast_queue: `{len(subsets.get('traffic_light_contrast_queue') or [])}` routes",
            f"- lane_keep_review_pack: `{len(subsets.get('lane_keep_review_pack') or [])}` routes",
            f"- curve_lane_follow_review_pack: `{len(subsets.get('curve_lane_follow_review_pack') or [])}` routes",
            f"- junction_traverse_review_pack: `{len(subsets.get('junction_traverse_review_pack') or [])}` routes",
            f"- traffic_light_review_pack: `{len(subsets.get('traffic_light_review_pack') or [])}` routes",
            f"- lane_keep_review_priority_queue: `{len(subsets.get('lane_keep_review_priority_queue') or [])}` routes",
            f"- curve_lane_follow_review_priority_queue: `{len(subsets.get('curve_lane_follow_review_priority_queue') or [])}` routes",
            f"- junction_traverse_review_priority_queue: `{len(subsets.get('junction_traverse_review_priority_queue') or [])}` routes",
            f"- traffic_light_review_priority_queue: `{len(subsets.get('traffic_light_review_priority_queue') or [])}` routes",
            f"- lane_keep_focus_pack: `{len(subsets.get('lane_keep_focus_pack') or [])}` routes",
            f"- curve_lane_follow_focus_pack: `{len(subsets.get('curve_lane_follow_focus_pack') or [])}` routes",
            f"- junction_traverse_focus_pack: `{len(subsets.get('junction_traverse_focus_pack') or [])}` routes",
            f"- traffic_light_focus_pack: `{len(subsets.get('traffic_light_focus_pack') or [])}` routes",
            f"- lane_keep_proxy_pack: `{len(subsets.get('lane_keep_proxy_pack') or [])}` routes",
            f"- curve_lane_follow_proxy_pack: `{len(subsets.get('curve_lane_follow_proxy_pack') or [])}` routes",
            f"- junction_traverse_proxy_pack: `{len(subsets.get('junction_traverse_proxy_pack') or [])}` routes",
            f"- traffic_light_proxy_pack: `{len(subsets.get('traffic_light_proxy_pack') or [])}` routes",
            f"- lane_keep_seed_pack: `{len(subsets.get('lane_keep_seed_pack') or [])}` routes",
            f"- curve_lane_follow_seed_pack: `{len(subsets.get('curve_lane_follow_seed_pack') or [])}` routes",
            f"- junction_traverse_seed_pack: `{len(subsets.get('junction_traverse_seed_pack') or [])}` routes",
            f"- traffic_light_seed_pack: `{len(subsets.get('traffic_light_seed_pack') or [])}` routes",
            f"- lane_keep_history_gap_queue: `{len(subsets.get('lane_keep_history_gap_queue') or [])}` routes",
            f"- curve_lane_follow_history_gap_queue: `{len(subsets.get('curve_lane_follow_history_gap_queue') or [])}` routes",
            f"- junction_traverse_history_gap_queue: `{len(subsets.get('junction_traverse_history_gap_queue') or [])}` routes",
            f"- traffic_light_history_gap_queue: `{len(subsets.get('traffic_light_history_gap_queue') or [])}` routes",
            f"- curve_lane_follow_pair_gap_queue: `{len(subsets.get('curve_lane_follow_pair_gap_queue') or [])}` routes",
            f"- junction_traverse_pair_gap_queue: `{len(subsets.get('junction_traverse_pair_gap_queue') or [])}` routes",
            f"- traffic_light_pair_gap_queue: `{len(subsets.get('traffic_light_pair_gap_queue') or [])}` routes",
            f"- guarded_lateral_first_wave_smoke: `{len(subsets.get('guarded_lateral_first_wave_smoke') or [])}` routes",
            f"- guarded_lateral_repeat_verified_smoke: `{len(subsets.get('guarded_lateral_repeat_verified_smoke') or [])}` routes",
            f"- lateral_smoke_candidate: `{len(subsets.get('lateral_smoke_candidate') or [])}` routes",
            "",
            "## Current Recommended Route IDs",
            "",
            f"- mainline_regression: `{', '.join(subsets.get('mainline_regression') or []) or 'none'}`",
            f"- demo_candidate: `{', '.join(subsets.get('demo_candidate') or []) or 'none'}`",
            f"- calibration_compare_candidate: `{', '.join(subsets.get('calibration_compare_candidate') or []) or 'none'}`",
            f"- lane_keep_candidate: `{', '.join(subsets.get('lane_keep_candidate') or []) or 'none'}`",
            f"- curve_lane_follow_candidate: `{', '.join(subsets.get('curve_lane_follow_candidate') or []) or 'none'}`",
            f"- junction_traverse_candidate: `{', '.join(subsets.get('junction_traverse_candidate') or []) or 'none'}`",
            f"- traffic_light_candidate: `{', '.join(subsets.get('traffic_light_candidate') or []) or 'none'}`",
            f"- lane_keep_first_wave_smoke: `{', '.join(subsets.get('lane_keep_first_wave_smoke') or []) or 'none'}`",
            f"- curve_lane_follow_first_wave_smoke: `{', '.join(subsets.get('curve_lane_follow_first_wave_smoke') or []) or 'none'}`",
            f"- junction_traverse_first_wave_smoke: `{', '.join(subsets.get('junction_traverse_first_wave_smoke') or []) or 'none'}`",
            f"- traffic_light_first_wave_smoke: `{', '.join(subsets.get('traffic_light_first_wave_smoke') or []) or 'none'}`",
            f"- lane_keep_next_review_queue: `{', '.join(subsets.get('lane_keep_next_review_queue') or []) or 'none'}`",
            f"- curve_lane_follow_next_review_queue: `{', '.join(subsets.get('curve_lane_follow_next_review_queue') or []) or 'none'}`",
            f"- junction_traverse_next_review_queue: `{', '.join(subsets.get('junction_traverse_next_review_queue') or []) or 'none'}`",
            f"- traffic_light_next_review_queue: `{', '.join(subsets.get('traffic_light_next_review_queue') or []) or 'none'}`",
            f"- lane_keep_contrast_queue: `{', '.join(subsets.get('lane_keep_contrast_queue') or []) or 'none'}`",
            f"- curve_lane_follow_contrast_queue: `{', '.join(subsets.get('curve_lane_follow_contrast_queue') or []) or 'none'}`",
            f"- junction_traverse_contrast_queue: `{', '.join(subsets.get('junction_traverse_contrast_queue') or []) or 'none'}`",
            f"- traffic_light_contrast_queue: `{', '.join(subsets.get('traffic_light_contrast_queue') or []) or 'none'}`",
            f"- lane_keep_review_pack: `{', '.join(subsets.get('lane_keep_review_pack') or []) or 'none'}`",
            f"- curve_lane_follow_review_pack: `{', '.join(subsets.get('curve_lane_follow_review_pack') or []) or 'none'}`",
            f"- junction_traverse_review_pack: `{', '.join(subsets.get('junction_traverse_review_pack') or []) or 'none'}`",
            f"- traffic_light_review_pack: `{', '.join(subsets.get('traffic_light_review_pack') or []) or 'none'}`",
            f"- lane_keep_review_priority_queue: `{', '.join(subsets.get('lane_keep_review_priority_queue') or []) or 'none'}`",
            f"- curve_lane_follow_review_priority_queue: `{', '.join(subsets.get('curve_lane_follow_review_priority_queue') or []) or 'none'}`",
            f"- junction_traverse_review_priority_queue: `{', '.join(subsets.get('junction_traverse_review_priority_queue') or []) or 'none'}`",
            f"- traffic_light_review_priority_queue: `{', '.join(subsets.get('traffic_light_review_priority_queue') or []) or 'none'}`",
            f"- lane_keep_focus_pack: `{', '.join(subsets.get('lane_keep_focus_pack') or []) or 'none'}`",
            f"- curve_lane_follow_focus_pack: `{', '.join(subsets.get('curve_lane_follow_focus_pack') or []) or 'none'}`",
            f"- junction_traverse_focus_pack: `{', '.join(subsets.get('junction_traverse_focus_pack') or []) or 'none'}`",
            f"- traffic_light_focus_pack: `{', '.join(subsets.get('traffic_light_focus_pack') or []) or 'none'}`",
            f"- lane_keep_proxy_pack: `{', '.join(subsets.get('lane_keep_proxy_pack') or []) or 'none'}`",
            f"- curve_lane_follow_proxy_pack: `{', '.join(subsets.get('curve_lane_follow_proxy_pack') or []) or 'none'}`",
            f"- junction_traverse_proxy_pack: `{', '.join(subsets.get('junction_traverse_proxy_pack') or []) or 'none'}`",
            f"- traffic_light_proxy_pack: `{', '.join(subsets.get('traffic_light_proxy_pack') or []) or 'none'}`",
            f"- lane_keep_seed_pack: `{', '.join(subsets.get('lane_keep_seed_pack') or []) or 'none'}`",
            f"- curve_lane_follow_seed_pack: `{', '.join(subsets.get('curve_lane_follow_seed_pack') or []) or 'none'}`",
            f"- junction_traverse_seed_pack: `{', '.join(subsets.get('junction_traverse_seed_pack') or []) or 'none'}`",
            f"- traffic_light_seed_pack: `{', '.join(subsets.get('traffic_light_seed_pack') or []) or 'none'}`",
            f"- lane_keep_history_gap_queue: `{', '.join(subsets.get('lane_keep_history_gap_queue') or []) or 'none'}`",
            f"- curve_lane_follow_history_gap_queue: `{', '.join(subsets.get('curve_lane_follow_history_gap_queue') or []) or 'none'}`",
            f"- junction_traverse_history_gap_queue: `{', '.join(subsets.get('junction_traverse_history_gap_queue') or []) or 'none'}`",
            f"- traffic_light_history_gap_queue: `{', '.join(subsets.get('traffic_light_history_gap_queue') or []) or 'none'}`",
            f"- curve_lane_follow_pair_gap_queue: `{', '.join(subsets.get('curve_lane_follow_pair_gap_queue') or []) or 'none'}`",
            f"- junction_traverse_pair_gap_queue: `{', '.join(subsets.get('junction_traverse_pair_gap_queue') or []) or 'none'}`",
            f"- traffic_light_pair_gap_queue: `{', '.join(subsets.get('traffic_light_pair_gap_queue') or []) or 'none'}`",
            f"- guarded_lateral_first_wave_smoke: `{', '.join(subsets.get('guarded_lateral_first_wave_smoke') or []) or 'none'}`",
            f"- guarded_lateral_repeat_verified_smoke: `{', '.join(subsets.get('guarded_lateral_repeat_verified_smoke') or []) or 'none'}`",
            f"- lateral_smoke_candidate: `{', '.join(subsets.get('lateral_smoke_candidate') or []) or 'none'}`",
            "",
            "## Health Tags",
            "",
        ]
    )
    lines[-3:-3] = ["", "## Capability Pair Coverage", ""]
    for candidate_subset in sorted(PAIR_AWARE_CAPABILITY_SUBSET_NAMES):
        capability_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(candidate_subset) or [])
            if route_id in route_by_id
        ]
        candidate_pair_coverage = _capability_pair_coverage(capability_routes, candidate_subset)
        first_wave_subset = _capability_first_wave_subset_name(candidate_subset)
        next_review_subset = _capability_next_review_subset_name(candidate_subset)
        contrast_subset = _capability_contrast_subset_name(candidate_subset)
        first_wave_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(first_wave_subset) or [])
            if route_id in route_by_id
        ]
        next_review_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(next_review_subset) or [])
            if route_id in route_by_id
        ]
        contrast_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(contrast_subset) or [])
            if route_id in route_by_id
        ]
        review_pack_subset = _capability_review_pack_subset_name(candidate_subset)
        review_pack_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(review_pack_subset) or [])
            if route_id in route_by_id
        ]
        review_priority_subset = _capability_review_priority_subset_name(candidate_subset)
        review_priority_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(review_priority_subset) or [])
            if route_id in route_by_id
        ]
        focus_pack_subset = _capability_focus_pack_subset_name(candidate_subset)
        focus_pack_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(focus_pack_subset) or [])
            if route_id in route_by_id
        ]
        proxy_pack_subset = _capability_proxy_pack_subset_name(candidate_subset)
        proxy_pack_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(proxy_pack_subset) or [])
            if route_id in route_by_id
        ]
        seed_pack_subset = _capability_seed_pack_subset_name(candidate_subset)
        seed_pack_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(seed_pack_subset) or [])
            if route_id in route_by_id
        ]
        pair_gap_subset = _capability_pair_gap_subset_name(candidate_subset)
        pair_gap_routes = [
            route_by_id[route_id]
            for route_id in list(subsets.get(pair_gap_subset) or [])
            if route_id in route_by_id
        ] if pair_gap_subset else []
        first_wave_pair_coverage = _capability_pair_coverage(first_wave_routes, candidate_subset)
        next_review_pair_coverage = _capability_pair_coverage(next_review_routes, candidate_subset)
        contrast_pair_coverage = _capability_pair_coverage(contrast_routes, candidate_subset)
        review_pack_pair_coverage = _capability_pair_coverage(review_pack_routes, candidate_subset)
        review_priority_pair_coverage = _capability_pair_coverage(review_priority_routes, candidate_subset)
        focus_pack_pair_coverage = _capability_pair_coverage(focus_pack_routes, candidate_subset)
        proxy_pack_pair_coverage = _capability_pair_coverage(proxy_pack_routes, candidate_subset)
        seed_pack_pair_coverage = _capability_pair_coverage(seed_pack_routes, candidate_subset)
        pair_gap_pair_coverage = _capability_pair_coverage(pair_gap_routes, candidate_subset)
        missing_contrast_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(contrast_pair_coverage)
        )
        missing_review_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_pack_pair_coverage)
        )
        missing_review_priority_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(review_priority_pair_coverage)
        )
        missing_focus_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(focus_pack_pair_coverage)
        )
        missing_proxy_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(proxy_pack_pair_coverage)
        )
        missing_seed_pack_pair_coverage = sorted(
            set(candidate_pair_coverage).difference(seed_pack_pair_coverage)
        )
        lines[-3:-3] = [
            f"- {candidate_subset}: candidate_pairs=`{', '.join(candidate_pair_coverage) or 'none'}` | first_wave_pairs=`{', '.join(first_wave_pair_coverage) or 'none'}` | next_review_pairs=`{', '.join(next_review_pair_coverage) or 'none'}` | contrast_pairs=`{', '.join(contrast_pair_coverage) or 'none'}` | review_pack_pairs=`{', '.join(review_pack_pair_coverage) or 'none'}` | review_priority_pairs=`{', '.join(review_priority_pair_coverage) or 'none'}` | focus_pack_pairs=`{', '.join(focus_pack_pair_coverage) or 'none'}` | proxy_pack_pairs=`{', '.join(proxy_pack_pair_coverage) or 'none'}` | seed_pack_pairs=`{', '.join(seed_pack_pair_coverage) or 'none'}` | pair_gap_pairs=`{', '.join(pair_gap_pair_coverage) or 'none'}` | missing_contrast_pairs=`{', '.join(missing_contrast_pair_coverage) or 'none'}` | missing_review_pack_pairs=`{', '.join(missing_review_pack_pair_coverage) or 'none'}` | missing_review_priority_pairs=`{', '.join(missing_review_priority_pair_coverage) or 'none'}` | missing_focus_pack_pairs=`{', '.join(missing_focus_pack_pair_coverage) or 'none'}` | missing_proxy_pack_pairs=`{', '.join(missing_proxy_pack_pair_coverage) or 'none'}` | missing_seed_pack_pairs=`{', '.join(missing_seed_pack_pair_coverage) or 'none'}`"
        ]
    if health_tag_counts:
        for tag, count in sorted(health_tag_counts.items()):
            lines.append(f"- `{tag}`: `{count}` routes")
    else:
        lines.append("- no health tags recorded")
    lines.extend(
        [
            "",
            "## Empirical Deprioritization",
            "",
            f"- lateral_smoke_candidate blocked routes: `{', '.join(sorted(set(lateral_deprioritized_routes))) or 'none'}`",
            "",
            "## Coverage",
            "",
        ]
    )
    if coverage:
        for key, value in sorted(coverage.items()):
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- coverage unavailable")
    return "\n".join(lines) + "\n"


def route_id_from_metadata(scenario_meta: Dict[str, Any]) -> str:
    existing = str(scenario_meta.get("route_id") or "").strip()
    if existing:
        return existing
    spawn_idx = safe_int(scenario_meta.get("used_spawn_idx"))
    if spawn_idx is None:
        spawn_idx = safe_int(scenario_meta.get("requested_spawn_idx"))
    goal_trace_index = safe_int(scenario_meta.get("goal_trace_index"))
    if spawn_idx is None or goal_trace_index is None:
        return ""
    return f"town01_rh_spawn{int(spawn_idx):03d}_goal{int(goal_trace_index):03d}"


def _transform_xy_with_cfg(x: float, y: float, z: float, tf_cfg: Dict[str, Any]) -> Tuple[float, float, float]:
    yaw_rad = math.radians(float(tf_cfg.get("yaw_deg", 0.0) or 0.0))
    c = math.cos(yaw_rad)
    s = math.sin(yaw_rad)
    tx = float(tf_cfg.get("tx", 0.0) or 0.0)
    ty = float(tf_cfg.get("ty", 0.0) or 0.0)
    tz = float(tf_cfg.get("tz", 0.0) or 0.0)
    return (
        (c * x) - (s * y) + tx,
        (s * x) + (c * y) + ty,
        z + tz,
    )


def _compute_low_speed_creep_metrics(
    debug_timeseries_path: Path,
    *,
    start_after_sec: float,
    speed_below_mps: float,
    front_gap_above_m: float,
    require_routing_established: bool,
    ignore_when_terminal_stop_hold_active: bool,
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "available": False,
        "path": str(debug_timeseries_path),
        "start_after_sec": float(start_after_sec),
        "speed_below_mps": float(speed_below_mps),
        "front_gap_above_m": float(front_gap_above_m),
        "require_routing_established": bool(require_routing_established),
        "ignore_when_terminal_stop_hold_active": bool(ignore_when_terminal_stop_hold_active),
        "sample_count": 0,
        "matched_sample_count": 0,
        "longest_streak_frames": 0,
        "longest_streak_duration_sec": 0.0,
        "first_match_ts_sec": None,
        "longest_streak_start_ts_sec": None,
        "longest_streak_end_ts_sec": None,
    }
    if not debug_timeseries_path.exists():
        return metrics
    try:
        with debug_timeseries_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    except Exception:
        return metrics
    if not rows:
        metrics["available"] = True
        return metrics
    metrics["available"] = True
    metrics["sample_count"] = len(rows)
    first_ts = safe_float(rows[0].get("ts_sec"))
    if first_ts is None:
        return metrics
    current_frames = 0
    current_start_ts: Optional[float] = None
    prev_ts: Optional[float] = None
    best_duration = 0.0

    def _close_streak(end_ts: Optional[float]) -> None:
        nonlocal current_frames, current_start_ts, best_duration
        if current_frames <= 0 or current_start_ts is None or end_ts is None:
            current_frames = 0
            current_start_ts = None
            return
        duration = max(0.0, float(end_ts) - float(current_start_ts))
        if current_frames > int(metrics["longest_streak_frames"]) or (
            current_frames == int(metrics["longest_streak_frames"]) and duration > best_duration
        ):
            metrics["longest_streak_frames"] = int(current_frames)
            metrics["longest_streak_start_ts_sec"] = float(current_start_ts)
            metrics["longest_streak_end_ts_sec"] = float(end_ts)
            metrics["longest_streak_duration_sec"] = float(duration)
            best_duration = float(duration)
        current_frames = 0
        current_start_ts = None

    for row in rows:
        ts_sec = safe_float(row.get("ts_sec"))
        speed_mps = safe_float(row.get("speed_mps"))
        front_gap_lon_m = safe_float(row.get("front_obstacle_gap_lon_m"))
        routing_established = safe_bool(row.get("routing_established"))
        terminal_stop_hold_active = safe_bool(row.get("terminal_stop_hold_active"))
        prev_ts = ts_sec if ts_sec is not None else prev_ts
        if ts_sec is None or speed_mps is None or front_gap_lon_m is None:
            _close_streak(prev_ts)
            continue
        cond = ts_sec >= (first_ts + float(start_after_sec))
        cond = cond and speed_mps < float(speed_below_mps)
        cond = cond and front_gap_lon_m > float(front_gap_above_m)
        if require_routing_established:
            cond = cond and bool(routing_established)
        if ignore_when_terminal_stop_hold_active:
            cond = cond and not bool(terminal_stop_hold_active)
        if cond:
            metrics["matched_sample_count"] = int(metrics["matched_sample_count"]) + 1
            if metrics["first_match_ts_sec"] is None:
                metrics["first_match_ts_sec"] = float(ts_sec)
            if current_frames == 0:
                current_start_ts = float(ts_sec)
            current_frames += 1
        else:
            _close_streak(ts_sec)
        prev_ts = ts_sec
    _close_streak(prev_ts)
    return metrics


def _compute_basic_lateral_metrics(control_decode_path: Path) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "available": False,
        "path": str(control_decode_path),
        "sample_count": 0,
        "raw_steer_nonzero_count": 0,
        "raw_steer_nonzero_ratio": 0.0,
        "raw_steer_saturated_count": 0,
        "raw_steer_saturated_ratio": 0.0,
        "longest_continuous_saturation_frames": 0,
        "longest_continuous_saturation_sec": 0.0,
        "commanded_steer_nonzero_count": 0,
        "commanded_steer_nonzero_ratio": 0.0,
        "force_zero_steer_applied_count": 0,
        "guard_trigger_count": 0,
        "guard_trigger_ratio": 0.0,
        "guard_trigger_reason_top1": None,
    }
    rows = load_jsonl(control_decode_path)
    if not rows:
        return metrics
    metrics["available"] = True
    metrics["sample_count"] = len(rows)
    current_sat_frames = 0
    current_sat_start_ts: Optional[float] = None
    best_sat_duration = 0.0
    prev_ts: Optional[float] = None
    guard_reasons: Dict[str, int] = {}

    def _close_sat(ts_sec: Optional[float]) -> None:
        nonlocal current_sat_frames, current_sat_start_ts, best_sat_duration
        if current_sat_frames <= 0 or current_sat_start_ts is None or ts_sec is None:
            current_sat_frames = 0
            current_sat_start_ts = None
            return
        duration = max(0.0, float(ts_sec) - float(current_sat_start_ts))
        if current_sat_frames > int(metrics["longest_continuous_saturation_frames"]) or (
            current_sat_frames == int(metrics["longest_continuous_saturation_frames"]) and duration > best_sat_duration
        ):
            metrics["longest_continuous_saturation_frames"] = int(current_sat_frames)
            metrics["longest_continuous_saturation_sec"] = float(duration)
            best_sat_duration = float(duration)
        current_sat_frames = 0
        current_sat_start_ts = None

    for row in rows:
        ts_sec = safe_float(row.get("ts_sec"))
        raw_steer = safe_float(row.get("raw_steer"))
        commanded_steer = safe_float(row.get("commanded_steer"))
        if raw_steer is not None and abs(raw_steer) > 1e-6:
            metrics["raw_steer_nonzero_count"] = int(metrics["raw_steer_nonzero_count"]) + 1
        if raw_steer is not None and abs(raw_steer) >= 0.99:
            metrics["raw_steer_saturated_count"] = int(metrics["raw_steer_saturated_count"]) + 1
            if current_sat_frames == 0:
                current_sat_start_ts = ts_sec if ts_sec is not None else prev_ts
            current_sat_frames += 1
        else:
            _close_sat(ts_sec if ts_sec is not None else prev_ts)
        if commanded_steer is not None and abs(commanded_steer) > 1e-6:
            metrics["commanded_steer_nonzero_count"] = int(metrics["commanded_steer_nonzero_count"]) + 1
        if bool(row.get("force_zero_steer_applied")):
            metrics["force_zero_steer_applied_count"] = int(metrics["force_zero_steer_applied_count"]) + 1
        reasons: List[str] = []
        if bool(row.get("low_speed_steer_guard_applied")):
            reasons.append("low_speed_steer_guard")
        if bool(row.get("low_speed_sustained_guard_applied")):
            reasons.append("low_speed_sustained_guard")
        if bool(row.get("sustained_lateral_guard_applied")):
            reasons.append("sustained_lateral_guard")
        if bool(row.get("trajectory_contract_lateral_guard_applied")):
            reasons.append("trajectory_contract_lateral_guard")
        if reasons:
            metrics["guard_trigger_count"] = int(metrics["guard_trigger_count"]) + 1
            for reason in reasons:
                guard_reasons[reason] = int(guard_reasons.get(reason, 0)) + 1
        prev_ts = ts_sec if ts_sec is not None else prev_ts
    _close_sat(prev_ts)
    sample_count = int(metrics["sample_count"]) or 1
    metrics["raw_steer_nonzero_ratio"] = float(metrics["raw_steer_nonzero_count"]) / float(sample_count)
    metrics["raw_steer_saturated_ratio"] = float(metrics["raw_steer_saturated_count"]) / float(sample_count)
    metrics["commanded_steer_nonzero_ratio"] = float(metrics["commanded_steer_nonzero_count"]) / float(sample_count)
    metrics["guard_trigger_ratio"] = float(metrics["guard_trigger_count"]) / float(sample_count)
    if guard_reasons:
        metrics["guard_trigger_reason_top1"] = max(guard_reasons.items(), key=lambda item: item[1])[0]
    return metrics


def _read_debug_timeseries(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def _valid_map_points(debug_rows: List[Dict[str, Any]]) -> List[Tuple[float, float]]:
    points: List[Tuple[float, float]] = []
    for row in debug_rows:
        map_x = safe_float(row.get("map_x"))
        map_y = safe_float(row.get("map_y"))
        if map_x is None or map_y is None:
            continue
        points.append((float(map_x), float(map_y)))
    return points


def _route_progress_lower_bound(
    start_point: Tuple[float, float],
    goal_point: Optional[Tuple[float, float]],
    map_points: List[Tuple[float, float]],
    *,
    route_length_target_m: Optional[float],
) -> Dict[str, Optional[float]]:
    if not map_points:
        return {
            "final_goal_distance_m": None,
            "route_distance_achieved_m": None,
            "route_completion_ratio": None,
            "route_completion_percentage": None,
        }
    last_point = map_points[-1]
    max_displacement = max(
        math.sqrt(((point[0] - start_point[0]) ** 2) + ((point[1] - start_point[1]) ** 2))
        for point in map_points
    )
    if goal_point is None:
        if route_length_target_m is None or route_length_target_m <= 1e-6:
            return {
                "final_goal_distance_m": None,
                "route_distance_achieved_m": float(max_displacement),
                "route_completion_ratio": None,
                "route_completion_percentage": None,
            }
        completion = max(0.0, min(1.0, float(max_displacement) / float(route_length_target_m)))
        return {
            "final_goal_distance_m": None,
            "route_distance_achieved_m": float(max_displacement),
            "route_completion_ratio": float(completion),
            "route_completion_percentage": float(completion),
        }

    goal_dx = float(goal_point[0] - start_point[0])
    goal_dy = float(goal_point[1] - start_point[1])
    start_goal_distance = math.sqrt((goal_dx ** 2) + (goal_dy ** 2))
    if start_goal_distance <= 1e-6:
        return {
            "final_goal_distance_m": 0.0,
            "route_distance_achieved_m": 0.0,
            "route_completion_ratio": 0.0,
            "route_completion_percentage": 0.0,
        }
    unit_x = goal_dx / start_goal_distance
    unit_y = goal_dy / start_goal_distance
    max_forward_progress = max(
        0.0,
        min(
            start_goal_distance,
            max(
                (((point[0] - start_point[0]) * unit_x) + ((point[1] - start_point[1]) * unit_y))
                for point in map_points
            ),
        ),
    )
    final_goal_distance = math.sqrt(((goal_point[0] - last_point[0]) ** 2) + ((goal_point[1] - last_point[1]) ** 2))
    route_distance_achieved = max(0.0, max_forward_progress)
    completion = max(0.0, min(1.0, route_distance_achieved / start_goal_distance))
    return {
        "final_goal_distance_m": float(final_goal_distance),
        "route_distance_achieved_m": float(route_distance_achieved),
        "route_completion_ratio": float(completion),
        "route_completion_percentage": float(completion),
    }


def _run_max_speed(summary_data: Dict[str, Any], debug_rows: List[Dict[str, Any]]) -> float:
    candidate = safe_float(summary_data.get("max_speed_mps"))
    if candidate is not None:
        return float(candidate)
    speeds = [safe_float(row.get("speed_mps")) for row in debug_rows]
    speeds = [float(v) for v in speeds if v is not None]
    return max(speeds) if speeds else 0.0


def _compute_route_metrics(
    run_dir: Path,
    summary_data: Dict[str, Any],
    scenario_meta: Dict[str, Any],
    debug_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "available": False,
        "route_established": False,
        "route_length_target_m": safe_float(scenario_meta.get("route_length_m")),
        "start_goal_distance_m": None,
        "final_goal_distance_m": None,
        "route_distance_achieved_m": None,
        "route_completion_ratio": None,
        "route_completion_percentage": None,
        "spawn_lane": scenario_meta.get("spawn_lane"),
        "goal_lane": scenario_meta.get("goal_lane"),
        "goal_selection_attempt_count": len((scenario_meta.get("goal_selection_attempts") or [])),
    }
    live_route = summary_data.get("route_health") or {}
    if isinstance(live_route, dict):
        for key in (
            "route_established",
            "route_length_target_m",
            "start_goal_distance_m",
            "final_goal_distance_m",
            "route_distance_achieved_m",
            "route_completion_percentage",
            "spawn_lane",
            "goal_lane",
            "goal_selection_attempt_count",
        ):
            if key in live_route and live_route.get(key) is not None:
                metrics[key] = live_route.get(key)
        if metrics.get("route_completion_percentage") is not None:
            metrics["route_completion_ratio"] = metrics["route_completion_percentage"]
            metrics["available"] = True
    if metrics["available"]:
        return metrics

    map_points = _valid_map_points(debug_rows)
    spawn = scenario_meta.get("spawn") or {}
    goal = scenario_meta.get("goal") or {}
    sx = safe_float(spawn.get("x"))
    sy = safe_float(spawn.get("y"))
    gx = safe_float(goal.get("x"))
    gy = safe_float(goal.get("y"))
    start_point: Optional[Tuple[float, float]] = map_points[0] if map_points else None
    goal_point: Optional[Tuple[float, float]] = None
    start_goal_distance: Optional[float] = None
    if sx is not None and sy is not None and gx is not None and gy is not None:
        effective_cfg = load_effective_cfg(run_dir)
        tf_cfg = (((effective_cfg.get("algo", {}) or {}).get("apollo", {}) or {}).get("carla_to_apollo", {}) or {})
        start_candidates: List[Tuple[float, float]] = []
        goal_candidates: List[Tuple[float, float]] = []
        for goal_y_sign in (1.0, -1.0):
            if start_point is None:
                start_x, start_y, _ = _transform_xy_with_cfg(
                    float(sx),
                    float(sy) * goal_y_sign,
                    float(spawn.get("z", 0.0) or 0.0),
                    tf_cfg,
                )
                start_candidate = (float(start_x), float(start_y))
                start_candidates.append(start_candidate)
            goal_x, goal_y, _ = _transform_xy_with_cfg(
                float(gx),
                float(gy) * goal_y_sign,
                float(goal.get("z", 0.0) or 0.0),
                tf_cfg,
            )
            goal_candidates.append((float(goal_x), float(goal_y)))

        route_length_target_m = safe_float(metrics.get("route_length_target_m"))
        best_score: Optional[float] = None
        best_goal_point: Optional[Tuple[float, float]] = None
        best_start_goal_distance: Optional[float] = None
        best_start_point: Optional[Tuple[float, float]] = start_point
        if start_point is None:
            candidate_start_points = start_candidates
        else:
            candidate_start_points = [start_point] * len(goal_candidates)
        for candidate_start, candidate_goal in zip(candidate_start_points, goal_candidates):
            candidate_distance = math.sqrt(
                ((candidate_goal[0] - candidate_start[0]) ** 2) + ((candidate_goal[1] - candidate_start[1]) ** 2)
            )
            if route_length_target_m is not None:
                score = abs(candidate_distance - route_length_target_m)
            else:
                score = candidate_distance
            if best_score is None or score < best_score:
                best_score = float(score)
                best_goal_point = candidate_goal
                best_start_goal_distance = float(candidate_distance)
                best_start_point = candidate_start
        goal_point = best_goal_point
        start_goal_distance = best_start_goal_distance
        start_point = best_start_point
    if start_goal_distance is not None:
        metrics["start_goal_distance_m"] = float(start_goal_distance)

    route_progress = _route_progress_lower_bound(
        start_point if start_point is not None else (0.0, 0.0),
        goal_point,
        map_points,
        route_length_target_m=safe_float(metrics.get("route_length_target_m")),
    )
    for key, value in route_progress.items():
        if value is not None:
            metrics[key] = value
    if any(route_progress.get(key) is not None for key in ("route_distance_achieved_m", "route_completion_ratio", "final_goal_distance_m")):
        metrics["available"] = True
    return metrics


def _parse_log_timestamp(line: str) -> Optional[float]:
    match = re.match(r"^[IWEF](\d{2})(\d{2}) (\d{2}:\d{2}:\d{2}\.\d+)", line)
    if not match:
        return None
    month = int(match.group(1))
    day = int(match.group(2))
    time_text = match.group(3)
    year = datetime.now().year
    try:
        dt = datetime.strptime(f"{year}-{month:02d}-{day:02d} {time_text}", "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return None
    return dt.timestamp()


def _event_seq(row: Dict[str, Any]) -> Optional[int]:
    value = row.get("planning_header_sequence_num")
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _normalize_planning_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["timestamp"] = safe_float(item.get("timestamp"))
        item["planning_header_timestamp_sec"] = safe_float(item.get("planning_header_timestamp_sec"))
        item["planning_header_sequence_num"] = _event_seq(item)
        item["trajectory_point_count"] = int(item.get("trajectory_point_count", 0) or 0)
        item["trajectory_relative_time_min_sec"] = safe_float(item.get("trajectory_relative_time_min_sec"))
        item["trajectory_relative_time_max_sec"] = safe_float(item.get("trajectory_relative_time_max_sec"))
        item["planning_message_parsed_successfully"] = bool(
            item.get("planning_message_parsed_successfully", False)
        )
        out.append(item)
    out.sort(key=lambda row: float(row.get("timestamp") or 0.0))
    return out


def _build_planning_index(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        seq = row.get("planning_header_sequence_num")
        if seq is not None:
            out[int(seq)] = row
    return out


def _nearest_planning_row(
    rows: List[Dict[str, Any]],
    target_ts: float,
    *,
    max_dt_sec: Optional[float] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
    if not rows:
        return None, None
    ts_values = [float(item.get("timestamp") or 0.0) for item in rows]
    pos = bisect.bisect_left(ts_values, target_ts)
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    if pos < len(rows):
        candidates.append((abs(ts_values[pos] - target_ts), rows[pos]))
    if pos > 0:
        candidates.append((abs(ts_values[pos - 1] - target_ts), rows[pos - 1]))
    if not candidates:
        return None, None
    dt_sec, row = min(candidates, key=lambda item: item[0])
    if max_dt_sec is not None and dt_sec > max_dt_sec:
        return None, None
    return row, dt_sec


def _last_success_before(rows: List[Dict[str, Any]], target_ts: float, *, max_dt_sec: float = 0.3) -> Optional[Dict[str, Any]]:
    eligible = [row for row in rows if safe_float(row.get("timestamp")) is not None and float(row["timestamp"]) <= target_ts]
    if not eligible:
        return None
    row = eligible[-1]
    dt_sec = target_ts - float(row["timestamp"])
    return row if dt_sec <= max_dt_sec else None


def _is_control_mode_not_ready(state: Optional[Dict[str, Any]]) -> bool:
    if not state:
        return False
    for key in ("auto_drive_mode", "engage_state", "drive_mode"):
        value = state.get(key)
        if value is None:
            continue
        text = str(value).strip().lower()
        if any(token in text for token in ("disallow", "not_ready", "not ready", "manual")):
            return True
    return False


def _classify_reject_reason(
    event_ts: float,
    planning_row: Optional[Dict[str, Any]],
    recent_control_state: Optional[Dict[str, Any]],
) -> str:
    if planning_row is None:
        return "no_recent_planning_message"
    planning_ts = safe_float(planning_row.get("timestamp"))
    if planning_ts is None or (event_ts - planning_ts) > 0.300:
        return "no_recent_planning_message"
    if not bool(planning_row.get("planning_message_parsed_successfully", False)):
        return "planning_parse_failed"
    point_count = int(planning_row.get("trajectory_point_count", 0) or 0)
    if point_count <= 0:
        return "zero_trajectory_points"
    if bool(planning_row.get("estop")) or bool((recent_control_state or {}).get("estop_flag")):
        return "estop_or_safety_guard"
    now_rel = None
    plan_header_ts = safe_float(planning_row.get("planning_header_timestamp_sec"))
    if plan_header_ts is not None:
        now_rel = event_ts - plan_header_ts
    first_rel = safe_float(planning_row.get("trajectory_relative_time_min_sec"))
    last_rel = safe_float(planning_row.get("trajectory_relative_time_max_sec"))
    if now_rel is None or first_rel is None or last_rel is None:
        return "trajectory_invalid_format"
    if now_rel > (last_rel + 0.05):
        return "trajectory_all_points_expired"
    if now_rel < (first_rel - 0.05):
        return "trajectory_not_started_yet"
    if _is_control_mode_not_ready(recent_control_state):
        return "control_mode_not_ready"
    if bool((recent_control_state or {}).get("control_used_cached_trajectory")):
        return "cached_trajectory_used"
    return "unknown"


def _parse_control_no_trajectory_log(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    events: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Optional[int]]] = set()
    seq_re = re.compile(r"planning_seq_num:(\d+)")
    for line in path.read_text(errors="ignore").splitlines():
        if "planning has no trajectory point" not in line:
            continue
        ts_sec = _parse_log_timestamp(line)
        if ts_sec is None:
            continue
        seq_match = seq_re.search(line)
        seq = int(seq_match.group(1)) if seq_match else None
        ts_text = re.match(r"^[IWEF]\d{4} \d{2}:\d{2}:\d{2}\.\d+", line)
        dedupe_key = (ts_text.group(0) if ts_text else f"{ts_sec:.6f}", seq)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        events.append({"timestamp": ts_sec, "planning_seq_num": seq, "raw_line": line})
    events.sort(key=lambda item: float(item["timestamp"]))
    return events


def _normalize_live_control_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["event_type"] = "control_success"
        item["timestamp"] = safe_float(item.get("timestamp"))
        item["latest_planning_msg_timestamp"] = safe_float(item.get("latest_planning_msg_timestamp"))
        item["latest_planning_msg_age_ms"] = safe_float(item.get("latest_planning_msg_age_ms"))
        item["latest_planning_msg_sequence_num"] = (
            int(item["latest_planning_msg_sequence_num"])
            if item.get("latest_planning_msg_sequence_num") is not None
            else None
        )
        item["latest_planning_trajectory_point_count"] = int(
            item.get("latest_planning_trajectory_point_count", 0) or 0
        )
        item["trajectory_first_point_relative_time"] = safe_float(item.get("trajectory_first_point_relative_time"))
        item["trajectory_last_point_relative_time"] = safe_float(item.get("trajectory_last_point_relative_time"))
        item["control_now_relative_to_planning_header_sec"] = safe_float(
            item.get("control_now_relative_to_planning_header_sec")
        )
        out.append(item)
    out.sort(key=lambda row: float(row.get("timestamp") or 0.0))
    return out


def _build_control_consume_events(
    planning_rows: List[Dict[str, Any]],
    live_rows: List[Dict[str, Any]],
    miss_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    planning_by_seq = _build_planning_index(planning_rows)
    success_rows = _normalize_live_control_rows(live_rows)
    events: List[Dict[str, Any]] = [dict(row) for row in success_rows]

    for miss in miss_rows:
        ts = float(miss["timestamp"])
        planning_row = planning_by_seq.get(int(miss["planning_seq_num"])) if miss.get("planning_seq_num") is not None else None
        if planning_row is None:
            planning_row, _ = _nearest_planning_row(planning_rows, ts, max_dt_sec=0.300)
        recent_state = _last_success_before(success_rows, ts, max_dt_sec=0.300)
        planning_ts = safe_float((planning_row or {}).get("timestamp"))
        planning_header_ts = safe_float((planning_row or {}).get("planning_header_timestamp_sec"))
        first_rel = safe_float((planning_row or {}).get("trajectory_relative_time_min_sec"))
        last_rel = safe_float((planning_row or {}).get("trajectory_relative_time_max_sec"))
        now_rel = (ts - planning_header_ts) if planning_header_ts is not None else None
        reject_reason = _classify_reject_reason(ts, planning_row, recent_state)
        row = {
            "event_type": "control_no_trajectory",
            "timestamp": ts,
            "control_cycle_index": None,
            "latest_planning_msg_received": planning_row is not None,
            "latest_planning_msg_timestamp": planning_ts,
            "latest_planning_msg_age_ms": ((ts - planning_ts) * 1000.0) if planning_ts is not None else None,
            "latest_planning_msg_sequence_num": (
                int((planning_row or {}).get("planning_header_sequence_num"))
                if (planning_row or {}).get("planning_header_sequence_num") is not None
                else miss.get("planning_seq_num")
            ),
            "latest_planning_trajectory_point_count": int(
                (planning_row or {}).get("trajectory_point_count", 0) or 0
            ),
            "latest_planning_parse_success": bool(
                (planning_row or {}).get("planning_message_parsed_successfully", False)
            ),
            "trajectory_first_point_relative_time": first_rel,
            "trajectory_last_point_relative_time": last_rel,
            "control_now_relative_to_planning_header_sec": now_rel,
            "trajectory_all_points_expired": bool(now_rel is not None and last_rel is not None and now_rel > (last_rel + 0.05)),
            "trajectory_not_started_yet": bool(now_rel is not None and first_rel is not None and now_rel < (first_rel - 0.05)),
            "trajectory_time_window_valid": bool(
                now_rel is not None and first_rel is not None and last_rel is not None and first_rel - 0.05 <= now_rel <= last_rel + 0.05
            ),
            "control_used_planning_trajectory": False,
            "control_used_cached_trajectory": False,
            "control_had_no_trajectory": True,
            "reject_reason": reject_reason,
            "auto_drive_mode": (recent_state or {}).get("auto_drive_mode"),
            "engage_state": (recent_state or {}).get("engage_state"),
            "estop_flag": bool((planning_row or {}).get("estop")) or bool((recent_state or {}).get("estop_flag")),
            "chassis_speed_mps": safe_float((recent_state or {}).get("chassis_speed_mps")),
            "planning_header_sequence_num_used": miss.get("planning_seq_num"),
            "planning_header_timestamp_sec_used": planning_header_ts,
            "raw_line": miss.get("raw_line"),
        }
        events.append(row)

    events.sort(key=lambda row: float(row.get("timestamp") or 0.0))
    for index, row in enumerate(events, start=1):
        row["control_cycle_index"] = index

    no_traj = [row for row in events if row.get("event_type") == "control_no_trajectory"]
    reason_counts: Dict[str, int] = {}
    for row in no_traj:
        key = str(row.get("reject_reason") or "unknown")
        reason_counts[key] = int(reason_counts.get(key, 0)) + 1
    summary = {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "total_control_cycles": len(events),
        "total_no_trajectory_events": len(no_traj),
        "no_trajectory_by_reason": reason_counts,
        "total_used_planning_trajectory": sum(1 for row in events if bool(row.get("control_used_planning_trajectory"))),
        "total_used_cached_trajectory": sum(1 for row in events if bool(row.get("control_used_cached_trajectory"))),
        "expired_trajectory_events": int(reason_counts.get("trajectory_all_points_expired", 0)),
        "stale_planning_events": int(reason_counts.get("no_recent_planning_message", 0)),
        "zero_point_planning_events": int(reason_counts.get("zero_trajectory_points", 0)),
        "parse_fail_events": int(reason_counts.get("planning_parse_failed", 0)),
        "control_mode_not_ready_events": int(reason_counts.get("control_mode_not_ready", 0)),
        "estop_or_safety_guard_events": int(reason_counts.get("estop_or_safety_guard", 0)),
    }
    return events, summary


def _planning_summary_from_event_stream(planning_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    point_counts = [int(row.get("trajectory_point_count", 0) or 0) for row in planning_rows]
    nonzero_rows = [row for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) > 0]
    parse_fail_reasons: Dict[str, int] = {}
    for row in planning_rows:
        if bool(row.get("planning_message_parsed_successfully", False)):
            continue
        reason = str(row.get("parse_fail_reason") or "")
        if reason:
            parse_fail_reasons[reason] = int(parse_fail_reasons.get(reason, 0)) + 1
    parse_fail_topk = [
        {"reason": reason, "count": count}
        for reason, count in sorted(parse_fail_reasons.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]
    return {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "total_messages_received": len(planning_rows),
        "messages_with_nonzero_trajectory_points": len(nonzero_rows),
        "messages_with_zero_trajectory_points": sum(1 for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) <= 0),
        "max_trajectory_point_count": max(point_counts) if point_counts else 0,
        "mean_trajectory_point_count": (sum(point_counts) / float(len(point_counts))) if point_counts else 0.0,
        "first_nonzero_trajectory_timestamp": safe_float((nonzero_rows[0] or {}).get("timestamp")) if nonzero_rows else None,
        "parse_fail_count": sum(1 for row in planning_rows if not bool(row.get("planning_message_parsed_successfully", False))),
        "parse_fail_reasons_topk": parse_fail_topk,
        "last_trajectory_point_count": point_counts[-1] if point_counts else 0,
        "last_planning_header_sequence_num": safe_int((planning_rows[-1] or {}).get("planning_header_sequence_num")) if planning_rows else None,
    }


def _planning_nonzero_ratio_check(
    planning_rows: List[Dict[str, Any]],
    planning_summary: Mapping[str, Any],
    *,
    route_established_ts: float | None,
) -> Dict[str, Any]:
    source = "all_planning_messages"
    denominator = int(planning_summary.get("total_messages_received", 0) or 0)
    numerator = int(planning_summary.get("messages_with_nonzero_trajectory_points", 0) or 0)
    post_route_rows: list[Dict[str, Any]] = []
    if route_established_ts is not None:
        for row in planning_rows:
            ts = _route_health_event_ts(row)
            if ts is not None and float(ts) >= float(route_established_ts):
                post_route_rows.append(row)
        if post_route_rows:
            source = "post_route_established_planning_messages"
            denominator = len(post_route_rows)
            numerator = sum(
                1
                for row in post_route_rows
                if int(row.get("trajectory_point_count", 0) or 0) > 0
            )
    actual = (float(numerator) / float(denominator)) if denominator > 0 else None
    return {
        "actual": actual,
        "threshold": CORE_ACCEPTANCE_POLICY["planning_nonzero_ratio_min"],
        "source": source,
        "numerator": numerator,
        "denominator": denominator,
        "all_messages_numerator": int(planning_summary.get("messages_with_nonzero_trajectory_points", 0) or 0),
        "all_messages_denominator": int(planning_summary.get("total_messages_received", 0) or 0),
        "route_established_ts_sec": route_established_ts,
    }


def _route_health_event_ts(row: Mapping[str, Any]) -> float | None:
    for key in ("sim_time_sec", "sim_time", "ts_sec", "timestamp"):
        value = safe_float(row.get(key))
        if value is not None:
            return value
    return None


PLANNING_BRIDGE_CONTEXT_FIELDS: Sequence[str] = (
    "first_path_fallback_bridge_prev_normal_seq",
    "first_path_fallback_bridge_prev_normal_is_replan",
    "first_path_fallback_bridge_prev_normal_replan_reason_family",
    "first_path_fallback_bridge_prev_normal_point_count",
    "first_path_fallback_bridge_prev_normal_first_point_v",
    "first_path_fallback_bridge_prev_normal_total_path_length",
    "first_path_fallback_bridge_prev_normal_total_time",
    "first_path_fallback_bridge_prev_normal_routing_segment_count",
    "first_path_fallback_bridge_prev_normal_reference_line_count",
    "first_path_fallback_bridge_prev_normal_route_segment_count",
    "first_path_fallback_bridge_prev_normal_route_segment_total_length",
    "first_path_fallback_bridge_prev_normal_lane_follow_map_status",
    "first_path_fallback_bridge_prev_normal_reference_line_provider_status",
    "first_path_fallback_bridge_prev_normal_planning_empty_reason_guess",
    "first_path_fallback_bridge_prev_normal_path_end_like_condition",
    "first_path_fallback_bridge_prev_normal_current_lane_id",
    "first_path_fallback_bridge_prev_normal_last_reroute_timestamp",
    "first_path_fallback_bridge_route_segment_count_delta_from_prev_normal",
    "first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal",
    "first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal",
    "first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready",
    "first_path_fallback_bridge_unknown_seq",
    "first_path_fallback_bridge_unknown_seq_gap_to_fallback",
    "first_path_fallback_bridge_unknown_is_replan",
    "first_path_fallback_bridge_unknown_point_count",
    "first_path_fallback_bridge_unknown_total_path_length",
    "first_path_fallback_bridge_unknown_total_time",
    "first_path_fallback_bridge_unknown_routing_segment_count",
    "first_path_fallback_bridge_unknown_reference_line_count",
    "first_path_fallback_bridge_unknown_route_segment_count",
    "first_path_fallback_bridge_unknown_route_segment_total_length",
    "first_path_fallback_bridge_unknown_create_route_segments_status",
    "first_path_fallback_bridge_unknown_lane_follow_map_status",
    "first_path_fallback_bridge_unknown_lane_follow_map_inconsistent",
    "first_path_fallback_bridge_unknown_reference_line_provider_status",
    "first_path_fallback_bridge_unknown_planning_empty_reason_guess",
    "first_path_fallback_bridge_unknown_path_end_like_condition",
    "first_path_fallback_bridge_unknown_current_lane_id",
    "first_path_fallback_bridge_unknown_last_reroute_timestamp",
    "first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec",
    "first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal",
    "first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal",
    "first_path_fallback_bridge_occurs_on_reroute_propagation_frame",
    "first_path_fallback_bridge_likely_codepath_family",
    "first_path_fallback_bridge_likely_provider_failure_site",
    "first_path_fallback_bridge_empty_previous_seq",
    "first_path_fallback_bridge_empty_previous_seq_gap_from_unknown",
    "first_path_fallback_bridge_empty_previous_seq_gap_to_fallback",
    "first_path_fallback_bridge_empty_previous_point_count",
    "first_path_fallback_bridge_empty_previous_first_point_v",
    "first_path_fallback_bridge_empty_previous_total_path_length",
    "first_path_fallback_bridge_empty_previous_total_time",
    "first_path_fallback_bridge_empty_previous_routing_segment_count",
    "first_path_fallback_bridge_empty_previous_reference_line_count",
    "first_path_fallback_bridge_empty_previous_replan_reason",
    "first_path_fallback_trigger_last_reroute_timestamp",
    "first_path_fallback_trigger_planning_header_age_from_last_reroute_sec",
)


def _planning_bridge_context(planning_summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "first_path_fallback_bridge_prev_normal_seq": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_seq")
        ),
        "first_path_fallback_bridge_prev_normal_is_replan": safe_bool(
            planning_summary.get("first_path_fallback_bridge_prev_normal_is_replan")
        ),
        "first_path_fallback_bridge_prev_normal_replan_reason_family": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_replan_reason_family"
        ),
        "first_path_fallback_bridge_prev_normal_point_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_point_count")
        ),
        "first_path_fallback_bridge_prev_normal_first_point_v": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_first_point_v")
        ),
        "first_path_fallback_bridge_prev_normal_total_path_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_total_path_length")
        ),
        "first_path_fallback_bridge_prev_normal_total_time": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_total_time")
        ),
        "first_path_fallback_bridge_prev_normal_routing_segment_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_routing_segment_count")
        ),
        "first_path_fallback_bridge_prev_normal_reference_line_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_reference_line_count")
        ),
        "first_path_fallback_bridge_prev_normal_route_segment_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_prev_normal_route_segment_count")
        ),
        "first_path_fallback_bridge_prev_normal_route_segment_total_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_route_segment_total_length")
        ),
        "first_path_fallback_bridge_prev_normal_lane_follow_map_status": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_lane_follow_map_status"
        ),
        "first_path_fallback_bridge_prev_normal_reference_line_provider_status": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_reference_line_provider_status"
        ),
        "first_path_fallback_bridge_prev_normal_planning_empty_reason_guess": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_planning_empty_reason_guess"
        ),
        "first_path_fallback_bridge_prev_normal_path_end_like_condition": safe_bool(
            planning_summary.get("first_path_fallback_bridge_prev_normal_path_end_like_condition")
        ),
        "first_path_fallback_bridge_prev_normal_current_lane_id": planning_summary.get(
            "first_path_fallback_bridge_prev_normal_current_lane_id"
        ),
        "first_path_fallback_bridge_prev_normal_last_reroute_timestamp": safe_float(
            planning_summary.get("first_path_fallback_bridge_prev_normal_last_reroute_timestamp")
        ),
        "first_path_fallback_bridge_route_segment_count_delta_from_prev_normal": safe_int(
            planning_summary.get("first_path_fallback_bridge_route_segment_count_delta_from_prev_normal")
        ),
        "first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal": safe_float(
            planning_summary.get("first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal")
        ),
        "first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal")
        ),
        "first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready")
        ),
        "first_path_fallback_bridge_unknown_seq": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_seq")
        ),
        "first_path_fallback_bridge_unknown_seq_gap_to_fallback": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_seq_gap_to_fallback")
        ),
        "first_path_fallback_bridge_unknown_is_replan": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_is_replan")
        ),
        "first_path_fallback_bridge_unknown_point_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_point_count")
        ),
        "first_path_fallback_bridge_unknown_total_path_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_total_path_length")
        ),
        "first_path_fallback_bridge_unknown_total_time": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_total_time")
        ),
        "first_path_fallback_bridge_unknown_routing_segment_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_routing_segment_count")
        ),
        "first_path_fallback_bridge_unknown_reference_line_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_reference_line_count")
        ),
        "first_path_fallback_bridge_unknown_route_segment_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_unknown_route_segment_count")
        ),
        "first_path_fallback_bridge_unknown_route_segment_total_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_route_segment_total_length")
        ),
        "first_path_fallback_bridge_unknown_create_route_segments_status": planning_summary.get(
            "first_path_fallback_bridge_unknown_create_route_segments_status"
        ),
        "first_path_fallback_bridge_unknown_lane_follow_map_status": planning_summary.get(
            "first_path_fallback_bridge_unknown_lane_follow_map_status"
        ),
        "first_path_fallback_bridge_unknown_lane_follow_map_inconsistent": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_lane_follow_map_inconsistent")
        ),
        "first_path_fallback_bridge_unknown_reference_line_provider_status": planning_summary.get(
            "first_path_fallback_bridge_unknown_reference_line_provider_status"
        ),
        "first_path_fallback_bridge_unknown_planning_empty_reason_guess": planning_summary.get(
            "first_path_fallback_bridge_unknown_planning_empty_reason_guess"
        ),
        "first_path_fallback_bridge_unknown_path_end_like_condition": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_path_end_like_condition")
        ),
        "first_path_fallback_bridge_unknown_current_lane_id": planning_summary.get(
            "first_path_fallback_bridge_unknown_current_lane_id"
        ),
        "first_path_fallback_bridge_unknown_last_reroute_timestamp": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_last_reroute_timestamp")
        ),
        "first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec")
        ),
        "first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal": safe_float(
            planning_summary.get("first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal")
        ),
        "first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal": safe_bool(
            planning_summary.get("first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal")
        ),
        "first_path_fallback_bridge_occurs_on_reroute_propagation_frame": safe_bool(
            planning_summary.get("first_path_fallback_bridge_occurs_on_reroute_propagation_frame")
        ),
        "first_path_fallback_bridge_likely_codepath_family": planning_summary.get(
            "first_path_fallback_bridge_likely_codepath_family"
        ),
        "first_path_fallback_bridge_likely_provider_failure_site": planning_summary.get(
            "first_path_fallback_bridge_likely_provider_failure_site"
        ),
        "first_path_fallback_bridge_empty_previous_seq": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_seq")
        ),
        "first_path_fallback_bridge_empty_previous_seq_gap_from_unknown": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_seq_gap_from_unknown")
        ),
        "first_path_fallback_bridge_empty_previous_seq_gap_to_fallback": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_seq_gap_to_fallback")
        ),
        "first_path_fallback_bridge_empty_previous_point_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_point_count")
        ),
        "first_path_fallback_bridge_empty_previous_first_point_v": safe_float(
            planning_summary.get("first_path_fallback_bridge_empty_previous_first_point_v")
        ),
        "first_path_fallback_bridge_empty_previous_total_path_length": safe_float(
            planning_summary.get("first_path_fallback_bridge_empty_previous_total_path_length")
        ),
        "first_path_fallback_bridge_empty_previous_total_time": safe_float(
            planning_summary.get("first_path_fallback_bridge_empty_previous_total_time")
        ),
        "first_path_fallback_bridge_empty_previous_routing_segment_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_routing_segment_count")
        ),
        "first_path_fallback_bridge_empty_previous_reference_line_count": safe_int(
            planning_summary.get("first_path_fallback_bridge_empty_previous_reference_line_count")
        ),
        "first_path_fallback_bridge_empty_previous_replan_reason": planning_summary.get(
            "first_path_fallback_bridge_empty_previous_replan_reason"
        ),
        "first_path_fallback_trigger_last_reroute_timestamp": safe_float(
            planning_summary.get("first_path_fallback_trigger_last_reroute_timestamp")
        ),
        "first_path_fallback_trigger_planning_header_age_from_last_reroute_sec": safe_float(
            planning_summary.get("first_path_fallback_trigger_planning_header_age_from_last_reroute_sec")
        ),
    }


def _compute_planning_trajectory_type_summary(
    planning_rows: List[Dict[str, Any]],
    route_debug_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    type_counts: Dict[str, int] = {}
    first_path_fallback_row: Optional[Dict[str, Any]] = None
    first_path_fallback_prev_normal_row: Optional[Dict[str, Any]] = None
    first_path_fallback_trigger_row: Optional[Dict[str, Any]] = None
    first_path_fallback_trigger_index: Optional[int] = None
    first_path_fallback_precurrent_time_smaller_rows: List[Dict[str, Any]] = []
    first_path_fallback_bridge_prev_normal_row: Optional[Dict[str, Any]] = None
    first_path_fallback_bridge_prev_normal_route_row: Optional[Dict[str, Any]] = None
    first_path_fallback_bridge_unknown_row: Optional[Dict[str, Any]] = None
    first_path_fallback_bridge_unknown_index: Optional[int] = None
    first_path_fallback_bridge_unknown_route_row: Optional[Dict[str, Any]] = None
    first_path_fallback_bridge_empty_previous_row: Optional[Dict[str, Any]] = None
    first_recovery_after_path_fallback_row: Optional[Dict[str, Any]] = None
    first_relapse_after_recovery_row: Optional[Dict[str, Any]] = None
    first_relapse_prev_normal_row: Optional[Dict[str, Any]] = None
    last_row = planning_rows[-1] if planning_rows else None
    final_path_fallback_suffix_start_row: Optional[Dict[str, Any]] = None
    last_normal_before_final_path_fallback_row: Optional[Dict[str, Any]] = None
    final_path_fallback_suffix_length = 0
    max_consecutive_path_fallback_length = 0
    current_consecutive_path_fallback_length = 0
    final_normal_tail_length = 0
    running_final_normal_tail_length = 0
    path_fallback_count_after_first_recovery = 0
    max_consecutive_path_fallback_after_first_recovery = 0
    current_consecutive_path_fallback_after_first_recovery = 0
    normal_count_after_first_recovery = 0
    first_path_fallback_cluster_length = 0
    first_relapse_cluster_length = 0
    last_normal_row: Optional[Dict[str, Any]] = None
    tracking_first_path_fallback_cluster = False
    route_debug_by_seq: Dict[int, Dict[str, Any]] = {}
    for row in route_debug_rows or []:
        seq = safe_int(row.get("planning_header_sequence_num"))
        if seq is None or seq in route_debug_by_seq:
            continue
        route_debug_by_seq[seq] = row
    for index, row in enumerate(planning_rows):
        traj_type = str(row.get("trajectory_type") or "")
        if not traj_type:
            traj_type = "UNKNOWN"
        type_counts[traj_type] = int(type_counts.get(traj_type, 0)) + 1
        if traj_type == "PATH_FALLBACK" and first_path_fallback_row is None:
            first_path_fallback_row = row
            first_path_fallback_prev_normal_row = last_normal_row
            for candidate_index in range(index, max(-1, index - 4), -1):
                candidate = planning_rows[candidate_index]
                candidate_reason = str(candidate.get("replan_reason") or "").strip()
                if candidate_reason:
                    first_path_fallback_trigger_row = candidate
                    first_path_fallback_trigger_index = candidate_index
                    break
            precursor_index = index - 1
            precursor_rows: List[Dict[str, Any]] = []
            while precursor_index >= 0:
                candidate = planning_rows[precursor_index]
                if str(candidate.get("trajectory_type") or "") != "NORMAL":
                    break
                if classify_replan_reason(candidate.get("replan_reason")) != "current_time_smaller":
                    break
                precursor_rows.append(candidate)
                precursor_index -= 1
            first_path_fallback_precurrent_time_smaller_rows = list(reversed(precursor_rows))
            first_path_fallback_seq_hint = safe_int(row.get("planning_header_sequence_num"))
            for candidate_index in range(index - 1, -1, -1):
                candidate = planning_rows[candidate_index]
                candidate_seq = safe_int(candidate.get("planning_header_sequence_num"))
                if (
                    first_path_fallback_seq_hint is not None
                    and candidate_seq is not None
                    and int(first_path_fallback_seq_hint) - int(candidate_seq) > 15
                ):
                    break
                candidate_type = str(candidate.get("trajectory_type") or "").strip()
                candidate_point_count = safe_int(candidate.get("trajectory_point_count"))
                if candidate_type == "UNKNOWN" or (
                    candidate_point_count is not None and candidate_point_count <= 0
                ):
                    first_path_fallback_bridge_unknown_row = candidate
                    first_path_fallback_bridge_unknown_index = candidate_index
                    bridge_unknown_seq = safe_int(candidate.get("planning_header_sequence_num"))
                    if bridge_unknown_seq is not None:
                        first_path_fallback_bridge_unknown_route_row = route_debug_by_seq.get(bridge_unknown_seq)
                    break
            if first_path_fallback_bridge_unknown_index is not None:
                for candidate_index in range(first_path_fallback_bridge_unknown_index - 1, -1, -1):
                    candidate = planning_rows[candidate_index]
                    if str(candidate.get("trajectory_type") or "") != "NORMAL":
                        continue
                    first_path_fallback_bridge_prev_normal_row = candidate
                    bridge_prev_normal_seq = safe_int(candidate.get("planning_header_sequence_num"))
                    if bridge_prev_normal_seq is not None:
                        first_path_fallback_bridge_prev_normal_route_row = route_debug_by_seq.get(
                            bridge_prev_normal_seq
                        )
                    break
                for candidate_index in range(first_path_fallback_bridge_unknown_index + 1, index):
                    candidate = planning_rows[candidate_index]
                    if is_empty_previous_trajectory_replan(candidate.get("replan_reason")):
                        first_path_fallback_bridge_empty_previous_row = candidate
                        break
            tracking_first_path_fallback_cluster = True
        if traj_type == "PATH_FALLBACK":
            current_consecutive_path_fallback_length += 1
            max_consecutive_path_fallback_length = max(
                max_consecutive_path_fallback_length,
                current_consecutive_path_fallback_length,
            )
            if tracking_first_path_fallback_cluster:
                first_path_fallback_cluster_length += 1
            running_final_normal_tail_length = 0
        else:
            current_consecutive_path_fallback_length = 0
            if tracking_first_path_fallback_cluster:
                tracking_first_path_fallback_cluster = False
            if traj_type == "NORMAL":
                running_final_normal_tail_length += 1
                last_normal_row = row
            else:
                running_final_normal_tail_length = 0
    final_normal_tail_length = running_final_normal_tail_length

    last_type = str((last_row or {}).get("trajectory_type") or "") or None
    recovered_after_path_fallback = False
    if first_path_fallback_row is not None:
        seen_first = False
        for row in planning_rows:
            if row is first_path_fallback_row:
                seen_first = True
                continue
            if not seen_first:
                continue
            if str(row.get("trajectory_type") or "") == "NORMAL":
                recovered_after_path_fallback = True
                first_recovery_after_path_fallback_row = row
                break
    if first_recovery_after_path_fallback_row is not None:
        seen_recovery = False
        last_normal_after_recovery_row: Optional[Dict[str, Any]] = None
        tracking_first_relapse_cluster = False
        for row in planning_rows:
            if row is first_recovery_after_path_fallback_row:
                seen_recovery = True
            if not seen_recovery:
                continue
            traj_type = str(row.get("trajectory_type") or "")
            if traj_type == "PATH_FALLBACK":
                path_fallback_count_after_first_recovery += 1
                current_consecutive_path_fallback_after_first_recovery += 1
                max_consecutive_path_fallback_after_first_recovery = max(
                    max_consecutive_path_fallback_after_first_recovery,
                    current_consecutive_path_fallback_after_first_recovery,
                )
                if first_relapse_after_recovery_row is None:
                    first_relapse_after_recovery_row = row
                    first_relapse_prev_normal_row = last_normal_after_recovery_row
                    tracking_first_relapse_cluster = True
                if tracking_first_relapse_cluster:
                    first_relapse_cluster_length += 1
            else:
                current_consecutive_path_fallback_after_first_recovery = 0
                if tracking_first_relapse_cluster:
                    tracking_first_relapse_cluster = False
                if traj_type == "NORMAL":
                    normal_count_after_first_recovery += 1
                    last_normal_after_recovery_row = row

    first_path_fallback_prev_normal_total_path_length = safe_float(
        (first_path_fallback_prev_normal_row or {}).get("trajectory_total_path_length")
    )
    first_path_fallback_total_path_length = safe_float((first_path_fallback_row or {}).get("trajectory_total_path_length"))
    first_path_fallback_path_length_collapse_ratio = None
    if (
        first_path_fallback_prev_normal_total_path_length is not None
        and first_path_fallback_total_path_length is not None
        and first_path_fallback_prev_normal_total_path_length > 0.0
    ):
        first_path_fallback_path_length_collapse_ratio = (
            float(first_path_fallback_total_path_length) / float(first_path_fallback_prev_normal_total_path_length)
        )
    first_path_fallback_trigger_replan_reason = str((first_path_fallback_trigger_row or {}).get("replan_reason") or "").strip() or None
    first_path_fallback_trigger_reason_family = classify_replan_reason(first_path_fallback_trigger_replan_reason)
    first_path_fallback_trigger_seq = safe_int((first_path_fallback_trigger_row or {}).get("planning_header_sequence_num"))
    first_path_fallback_trigger_seq_gap = None
    first_path_fallback_seq = safe_int((first_path_fallback_row or {}).get("planning_header_sequence_num"))
    if first_path_fallback_seq is not None and first_path_fallback_trigger_seq is not None:
        first_path_fallback_trigger_seq_gap = int(first_path_fallback_seq) - int(first_path_fallback_trigger_seq)
    first_path_fallback_trigger_reason_streak_length = 0
    if first_path_fallback_trigger_index is not None and first_path_fallback_trigger_reason_family:
        streak_index = first_path_fallback_trigger_index
        while streak_index >= 0:
            candidate_family = classify_replan_reason(planning_rows[streak_index].get("replan_reason"))
            if candidate_family != first_path_fallback_trigger_reason_family:
                break
            first_path_fallback_trigger_reason_streak_length += 1
            streak_index -= 1
    first_relapse_prev_normal_total_path_length = safe_float(
        (first_relapse_prev_normal_row or {}).get("trajectory_total_path_length")
    )
    first_relapse_after_recovery_total_path_length = safe_float(
        (first_relapse_after_recovery_row or {}).get("trajectory_total_path_length")
    )
    first_relapse_path_length_collapse_ratio = None
    if (
        first_relapse_prev_normal_total_path_length is not None
        and first_relapse_after_recovery_total_path_length is not None
        and first_relapse_prev_normal_total_path_length > 0.0
    ):
        first_relapse_path_length_collapse_ratio = (
            float(first_relapse_after_recovery_total_path_length) / float(first_relapse_prev_normal_total_path_length)
        )
    first_path_fallback_replan_reason = str((first_path_fallback_row or {}).get("replan_reason") or "").strip() or None
    first_relapse_replan_reason = str((first_relapse_after_recovery_row or {}).get("replan_reason") or "").strip() or None
    first_path_fallback_precurrent_time_smaller_rel_min_values = [
        safe_float(row.get("trajectory_relative_time_min_sec"))
        for row in first_path_fallback_precurrent_time_smaller_rows
        if safe_float(row.get("trajectory_relative_time_min_sec")) is not None
    ]
    first_path_fallback_precurrent_time_smaller_first_point_v_values = [
        safe_float(row.get("first_trajectory_point_v"))
        for row in first_path_fallback_precurrent_time_smaller_rows
        if safe_float(row.get("first_trajectory_point_v")) is not None
    ]
    first_path_fallback_precurrent_time_smaller_point_count_values = [
        safe_int(row.get("trajectory_point_count"))
        for row in first_path_fallback_precurrent_time_smaller_rows
        if safe_int(row.get("trajectory_point_count")) is not None
    ]
    first_path_fallback_precurrent_time_smaller_path_length_values = [
        safe_float(row.get("trajectory_total_path_length"))
        for row in first_path_fallback_precurrent_time_smaller_rows
        if safe_float(row.get("trajectory_total_path_length")) is not None
    ]
    published_trajectory_rows = [
        row
        for row in planning_rows
        if str(row.get("trajectory_type") or "").strip()
        and str(row.get("trajectory_type") or "").strip() != "UNKNOWN"
        and (safe_int(row.get("trajectory_point_count")) or 0) > 0
    ]
    published_trajectory_path_lengths = [
        safe_float(row.get("trajectory_total_path_length"))
        for row in published_trajectory_rows
        if safe_float(row.get("trajectory_total_path_length")) is not None
    ]
    short_published_trajectory_under_20m_count = sum(
        1 for value in published_trajectory_path_lengths if float(value) < 20.0
    )
    planning_debug_path_fallback_used_count = sum(
        1 for row in planning_rows if safe_bool(row.get("planning_debug_path_fallback_used")) is True
    )
    normal_with_debug_path_fallback_used_count = sum(
        1
        for row in planning_rows
        if str(row.get("trajectory_type") or "").strip() == "NORMAL"
        and safe_bool(row.get("planning_debug_path_fallback_used")) is True
    )
    debug_path_fallback_without_published_path_fallback = bool(
        planning_debug_path_fallback_used_count > 0 and type_counts.get("PATH_FALLBACK", 0) == 0
    )
    first_path_fallback_bridge_prev_normal_is_replan = safe_bool(
        (first_path_fallback_bridge_prev_normal_row or {}).get("is_replan")
    )
    first_path_fallback_bridge_prev_normal_replan_reason_family = classify_replan_reason(
        (first_path_fallback_bridge_prev_normal_row or {}).get("replan_reason")
    )
    first_path_fallback_bridge_prev_normal_total_time = safe_float(
        (first_path_fallback_bridge_prev_normal_row or {}).get("trajectory_total_time")
    )
    first_path_fallback_bridge_prev_normal_routing_segment_count = safe_int(
        (first_path_fallback_bridge_prev_normal_row or {}).get("routing_segment_count")
    )
    first_path_fallback_bridge_prev_normal_reference_line_count = safe_int(
        (first_path_fallback_bridge_prev_normal_row or {}).get("reference_line_count")
    )
    first_path_fallback_bridge_unknown_is_replan = safe_bool(
        (first_path_fallback_bridge_unknown_row or {}).get("is_replan")
    )
    first_path_fallback_bridge_unknown_total_time = safe_float(
        (first_path_fallback_bridge_unknown_row or {}).get("trajectory_total_time")
    )
    first_path_fallback_bridge_unknown_routing_segment_count = safe_int(
        (first_path_fallback_bridge_unknown_row or {}).get("routing_segment_count")
    )
    first_path_fallback_bridge_unknown_reference_line_count = safe_int(
        (first_path_fallback_bridge_unknown_row or {}).get("reference_line_count")
    )
    first_path_fallback_bridge_empty_previous_total_time = safe_float(
        (first_path_fallback_bridge_empty_previous_row or {}).get("trajectory_total_time")
    )
    first_path_fallback_bridge_empty_previous_routing_segment_count = safe_int(
        (first_path_fallback_bridge_empty_previous_row or {}).get("routing_segment_count")
    )
    first_path_fallback_bridge_empty_previous_reference_line_count = safe_int(
        (first_path_fallback_bridge_empty_previous_row or {}).get("reference_line_count")
    )
    if planning_rows and last_type == "PATH_FALLBACK":
        final_start_index = len(planning_rows) - 1
        while final_start_index > 0 and str(planning_rows[final_start_index - 1].get("trajectory_type") or "") == "PATH_FALLBACK":
            final_start_index -= 1
        final_path_fallback_suffix_start_row = planning_rows[final_start_index]
        final_path_fallback_suffix_length = len(planning_rows) - final_start_index
        for row in reversed(planning_rows[:final_start_index]):
            if str(row.get("trajectory_type") or "") == "NORMAL":
                last_normal_before_final_path_fallback_row = row
                break

    first_path_fallback_bridge_unknown_seq = safe_int(
        (first_path_fallback_bridge_unknown_row or {}).get("planning_header_sequence_num")
    )
    first_path_fallback_bridge_unknown_seq_gap_to_fallback = None
    if first_path_fallback_bridge_unknown_seq is not None and first_path_fallback_seq is not None:
        first_path_fallback_bridge_unknown_seq_gap_to_fallback = (
            int(first_path_fallback_seq) - int(first_path_fallback_bridge_unknown_seq)
        )
    first_path_fallback_bridge_empty_previous_seq = safe_int(
        (first_path_fallback_bridge_empty_previous_row or {}).get("planning_header_sequence_num")
    )
    first_path_fallback_bridge_empty_previous_seq_gap_from_unknown = None
    if (
        first_path_fallback_bridge_unknown_seq is not None
        and first_path_fallback_bridge_empty_previous_seq is not None
    ):
        first_path_fallback_bridge_empty_previous_seq_gap_from_unknown = (
            int(first_path_fallback_bridge_empty_previous_seq) - int(first_path_fallback_bridge_unknown_seq)
        )
    first_path_fallback_bridge_empty_previous_seq_gap_to_fallback = None
    if first_path_fallback_bridge_empty_previous_seq is not None and first_path_fallback_seq is not None:
        first_path_fallback_bridge_empty_previous_seq_gap_to_fallback = (
            int(first_path_fallback_seq) - int(first_path_fallback_bridge_empty_previous_seq)
        )
    first_path_fallback_bridge_prev_normal_last_reroute_timestamp = safe_float(
        (first_path_fallback_bridge_prev_normal_route_row or {}).get("last_reroute_timestamp")
    )
    first_path_fallback_bridge_unknown_last_reroute_timestamp = safe_float(
        (first_path_fallback_bridge_unknown_route_row or {}).get("last_reroute_timestamp")
    )
    first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec = None
    first_path_fallback_bridge_unknown_planning_header_timestamp_sec = safe_float(
        (first_path_fallback_bridge_unknown_route_row or {}).get("planning_header_timestamp_sec")
    )
    if (
        first_path_fallback_bridge_unknown_last_reroute_timestamp is not None
        and first_path_fallback_bridge_unknown_planning_header_timestamp_sec is not None
    ):
        first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec = (
            float(first_path_fallback_bridge_unknown_planning_header_timestamp_sec)
            - float(first_path_fallback_bridge_unknown_last_reroute_timestamp)
        )
    first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal = None
    if (
        first_path_fallback_bridge_prev_normal_last_reroute_timestamp is not None
        and first_path_fallback_bridge_unknown_last_reroute_timestamp is not None
    ):
        first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal = (
            float(first_path_fallback_bridge_unknown_last_reroute_timestamp)
            - float(first_path_fallback_bridge_prev_normal_last_reroute_timestamp)
        )
    first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal = None
    if (
        first_path_fallback_bridge_prev_normal_last_reroute_timestamp is not None
        and first_path_fallback_bridge_unknown_last_reroute_timestamp is not None
    ):
        first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal = bool(
            abs(
                float(first_path_fallback_bridge_unknown_last_reroute_timestamp)
                - float(first_path_fallback_bridge_prev_normal_last_reroute_timestamp)
            )
            > 1e-6
        )
    first_path_fallback_bridge_prev_normal_route_segment_count = safe_int(
        (first_path_fallback_bridge_prev_normal_route_row or {}).get("route_segment_count")
    )
    first_path_fallback_bridge_unknown_route_segment_count = safe_int(
        (first_path_fallback_bridge_unknown_route_row or {}).get("route_segment_count")
    )
    first_path_fallback_bridge_route_segment_count_delta_from_prev_normal = None
    if (
        first_path_fallback_bridge_prev_normal_route_segment_count is not None
        and first_path_fallback_bridge_unknown_route_segment_count is not None
    ):
        first_path_fallback_bridge_route_segment_count_delta_from_prev_normal = (
            int(first_path_fallback_bridge_unknown_route_segment_count)
            - int(first_path_fallback_bridge_prev_normal_route_segment_count)
        )
    first_path_fallback_bridge_prev_normal_route_segment_total_length = safe_float(
        (first_path_fallback_bridge_prev_normal_route_row or {}).get("route_segment_total_length")
    )
    first_path_fallback_bridge_unknown_route_segment_total_length = safe_float(
        (first_path_fallback_bridge_unknown_route_row or {}).get("route_segment_total_length")
    )
    first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal = None
    if (
        first_path_fallback_bridge_prev_normal_route_segment_total_length is not None
        and first_path_fallback_bridge_unknown_route_segment_total_length is not None
    ):
        first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal = (
            float(first_path_fallback_bridge_unknown_route_segment_total_length)
            - float(first_path_fallback_bridge_prev_normal_route_segment_total_length)
        )
    first_path_fallback_bridge_prev_normal_current_lane_id = (
        first_path_fallback_bridge_prev_normal_route_row or {}
    ).get("current_lane_id")
    first_path_fallback_bridge_unknown_current_lane_id = (
        first_path_fallback_bridge_unknown_route_row or {}
    ).get("current_lane_id")
    first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal = None
    if (
        first_path_fallback_bridge_prev_normal_row is not None
        and first_path_fallback_bridge_unknown_row is not None
    ):
        first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal = bool(
            str(first_path_fallback_bridge_prev_normal_current_lane_id or "").strip()
            and not str(first_path_fallback_bridge_unknown_current_lane_id or "").strip()
        )
    first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready = None
    if first_path_fallback_bridge_unknown_row is not None:
        first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready = bool(
            str((first_path_fallback_bridge_unknown_route_row or {}).get("create_route_segments_status") or "").strip()
            == "ready"
            and str((first_path_fallback_bridge_unknown_route_row or {}).get("reference_line_provider_status") or "").strip()
            == "failed"
        )
    first_path_fallback_bridge_occurs_on_reroute_propagation_frame = None
    if first_path_fallback_bridge_unknown_row is not None:
        first_path_fallback_bridge_occurs_on_reroute_propagation_frame = bool(
            first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal is True
            and first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready is True
            and first_path_fallback_bridge_empty_previous_seq is not None
        )
    first_path_fallback_bridge_likely_codepath_family = None
    if (
        first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready is True
        and first_path_fallback_bridge_empty_previous_seq is not None
    ):
        if first_path_fallback_bridge_occurs_on_reroute_propagation_frame is True:
            first_path_fallback_bridge_likely_codepath_family = (
                "reroute_landing_reference_line_empty_output"
            )
        else:
            first_path_fallback_bridge_likely_codepath_family = (
                "reference_line_empty_output"
            )
    first_path_fallback_bridge_likely_provider_failure_site = None
    if (
        first_path_fallback_bridge_likely_codepath_family
        == "reroute_landing_reference_line_empty_output"
    ):
        first_path_fallback_bridge_likely_provider_failure_site = (
            "create_reference_line_new_command_empty_output"
        )
    elif (
        first_path_fallback_bridge_likely_codepath_family
        == "reference_line_empty_output"
    ):
        first_path_fallback_bridge_likely_provider_failure_site = (
            "create_reference_line_empty_output"
        )
    first_path_fallback_trigger_route_row = route_debug_by_seq.get(first_path_fallback_trigger_seq or -1)
    first_path_fallback_trigger_last_reroute_timestamp = safe_float(
        (first_path_fallback_trigger_route_row or {}).get("last_reroute_timestamp")
    )
    first_path_fallback_trigger_planning_header_age_from_last_reroute_sec = None
    first_path_fallback_trigger_planning_header_timestamp_sec = safe_float(
        (first_path_fallback_trigger_route_row or {}).get("planning_header_timestamp_sec")
    )
    if (
        first_path_fallback_trigger_last_reroute_timestamp is not None
        and first_path_fallback_trigger_planning_header_timestamp_sec is not None
    ):
        first_path_fallback_trigger_planning_header_age_from_last_reroute_sec = (
            float(first_path_fallback_trigger_planning_header_timestamp_sec)
            - float(first_path_fallback_trigger_last_reroute_timestamp)
        )

    return {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "trajectory_type_counts": type_counts,
        "path_fallback_count": int(type_counts.get("PATH_FALLBACK", 0)),
        "speed_fallback_count": int(type_counts.get("SPEED_FALLBACK", 0)),
        "published_trajectory_count": int(len(published_trajectory_rows)),
        "short_published_trajectory_under_20m_count": int(short_published_trajectory_under_20m_count),
        "published_trajectory_path_length_min": (
            min(published_trajectory_path_lengths) if published_trajectory_path_lengths else None
        ),
        "published_trajectory_path_length_p50": _interpolated_percentile(
            published_trajectory_path_lengths,
            0.50,
        )
        if published_trajectory_path_lengths
        else None,
        "planning_debug_path_fallback_used_count": int(planning_debug_path_fallback_used_count),
        "normal_with_debug_path_fallback_used_count": int(normal_with_debug_path_fallback_used_count),
        "debug_path_fallback_without_published_path_fallback": debug_path_fallback_without_published_path_fallback,
        "first_path_fallback_seq": first_path_fallback_seq,
        "first_path_fallback_timestamp": safe_float((first_path_fallback_row or {}).get("timestamp")),
        "first_path_fallback_point_count": safe_int((first_path_fallback_row or {}).get("trajectory_point_count")),
        "first_path_fallback_first_point_v": safe_float((first_path_fallback_row or {}).get("first_trajectory_point_v")),
        "first_path_fallback_total_path_length": first_path_fallback_total_path_length,
        "first_path_fallback_replan_reason": first_path_fallback_replan_reason,
        "first_path_fallback_lon_diff": extract_lon_diff(first_path_fallback_replan_reason),
        "first_path_fallback_trigger_seq": first_path_fallback_trigger_seq,
        "first_path_fallback_trigger_timestamp": safe_float((first_path_fallback_trigger_row or {}).get("timestamp")),
        "first_path_fallback_trigger_seq_gap": first_path_fallback_trigger_seq_gap,
        "first_path_fallback_trigger_replan_reason": first_path_fallback_trigger_replan_reason,
        "first_path_fallback_trigger_reason_family": first_path_fallback_trigger_reason_family,
        "first_path_fallback_trigger_reason_streak_length": int(first_path_fallback_trigger_reason_streak_length),
        "first_path_fallback_trigger_lon_diff": extract_lon_diff(first_path_fallback_trigger_replan_reason),
        "first_path_fallback_trigger_relative_time_min_sec": safe_float(
            (first_path_fallback_trigger_row or {}).get("trajectory_relative_time_min_sec")
        ),
        "first_path_fallback_trigger_last_reroute_timestamp": first_path_fallback_trigger_last_reroute_timestamp,
        "first_path_fallback_trigger_planning_header_age_from_last_reroute_sec": (
            first_path_fallback_trigger_planning_header_age_from_last_reroute_sec
        ),
        "first_path_fallback_trigger_first_point_v": safe_float(
            (first_path_fallback_trigger_row or {}).get("first_trajectory_point_v")
        ),
        "first_path_fallback_trigger_total_path_length": safe_float(
            (first_path_fallback_trigger_row or {}).get("trajectory_total_path_length")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_count": int(
            len(first_path_fallback_precurrent_time_smaller_rows)
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_start": safe_int(
            (first_path_fallback_precurrent_time_smaller_rows[0] or {}).get("planning_header_sequence_num")
        )
        if first_path_fallback_precurrent_time_smaller_rows
        else None,
        "first_path_fallback_precurrent_time_smaller_normal_seq_end": safe_int(
            (first_path_fallback_precurrent_time_smaller_rows[-1] or {}).get("planning_header_sequence_num")
        )
        if first_path_fallback_precurrent_time_smaller_rows
        else None,
        "first_path_fallback_precurrent_time_smaller_rel_min_min": (
            min(first_path_fallback_precurrent_time_smaller_rel_min_values)
            if first_path_fallback_precurrent_time_smaller_rel_min_values
            else None
        ),
        "first_path_fallback_precurrent_time_smaller_rel_min_max": (
            max(first_path_fallback_precurrent_time_smaller_rel_min_values)
            if first_path_fallback_precurrent_time_smaller_rel_min_values
            else None
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_min": (
            min(first_path_fallback_precurrent_time_smaller_first_point_v_values)
            if first_path_fallback_precurrent_time_smaller_first_point_v_values
            else None
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_max": (
            max(first_path_fallback_precurrent_time_smaller_first_point_v_values)
            if first_path_fallback_precurrent_time_smaller_first_point_v_values
            else None
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_min": (
            min(first_path_fallback_precurrent_time_smaller_point_count_values)
            if first_path_fallback_precurrent_time_smaller_point_count_values
            else None
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_max": (
            max(first_path_fallback_precurrent_time_smaller_point_count_values)
            if first_path_fallback_precurrent_time_smaller_point_count_values
            else None
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_min": (
            min(first_path_fallback_precurrent_time_smaller_path_length_values)
            if first_path_fallback_precurrent_time_smaller_path_length_values
            else None
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_max": (
            max(first_path_fallback_precurrent_time_smaller_path_length_values)
            if first_path_fallback_precurrent_time_smaller_path_length_values
            else None
        ),
        "first_path_fallback_bridge_prev_normal_seq": safe_int(
            (first_path_fallback_bridge_prev_normal_row or {}).get("planning_header_sequence_num")
        ),
        "first_path_fallback_bridge_prev_normal_is_replan": first_path_fallback_bridge_prev_normal_is_replan,
        "first_path_fallback_bridge_prev_normal_replan_reason_family": (
            first_path_fallback_bridge_prev_normal_replan_reason_family
        ),
        "first_path_fallback_bridge_prev_normal_point_count": safe_int(
            (first_path_fallback_bridge_prev_normal_row or {}).get("trajectory_point_count")
        ),
        "first_path_fallback_bridge_prev_normal_first_point_v": safe_float(
            (first_path_fallback_bridge_prev_normal_row or {}).get("first_trajectory_point_v")
        ),
        "first_path_fallback_bridge_prev_normal_total_path_length": safe_float(
            (first_path_fallback_bridge_prev_normal_row or {}).get("trajectory_total_path_length")
        ),
        "first_path_fallback_bridge_prev_normal_total_time": first_path_fallback_bridge_prev_normal_total_time,
        "first_path_fallback_bridge_prev_normal_routing_segment_count": (
            first_path_fallback_bridge_prev_normal_routing_segment_count
        ),
        "first_path_fallback_bridge_prev_normal_reference_line_count": (
            first_path_fallback_bridge_prev_normal_reference_line_count
        ),
        "first_path_fallback_bridge_prev_normal_route_segment_count": first_path_fallback_bridge_prev_normal_route_segment_count,
        "first_path_fallback_bridge_prev_normal_route_segment_total_length": first_path_fallback_bridge_prev_normal_route_segment_total_length,
        "first_path_fallback_bridge_prev_normal_lane_follow_map_status": (
            first_path_fallback_bridge_prev_normal_route_row or {}
        ).get("lane_follow_map_status"),
        "first_path_fallback_bridge_prev_normal_reference_line_provider_status": (
            first_path_fallback_bridge_prev_normal_route_row or {}
        ).get("reference_line_provider_status"),
        "first_path_fallback_bridge_prev_normal_planning_empty_reason_guess": (
            first_path_fallback_bridge_prev_normal_route_row or {}
        ).get("planning_empty_reason_guess"),
        "first_path_fallback_bridge_prev_normal_path_end_like_condition": safe_bool(
            (first_path_fallback_bridge_prev_normal_route_row or {}).get("path_end_like_condition")
        ),
        "first_path_fallback_bridge_prev_normal_current_lane_id": first_path_fallback_bridge_prev_normal_current_lane_id,
        "first_path_fallback_bridge_prev_normal_last_reroute_timestamp": (
            first_path_fallback_bridge_prev_normal_last_reroute_timestamp
        ),
        "first_path_fallback_bridge_route_segment_count_delta_from_prev_normal": (
            first_path_fallback_bridge_route_segment_count_delta_from_prev_normal
        ),
        "first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal": (
            first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal
        ),
        "first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal": (
            first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal
        ),
        "first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready": (
            first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready
        ),
        "first_path_fallback_bridge_unknown_seq": first_path_fallback_bridge_unknown_seq,
        "first_path_fallback_bridge_unknown_seq_gap_to_fallback": first_path_fallback_bridge_unknown_seq_gap_to_fallback,
        "first_path_fallback_bridge_unknown_is_replan": first_path_fallback_bridge_unknown_is_replan,
        "first_path_fallback_bridge_unknown_point_count": safe_int(
            (first_path_fallback_bridge_unknown_row or {}).get("trajectory_point_count")
        ),
        "first_path_fallback_bridge_unknown_total_path_length": safe_float(
            (first_path_fallback_bridge_unknown_row or {}).get("trajectory_total_path_length")
        ),
        "first_path_fallback_bridge_unknown_total_time": first_path_fallback_bridge_unknown_total_time,
        "first_path_fallback_bridge_unknown_routing_segment_count": (
            first_path_fallback_bridge_unknown_routing_segment_count
        ),
        "first_path_fallback_bridge_unknown_reference_line_count": (
            first_path_fallback_bridge_unknown_reference_line_count
        ),
        "first_path_fallback_bridge_unknown_route_segment_count": first_path_fallback_bridge_unknown_route_segment_count,
        "first_path_fallback_bridge_unknown_route_segment_total_length": first_path_fallback_bridge_unknown_route_segment_total_length,
        "first_path_fallback_bridge_unknown_create_route_segments_status": (
            first_path_fallback_bridge_unknown_route_row or {}
        ).get("create_route_segments_status"),
        "first_path_fallback_bridge_unknown_lane_follow_map_status": (
            first_path_fallback_bridge_unknown_route_row or {}
        ).get("lane_follow_map_status"),
        "first_path_fallback_bridge_unknown_lane_follow_map_inconsistent": safe_bool(
            (first_path_fallback_bridge_unknown_route_row or {}).get("lane_follow_map_inconsistent")
        ),
        "first_path_fallback_bridge_unknown_reference_line_provider_status": (
            first_path_fallback_bridge_unknown_route_row or {}
        ).get("reference_line_provider_status"),
        "first_path_fallback_bridge_unknown_planning_empty_reason_guess": (
            first_path_fallback_bridge_unknown_route_row or {}
        ).get("planning_empty_reason_guess"),
        "first_path_fallback_bridge_unknown_path_end_like_condition": safe_bool(
            (first_path_fallback_bridge_unknown_route_row or {}).get("path_end_like_condition")
        ),
        "first_path_fallback_bridge_unknown_current_lane_id": first_path_fallback_bridge_unknown_current_lane_id,
        "first_path_fallback_bridge_unknown_last_reroute_timestamp": (
            first_path_fallback_bridge_unknown_last_reroute_timestamp
        ),
        "first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec": (
            first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec
        ),
        "first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal": (
            first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal
        ),
        "first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal": (
            first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal
        ),
        "first_path_fallback_bridge_occurs_on_reroute_propagation_frame": (
            first_path_fallback_bridge_occurs_on_reroute_propagation_frame
        ),
        "first_path_fallback_bridge_likely_codepath_family": (
            first_path_fallback_bridge_likely_codepath_family
        ),
        "first_path_fallback_bridge_likely_provider_failure_site": (
            first_path_fallback_bridge_likely_provider_failure_site
        ),
        "first_path_fallback_bridge_empty_previous_seq": first_path_fallback_bridge_empty_previous_seq,
        "first_path_fallback_bridge_empty_previous_seq_gap_from_unknown": (
            first_path_fallback_bridge_empty_previous_seq_gap_from_unknown
        ),
        "first_path_fallback_bridge_empty_previous_seq_gap_to_fallback": (
            first_path_fallback_bridge_empty_previous_seq_gap_to_fallback
        ),
        "first_path_fallback_bridge_empty_previous_point_count": safe_int(
            (first_path_fallback_bridge_empty_previous_row or {}).get("trajectory_point_count")
        ),
        "first_path_fallback_bridge_empty_previous_first_point_v": safe_float(
            (first_path_fallback_bridge_empty_previous_row or {}).get("first_trajectory_point_v")
        ),
        "first_path_fallback_bridge_empty_previous_total_path_length": safe_float(
            (first_path_fallback_bridge_empty_previous_row or {}).get("trajectory_total_path_length")
        ),
        "first_path_fallback_bridge_empty_previous_total_time": first_path_fallback_bridge_empty_previous_total_time,
        "first_path_fallback_bridge_empty_previous_routing_segment_count": (
            first_path_fallback_bridge_empty_previous_routing_segment_count
        ),
        "first_path_fallback_bridge_empty_previous_reference_line_count": (
            first_path_fallback_bridge_empty_previous_reference_line_count
        ),
        "first_path_fallback_bridge_empty_previous_replan_reason": (
            str((first_path_fallback_bridge_empty_previous_row or {}).get("replan_reason") or "").strip() or None
        ),
        "first_path_fallback_prev_normal_seq": safe_int(
            (first_path_fallback_prev_normal_row or {}).get("planning_header_sequence_num")
        ),
        "first_path_fallback_prev_normal_timestamp": safe_float((first_path_fallback_prev_normal_row or {}).get("timestamp")),
        "first_path_fallback_prev_normal_point_count": safe_int(
            (first_path_fallback_prev_normal_row or {}).get("trajectory_point_count")
        ),
        "first_path_fallback_prev_normal_relative_time_min_sec": safe_float(
            (first_path_fallback_prev_normal_row or {}).get("trajectory_relative_time_min_sec")
        ),
        "first_path_fallback_prev_normal_first_point_v": safe_float(
            (first_path_fallback_prev_normal_row or {}).get("first_trajectory_point_v")
        ),
        "first_path_fallback_prev_normal_total_path_length": first_path_fallback_prev_normal_total_path_length,
        "first_path_fallback_cluster_length": int(first_path_fallback_cluster_length),
        "first_path_fallback_path_length_collapse_ratio": first_path_fallback_path_length_collapse_ratio,
        "first_recovery_after_path_fallback_seq": safe_int(
            (first_recovery_after_path_fallback_row or {}).get("planning_header_sequence_num")
        ),
        "first_recovery_after_path_fallback_timestamp": safe_float(
            (first_recovery_after_path_fallback_row or {}).get("timestamp")
        ),
        "first_recovery_after_path_fallback_point_count": safe_int(
            (first_recovery_after_path_fallback_row or {}).get("trajectory_point_count")
        ),
        "first_recovery_after_path_fallback_first_point_v": safe_float(
            (first_recovery_after_path_fallback_row or {}).get("first_trajectory_point_v")
        ),
        "first_recovery_after_path_fallback_total_path_length": safe_float(
            (first_recovery_after_path_fallback_row or {}).get("trajectory_total_path_length")
        ),
        "first_relapse_after_recovery_seq": safe_int((first_relapse_after_recovery_row or {}).get("planning_header_sequence_num")),
        "first_relapse_after_recovery_timestamp": safe_float((first_relapse_after_recovery_row or {}).get("timestamp")),
        "first_relapse_after_recovery_point_count": safe_int(
            (first_relapse_after_recovery_row or {}).get("trajectory_point_count")
        ),
        "first_relapse_after_recovery_first_point_v": safe_float(
            (first_relapse_after_recovery_row or {}).get("first_trajectory_point_v")
        ),
        "first_relapse_after_recovery_total_path_length": first_relapse_after_recovery_total_path_length,
        "first_relapse_after_recovery_replan_reason": first_relapse_replan_reason,
        "first_relapse_after_recovery_reason_family": classify_replan_reason(first_relapse_replan_reason),
        "first_relapse_after_recovery_lon_diff": extract_lon_diff(first_relapse_replan_reason),
        "first_relapse_prev_normal_seq": safe_int((first_relapse_prev_normal_row or {}).get("planning_header_sequence_num")),
        "first_relapse_prev_normal_timestamp": safe_float((first_relapse_prev_normal_row or {}).get("timestamp")),
        "first_relapse_prev_normal_point_count": safe_int(
            (first_relapse_prev_normal_row or {}).get("trajectory_point_count")
        ),
        "first_relapse_prev_normal_first_point_v": safe_float(
            (first_relapse_prev_normal_row or {}).get("first_trajectory_point_v")
        ),
        "first_relapse_prev_normal_total_path_length": first_relapse_prev_normal_total_path_length,
        "first_relapse_cluster_length": int(first_relapse_cluster_length),
        "first_relapse_path_length_collapse_ratio": first_relapse_path_length_collapse_ratio,
        "path_fallback_count_after_first_recovery": int(path_fallback_count_after_first_recovery),
        "max_consecutive_path_fallback_after_first_recovery": int(
            max_consecutive_path_fallback_after_first_recovery
        ),
        "normal_count_after_first_recovery": int(normal_count_after_first_recovery),
        "reentered_path_fallback_after_recovery": bool(first_relapse_after_recovery_row is not None),
        "max_consecutive_path_fallback_length": int(max_consecutive_path_fallback_length),
        "final_path_fallback_suffix_start_seq": safe_int(
            (final_path_fallback_suffix_start_row or {}).get("planning_header_sequence_num")
        ),
        "final_path_fallback_suffix_start_timestamp": safe_float(
            (final_path_fallback_suffix_start_row or {}).get("timestamp")
        ),
        "final_path_fallback_suffix_start_point_count": safe_int(
            (final_path_fallback_suffix_start_row or {}).get("trajectory_point_count")
        ),
        "final_path_fallback_suffix_start_first_point_v": safe_float(
            (final_path_fallback_suffix_start_row or {}).get("first_trajectory_point_v")
        ),
        "final_path_fallback_suffix_start_total_path_length": safe_float(
            (final_path_fallback_suffix_start_row or {}).get("trajectory_total_path_length")
        ),
        "final_path_fallback_suffix_length": int(final_path_fallback_suffix_length),
        "last_normal_before_final_path_fallback_seq": safe_int(
            (last_normal_before_final_path_fallback_row or {}).get("planning_header_sequence_num")
        ),
        "last_normal_before_final_path_fallback_timestamp": safe_float(
            (last_normal_before_final_path_fallback_row or {}).get("timestamp")
        ),
        "last_normal_before_final_path_fallback_point_count": safe_int(
            (last_normal_before_final_path_fallback_row or {}).get("trajectory_point_count")
        ),
        "last_normal_before_final_path_fallback_first_point_v": safe_float(
            (last_normal_before_final_path_fallback_row or {}).get("first_trajectory_point_v")
        ),
        "last_normal_before_final_path_fallback_total_path_length": safe_float(
            (last_normal_before_final_path_fallback_row or {}).get("trajectory_total_path_length")
        ),
        "last_trajectory_type": last_type,
        "last_trajectory_point_count": safe_int((last_row or {}).get("trajectory_point_count")),
        "last_trajectory_first_point_v": safe_float((last_row or {}).get("first_trajectory_point_v")),
        "last_trajectory_total_path_length": safe_float((last_row or {}).get("trajectory_total_path_length")),
        "final_normal_tail_length": int(final_normal_tail_length),
        "recovered_after_path_fallback": recovered_after_path_fallback,
        "persistent_path_fallback_at_end": bool(last_type == "PATH_FALLBACK"),
    }


def _compute_planning_control_alignment(
    planning_rows: List[Dict[str, Any]],
    live_control_rows: List[Dict[str, Any]],
    control_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    nonzero_planning_rows = [row for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) > 0]
    live_rows = _normalize_live_control_rows(live_control_rows)
    live_nonzero_rows = [
        row for row in live_rows if int(row.get("latest_planning_trajectory_point_count", 0) or 0) > 0
    ]
    no_traj_after_first_nonzero = 0
    first_nonzero_ts = safe_float((nonzero_planning_rows[0] or {}).get("timestamp")) if nonzero_planning_rows else None
    if first_nonzero_ts is not None:
        no_traj_after_first_nonzero = sum(
            1
            for row in control_events
            if row.get("event_type") == "control_no_trajectory"
            and safe_float(row.get("timestamp")) is not None
            and float(row["timestamp"]) >= float(first_nonzero_ts)
        )
    last_live_row = live_rows[-1] if live_rows else None
    last_live_seq = safe_int((last_live_row or {}).get("latest_planning_msg_sequence_num"))
    last_live_ts = safe_float((last_live_row or {}).get("timestamp"))
    last_live_input_seq = safe_int((last_live_row or {}).get("control_input_trajectory_header_sequence_num"))
    last_live_candidate_input_seq = safe_int(
        (last_live_row or {}).get("control_input_candidate_trajectory_header_sequence_num")
    )
    last_live_latest_replan_input_seq = safe_int(
        (last_live_row or {}).get("control_input_latest_replan_trajectory_header_sequence_num")
    )
    last_live_effective_source = (last_live_row or {}).get("effective_planning_source")
    last_live_candidate_source = (last_live_row or {}).get("control_input_candidate_source")
    exact_match_seen = any(bool(row.get("matched_planning_event_found_exactly")) for row in live_rows)
    exact_candidate_match_seen = any(
        bool(row.get("matched_candidate_planning_event_found_exactly")) for row in live_rows
    )
    first_nonzero_seq = safe_int((nonzero_planning_rows[0] or {}).get("planning_header_sequence_num")) if nonzero_planning_rows else None
    first_nonzero_gap_sec = (
        float(first_nonzero_ts - last_live_ts)
        if first_nonzero_ts is not None and last_live_ts is not None
        else None
    )
    stalled_before_first_nonzero = bool(
        nonzero_planning_rows
        and live_rows
        and not live_nonzero_rows
        and last_live_seq is not None
        and first_nonzero_seq is not None
        and int(last_live_seq) == int(first_nonzero_seq) - 1
        and first_nonzero_gap_sec is not None
        and 0.0 <= float(first_nonzero_gap_sec) <= 0.10
    )
    return {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "planning_total_messages": len(planning_rows),
        "planning_nonzero_messages": len(nonzero_planning_rows),
        "first_nonzero_planning_seq": first_nonzero_seq,
        "first_nonzero_planning_timestamp": first_nonzero_ts,
        "live_control_total_messages": len(live_rows),
        "live_control_latest_nonzero_messages": len(live_nonzero_rows),
        "last_live_control_planning_seq": last_live_seq,
        "last_live_control_timestamp": last_live_ts,
        "last_live_control_input_trajectory_header_sequence_num": last_live_input_seq,
        "last_live_control_candidate_trajectory_header_sequence_num": last_live_candidate_input_seq,
        "last_live_control_latest_replan_trajectory_header_sequence_num": last_live_latest_replan_input_seq,
        "last_live_effective_planning_source": last_live_effective_source,
        "last_live_control_input_candidate_source": last_live_candidate_source,
        "exact_match_seen": exact_match_seen,
        "exact_candidate_match_seen": exact_candidate_match_seen,
        "live_control_stops_before_first_nonzero_planning": stalled_before_first_nonzero,
        "live_control_to_first_nonzero_gap_sec": first_nonzero_gap_sec,
        "control_no_trajectory_after_first_nonzero": no_traj_after_first_nonzero,
    }


def _load_json_object_lines_from_text_log(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    except Exception:
        return []
    return rows


def _nonempty_line_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return sum(1 for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip())
    except Exception:
        return 0


def _text_file_has_nonwhitespace(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        return bool(path.read_text(encoding="utf-8", errors="replace").strip())
    except Exception:
        return False


def _extract_carla_bootstrap_summary(run_dir: Path) -> Dict[str, Any]:
    probe_path = run_dir / "carla_boot" / "carla_startup_probe.json"
    payload = load_json(probe_path)
    attempts = payload.get("attempts") or []
    if not isinstance(attempts, list):
        attempts = []
    memory_preflight = payload.get("memory_preflight") or {}
    if not isinstance(memory_preflight, dict):
        memory_preflight = {}
    final_attempt = attempts[-1] if attempts else {}
    if not isinstance(final_attempt, dict):
        final_attempt = {}
    diagnostics = final_attempt.get("launcher_diagnostics") or {}
    if not isinstance(diagnostics, dict):
        diagnostics = {}
    server_tail = diagnostics.get("latest_server_log_tail") or []
    if not isinstance(server_tail, list):
        server_tail = []
    server_tail_text = "\n".join(str(item) for item in server_tail).lower()
    request_exit_seen = "requestexitwithstatus" in server_tail_text or "request exit" in server_tail_text
    sig11_seen = "signal 11" in server_tail_text
    stream_eof_seen = "error retrieving stream id" in server_tail_text or "failed to read header" in server_tail_text
    process_alive = safe_bool(diagnostics.get("process_alive"))
    world_ready = bool(final_attempt.get("world_ready"))
    memory_preflight_blocked = bool(memory_preflight.get("blocked"))
    if memory_preflight_blocked:
        bootstrap_status = "memory_preflight_blocked"
    elif world_ready:
        bootstrap_status = "bootstrap_alive_then_world_ready"
    elif process_alive is False or request_exit_seen or sig11_seen:
        bootstrap_status = "bootstrap_process_died_before_world_attach"
    elif str(final_attempt.get("status") or "") in {"world_not_ready", "rpc_ready", "rpc_not_ready"}:
        bootstrap_status = "bootstrap_alive_world_ready_timeout"
    else:
        bootstrap_status = "bootstrap_unknown"
    oom_guard_triggered = bool(
        diagnostics.get("memory_guard_enabled")
        and process_alive is False
        and ("out of memory" in server_tail_text or sig11_seen)
    )
    return {
        "carla_bootstrap_status": bootstrap_status,
        "carla_bootstrap_stability_sec": safe_float(diagnostics.get("bootstrap_stability_elapsed_s")),
        "carla_process_survived_stability_window": safe_bool(
            diagnostics.get("process_survived_stability_window")
        ),
        "carla_server_request_exit_seen": request_exit_seen,
        "carla_server_sig11_seen": sig11_seen,
        "carla_server_stream_eof_seen": stream_eof_seen,
        "memory_preflight_status": memory_preflight.get("status"),
        "host_swap_enabled": safe_bool(memory_preflight.get("host_swap_enabled")),
        "available_memory_mb_before_start": safe_float(memory_preflight.get("available_memory_mb_before_start")),
        "carla_peak_rss_mb": safe_float(diagnostics.get("carla_process_rss_mb")),
        "oom_guard_triggered": oom_guard_triggered,
        "carla_bootstrap_probe_path": str(probe_path),
    }


def _extract_bridge_runtime_summary(artifacts_dir: Path) -> Dict[str, Any]:
    payload = load_json(artifacts_dir / "bridge_runtime_preflight.json")
    status = str(payload.get("bridge_runtime_preflight_status") or "unknown")
    return {
        "bridge_runtime_preflight_status": status,
        "bridge_runtime_import_ok": safe_bool(payload.get("bridge_runtime_import_ok")),
        "bridge_runtime_import_error": str(payload.get("bridge_runtime_import_error") or "").strip() or None,
        "bridge_runtime_dependency_probe_status": str(
            payload.get("bridge_runtime_dependency_probe_status") or "unknown"
        ),
        "bridge_runtime_missing_shared_libs": list(payload.get("bridge_runtime_missing_shared_libs") or []),
        "bridge_runtime_missing_python_modules": list(payload.get("bridge_runtime_missing_python_modules") or []),
        "bridge_runtime_python_executable": str(payload.get("bridge_runtime_python_executable") or "").strip()
        or None,
        "bridge_runtime_pythonpath_head": list(payload.get("bridge_runtime_pythonpath_head") or []),
        "bridge_runtime_ld_library_path_head": list(payload.get("bridge_runtime_ld_library_path_head") or []),
        "bridge_runtime_preflight": payload,
    }


def _extract_bridge_transport_summary(artifacts_dir: Path) -> Dict[str, Any]:
    payload = load_json(artifacts_dir / "bridge_transport_summary.json")
    return {
        "transport_mode": str(payload.get("transport_mode") or "ros2_gt").strip() or "ros2_gt",
        "gt_source": str(payload.get("gt_source") or "").strip() or None,
        "control_apply_path": str(payload.get("control_apply_path") or "").strip() or None,
        "tick_owner": str(payload.get("tick_owner") or "").strip() or None,
        "uses_ros2_gt": bool(payload.get("uses_ros2_gt", payload.get("ros2_gt_enabled", False))),
        "uses_ros2_control_bridge": bool(payload.get("uses_ros2_control_bridge", False)),
        "requires_ros2_reexec": bool(payload.get("requires_ros2_reexec", False)),
        "route_command_mode": str(payload.get("route_command_mode") or "").strip() or None,
        "route_command_path": str(payload.get("route_command_path") or "").strip() or None,
        "bridge_transport": payload,
    }


def _extract_command_materialization_summary(artifacts_dir: Path) -> Dict[str, Any]:
    payload = load_json(artifacts_dir / "command_materialization_summary.json")
    if not payload:
        bridge_health = load_json(artifacts_dir / "bridge_health_summary.json")
        if not bridge_health:
            bridge_health = load_json(artifacts_dir / "bridge_health_summary.finalized.json")
        candidate = bridge_health.get("command_materialization")
        if isinstance(candidate, dict):
            payload = dict(candidate)
    return {
        "command_materialization_stage": str(payload.get("command_path_stage") or "").strip() or None,
        "command_materialization_layer": str(payload.get("first_divergence_layer") or "").strip() or None,
        "command_materialization_reason": str(payload.get("first_divergence_reason") or "").strip() or None,
        "command_materialization": payload,
    }


def _compute_direct_control_apply_summary(
    artifacts_dir: Path,
    direct_bridge_stats: Dict[str, Any],
) -> Dict[str, Any]:
    rows = load_jsonl(artifacts_dir / "direct_bridge_control_apply.jsonl")
    frames: List[int] = []
    first_ts: Optional[float] = None
    last_ts: Optional[float] = None
    max_throttle = 0.0
    max_brake = 0.0
    max_speed = 0.0
    source_counts: Dict[str, int] = {}
    for row in rows:
        ts = safe_float(row.get("ts_sec"))
        if ts is not None:
            if first_ts is None:
                first_ts = ts
            last_ts = ts
        frame = safe_int(row.get("frame_id"))
        if frame is not None:
            frames.append(frame)
        source = str(row.get("source") or "unknown").strip() or "unknown"
        source_counts[source] = source_counts.get(source, 0) + 1
        max_throttle = max(max_throttle, safe_float(row.get("throttle")) or 0.0)
        max_brake = max(max_brake, safe_float(row.get("brake")) or 0.0)
        max_speed = max(max_speed, safe_float(row.get("speed_mps")) or 0.0)

    stats_count = safe_int(direct_bridge_stats.get("control_apply_count")) or 0
    apply_count = max(stats_count, len(rows))
    first_frame = min(frames) if frames else safe_int(direct_bridge_stats.get("control_apply_first_frame"))
    last_frame = max(frames) if frames else safe_int(direct_bridge_stats.get("control_apply_last_frame"))
    frame_span: Optional[int]
    if first_frame is not None and last_frame is not None:
        frame_span = int(last_frame) - int(first_frame) + 1
    else:
        frame_span = safe_int(direct_bridge_stats.get("control_apply_frame_span"))
    if first_ts is None:
        first_ts = safe_float(direct_bridge_stats.get("control_apply_first_ts_sec"))
    if last_ts is None:
        last_ts = safe_float(direct_bridge_stats.get("control_apply_last_ts_sec"))
    duration_sec = (last_ts - first_ts) if first_ts is not None and last_ts is not None else None
    max_throttle = max(max_throttle, safe_float(direct_bridge_stats.get("control_apply_max_throttle")) or 0.0)
    max_brake = max(max_brake, safe_float(direct_bridge_stats.get("control_apply_max_brake")) or 0.0)
    max_speed = max(max_speed, safe_float(direct_bridge_stats.get("control_apply_max_speed_mps")) or 0.0)

    if apply_count <= 0:
        window_status = "no_direct_apply"
    elif frame_span is None:
        window_status = "unknown_apply_window"
    elif frame_span is not None and frame_span < 20:
        window_status = "short_apply_window"
    else:
        window_status = "observable_apply_window"

    return {
        "apply_count": int(apply_count),
        "apply_fail_count": safe_int(direct_bridge_stats.get("control_apply_fail_count")) or 0,
        "apply_mode": str(direct_bridge_stats.get("control_apply_mode") or "").strip() or None,
        "frame_flush_queue_count": safe_int(
            direct_bridge_stats.get("control_apply_frame_flush_queue_count")
        ),
        "frame_flush_overwrite_count": safe_int(
            direct_bridge_stats.get("control_apply_frame_flush_overwrite_count")
        ),
        "first_apply_frame": first_frame,
        "last_apply_frame": last_frame,
        "apply_frame_span": frame_span,
        "first_apply_ts_sec": first_ts,
        "last_apply_ts_sec": last_ts,
        "apply_duration_sec": duration_sec,
        "max_throttle": max_throttle,
        "max_brake": max_brake,
        "max_speed_mps": max_speed,
        "source_counts": source_counts,
        "window_status": window_status,
    }


def _compute_direct_metric_consistency(
    bridge_transport_summary: Dict[str, Any],
    route_metrics: Dict[str, Any],
    max_speed_mps: float,
    direct_control_apply_summary: Dict[str, Any],
) -> Dict[str, Any]:
    transport_mode = str(bridge_transport_summary.get("transport_mode") or "").strip()
    route_distance = safe_float(route_metrics.get("route_distance_achieved_m"))
    route_completion = safe_float(route_metrics.get("route_completion_ratio"))
    direct_max_speed = safe_float(direct_control_apply_summary.get("max_speed_mps")) or 0.0
    apply_count = safe_int(direct_control_apply_summary.get("apply_count")) or 0
    apply_span = safe_int(direct_control_apply_summary.get("apply_frame_span"))
    window_status = str(direct_control_apply_summary.get("window_status") or "").strip()
    summary_max_speed = safe_float(max_speed_mps) or 0.0
    reasons: List[str] = []

    if transport_mode != "carla_direct":
        status = "not_applicable"
    elif apply_count <= 0:
        status = "no_direct_apply"
    elif window_status == "short_apply_window":
        if summary_max_speed - direct_max_speed >= 2.0:
            reasons.append("summary_speed_exceeds_direct_apply_speed")
        if route_distance is not None and abs(route_distance) >= 20.0:
            reasons.append("route_distance_too_large_for_short_direct_apply_window")
        status = "short_apply_metric_mismatch" if reasons else "short_apply_insufficient_window"
    else:
        if summary_max_speed - direct_max_speed >= 5.0 and direct_max_speed < 1.0:
            reasons.append("summary_speed_not_supported_by_direct_apply_speed")
        status = "metric_mismatch" if reasons else "consistent"

    return {
        "status": status,
        "reasons": reasons,
        "transport_mode": transport_mode,
        "route_distance_achieved_m": route_distance,
        "route_completion_ratio": route_completion,
        "summary_max_speed_mps": summary_max_speed,
        "direct_apply_count": apply_count,
        "direct_apply_window_status": window_status,
        "direct_apply_frame_span": apply_span,
        "direct_apply_max_speed_mps": direct_max_speed,
        "direct_apply_max_throttle": safe_float(direct_control_apply_summary.get("max_throttle")),
    }


def _control_process_alive_from_status_logs(artifacts_dir: Path) -> bool:
    control_pattern = "modules/control/control_component/dag/control.dag"
    for name in (
        "apollo_modules_status.log",
        "apollo_modules_status_final.log",
        "apollo_control_deferred_status.log",
    ):
        path = artifacts_dir / name
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if control_pattern in text:
            return True
    return False


def _status_log_runtime_filtered_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not raw_line.startswith("runtime_filtered_lines:"):
                continue
            payload = json.loads(raw_line.split(":", 1)[1].strip())
            if isinstance(payload, list):
                return [str(item).strip() for item in payload if str(item).strip()]
    except Exception:
        return []
    return []


def _final_control_modules_status(artifacts_dir: Path) -> Dict[str, Any]:
    path = artifacts_dir / "apollo_modules_status_final.log"
    lines = _status_log_runtime_filtered_lines(path)
    control_pattern = "modules/control/control_component/dag/control.dag"
    control_present = any(control_pattern in line for line in lines)
    if not lines and path.exists():
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            text = ""
        if control_pattern in text:
            control_present = True
    return {
        "status_path": str(path),
        "exists": path.exists(),
        "runtime_filtered_lines": lines,
        "control_present": control_present,
    }


def _load_control_deferred_survival_summary(artifacts_dir: Path) -> Dict[str, Any]:
    path = artifacts_dir / "apollo_control_deferred_survival.json"
    if not path.exists():
        return {"path": str(path), "exists": False}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception as exc:
        return {
            "path": str(path),
            "exists": True,
            "parse_error": str(exc),
        }
    if not isinstance(payload, dict):
        return {
            "path": str(path),
            "exists": True,
            "parse_error": "payload_not_object",
        }
    payload = dict(payload)
    payload.setdefault("path", str(path))
    payload["exists"] = True
    return payload


def _load_control_deferred_start_summary(artifacts_dir: Path) -> Dict[str, Any]:
    path = artifacts_dir / "apollo_control_deferred_start.log"
    summary: Dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "preferred_start_mode": None,
        "actual_start_mode": None,
        "direct_log": None,
        "launch_log": None,
        "returncode": None,
    }
    if not path.exists():
        return summary
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        summary["parse_error"] = str(exc)
        return summary
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key == "preferred_start_mode":
            summary["preferred_start_mode"] = value or None
        elif key == "actual_start_mode":
            summary["actual_start_mode"] = value or None
        elif key == "direct_log":
            summary["direct_log"] = value or None
        elif key == "launch_log":
            summary["launch_log"] = value or None
        elif key == "returncode":
            summary["returncode"] = safe_int(value)
    return summary


def _classify_control_crash_reason(text: str) -> Optional[str]:
    lowered = str(text or "").lower()
    if not lowered.strip():
        return None
    if "attempt to free invalid pointer" in lowered or "src/tcmalloc.cc" in lowered:
        return "tcmalloc_invalid_free"
    if "segmentation fault" in lowered or "signal 11" in lowered or "sigsegv" in lowered:
        return "segfault"
    if "cannot open shared object file" in lowered or "error while loading shared libraries" in lowered:
        return "shared_library_error"
    if "missing_control_launch_or_dag" in lowered:
        return "launch_failed"
    return None


def _extract_control_crash_summary(
    artifacts_dir: Path,
    *,
    control_started_pid_seen: bool,
    control_final_process_alive: bool,
    control_consume_row_count: int,
    apollo_control_raw_row_count: int,
) -> Dict[str, Any]:
    start_summary = _load_control_deferred_start_summary(artifacts_dir)
    actual_start_mode = str(start_summary.get("actual_start_mode") or start_summary.get("preferred_start_mode") or "")
    artifact_candidates: List[Tuple[str, Path, Optional[str]]] = [
        ("dag", artifacts_dir / "apollo_control_deferred_mainboard.log", start_summary.get("direct_log")),
        ("launch", artifacts_dir / "apollo_control_deferred_launch.log", start_summary.get("launch_log")),
    ]
    selected_mode = actual_start_mode
    selected_path: Optional[str] = None
    selected_text = ""
    for mode, artifact_path, runtime_path in artifact_candidates:
        if selected_mode and mode not in selected_mode:
            continue
        if artifact_path.exists():
            try:
                selected_text = artifact_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                selected_text = ""
            selected_path = str(artifact_path)
            break
        if runtime_path:
            selected_path = str(runtime_path)
    if not selected_text:
        for mode, artifact_path, runtime_path in artifact_candidates:
            if artifact_path.exists():
                try:
                    selected_text = artifact_path.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    selected_text = ""
                selected_mode = mode
                selected_path = str(artifact_path)
                break
            if selected_path is None and runtime_path:
                selected_path = str(runtime_path)
    crash_reason = _classify_control_crash_reason(selected_text)
    control_output_materialized = bool(control_consume_row_count > 0 or apollo_control_raw_row_count > 0)
    crash_detected = bool(
        crash_reason
        or (
            control_started_pid_seen
            and not control_output_materialized
            and not control_final_process_alive
        )
    )
    if crash_detected and crash_reason is None:
        crash_reason = "unknown_control_crash"
    return {
        "control_deferred_start_mode": selected_mode or None,
        "control_deferred_log_path": selected_path,
        "control_crash_detected": crash_detected,
        "control_crash_reason": crash_reason,
        "control_crash_log_tail": "\n".join(selected_text.splitlines()[-20:]) if selected_text else "",
        "control_output_materialized": control_output_materialized,
        "control_deferred_start": start_summary,
    }


def _extract_control_info_summary(artifacts_dir: Path) -> Dict[str, Any]:
    path = artifacts_dir / "apollo_control.INFO"
    summary: Dict[str, Any] = {
        "apollo_control_info_path": str(path),
        "apollo_control_info_exists": path.exists(),
        "apollo_control_chassis_not_ready_count": 0,
        "apollo_control_waiting_for_planning_count": 0,
        "apollo_control_info_tail": "",
    }
    if not path.exists():
        return summary
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        summary["apollo_control_info_parse_error"] = str(exc)
        return summary
    lines = text.splitlines()
    summary["apollo_control_chassis_not_ready_count"] = sum(
        1 for line in lines if "Chassis msg is not ready!" in line
    )
    summary["apollo_control_waiting_for_planning_count"] = sum(
        1 for line in lines if "Control waiting for first nonempty planning trajectory" in line
    )
    summary["apollo_control_info_tail"] = "\n".join(lines[-20:]) if lines else ""
    return summary


def _extract_control_raw_summary(
    artifacts_dir: Path,
    live_success_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    raw_rows = load_jsonl(artifacts_dir / "apollo_control_raw.jsonl")
    normalized_live_rows = _normalize_live_control_rows(live_success_rows)
    control_last_output_ts = next(
        (
            safe_float(row.get("ts_sec"))
            for row in reversed(raw_rows)
            if safe_float(row.get("ts_sec")) is not None
        ),
        None,
    )
    if control_last_output_ts is None:
        control_last_output_ts = next(
            (
                safe_float(row.get("timestamp"))
                for row in reversed(normalized_live_rows)
                if safe_float(row.get("timestamp")) is not None
            ),
            None,
        )

    control_first_nonzero_planning_seen_by_control_at = next(
        (
            safe_float(row.get("timestamp"))
            for row in normalized_live_rows
            if int(row.get("latest_planning_trajectory_point_count", 0) or 0) > 0
        ),
        None,
    )
    last_input_row = next(
        (
            row
            for row in reversed(normalized_live_rows)
            if (safe_int(row.get("control_input_trajectory_header_sequence_num")) or 0) > 0
        ),
        None,
    )
    if last_input_row is None:
        last_input_row = next(
            (
                row
                for row in reversed(normalized_live_rows)
                if bool(row.get("control_used_planning_trajectory"))
                and int(row.get("latest_planning_trajectory_point_count", 0) or 0) > 0
            ),
            None,
        )
    control_last_input_trajectory_seq = (
        safe_int(last_input_row.get("control_input_trajectory_header_sequence_num"))
        if last_input_row is not None
        else None
    )
    control_last_input_trajectory_point_count = (
        int(last_input_row.get("latest_planning_trajectory_point_count", 0) or 0)
        if last_input_row is not None
        else 0
    )
    if control_last_input_trajectory_seq is None and last_input_row is not None:
        control_last_input_trajectory_seq = safe_int(last_input_row.get("latest_planning_msg_sequence_num"))

    zero_hold_rows = 0
    last_raw_input_seq = None
    for payload in raw_rows:
        raw = payload.get("apollo_control_raw") or {}
        if not isinstance(raw, dict):
            continue
        input_seq = safe_int(raw.get("debug_input_trajectory_header_sequence_num"))
        if input_seq is not None:
            last_raw_input_seq = input_seq
        throttle = safe_float(raw.get("throttle")) or 0.0
        brake = safe_float(raw.get("brake")) or 0.0
        steering_target = safe_float(raw.get("steering_target")) or 0.0
        if abs(throttle) <= 1e-6 and abs(steering_target) <= 1e-6 and brake >= 14.5:
            zero_hold_rows += 1

    if control_last_input_trajectory_seq is None and last_raw_input_seq is not None and last_raw_input_seq > 0:
        control_last_input_trajectory_seq = last_raw_input_seq
    if control_last_input_trajectory_seq is None:
        control_last_input_trajectory_seq = 0

    control_output_zero_hold_ratio = (
        float(zero_hold_rows) / float(len(raw_rows))
        if raw_rows
        else None
    )
    control_zero_hold_only = bool(raw_rows) and (
        control_first_nonzero_planning_seen_by_control_at is None
        and int(control_last_input_trajectory_seq or 0) <= 0
        and (control_output_zero_hold_ratio or 0.0) >= 0.95
    )

    return {
        "apollo_control_raw_row_count": len(raw_rows),
        "control_zero_hold_only": control_zero_hold_only,
        "control_last_input_trajectory_seq": control_last_input_trajectory_seq,
        "control_last_input_trajectory_point_count": control_last_input_trajectory_point_count,
        "control_first_nonzero_planning_seen_by_control_at": control_first_nonzero_planning_seen_by_control_at,
        "control_last_output_ts": control_last_output_ts,
        "control_output_zero_hold_ratio": control_output_zero_hold_ratio,
    }


def _interpolated_percentile(values: List[float], quantile: float) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    bounded_quantile = min(1.0, max(0.0, float(quantile)))
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * bounded_quantile
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return float(ordered[lower_index])
    upper_weight = position - lower_index
    lower_weight = 1.0 - upper_weight
    return float((ordered[lower_index] * lower_weight) + (ordered[upper_index] * upper_weight))


def _first_matched_point_too_large_seq(
    planning_trajectory_type_summary: Dict[str, Any],
) -> Optional[int]:
    candidates: List[int] = []
    if (
        str(planning_trajectory_type_summary.get("first_path_fallback_trigger_reason_family") or "")
        == "matched_point_too_large"
    ):
        candidate = safe_int(planning_trajectory_type_summary.get("first_path_fallback_trigger_seq"))
        if candidate is None:
            candidate = safe_int(planning_trajectory_type_summary.get("first_path_fallback_seq"))
        if candidate is not None:
            candidates.append(candidate)
    if (
        str(planning_trajectory_type_summary.get("first_relapse_after_recovery_reason_family") or "")
        == "matched_point_too_large"
    ):
        candidate = safe_int(planning_trajectory_type_summary.get("first_relapse_after_recovery_seq"))
        if candidate is not None:
            candidates.append(candidate)
    if not candidates:
        return None
    return min(candidates)


def _payload_float_from_paths(payload: Dict[str, Any], *paths: Sequence[str]) -> Optional[float]:
    for path in paths:
        value = safe_float(_nested_get(payload, *path))
        if value is not None:
            return value
    return None


def _payload_int_from_paths(payload: Dict[str, Any], *paths: Sequence[str]) -> Optional[int]:
    for path in paths:
        value = safe_int(_nested_get(payload, *path))
        if value is not None:
            return value
    return None


def _compute_longitudinal_tracking_health_summary(
    effective_cfg: Dict[str, Any],
    debug_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    routing_target_speed_mps = safe_float(
        _nested_get(effective_cfg, "algo", "apollo", "routing", "target_speed_mps")
    )
    planning_default_cruise_speed_mps = safe_float(
        _nested_get(effective_cfg, "algo", "apollo", "planning", "default_cruise_speed_mps")
    )
    speed_values: List[float] = []
    speed_ratio_values: List[float] = []
    throttle_values: List[float] = []
    brake_values: List[float] = []
    acceleration_values: List[float] = []
    throttle_brake_overlap_rows = 0
    throttle_rows = 0
    throttle_saturated_rows = 0
    trailing_speed_values: List[float] = []
    trailing_window_size = 20

    for row in debug_rows:
        speed = safe_float(row.get("speed_mps"))
        if speed is not None:
            speed = abs(float(speed))
            speed_values.append(speed)
            trailing_speed_values.append(speed)
            if len(trailing_speed_values) > trailing_window_size:
                trailing_speed_values.pop(0)
            if routing_target_speed_mps is not None and routing_target_speed_mps > 1e-6:
                speed_ratio_values.append(speed / float(routing_target_speed_mps))

        throttle = _payload_float_from_paths(
            row,
            ("mapped_throttle_cmd",),
            ("commanded_throttle",),
            ("throttle_after_boost",),
            ("apollo_desired_throttle",),
        )
        brake = _payload_float_from_paths(
            row,
            ("mapped_brake_cmd",),
            ("commanded_brake",),
            ("brake_after_deadzone",),
            ("apollo_desired_brake",),
        )
        acceleration = _payload_float_from_paths(
            row,
            ("apollo_acceleration_mps2",),
            ("target_accel_mps2",),
        )
        if acceleration is not None:
            acceleration_values.append(abs(float(acceleration)))
        if throttle is not None:
            throttle = abs(float(throttle))
            throttle_values.append(throttle)
            throttle_rows += 1
            if throttle >= 0.999:
                throttle_saturated_rows += 1
        if brake is not None:
            brake_values.append(abs(float(brake)))
        if throttle is not None and brake is not None and throttle > 0.05 and brake > 0.05:
            throttle_brake_overlap_rows += 1

    persistent_low_speed_at_end = None
    if trailing_speed_values:
        threshold = 2.5
        if routing_target_speed_mps is not None and routing_target_speed_mps > 0.0:
            threshold = max(2.5, float(routing_target_speed_mps) * 0.5)
        slow_ratio = float(sum(1 for item in trailing_speed_values if item < threshold)) / float(
            len(trailing_speed_values)
        )
        persistent_low_speed_at_end = slow_ratio >= 0.8

    denominator = max(len(throttle_values), len(brake_values))
    return {
        "routing_target_speed_mps": routing_target_speed_mps,
        "planning_default_cruise_speed_mps": planning_default_cruise_speed_mps,
        "speed_abs_p95": _interpolated_percentile(speed_values, 0.95),
        "speed_ratio_to_target_p95": _interpolated_percentile(speed_ratio_values, 0.95),
        "throttle_p95": _interpolated_percentile(throttle_values, 0.95),
        "brake_p95": _interpolated_percentile(brake_values, 0.95),
        "throttle_brake_overlap_ratio": (
            float(throttle_brake_overlap_rows) / float(denominator)
            if denominator > 0
            else None
        ),
        "acc_cmd_p95": _interpolated_percentile(acceleration_values, 0.95),
        "speed_gain_limited_ratio": (
            float(throttle_saturated_rows) / float(throttle_rows)
            if throttle_rows > 0
            else None
        ),
        "persistent_low_speed_at_end": persistent_low_speed_at_end,
    }


def _compute_curve_tracking_health_summary(
    artifacts_dir: Path,
    planning_trajectory_type_summary: Dict[str, Any],
) -> Dict[str, Any]:
    raw_rows = load_jsonl(artifacts_dir / "apollo_control_raw.jsonl")
    decoded_rows = load_jsonl(artifacts_dir / "control_decode_debug.jsonl")
    steer_abs_values: List[float] = []
    steer_over95_count = 0
    first_high_steer_seq = None
    first_high_steer_at = None
    lateral_error_abs_values: List[float] = []
    heading_error_abs_values: List[float] = []
    target_point_kappa_abs_values: List[float] = []
    mapped_carla_steer_abs_values: List[float] = []
    target_front_wheel_angle_deg_abs_values: List[float] = []
    for payload in raw_rows:
        raw = payload.get("apollo_control_raw") or {}
        if not isinstance(raw, dict):
            continue
        selected_steering_field = str(payload.get("selected_steering_field") or "steering_target")
        steering_target = safe_float(raw.get(selected_steering_field))
        if steering_target is None and selected_steering_field != "steering_target":
            steering_target = safe_float(raw.get("steering_target"))
        if steering_target is None:
            continue
        steer_abs = abs(float(steering_target))
        steer_abs_values.append(steer_abs)
        if steer_abs < 95.0:
            continue
        steer_over95_count += 1
        if first_high_steer_seq is not None:
            continue
        first_high_steer_seq = safe_int(raw.get("debug_input_trajectory_header_sequence_num"))
        if first_high_steer_seq is None:
            first_high_steer_seq = safe_int(raw.get("debug_input_latest_replan_trajectory_header_sequence_num"))
        if first_high_steer_seq is None:
            first_high_steer_seq = safe_int(raw.get("control_header_sequence_num"))
        first_high_steer_at = safe_float(payload.get("ts_sec"))
        if first_high_steer_at is None:
            first_high_steer_at = safe_float(raw.get("control_header_timestamp_sec"))

    for payload in decoded_rows:
        parsed_control = payload.get("parsed_control") or {}
        if isinstance(parsed_control, dict):
            lateral_error_abs = safe_float(parsed_control.get("debug_simple_lat_lateral_error_m"))
            if lateral_error_abs is not None:
                lateral_error_abs_values.append(abs(float(lateral_error_abs)))
            heading_error_abs = safe_float(parsed_control.get("debug_simple_lat_heading_error_rad"))
            if heading_error_abs is not None:
                heading_error_abs_values.append(abs(float(heading_error_abs)))
            target_point_kappa_abs = safe_float(parsed_control.get("debug_simple_lat_target_point_kappa"))
            if target_point_kappa_abs is not None:
                target_point_kappa_abs_values.append(abs(float(target_point_kappa_abs)))
        output_to_carla = payload.get("output_to_carla") or {}
        if isinstance(output_to_carla, dict):
            mapped_carla_steer_abs = safe_float(output_to_carla.get("mapped_carla_steer_cmd"))
            if mapped_carla_steer_abs is not None:
                mapped_carla_steer_abs_values.append(abs(float(mapped_carla_steer_abs)))
            target_front_wheel_angle_deg_abs = safe_float(output_to_carla.get("target_front_wheel_angle_deg"))
            if target_front_wheel_angle_deg_abs is not None:
                target_front_wheel_angle_deg_abs_values.append(abs(float(target_front_wheel_angle_deg_abs)))

    first_matched_point_too_large_seq = _first_matched_point_too_large_seq(planning_trajectory_type_summary)
    high_steer_before_first_matched_point_too_large = None
    if first_high_steer_seq is not None and first_matched_point_too_large_seq is not None:
        high_steer_before_first_matched_point_too_large = (
            int(first_high_steer_seq) <= int(first_matched_point_too_large_seq)
        )

    parsed_steer_count = len(steer_abs_values)
    return {
        "apollo_raw_steer_abs_p95": _interpolated_percentile(steer_abs_values, 0.95),
        "apollo_raw_steer_abs_p99": _interpolated_percentile(steer_abs_values, 0.99),
        "apollo_raw_steer_over95_ratio": (
            float(steer_over95_count) / float(parsed_steer_count)
            if parsed_steer_count > 0
            else None
        ),
        "apollo_simple_lat_lateral_error_abs_p95": _interpolated_percentile(lateral_error_abs_values, 0.95),
        "apollo_simple_lat_lateral_error_abs_p99": _interpolated_percentile(lateral_error_abs_values, 0.99),
        "apollo_simple_lat_heading_error_abs_p95": _interpolated_percentile(heading_error_abs_values, 0.95),
        "apollo_simple_lat_heading_error_abs_p99": _interpolated_percentile(heading_error_abs_values, 0.99),
        "apollo_simple_lat_target_point_kappa_abs_p95": _interpolated_percentile(
            target_point_kappa_abs_values, 0.95
        ),
        "apollo_simple_lat_target_point_kappa_abs_p99": _interpolated_percentile(
            target_point_kappa_abs_values, 0.99
        ),
        "mapped_carla_steer_cmd_abs_p95": _interpolated_percentile(mapped_carla_steer_abs_values, 0.95),
        "mapped_carla_steer_cmd_abs_p99": _interpolated_percentile(mapped_carla_steer_abs_values, 0.99),
        "target_front_wheel_angle_deg_abs_p95": _interpolated_percentile(
            target_front_wheel_angle_deg_abs_values, 0.95
        ),
        "target_front_wheel_angle_deg_abs_p99": _interpolated_percentile(
            target_front_wheel_angle_deg_abs_values, 0.99
        ),
        "first_high_steer_seq": first_high_steer_seq,
        "first_high_steer_at": first_high_steer_at,
        "first_matched_point_too_large_seq": first_matched_point_too_large_seq,
        "high_steer_before_first_matched_point_too_large": high_steer_before_first_matched_point_too_large,
        "path_fallback_count": safe_int(planning_trajectory_type_summary.get("path_fallback_count")),
        "persistent_path_fallback_at_end": bool(
            planning_trajectory_type_summary.get("persistent_path_fallback_at_end")
        ),
        "path_fallback_count_after_first_recovery": safe_int(
            planning_trajectory_type_summary.get("path_fallback_count_after_first_recovery")
        ),
    }


def _compute_curve_semantic_window(
    artifacts_dir: Path,
    planning_rows: List[Dict[str, Any]],
    planning_trajectory_type_summary: Dict[str, Any],
    curve_tracking_health_summary: Dict[str, Any],
) -> Dict[str, Any]:
    decoded_rows = load_jsonl(artifacts_dir / "control_decode_debug.jsonl")
    first_matched_point_too_large_seq = safe_int(
        curve_tracking_health_summary.get("first_matched_point_too_large_seq")
    )
    first_high_steer_seq = safe_int(curve_tracking_health_summary.get("first_high_steer_seq"))
    first_high_steer_at = safe_float(curve_tracking_health_summary.get("first_high_steer_at"))
    first_matched_point_too_large_at = None
    if first_matched_point_too_large_seq is not None:
        for row in planning_rows:
            if safe_int(row.get("planning_header_sequence_num")) != first_matched_point_too_large_seq:
                continue
            first_matched_point_too_large_at = safe_float(row.get("timestamp"))
            if first_matched_point_too_large_at is None:
                first_matched_point_too_large_at = safe_float(row.get("planning_header_timestamp_sec"))
            if first_matched_point_too_large_at is not None:
                break

    semantic_window_anchor_kind = "matched_point_too_large"
    semantic_window_anchor_seq = first_matched_point_too_large_seq
    semantic_window_anchor_at = first_matched_point_too_large_at
    if semantic_window_anchor_at is None and first_high_steer_seq is not None:
        semantic_window_anchor_kind = "first_high_steer"
        semantic_window_anchor_seq = first_high_steer_seq
        semantic_window_anchor_at = first_high_steer_at
    elif semantic_window_anchor_at is None:
        semantic_window_anchor_kind = "unavailable"
        semantic_window_anchor_seq = None
        semantic_window_anchor_at = None

    window_rows: List[Dict[str, Any]] = []
    if semantic_window_anchor_at is not None:
        lower_bound = float(semantic_window_anchor_at) - 5.0
        for payload in decoded_rows:
            ts_sec = safe_float(payload.get("ts_sec"))
            if ts_sec is None:
                continue
            if lower_bound <= float(ts_sec) <= float(semantic_window_anchor_at):
                window_rows.append(payload)
    if not window_rows and semantic_window_anchor_seq is not None:
        filtered: List[Dict[str, Any]] = []
        for payload in decoded_rows:
            matched_seq = _payload_int_from_paths(
                payload,
                ("output_to_carla", "planning_lateral_matched_sequence_num"),
                ("output_to_carla", "planning_lateral_latest_sequence_num"),
                ("raw_control_msg_dump", "debug_input_trajectory_header_sequence_num"),
                ("raw_control_msg_dump", "debug_input_latest_replan_trajectory_header_sequence_num"),
            )
            if matched_seq is not None and matched_seq <= int(semantic_window_anchor_seq):
                filtered.append(payload)
        window_rows = filtered[-120:]

    lateral_error_abs_values: List[float] = []
    heading_error_abs_values: List[float] = []
    target_point_kappa_abs_values: List[float] = []
    mapped_carla_steer_abs_values: List[float] = []

    for payload in window_rows:
        lateral_error = _payload_float_from_paths(
            payload,
            ("parsed_control", "debug_simple_lat_lateral_error_m"),
            ("parsed_control", "debug_simple_lat_lateral_error"),
            ("raw_control_msg_dump", "debug_simple_lat_lateral_error"),
        )
        if lateral_error is not None:
            lateral_error_abs_values.append(abs(float(lateral_error)))
        heading_error = _payload_float_from_paths(
            payload,
            ("parsed_control", "debug_simple_lat_heading_error_rad"),
            ("parsed_control", "debug_simple_lat_heading_error"),
            ("raw_control_msg_dump", "debug_simple_lat_heading_error"),
        )
        if heading_error is not None:
            heading_error_abs_values.append(abs(float(heading_error)))
        target_point_kappa = _payload_float_from_paths(
            payload,
            ("parsed_control", "debug_simple_lat_target_point_kappa"),
            ("raw_control_msg_dump", "debug_simple_lat_current_target_point_kappa"),
            ("raw_control_msg_dump", "debug_simple_lat_target_point_kappa"),
        )
        if target_point_kappa is not None:
            target_point_kappa_abs_values.append(abs(float(target_point_kappa)))
        mapped_carla_steer = _payload_float_from_paths(
            payload,
            ("output_to_carla", "mapped_carla_steer_cmd"),
            ("output_to_carla", "steer"),
        )
        if mapped_carla_steer is not None:
            mapped_carla_steer_abs_values.append(abs(float(mapped_carla_steer)))

    return {
        "semantic_window_anchor_kind": semantic_window_anchor_kind,
        "semantic_window_anchor_seq": semantic_window_anchor_seq,
        "semantic_window_anchor_at": semantic_window_anchor_at,
        "first_matched_point_too_large_seq": first_matched_point_too_large_seq,
        "first_matched_point_too_large_at": first_matched_point_too_large_at,
        "simple_lat_lateral_error_abs_p95_before_failure": _interpolated_percentile(
            lateral_error_abs_values, 0.95
        ),
        "simple_lat_heading_error_abs_p95_before_failure": _interpolated_percentile(
            heading_error_abs_values, 0.95
        ),
        "target_point_kappa_abs_p95_before_failure": _interpolated_percentile(
            target_point_kappa_abs_values, 0.95
        ),
        "mapped_carla_steer_cmd_abs_p95_before_failure": _interpolated_percentile(
            mapped_carla_steer_abs_values, 0.95
        ),
        "simple_lat_lateral_error_abs_p95_before_anchor": _interpolated_percentile(
            lateral_error_abs_values, 0.95
        ),
        "simple_lat_heading_error_abs_p95_before_anchor": _interpolated_percentile(
            heading_error_abs_values, 0.95
        ),
        "target_point_kappa_abs_p95_before_anchor": _interpolated_percentile(
            target_point_kappa_abs_values, 0.95
        ),
        "mapped_carla_steer_cmd_abs_p95_before_anchor": _interpolated_percentile(
            mapped_carla_steer_abs_values, 0.95
        ),
        "high_steer_before_first_matched_point_too_large": curve_tracking_health_summary.get(
            "high_steer_before_first_matched_point_too_large"
        ),
        "persistent_path_fallback_at_end": bool(
            planning_trajectory_type_summary.get("persistent_path_fallback_at_end")
        ),
    }


def _compute_control_handoff_summary(
    *,
    artifacts_dir: Path,
    routing_rows: List[Dict[str, Any]],
    planning_rows: List[Dict[str, Any]],
    live_control_rows: List[Dict[str, Any]],
    bridge_health_live: Dict[str, Any],
) -> Dict[str, Any]:
    planning_ready_wait_rows: List[Dict[str, Any]] = []
    for wait_name in (
        "apollo_control_route_seen_wait.log",
        "apollo_control_route_established_wait.log",
        "apollo_control_planning_ready_wait.log",
    ):
        planning_ready_wait_rows.extend(
            _load_json_object_lines_from_text_log(artifacts_dir / wait_name)
        )
    planning_route_seen_at = next(
        (
            safe_float(row.get("route_seen_at"))
            for row in planning_ready_wait_rows
            if safe_float(row.get("route_seen_at")) is not None
        ),
        None,
    )
    if planning_route_seen_at is None:
        planning_route_seen_at = next(
            (
                safe_float(row.get("route_established_at"))
                for row in planning_ready_wait_rows
                if safe_float(row.get("route_established_at")) is not None
            ),
            None,
        )
    if planning_route_seen_at is None:
        planning_route_seen_at = next(
            (
                safe_float(row.get("routing_success_at"))
                for row in planning_ready_wait_rows
                if safe_float(row.get("routing_success_at")) is not None
            ),
            None,
        )
    if planning_route_seen_at is None:
        planning_route_seen_at = next(
            (
                safe_float(row.get("timestamp"))
                for row in routing_rows
                if bool(row.get("routing_success")) or bool(row.get("route_established"))
            ),
            None,
        )
    if planning_route_seen_at is None:
        planning_route_seen_at = next(
            (
                safe_float(row.get("timestamp"))
                for row in routing_rows
                if bool(row.get("routing_request_sent"))
            ),
            None,
        )

    planning_first_nonempty_at = safe_float(bridge_health_live.get("planning_first_nonempty_ts_sec"))
    if planning_first_nonempty_at is None:
        planning_first_nonempty_at = next(
            (
                safe_float(row.get("timestamp"))
                for row in planning_rows
                if int(row.get("trajectory_point_count", 0) or 0) > 0
            ),
            None,
        )

    live_success_rows = _normalize_live_control_rows(live_control_rows)
    control_first_consume_at = next(
        (safe_float(row.get("timestamp")) for row in live_success_rows if safe_float(row.get("timestamp")) is not None),
        None,
    )
    control_consume_row_count = len(live_success_rows)
    control_raw_summary = _extract_control_raw_summary(artifacts_dir, live_success_rows)
    apollo_control_raw_row_count = int(control_raw_summary.get("apollo_control_raw_row_count", 0) or 0)
    apollo_control_log_nonempty = _text_file_has_nonwhitespace(artifacts_dir / "apollo_control.INFO")
    control_info_summary = _extract_control_info_summary(artifacts_dir)
    apollo_control_process_alive = (
        _control_process_alive_from_status_logs(artifacts_dir)
        or control_consume_row_count > 0
        or apollo_control_raw_row_count > 0
        or apollo_control_log_nonempty
    )
    control_final_modules_status = _final_control_modules_status(artifacts_dir)
    control_final_process_alive = bool(control_final_modules_status.get("control_present"))
    control_deferred_survival = _load_control_deferred_survival_summary(artifacts_dir)
    control_started_pid_seen = bool(control_deferred_survival.get("control_started_pid_seen"))
    control_survived_5s = safe_bool(control_deferred_survival.get("control_survived_5s"))
    control_survived_10s = safe_bool(control_deferred_survival.get("control_survived_10s"))
    control_present_after_first_nonzero_planning = safe_bool(
        control_deferred_survival.get("control_present_after_first_nonzero_planning")
    )
    control_crash_summary = _extract_control_crash_summary(
        artifacts_dir,
        control_started_pid_seen=control_started_pid_seen,
        control_final_process_alive=control_final_process_alive,
        control_consume_row_count=control_consume_row_count,
        apollo_control_raw_row_count=apollo_control_raw_row_count,
    )
    control_handoff_latency_sec = (
        float(control_first_consume_at - planning_first_nonempty_at)
        if control_first_consume_at is not None and planning_first_nonempty_at is not None
        else None
    )
    control_first_nonzero_planning_seen_by_control_at = safe_float(
        control_raw_summary.get("control_first_nonzero_planning_seen_by_control_at")
    )
    control_last_output_ts = safe_float(control_raw_summary.get("control_last_output_ts"))
    control_last_input_trajectory_seq = safe_int(control_raw_summary.get("control_last_input_trajectory_seq"))
    control_last_input_trajectory_point_count = int(
        control_raw_summary.get("control_last_input_trajectory_point_count", 0) or 0
    )
    control_zero_hold_only = bool(control_raw_summary.get("control_zero_hold_only"))
    control_output_zero_hold_ratio = safe_float(control_raw_summary.get("control_output_zero_hold_ratio"))
    control_output_stopped_before_nonzero_planning = bool(
        planning_first_nonempty_at is not None
        and control_first_nonzero_planning_seen_by_control_at is None
        and control_last_output_ts is not None
        and control_last_output_ts + 1e-6 < planning_first_nonempty_at
        and apollo_control_raw_row_count > 0
    )
    control_process_died_before_nonzero_planning = bool(
        control_output_stopped_before_nonzero_planning
        and bool(control_final_modules_status.get("exists"))
        and not control_final_process_alive
    )
    sustained_control_runtime_without_output = bool(
        control_started_pid_seen
        and control_consume_row_count <= 0
        and apollo_control_raw_row_count <= 0
        and planning_route_seen_at is not None
        and planning_first_nonempty_at is not None
        and (
            bool(control_survived_10s)
            or bool(control_present_after_first_nonzero_planning)
        )
    )

    if control_consume_row_count > 0:
        if control_process_died_before_nonzero_planning:
            control_handoff_status = "control_process_died_before_nonzero_planning"
        elif control_output_stopped_before_nonzero_planning:
            control_handoff_status = "control_output_stopped_before_nonzero_planning"
        elif (
            control_first_nonzero_planning_seen_by_control_at is not None
            or control_last_input_trajectory_point_count > 0
            or int(control_last_input_trajectory_seq or 0) > 0
        ):
            control_handoff_status = "control_consuming_with_nonzero_planning"
        elif control_zero_hold_only:
            control_handoff_status = "control_consuming_zero_hold_only"
        else:
            control_handoff_status = "control_consuming"
    elif sustained_control_runtime_without_output:
        control_handoff_status = "planning_ready_control_not_consuming"
    elif control_started_pid_seen and apollo_control_raw_row_count <= 0 and not control_final_process_alive:
        control_handoff_status = "control_started_then_died_before_any_output"
    elif not apollo_control_process_alive:
        control_handoff_status = "control_process_missing"
    elif planning_route_seen_at is not None and planning_first_nonempty_at is not None:
        control_handoff_status = "planning_ready_control_not_consuming"
    elif apollo_control_process_alive and apollo_control_raw_row_count <= 0:
        control_handoff_status = "control_output_not_observed"
    else:
        control_handoff_status = "planning_not_ready"

    return {
        "control_handoff_status": control_handoff_status,
        "planning_route_seen_at": planning_route_seen_at,
        "planning_first_nonempty_at": planning_first_nonempty_at,
        "control_first_consume_at": control_first_consume_at,
        "control_consume_row_count": control_consume_row_count,
        "control_handoff_latency_sec": control_handoff_latency_sec,
        "apollo_control_process_alive": apollo_control_process_alive,
        "apollo_control_log_nonempty": apollo_control_log_nonempty,
        "apollo_control_raw_row_count": apollo_control_raw_row_count,
        "control_zero_hold_only": control_zero_hold_only,
        "control_last_input_trajectory_seq": control_last_input_trajectory_seq,
        "control_last_input_trajectory_point_count": control_last_input_trajectory_point_count,
        "control_first_nonzero_planning_seen_by_control_at": control_first_nonzero_planning_seen_by_control_at,
        "control_last_output_ts": control_last_output_ts,
        "control_output_zero_hold_ratio": control_output_zero_hold_ratio,
        "control_final_process_alive": control_final_process_alive,
        "control_final_modules_status": control_final_modules_status,
        "control_started_pid_seen": control_started_pid_seen,
        "control_survived_5s": control_survived_5s,
        "control_survived_10s": control_survived_10s,
        "control_present_after_first_nonzero_planning": control_present_after_first_nonzero_planning,
        "control_deferred_survival": control_deferred_survival,
        "control_crash_detected": control_crash_summary["control_crash_detected"],
        "control_crash_reason": control_crash_summary["control_crash_reason"],
        "control_crash_log_tail": control_crash_summary["control_crash_log_tail"],
        "control_deferred_log_path": control_crash_summary["control_deferred_log_path"],
        "control_deferred_start_mode": control_crash_summary["control_deferred_start_mode"],
        "control_output_materialized": control_crash_summary["control_output_materialized"],
        "control_deferred_start": control_crash_summary["control_deferred_start"],
        "apollo_control_info_path": control_info_summary["apollo_control_info_path"],
        "apollo_control_info_exists": control_info_summary["apollo_control_info_exists"],
        "apollo_control_chassis_not_ready_count": control_info_summary[
            "apollo_control_chassis_not_ready_count"
        ],
        "apollo_control_waiting_for_planning_count": control_info_summary[
            "apollo_control_waiting_for_planning_count"
        ],
        "apollo_control_info_tail": control_info_summary["apollo_control_info_tail"],
    }


def _build_state_timeline(
    *,
    routing_rows: List[Dict[str, Any]],
    planning_rows: List[Dict[str, Any]],
    control_events: List[Dict[str, Any]],
    debug_rows: List[Dict[str, Any]],
    route_metrics: Dict[str, Any],
    routing_success_count: int,
) -> Tuple[List[Dict[str, Any]], str]:
    timeline: List[Dict[str, Any]] = []
    timestamps: List[float] = []

    def _event_ts(row: Dict[str, Any]) -> Optional[float]:
        # Prefer simulation time when available. Several Apollo/CARLA debug
        # streams carry wall-clock `timestamp` and sim-time `sim_time_sec`;
        # mixing both in one timeline creates meaningless billion-second
        # route-establishment latencies.
        for key in ("sim_time_sec", "ts_sec", "timestamp"):
            value = safe_float(row.get(key))
            if value is not None:
                return value
        return None

    for rows in (routing_rows, planning_rows, control_events):
        for row in rows:
            ts = _event_ts(row)
            if ts is not None:
                timestamps.append(ts)
    for row in debug_rows:
        ts = _event_ts(row)
        if ts is not None:
            timestamps.append(ts)
    base_ts = min(timestamps) if timestamps else 0.0

    def _append(state: str, ts: Optional[float], reason: str) -> None:
        if ts is None:
            return
        if timeline and timeline[-1]["state"] == state:
            return
        timeline.append({"state": state, "entered_ts_sec": float(ts), "reason": reason})

    _append("CARLA_READY", base_ts, "earliest_event_stream_timestamp")
    _append("MAP_READY", base_ts, "scenario_metadata_available")

    routing_ready_ts = next((_event_ts(row) for row in routing_rows if bool(row.get("routing_request_sent"))), None)
    _append("ROUTING_READY", routing_ready_ts, "routing_request_sent")

    route_established_ts = routing_ready_ts if routing_success_count > 0 else None
    _append("ROUTE_ESTABLISHED", route_established_ts, "routing_success_count_gt_zero")

    planning_ready_ts = next(
        (
            _event_ts(row)
            for row in planning_rows
            if int(row.get("trajectory_point_count", 0) or 0) > 0
        ),
        None,
    )
    _append("PLANNING_READY", planning_ready_ts, "first_nonzero_planning_trajectory")

    control_ready_ts = next(
        (
            _event_ts(row)
            for row in control_events
            if bool(row.get("control_used_planning_trajectory"))
        ),
        None,
    )
    _append("CONTROL_READY", control_ready_ts, "control_consume_event_stream")

    cruise_active_ts = None
    if debug_rows:
        start_x = safe_float(debug_rows[0].get("map_x"))
        start_y = safe_float(debug_rows[0].get("map_y"))
        if start_x is not None and start_y is not None:
            for row in debug_rows:
                ts_sec = safe_float(row.get("ts_sec"))
                map_x = safe_float(row.get("map_x"))
                map_y = safe_float(row.get("map_y"))
                speed_mps = safe_float(row.get("speed_mps"))
                if ts_sec is None or map_x is None or map_y is None or speed_mps is None:
                    continue
                moved = math.sqrt(((map_x - start_x) ** 2) + ((map_y - start_y) ** 2))
                if moved >= 5.0 and speed_mps >= 1.0:
                    cruise_active_ts = float(ts_sec)
                    break
    _append("CRUISE_ACTIVE", cruise_active_ts, "distance_ge_5m_and_speed_ge_1mps")

    completion_ratio = safe_float(route_metrics.get("route_completion_ratio"))
    final_goal_distance_m = safe_float(route_metrics.get("final_goal_distance_m"))
    completed = bool(
        (completion_ratio is not None and completion_ratio >= 0.95)
        or (final_goal_distance_m is not None and final_goal_distance_m <= CORE_ACCEPTANCE_POLICY["completion_goal_distance_m_max"])
    )
    end_ts = None
    if debug_rows:
        end_ts = _event_ts(debug_rows[-1])
    elif control_events:
        end_ts = _event_ts(control_events[-1])
    elif planning_rows:
        end_ts = _event_ts(planning_rows[-1])
    elif routing_rows:
        end_ts = _event_ts(routing_rows[-1])
    if completed:
        _append("ROUTE_COMPLETED", end_ts, "completion_threshold_met")
        failure_stage = "ROUTE_COMPLETED"
    else:
        _append("ROUTE_FAILED", end_ts if end_ts is not None else base_ts, "run_ended_without_completion")
        reached = {item["state"] for item in timeline}
        if "ROUTING_READY" not in reached:
            failure_stage = "MAP_READY -> ROUTING_READY"
        elif "ROUTE_ESTABLISHED" not in reached:
            failure_stage = "ROUTING_READY -> ROUTE_ESTABLISHED"
        elif "PLANNING_READY" not in reached:
            failure_stage = "ROUTE_ESTABLISHED -> PLANNING_READY"
        elif "CONTROL_READY" not in reached:
            failure_stage = "PLANNING_READY -> CONTROL_READY"
        elif "CRUISE_ACTIVE" not in reached:
            failure_stage = "CONTROL_READY -> CRUISE_ACTIVE"
        else:
            failure_stage = "CRUISE_ACTIVE"
    return timeline, failure_stage


def _compute_manifest_completeness(run_dir: Path, *, enable_recording: bool) -> Tuple[float, List[str]]:
    required = [
        run_dir / "effective.yaml",
        run_dir / "summary.provisional.json",
        run_dir / "artifacts" / "scenario_metadata.json",
        run_dir / "artifacts" / "planning_topic_debug_summary.finalized.json",
        run_dir / "artifacts" / "control_trajectory_consume_summary.json",
        run_dir / "artifacts" / "bridge_health_summary.finalized.json",
        run_dir / "artifacts" / "town01_route_health_state_timeline.jsonl",
        run_dir / "summary.json",
    ]
    if enable_recording:
        required.extend(
            [
                run_dir / "artifacts" / "dreamview_recording_status.json",
                run_dir / "artifacts" / "dreamview_capture_manifest.json",
            ]
        )
    missing = [str(path) for path in required if not path.exists()]
    present = len(required) - len(missing)
    completeness = (float(present) / float(len(required))) if required else 1.0
    return completeness, missing


def finalize_town01_run(run_dir: Path, *, flags: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    run_dir = run_dir.expanduser().resolve()
    artifacts_dir = run_dir / "artifacts"
    provisional_path = run_dir / "summary.provisional.json"
    summary_path = run_dir / "summary.json"
    if provisional_path.exists():
        summary_data = load_json(provisional_path)
    else:
        summary_data = load_json(summary_path)
    write_json(provisional_path, summary_data)

    effective_cfg = load_effective_cfg(run_dir)
    scenario_meta = load_json(artifacts_dir / "scenario_metadata.json")
    flags = _resolved_runtime_flags(flags, effective_cfg, scenario_meta)
    route_id = route_id_from_metadata(scenario_meta)
    runtime_mode = _runtime_mode_snapshot(flags, effective_cfg)
    runtime_contract = evaluate_runtime_contract(
        str(_nested_get(effective_cfg, "run", "capability_profile") or scenario_meta.get("capability_profile") or ""),
        runtime_mode,
    )
    if route_id and not scenario_meta.get("route_id"):
        scenario_meta["route_id"] = route_id
    scenario_meta.update(
        {
            key: value
            for key, value in runtime_mode.items()
            if value is not None or key.startswith("enable_")
        }
    )
    scenario_meta["runtime_contract"] = runtime_contract
    write_json(artifacts_dir / "scenario_metadata.json", scenario_meta)
    bridge_health_live = load_json(artifacts_dir / "bridge_health_summary.json")
    bridge_healthcheck = load_json(artifacts_dir / "cyber_bridge_healthcheck.json")
    bridge_runtime_summary = _extract_bridge_runtime_summary(artifacts_dir)
    bridge_transport_summary = _extract_bridge_transport_summary(artifacts_dir)
    command_materialization_summary = _extract_command_materialization_summary(artifacts_dir)
    cyber_stats = load_json(artifacts_dir / "cyber_bridge_stats.json")
    direct_bridge_stats = load_json(artifacts_dir / "direct_bridge_stats.json")
    planning_rows = _normalize_planning_rows(load_jsonl(artifacts_dir / "planning_topic_debug.jsonl"))
    route_debug_rows = load_jsonl(artifacts_dir / "stage5_apollo_reference_line_debug.jsonl")
    routing_rows = load_jsonl(artifacts_dir / "routing_event_debug.jsonl")
    reroute_rows = load_jsonl(artifacts_dir / "reroute_decision_debug.jsonl")
    goal_rows = load_jsonl(artifacts_dir / "goal_validity_debug.jsonl")
    live_control_rows = load_jsonl(artifacts_dir / "control_trajectory_consume_debug_live.jsonl")
    debug_rows = _read_debug_timeseries(artifacts_dir / "debug_timeseries.csv")

    control_log_path = artifacts_dir / "apollo_control.INFO"
    miss_rows = _parse_control_no_trajectory_log(control_log_path)
    control_events, control_summary = _build_control_consume_events(planning_rows, live_control_rows, miss_rows)
    write_jsonl(artifacts_dir / "control_trajectory_consume_debug.jsonl", control_events)
    write_json(artifacts_dir / "control_trajectory_consume_summary.json", control_summary)

    planning_summary = _planning_summary_from_event_stream(planning_rows)
    write_json(artifacts_dir / "planning_topic_debug_summary.finalized.json", planning_summary)
    planning_trajectory_type_summary = _compute_planning_trajectory_type_summary(
        planning_rows,
        route_debug_rows=route_debug_rows,
    )
    write_json(
        artifacts_dir / "planning_trajectory_type_summary.finalized.json",
        planning_trajectory_type_summary,
    )
    curve_tracking_health_summary = _compute_curve_tracking_health_summary(
        artifacts_dir,
        planning_trajectory_type_summary,
    )
    write_json(
        artifacts_dir / "curve_tracking_health_summary.finalized.json",
        curve_tracking_health_summary,
    )
    curve_semantic_window = _compute_curve_semantic_window(
        artifacts_dir,
        planning_rows,
        planning_trajectory_type_summary,
        curve_tracking_health_summary,
    )
    write_json(
        artifacts_dir / "curve_semantic_window.finalized.json",
        curve_semantic_window,
    )
    planning_control_alignment = _compute_planning_control_alignment(planning_rows, live_control_rows, control_events)
    write_json(artifacts_dir / "planning_control_alignment.finalized.json", planning_control_alignment)
    control_handoff_summary = _compute_control_handoff_summary(
        artifacts_dir=artifacts_dir,
        routing_rows=routing_rows,
        planning_rows=planning_rows,
        live_control_rows=live_control_rows,
        bridge_health_live=bridge_health_live,
    )
    write_json(artifacts_dir / "control_handoff_summary.json", control_handoff_summary)
    carla_bootstrap_summary = _extract_carla_bootstrap_summary(run_dir)
    write_json(artifacts_dir / "carla_bootstrap_summary.json", carla_bootstrap_summary)

    route_metrics = _compute_route_metrics(run_dir, summary_data, scenario_meta, debug_rows)
    longitudinal_tracking_health_summary = _compute_longitudinal_tracking_health_summary(
        effective_cfg,
        debug_rows,
    )
    write_json(
        artifacts_dir / "longitudinal_tracking_health_summary.finalized.json",
        longitudinal_tracking_health_summary,
    )
    routing_request_count = max(
        safe_int(cyber_stats.get("routing_request_count")) or 0,
        sum(1 for row in routing_rows if bool(row.get("routing_request_sent"))),
    )
    routing_success_count = safe_int(cyber_stats.get("routing_success_count")) or 0
    direct_control_apply_count = safe_int(direct_bridge_stats.get("control_apply_count")) or 0
    direct_control_apply_fail_count = safe_int(direct_bridge_stats.get("control_apply_fail_count")) or 0
    direct_control_apply_summary = _compute_direct_control_apply_summary(artifacts_dir, direct_bridge_stats)
    direct_control_apply_count = int(direct_control_apply_summary["apply_count"])
    direct_control_apply_fail_count = int(direct_control_apply_summary["apply_fail_count"])
    write_json(
        artifacts_dir / "direct_control_apply_summary.finalized.json",
        direct_control_apply_summary,
    )
    bridge_runtime_preflight_status = str(
        bridge_runtime_summary.get("bridge_runtime_preflight_status") or "unknown"
    )
    bridge_runtime_import_ok = bridge_runtime_summary.get("bridge_runtime_import_ok")
    bridge_runtime_import_error = bridge_runtime_summary.get("bridge_runtime_import_error")
    bridge_runtime_dependency_probe_status = str(
        bridge_runtime_summary.get("bridge_runtime_dependency_probe_status") or "unknown"
    )
    bridge_runtime_missing_shared_libs = list(bridge_runtime_summary.get("bridge_runtime_missing_shared_libs") or [])
    bridge_runtime_missing_python_modules = list(
        bridge_runtime_summary.get("bridge_runtime_missing_python_modules") or []
    )
    bridge_stats_materialized = bool(bridge_healthcheck.get("stats_exists")) or bool(cyber_stats)
    routing_first_request_at = next(
        (
            safe_float(row.get("timestamp"))
            for row in routing_rows
            if bool(row.get("routing_request_sent")) and safe_float(row.get("timestamp")) is not None
        ),
        None,
    )
    planning_first_message_at = next(
        (
            safe_float(row.get("timestamp"))
            for row in planning_rows
            if safe_float(row.get("timestamp")) is not None
        ),
        None,
    )
    routing_materialized = routing_request_count > 0
    planning_materialized = int(planning_summary.get("total_messages_received", 0) or 0) > 0
    if bridge_runtime_preflight_status == "bridge_runtime_import_failed":
        materialization_status = "bridge_runtime_import_failed"
    elif not routing_materialized:
        materialization_status = "bridge_alive_no_routing"
    elif not planning_materialized:
        materialization_status = "routing_present_no_planning"
    elif control_handoff_summary["control_handoff_status"] == "control_process_missing":
        materialization_status = "planning_present_control_missing"
    else:
        materialization_status = "planning_control_materialized"
    route_metrics["route_established"] = routing_success_count > 0
    max_speed_mps = _run_max_speed(summary_data, debug_rows)
    direct_metric_consistency = _compute_direct_metric_consistency(
        bridge_transport_summary,
        route_metrics,
        max_speed_mps,
        direct_control_apply_summary,
    )
    write_json(
        artifacts_dir / "direct_metric_consistency.finalized.json",
        direct_metric_consistency,
    )
    low_speed_creep = _compute_low_speed_creep_metrics(
        artifacts_dir / "debug_timeseries.csv",
        start_after_sec=20.0,
        speed_below_mps=1.0,
        front_gap_above_m=20.0,
        require_routing_established=True,
        ignore_when_terminal_stop_hold_active=True,
    )
    lateral_metrics = _compute_basic_lateral_metrics(artifacts_dir / "bridge_control_decode.jsonl")

    state_timeline, failure_stage = _build_state_timeline(
        routing_rows=routing_rows,
        planning_rows=planning_rows,
        control_events=control_events,
        debug_rows=debug_rows,
        route_metrics=route_metrics,
        routing_success_count=routing_success_count,
    )
    state_timeline_path = artifacts_dir / "town01_route_health_state_timeline.jsonl"
    write_jsonl(state_timeline_path, state_timeline)

    if state_timeline:
        base_ts = float(state_timeline[0]["entered_ts_sec"])
        route_established_ts = next(
            (float(item["entered_ts_sec"]) for item in state_timeline if item["state"] == "ROUTE_ESTABLISHED"),
            None,
        )
        route_establishment_latency_sec = (
            float(route_established_ts - base_ts) if route_established_ts is not None else None
        )
    else:
        route_establishment_latency_sec = None

    reroute_reason_counts: Dict[str, int] = {}
    for row in reroute_rows:
        reason = str(row.get("reroute_reason") or "")
        if not reason:
            continue
        reroute_reason_counts[reason] = int(reroute_reason_counts.get(reason, 0)) + 1
    if not reroute_reason_counts:
        for key, value in ((bridge_health_live.get("reroute_reason_counts") or {}).items()):
            reroute_reason_counts[str(key)] = int(value)

    invalid_goal_count = sum(1 for row in goal_rows if bool(row.get("invalid_goal")))
    if invalid_goal_count <= 0:
        invalid_goal_count = max(
            safe_int(bridge_health_live.get("invalid_goal_count")) or 0,
            safe_int(cyber_stats.get("invalid_goal_count")) or 0,
        )

    total_control_cycles = int(control_summary.get("total_control_cycles", 0) or 0)
    control_used_planning_ratio = (
        float(control_summary.get("total_used_planning_trajectory", 0) or 0) / float(total_control_cycles)
        if total_control_cycles > 0
        else None
    )
    whether_vehicle_moved = bool(
        max_speed_mps > 0.5 or (safe_float(route_metrics.get("route_distance_achieved_m")) or 0.0) >= 5.0
    )

    manifest_completeness_before, missing_before = _compute_manifest_completeness(
        run_dir,
        enable_recording=bool(flags.get("enable_recording")),
    )
    recording_status = load_json(artifacts_dir / "dreamview_recording_status.json")
    recording_manifest = load_json(artifacts_dir / "dreamview_capture_manifest.json")
    recording_ok = True
    if flags.get("enable_recording"):
        recording_ok = bool(recording_status) and bool(recording_manifest) and str(recording_status.get("recording_status") or "") not in {"", "failed"}

    planning_nonzero_ratio_check = _planning_nonzero_ratio_check(
        planning_rows,
        planning_summary,
        route_established_ts=route_established_ts,
    )

    checks: Dict[str, Any] = {
        "bridge_runtime_import": {
            "actual": bridge_runtime_import_ok,
            "status": bridge_runtime_preflight_status,
            "error": bridge_runtime_import_error,
            "ok": bridge_runtime_import_ok is not False,
        },
        "routing_request_count": {
            "actual": routing_request_count,
            "threshold": CORE_ACCEPTANCE_POLICY["routing_request_count_min"],
            "ok": routing_request_count >= CORE_ACCEPTANCE_POLICY["routing_request_count_min"],
        },
        "route_establishment_latency_sec": {
            "actual": route_establishment_latency_sec,
            "threshold": CORE_ACCEPTANCE_POLICY["route_establishment_latency_sec_max"],
            "ok": route_establishment_latency_sec is not None and route_establishment_latency_sec <= CORE_ACCEPTANCE_POLICY["route_establishment_latency_sec_max"],
        },
        "planning_nonzero_ratio": planning_nonzero_ratio_check,
        "control_used_planning_ratio": {
            "actual": control_used_planning_ratio,
            "threshold": CORE_ACCEPTANCE_POLICY["control_used_planning_ratio_min"],
        },
        "invalid_goal_count": {
            "actual": invalid_goal_count,
            "threshold": CORE_ACCEPTANCE_POLICY["invalid_goal_count_max"],
        },
        "long_phase_invalid_goal_skip": {
            "actual": int(reroute_reason_counts.get("long_phase_invalid_goal_skip", 0) or 0),
            "threshold": CORE_ACCEPTANCE_POLICY["long_phase_invalid_goal_skip_max"],
        },
        "unstable_reference_line_skip": {
            "actual": int(reroute_reason_counts.get("long_phase_unstable_reference_line_skip", 0) or 0),
            "threshold": CORE_ACCEPTANCE_POLICY["unstable_reference_line_skip_max"],
        },
        "control_no_trajectory_count": {
            "actual": int(control_summary.get("total_no_trajectory_events", 0) or 0),
            "threshold": CORE_ACCEPTANCE_POLICY["control_no_trajectory_count_max"],
        },
        "route_distance_achieved_m": {
            "actual": safe_float(route_metrics.get("route_distance_achieved_m")),
            "threshold": CORE_ACCEPTANCE_POLICY["route_distance_achieved_m_min"],
        },
        "route_completion_ratio": {
            "actual": safe_float(route_metrics.get("route_completion_ratio")),
            "threshold": CORE_ACCEPTANCE_POLICY["route_completion_ratio_min"],
        },
        "max_speed_mps": {
            "actual": max_speed_mps,
            "threshold": CORE_ACCEPTANCE_POLICY["max_speed_mps_min"],
        },
        "whether_vehicle_moved": {
            "actual": whether_vehicle_moved,
            "threshold": True,
        },
        "low_speed_creep_duration_sec": {
            "actual": safe_float(low_speed_creep.get("longest_streak_duration_sec")),
            "threshold": CORE_ACCEPTANCE_POLICY["low_speed_creep_duration_sec_max"],
        },
        "lateral_metrics": lateral_metrics,
        "longitudinal_tracking_health_summary": longitudinal_tracking_health_summary,
        "planning_trajectory_type_summary": planning_trajectory_type_summary,
        "curve_tracking_health_summary": curve_tracking_health_summary,
        "curve_semantic_window": curve_semantic_window,
        "planning_control_alignment": planning_control_alignment,
        "control_handoff": control_handoff_summary,
        "summary_status": {
            "actual": "finalized",
            "threshold": "finalized",
            "ok": True,
        },
        "finalized_from_event_stream": {
            "actual": True,
            "threshold": True,
            "ok": True,
        },
        "summary_written_successfully": {
            "actual": True,
            "threshold": True,
            "ok": True,
        },
        "manifest_completeness": {
            "actual": manifest_completeness_before,
            "threshold": 1.0,
        },
        "recording": {
            "enabled": bool(flags.get("enable_recording")),
            "ok": recording_ok,
            "status": recording_status,
            "manifest": recording_manifest,
        },
        "offlane_departure_risk": {
            "actual": None,
            "ok": True,
            "status": "unavailable",
        },
    }
    checks["planning_nonzero_ratio"]["ok"] = (
        checks["planning_nonzero_ratio"]["actual"] is not None
        and checks["planning_nonzero_ratio"]["actual"] >= checks["planning_nonzero_ratio"]["threshold"]
    )
    checks["control_used_planning_ratio"]["ok"] = (
        checks["control_used_planning_ratio"]["actual"] is not None
        and checks["control_used_planning_ratio"]["actual"] >= checks["control_used_planning_ratio"]["threshold"]
    )
    checks["invalid_goal_count"]["ok"] = checks["invalid_goal_count"]["actual"] <= checks["invalid_goal_count"]["threshold"]
    checks["long_phase_invalid_goal_skip"]["ok"] = (
        checks["long_phase_invalid_goal_skip"]["actual"] <= checks["long_phase_invalid_goal_skip"]["threshold"]
    )
    checks["unstable_reference_line_skip"]["ok"] = (
        checks["unstable_reference_line_skip"]["actual"] <= checks["unstable_reference_line_skip"]["threshold"]
    )
    checks["control_no_trajectory_count"]["ok"] = (
        checks["control_no_trajectory_count"]["actual"] <= checks["control_no_trajectory_count"]["threshold"]
    )
    checks["route_distance_achieved_m"]["ok"] = (
        checks["route_distance_achieved_m"]["actual"] is not None
        and checks["route_distance_achieved_m"]["actual"] >= checks["route_distance_achieved_m"]["threshold"]
    )
    checks["route_completion_ratio"]["ok"] = (
        checks["route_completion_ratio"]["actual"] is not None
        and checks["route_completion_ratio"]["actual"] >= checks["route_completion_ratio"]["threshold"]
    )
    checks["max_speed_mps"]["ok"] = checks["max_speed_mps"]["actual"] >= checks["max_speed_mps"]["threshold"]
    checks["whether_vehicle_moved"]["ok"] = bool(checks["whether_vehicle_moved"]["actual"])
    checks["low_speed_creep_duration_sec"]["ok"] = (
        checks["low_speed_creep_duration_sec"]["actual"] is not None
        and checks["low_speed_creep_duration_sec"]["actual"] <= checks["low_speed_creep_duration_sec"]["threshold"]
    )
    if flags.get("enable_lateral"):
        lat_ok = (
            safe_float(lateral_metrics.get("raw_steer_saturated_ratio")) is not None
            and safe_float(lateral_metrics.get("raw_steer_saturated_ratio")) <= CORE_ACCEPTANCE_POLICY["lateral_max_raw_steer_saturated_ratio"]
            and safe_float(lateral_metrics.get("longest_continuous_saturation_sec")) is not None
            and safe_float(lateral_metrics.get("longest_continuous_saturation_sec")) <= CORE_ACCEPTANCE_POLICY["lateral_max_longest_saturation_sec"]
        )
    else:
        lat_ok = True
    if flags.get("enable_guard"):
        guard_ok = (
            safe_float(lateral_metrics.get("guard_trigger_ratio")) is not None
            and safe_float(lateral_metrics.get("guard_trigger_ratio")) <= CORE_ACCEPTANCE_POLICY["guard_trigger_ratio_max"]
        ) if lateral_metrics.get("available") else True
    else:
        guard_ok = True
    core_ok = all(
        checks[name]["ok"]
        for name in (
            "routing_request_count",
            "route_establishment_latency_sec",
            "planning_nonzero_ratio",
            "control_used_planning_ratio",
            "invalid_goal_count",
            "long_phase_invalid_goal_skip",
            "unstable_reference_line_skip",
            "control_no_trajectory_count",
            "route_distance_achieved_m",
            "route_completion_ratio",
            "max_speed_mps",
            "whether_vehicle_moved",
            "low_speed_creep_duration_sec",
            "summary_status",
            "finalized_from_event_stream",
            "summary_written_successfully",
        )
    )
    route_established = bool(route_metrics.get("route_established"))
    control_ready = any(item["state"] == "CONTROL_READY" for item in state_timeline)
    cruise_active = any(item["state"] == "CRUISE_ACTIVE" for item in state_timeline)
    if not route_established:
        route_health_label = "route_not_established"
    elif not control_ready or not cruise_active:
        route_health_label = "route_established_but_no_control_progress"
    elif not core_ok or not lat_ok or not guard_ok or not recording_ok:
        route_health_label = "route_established_but_behavior_unhealthy"
    elif (safe_float(route_metrics.get("route_completion_ratio")) or 0.0) >= CORE_ACCEPTANCE_POLICY["route_health_pass_completion_ratio_min"]:
        route_health_label = "route_health_pass"
    else:
        route_health_label = "route_health_candidate"

    acceptance = {
        "success": bool(core_ok and lat_ok and guard_ok and recording_ok and manifest_completeness_before >= 1.0),
        "fail_reason": None,
        "failure_codes": [],
        "failure_stage": failure_stage,
        "checks": checks,
        "route_health_label": route_health_label,
        "materialization_status": materialization_status,
    }
    for key, value in checks.items():
        if isinstance(value, dict) and value.get("ok") is False:
            acceptance["failure_codes"].append(key.upper())
    if not lat_ok:
        acceptance["failure_codes"].append("LATERAL_METRICS")
    if not guard_ok:
        acceptance["failure_codes"].append("GUARD_TRIGGER_RATIO")
    if not recording_ok and flags.get("enable_recording"):
        acceptance["failure_codes"].append("RECORDING_INCOMPLETE")
    if manifest_completeness_before < 1.0:
        acceptance["failure_codes"].append("MANIFEST_INCOMPLETE")
    acceptance["fail_reason"] = acceptance["failure_codes"][0] if acceptance["failure_codes"] else None

    finalized_bridge_health = dict(bridge_health_live)
    finalized_bridge_health.update(
        {
            "summary_status": "finalized",
            "finalized_from_event_stream": True,
            "bridge_runtime_preflight_status": bridge_runtime_preflight_status,
            "bridge_runtime_import_ok": bridge_runtime_import_ok,
            "bridge_runtime_import_error": bridge_runtime_import_error,
            "bridge_runtime_dependency_probe_status": bridge_runtime_dependency_probe_status,
            "bridge_runtime_missing_shared_libs": bridge_runtime_missing_shared_libs,
            "bridge_runtime_missing_python_modules": bridge_runtime_missing_python_modules,
            "bridge_stats_materialized": bridge_stats_materialized,
            "routing_materialized": routing_materialized,
            "planning_materialized": planning_materialized,
            "routing_first_request_at": routing_first_request_at,
            "planning_first_message_at": planning_first_message_at,
            "planning_first_nonempty_at": control_handoff_summary["planning_first_nonempty_at"],
            "materialization_status": materialization_status,
            "routing_request_count": routing_request_count,
            "routing_phase_counts": bridge_health_live.get("routing_phase_counts") or cyber_stats.get("routing_phase_counts") or {},
            "reroute_reason_counts": reroute_reason_counts,
            "invalid_goal_count": invalid_goal_count,
            "control_no_trajectory_count": int(control_summary.get("total_no_trajectory_events", 0) or 0),
            "control_used_planning_ratio": control_used_planning_ratio,
            "command_materialization": command_materialization_summary["command_materialization"],
        }
    )
    write_json(artifacts_dir / "bridge_health_summary.finalized.json", finalized_bridge_health)

    planning_bridge_context = _planning_bridge_context(planning_trajectory_type_summary)
    final_summary = dict(summary_data)
    final_summary["summary_status"] = "finalized"
    final_summary["finalized_from_event_stream"] = True
    final_summary["summary_written_successfully"] = True
    final_summary["town01_mainline_flags"] = dict(flags)
    final_summary["runtime_mode"] = runtime_mode
    final_summary["runtime_contract"] = runtime_contract
    final_summary["route_id"] = route_id
    final_summary["acceptance"] = acceptance
    final_summary["route_health"] = {
        **route_metrics,
        "label": route_health_label,
        "raw_steer_saturated_ratio": safe_float(lateral_metrics.get("raw_steer_saturated_ratio")),
        "longest_continuous_saturation_sec": safe_float(lateral_metrics.get("longest_continuous_saturation_sec")),
        "guard_trigger_ratio": safe_float(lateral_metrics.get("guard_trigger_ratio")),
        "path_fallback_count": safe_int(planning_trajectory_type_summary.get("path_fallback_count")),
        "first_path_fallback_seq": safe_int(planning_trajectory_type_summary.get("first_path_fallback_seq")),
        "first_path_fallback_lon_diff": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_lon_diff")
        ),
        "first_path_fallback_trigger_seq": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_trigger_seq")
        ),
        "first_path_fallback_trigger_seq_gap": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_trigger_seq_gap")
        ),
        "first_path_fallback_trigger_reason_family": planning_trajectory_type_summary.get(
            "first_path_fallback_trigger_reason_family"
        ),
        "first_path_fallback_trigger_reason_streak_length": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_trigger_reason_streak_length")
        ),
        "first_path_fallback_trigger_lon_diff": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_trigger_lon_diff")
        ),
        "first_path_fallback_trigger_relative_time_min_sec": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_trigger_relative_time_min_sec")
        ),
        "first_path_fallback_trigger_first_point_v": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_trigger_first_point_v")
        ),
        "first_path_fallback_trigger_total_path_length": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_trigger_total_path_length")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_count": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_normal_count")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_start": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_start")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_end": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_end")
        ),
        "first_path_fallback_precurrent_time_smaller_rel_min_min": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_rel_min_min")
        ),
        "first_path_fallback_precurrent_time_smaller_rel_min_max": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_rel_min_max")
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_min": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_first_point_v_min")
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_max": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_first_point_v_max")
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_min": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_point_count_min")
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_max": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_point_count_max")
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_min": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_path_length_min")
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_max": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_path_length_max")
        ),
        "first_path_fallback_prev_normal_relative_time_min_sec": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_prev_normal_relative_time_min_sec")
        ),
        "first_path_fallback_prev_normal_first_point_v": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_prev_normal_first_point_v")
        ),
        "first_path_fallback_prev_normal_total_path_length": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_prev_normal_total_path_length")
        ),
        "first_path_fallback_first_point_v": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_first_point_v")
        ),
        "first_path_fallback_total_path_length": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_total_path_length")
        ),
        "first_path_fallback_cluster_length": safe_int(
            planning_trajectory_type_summary.get("first_path_fallback_cluster_length")
        ),
        "first_path_fallback_path_length_collapse_ratio": safe_float(
            planning_trajectory_type_summary.get("first_path_fallback_path_length_collapse_ratio")
        ),
        "first_recovery_after_path_fallback_seq": safe_int(
            planning_trajectory_type_summary.get("first_recovery_after_path_fallback_seq")
        ),
        "first_relapse_after_recovery_seq": safe_int(
            planning_trajectory_type_summary.get("first_relapse_after_recovery_seq")
        ),
        "first_relapse_after_recovery_reason_family": planning_trajectory_type_summary.get(
            "first_relapse_after_recovery_reason_family"
        ),
        "first_relapse_after_recovery_lon_diff": safe_float(
            planning_trajectory_type_summary.get("first_relapse_after_recovery_lon_diff")
        ),
        "first_relapse_prev_normal_first_point_v": safe_float(
            planning_trajectory_type_summary.get("first_relapse_prev_normal_first_point_v")
        ),
        "first_relapse_prev_normal_total_path_length": safe_float(
            planning_trajectory_type_summary.get("first_relapse_prev_normal_total_path_length")
        ),
        "first_relapse_after_recovery_first_point_v": safe_float(
            planning_trajectory_type_summary.get("first_relapse_after_recovery_first_point_v")
        ),
        "first_relapse_after_recovery_total_path_length": safe_float(
            planning_trajectory_type_summary.get("first_relapse_after_recovery_total_path_length")
        ),
        "first_relapse_cluster_length": safe_int(
            planning_trajectory_type_summary.get("first_relapse_cluster_length")
        ),
        "first_relapse_path_length_collapse_ratio": safe_float(
            planning_trajectory_type_summary.get("first_relapse_path_length_collapse_ratio")
        ),
        "path_fallback_count_after_first_recovery": safe_int(
            planning_trajectory_type_summary.get("path_fallback_count_after_first_recovery")
        ),
        "max_consecutive_path_fallback_after_first_recovery": safe_int(
            planning_trajectory_type_summary.get("max_consecutive_path_fallback_after_first_recovery")
        ),
        "normal_count_after_first_recovery": safe_int(
            planning_trajectory_type_summary.get("normal_count_after_first_recovery")
        ),
        "reentered_path_fallback_after_recovery": bool(
            planning_trajectory_type_summary.get("reentered_path_fallback_after_recovery")
        ),
        "max_consecutive_path_fallback_length": safe_int(
            planning_trajectory_type_summary.get("max_consecutive_path_fallback_length")
        ),
        "final_path_fallback_suffix_start_seq": safe_int(
            planning_trajectory_type_summary.get("final_path_fallback_suffix_start_seq")
        ),
        "final_path_fallback_suffix_length": safe_int(
            planning_trajectory_type_summary.get("final_path_fallback_suffix_length")
        ),
        "final_normal_tail_length": safe_int(planning_trajectory_type_summary.get("final_normal_tail_length")),
        "recovered_after_path_fallback": bool(planning_trajectory_type_summary.get("recovered_after_path_fallback")),
        "persistent_path_fallback_at_end": bool(planning_trajectory_type_summary.get("persistent_path_fallback_at_end")),
        "last_trajectory_type": planning_trajectory_type_summary.get("last_trajectory_type"),
        "routing_target_speed_mps": safe_float(longitudinal_tracking_health_summary.get("routing_target_speed_mps")),
        "planning_default_cruise_speed_mps": safe_float(
            longitudinal_tracking_health_summary.get("planning_default_cruise_speed_mps")
        ),
        "speed_abs_p95": safe_float(longitudinal_tracking_health_summary.get("speed_abs_p95")),
        "speed_ratio_to_target_p95": safe_float(
            longitudinal_tracking_health_summary.get("speed_ratio_to_target_p95")
        ),
        "throttle_p95": safe_float(longitudinal_tracking_health_summary.get("throttle_p95")),
        "brake_p95": safe_float(longitudinal_tracking_health_summary.get("brake_p95")),
        "throttle_brake_overlap_ratio": safe_float(
            longitudinal_tracking_health_summary.get("throttle_brake_overlap_ratio")
        ),
        "acc_cmd_p95": safe_float(longitudinal_tracking_health_summary.get("acc_cmd_p95")),
        "speed_gain_limited_ratio": safe_float(
            longitudinal_tracking_health_summary.get("speed_gain_limited_ratio")
        ),
        "persistent_low_speed_at_end": longitudinal_tracking_health_summary.get("persistent_low_speed_at_end"),
        "apollo_raw_steer_abs_p95": safe_float(curve_tracking_health_summary.get("apollo_raw_steer_abs_p95")),
        "apollo_raw_steer_abs_p99": safe_float(curve_tracking_health_summary.get("apollo_raw_steer_abs_p99")),
        "apollo_raw_steer_over95_ratio": safe_float(
            curve_tracking_health_summary.get("apollo_raw_steer_over95_ratio")
        ),
        "apollo_simple_lat_lateral_error_abs_p95": safe_float(
            curve_tracking_health_summary.get("apollo_simple_lat_lateral_error_abs_p95")
        ),
        "apollo_simple_lat_lateral_error_abs_p99": safe_float(
            curve_tracking_health_summary.get("apollo_simple_lat_lateral_error_abs_p99")
        ),
        "apollo_simple_lat_heading_error_abs_p95": safe_float(
            curve_tracking_health_summary.get("apollo_simple_lat_heading_error_abs_p95")
        ),
        "apollo_simple_lat_heading_error_abs_p99": safe_float(
            curve_tracking_health_summary.get("apollo_simple_lat_heading_error_abs_p99")
        ),
        "apollo_simple_lat_target_point_kappa_abs_p95": safe_float(
            curve_tracking_health_summary.get("apollo_simple_lat_target_point_kappa_abs_p95")
        ),
        "apollo_simple_lat_target_point_kappa_abs_p99": safe_float(
            curve_tracking_health_summary.get("apollo_simple_lat_target_point_kappa_abs_p99")
        ),
        "mapped_carla_steer_cmd_abs_p95": safe_float(
            curve_tracking_health_summary.get("mapped_carla_steer_cmd_abs_p95")
        ),
        "mapped_carla_steer_cmd_abs_p99": safe_float(
            curve_tracking_health_summary.get("mapped_carla_steer_cmd_abs_p99")
        ),
        "target_front_wheel_angle_deg_abs_p95": safe_float(
            curve_tracking_health_summary.get("target_front_wheel_angle_deg_abs_p95")
        ),
        "target_front_wheel_angle_deg_abs_p99": safe_float(
            curve_tracking_health_summary.get("target_front_wheel_angle_deg_abs_p99")
        ),
        "first_high_steer_seq": safe_int(curve_tracking_health_summary.get("first_high_steer_seq")),
        "first_high_steer_at": safe_float(curve_tracking_health_summary.get("first_high_steer_at")),
        "first_matched_point_too_large_seq": safe_int(
            curve_tracking_health_summary.get("first_matched_point_too_large_seq")
        ),
        "high_steer_before_first_matched_point_too_large": curve_tracking_health_summary.get(
            "high_steer_before_first_matched_point_too_large"
        ),
        "semantic_window_anchor_kind": curve_semantic_window.get("semantic_window_anchor_kind"),
        "semantic_window_anchor_seq": safe_int(curve_semantic_window.get("semantic_window_anchor_seq")),
        "semantic_window_anchor_at": safe_float(curve_semantic_window.get("semantic_window_anchor_at")),
        "first_matched_point_too_large_at": safe_float(curve_semantic_window.get("first_matched_point_too_large_at")),
        "simple_lat_lateral_error_abs_p95_before_failure": safe_float(
            curve_semantic_window.get("simple_lat_lateral_error_abs_p95_before_failure")
        ),
        "simple_lat_heading_error_abs_p95_before_failure": safe_float(
            curve_semantic_window.get("simple_lat_heading_error_abs_p95_before_failure")
        ),
        "target_point_kappa_abs_p95_before_failure": safe_float(
            curve_semantic_window.get("target_point_kappa_abs_p95_before_failure")
        ),
        "mapped_carla_steer_cmd_abs_p95_before_failure": safe_float(
            curve_semantic_window.get("mapped_carla_steer_cmd_abs_p95_before_failure")
        ),
        "simple_lat_lateral_error_abs_p95_before_anchor": safe_float(
            curve_semantic_window.get("simple_lat_lateral_error_abs_p95_before_anchor")
        ),
        "simple_lat_heading_error_abs_p95_before_anchor": safe_float(
            curve_semantic_window.get("simple_lat_heading_error_abs_p95_before_anchor")
        ),
        "target_point_kappa_abs_p95_before_anchor": safe_float(
            curve_semantic_window.get("target_point_kappa_abs_p95_before_anchor")
        ),
        "mapped_carla_steer_cmd_abs_p95_before_anchor": safe_float(
            curve_semantic_window.get("mapped_carla_steer_cmd_abs_p95_before_anchor")
        ),
        **planning_bridge_context,
    }
    final_summary["route_health_label"] = route_health_label
    final_summary["bridge_runtime_preflight"] = bridge_runtime_summary["bridge_runtime_preflight"]
    final_summary["bridge_runtime_preflight_status"] = bridge_runtime_preflight_status
    final_summary["bridge_runtime_import_ok"] = bridge_runtime_import_ok
    final_summary["bridge_runtime_import_error"] = bridge_runtime_import_error
    final_summary["bridge_runtime_dependency_probe_status"] = bridge_runtime_dependency_probe_status
    final_summary["bridge_runtime_missing_shared_libs"] = bridge_runtime_missing_shared_libs
    final_summary["bridge_runtime_missing_python_modules"] = bridge_runtime_missing_python_modules
    final_summary["bridge_transport"] = bridge_transport_summary["bridge_transport"]
    final_summary["transport_mode"] = bridge_transport_summary["transport_mode"]
    final_summary["gt_source"] = bridge_transport_summary["gt_source"]
    final_summary["control_apply_path"] = bridge_transport_summary["control_apply_path"]
    final_summary["tick_owner"] = bridge_transport_summary["tick_owner"]
    final_summary["uses_ros2_gt"] = bridge_transport_summary["uses_ros2_gt"]
    final_summary["uses_ros2_control_bridge"] = bridge_transport_summary["uses_ros2_control_bridge"]
    final_summary["requires_ros2_reexec"] = bridge_transport_summary["requires_ros2_reexec"]
    final_summary["route_command_mode"] = bridge_transport_summary["route_command_mode"]
    final_summary["route_command_path"] = bridge_transport_summary["route_command_path"]
    final_summary["command_materialization"] = command_materialization_summary["command_materialization"]
    final_summary["command_materialization_stage"] = command_materialization_summary["command_materialization_stage"]
    final_summary["command_materialization_layer"] = command_materialization_summary["command_materialization_layer"]
    final_summary["command_materialization_reason"] = command_materialization_summary["command_materialization_reason"]
    final_summary["bridge_stats_materialized"] = bridge_stats_materialized
    final_summary["routing_materialized"] = routing_materialized
    final_summary["planning_materialized"] = planning_materialized
    final_summary["routing_first_request_at"] = routing_first_request_at
    final_summary["planning_first_message_at"] = planning_first_message_at
    final_summary["materialization_status"] = materialization_status
    final_summary["routing_request_count"] = routing_request_count
    final_summary["routing_success_count"] = routing_success_count
    final_summary["direct_control_apply_count"] = direct_control_apply_count
    final_summary["direct_control_apply_fail_count"] = direct_control_apply_fail_count
    final_summary["direct_control_apply_summary"] = direct_control_apply_summary
    final_summary["direct_control_apply_window_status"] = direct_control_apply_summary["window_status"]
    final_summary["direct_control_first_apply_frame"] = direct_control_apply_summary["first_apply_frame"]
    final_summary["direct_control_last_apply_frame"] = direct_control_apply_summary["last_apply_frame"]
    final_summary["direct_control_apply_frame_span"] = direct_control_apply_summary["apply_frame_span"]
    final_summary["direct_control_apply_max_throttle"] = direct_control_apply_summary["max_throttle"]
    final_summary["direct_control_apply_max_speed_mps"] = direct_control_apply_summary["max_speed_mps"]
    final_summary["direct_metric_consistency"] = direct_metric_consistency
    final_summary["direct_metric_consistency_status"] = direct_metric_consistency["status"]
    final_summary["direct_metric_consistency_reasons"] = direct_metric_consistency["reasons"]
    final_summary["route_establishment_latency_sec"] = route_establishment_latency_sec
    final_summary["planning_nonzero_ratio"] = checks["planning_nonzero_ratio"]["actual"]
    final_summary["control_used_planning_ratio"] = control_used_planning_ratio
    final_summary["longitudinal_tracking_health_summary"] = longitudinal_tracking_health_summary
    final_summary["routing_target_speed_mps"] = safe_float(longitudinal_tracking_health_summary.get("routing_target_speed_mps"))
    final_summary["planning_default_cruise_speed_mps"] = safe_float(
        longitudinal_tracking_health_summary.get("planning_default_cruise_speed_mps")
    )
    final_summary["speed_abs_p95"] = safe_float(longitudinal_tracking_health_summary.get("speed_abs_p95"))
    final_summary["speed_ratio_to_target_p95"] = safe_float(
        longitudinal_tracking_health_summary.get("speed_ratio_to_target_p95")
    )
    final_summary["throttle_p95"] = safe_float(longitudinal_tracking_health_summary.get("throttle_p95"))
    final_summary["brake_p95"] = safe_float(longitudinal_tracking_health_summary.get("brake_p95"))
    final_summary["throttle_brake_overlap_ratio"] = safe_float(
        longitudinal_tracking_health_summary.get("throttle_brake_overlap_ratio")
    )
    final_summary["acc_cmd_p95"] = safe_float(longitudinal_tracking_health_summary.get("acc_cmd_p95"))
    final_summary["speed_gain_limited_ratio"] = safe_float(
        longitudinal_tracking_health_summary.get("speed_gain_limited_ratio")
    )
    final_summary["persistent_low_speed_at_end"] = longitudinal_tracking_health_summary.get(
        "persistent_low_speed_at_end"
    )
    final_summary["curve_tracking_health_summary"] = curve_tracking_health_summary
    final_summary["curve_semantic_window"] = curve_semantic_window
    final_summary["apollo_raw_steer_abs_p95"] = safe_float(curve_tracking_health_summary.get("apollo_raw_steer_abs_p95"))
    final_summary["apollo_raw_steer_abs_p99"] = safe_float(curve_tracking_health_summary.get("apollo_raw_steer_abs_p99"))
    final_summary["apollo_raw_steer_over95_ratio"] = safe_float(
        curve_tracking_health_summary.get("apollo_raw_steer_over95_ratio")
    )
    final_summary["apollo_simple_lat_lateral_error_abs_p95"] = safe_float(
        curve_tracking_health_summary.get("apollo_simple_lat_lateral_error_abs_p95")
    )
    final_summary["apollo_simple_lat_lateral_error_abs_p99"] = safe_float(
        curve_tracking_health_summary.get("apollo_simple_lat_lateral_error_abs_p99")
    )
    final_summary["apollo_simple_lat_heading_error_abs_p95"] = safe_float(
        curve_tracking_health_summary.get("apollo_simple_lat_heading_error_abs_p95")
    )
    final_summary["apollo_simple_lat_heading_error_abs_p99"] = safe_float(
        curve_tracking_health_summary.get("apollo_simple_lat_heading_error_abs_p99")
    )
    final_summary["apollo_simple_lat_target_point_kappa_abs_p95"] = safe_float(
        curve_tracking_health_summary.get("apollo_simple_lat_target_point_kappa_abs_p95")
    )
    final_summary["apollo_simple_lat_target_point_kappa_abs_p99"] = safe_float(
        curve_tracking_health_summary.get("apollo_simple_lat_target_point_kappa_abs_p99")
    )
    final_summary["mapped_carla_steer_cmd_abs_p95"] = safe_float(
        curve_tracking_health_summary.get("mapped_carla_steer_cmd_abs_p95")
    )
    final_summary["mapped_carla_steer_cmd_abs_p99"] = safe_float(
        curve_tracking_health_summary.get("mapped_carla_steer_cmd_abs_p99")
    )
    final_summary["target_front_wheel_angle_deg_abs_p95"] = safe_float(
        curve_tracking_health_summary.get("target_front_wheel_angle_deg_abs_p95")
    )
    final_summary["target_front_wheel_angle_deg_abs_p99"] = safe_float(
        curve_tracking_health_summary.get("target_front_wheel_angle_deg_abs_p99")
    )
    final_summary["first_high_steer_seq"] = safe_int(curve_tracking_health_summary.get("first_high_steer_seq"))
    final_summary["first_high_steer_at"] = safe_float(curve_tracking_health_summary.get("first_high_steer_at"))
    final_summary["first_matched_point_too_large_seq"] = safe_int(
        curve_tracking_health_summary.get("first_matched_point_too_large_seq")
    )
    final_summary["semantic_window_anchor_kind"] = curve_semantic_window.get("semantic_window_anchor_kind")
    final_summary["semantic_window_anchor_seq"] = safe_int(curve_semantic_window.get("semantic_window_anchor_seq"))
    final_summary["semantic_window_anchor_at"] = safe_float(curve_semantic_window.get("semantic_window_anchor_at"))
    final_summary["first_matched_point_too_large_at"] = safe_float(
        curve_semantic_window.get("first_matched_point_too_large_at")
    )
    final_summary["high_steer_before_first_matched_point_too_large"] = curve_tracking_health_summary.get(
        "high_steer_before_first_matched_point_too_large"
    )
    final_summary["simple_lat_lateral_error_abs_p95_before_failure"] = safe_float(
        curve_semantic_window.get("simple_lat_lateral_error_abs_p95_before_failure")
    )
    final_summary["simple_lat_heading_error_abs_p95_before_failure"] = safe_float(
        curve_semantic_window.get("simple_lat_heading_error_abs_p95_before_failure")
    )
    final_summary["target_point_kappa_abs_p95_before_failure"] = safe_float(
        curve_semantic_window.get("target_point_kappa_abs_p95_before_failure")
    )
    final_summary["mapped_carla_steer_cmd_abs_p95_before_failure"] = safe_float(
        curve_semantic_window.get("mapped_carla_steer_cmd_abs_p95_before_failure")
    )
    final_summary["simple_lat_lateral_error_abs_p95_before_anchor"] = safe_float(
        curve_semantic_window.get("simple_lat_lateral_error_abs_p95_before_anchor")
    )
    final_summary["simple_lat_heading_error_abs_p95_before_anchor"] = safe_float(
        curve_semantic_window.get("simple_lat_heading_error_abs_p95_before_anchor")
    )
    final_summary["target_point_kappa_abs_p95_before_anchor"] = safe_float(
        curve_semantic_window.get("target_point_kappa_abs_p95_before_anchor")
    )
    final_summary["mapped_carla_steer_cmd_abs_p95_before_anchor"] = safe_float(
        curve_semantic_window.get("mapped_carla_steer_cmd_abs_p95_before_anchor")
    )
    final_summary["control_handoff"] = control_handoff_summary
    final_summary["control_handoff_status"] = control_handoff_summary["control_handoff_status"]
    final_summary["planning_route_seen_at"] = control_handoff_summary["planning_route_seen_at"]
    final_summary["planning_first_nonempty_at"] = control_handoff_summary["planning_first_nonempty_at"]
    final_summary["control_first_consume_at"] = control_handoff_summary["control_first_consume_at"]
    final_summary["control_consume_row_count"] = control_handoff_summary["control_consume_row_count"]
    final_summary["control_handoff_latency_sec"] = control_handoff_summary["control_handoff_latency_sec"]
    final_summary["apollo_control_process_alive"] = control_handoff_summary["apollo_control_process_alive"]
    final_summary["apollo_control_log_nonempty"] = control_handoff_summary["apollo_control_log_nonempty"]
    final_summary["apollo_control_raw_row_count"] = control_handoff_summary["apollo_control_raw_row_count"]
    final_summary["control_zero_hold_only"] = control_handoff_summary["control_zero_hold_only"]
    final_summary["control_last_input_trajectory_seq"] = control_handoff_summary["control_last_input_trajectory_seq"]
    final_summary["control_last_input_trajectory_point_count"] = control_handoff_summary[
        "control_last_input_trajectory_point_count"
    ]
    final_summary["control_first_nonzero_planning_seen_by_control_at"] = control_handoff_summary[
        "control_first_nonzero_planning_seen_by_control_at"
    ]
    final_summary["control_last_output_ts"] = control_handoff_summary["control_last_output_ts"]
    final_summary["control_output_zero_hold_ratio"] = control_handoff_summary["control_output_zero_hold_ratio"]
    final_summary["control_final_process_alive"] = control_handoff_summary["control_final_process_alive"]
    final_summary["control_final_modules_status"] = control_handoff_summary["control_final_modules_status"]
    final_summary["control_started_pid_seen"] = control_handoff_summary["control_started_pid_seen"]
    final_summary["control_survived_5s"] = control_handoff_summary["control_survived_5s"]
    final_summary["control_survived_10s"] = control_handoff_summary["control_survived_10s"]
    final_summary["control_present_after_first_nonzero_planning"] = control_handoff_summary[
        "control_present_after_first_nonzero_planning"
    ]
    final_summary["control_deferred_survival"] = control_handoff_summary["control_deferred_survival"]
    final_summary["control_crash_detected"] = control_handoff_summary["control_crash_detected"]
    final_summary["control_crash_reason"] = control_handoff_summary["control_crash_reason"]
    final_summary["control_crash_log_tail"] = control_handoff_summary["control_crash_log_tail"]
    final_summary["control_deferred_log_path"] = control_handoff_summary["control_deferred_log_path"]
    final_summary["control_deferred_start_mode"] = control_handoff_summary["control_deferred_start_mode"]
    final_summary["control_output_materialized"] = control_handoff_summary["control_output_materialized"]
    final_summary["control_deferred_start"] = control_handoff_summary["control_deferred_start"]
    final_summary["apollo_control_info_path"] = control_handoff_summary["apollo_control_info_path"]
    final_summary["apollo_control_info_exists"] = control_handoff_summary["apollo_control_info_exists"]
    final_summary["apollo_control_chassis_not_ready_count"] = control_handoff_summary[
        "apollo_control_chassis_not_ready_count"
    ]
    final_summary["apollo_control_waiting_for_planning_count"] = control_handoff_summary[
        "apollo_control_waiting_for_planning_count"
    ]
    final_summary["apollo_control_info_tail"] = control_handoff_summary["apollo_control_info_tail"]
    final_summary["carla_bootstrap"] = carla_bootstrap_summary
    final_summary["carla_bootstrap_status"] = carla_bootstrap_summary["carla_bootstrap_status"]
    final_summary["carla_bootstrap_stability_sec"] = carla_bootstrap_summary["carla_bootstrap_stability_sec"]
    final_summary["carla_process_survived_stability_window"] = carla_bootstrap_summary[
        "carla_process_survived_stability_window"
    ]
    final_summary["carla_server_request_exit_seen"] = carla_bootstrap_summary["carla_server_request_exit_seen"]
    final_summary["carla_server_sig11_seen"] = carla_bootstrap_summary["carla_server_sig11_seen"]
    final_summary["carla_server_stream_eof_seen"] = carla_bootstrap_summary["carla_server_stream_eof_seen"]
    final_summary["memory_preflight_status"] = carla_bootstrap_summary["memory_preflight_status"]
    final_summary["host_swap_enabled"] = carla_bootstrap_summary["host_swap_enabled"]
    final_summary["available_memory_mb_before_start"] = carla_bootstrap_summary[
        "available_memory_mb_before_start"
    ]
    final_summary["carla_peak_rss_mb"] = carla_bootstrap_summary["carla_peak_rss_mb"]
    final_summary["oom_guard_triggered"] = carla_bootstrap_summary["oom_guard_triggered"]
    final_summary["invalid_goal_count"] = invalid_goal_count
    final_summary["long_phase_invalid_goal_skip"] = checks["long_phase_invalid_goal_skip"]["actual"]
    final_summary["unstable_reference_line_skip"] = checks["unstable_reference_line_skip"]["actual"]
    final_summary["control_no_trajectory_count"] = checks["control_no_trajectory_count"]["actual"]
    final_summary["route_distance_achieved_m"] = checks["route_distance_achieved_m"]["actual"]
    final_summary["route_completion_ratio"] = checks["route_completion_ratio"]["actual"]
    final_summary["final_goal_distance_m"] = safe_float(route_metrics.get("final_goal_distance_m"))
    final_summary["max_speed_mps"] = max_speed_mps
    final_summary["whether_vehicle_moved"] = whether_vehicle_moved
    final_summary["low_speed_creep_duration_sec"] = checks["low_speed_creep_duration_sec"]["actual"]
    final_summary["raw_steer_saturated_ratio"] = safe_float(lateral_metrics.get("raw_steer_saturated_ratio"))
    final_summary["longest_continuous_saturation_sec"] = safe_float(
        lateral_metrics.get("longest_continuous_saturation_sec")
    )
    final_summary["guard_trigger_ratio"] = safe_float(lateral_metrics.get("guard_trigger_ratio"))
    final_summary["path_fallback_count"] = safe_int(planning_trajectory_type_summary.get("path_fallback_count"))
    final_summary["first_path_fallback_seq"] = safe_int(planning_trajectory_type_summary.get("first_path_fallback_seq"))
    final_summary["first_path_fallback_lon_diff"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_lon_diff")
    )
    final_summary["first_path_fallback_trigger_seq"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_trigger_seq")
    )
    final_summary["first_path_fallback_trigger_seq_gap"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_trigger_seq_gap")
    )
    final_summary["first_path_fallback_trigger_reason_family"] = planning_trajectory_type_summary.get(
        "first_path_fallback_trigger_reason_family"
    )
    final_summary["first_path_fallback_trigger_reason_streak_length"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_trigger_reason_streak_length")
    )
    final_summary["first_path_fallback_trigger_lon_diff"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_trigger_lon_diff")
    )
    final_summary["first_path_fallback_trigger_relative_time_min_sec"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_trigger_relative_time_min_sec")
    )
    final_summary["first_path_fallback_trigger_first_point_v"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_trigger_first_point_v")
    )
    final_summary["first_path_fallback_trigger_total_path_length"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_trigger_total_path_length")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_normal_count"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_normal_count")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_normal_seq_start"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_start")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_normal_seq_end"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_normal_seq_end")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_rel_min_min"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_rel_min_min")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_rel_min_max"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_rel_min_max")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_first_point_v_min"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_first_point_v_min")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_first_point_v_max"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_first_point_v_max")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_point_count_min"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_point_count_min")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_point_count_max"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_point_count_max")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_path_length_min"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_path_length_min")
    )
    final_summary["first_path_fallback_precurrent_time_smaller_path_length_max"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_precurrent_time_smaller_path_length_max")
    )
    final_summary["first_path_fallback_prev_normal_relative_time_min_sec"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_prev_normal_relative_time_min_sec")
    )
    final_summary["first_path_fallback_prev_normal_first_point_v"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_prev_normal_first_point_v")
    )
    final_summary["first_path_fallback_prev_normal_total_path_length"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_prev_normal_total_path_length")
    )
    final_summary["first_path_fallback_first_point_v"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_first_point_v")
    )
    final_summary["first_path_fallback_total_path_length"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_total_path_length")
    )
    final_summary["first_path_fallback_cluster_length"] = safe_int(
        planning_trajectory_type_summary.get("first_path_fallback_cluster_length")
    )
    final_summary["first_path_fallback_path_length_collapse_ratio"] = safe_float(
        planning_trajectory_type_summary.get("first_path_fallback_path_length_collapse_ratio")
    )
    final_summary["first_recovery_after_path_fallback_seq"] = safe_int(
        planning_trajectory_type_summary.get("first_recovery_after_path_fallback_seq")
    )
    final_summary["first_relapse_after_recovery_seq"] = safe_int(
        planning_trajectory_type_summary.get("first_relapse_after_recovery_seq")
    )
    final_summary["first_relapse_after_recovery_reason_family"] = planning_trajectory_type_summary.get(
        "first_relapse_after_recovery_reason_family"
    )
    final_summary["first_relapse_after_recovery_lon_diff"] = safe_float(
        planning_trajectory_type_summary.get("first_relapse_after_recovery_lon_diff")
    )
    final_summary["first_relapse_prev_normal_first_point_v"] = safe_float(
        planning_trajectory_type_summary.get("first_relapse_prev_normal_first_point_v")
    )
    final_summary["first_relapse_prev_normal_total_path_length"] = safe_float(
        planning_trajectory_type_summary.get("first_relapse_prev_normal_total_path_length")
    )
    final_summary["first_relapse_after_recovery_first_point_v"] = safe_float(
        planning_trajectory_type_summary.get("first_relapse_after_recovery_first_point_v")
    )
    final_summary["first_relapse_after_recovery_total_path_length"] = safe_float(
        planning_trajectory_type_summary.get("first_relapse_after_recovery_total_path_length")
    )
    final_summary["first_relapse_cluster_length"] = safe_int(
        planning_trajectory_type_summary.get("first_relapse_cluster_length")
    )
    final_summary["first_relapse_path_length_collapse_ratio"] = safe_float(
        planning_trajectory_type_summary.get("first_relapse_path_length_collapse_ratio")
    )
    final_summary["path_fallback_count_after_first_recovery"] = safe_int(
        planning_trajectory_type_summary.get("path_fallback_count_after_first_recovery")
    )
    final_summary["max_consecutive_path_fallback_after_first_recovery"] = safe_int(
        planning_trajectory_type_summary.get("max_consecutive_path_fallback_after_first_recovery")
    )
    final_summary["normal_count_after_first_recovery"] = safe_int(
        planning_trajectory_type_summary.get("normal_count_after_first_recovery")
    )
    final_summary["reentered_path_fallback_after_recovery"] = bool(
        planning_trajectory_type_summary.get("reentered_path_fallback_after_recovery")
    )
    final_summary["max_consecutive_path_fallback_length"] = safe_int(
        planning_trajectory_type_summary.get("max_consecutive_path_fallback_length")
    )
    final_summary["final_path_fallback_suffix_start_seq"] = safe_int(
        planning_trajectory_type_summary.get("final_path_fallback_suffix_start_seq")
    )
    final_summary["final_path_fallback_suffix_length"] = safe_int(
        planning_trajectory_type_summary.get("final_path_fallback_suffix_length")
    )
    final_summary["final_normal_tail_length"] = safe_int(
        planning_trajectory_type_summary.get("final_normal_tail_length")
    )
    final_summary["recovered_after_path_fallback"] = bool(
        planning_trajectory_type_summary.get("recovered_after_path_fallback")
    )
    final_summary["persistent_path_fallback_at_end"] = bool(
        planning_trajectory_type_summary.get("persistent_path_fallback_at_end")
    )
    final_summary["last_trajectory_type"] = planning_trajectory_type_summary.get("last_trajectory_type")
    final_summary.update(planning_bridge_context)
    final_summary["planning_trajectory_type_summary"] = planning_trajectory_type_summary
    final_summary["planning_control_alignment"] = planning_control_alignment
    final_summary["state_machine"] = {
        "failure_stage": failure_stage,
        "state_timeline_path": str(state_timeline_path),
        "states_reached": [item["state"] for item in state_timeline],
    }
    final_summary["state_timeline_path"] = str(state_timeline_path)
    final_summary["latest_state"] = state_timeline[-1]["state"] if state_timeline else None
    final_summary["latest_failure_edge"] = failure_stage
    final_summary["manifest_completeness"] = manifest_completeness_before
    final_summary["manifest_missing_files"] = missing_before
    if recording_status:
        final_summary["dreamview_recording"] = recording_manifest or final_summary.get("dreamview_recording") or {}
        final_summary["dreamview_recording_status_path"] = str(artifacts_dir / "dreamview_recording_status.json")
        final_summary["dreamview_capture_manifest_path"] = str(artifacts_dir / "dreamview_capture_manifest.json")
        final_summary["dreamview_runtime_config_snapshot_path"] = str(artifacts_dir / "dreamview_runtime_config_snapshot.json")
    write_json(summary_path, final_summary)

    manifest_completeness_after, missing_after = _compute_manifest_completeness(
        run_dir,
        enable_recording=bool(flags.get("enable_recording")),
    )
    final_summary["manifest_completeness"] = manifest_completeness_after
    final_summary["manifest_missing_files"] = missing_after
    final_summary["acceptance"]["checks"]["manifest_completeness"]["actual"] = manifest_completeness_after
    final_summary["acceptance"]["checks"]["manifest_completeness"]["ok"] = manifest_completeness_after >= 1.0
    if manifest_completeness_after >= 1.0 and "MANIFEST_INCOMPLETE" in final_summary["acceptance"]["failure_codes"]:
        final_summary["acceptance"]["failure_codes"] = [
            item for item in final_summary["acceptance"]["failure_codes"] if item != "MANIFEST_INCOMPLETE"
        ]
        final_summary["acceptance"]["fail_reason"] = (
            final_summary["acceptance"]["failure_codes"][0]
            if final_summary["acceptance"]["failure_codes"]
            else None
        )
    final_summary["acceptance"]["success"] = bool(
        final_summary["acceptance"]["fail_reason"] is None
        and final_summary["acceptance"]["checks"]["manifest_completeness"]["ok"]
    )
    final_summary["success"] = bool(final_summary["acceptance"]["success"])
    final_summary["fail_reason"] = final_summary["acceptance"]["fail_reason"]
    write_json(summary_path, final_summary)
    return final_summary


def collect_run_row(run_dir: Path) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    acceptance = summary.get("acceptance") or {}
    checks = acceptance.get("checks") or {}
    route_health = summary.get("route_health") or {}
    planning_bridge_context = _planning_bridge_context(checks.get("planning_trajectory_type_summary") or {})
    scenario_meta = load_json(run_dir / "artifacts" / "scenario_metadata.json")
    route_id = str(summary.get("route_id") or route_id_from_metadata(scenario_meta) or "")
    return {
        "run_dir": str(run_dir),
        "profile_name": summary.get("profile_name") or "",
        "route_id": route_id,
        "comparison_label": summary.get("comparison_label") or summary.get("profile_name") or "",
        "success": bool(acceptance.get("success")),
        "fail_reason": acceptance.get("fail_reason"),
        "failure_stage": acceptance.get("failure_stage"),
        "route_health_label": summary.get("route_health_label") or acceptance.get("route_health_label"),
        "routing_request_count": (checks.get("routing_request_count") or {}).get("actual"),
        "route_establishment_latency_sec": (checks.get("route_establishment_latency_sec") or {}).get("actual"),
        "planning_nonzero_ratio": (checks.get("planning_nonzero_ratio") or {}).get("actual"),
        "control_used_planning_ratio": (checks.get("control_used_planning_ratio") or {}).get("actual"),
        "invalid_goal_count": (checks.get("invalid_goal_count") or {}).get("actual"),
        "long_phase_invalid_goal_skip": (checks.get("long_phase_invalid_goal_skip") or {}).get("actual"),
        "unstable_reference_line_skip": (checks.get("unstable_reference_line_skip") or {}).get("actual"),
        "control_no_trajectory_count": (checks.get("control_no_trajectory_count") or {}).get("actual"),
        "route_distance_achieved_m": (checks.get("route_distance_achieved_m") or {}).get("actual"),
        "route_completion_ratio": (checks.get("route_completion_ratio") or {}).get("actual"),
        "max_speed_mps": (checks.get("max_speed_mps") or {}).get("actual"),
        "whether_vehicle_moved": (checks.get("whether_vehicle_moved") or {}).get("actual"),
        "low_speed_creep_duration_sec": (checks.get("low_speed_creep_duration_sec") or {}).get("actual"),
        "summary_status": summary.get("summary_status"),
        "finalized_from_event_stream": summary.get("finalized_from_event_stream"),
        "summary_written_successfully": summary.get("summary_written_successfully"),
        "manifest_completeness": summary.get("manifest_completeness"),
        "enable_lateral": summary.get("runtime_mode", {}).get("enable_lateral", scenario_meta.get("enable_lateral")),
        "enable_guard": summary.get("runtime_mode", {}).get("enable_guard", scenario_meta.get("enable_guard")),
        "enable_lateral_debug": summary.get("runtime_mode", {}).get("enable_lateral_debug", scenario_meta.get("enable_lateral_debug")),
        "enable_stage6_reference_line": summary.get("runtime_mode", {}).get(
            "enable_stage6_reference_line", scenario_meta.get("enable_stage6_reference_line")
        ),
        "stage6_clear_lane_follow_cache_on_new_command": summary.get("runtime_mode", {}).get(
            "stage6_clear_lane_follow_cache_on_new_command",
            scenario_meta.get("stage6_clear_lane_follow_cache_on_new_command"),
        ),
        "stage6_reference_line_generation_guard": summary.get("runtime_mode", {}).get(
            "stage6_reference_line_generation_guard",
            scenario_meta.get("stage6_reference_line_generation_guard"),
        ),
        "effective_stage6_reference_line_enabled": summary.get("runtime_mode", {}).get(
            "effective_stage6_reference_line_enabled"
        ),
        "effective_stage6_clear_lane_follow_cache_on_new_command": summary.get("runtime_mode", {}).get(
            "effective_stage6_clear_lane_follow_cache_on_new_command"
        ),
        "effective_stage6_reference_line_generation_guard": summary.get("runtime_mode", {}).get(
            "effective_stage6_reference_line_generation_guard"
        ),
        "capability_profile": summary.get("runtime_mode", {}).get(
            "capability_profile", scenario_meta.get("capability_profile")
        ),
        "effective_acc_only_mode": summary.get("runtime_mode", {}).get(
            "effective_acc_only_mode", scenario_meta.get("effective_acc_only_mode")
        ),
        "effective_longitudinal_only_pipeline": summary.get("runtime_mode", {}).get(
            "effective_longitudinal_only_pipeline", scenario_meta.get("effective_longitudinal_only_pipeline")
        ),
        "effective_longitudinal_only_keep_lane_follow_path": summary.get("runtime_mode", {}).get(
            "effective_longitudinal_only_keep_lane_follow_path",
            scenario_meta.get("effective_longitudinal_only_keep_lane_follow_path"),
        ),
        "runtime_contract_status": (summary.get("runtime_contract") or {}).get(
            "status", (scenario_meta.get("runtime_contract") or {}).get("status")
        ),
        "runtime_contract_requires_true_lateral": (summary.get("runtime_contract") or {}).get(
            "requires_true_lateral",
            (scenario_meta.get("runtime_contract") or {}).get("requires_true_lateral"),
        ),
        "runtime_contract_requested_true_lateral": (summary.get("runtime_contract") or {}).get(
            "requested_true_lateral",
            (scenario_meta.get("runtime_contract") or {}).get("requested_true_lateral"),
        ),
        "runtime_contract_blockers": "|".join(
            str(item)
            for item in ((summary.get("runtime_contract") or {}).get("blockers") or [])
        )
        or "|".join(str(item) for item in ((scenario_meta.get("runtime_contract") or {}).get("blockers") or [])),
        "force_zero_steer_output": summary.get("runtime_mode", {}).get("force_zero_steer_output", scenario_meta.get("force_zero_steer_output")),
        "straight_lane_zero_steer_enabled": summary.get("runtime_mode", {}).get(
            "straight_lane_zero_steer_enabled", scenario_meta.get("straight_lane_zero_steer_enabled")
        ),
        "effective_localization_back_offset_m": summary.get("runtime_mode", {}).get(
            "effective_localization_back_offset_m",
            scenario_meta.get("effective_localization_back_offset_m"),
        ),
        "localization_reference_mode": summary.get("runtime_mode", {}).get(
            "localization_reference_mode",
            scenario_meta.get("localization_reference_mode"),
        ),
        "localization_reference_expected_back_offset_m": summary.get("runtime_mode", {}).get(
            "localization_reference_expected_back_offset_m",
            scenario_meta.get("localization_reference_expected_back_offset_m"),
        ),
        "localization_reference_offset_error_m": summary.get("runtime_mode", {}).get(
            "localization_reference_offset_error_m",
            scenario_meta.get("localization_reference_offset_error_m"),
        ),
        "apollo_control_state_reference": summary.get("runtime_mode", {}).get(
            "apollo_control_state_reference",
            scenario_meta.get("apollo_control_state_reference"),
        ),
        "run_ticks": summary.get("runtime_mode", {}).get("run_ticks", scenario_meta.get("run_ticks")),
        "raw_steer_saturated_ratio": ((checks.get("lateral_metrics") or {}).get("raw_steer_saturated_ratio")),
        "longest_continuous_saturation_sec": ((checks.get("lateral_metrics") or {}).get("longest_continuous_saturation_sec")),
        "guard_trigger_ratio": ((checks.get("lateral_metrics") or {}).get("guard_trigger_ratio")),
        "path_fallback_count": ((checks.get("planning_trajectory_type_summary") or {}).get("path_fallback_count")),
        "first_path_fallback_seq": ((checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_seq")),
        "first_path_fallback_lon_diff": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_lon_diff")
        ),
        "first_path_fallback_trigger_seq": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_seq")
        ),
        "first_path_fallback_trigger_seq_gap": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_seq_gap")
        ),
        "first_path_fallback_trigger_reason_family": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_reason_family")
        ),
        "first_path_fallback_trigger_reason_streak_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_reason_streak_length")
        ),
        "first_path_fallback_trigger_lon_diff": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_lon_diff")
        ),
        "first_path_fallback_trigger_relative_time_min_sec": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_relative_time_min_sec")
        ),
        "first_path_fallback_trigger_first_point_v": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_first_point_v")
        ),
        "first_path_fallback_trigger_total_path_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_trigger_total_path_length")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_count": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_normal_count")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_start": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_normal_seq_start")
        ),
        "first_path_fallback_precurrent_time_smaller_normal_seq_end": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_normal_seq_end")
        ),
        "first_path_fallback_precurrent_time_smaller_rel_min_min": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_rel_min_min")
        ),
        "first_path_fallback_precurrent_time_smaller_rel_min_max": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_rel_min_max")
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_min": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_first_point_v_min")
        ),
        "first_path_fallback_precurrent_time_smaller_first_point_v_max": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_first_point_v_max")
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_min": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_point_count_min")
        ),
        "first_path_fallback_precurrent_time_smaller_point_count_max": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_point_count_max")
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_min": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_path_length_min")
        ),
        "first_path_fallback_precurrent_time_smaller_path_length_max": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_precurrent_time_smaller_path_length_max")
        ),
        "first_path_fallback_prev_normal_relative_time_min_sec": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_prev_normal_relative_time_min_sec")
        ),
        "first_path_fallback_prev_normal_first_point_v": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_prev_normal_first_point_v")
        ),
        "first_path_fallback_prev_normal_total_path_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_prev_normal_total_path_length")
        ),
        "first_path_fallback_first_point_v": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_first_point_v")
        ),
        "first_path_fallback_total_path_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_total_path_length")
        ),
        "first_path_fallback_cluster_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_cluster_length")
        ),
        "first_path_fallback_path_length_collapse_ratio": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_path_fallback_path_length_collapse_ratio")
        ),
        "first_recovery_after_path_fallback_seq": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_recovery_after_path_fallback_seq")
        ),
        "first_relapse_after_recovery_seq": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_after_recovery_seq")
        ),
        "first_relapse_after_recovery_reason_family": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_after_recovery_reason_family")
        ),
        "first_relapse_after_recovery_lon_diff": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_after_recovery_lon_diff")
        ),
        "first_relapse_prev_normal_first_point_v": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_prev_normal_first_point_v")
        ),
        "first_relapse_prev_normal_total_path_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_prev_normal_total_path_length")
        ),
        "first_relapse_after_recovery_first_point_v": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_after_recovery_first_point_v")
        ),
        "first_relapse_after_recovery_total_path_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_after_recovery_total_path_length")
        ),
        "first_relapse_cluster_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_cluster_length")
        ),
        "first_relapse_path_length_collapse_ratio": (
            (checks.get("planning_trajectory_type_summary") or {}).get("first_relapse_path_length_collapse_ratio")
        ),
        "path_fallback_count_after_first_recovery": (
            (checks.get("planning_trajectory_type_summary") or {}).get("path_fallback_count_after_first_recovery")
        ),
        "max_consecutive_path_fallback_after_first_recovery": (
            (checks.get("planning_trajectory_type_summary") or {}).get("max_consecutive_path_fallback_after_first_recovery")
        ),
        "normal_count_after_first_recovery": (
            (checks.get("planning_trajectory_type_summary") or {}).get("normal_count_after_first_recovery")
        ),
        "reentered_path_fallback_after_recovery": (
            (checks.get("planning_trajectory_type_summary") or {}).get("reentered_path_fallback_after_recovery")
        ),
        "max_consecutive_path_fallback_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("max_consecutive_path_fallback_length")
        ),
        "final_path_fallback_suffix_start_seq": (
            (checks.get("planning_trajectory_type_summary") or {}).get("final_path_fallback_suffix_start_seq")
        ),
        "final_path_fallback_suffix_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("final_path_fallback_suffix_length")
        ),
        "final_normal_tail_length": (
            (checks.get("planning_trajectory_type_summary") or {}).get("final_normal_tail_length")
        ),
        "recovered_after_path_fallback": ((checks.get("planning_trajectory_type_summary") or {}).get("recovered_after_path_fallback")),
        "persistent_path_fallback_at_end": ((checks.get("planning_trajectory_type_summary") or {}).get("persistent_path_fallback_at_end")),
        "last_trajectory_type": ((checks.get("planning_trajectory_type_summary") or {}).get("last_trajectory_type")),
        "apollo_raw_steer_abs_p95": ((checks.get("curve_tracking_health_summary") or {}).get("apollo_raw_steer_abs_p95")),
        "apollo_raw_steer_abs_p99": ((checks.get("curve_tracking_health_summary") or {}).get("apollo_raw_steer_abs_p99")),
        "apollo_raw_steer_over95_ratio": (
            (checks.get("curve_tracking_health_summary") or {}).get("apollo_raw_steer_over95_ratio")
        ),
        "first_high_steer_seq": ((checks.get("curve_tracking_health_summary") or {}).get("first_high_steer_seq")),
        "first_matched_point_too_large_seq": (
            (checks.get("curve_tracking_health_summary") or {}).get("first_matched_point_too_large_seq")
        ),
        "high_steer_before_first_matched_point_too_large": (
            (checks.get("curve_tracking_health_summary") or {}).get("high_steer_before_first_matched_point_too_large")
        ),
        **planning_bridge_context,
        "first_nonzero_planning_seq": ((checks.get("planning_control_alignment") or {}).get("first_nonzero_planning_seq")),
        "last_live_control_planning_seq": ((checks.get("planning_control_alignment") or {}).get("last_live_control_planning_seq")),
        "live_control_to_first_nonzero_gap_sec": ((checks.get("planning_control_alignment") or {}).get("live_control_to_first_nonzero_gap_sec")),
        "live_control_stops_before_first_nonzero_planning": ((checks.get("planning_control_alignment") or {}).get("live_control_stops_before_first_nonzero_planning")),
        "control_no_trajectory_after_first_nonzero": ((checks.get("planning_control_alignment") or {}).get("control_no_trajectory_after_first_nonzero")),
    }


def write_platform_comparison(rows: List[Dict[str, Any]], path: Path) -> None:
    fieldnames = [
        "comparison_label",
        "route_id",
        "run_dir",
        "success",
        "fail_reason",
        "failure_stage",
        "route_health_label",
        "routing_request_count",
        "route_establishment_latency_sec",
        "planning_nonzero_ratio",
        "control_used_planning_ratio",
        "invalid_goal_count",
        "long_phase_invalid_goal_skip",
        "unstable_reference_line_skip",
        "control_no_trajectory_count",
        "route_distance_achieved_m",
        "route_completion_ratio",
        "max_speed_mps",
        "whether_vehicle_moved",
        "low_speed_creep_duration_sec",
        "raw_steer_saturated_ratio",
        "longest_continuous_saturation_sec",
        "guard_trigger_ratio",
        "path_fallback_count",
        "first_path_fallback_seq",
        "first_path_fallback_lon_diff",
        "first_path_fallback_trigger_seq",
        "first_path_fallback_trigger_seq_gap",
        "first_path_fallback_trigger_reason_family",
        "first_path_fallback_trigger_reason_streak_length",
        "first_path_fallback_trigger_lon_diff",
        "first_path_fallback_trigger_relative_time_min_sec",
        "first_path_fallback_trigger_first_point_v",
        "first_path_fallback_trigger_total_path_length",
        "first_path_fallback_precurrent_time_smaller_normal_count",
        "first_path_fallback_precurrent_time_smaller_normal_seq_start",
        "first_path_fallback_precurrent_time_smaller_normal_seq_end",
        "first_path_fallback_precurrent_time_smaller_rel_min_min",
        "first_path_fallback_precurrent_time_smaller_rel_min_max",
        "first_path_fallback_precurrent_time_smaller_first_point_v_min",
        "first_path_fallback_precurrent_time_smaller_first_point_v_max",
        "first_path_fallback_precurrent_time_smaller_point_count_min",
        "first_path_fallback_precurrent_time_smaller_point_count_max",
        "first_path_fallback_precurrent_time_smaller_path_length_min",
        "first_path_fallback_precurrent_time_smaller_path_length_max",
        "first_path_fallback_prev_normal_relative_time_min_sec",
        "first_path_fallback_prev_normal_first_point_v",
        "first_path_fallback_prev_normal_total_path_length",
        "first_path_fallback_first_point_v",
        "first_path_fallback_total_path_length",
        "first_path_fallback_cluster_length",
        "first_path_fallback_path_length_collapse_ratio",
        "first_recovery_after_path_fallback_seq",
        "first_relapse_after_recovery_seq",
        "first_relapse_after_recovery_reason_family",
        "first_relapse_after_recovery_lon_diff",
        "first_relapse_prev_normal_first_point_v",
        "first_relapse_prev_normal_total_path_length",
        "first_relapse_after_recovery_first_point_v",
        "first_relapse_after_recovery_total_path_length",
        "first_relapse_cluster_length",
        "first_relapse_path_length_collapse_ratio",
        "path_fallback_count_after_first_recovery",
        "max_consecutive_path_fallback_after_first_recovery",
        "normal_count_after_first_recovery",
        "reentered_path_fallback_after_recovery",
        "max_consecutive_path_fallback_length",
        "final_path_fallback_suffix_start_seq",
        "final_path_fallback_suffix_length",
        "final_normal_tail_length",
        "recovered_after_path_fallback",
        "persistent_path_fallback_at_end",
        "last_trajectory_type",
        "apollo_raw_steer_abs_p95",
        "apollo_raw_steer_abs_p99",
        "apollo_raw_steer_over95_ratio",
        "first_high_steer_seq",
        "first_matched_point_too_large_seq",
        "high_steer_before_first_matched_point_too_large",
        *PLANNING_BRIDGE_CONTEXT_FIELDS,
        "first_nonzero_planning_seq",
        "last_live_control_planning_seq",
        "live_control_to_first_nonzero_gap_sec",
        "live_control_stops_before_first_nonzero_planning",
        "control_no_trajectory_after_first_nonzero",
        "summary_status",
        "finalized_from_event_stream",
        "summary_written_successfully",
        "manifest_completeness",
        "enable_lateral",
        "enable_guard",
        "enable_lateral_debug",
        "enable_stage6_reference_line",
        "stage6_clear_lane_follow_cache_on_new_command",
        "stage6_reference_line_generation_guard",
        "effective_stage6_reference_line_enabled",
        "effective_stage6_clear_lane_follow_cache_on_new_command",
        "effective_stage6_reference_line_generation_guard",
        "force_zero_steer_output",
        "straight_lane_zero_steer_enabled",
        "run_ticks",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def write_platform_report(rows: List[Dict[str, Any]], path: Path) -> None:
    report_rows = [row for row in rows if not _is_stage6_experimental_mode_row(row)]
    selected_rows = report_rows if report_rows else rows
    excluded_experimental_rows = len(rows) - len(selected_rows)
    lines = [
        "# Town01 Route Health Platform Mainline Report",
        "",
        f"- run_count: `{len(selected_rows)}`",
        "",
        "## Answers",
        "",
    ]
    if excluded_experimental_rows > 0 and report_rows:
        lines.insert(3, f"- experimental_probe_rows_excluded_from_answers: `{excluded_experimental_rows}`")
        lines.insert(4, "")
    skeleton_built = bool(selected_rows)
    lines.append(f"1. Town01 健康主线骨架是否已经建立: `{skeleton_built}`")
    lines.append("2. route corpus 是否已经形成: `see carla_testbed/assets/routes/town01/town01_route_corpus.json`")
    if selected_rows:
        recommended = max(selected_rows, key=lambda item: safe_float(item.get("route_completion_ratio")) or 0.0)
        failure_code_counts: Dict[str, int] = {}
        failure_examples: Dict[str, str] = {}
        live_stop_before_nonzero_count = 0
        any_control_closure = False
        any_lateral_runtime = False
        best_lateral_completion: Optional[float] = None
        best_lateral_has_saturation_issue = False
        persistent_path_fallback_routes: List[str] = []
        recovered_path_fallback_routes: List[str] = []
        stable_recovery_routes: List[str] = []
        relapse_heavy_routes: List[str] = []
        guarded_runtime_ok_labels_by_route: Dict[str, set[str]] = {}
        guarded_repeat_ok_labels_by_route: Dict[str, set[str]] = {}
        guarded_unstable_labels_by_route: Dict[str, set[str]] = {}
        persistent_path_fallback_rows: List[Dict[str, Any]] = []
        stable_recovery_prev_v_by_route: Dict[str, List[float]] = {}
        stable_recovery_relapse_path_by_route: Dict[str, List[float]] = {}
        relapse_heavy_prev_v_by_route: Dict[str, List[float]] = {}
        relapse_heavy_relapse_path_by_route: Dict[str, List[float]] = {}
        for row in rows:
            summary = load_json(Path(str(row["run_dir"])) / "summary.json")
            acceptance = summary.get("acceptance") or {}
            alignment = summary.get("planning_control_alignment") or {}
            planning_type_summary = summary.get("planning_trajectory_type_summary") or {}
            states_reached = set((((summary.get("state_machine") or {}).get("states_reached")) or []))
            lateral_metrics = (((acceptance.get("checks") or {}).get("lateral_metrics")) or {})
            for code in list(acceptance.get("failure_codes") or []):
                key = str(code or "").strip()
                if not key:
                    continue
                failure_code_counts[key] = int(failure_code_counts.get(key, 0)) + 1
                failure_examples.setdefault(key, str(row.get("route_id") or ""))
            if bool(alignment.get("live_control_stops_before_first_nonzero_planning")):
                live_stop_before_nonzero_count += 1
            route_id = str(row.get("route_id") or "")
            comparison_label = str(row.get("comparison_label") or "")
            row_completion = safe_float(row.get("route_completion_ratio")) or 0.0
            row_distance = safe_float(row.get("route_distance_achieved_m")) or 0.0
            row_control_used_planning_ratio = safe_float(row.get("control_used_planning_ratio")) or 0.0
            row_raw_steer_saturated_ratio = safe_float(row.get("raw_steer_saturated_ratio")) or 0.0
            row_guard_trigger_ratio = safe_float(row.get("guard_trigger_ratio")) or 0.0
            is_guarded_lateral = safe_bool(row.get("enable_lateral")) is True and safe_bool(row.get("enable_guard")) is True
            is_persistent_at_end = bool(planning_type_summary.get("persistent_path_fallback_at_end"))
            path_fallback_count_after_first_recovery = (
                safe_int(planning_type_summary.get("path_fallback_count_after_first_recovery")) or 0
            )
            final_normal_tail_length = safe_int(planning_type_summary.get("final_normal_tail_length")) or 0
            if bool(planning_type_summary.get("persistent_path_fallback_at_end")) and route_id:
                persistent_path_fallback_routes.append(route_id)
                persistent_path_fallback_rows.append(row)
            elif bool(planning_type_summary.get("recovered_after_path_fallback")) and route_id:
                recovered_path_fallback_routes.append(route_id)
                if (
                    (safe_int(planning_type_summary.get("path_fallback_count_after_first_recovery")) or 0) <= 10
                    and (safe_int(planning_type_summary.get("final_normal_tail_length")) or 0) >= 500
                ):
                    stable_recovery_routes.append(route_id)
                    stable_prev_v = safe_float(
                        row.get("first_relapse_prev_normal_first_point_v")
                        if row.get("first_relapse_prev_normal_first_point_v") not in (None, "")
                        else planning_type_summary.get("first_relapse_prev_normal_first_point_v")
                    )
                    stable_relapse_path = safe_float(
                        row.get("first_relapse_after_recovery_total_path_length")
                        if row.get("first_relapse_after_recovery_total_path_length") not in (None, "")
                        else planning_type_summary.get("first_relapse_after_recovery_total_path_length")
                    )
                    if stable_prev_v is not None:
                        stable_recovery_prev_v_by_route.setdefault(route_id, []).append(float(stable_prev_v))
                    if stable_relapse_path is not None:
                        stable_recovery_relapse_path_by_route.setdefault(route_id, []).append(float(stable_relapse_path))
                if (
                    (safe_int(planning_type_summary.get("path_fallback_count_after_first_recovery")) or 0) >= 100
                    and (safe_int(planning_type_summary.get("first_relapse_cluster_length")) or 0) >= 12
                    and (safe_int(planning_type_summary.get("final_normal_tail_length")) or 0) <= 10
                ):
                    relapse_heavy_routes.append(route_id)
                    relapse_prev_v = safe_float(
                        row.get("first_relapse_prev_normal_first_point_v")
                        if row.get("first_relapse_prev_normal_first_point_v") not in (None, "")
                        else planning_type_summary.get("first_relapse_prev_normal_first_point_v")
                    )
                    relapse_path = safe_float(
                        row.get("first_relapse_after_recovery_total_path_length")
                        if row.get("first_relapse_after_recovery_total_path_length") not in (None, "")
                        else planning_type_summary.get("first_relapse_after_recovery_total_path_length")
                    )
                    if relapse_prev_v is not None:
                        relapse_heavy_prev_v_by_route.setdefault(route_id, []).append(float(relapse_prev_v))
                    if relapse_path is not None:
                        relapse_heavy_relapse_path_by_route.setdefault(route_id, []).append(float(relapse_path))
            if is_guarded_lateral and route_id:
                if (
                    not is_persistent_at_end
                    and row_control_used_planning_ratio >= 0.80
                    and row_completion >= 0.50
                    and row_distance >= 80.0
                    and row_raw_steer_saturated_ratio <= 0.15
                    and row_guard_trigger_ratio <= 0.05
                ):
                    guarded_runtime_ok_labels_by_route.setdefault(route_id, set()).add(comparison_label)
                if (
                    not is_persistent_at_end
                    and row_control_used_planning_ratio >= 0.80
                    and row_completion >= 0.75
                    and row_distance >= 140.0
                    and row_raw_steer_saturated_ratio <= 0.15
                    and row_guard_trigger_ratio <= 0.05
                    and path_fallback_count_after_first_recovery <= 10
                    and final_normal_tail_length >= 1000
                ):
                    guarded_repeat_ok_labels_by_route.setdefault(route_id, set()).add(comparison_label)
                if is_persistent_at_end or (
                    path_fallback_count_after_first_recovery >= 100 and final_normal_tail_length <= 10
                ):
                    guarded_unstable_labels_by_route.setdefault(route_id, set()).add(comparison_label)
            if "CONTROL_READY" in states_reached or "CRUISE_ACTIVE" in states_reached:
                any_control_closure = True
            runtime_mode = summary.get("runtime_mode") or {}
            lateral_sample_count = safe_int(lateral_metrics.get("sample_count")) or 0
            is_lateral_runtime = bool(
                (
                    safe_bool(runtime_mode.get("enable_lateral"))
                    or lateral_sample_count > 0
                )
                and "CRUISE_ACTIVE" in states_reached
            )
            if is_lateral_runtime:
                any_lateral_runtime = True
                lateral_completion = safe_float(summary.get("route_completion_ratio"))
                if lateral_completion is None:
                    lateral_completion = safe_float(row.get("route_completion_ratio"))
                if best_lateral_completion is None or (
                    lateral_completion is not None and float(lateral_completion) > float(best_lateral_completion)
                ):
                    best_lateral_completion = float(lateral_completion) if lateral_completion is not None else None
                    best_lateral_has_saturation_issue = bool(
                        (safe_float(lateral_metrics.get("raw_steer_saturated_ratio")) or 0.0) > 0.05
                        or (safe_float(lateral_metrics.get("guard_trigger_ratio")) or 0.0) > 0.02
                    )
        top_failures = sorted(failure_code_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
        top_failure_text = (
            ", ".join(f"`{code}` x`{count}` route=`{failure_examples.get(code) or 'unknown'}`" for code, count in top_failures)
            if top_failures
            else "`no_failure_codes_detected`"
        )
        any_success = any(bool(row.get("success")) for row in selected_rows)
        trusted_summary = all(bool(row.get("finalized_from_event_stream")) for row in selected_rows)
        completion_values = [safe_float(row.get("route_completion_ratio")) for row in selected_rows]
        completion_values = [float(value) for value in completion_values if value is not None]
        mean_completion = (sum(completion_values) / float(len(completion_values))) if completion_values else None
        lines.append(
            f"3. 当前 Town01 主线最推荐怎么跑: `tools/run_town01_route_health.py run --sample-size 12`，当前最佳样本 route=`{recommended.get('route_id')}`"
        )
        lines.append(f"4. 当前 top3 健康问题是什么: {top_failure_text}")
        if any_success:
            lines.append("5. 当前 route completion 和 behavior health 是否比之前更好: `see comparison_label deltas in platform comparison csv`")
        elif any_control_closure:
            lines.append(
                "5. 当前 route completion 和 behavior health 是否比之前更好: "
                f"`是；控制闭环已建立，样本已能进入 CRUISE_ACTIVE，但 completion 仍偏低，mean_completion={mean_completion if mean_completion is not None else 'n/a'}`"
            )
        else:
            lines.append(
                "5. 当前 route completion 和 behavior health 是否比之前更好: "
                f"`当前离线统一重算主要提升了 attribution/finalization；运行时行为未见改善，mean_completion={mean_completion if mean_completion is not None else 'n/a'}`"
            )
        lines.append(f"6. summary/finalization 是否更可信: `{trusted_summary}`")
        extension_ready = any(bool(row.get("summary_written_successfully")) for row in selected_rows)
        lines.append(f"7. 单一主线是否已能承接后续横向验证和 Demo: `{extension_ready}`")
        lines.append(
            f"- planning/control alignment signal: `live_control_stops_before_first_nonzero_planning={live_stop_before_nonzero_count}/{len(selected_rows)}`"
        )
        if persistent_path_fallback_routes:
            persistent_text = ", ".join(f"`{route}`" for route in sorted(set(persistent_path_fallback_routes))[:3])
            recovered_only_routes = sorted(set(recovered_path_fallback_routes) - set(persistent_path_fallback_routes))
            recovered_text = ", ".join(f"`{route}`" for route in recovered_only_routes[:3]) or "`none`"
            lines.append(
                "- planning fallback note: "
                f"`存在 route-specific 的 persistent PATH_FALLBACK；当前已确认 ending fallback route={persistent_text}，"
                f"而可恢复的 route 例如 {recovered_text}`"
            )
            if stable_recovery_routes or relapse_heavy_routes:
                stable_recovery_unique = sorted(
                    set(stable_recovery_routes)
                    - set(persistent_path_fallback_routes)
                    - set(relapse_heavy_routes)
                )
                relapse_heavy_unique = sorted(set(relapse_heavy_routes) - set(persistent_path_fallback_routes))
                stable_text = ", ".join(f"`{route}`" for route in stable_recovery_unique[:3]) or "`none`"
                relapse_text = ", ".join(f"`{route}`" for route in relapse_heavy_unique[:3]) or "`none`"
                lines.append(
                    "- recovery/relapse note: "
                    f"`stable recovery route={stable_text}；relapse-heavy route={relapse_text}`"
                )
                stable_recovery_prev_v_values = [
                    value
                    for route in stable_recovery_unique
                    for value in stable_recovery_prev_v_by_route.get(route, [])
                ]
                stable_recovery_relapse_path_values = [
                    value
                    for route in stable_recovery_unique
                    for value in stable_recovery_relapse_path_by_route.get(route, [])
                ]
                relapse_heavy_prev_v_values = [
                    value
                    for route in relapse_heavy_unique
                    for value in relapse_heavy_prev_v_by_route.get(route, [])
                ]
                relapse_heavy_relapse_path_values = [
                    value
                    for route in relapse_heavy_unique
                    for value in relapse_heavy_relapse_path_by_route.get(route, [])
                ]
                if stable_recovery_prev_v_values or relapse_heavy_prev_v_values:
                    stable_prev_v_mean = (
                        sum(stable_recovery_prev_v_values) / len(stable_recovery_prev_v_values)
                        if stable_recovery_prev_v_values
                        else None
                    )
                    relapse_prev_v_mean = (
                        sum(relapse_heavy_prev_v_values) / len(relapse_heavy_prev_v_values)
                        if relapse_heavy_prev_v_values
                        else None
                    )
                    stable_relapse_path_mean = (
                        sum(stable_recovery_relapse_path_values) / len(stable_recovery_relapse_path_values)
                        if stable_recovery_relapse_path_values
                        else None
                    )
                    relapse_relapse_path_mean = (
                        sum(relapse_heavy_relapse_path_values) / len(relapse_heavy_relapse_path_values)
                        if relapse_heavy_relapse_path_values
                        else None
                    )
                    lines.append(
                        "- relapse window note: "
                        f"`stable recovery first-relapse window mean(prev_v={stable_prev_v_mean if stable_prev_v_mean is not None else 'n/a'}, fallback_path_len={stable_relapse_path_mean if stable_relapse_path_mean is not None else 'n/a'})；"
                        f"relapse-heavy mean(prev_v={relapse_prev_v_mean if relapse_prev_v_mean is not None else 'n/a'}, fallback_path_len={relapse_relapse_path_mean if relapse_relapse_path_mean is not None else 'n/a'})`"
                    )
                repeat_verified_unique = sorted(
                    route
                    for route, labels in guarded_repeat_ok_labels_by_route.items()
                    if len(set(labels)) >= 2
                )
                repeat_instability_unique = sorted(
                    route
                    for route, labels in guarded_runtime_ok_labels_by_route.items()
                    if labels
                    and guarded_unstable_labels_by_route.get(route)
                    and len(set(labels).union(set(guarded_unstable_labels_by_route.get(route) or set()))) >= 2
                )
                if repeat_verified_unique or repeat_instability_unique:
                    repeat_verified_text = ", ".join(f"`{route}`" for route in repeat_verified_unique[:3]) or "`none`"
                    repeat_instability_text = ", ".join(f"`{route}`" for route in repeat_instability_unique[:3]) or "`none`"
                    lines.append(
                        "- repeat stability note: "
                        f"`repeat-verified route={repeat_verified_text}；repeat-instability route={repeat_instability_text}`"
                    )
                if repeat_instability_unique:
                    instability_route = repeat_instability_unique[0]
                    instability_rows = [row for row in selected_rows if row.get("route_id") == instability_route]
                    repeat_good_row = max(
                        (
                            row
                            for row in instability_rows
                            if safe_bool(row.get("persistent_path_fallback_at_end")) is not True
                        ),
                        key=lambda item: (
                            safe_float(item.get("route_completion_ratio")) or 0.0,
                            safe_int(item.get("final_normal_tail_length")) or 0,
                        ),
                        default=None,
                    )
                    repeat_bad_row = max(
                        (
                            row
                            for row in instability_rows
                            if safe_bool(row.get("persistent_path_fallback_at_end")) is True
                        ),
                        key=lambda item: (
                            safe_int(item.get("path_fallback_count_after_first_recovery")) or 0,
                            -(safe_int(item.get("first_path_fallback_seq")) or 10**9),
                        ),
                        default=None,
                    )
                    if repeat_good_row is not None and repeat_bad_row is not None:
                        lines.append(
                            "- repeat first-fallback onset note: "
                            f"`route={instability_route} smoke_trigger={repeat_good_row.get('first_path_fallback_trigger_reason_family') or classify_replan_reason(repeat_good_row.get('first_path_fallback_trigger_replan_reason')) or 'unknown'}@{safe_int(repeat_good_row.get('first_path_fallback_trigger_seq')) or 'n/a'}；"
                            f"repeat_trigger={repeat_bad_row.get('first_path_fallback_trigger_reason_family') or classify_replan_reason(repeat_bad_row.get('first_path_fallback_trigger_replan_reason')) or 'unknown'}@{safe_int(repeat_bad_row.get('first_path_fallback_trigger_seq')) or 'n/a'}；"
                            f"smoke_precursor_count={safe_int(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_normal_count')) if safe_int(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_normal_count')) is not None else 'n/a'}；"
                            f"repeat_precursor_count={safe_int(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_normal_count')) if safe_int(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_normal_count')) is not None else 'n/a'}；"
                            f"smoke_precursor_relmax={safe_float(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_rel_min_max')) if safe_float(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_rel_min_max')) is not None else 'n/a'}；"
                            f"repeat_precursor_relmax={safe_float(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_rel_min_max')) if safe_float(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_rel_min_max')) is not None else 'n/a'}；"
                            f"smoke_precursor_v0={safe_float(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_min')) if safe_float(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_min')) is not None else 'n/a'}..{safe_float(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_max')) if safe_float(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_max')) is not None else 'n/a'}；"
                            f"repeat_precursor_v0={safe_float(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_min')) if safe_float(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_min')) is not None else 'n/a'}..{safe_float(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_max')) if safe_float(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_first_point_v_max')) is not None else 'n/a'}；"
                            f"smoke_precursor_points={safe_int(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_point_count_min')) if safe_int(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_point_count_min')) is not None else 'n/a'}..{safe_int(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_point_count_max')) if safe_int(repeat_good_row.get('first_path_fallback_precurrent_time_smaller_point_count_max')) is not None else 'n/a'}；"
                            f"repeat_precursor_points={safe_int(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_point_count_min')) if safe_int(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_point_count_min')) is not None else 'n/a'}..{safe_int(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_point_count_max')) if safe_int(repeat_bad_row.get('first_path_fallback_precurrent_time_smaller_point_count_max')) is not None else 'n/a'}；"
                            f"repeat_bridge_seg_delta={safe_int(repeat_bad_row.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal')) if safe_int(repeat_bad_row.get('first_path_fallback_bridge_route_segment_count_delta_from_prev_normal')) is not None else 'n/a'}；"
                            f"repeat_bridge_prev_reason={repeat_bad_row.get('first_path_fallback_bridge_prev_normal_replan_reason_family') or 'n/a'}；"
                            f"repeat_bridge_prev_routing_segments={safe_int(repeat_bad_row.get('first_path_fallback_bridge_prev_normal_routing_segment_count')) if safe_int(repeat_bad_row.get('first_path_fallback_bridge_prev_normal_routing_segment_count')) is not None else 'n/a'}；"
                            f"repeat_bridge_prev_refline_count={safe_int(repeat_bad_row.get('first_path_fallback_bridge_prev_normal_reference_line_count')) if safe_int(repeat_bad_row.get('first_path_fallback_bridge_prev_normal_reference_line_count')) is not None else 'n/a'}；"
                            f"repeat_bridge_lane_drop={safe_bool(repeat_bad_row.get('first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal')) if safe_bool(repeat_bad_row.get('first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal')) is not None else 'n/a'}；"
                            f"repeat_bridge_failed_ready={safe_bool(repeat_bad_row.get('first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready')) if safe_bool(repeat_bad_row.get('first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready')) is not None else 'n/a'}；"
                            f"repeat_bridge_on_reroute_frame={safe_bool(repeat_bad_row.get('first_path_fallback_bridge_occurs_on_reroute_propagation_frame')) if safe_bool(repeat_bad_row.get('first_path_fallback_bridge_occurs_on_reroute_propagation_frame')) is not None else 'n/a'}；"
                            f"repeat_bridge_codepath={repeat_bad_row.get('first_path_fallback_bridge_likely_codepath_family') or 'n/a'}；"
                            f"repeat_bridge_failure_site={repeat_bad_row.get('first_path_fallback_bridge_likely_provider_failure_site') or 'n/a'}；"
                            f"repeat_bridge_age_from_last_reroute={safe_float(repeat_bad_row.get('first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec')) if safe_float(repeat_bad_row.get('first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec')) is not None else 'n/a'}；"
                            f"smoke_trigger_age_from_last_reroute={safe_float(repeat_good_row.get('first_path_fallback_trigger_planning_header_age_from_last_reroute_sec')) if safe_float(repeat_good_row.get('first_path_fallback_trigger_planning_header_age_from_last_reroute_sec')) is not None else 'n/a'}；"
                            f"repeat_trigger_age_from_last_reroute={safe_float(repeat_bad_row.get('first_path_fallback_trigger_planning_header_age_from_last_reroute_sec')) if safe_float(repeat_bad_row.get('first_path_fallback_trigger_planning_header_age_from_last_reroute_sec')) is not None else 'n/a'}；"
                            f"smoke_trigger_rtmin={safe_float(repeat_good_row.get('first_path_fallback_trigger_relative_time_min_sec')) if safe_float(repeat_good_row.get('first_path_fallback_trigger_relative_time_min_sec')) is not None else 'n/a'}；"
                            f"repeat_trigger_rtmin={safe_float(repeat_bad_row.get('first_path_fallback_trigger_relative_time_min_sec')) if safe_float(repeat_bad_row.get('first_path_fallback_trigger_relative_time_min_sec')) is not None else 'n/a'}`"
                        )
            def _persistent_row_rank(item: Dict[str, Any]) -> Tuple[int, float]:
                saturation_issue = (
                    (safe_float(item.get("raw_steer_saturated_ratio")) or 0.0) > 0.05
                    or (safe_float(item.get("guard_trigger_ratio")) or 0.0) > 0.02
                )
                completion = safe_float(item.get("route_completion_ratio")) or 0.0
                return (1 if saturation_issue else 0, float(completion))
            strongest_persistent = min(
                persistent_path_fallback_rows,
                key=_persistent_row_rank,
            )
            strongest_persistent_run_dir = Path(str(strongest_persistent["run_dir"]))
            strongest_persistent_summary = load_json(strongest_persistent_run_dir / "summary.json")
            strongest_persistent_planning = strongest_persistent_summary.get("planning_trajectory_type_summary") or {}
            if safe_int(strongest_persistent_planning.get("max_consecutive_path_fallback_length")) is None or safe_int(
                strongest_persistent_planning.get("final_normal_tail_length")
            ) is None:
                strongest_persistent_planning = _compute_planning_trajectory_type_summary(
                    load_jsonl(strongest_persistent_run_dir / "artifacts" / "planning_topic_debug.jsonl"),
                    route_debug_rows=load_jsonl(
                        strongest_persistent_run_dir / "artifacts" / "stage5_apollo_reference_line_debug.jsonl"
                    ),
                )
            lines.append(
                "- persistent fallback focus: "
                f"`当前最强 persistent 样本 route={strongest_persistent.get('route_id')}, "
                f"comparison_label={strongest_persistent.get('comparison_label')}, "
                f"max_consecutive_path_fallback_length={safe_int(strongest_persistent.get('max_consecutive_path_fallback_length')) if safe_int(strongest_persistent.get('max_consecutive_path_fallback_length')) is not None else safe_int(strongest_persistent_planning.get('max_consecutive_path_fallback_length'))}, "
                f"final_suffix_start_seq={strongest_persistent.get('final_path_fallback_suffix_start_seq')}, "
                f"final_suffix_length={strongest_persistent.get('final_path_fallback_suffix_length')}, "
                f"final_normal_tail_length={safe_int(strongest_persistent.get('final_normal_tail_length')) if safe_int(strongest_persistent.get('final_normal_tail_length')) is not None else safe_int(strongest_persistent_planning.get('final_normal_tail_length'))}`"
            )
        if safe_bool(recommended.get("force_zero_steer_output")):
            lines.append(
                "- runtime mode note: `当前最佳样本仍是 non-lateral closure 下的 steer-gated 样本，"
                "force_zero_steer_output=true；当前 completion 只能视作非 lateral 模式下的下界`"
            )
        elif any_lateral_runtime and not best_lateral_has_saturation_issue:
            lines.append(
                "- runtime mode note: `guarded lateral-enabled 样本已在多条 route 上进入 CRUISE_ACTIVE；"
                "steer saturation / guard over-trigger 已不再是当前首要矛盾，主问题转为 progress / completion 偏低`"
            )
    else:
        lines.append("3. 当前 Town01 主线最推荐怎么跑: `tools/run_town01_route_health.py run --sample-size 12`")
        lines.append("4. 当前 top3 健康问题是什么: `baseline not executed yet`")
        lines.append("5. 当前 route completion 和 behavior health 是否比之前更好: `baseline not executed yet`")
        lines.append("6. summary/finalization 是否更可信: `implementation added; batch validation pending`")
        lines.append("7. 单一主线是否已能承接后续横向验证和 Demo: `implementation added; full validation pending`")
    if selected_rows and any_lateral_runtime and best_lateral_has_saturation_issue:
        lines.append("8. 下一阶段最该继续推进的一个最小点是什么: `继续压 guarded lateral 样本的 steer saturation / guard over-trigger，同时提升 completion`")
    elif selected_rows and any_lateral_runtime and persistent_path_fallback_routes:
        lines.append("8. 下一阶段最该继续推进的一个最小点是什么: `继续区分可恢复与不可恢复的 PATH_FALLBACK route，并优先定位 persistent PATH_FALLBACK 的 planning 行为根因`")
    elif selected_rows and any_lateral_runtime:
        lines.append("8. 下一阶段最该继续推进的一个最小点是什么: `继续提升 guarded lateral 样本的 route progress / completion，并扩到更多 corpus routes`")
    elif selected_rows and any_control_closure:
        lines.append("8. 下一阶段最该继续推进的一个最小点是什么: `在已建立的 control closure 上继续提升 completion / behavior health，并争取稳定 lateral runtime`")
    else:
        lines.append("8. 下一阶段最该继续推进的一个最小点是什么: `继续压 summary/finalization 漂移后的 control consume / completion 主矛盾`")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
