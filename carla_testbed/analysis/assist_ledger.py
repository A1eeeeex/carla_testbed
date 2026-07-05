from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

ASSIST_LEDGER_SCHEMA_VERSION = "assist_ledger.v1"

ASSIST_NAMES = {
    "carla_direct_transport",
    "straight_lane_lateral_stabilizer",
    "straight_acc_override",
    "terminal_stop_hold",
    "goal_planner_module_disabled",
    "planning_common_dynamics_override",
    "speed_feedback_bridge_profile",
    "dummy_lateral",
    "force_green",
    "legacy_followstop",
    "route_follower",
    "direct_autopilot",
    "manual_intervention",
    "steering_sign_diagnostic_override",
}

DEFAULT_BLOCKING_ASSISTS = {
    "straight_lane_lateral_stabilizer",
    "straight_acc_override",
    "terminal_stop_hold",
    "goal_planner_module_disabled",
    "planning_common_dynamics_override",
    "speed_feedback_bridge_profile",
    "dummy_lateral",
    "force_green",
    "legacy_followstop",
    "route_follower",
    "direct_autopilot",
    "manual_intervention",
    "steering_sign_diagnostic_override",
}

NON_BLOCKING_ASSISTS = {
    "carla_direct_transport",
}

SOURCE_PRIORITY = ("manifest", "summary", "bridge_stats", "config", "manual", "unknown")


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "enabled", "on"}


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _iter_mappings(payload: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping):
        found.append(payload)
        for value in payload.values():
            found.extend(_iter_mappings(value))
    elif isinstance(payload, list):
        for value in payload:
            found.extend(_iter_mappings(value))
    return found


def _iter_values_for_key(payload: Any, target_keys: set[str]) -> list[Any]:
    found: list[Any] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            if str(key) in target_keys:
                found.append(value)
            found.extend(_iter_values_for_key(value, target_keys))
    elif isinstance(payload, list):
        for value in payload:
            found.extend(_iter_values_for_key(value, target_keys))
    return found


def _normalize_assist_items(value: Any) -> tuple[list[str], set[str]]:
    active: list[str] = []
    diagnostic_only: set[str] = set()
    items = value if isinstance(value, list) else [value]
    for item in items:
        name = ""
        diagnostic = False
        if isinstance(item, Mapping):
            name = str(item.get("name") or item.get("assist") or item.get("assist_name") or "").strip()
            diagnostic = _boolish(item.get("diagnostic_only"))
        else:
            name = str(item or "").strip()
        if not name:
            continue
        if name in ASSIST_NAMES:
            active.append(name)
            if diagnostic:
                diagnostic_only.add(name)
    return active, diagnostic_only


def _record_assist(
    active: set[str],
    assist_sources: dict[str, str],
    assist: str,
    source: str,
    *,
    force_source: bool = False,
) -> None:
    if assist not in ASSIST_NAMES:
        return
    active.add(assist)
    if force_source or assist not in assist_sources:
        assist_sources[assist] = source


def _source_artifact(sources: Mapping[str, str]) -> str:
    values = set(str(value) for value in sources.values() if value)
    for source in SOURCE_PRIORITY:
        if source in values:
            return source
    return "unknown"


def classify_assists(
    active_assists: Sequence[Any],
    *,
    diagnostic_only_assists: Sequence[str] | None = None,
) -> dict[str, list[str] | bool]:
    active, item_diagnostic_only = _normalize_assist_items(list(active_assists))
    diagnostic_only = set(str(item) for item in (diagnostic_only_assists or [])) | item_diagnostic_only
    blocking: list[str] = []
    non_blocking: list[str] = []
    for assist in sorted(set(active)):
        if assist in diagnostic_only:
            non_blocking.append(assist)
        elif assist in DEFAULT_BLOCKING_ASSISTS:
            blocking.append(assist)
        else:
            non_blocking.append(assist)
    return {
        "active_assists": sorted(set(active)),
        "blocking_assists": blocking,
        "non_blocking_assists": non_blocking,
        "can_claim_unassisted_natural_driving": not blocking,
    }


def _merge_existing_ledger(
    payload: Mapping[str, Any],
    *,
    active: set[str],
    assist_sources: dict[str, str],
    diagnostic_only: set[str],
    source: str,
) -> bool:
    ledger = payload.get("assist_ledger")
    if not isinstance(ledger, Mapping):
        return False
    explicit = str(ledger.get("assist_confidence") or "").strip() in {"explicit", "inferred"}
    for assist in ledger.get("active_assists") or []:
        normalized, item_diagnostic = _normalize_assist_items(assist)
        diagnostic_only.update(item_diagnostic)
        for name in normalized:
            _record_assist(active, assist_sources, name, source, force_source=True)
            explicit = True
    for assist in ledger.get("non_blocking_assists") or []:
        normalized, _item_diagnostic = _normalize_assist_items(assist)
        diagnostic_only.update(normalized)
        for name in normalized:
            _record_assist(active, assist_sources, name, source, force_source=True)
            explicit = True
    for assist in ledger.get("diagnostic_only_assists") or []:
        if str(assist) in ASSIST_NAMES:
            diagnostic_only.add(str(assist))
            explicit = True
    return explicit


def _collect_explicit_assists(
    payload: Mapping[str, Any],
    *,
    active: set[str],
    assist_sources: dict[str, str],
    diagnostic_only: set[str],
    source: str,
) -> bool:
    explicit = _merge_existing_ledger(
        payload,
        active=active,
        assist_sources=assist_sources,
        diagnostic_only=diagnostic_only,
        source=source,
    )
    for key in ("active_assists", "declared_assists", "expected_assists", "assists"):
        for value in _iter_values_for_key(payload, {key}):
            names, item_diagnostic = _normalize_assist_items(value)
            if names:
                explicit = True
            diagnostic_only.update(item_diagnostic)
            for name in names:
                _record_assist(active, assist_sources, name, source, force_source=True)
    for value in _iter_values_for_key(payload, {"diagnostic_only_assists"}):
        names, _item_diagnostic = _normalize_assist_items(value)
        diagnostic_only.update(names)
    return explicit


def _collect_inferred_assists(
    payload: Mapping[str, Any],
    *,
    active: set[str],
    assist_sources: dict[str, str],
    source: str,
) -> bool:
    inferred = False
    for item in _iter_mappings(payload):
        for key, value in item.items():
            key_text = str(key)
            if key_text in {"backend", "backend_name", "transport_mode"} and str(value) == "carla_direct":
                _record_assist(active, assist_sources, "carla_direct_transport", source)
                inferred = True
            if key_text in {"carla_direct_transport", "carla_direct_transport_enabled"} and _boolish(value):
                _record_assist(active, assist_sources, "carla_direct_transport", source)
                inferred = True
            if key_text in {
                "straight_lane_lateral_stabilizer",
                "straight_lane_lateral_stabilizer_enabled",
                "enable_lateral_stabilizer",
            } and _boolish(value):
                _record_assist(active, assist_sources, "straight_lane_lateral_stabilizer", source)
                inferred = True
            if key_text in {
                "straight_lane_lateral_stabilizer_apply_count",
                "lateral_stabilizer_apply_count",
            } and (_num(value) or 0.0) > 0:
                _record_assist(active, assist_sources, "straight_lane_lateral_stabilizer", source)
                inferred = True
            if key_text in {
                "straight_acc_override",
                "straight_acc_override_enabled",
                "enable_straight_acc_override",
            } and _boolish(value):
                _record_assist(active, assist_sources, "straight_acc_override", source)
                inferred = True
            if key_text in {"straight_acc_override_apply_count"} and (_num(value) or 0.0) > 0:
                _record_assist(active, assist_sources, "straight_acc_override", source)
                inferred = True
            if key_text in {"terminal_stop_hold", "terminal_stop_hold_enabled"} and _boolish(value):
                _record_assist(active, assist_sources, "terminal_stop_hold", source)
                inferred = True
            if key_text in {"terminal_stop_hold_apply_count"} and (_num(value) or 0.0) > 0:
                _record_assist(active, assist_sources, "terminal_stop_hold", source)
                inferred = True
            if key_text == "launch_goal_planner_module" and value is not None and _boolish(value) is False:
                _record_assist(active, assist_sources, "goal_planner_module_disabled", source)
                inferred = True
            if key_text in {
                "goal_planner_module_disabled",
                "launch_goal_planner_module_disabled",
            } and _boolish(value):
                _record_assist(active, assist_sources, "goal_planner_module_disabled", source)
                inferred = True
            if key_text.startswith("planning_common_") or key_text in {"planning_common_overrides"}:
                _record_assist(active, assist_sources, "planning_common_dynamics_override", source)
                inferred = True
            if key_text in {"longitudinal_mode"} and str(value) == "speed_feedback":
                _record_assist(active, assist_sources, "speed_feedback_bridge_profile", source)
                inferred = True
            if key_text.startswith("speed_feedback_"):
                _record_assist(active, assist_sources, "speed_feedback_bridge_profile", source)
                inferred = True
            if key_text in {
                "dummy_lateral",
                "dummy_lateral_enabled",
                "enable_dummy_lateral",
            } and _boolish(value):
                _record_assist(active, assist_sources, "dummy_lateral", source)
                inferred = True
            if key_text == "lateral_mode" and str(value).strip().lower() == "dummy":
                if not _legacy_controller_placeholder_not_applied(item):
                    _record_assist(active, assist_sources, "dummy_lateral", source)
                    inferred = True
            if key_text in {"force_green", "traffic_light_force_green"} and _boolish(value):
                _record_assist(active, assist_sources, "force_green", source)
                inferred = True
            if key_text in {"traffic_light_policy", "policy"} and str(value).strip().lower() == "force_green":
                _record_assist(active, assist_sources, "force_green", source)
                inferred = True
            if key_text in {"stimulus_mode", "traffic_light_stimulus_mode"} and str(value).strip().lower() == "force_green":
                _record_assist(active, assist_sources, "force_green", source)
                inferred = True
            if key_text in {
                "legacy_followstop",
                "legacy_followstop_enabled",
                "legacy_follow_stop",
                "legacy_follow_stop_enabled",
            } and _boolish(value):
                _record_assist(active, assist_sources, "legacy_followstop", source)
                inferred = True
            if key_text in {
                "legacy_followstop_apply_count",
                "legacy_follow_stop_apply_count",
                "followstop_legacy_apply_count",
            } and (_num(value) or 0.0) > 0:
                _record_assist(active, assist_sources, "legacy_followstop", source)
                inferred = True
            if key_text in {
                "route_follower",
                "route_follower_enabled",
                "enable_route_follower",
            } and _boolish(value):
                _record_assist(active, assist_sources, "route_follower", source)
                inferred = True
            if key_text in {
                "route_follower_apply_count",
                "route_follower_override_count",
            } and (_num(value) or 0.0) > 0:
                _record_assist(active, assist_sources, "route_follower", source)
                inferred = True
            if key_text in {
                "direct_autopilot",
                "direct_autopilot_enabled",
                "carla_autopilot_enabled",
                "autopilot_enabled",
            } and _boolish(value):
                _record_assist(active, assist_sources, "direct_autopilot", source)
                inferred = True
            if key_text in {
                "direct_autopilot_apply_count",
                "autopilot_apply_count",
                "carla_autopilot_apply_count",
            } and (_num(value) or 0.0) > 0:
                _record_assist(active, assist_sources, "direct_autopilot", source)
                inferred = True
            if key_text in {
                "manual_intervention",
                "manual_intervention_detected",
                "manual_override",
                "manual_override_active",
            } and _boolish(value):
                _record_assist(active, assist_sources, "manual_intervention", source)
                inferred = True
            if key_text in {
                "manual_intervention_count",
                "manual_override_count",
                "manual_control_apply_count",
            } and (_num(value) or 0.0) > 0:
                _record_assist(active, assist_sources, "manual_intervention", source)
                inferred = True
            if key_text in {"control_source", "controller"}:
                mode = str(value).strip().lower()
                if mode in {"route_follower", "route-following", "route_following"}:
                    _record_assist(active, assist_sources, "route_follower", source)
                    inferred = True
                if mode in {"direct_autopilot", "carla_autopilot", "autopilot"}:
                    _record_assist(active, assist_sources, "direct_autopilot", source)
                    inferred = True
                if mode in {"manual", "manual_control", "manual_intervention"}:
                    _record_assist(active, assist_sources, "manual_intervention", source)
                    inferred = True
    return inferred


def _legacy_controller_placeholder_not_applied(item: Mapping[str, Any]) -> bool:
    role = str(item.get("legacy_controller_role") or item.get("controller_role") or item.get("role") or "").strip().lower()
    if role in {"compatibility_placeholder", "external_stack_placeholder"}:
        return True
    control_source = str(item.get("control_source") or "").strip().lower()
    if control_source == "external_stack":
        return True
    if _boolish(item.get("harness_control_disabled")):
        return True
    if item.get("legacy_controller_applied") is False:
        return True
    if item.get("applied") is False:
        return True
    if item.get("control_applied") is False and "lateral_mode" in item:
        return True
    return False


def build_assist_ledger(
    *,
    config: Mapping[str, Any] | None = None,
    bridge_stats: Mapping[str, Any] | None = None,
    summary: Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    active: set[str] = set()
    assist_sources: dict[str, str] = {}
    diagnostic_only: set[str] = set()
    explicit_sources: set[str] = set()
    explicit_seen = False
    inferred_seen = False
    for source, payload in (
        ("manifest", _as_mapping(manifest)),
        ("summary", _as_mapping(summary)),
        ("bridge_stats", _as_mapping(bridge_stats)),
        ("config", _as_mapping(config)),
    ):
        source_explicit = _collect_explicit_assists(
            payload,
            active=active,
            assist_sources=assist_sources,
            diagnostic_only=diagnostic_only,
            source=source,
        )
        if source_explicit:
            explicit_sources.add(source)
        explicit_seen = source_explicit or explicit_seen
        inferred_seen = (
            _collect_inferred_assists(
                payload,
                active=active,
                assist_sources=assist_sources,
                source=source,
            )
            or inferred_seen
        )
    classified = classify_assists(sorted(active), diagnostic_only_assists=sorted(diagnostic_only))
    if explicit_seen:
        confidence = "explicit"
    elif inferred_seen:
        confidence = "inferred"
    else:
        confidence = "unknown"
    notes: list[str] = []
    if classified["blocking_assists"]:
        notes.append("blocking assists prevent unassisted natural-driving claims")
    if "carla_direct_transport" in classified["active_assists"]:
        notes.append("carla_direct_transport is recorded as a non-blocking transport candidate")
    if confidence == "unknown":
        notes.append("no assist declaration found; ledger assumes no active assists for offline analysis")
    source_artifact = _source_artifact(assist_sources)
    if source_artifact == "unknown" and explicit_sources:
        source_artifact = _source_artifact({source: source for source in explicit_sources})
    return {
        "schema_version": ASSIST_LEDGER_SCHEMA_VERSION,
        "active_assists": classified["active_assists"],
        "blocking_assists": classified["blocking_assists"],
        "non_blocking_assists": classified["non_blocking_assists"],
        "assist_sources": {name: assist_sources.get(name, "unknown") for name in classified["active_assists"]},
        "assist_confidence": confidence,
        "source_artifact": source_artifact,
        "can_claim_unassisted_natural_driving": classified["can_claim_unassisted_natural_driving"],
        "notes": notes,
    }


def build_runtime_assist_ledger(
    *,
    config: Mapping[str, Any] | None = None,
    bridge_stats: Mapping[str, Any] | None = None,
    summary: Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an artifact-grade assist ledger for online run outputs.

    Offline analysis may treat a blank fixture as "no assists." Runtime
    artifacts need a stricter boundary: if no source declares or implies assist
    state, the run cannot support an unassisted natural-driving claim.
    """
    ledger = build_assist_ledger(
        config=config,
        bridge_stats=bridge_stats,
        summary=summary,
        manifest=manifest,
    )
    warnings = list(ledger.get("warnings") or [])
    if ledger.get("assist_confidence") == "unknown":
        ledger["can_claim_unassisted_natural_driving"] = False
        warning = "assist_ledger_unknown_unassisted_claim_not_allowed"
        if warning not in warnings:
            warnings.append(warning)
        note = "runtime artifact does not prove absence of active assists"
        notes = [
            item
            for item in list(ledger.get("notes") or [])
            if item != "no assist declaration found; ledger assumes no active assists for offline analysis"
        ]
        if note not in notes:
            notes.append(note)
        ledger["notes"] = notes
    ledger["warnings"] = warnings
    return ledger


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _embedded_ledger(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    ledger = payload.get("assist_ledger")
    if isinstance(ledger, Mapping) and ledger.get("schema_version") == ASSIST_LEDGER_SCHEMA_VERSION:
        return dict(ledger)
    return None


def _first_existing(root: Path, *relative_paths: str) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    return None


def _read_config_with_assist_ledger(root: Path) -> dict[str, Any]:
    fallback: dict[str, Any] = {}
    for relative in ("config.resolved.yaml", "effective_config.yaml", "effective.yaml"):
        path = root / relative
        if not path.exists():
            continue
        payload = _read_yaml(path)
        if not fallback:
            fallback = payload
        if isinstance(payload.get("assist_ledger"), Mapping):
            return payload
    return fallback


def read_assist_ledger_from_run_dir(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    explicit_path = _first_existing(
        root,
        "assist_ledger.json",
        "artifacts/assist_ledger.json",
        "analysis/assist_ledger/assist_ledger.json",
    )
    if explicit_path is not None:
        payload = _read_json(explicit_path)
        if payload.get("schema_version") == ASSIST_LEDGER_SCHEMA_VERSION:
            return dict(payload)
    summary_payload = _read_json(root / "summary.json")
    manifest_payload = _read_json(root / "manifest.json")
    if _embedded_ledger(summary_payload) is not None or _embedded_ledger(manifest_payload) is not None:
        return build_runtime_assist_ledger(summary=summary_payload, manifest=manifest_payload)
    bridge_stats_path = _first_existing(
        root,
        "artifacts/direct_bridge_stats.json",
        "artifacts/cyber_bridge_stats.json",
        "direct_bridge_stats.json",
        "cyber_bridge_stats.json",
    )
    return build_assist_ledger(
        config=_read_config_with_assist_ledger(root),
        bridge_stats=_read_json(bridge_stats_path) if bridge_stats_path is not None else None,
        summary=summary_payload,
        manifest=manifest_payload,
    )
