from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


CAPABILITY_TO_SCENARIO_CLASS = {
    "lane_keep": "lane_keep",
    "curve_lane_follow": "curve_diagnostic",
    "junction_traverse": "junction_turn",
}


def online_claim_manifest_updates(
    *,
    effective_config: Mapping[str, Any] | None = None,
    scenario_metadata: Mapping[str, Any] | None = None,
    summary: Mapping[str, Any] | None = None,
    config_path: str | Path | None = None,
    profile_name: str | None = None,
) -> dict[str, Any]:
    """Build standard claim-evidence fields for legacy online runs.

    The helper only surfaces values that are explicit in config/runtime artifacts
    or safely inferred from an explicit scenario capability. It must not invent
    algorithm variants or timing metadata for claim-grade reports.
    """

    cfg = effective_config if isinstance(effective_config, Mapping) else {}
    scenario_meta = scenario_metadata if isinstance(scenario_metadata, Mapping) else {}
    summary_map = summary if isinstance(summary, Mapping) else {}
    run_cfg = _mapping(cfg.get("run"))
    scenario_cfg = _mapping(cfg.get("scenario"))
    algo_cfg = _mapping(cfg.get("algo"))
    apollo_cfg = _mapping(algo_cfg.get("apollo"))
    mode_cfg = _mapping(cfg.get("mode"))
    recording_artifacts = _mapping(_mapping(cfg.get("recording")).get("artifacts"))

    route_id = _first_text(scenario_meta.get("route_id"), run_cfg.get("route_id"))
    capability_profile = _first_text(
        run_cfg.get("capability_profile"),
        scenario_meta.get("capability_profile"),
        cfg.get("capability_profile"),
    )
    scenario_class = _first_text(
        scenario_meta.get("scenario_class"),
        run_cfg.get("scenario_class"),
        CAPABILITY_TO_SCENARIO_CLASS.get(str(capability_profile or "")),
    )
    scenario_id = _first_text(
        scenario_meta.get("scenario_id"),
        run_cfg.get("scenario_id"),
        _scenario_id_from_profile(capability_profile, route_id),
    )

    ticks = _first_number(run_cfg.get("ticks"), summary_map.get("frames"))
    fixed_delta_seconds = _first_number(
        run_cfg.get("fixed_delta_seconds"),
        run_cfg.get("fixed_dt_s"),
        run_cfg.get("dt"),
    )
    duration_s = _first_number(run_cfg.get("duration_s"))
    if duration_s is None and ticks is not None and fixed_delta_seconds is not None:
        duration_s = float(ticks) * float(fixed_delta_seconds)

    transport_mode, transport_mode_source, legacy_transport_name, compat_layers = _transport_mode(
        scenario_cfg,
        apollo_cfg,
    )
    stack = str(algo_cfg.get("stack") or "").strip().lower()
    backend = _first_text(
        cfg.get("backend"),
        mode_cfg.get("backend"),
        apollo_cfg.get("backend"),
        recording_artifacts.get("backend"),
        "apollo_cyberrt" if stack == "apollo" else None,
    )
    truth_input = _first_bool(
        cfg.get("truth_input"),
        mode_cfg.get("truth_input"),
        scenario_cfg.get("publish_ros2_gt"),
        recording_artifacts.get("truth_input"),
    )

    updates: dict[str, Any] = {}
    _set_if_present(updates, "scenario_id", scenario_id)
    _set_if_present(updates, "scenario_class", scenario_class)
    _set_if_present(updates, "route_id", route_id)
    _set_if_present(updates, "capability_profile", capability_profile)
    _set_if_present(updates, "algorithm_variant_id", _first_text(
        cfg.get("algorithm_variant_id"),
        mode_cfg.get("algorithm_variant_id"),
        apollo_cfg.get("algorithm_variant_id"),
        recording_artifacts.get("algorithm_variant_id"),
    ))
    _set_if_present(updates, "algorithm_variant_manifest_path", _first_text(
        cfg.get("algorithm_variant_manifest_path"),
        mode_cfg.get("algorithm_variant_manifest_path"),
        apollo_cfg.get("algorithm_variant_manifest_path"),
        recording_artifacts.get("algorithm_variant_manifest_path"),
    ))
    if config_path not in {None, ""}:
        updates["online_config_path"] = str(config_path)
    _set_if_present(updates, "online_config_profile_name", profile_name or run_cfg.get("profile_name"))
    _set_if_present(updates, "map", _first_text(run_cfg.get("map"), scenario_meta.get("map")))
    _set_if_present(updates, "transport_mode", transport_mode)
    _set_if_present(updates, "transport_mode_source", transport_mode_source)
    _set_if_present(updates, "canonical_transport_mode", transport_mode)
    _set_if_present(updates, "legacy_transport_name", legacy_transport_name)
    if compat_layers:
        updates["compat_layers"] = list(compat_layers)
    _set_if_present(updates, "backend", backend)
    if truth_input is not None:
        updates["truth_input"] = bool(truth_input)
    claim_profile = _first_bool(run_cfg.get("claim_profile"), cfg.get("claim_profile"))
    if claim_profile is not None:
        updates["claim_profile"] = bool(claim_profile)
    materialization_probe = _first_bool(
        run_cfg.get("materialization_probe"),
        cfg.get("materialization_probe"),
        _mapping(cfg.get("reports")).get("require_route_materialization"),
    )
    if materialization_probe is not None:
        updates["materialization_probe"] = bool(materialization_probe)
    if duration_s is not None:
        updates["duration_s"] = float(duration_s)
    if fixed_delta_seconds is not None:
        updates["fixed_delta_seconds"] = float(fixed_delta_seconds)
    if ticks is not None:
        updates["ticks"] = int(ticks)
    runtime_contract = _runtime_contract(
        backend=backend,
        stack=stack,
        truth_input=truth_input,
        transport_mode=transport_mode,
        claim_profile=claim_profile,
        materialization_probe=materialization_probe,
        profile=_mapping(summary_map.get("profile")),
        algo_cfg=algo_cfg,
    )
    updates["runtime_contract"] = runtime_contract
    updates["runtime_contract_status"] = runtime_contract["status"]
    for key in (
        "routing_success_count",
        "routing_materialized",
        "planning_message_count",
        "planning_nonempty_count",
        "planning_nonempty_trajectory_ratio",
        "planning_materialized",
        "control_rx_count",
        "control_tx_count",
        "control_handoff_status",
    ):
        _set_if_present(updates, key, summary_map.get(key))
    return updates


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value in {None, ""}:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_bool(*values: Any) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
        if value in {None, ""}:
            continue
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    return None


def _set_if_present(target: dict[str, Any], key: str, value: Any) -> None:
    if value not in {None, ""}:
        target[key] = value


def _scenario_id_from_profile(capability_profile: str | None, route_id: str | None) -> str | None:
    if not capability_profile or not route_id:
        return None
    return f"{capability_profile}_{route_id}"


def _transport_mode(
    scenario_cfg: Mapping[str, Any],
    apollo_cfg: Mapping[str, Any],
) -> tuple[str | None, str | None, str | None, list[str]]:
    explicit = _first_text(apollo_cfg.get("transport_mode"))
    if explicit is not None:
        return (
            _canonical_transport_mode(explicit),
            "online_config.algo.apollo.transport_mode",
            _legacy_transport_name(explicit),
            _compat_layers(explicit),
        )
    publish_ros2_gt = scenario_cfg.get("publish_ros2_gt")
    if publish_ros2_gt is True:
        legacy = "ros2_gt"
        return _canonical_transport_mode(legacy), "online_config.scenario.publish_ros2_gt", legacy, _compat_layers(legacy)
    if publish_ros2_gt is False:
        return "non_ros2_gt", "online_config.scenario.publish_ros2_gt", None, []
    return None, None, None, []


def _canonical_transport_mode(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    if text == "ros2_gt":
        return "apollo_cyberrt_gt_over_ros2_transition"
    return text or None


def _legacy_transport_name(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    if text == "ros2_gt":
        return "ros2_gt"
    return None


def _compat_layers(value: str | None) -> list[str]:
    text = str(value or "").strip().lower()
    if text == "ros2_gt":
        return ["ros2_gt_transition", "legacy_route_health_transition"]
    return []


def _runtime_contract(
    *,
    backend: str | None,
    stack: str,
    truth_input: bool | None,
    transport_mode: str | None,
    claim_profile: bool | None,
    materialization_probe: bool | None,
    profile: Mapping[str, Any],
    algo_cfg: Mapping[str, Any],
) -> dict[str, Any]:
    """Return an explicit runtime contract for online Apollo claim artifacts.

    This is intentionally conservative: it records why the runtime can be used
    as an Apollo truth-input materialization sample, but it does not assert
    natural-driving success or behavior quality.
    """

    blockers: list[str] = []
    unknowns: list[str] = []
    canonical_transport = _canonical_transport_mode(transport_mode)
    expected_transport = "apollo_cyberrt_gt_over_ros2_transition"
    if (backend or "").strip() != "apollo_cyberrt":
        blockers.append("backend_not_apollo_cyberrt")
    if stack and stack != "apollo":
        blockers.append("stack_not_apollo")
    elif not stack:
        unknowns.append("algo.stack")
    if truth_input is not True:
        blockers.append("truth_input_not_enabled" if truth_input is False else "truth_input_unknown")
    if canonical_transport != expected_transport:
        blockers.append("transport_not_apollo_cyberrt_gt_over_ros2_transition")
    harness_disabled = _first_bool(
        profile.get("harness_disable_control_effective"),
        profile.get("disable_legacy_harness_control_for_external_stack"),
        algo_cfg.get("disable_legacy_harness_control_for_external_stack"),
    )
    if harness_disabled is not True:
        blockers.append(
            "harness_control_not_disabled_for_external_stack"
            if harness_disabled is False
            else "harness_control_disable_unknown"
        )
    if claim_profile is not True and materialization_probe is not True:
        blockers.append("claim_or_materialization_profile_not_enabled")
    status = "aligned" if not blockers and not unknowns else ("insufficient_data" if unknowns else "misconfigured")
    return {
        "schema_version": "runtime_contract.v1",
        "status": status,
        "blockers": blockers + unknowns,
        "backend": backend,
        "stack": stack or None,
        "truth_input": truth_input,
        "transport_mode": canonical_transport,
        "harness_control_disabled_for_external_stack": harness_disabled,
        "claim_profile": bool(claim_profile),
        "materialization_probe": bool(materialization_probe),
        "claim_boundary": (
            "Runtime contract alignment is a prerequisite only; it does not prove "
            "Apollo natural-driving behavior without downstream evidence reports."
        ),
    }
