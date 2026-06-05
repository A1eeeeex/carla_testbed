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

    transport_mode, transport_mode_source = _transport_mode(scenario_cfg, apollo_cfg)
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
    _set_if_present(updates, "backend", backend)
    if truth_input is not None:
        updates["truth_input"] = bool(truth_input)
    if duration_s is not None:
        updates["duration_s"] = float(duration_s)
    if fixed_delta_seconds is not None:
        updates["fixed_delta_seconds"] = float(fixed_delta_seconds)
    if ticks is not None:
        updates["ticks"] = int(ticks)
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
) -> tuple[str | None, str | None]:
    explicit = _first_text(apollo_cfg.get("transport_mode"))
    if explicit is not None:
        return explicit, "online_config.algo.apollo.transport_mode"
    publish_ros2_gt = scenario_cfg.get("publish_ros2_gt")
    if publish_ros2_gt is True:
        return "ros2_gt", "online_config.scenario.publish_ros2_gt"
    if publish_ros2_gt is False:
        return "non_ros2_gt", "online_config.scenario.publish_ros2_gt"
    return None, None
