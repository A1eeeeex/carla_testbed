from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

TRAFFIC_FLOW_PROFILE_SCHEMA_VERSION = "traffic_flow_profile.v1"
VALID_PROVIDERS = {
    "none",
    "carla_traffic_manager",
    "carla_walker_ai_controller",
    "mixed_carla_flow",
}


class TrafficFlowConfigError(ValueError):
    pass


def load_traffic_flow_profile(path: str | Path) -> dict[str, Any]:
    profile_path = Path(path).expanduser()
    payload = yaml.safe_load(profile_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise TrafficFlowConfigError(f"traffic flow profile must be a mapping: {profile_path}")
    errors = validate_traffic_flow_profile(payload)
    if errors:
        raise TrafficFlowConfigError("; ".join(errors))
    payload["_source_path"] = str(profile_path)
    return payload


def validate_traffic_flow_profile(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != TRAFFIC_FLOW_PROFILE_SCHEMA_VERSION:
        errors.append(f"schema_version must be {TRAFFIC_FLOW_PROFILE_SCHEMA_VERSION}")
    traffic_flow = payload.get("traffic_flow")
    if not isinstance(traffic_flow, Mapping):
        errors.append("traffic_flow must be a mapping")
        return errors
    provider = str(traffic_flow.get("provider") or "").strip()
    enabled = bool(traffic_flow.get("enabled", False))
    if not provider:
        errors.append("traffic_flow.provider is required")
    elif provider not in VALID_PROVIDERS:
        errors.append(f"traffic_flow.provider must be one of {sorted(VALID_PROVIDERS)}")
    if enabled and provider != "none" and traffic_flow.get("seed") is None:
        errors.append("traffic_flow.seed is required for deterministic traffic flow")
    if provider in {"carla_traffic_manager", "mixed_carla_flow"} and enabled:
        vehicles_enabled_default = provider == "carla_traffic_manager"
        vehicles = traffic_flow.get("vehicles")
        vehicles_enabled = bool(vehicles.get("enabled", vehicles_enabled_default)) if isinstance(vehicles, Mapping) else False
        if vehicles_enabled:
            _validate_count_block(
                errors,
                vehicles,
                block_name="traffic_flow.vehicles",
                count_name="traffic_flow.vehicles.count",
            )
        tm_cfg = traffic_flow.get("traffic_manager")
        if vehicles_enabled and not isinstance(tm_cfg, Mapping):
            errors.append("traffic_flow.traffic_manager must be a mapping for CARLA Traffic Manager vehicles")
    if provider in {"carla_walker_ai_controller", "mixed_carla_flow"} and enabled:
        walkers_enabled_default = provider == "carla_walker_ai_controller"
        walkers = traffic_flow.get("walkers")
        walkers_enabled = bool(walkers.get("enabled", walkers_enabled_default)) if isinstance(walkers, Mapping) else False
        if walkers_enabled:
            _validate_count_block(
                errors,
                walkers,
                block_name="traffic_flow.walkers",
                count_name="traffic_flow.walkers.count",
            )
            controller = walkers.get("controller") if isinstance(walkers, Mapping) else None
            if controller is not None and not isinstance(controller, Mapping):
                errors.append("traffic_flow.walkers.controller must be a mapping when provided")
    return errors


def _validate_count_block(
    errors: list[str],
    block: Any,
    *,
    block_name: str,
    count_name: str,
) -> None:
    if not isinstance(block, Mapping):
        errors.append(f"{block_name} must be a mapping when enabled")
        return
    count = block.get("count")
    if not isinstance(count, int) or count < 1:
        errors.append(f"{count_name} must be a positive integer")
