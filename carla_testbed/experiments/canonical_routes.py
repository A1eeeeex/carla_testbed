from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

SCHEMA_VERSION = "canonical_routes.v1"
HARD_GATE_STABLE_IDS = {
    "town01_rh_spawn097_goal046": "lane_keep_097",
    "town01_rh_spawn217_goal046": "lane_keep_217",
    "town01_rh_spawn031_goal056": "junction_031",
}
CURVE_DIAGNOSTIC_STABLE_IDS = {
    "town01_rh_spawn217_goal048",
    "town01_rh_spawn213_goal059",
}


class CanonicalRoutesError(ValueError):
    pass


@dataclass(frozen=True)
class CanonicalRoutesValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": list(self.errors), "warnings": list(self.warnings)}


def _as_routes(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    routes = config.get("routes")
    if not isinstance(routes, list):
        return []
    return [route for route in routes if isinstance(route, dict)]


def _basename_set(values: Sequence[Any]) -> set[str]:
    return {Path(str(value)).name for value in values}


def _has_placeholder(route: Mapping[str, Any], key: str) -> bool:
    value = route.get(key)
    if value is None:
        return False
    text = str(value)
    return text.startswith("placeholder:") or text.startswith("todo:")


def _validate_required_artifacts(route: Mapping[str, Any], errors: list[str]) -> None:
    route_id = route.get("route_id")
    artifacts = route.get("required_artifacts")
    if not isinstance(artifacts, list):
        errors.append(f"{route_id}: required_artifacts must be a list")
        return
    names = _basename_set(artifacts)
    for required in ("summary.json", "manifest.json", "route_health.json"):
        if required not in names:
            errors.append(f"{route_id}: required_artifacts missing {required}")
    if not {"timeseries.csv", "timeseries.jsonl"}.intersection(names):
        errors.append(f"{route_id}: required_artifacts must include timeseries.csv or timeseries.jsonl")


def validate_canonical_routes(
    config: Mapping[str, Any],
    *,
    require_hard_gates: bool = True,
) -> CanonicalRoutesValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if config.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if config.get("map") != "Town01":
        errors.append("map must be Town01")
    routes = _as_routes(config)
    if not routes:
        errors.append("routes must be a non-empty list")
        return CanonicalRoutesValidation(tuple(errors), tuple(warnings))

    seen_route_ids: set[str] = set()
    seen_stable_ids: set[str] = set()
    stable_index: dict[str, Mapping[str, Any]] = {}
    required_fields = {
        "route_id",
        "stable_id",
        "map",
        "route_class",
        "curve_class",
        "expected_duration_s",
        "success_criteria",
        "no_regression_tags",
        "required_artifacts",
        "gate_role",
        "notes",
    }

    for route in routes:
        route_id = str(route.get("route_id") or "")
        stable_id = str(route.get("stable_id") or "")
        missing = sorted(field for field in required_fields if field not in route)
        if missing:
            errors.append(f"{route_id or '<missing route_id>'}: missing fields {', '.join(missing)}")
        if route_id in seen_route_ids:
            errors.append(f"duplicate route_id: {route_id}")
        if stable_id in seen_stable_ids:
            errors.append(f"duplicate stable_id: {stable_id}")
        if route_id:
            seen_route_ids.add(route_id)
        if stable_id:
            seen_stable_ids.add(stable_id)
            stable_index[stable_id] = route
        if route.get("map") != config.get("map"):
            errors.append(f"{route_id}: route map must match top-level map")
        if route.get("gate_role") not in {"hard_gate", "diagnostic_gate", "informational"}:
            errors.append(f"{route_id}: invalid gate_role {route.get('gate_role')!r}")
        if "spawn_pose" not in route and "spawn_ref" not in route:
            errors.append(f"{route_id}: requires spawn_pose or spawn_ref")
        if "goal_pose" not in route and "goal_ref" not in route:
            warnings.append(f"{route_id}: missing goal_pose/goal_ref")
        if "route_definition" not in route and "route_ref" not in route:
            errors.append(f"{route_id}: requires route_definition or route_ref")
        if _has_placeholder(route, "route_ref"):
            warnings.append(f"{route_id}: route_ref is a placeholder")
        if _has_placeholder(route, "spawn_ref"):
            warnings.append(f"{route_id}: spawn_ref is a placeholder")
        _validate_required_artifacts(route, errors)

    if require_hard_gates:
        for stable_id, expected_tag in HARD_GATE_STABLE_IDS.items():
            route = stable_index.get(stable_id)
            if route is None:
                errors.append(f"missing required hard gate stable_id: {stable_id}")
                continue
            if route.get("gate_role") != "hard_gate":
                errors.append(f"{route.get('route_id')}: {stable_id} must use gate_role=hard_gate")
            tags = {str(tag) for tag in route.get("no_regression_tags") or []}
            if expected_tag not in tags:
                errors.append(f"{route.get('route_id')}: no_regression_tags missing {expected_tag}")

    for stable_id in CURVE_DIAGNOSTIC_STABLE_IDS:
        route = stable_index.get(stable_id)
        if route is None:
            warnings.append(f"missing recommended curve diagnostic stable_id: {stable_id}")
        elif route.get("gate_role") != "diagnostic_gate":
            errors.append(f"{route.get('route_id')}: {stable_id} should use gate_role=diagnostic_gate")

    return CanonicalRoutesValidation(tuple(errors), tuple(warnings))


def load_canonical_routes(
    path: str | Path,
    *,
    require_hard_gates: bool | None = None,
) -> dict[str, Any]:
    route_path = Path(path).expanduser()
    payload = yaml.safe_load(route_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise CanonicalRoutesError(f"canonical route config must be a mapping: {route_path}")
    if require_hard_gates is None:
        require_hard_gates = payload.get("name") == "town01_canonical_five"
    validation = validate_canonical_routes(payload, require_hard_gates=bool(require_hard_gates))
    if validation.errors:
        raise CanonicalRoutesError("; ".join(validation.errors))
    payload["_validation"] = validation.to_dict()
    payload["_source_path"] = str(route_path)
    return payload


def list_hard_gates(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [route for route in _as_routes(config) if route.get("gate_role") == "hard_gate"]


def list_diagnostic_gates(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [route for route in _as_routes(config) if route.get("gate_role") == "diagnostic_gate"]


def list_informational_routes(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [route for route in _as_routes(config) if route.get("gate_role") == "informational"]


def route_ids(config: Mapping[str, Any]) -> list[str]:
    return [str(route["route_id"]) for route in _as_routes(config)]
