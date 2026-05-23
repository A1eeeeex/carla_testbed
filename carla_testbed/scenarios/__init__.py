from __future__ import annotations

from .base import ActorRefs, Scenario, ScenarioRunner, ScenarioSpec, ScenarioState
from .registry import (
    DEFAULT_SCENARIO_REGISTRY,
    ScenarioEntry,
    ScenarioRegistry,
    build_scenario,
    get_scenario_builder,
    get_scenario_entry,
)


_LAZY_EXPORTS = {
    "ApolloSemanticSuiteConfig": (".apollo_semantic_suite", "ApolloSemanticSuiteConfig"),
    "ApolloSemanticSuiteScenario": (".apollo_semantic_suite", "ApolloSemanticSuiteScenario"),
    "CalibrationOnlyConfig": (".calibration_only", "CalibrationOnlyConfig"),
    "CalibrationOnlyScenario": (".calibration_only", "CalibrationOnlyScenario"),
    "FollowStopConfig": (".followstop", "FollowStopConfig"),
    "FollowStopScenario": (".followstop", "FollowStopScenario"),
    "Town01RouteHealthConfig": (".town01_route_health", "Town01RouteHealthConfig"),
    "Town01RouteHealthScenario": (".town01_route_health", "Town01RouteHealthScenario"),
}


def __getattr__(name: str):
    try:
        module_name, attr_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    import importlib

    module = importlib.import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


__all__ = [
    "ActorRefs",
    "Scenario",
    "ScenarioRunner",
    "ScenarioSpec",
    "ScenarioState",
    "ScenarioEntry",
    "ScenarioRegistry",
    "DEFAULT_SCENARIO_REGISTRY",
    "build_scenario",
    "get_scenario_builder",
    "get_scenario_entry",
    "FollowStopScenario",
    "FollowStopConfig",
    "ApolloSemanticSuiteScenario",
    "ApolloSemanticSuiteConfig",
    "CalibrationOnlyScenario",
    "CalibrationOnlyConfig",
    "Town01RouteHealthScenario",
    "Town01RouteHealthConfig",
]
