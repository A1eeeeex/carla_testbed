from __future__ import annotations

from pathlib import Path

from carla_testbed.experiments.canonical_routes import (
    load_canonical_routes,
    list_diagnostic_gates,
    list_hard_gates,
    list_informational_routes,
    route_ids,
    validate_canonical_routes,
)


CANONICAL_PATH = Path("configs/routes/town01/canonical_five.yaml")
CURVE_DIAGNOSTICS_PATH = Path("configs/routes/town01/curve_diagnostics.yaml")
RANDOM_POOL_PATH = Path("configs/routes/town01/random_regression_pool_20260416.yaml")


def test_canonical_five_schema_loads_from_yaml() -> None:
    cfg = load_canonical_routes(CANONICAL_PATH)

    assert cfg["schema_version"] == "canonical_routes.v1"
    assert cfg["name"] == "town01_canonical_five"
    assert cfg["map"] == "Town01"
    assert route_ids(cfg) == ["lane097", "lane217", "junction031", "curve217", "curve213"]


def test_route_id_and_stable_id_are_unique() -> None:
    cfg = load_canonical_routes(CANONICAL_PATH)
    routes = cfg["routes"]

    assert len({route["route_id"] for route in routes}) == len(routes)
    assert len({route["stable_id"] for route in routes}) == len(routes)


def test_097_217_031_are_hard_gates() -> None:
    cfg = load_canonical_routes(CANONICAL_PATH)
    hard = {route["stable_id"]: route for route in list_hard_gates(cfg)}

    assert hard["town01_rh_spawn097_goal046"]["route_id"] == "lane097"
    assert hard["town01_rh_spawn217_goal046"]["route_id"] == "lane217"
    assert hard["town01_rh_spawn031_goal056"]["route_id"] == "junction031"
    assert all(route["gate_role"] == "hard_gate" for route in hard.values())


def test_curve_pair_are_diagnostic_not_hard_gate() -> None:
    cfg = load_canonical_routes(CANONICAL_PATH)
    diagnostic = {route["stable_id"]: route for route in list_diagnostic_gates(cfg)}

    assert diagnostic["town01_rh_spawn217_goal048"]["route_id"] == "curve217"
    assert diagnostic["town01_rh_spawn213_goal059"]["route_id"] == "curve213"
    assert all(route["gate_role"] == "diagnostic_gate" for route in diagnostic.values())


def test_placeholder_route_ref_warns_but_does_not_fail() -> None:
    cfg = load_canonical_routes(CANONICAL_PATH)
    validation = validate_canonical_routes(cfg)

    assert validation.ok
    assert any("route_ref is a placeholder" in warning for warning in validation.warnings)
    assert cfg["_validation"]["warnings"]


def test_required_artifacts_include_route_health_and_timeseries() -> None:
    cfg = load_canonical_routes(CANONICAL_PATH)

    for route in cfg["routes"]:
        artifacts = {Path(str(item)).name for item in route["required_artifacts"]}
        assert {"summary.json", "manifest.json", "route_health.json"}.issubset(artifacts)
        assert {"timeseries.csv", "timeseries.jsonl"}.intersection(artifacts)


def test_curve_diagnostics_subset_loads_without_hard_gate_requirement() -> None:
    cfg = load_canonical_routes(CURVE_DIAGNOSTICS_PATH)

    assert route_ids(cfg) == ["curve217", "curve213"]
    assert list_hard_gates(cfg) == []
    assert [route["route_id"] for route in list_diagnostic_gates(cfg)] == ["curve217", "curve213"]


def test_random_regression_pool_loads_as_informational_routes() -> None:
    cfg = load_canonical_routes(RANDOM_POOL_PATH)

    assert cfg["name"] == "town01_random_regression_pool_20260416"
    assert cfg["sampling"]["seed"] == 20260416
    assert list_hard_gates(cfg) == []
    assert list_diagnostic_gates(cfg) == []
    assert route_ids(cfg) == [
        "random_lane_183_044",
        "random_lane_213_048",
        "random_junction_176_063",
        "random_junction_071_063",
        "random_curve_219_048",
        "random_curve_177_051",
    ]
    assert len(list_informational_routes(cfg)) == 6
