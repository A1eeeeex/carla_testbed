from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from carla_testbed.experiments.natural_driving_schema import (
    NaturalDrivingSuiteError,
    list_scenarios,
    list_scenarios_by_class,
    load_natural_driving_suite,
    validate_natural_driving_suite,
)

SUITE_PATH = Path("configs/scenarios/town01_natural_driving_suite.yaml")


def test_town01_natural_driving_suite_loads() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)

    assert suite["schema_version"] == "natural_driving_suite.v1"
    assert suite["map"] == "Town01"
    assert suite["_validation"]["ok"] is True


def test_required_gate_layers_exist() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)

    assert set(suite["gates"]) == {
        "link_health",
        "geometry_health",
        "behavior_health",
        "control_health",
    }


def test_required_route_placeholders_exist() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    route_ids = {scenario["route_id"] for scenario in list_scenarios(suite)}

    assert {"lane097", "lane217", "junction031"}.issubset(route_ids)


def test_required_traffic_light_classes_have_placeholders() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)

    assert list_scenarios_by_class(suite, "traffic_light_red_stop")
    assert list_scenarios_by_class(suite, "traffic_light_green_go")
    assert list_scenarios_by_class(suite, "traffic_light_red_to_green_release")


def test_traffic_light_scenarios_have_runnable_probe_routes() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)

    for scenario_class in (
        "traffic_light_red_stop",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    ):
        scenario = list_scenarios_by_class(suite, scenario_class)[0]
        assert scenario["gate_role"] == "informational"
        assert str(scenario["route_ref"]).startswith("town01_rh_")
        assert not str(scenario["route_ref"]).startswith("placeholder:")


def test_each_scenario_has_contract_fields_and_artifacts() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)

    for scenario in list_scenarios(suite):
        assert scenario["scenario_id"]
        assert scenario["route_id"]
        assert scenario["scenario_class"]
        assert scenario["map"] == "Town01"
        assert scenario["duration_s"] > 0
        assert "backend_role" not in scenario
        assert scenario["gate_role"] in {"hard_gate", "diagnostic_gate", "informational"}
        assert scenario["route_ref"]
        assert scenario.get("spawn_ref") or scenario.get("spawn_pose")
        assert scenario.get("goal_ref") or scenario.get("goal_pose")
        assert scenario["required_channels"]
        assert {"link_health", "geometry_health", "behavior_health", "control_health"}.issubset(
            scenario["success_criteria"]
        )
        artifact_names = {Path(item).name for item in scenario["required_artifacts"]}
        assert "summary.json" in artifact_names
        assert "manifest.json" in artifact_names
        assert {"config.resolved.yaml", "effective_config.yaml"} & artifact_names
        assert "events.jsonl" in artifact_names
        assert {"timeseries.csv", "timeseries.jsonl"} & artifact_names
        assert "route_health.json" in artifact_names
        assert "route_health.csv" in artifact_names
        assert "curve_segments.csv" in artifact_names
        assert "route_health_summary.md" in artifact_names
        assert "apollo_channel_health_report.json" in artifact_names
        assert "control_health_report.json" in artifact_names
        assert "failure_timeline_report.json" in artifact_names
        assert "route_start_alignment_report.json" in artifact_names
        assert "artifact_completeness_report.json" in artifact_names
        if scenario["scenario_class"].startswith("traffic_light"):
            assert "/apollo/perception/traffic_light" in scenario["required_channels"]
            assert scenario["traffic_light_expectation"]["expected_behavior"]
            assert scenario["traffic_light_expectation"]["stimulus_mode"] == "deterministic_gt_control"
            assert scenario["traffic_light_expectation"]["claim_grade"] is True
            assert scenario["traffic_light_expectation"]["required_report_fields"]
            assert "traffic_light_contract_report.json" in artifact_names
            assert "traffic_light_behavior_report.json" in artifact_names
            assert "analysis/traffic_light/traffic_light_behavior_report.json" in scenario[
                "required_artifacts"
            ]


def test_gate_roles_match_current_promotion_boundary() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    by_route = {scenario["route_id"]: scenario for scenario in list_scenarios(suite)}

    assert by_route["lane097"]["gate_role"] == "hard_gate"
    assert by_route["lane217"]["gate_role"] == "hard_gate"
    assert by_route["junction031"]["gate_role"] == "hard_gate"
    assert by_route["curve217"]["gate_role"] == "diagnostic_gate"
    assert by_route["curve213"]["gate_role"] == "diagnostic_gate"
    assert by_route["town01_rh_spawn129_goal051"]["gate_role"] == "informational"


def test_truth_input_suite_must_not_claim_perception_or_localization_reproduction() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    mutated["mode"]["apollo_perception_reproduced"] = True
    mutated["mode"]["apollo_localization_reproduced"] = True

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("perception reproduction" in error for error in validation.errors)
    assert any("localization reproduction" in error for error in validation.errors)


def test_suite_mode_records_reproduction_identity() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mode = suite["mode"]

    assert mode["truth_input"] is True
    assert mode["algorithm_variant_id"] == "apollo_10_0_carla_gt_town01_reference"
    assert mode["algorithm_variant_manifest_path"] == "configs/algorithms/apollo_variant.carla_gt.example.yaml"
    assert mode["transport_mode"] == "ros2_gt"
    assert mode["backend"] == "apollo_cyberrt"


def test_missing_suite_mode_reproduction_identity_fails_validation() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    mutated["mode"].pop("algorithm_variant_id")
    mutated["mode"].pop("algorithm_variant_manifest_path")
    mutated["mode"].pop("transport_mode")

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("algorithm_variant_id" in error for error in validation.errors)
    assert any("algorithm_variant_manifest_path" in error for error in validation.errors)
    assert any("transport_mode" in error for error in validation.errors)


def test_carla_direct_cannot_be_default_backend() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    mutated["mode"]["default_backend"] = "carla_direct"

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("carla_direct" in error for error in validation.errors)


def test_missing_route_reference_fields_fail_validation() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    scenario = mutated["scenarios"][0]
    scenario.pop("route_ref")
    scenario.pop("spawn_ref")
    scenario.pop("goal_ref")

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("missing fields route_ref" in error for error in validation.errors)
    assert any("missing spawn_pose or spawn_ref" in error for error in validation.errors)
    assert any("missing goal_pose or goal_ref" in error for error in validation.errors)


def test_backend_role_is_rejected_in_favor_of_gate_role() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    mutated["scenarios"][0]["backend_role"] = "hard_gate"

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("backend_role is deprecated" in error for error in validation.errors)


def test_missing_required_diagnostic_artifacts_fail_validation() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    scenario = mutated["scenarios"][0]
    scenario["required_artifacts"] = [
        "summary.json",
        "manifest.json",
        "timeseries.csv",
        "analysis/route_health/route_health.json",
    ]

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("events.jsonl" in error for error in validation.errors)
    assert any("config.resolved.yaml or effective_config.yaml" in error for error in validation.errors)
    assert any("route_health.csv" in error for error in validation.errors)
    assert any("curve_segments.csv" in error for error in validation.errors)
    assert any("apollo_channel_health_report.json" in error for error in validation.errors)
    assert any("control_health_report.json" in error for error in validation.errors)
    assert any("failure_timeline_report.json" in error for error in validation.errors)
    assert any("route_start_alignment_report.json" in error for error in validation.errors)
    assert any("artifact_completeness_report.json" in error for error in validation.errors)


def test_traffic_light_contract_artifact_is_required_for_traffic_scenarios() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    traffic = next(
        scenario
        for scenario in mutated["scenarios"]
        if scenario["scenario_class"] == "traffic_light_red_stop"
    )
    traffic["required_artifacts"] = [
        item
        for item in traffic["required_artifacts"]
        if Path(item).name != "traffic_light_contract_report.json"
    ]

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("traffic_light_contract_report.json" in error for error in validation.errors)


def test_traffic_light_behavior_artifact_is_required_for_traffic_scenarios() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    traffic = next(
        scenario
        for scenario in mutated["scenarios"]
        if scenario["scenario_class"] == "traffic_light_red_stop"
    )
    traffic["required_artifacts"] = [
        item
        for item in traffic["required_artifacts"]
        if Path(item).name != "traffic_light_behavior_report.json"
    ]

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("traffic_light_behavior_report.json" in error for error in validation.errors)


def test_traffic_light_expectation_is_required_for_traffic_scenarios() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    traffic = next(
        scenario
        for scenario in mutated["scenarios"]
        if scenario["scenario_class"] == "traffic_light_red_stop"
    )
    traffic.pop("traffic_light_expectation")

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("traffic_light_expectation" in error for error in validation.errors)


def test_traffic_light_expectation_must_match_scenario_class() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    traffic = next(
        scenario
        for scenario in mutated["scenarios"]
        if scenario["scenario_class"] == "traffic_light_green_go"
    )
    traffic["traffic_light_expectation"]["expected_behavior"] = "red_stop"

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("expected_behavior must be green_go" in error for error in validation.errors)


def test_traffic_light_expectation_requires_supported_stimulus_mode() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    traffic = next(
        scenario
        for scenario in mutated["scenarios"]
        if scenario["scenario_class"] == "traffic_light_red_stop"
    )
    traffic["traffic_light_expectation"]["stimulus_mode"] = "ambient_magic"

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("stimulus_mode" in error for error in validation.errors)


def test_non_claim_grade_traffic_light_stimulus_must_stay_informational() -> None:
    suite = load_natural_driving_suite(SUITE_PATH)
    mutated = deepcopy(suite)
    mutated.pop("_validation", None)
    mutated.pop("_source_path", None)
    traffic = next(
        scenario
        for scenario in mutated["scenarios"]
        if scenario["scenario_class"] == "traffic_light_red_stop"
    )
    traffic["traffic_light_expectation"]["stimulus_mode"] = "carla_actual_observed"
    traffic["traffic_light_expectation"]["claim_grade"] = False
    traffic["gate_role"] = "hard_gate"

    validation = validate_natural_driving_suite(mutated)

    assert not validation.ok
    assert any("non-claim-grade traffic-light stimulus" in error for error in validation.errors)


def test_loader_raises_on_invalid_suite(tmp_path: Path) -> None:
    path = tmp_path / "bad_suite.yaml"
    path.write_text("schema_version: wrong\n", encoding="utf-8")

    with pytest.raises(NaturalDrivingSuiteError):
        load_natural_driving_suite(path)
