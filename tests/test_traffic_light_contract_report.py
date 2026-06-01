from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from carla_testbed.adapters.apollo.traffic_light_gt import load_traffic_light_mappings
from carla_testbed.analysis.traffic_light_contract import (
    TRAFFIC_LIGHT_CONTRACT_REPORT_SCHEMA_VERSION,
    build_traffic_light_contract_report,
    build_traffic_light_contract_report_files,
    write_traffic_light_contract_report,
)
from carla_testbed.apollo.town01_contract import load_town01_apollo_contract

TOWN01_CONTRACT = Path("configs/town01/apollo_contract.example.yaml")
TRAFFIC_LIGHT_MAPPING = Path("configs/town01/traffic_lights.example.yaml")


def test_default_fixture_generates_warn_contract_report_for_placeholders() -> None:
    report = build_traffic_light_contract_report_files(
        town01_contract_path=TOWN01_CONTRACT,
        traffic_light_mapping_path=TRAFFIC_LIGHT_MAPPING,
        scenario_class="traffic_light_red_stop",
    )

    assert report["schema_version"] == TRAFFIC_LIGHT_CONTRACT_REPORT_SCHEMA_VERSION
    assert report["status"] == "warn"
    assert report["scenario_class"] == "traffic_light_red_stop"
    assert report["mapping_results"]
    assert {item["logical_id"] for item in report["mapping_results"]} == {
        "town01_tl_red_stop_placeholder"
    }
    assert "town01_apollo_contract_warn" in report["warnings"]
    assert any("placeholder_unverified" in warning for warning in report["warnings"])


def test_mismatched_apollo_signal_id_fails() -> None:
    contract = load_town01_apollo_contract(TOWN01_CONTRACT)
    mapping = load_traffic_light_mappings(TRAFFIC_LIGHT_MAPPING)
    mutated = deepcopy(mapping)
    mutated["traffic_lights"][0]["apollo_signal_id"] = "apollo_signal_mismatch"

    report = build_traffic_light_contract_report(
        contract,
        mutated,
        scenario_class="traffic_light_red_stop",
    )

    assert report["status"] == "fail"
    assert "apollo_signal_id_mismatch" in report["errors"]


def test_missing_scenario_mapping_fails_without_fake_unknown_publish() -> None:
    contract = load_town01_apollo_contract(TOWN01_CONTRACT)
    mapping = load_traffic_light_mappings(TRAFFIC_LIGHT_MAPPING)
    mutated = deepcopy(mapping)
    mutated["traffic_lights"] = []

    report = build_traffic_light_contract_report(
        contract,
        mutated,
        scenario_class="traffic_light_red_stop",
    )

    assert report["status"] == "fail"
    assert "no_traffic_light_mapping_for_scenario" in report["errors"]


def test_write_traffic_light_contract_report(tmp_path: Path) -> None:
    report = build_traffic_light_contract_report_files(
        town01_contract_path=TOWN01_CONTRACT,
        traffic_light_mapping_path=TRAFFIC_LIGHT_MAPPING,
        scenario_class="traffic_light_red_stop",
    )

    outputs = write_traffic_light_contract_report(report, tmp_path)
    report_path = Path(outputs["traffic_light_contract_report"])
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert report_path.name == "traffic_light_contract_report.json"
    assert payload["schema_version"] == TRAFFIC_LIGHT_CONTRACT_REPORT_SCHEMA_VERSION
