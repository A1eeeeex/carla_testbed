from __future__ import annotations

import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

from carla_testbed.apollo.town01_contract import (
    CONTRACT_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
    check_town01_apollo_contract,
    load_town01_apollo_contract,
)

FIXTURE = Path("configs/town01/apollo_contract.example.yaml")


def test_example_contract_loads_and_warns_for_placeholders() -> None:
    contract = load_town01_apollo_contract(FIXTURE)

    report = check_town01_apollo_contract(contract)

    assert contract["schema_version"] == CONTRACT_SCHEMA_VERSION
    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["status"] == "warn"
    assert {route["route_id"] for route in report["route_results"]} >= {
        "lane097",
        "lane217",
        "junction031",
        "traffic_light_red_stop_placeholder",
    }
    assert report["signal_results"]
    assert not report["errors"]
    assert "apollo_map_root_env_placeholder_unverified" in report["warnings"]
    assert report["signal_results"][0]["traffic_light_mappable"] is False
    assert "carla_traffic_light_id_placeholder" in report["signal_results"][0]["warnings"]


def test_duplicate_route_id_fails() -> None:
    contract = load_town01_apollo_contract(FIXTURE)
    contract["routes"].append(deepcopy(contract["routes"][0]))

    report = check_town01_apollo_contract(contract)

    assert report["status"] == "fail"
    assert any("duplicate route_id: lane097" == error for error in report["errors"])


def test_spawn_too_far_from_route_fails() -> None:
    contract = load_town01_apollo_contract(FIXTURE)
    contract["routes"][0]["spawn_pose"]["x"] = 1000.0

    report = check_town01_apollo_contract(contract)
    lane097 = next(route for route in report["route_results"] if route["route_id"] == "lane097")

    assert report["status"] == "fail"
    assert "spawn_too_far_from_route" in lane097["issues"]


def test_spawn_heading_misaligned_fails() -> None:
    contract = load_town01_apollo_contract(FIXTURE)
    contract["routes"][0]["spawn_pose"]["heading"] = 3.141592653589793

    report = check_town01_apollo_contract(contract)
    lane097 = next(route for route in report["route_results"] if route["route_id"] == "lane097")

    assert report["status"] == "fail"
    assert "spawn_heading_misaligned" in lane097["issues"]


def test_missing_signal_stop_line_fails_without_crashing() -> None:
    contract = load_town01_apollo_contract(FIXTURE)
    del contract["signals"][0]["stop_line_id"]

    report = check_town01_apollo_contract(contract)
    signal = report["signal_results"][0]

    assert report["status"] == "fail"
    assert "missing_stop_line_id" in signal["issues"]


def test_missing_signal_mapping_fails_without_faking_id() -> None:
    contract = load_town01_apollo_contract(FIXTURE)
    contract["signals"][0].pop("carla_landmark_id")
    contract["signals"][0].pop("carla_actor_id", None)

    report = check_town01_apollo_contract(contract)
    signal = report["signal_results"][0]

    assert report["status"] == "fail"
    assert signal["traffic_light_mappable"] is False
    assert "missing_carla_actor_or_landmark_id" in signal["issues"]


def test_missing_signals_warns_because_real_map_parser_is_not_required() -> None:
    contract = load_town01_apollo_contract(FIXTURE)
    contract.pop("signals")

    report = check_town01_apollo_contract(contract)

    assert report["status"] == "warn"
    assert "signals_missing_or_not_list" in report["warnings"]
    assert "traffic_light_signal_contract_unverified" in report["warnings"]


def test_cli_writes_contract_report(tmp_path: Path) -> None:
    out_dir = tmp_path / "town01_contract"
    result = subprocess.run(
        [
            sys.executable,
            "tools/check_town01_apollo_contract.py",
            "--config",
            str(FIXTURE),
            "--out",
            str(out_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = json.loads(result.stdout)
    report_path = out_dir / "town01_apollo_contract_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert stdout["status"] == "warn"
    assert stdout["report"] == str(report_path)
    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["status"] == "warn"
