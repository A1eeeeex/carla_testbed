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
    assert report["route_contract_level"] == "placeholder"
    assert report["signal_contract_level"] == "placeholder"
    assert report["overall_contract_level"] == "placeholder"
    assert report["can_claim_lane_keep"] is False
    assert report["can_claim_junction"] is False
    assert report["can_claim_traffic_light"] is False
    assert "signal_overlap_report" in report["missing_evidence"]
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
    assert report["can_claim_traffic_light"] is False


def test_real_looking_signal_stays_schema_only_without_hdmap_parser(tmp_path: Path) -> None:
    contract = _realistic_contract(tmp_path)

    report = check_town01_apollo_contract(contract)

    assert report["status"] == "pass"
    assert report["route_contract_level"] == "route_geometry_available"
    assert report["spawn_contract_level"] == "route_geometry_available"
    assert report["signal_contract_level"] == "schema_only"
    assert report["hdmap_contract_level"] == "schema_only"
    assert report["can_claim_lane_keep"] is True
    assert report["can_claim_traffic_light"] is False


def test_missing_spawn_blocks_lane_keep_claim(tmp_path: Path) -> None:
    contract = _realistic_contract(tmp_path)
    contract["routes"][0].pop("spawn_pose")
    contract["routes"][0].pop("spawn_ref", None)

    report = check_town01_apollo_contract(contract)

    assert report["status"] == "fail"
    assert report["spawn_contract_level"] == "missing"
    assert report["can_claim_lane_keep"] is False


def test_hdmap_validation_artifact_sets_hdmap_verified(tmp_path: Path) -> None:
    artifact = tmp_path / "hdmap_validation_report.json"
    artifact.write_text(json.dumps({"status": "pass", "hdmap_verified": True}), encoding="utf-8")
    contract = _realistic_contract(tmp_path, verification_artifacts={"hdmap_validation_report": str(artifact)})

    report = check_town01_apollo_contract(contract)

    assert report["hdmap_contract_level"] == "hdmap_verified"
    assert report["verification_artifacts"]["hdmap_validation_report"]["exists"] is True


def test_signal_overlap_artifact_sets_signal_overlap_verified(tmp_path: Path) -> None:
    artifact = tmp_path / "signal_overlap_report.json"
    artifact.write_text(
        json.dumps({"status": "pass", "signal_overlap_verified": True}),
        encoding="utf-8",
    )
    contract = _realistic_contract(tmp_path, verification_artifacts={"signal_overlap_report": str(artifact)})

    report = check_town01_apollo_contract(contract)

    assert report["signal_contract_level"] == "signal_overlap_verified"
    assert report["can_claim_traffic_light"] is True


def test_placeholder_signal_does_not_upgrade_to_verified_with_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "signal_overlap_report.json"
    artifact.write_text(json.dumps({"status": "pass", "signal_overlap_verified": True}), encoding="utf-8")
    contract = load_town01_apollo_contract(FIXTURE)
    contract["verification_artifacts"] = {"signal_overlap_report": str(artifact)}

    report = check_town01_apollo_contract(contract)

    assert report["signal_contract_level"] == "placeholder"
    assert report["can_claim_traffic_light"] is False


def test_roadrunner_conversion_metadata_enters_report(tmp_path: Path) -> None:
    artifact = tmp_path / "roadrunner_conversion_report.json"
    payload = {
        "schema_version": "roadrunner_conversion_metadata.v1",
        "source_map_path": "/tmp/baguang/source.xml",
        "source_map_hash": "abc",
        "generated_base_map_path": "/tmp/baguang/base_map.xml",
        "generated_base_map_hash": "def",
        "scale": {"x_scale": 1.0, "y_scale": 1.0},
        "lane_count": 2,
        "signal_count": 1,
        "stop_line_count": 1,
        "warnings": [],
    }
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    contract = _realistic_contract(
        tmp_path,
        map_name="straight_road_for_baguang",
        verification_artifacts={"roadrunner_conversion_report": str(artifact)},
    )

    report = check_town01_apollo_contract(contract)

    assert report["map_name"] == "straight_road_for_baguang"
    assert report["map_source"] == "roadrunner_conversion"
    assert report["roadrunner_conversion_metadata"]["lane_count"] == 2


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


def _realistic_contract(
    tmp_path: Path,
    *,
    map_name: str = "Town01",
    verification_artifacts: dict[str, str] | None = None,
) -> dict[str, object]:
    map_root = tmp_path / "apollo_map"
    map_root.mkdir()
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "map_name": map_name,
        "apollo_map_root": str(map_root),
        "verification_artifacts": verification_artifacts or {},
        "routes": [
            {
                "route_id": "lane097",
                "route_ref": "inline:lane097",
                "route_definition_hash": "route-hash-1",
                "spawn_pose": {"x": 0.0, "y": 0.0, "z": 0.0, "heading": 0.0},
                "goal_pose": {"x": 30.0, "y": 0.0, "z": 0.0, "heading": 0.0},
                "route_points": [
                    {"x": 0.0, "y": 0.0, "z": 0.0, "heading": 0.0, "lane_id": "lane_097"},
                    {"x": 30.0, "y": 0.0, "z": 0.0, "heading": 0.0, "lane_id": "lane_097"},
                ],
            }
        ],
        "signals": [
            {
                "logical_id": "town01_signal_001",
                "apollo_signal_id": "signal_001",
                "stop_line_id": "stop_line_001",
                "carla_landmark_id": "landmark_001",
                "lane_ids": ["lane_097"],
            }
        ],
    }
