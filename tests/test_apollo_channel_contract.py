from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from carla_testbed.algorithms.channel_contract import (
    check_channel_stats,
    check_channel_stats_file,
    generate_adapter_contract_report,
    load_channel_contract,
    load_channel_stats,
)

CONTRACT_PATH = Path("configs/algorithms/apollo_channel_contract.yaml")
VALID_STATS_PATH = Path("tests/fixtures/apollo/channel_stats_valid.json")
MISSING_CONTROL_PATH = Path("tests/fixtures/apollo/channel_stats_missing_control.json")


def test_valid_channel_stats_pass() -> None:
    contract = load_channel_contract(CONTRACT_PATH)
    result = check_channel_stats_file(contract, VALID_STATS_PATH)

    assert result.status == "pass"
    assert result.report["schema_version"] == "adapter_contract_report.v1"
    assert result.report["missing_required_channels"] == []


def test_missing_control_fails() -> None:
    contract = load_channel_contract(CONTRACT_PATH)
    result = check_channel_stats_file(contract, MISSING_CONTROL_PATH)

    assert result.status == "fail"
    assert "control" in result.report["missing_required_channels"]
    assert result.report["channel_results"]["control"]["issues"] == ["missing_channel"]


def test_low_planning_rate_fails_for_required_channel() -> None:
    contract = load_channel_contract(CONTRACT_PATH)
    stats = load_channel_stats(VALID_STATS_PATH)
    stats["channels"]["/apollo/planning"]["hz"] = 0.5

    report = generate_adapter_contract_report(contract, stats)

    assert report["status"] == "fail"
    assert "planning" in report["low_rate_channels"]
    assert "low_rate" in report["channel_results"]["planning"]["issues"]


def test_timestamp_non_monotonic_fails() -> None:
    contract = load_channel_contract(CONTRACT_PATH)
    stats = load_channel_stats(VALID_STATS_PATH)
    stats["channels"]["/apollo/localization/pose"]["timestamp_monotonic"] = False

    result = check_channel_stats(contract, stats)

    assert result.status == "fail"
    assert "localization" in result.report["timestamp_failures"]
    assert "timestamp_non_monotonic" in result.report["channel_results"]["localization"]["issues"]


def test_optional_perception_missing_warns() -> None:
    contract = load_channel_contract(CONTRACT_PATH)
    stats = load_channel_stats(VALID_STATS_PATH)
    stats = deepcopy(stats)
    stats["channels"].pop("/apollo/perception/obstacles")

    result = check_channel_stats(contract, stats)

    assert result.status == "warn"
    assert "perception_obstacles" not in result.report["missing_required_channels"]
    assert result.report["channel_results"]["perception_obstacles"]["issues"] == ["missing_channel"]


def test_stale_channel_warns() -> None:
    contract = load_channel_contract(CONTRACT_PATH)
    stats = load_channel_stats(VALID_STATS_PATH)
    stats["channels"]["/apollo/control"]["stale_count"] = 3
    stats["channels"]["/apollo/control"]["max_gap_ms"] = 1500.0

    result = check_channel_stats(contract, stats)

    assert result.status == "warn"
    assert "control" in result.report["stale_channels"]
