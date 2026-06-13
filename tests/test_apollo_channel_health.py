from __future__ import annotations

import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

from carla_testbed.analysis.apollo_channel_health import (
    CHANNEL_HEALTH_REPORT_SCHEMA_VERSION,
    analyze_apollo_channel_health,
    load_channel_health_config,
    load_channel_stats,
    write_apollo_channel_health_report,
)

CONFIG = Path("configs/algorithms/apollo_natural_driving_channels.yaml")
STATS = Path("tests/fixtures/apollo/channel_stats_natural_valid.json")


def load_fixture() -> tuple[dict, dict]:
    return load_channel_health_config(CONFIG), load_channel_stats(STATS)


def test_valid_red_light_channel_stats_pass() -> None:
    config, stats = load_fixture()

    report = analyze_apollo_channel_health(
        config,
        stats,
        scenario_class="traffic_light_red_stop",
    )

    assert report["schema_version"] == CHANNEL_HEALTH_REPORT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["scenario_class"] == "traffic_light_red_stop"
    assert report["channel_results"]["traffic_light"]["required"] is True
    assert report["missing_required_channels"] == []


def test_lane_keep_missing_traffic_light_warns_only() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"].pop("/apollo/perception/traffic_light")

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    assert report["status"] == "warn"
    assert "traffic_light" in report["missing_optional_channels"]
    assert "traffic_light" not in report["missing_required_channels"]
    assert report["channel_results"]["traffic_light"]["issues"] == ["missing_channel"]


def test_lane_keep_zero_traffic_light_messages_warns_only() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/perception/traffic_light"].update(
        {
            "message_count": 0,
            "hz": 0.0,
            "max_gap_ms": None,
            "timestamp_monotonic": None,
            "sequence_monotonic": None,
            "stale_count": 0,
            "derived_from_bridge_counters": True,
            "max_gap_ms_estimated": True,
        }
    )

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    traffic_light = report["channel_results"]["traffic_light"]
    assert traffic_light["required"] is False
    assert traffic_light["status"] == "warn"
    assert traffic_light["issues"] == ["no_messages"]
    assert "traffic_light" not in report["low_rate_channels"]
    assert "traffic_light" not in report["timestamp_failures"]
    assert "traffic_light" not in report["sequence_failures"]


def test_lane_keep_missing_obstacles_warns_only_for_no_obstacle_probe() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"].pop("/apollo/perception/obstacles")

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    assert report["status"] == "warn"
    assert "obstacles" in report["missing_optional_channels"]
    assert "obstacles" not in report["missing_required_channels"]
    assert report["channel_results"]["obstacles"]["required"] is False


def test_red_light_missing_traffic_light_fails() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"].pop("/apollo/perception/traffic_light")

    report = analyze_apollo_channel_health(
        config,
        stats,
        scenario_class="traffic_light_red_stop",
    )

    assert report["status"] == "fail"
    assert "traffic_light" in report["missing_required_channels"]
    assert report["channel_results"]["traffic_light"]["required"] is True


def test_missing_planning_and_control_fail() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"].pop("/apollo/planning")
    stats["channels"].pop("/apollo/control")

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    assert report["status"] == "fail"
    assert set(report["missing_required_channels"]) >= {"planning", "control"}


def test_low_rate_and_large_gap_fail_for_required_planning() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/planning"]["hz"] = 0.5
    stats["channels"]["/apollo/planning"]["max_gap_ms"] = 2000.0

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    planning = report["channel_results"]["planning"]
    assert report["status"] == "fail"
    assert "planning" in report["low_rate_channels"]
    assert "planning" in report["gap_failures"]
    assert "low_rate" in planning["issues"]
    assert "message_gap_too_large" in planning["issues"]


def test_planning_large_header_gap_with_small_sim_gap_is_diagnosed_as_warning() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/planning"].update(
        {
            "max_gap_ms": 2000.0,
            "sim_time_max_gap_ms": 50.0,
            "primary_time_axis": "planning_header_timestamp_sec",
        }
    )

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    planning = report["channel_results"]["planning"]
    assert report["status"] == "warn"
    assert "planning" not in report["gap_failures"]
    assert "message_gap_too_large" not in planning["issues"]
    assert "message_gap_time_axis_warn" in planning["issues"]
    assert planning["time_axis_diagnosis"] == "header_or_wall_time_gap_large_sim_time_gap_ok"
    assert "planning_header_or_wall_gap_large_but_sim_time_gap_within_limit" in planning["warnings"]
    assert planning["sim_time_max_gap_ms"] == 50.0


def test_planning_half_second_gap_warns_even_below_fail_threshold() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/planning"].update(
        {
            "max_gap_ms": 530.0,
            "sim_time_max_gap_ms": 850.0,
            "gap_count_over_250ms": 110,
            "sim_time_gap_count_over_250ms": 90,
        }
    )

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    planning = report["channel_results"]["planning"]
    assert report["status"] == "warn"
    assert "planning" in report["planning_gap_warning_channels"]
    assert "planning_gap_warn" in planning["issues"]
    assert "planning_header_or_wall_gap_over_500ms" in planning["warnings"]
    assert "planning_sim_time_gap_over_500ms" in planning["warnings"]


def test_timestamp_and_sequence_non_monotonic_fail() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/localization/pose"]["timestamp_monotonic"] = False
    stats["channels"]["/apollo/control"]["sequence_monotonic"] = False

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    assert report["status"] == "fail"
    assert "localization" in report["timestamp_failures"]
    assert "control" in report["sequence_failures"]


def test_stale_count_warns_without_claiming_behavior_success() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/control"]["stale_count"] = 2

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    assert report["status"] == "warn"
    assert "control" in report["stale_channels"]
    assert "does not prove Apollo behavior correctness" in report["interpretation_boundary"]


def test_writer_and_cli_create_report(tmp_path: Path) -> None:
    config, stats = load_fixture()
    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")
    outputs = write_apollo_channel_health_report(report, tmp_path / "direct_writer")
    writer_report = json.loads(Path(outputs["apollo_channel_health_report"]).read_text(encoding="utf-8"))

    assert writer_report["status"] == "pass"

    cli_out = tmp_path / "cli"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_channel_health.py",
            "--config",
            str(CONFIG),
            "--stats",
            str(STATS),
            "--scenario-class",
            "traffic_light_red_stop",
            "--out",
            str(cli_out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = json.loads(result.stdout)
    cli_report = json.loads((cli_out / "apollo_channel_health_report.json").read_text(encoding="utf-8"))

    assert stdout["status"] == "pass"
    assert cli_report["scenario_class"] == "traffic_light_red_stop"


def test_channel_result_preserves_evidence_source() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/control"]["source"] = "control_decode_debug.jsonl"
    stats["channels"]["/apollo/control"]["evidence_source"] = "row_level_artifact"
    stats["channels"]["/apollo/control"]["derived_from_bridge_counters"] = False

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    control = report["channel_results"]["control"]
    assert control["source"] == "control_decode_debug.jsonl"
    assert control["evidence_source"] == "row_level_artifact"
    assert control["derived_from_bridge_counters"] is False


def test_channel_result_preserves_topic_publish_claim_grade_fields() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/localization/pose"].update(
        {
            "source": "topic_publish_stats.jsonl",
            "evidence_source": "topic_publish_stats",
            "promotion_grade_evidence": True,
            "fresh_message_count": 20,
            "fresh_world_frame_hz": 20.0,
            "delivery_wall_hz": 19.8,
            "header_sim_hz": 20.0,
            "duplicate_timestamp_count": 0,
            "derived_from_bridge_counters": False,
        }
    )

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")

    loc = report["channel_results"]["localization"]
    assert loc["source"] == "topic_publish_stats.jsonl"
    assert loc["evidence_source"] == "topic_publish_stats"
    assert loc["promotion_grade_evidence"] is True
    assert loc["fresh_world_frame_hz"] == 20.0
    assert loc["duplicate_timestamp_count"] == 0


def test_channel_result_preserves_gap_location_fields() -> None:
    config, stats = load_fixture()
    stats = deepcopy(stats)
    stats["channels"]["/apollo/control"].update(
        {
            "max_gap_start_timestamp": 10.0,
            "max_gap_end_timestamp": 11.2,
            "max_gap_index": 42,
            "gap_p95_ms": 120.0,
            "gap_count_over_250ms": 1,
            "gap_count_over_1000ms": 1,
        }
    )

    report = analyze_apollo_channel_health(config, stats, scenario_class="lane_keep")
    control = report["channel_results"]["control"]

    assert control["max_gap_start_timestamp"] == 10.0
    assert control["max_gap_end_timestamp"] == 11.2
    assert control["max_gap_index"] == 42
    assert control["gap_count_over_1000ms"] == 1
