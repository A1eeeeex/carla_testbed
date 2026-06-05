from __future__ import annotations

import json
from pathlib import Path

import pytest

from carla_testbed.analysis.apollo_channel_health import (
    analyze_apollo_channel_health_files,
)
from carla_testbed.analysis.channel_stats_normalizer import (
    CHANNEL_STATS_SCHEMA_VERSION,
    bridge_stats_to_channel_stats,
    normalize_channel_stats_for_run,
)

CHANNEL_CONFIG = Path("configs/algorithms/apollo_natural_driving_channels.yaml")


def _bridge_stats() -> dict:
    return {
        "loc_count": 600,
        "chassis_count": 600,
        "obstacles_count": 300,
        "control_rx_count": 300,
        "routing_response_count": 2,
        "planning": {"msg_count": 300},
        "traffic_light": {
            "channel": "/apollo/perception/traffic_light",
            "publish_count": 150,
        },
        "timing": {"sim_time_sec": 30.0},
    }


def test_bridge_stats_to_channel_stats_generates_expected_channels() -> None:
    stats = bridge_stats_to_channel_stats(_bridge_stats(), source_path="artifacts/cyber_bridge_stats.json")

    assert stats["schema_version"] == CHANNEL_STATS_SCHEMA_VERSION
    assert stats["source"]["type"] == "derived_from_bridge_counters"
    channels = stats["channels"]
    assert channels["/apollo/localization/pose"]["message_count"] == 600
    assert channels["/apollo/localization/pose"]["hz"] == 20.0
    assert channels["/apollo/planning"]["message_count"] == 300
    assert channels["/apollo/control"]["message_count"] == 300
    assert channels["/apollo/perception/traffic_light"]["message_count"] == 150
    assert channels["/apollo/control"]["derived_from_bridge_counters"] is True


def test_channel_health_marks_derived_stats_as_warn(tmp_path: Path) -> None:
    stats_path = tmp_path / "channel_stats.json"
    stats_path.write_text(json.dumps(bridge_stats_to_channel_stats(_bridge_stats())), encoding="utf-8")

    report = analyze_apollo_channel_health_files(
        CHANNEL_CONFIG,
        stats_path,
        scenario_class="traffic_light_red_stop",
    )

    assert report["status"] == "warn"
    assert "control_stats_derived_from_bridge_counters_not_promotion_grade" in report["warnings"]
    assert report["missing_required_channels"] == []


def test_normalize_channel_stats_for_run_writes_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(_bridge_stats()),
        encoding="utf-8",
    )

    stats = normalize_channel_stats_for_run(run_dir)

    assert stats is not None
    assert (run_dir / "channel_stats.json").is_file()
    assert stats["_output_path"] == str(run_dir / "channel_stats.json")


def test_normalize_channel_stats_prefers_timeseries_span_over_absolute_sim_stamp(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    bridge_stats = _bridge_stats()
    bridge_stats["loc_count"] = 200
    bridge_stats["chassis_count"] = 200
    bridge_stats["timing"] = {"sim_time_sec": 123.0}
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(bridge_stats),
        encoding="utf-8",
    )
    (run_dir / "timeseries.csv").write_text(
        "ts_sec,localization_timestamp\n1000.0,1000.0\n1020.0,1020.0\n",
        encoding="utf-8",
    )

    stats = normalize_channel_stats_for_run(run_dir)

    assert stats is not None
    assert stats["source"]["duration_s"] == 20.0
    assert stats["source"]["duration_source"] == "timeseries_span"
    assert stats["channels"]["/apollo/localization/pose"]["hz"] == 0.1
    assert stats["channels"]["/apollo/localization/pose"]["source"] == "timeseries.csv.localization_timestamp"


def test_normalize_channel_stats_prefers_row_level_artifacts_when_available(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(_bridge_stats()),
        encoding="utf-8",
    )
    (run_dir / "timeseries.csv").write_text(
        "\n".join(
            [
                "localization_timestamp,chassis_timestamp",
                "1000.0,1000.0",
                "1000.1,1000.1",
                "1000.3,1000.3",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "planning_topic_debug.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "planning_header_timestamp_sec": 1000.0,
                        "planning_header_sequence_num": 1,
                        "sim_time_sec": 10.0,
                    }
                ),
                json.dumps(
                    {
                        "planning_header_timestamp_sec": 1000.5,
                        "planning_header_sequence_num": 2,
                        "sim_time_sec": 10.05,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "control_decode_debug.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "parsed_control": {
                            "control_timestamp": 1000.0,
                            "control_header_sequence_num": 1,
                        }
                    }
                ),
                json.dumps(
                    {
                        "parsed_control": {
                            "control_timestamp": 1000.05,
                            "control_header_sequence_num": 2,
                        }
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    stats = normalize_channel_stats_for_run(run_dir)

    assert stats is not None
    assert stats["source"]["type"] == "mixed_bridge_and_row_level_artifacts"
    assert stats["source"]["base_type"] == "derived_from_bridge_counters"
    channels = stats["channels"]
    assert channels["/apollo/localization/pose"]["source"] == "timeseries.csv.localization_timestamp"
    assert channels["/apollo/localization/pose"]["derived_from_bridge_counters"] is False
    assert channels["/apollo/localization/pose"]["max_gap_ms"] == pytest.approx(200.0)
    assert channels["/apollo/localization/pose"]["max_gap_start_timestamp"] == 1000.1
    assert channels["/apollo/localization/pose"]["max_gap_end_timestamp"] == 1000.3
    assert channels["/apollo/localization/pose"]["gap_count_over_250ms"] == 0
    assert channels["/apollo/planning"]["source"] == "planning_topic_debug.jsonl"
    assert channels["/apollo/planning"]["message_count"] == 2
    assert channels["/apollo/planning"]["primary_time_axis"] == "planning_header_timestamp_sec"
    assert channels["/apollo/planning"]["sim_time_max_gap_ms"] == pytest.approx(50.0)
    assert channels["/apollo/control"]["source"] == "control_decode_debug.jsonl"
    assert channels["/apollo/control"]["sequence_monotonic"] is True
    assert str(artifacts / "planning_topic_debug.jsonl") in stats["source"]["row_level_artifacts"]


def test_normalize_channel_stats_prefers_topic_publish_stats_for_claim_grade_rows(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    bridge_stats = _bridge_stats()
    bridge_stats["loc_count"] = 999
    bridge_stats["chassis_count"] = 999
    bridge_stats["obstacles_count"] = 0
    (artifacts / "cyber_bridge_stats.json").write_text(json.dumps(bridge_stats), encoding="utf-8")
    rows = [
        {
            "channel": "/apollo/localization/pose",
            "wall_time_sec": 100.0,
            "sim_time_sec": 10.0,
            "header_timestamp_sec": 10.0,
            "sequence_num": 1,
            "frame_id": "map",
            "carla_world_frame": 100,
            "payload_count": 1,
            "source": "bridge_writer",
        },
        {
            "channel": "/apollo/localization/pose",
            "wall_time_sec": 100.05,
            "sim_time_sec": 10.05,
            "header_timestamp_sec": 10.05,
            "sequence_num": 2,
            "frame_id": "map",
            "carla_world_frame": 101,
            "payload_count": 1,
            "source": "bridge_writer",
        },
        {
            "channel": "/apollo/perception/obstacles",
            "wall_time_sec": 100.0,
            "sim_time_sec": 10.0,
            "header_timestamp_sec": 10.0,
            "sequence_num": 3,
            "frame_id": "map",
            "carla_world_frame": 100,
            "payload_count": 0,
            "empty_message": True,
            "source": "bridge_writer",
        },
    ]
    (artifacts / "topic_publish_stats.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    stats = normalize_channel_stats_for_run(run_dir)

    assert stats is not None
    loc = stats["channels"]["/apollo/localization/pose"]
    assert loc["source"] == "topic_publish_stats.jsonl"
    assert loc["evidence_source"] == "topic_publish_stats"
    assert loc["promotion_grade_evidence"] is True
    assert loc["message_count"] == 2
    assert loc["fresh_message_count"] == 2
    assert loc["fresh_world_frame_hz"] == pytest.approx(40.0)
    obstacles = stats["channels"]["/apollo/perception/obstacles"]
    assert obstacles["message_count"] == 1
    assert obstacles["obstacle_count"] == 0
    assert obstacles["empty_message_count"] == 1


def test_bridge_stats_to_channel_stats_prefers_publish_elapsed_wall_time() -> None:
    bridge_stats = _bridge_stats()
    bridge_stats["loc_count"] = 200
    bridge_stats["publish_elapsed_wall_sec"] = 10.0

    stats = bridge_stats_to_channel_stats(bridge_stats)

    assert stats["source"]["duration_s"] == 10.0
    assert stats["source"]["duration_source"] == "publish_elapsed_wall_sec"
    assert stats["channels"]["/apollo/localization/pose"]["hz"] == 20.0


def test_missing_bridge_stats_returns_none(tmp_path: Path) -> None:
    assert normalize_channel_stats_for_run(tmp_path / "missing") is None


def test_normalize_channel_stats_uses_lightweight_control_consume_artifact(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "cyber_bridge_stats.json").write_text(
        json.dumps(_bridge_stats()),
        encoding="utf-8",
    )
    (artifacts / "control_trajectory_consume_debug_live.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"timestamp": 1000.0, "control_cycle_index": 1}),
                json.dumps({"timestamp": 1000.05, "control_cycle_index": 2}),
                json.dumps({"timestamp": 1000.10, "control_cycle_index": 3}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    stats = normalize_channel_stats_for_run(run_dir)

    assert stats is not None
    control = stats["channels"]["/apollo/control"]
    assert control["source"] == "control_trajectory_consume_debug_live.jsonl"
    assert control["message_count"] == 3
    assert control["sequence_monotonic"] is True
    assert control["derived_from_bridge_counters"] is False
