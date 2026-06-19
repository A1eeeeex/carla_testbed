from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from carla_testbed.analysis.channel_cadence_diagnosis import (
    CHANNEL_CADENCE_DIAGNOSIS_SCHEMA_VERSION,
    analyze_channel_cadence_diagnosis_files,
    channel_cadence_diagnosis_summary_md,
    write_channel_cadence_diagnosis_report,
)

FIXTURES = Path("tests/fixtures/channel_cadence")
CONFIG = Path("configs/algorithms/apollo_natural_driving_channels.yaml")


def test_isolated_header_sim_gap_is_blocking_but_diagnosed() -> None:
    report = analyze_channel_cadence_diagnosis_files(
        channel_stats_path=FIXTURES / "header_sim_gap_channel_stats.json",
        carla_tick_health_summary_path=FIXTURES / "carla_tick_health_summary.json",
        carla_tick_health_log_path=FIXTURES / "carla_tick_health.jsonl",
        topic_publish_stats_path=FIXTURES / "topic_publish_stats.jsonl",
        publish_gap_trace_path=FIXTURES / "publish_gap_trace.jsonl",
        config_path=CONFIG,
    )

    assert report["schema_version"] == CHANNEL_CADENCE_DIAGNOSIS_SCHEMA_VERSION
    assert report["status"] == "fail"
    assert report["primary_cadence_issue"] == "localization:isolated_header_sim_gap_over_contract"
    assert report["primary_gap_source"] == "stale_sample_skip"
    assert report["publish_skip_reason"] == "stale_sample_skipped"
    assert report["carla_tick_stage"] == "sensor_capture"
    assert report["top_gap_window"]["channel_name"] == "localization"
    loc = report["channels"]["localization"]
    assert loc["cadence_class"] == "isolated_header_sim_gap"
    assert loc["suspected_layer"] == "gt_publish_cadence"
    assert "gap_p95_ok_but_max_gap_exceeds_contract" in loc["warnings"]
    window = loc["gap_windows"][0]
    assert window["gap_ms"] == pytest.approx(350.0)
    assert window["publish_gap_correlation"]["skip_reason_counts"]["stale_sample_skipped"] == 2
    assert window["carla_tick_correlation"]["max_stage_name"] == "sensor_capture"
    assert report["top_gap_windows"][0]["channel_name"] == "localization"
    assert "localization:isolated_header_sim_gap_over_contract" in report["blocking_reasons"]


def test_slow_wall_delivery_does_not_mask_healthy_header_sim_cadence() -> None:
    report = analyze_channel_cadence_diagnosis_files(
        channel_stats_path=FIXTURES / "wall_delivery_channel_stats.json",
        carla_tick_health_summary_path=FIXTURES / "carla_tick_health_summary.json",
        config_path=CONFIG,
    )

    assert report["status"] == "warn"
    assert report["primary_cadence_issue"] is None
    loc = report["channels"]["localization"]
    assert loc["status"] == "warn"
    assert loc["cadence_class"] == "healthy_header_sim_cadence"
    assert "delivery_wall_hz_below_contract_but_header_sim_hz_ok" in loc["warnings"]
    assert "missing_publish_gap_trace" in report["warnings"]
    assert "publish_gap_trace" in report["missing_artifacts"]
    assert report["blocking_reasons"] == []


def test_fast_sim_time_without_wall_pacing_is_diagnosed_as_time_axis_speedup() -> None:
    report = analyze_channel_cadence_diagnosis_files(
        channel_stats_path=FIXTURES / "sim_speedup_channel_stats.json",
        carla_tick_health_summary_path=FIXTURES / "carla_tick_health_summary_fast_sim.json",
        config_path=CONFIG,
    )

    assert report["status"] == "fail"
    assert report["primary_cadence_issue"] == "localization:sim_time_speedup_without_wall_pacing"
    assert report["primary_gap_source"] == "sim_time_speedup_without_wall_pacing"
    assert report["sim_wall_cadence"]["sim_wall_speedup_factor"] == pytest.approx(7.8648648649)
    assert report["sim_wall_cadence"]["wall_time_pacing_enabled"] is False
    assert "Enable a wall-time-paced diagnostic profile" in report["next_action"]

    loc = report["channels"]["localization"]
    assert loc["status"] == "fail"
    assert loc["suspected_layer"] == "sim_time_speedup_without_wall_pacing"
    assert loc["wall_hz_for_sim_contract"] == pytest.approx(19.8)
    assert loc["effective_required_wall_hz_for_sim_contract"] == pytest.approx(78.648648649)
    assert "sim_time_speedup_without_wall_pacing" in loc["warnings"]
    assert "delivery_wall_hz_below_effective_required_wall_hz_for_sim_contract" in loc["warnings"]


def test_missing_channel_stats_is_insufficient_data() -> None:
    report = analyze_channel_cadence_diagnosis_files(
        channel_stats_path=None,
        config_path=CONFIG,
    )

    assert report["status"] == "insufficient_data"
    assert "channel_stats_missing" in report["blocking_reasons"]
    assert "channel_stats" in report["missing_artifacts"]


def test_writer_and_cli_create_report(tmp_path: Path) -> None:
    report = analyze_channel_cadence_diagnosis_files(
        channel_stats_path=FIXTURES / "header_sim_gap_channel_stats.json",
        carla_tick_health_summary_path=FIXTURES / "carla_tick_health_summary.json",
        carla_tick_health_log_path=FIXTURES / "carla_tick_health.jsonl",
        topic_publish_stats_path=FIXTURES / "topic_publish_stats.jsonl",
        publish_gap_trace_path=FIXTURES / "publish_gap_trace.jsonl",
        config_path=CONFIG,
    )
    outputs = write_channel_cadence_diagnosis_report(report, tmp_path / "writer")
    writer_report = json.loads(
        Path(outputs["channel_cadence_diagnosis_report"]).read_text(encoding="utf-8")
    )
    summary_md = channel_cadence_diagnosis_summary_md(writer_report)

    assert writer_report["primary_cadence_issue"].startswith("localization:")
    assert "Channel Cadence Diagnosis Summary" in summary_md

    cli_out = tmp_path / "cli"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_channel_cadence_diagnosis.py",
            "--channel-stats",
            str(FIXTURES / "header_sim_gap_channel_stats.json"),
            "--carla-tick-health-summary",
            str(FIXTURES / "carla_tick_health_summary.json"),
            "--carla-tick-health-log",
            str(FIXTURES / "carla_tick_health.jsonl"),
            "--topic-publish-stats",
            str(FIXTURES / "topic_publish_stats.jsonl"),
            "--publish-gap-trace",
            str(FIXTURES / "publish_gap_trace.jsonl"),
            "--out",
            str(cli_out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    stdout = json.loads(result.stdout)
    cli_report = json.loads((cli_out / "channel_cadence_diagnosis_report.json").read_text())

    assert result.returncode == 1
    assert stdout["status"] == "fail"
    assert cli_report["primary_cadence_issue"] == "localization:isolated_header_sim_gap_over_contract"
