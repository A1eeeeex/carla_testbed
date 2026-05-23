from __future__ import annotations

import pytest

from carla_testbed.evaluation.metrics import MetricsAccumulator, RunMetrics


def test_metrics_accumulates_average_and_max_speed() -> None:
    metrics = MetricsAccumulator()

    metrics.update(frame_id=1, sim_time_s=0.05, ego_speed_mps=1.0)
    metrics.update(frame_id=2, sim_time_s=0.10, ego_speed_mps=3.0)
    result = metrics.finalize(wall_duration_s=0.2, exit_reason="max_steps_reached")

    assert result.frames == 2
    assert result.sim_duration_s == pytest.approx(0.05)
    assert result.wall_duration_s == pytest.approx(0.2)
    assert result.avg_speed_mps == pytest.approx(2.0)
    assert result.max_speed_mps == pytest.approx(3.0)
    assert result.exit_reason == "max_steps_reached"


def test_metrics_tracks_min_distance_and_event_counts() -> None:
    metrics = MetricsAccumulator()

    metrics.update(
        frame_id=1,
        sim_time_s=0.0,
        ego_speed_mps=0.0,
        lead_distance_m=12.0,
        collision_events_count=1,
        lane_invasion_events_count=2,
    )
    metrics.update(
        frame_id=2,
        sim_time_s=0.1,
        ego_speed_mps=0.0,
        lead_distance_m=7.5,
        collision_events_count=3,
        lane_invasion_events_count=0,
    )
    result = metrics.finalize()

    assert result.min_lead_distance_m == pytest.approx(7.5)
    assert result.collision_count == 4
    assert result.lane_invasion_count == 2


def test_metrics_counts_applied_control_frames() -> None:
    metrics = MetricsAccumulator()

    metrics.update(frame_id=1, sim_time_s=0.0, applied_control={"applied_ok": True, "throttle": 0.2})
    metrics.update(frame_id=2, sim_time_s=0.1, applied_control={"applied_ok": False, "throttle": 0.2})
    metrics.update(frame_id=3, sim_time_s=0.2, applied_control={"throttle": 0.0, "brake": 1.0})
    result = metrics.finalize()

    assert result.control_frames == 2


def test_metrics_handles_no_frames_without_dividing_by_zero() -> None:
    result = MetricsAccumulator().finalize(exit_reason="not_started")

    assert result.frames == 0
    assert result.sim_duration_s is None
    assert result.avg_speed_mps is None
    assert result.max_speed_mps is None
    assert result.min_lead_distance_m is None
    assert result.collision_count == 0


def test_run_metrics_to_dict() -> None:
    metrics = RunMetrics(frames=1, avg_speed_mps=2.0, max_speed_mps=3.0, exit_reason="success")

    payload = metrics.to_dict()

    assert payload["frames"] == 1
    assert payload["avg_speed_mps"] == 2.0
    assert payload["max_speed_mps"] == 3.0
    assert payload["exit_reason"] == "success"
