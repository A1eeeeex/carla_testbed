from __future__ import annotations

import json

from tbio.ros2.observability import assess_probe_results


def test_probe_assessment_requires_clock_by_default(tmp_path) -> None:
    probe = tmp_path / "sensor_probe.json"
    probe.write_text(
        json.dumps(
            {
                "/clock": {"count": 0},
                "/tf": {"count": 5},
                "/carla/hero/odom": {"count": 5},
            }
        ),
        encoding="utf-8",
    )

    assessment = assess_probe_results(
        probe,
        ["/clock", "/tf", "/carla/hero/odom"],
        ego_id="hero",
        namespace="/carla",
    )

    assert assessment.sensor_probe_ok is False
    assert assessment.ros2_sensor_ok is True


def test_probe_assessment_can_use_tf_without_clock_when_configured(tmp_path) -> None:
    probe = tmp_path / "sensor_probe.json"
    probe.write_text(
        json.dumps(
            {
                "/clock": {"count": 0},
                "/tf": {"count": 5},
                "/carla/hero/odom": {"count": 5},
            }
        ),
        encoding="utf-8",
    )

    assessment = assess_probe_results(
        probe,
        ["/tf", "/carla/hero/odom"],
        ego_id="hero",
        namespace="/carla",
        require_clock=False,
    )

    assert assessment.sensor_probe_ok is True
    assert assessment.clock_count == 0
    assert assessment.tf_count == 5
    assert assessment.ros2_sensor_topic_count == 1
    assert assessment.ros2_sensor_msgs == 5
    assert assessment.ros2_sensor_ok is True
