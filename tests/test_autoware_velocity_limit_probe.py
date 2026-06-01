import json

from carla_testbed.autoware.velocity_limit_probe import (
    DEFAULT_TOPIC,
    build_ros2_topic_pub_command,
    build_velocity_limit_message_dict,
)


def test_build_velocity_limit_message_dict_matches_autoware_internal_shape():
    msg = build_velocity_limit_message_dict(22.22, sender="api")

    assert msg["stamp"] == {"sec": 0, "nanosec": 0}
    assert msg["max_velocity"] == 22.22
    assert msg["use_constraints"] is False
    assert msg["constraints"]["max_acceleration"] == 1.0
    assert msg["constraints"]["min_acceleration"] == -2.5
    assert msg["sender"] == "api"


def test_build_velocity_limit_message_rejects_invalid_speed():
    try:
        build_velocity_limit_message_dict(0.0)
    except ValueError as exc:
        assert "max_velocity_mps" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected invalid speed to fail")


def test_build_ros2_topic_pub_command_is_manual_probe_equivalent():
    cmd = build_ros2_topic_pub_command(max_velocity_mps=22.22, rate_hz=5.0)

    assert cmd[:4] == ["ros2", "topic", "pub", "--rate"]
    assert "5.0" in cmd
    assert DEFAULT_TOPIC in cmd
    assert "autoware_internal_planning_msgs/msg/VelocityLimit" in cmd
    payload = json.loads(cmd[-1])
    assert payload["max_velocity"] == 22.22
