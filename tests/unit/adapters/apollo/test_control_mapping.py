from __future__ import annotations

import pytest

from carla_testbed.adapters.apollo import ApolloChannels, ApolloMVPConfig
from carla_testbed.adapters.apollo.control_mapping import (
    ApolloControlMappingConfig,
    map_apollo_control_dict_to_command,
)


def test_channel_defaults() -> None:
    channels = ApolloChannels()

    assert channels.localization == "/apollo/localization/pose"
    assert channels.chassis == "/apollo/canbus/chassis"
    assert channels.obstacles == "/apollo/perception/obstacles"
    assert channels.traffic_light == "/apollo/perception/traffic_light"
    assert channels.routing_request == "/apollo/routing_request"
    assert channels.routing_response == "/apollo/routing_response"
    assert channels.planning == "/apollo/planning"
    assert channels.control == "/apollo/control"
    assert channels.to_dict()["control"] == "/apollo/control"


def test_mvp_config_defaults_and_validation() -> None:
    cfg = ApolloMVPConfig()

    cfg.validate()
    assert cfg.time_source == "sim_time"
    assert cfg.use_ground_truth_obstacles is True
    assert cfg.to_dict()["channels"]["planning"] == "/apollo/planning"


def test_control_mapping_scales_percentage_fields() -> None:
    command = map_apollo_control_dict_to_command(
        {"throttle": 25.0, "brake": 10.0, "steering_target": -40.0}
    )

    assert command.throttle == pytest.approx(0.25)
    assert command.brake == pytest.approx(0.10)
    assert command.steer == pytest.approx(-0.40)
    assert command.source == "apollo_mvp"
    assert command.metadata["mapping"]["steering_key"] == "steering_target"


def test_control_mapping_clamps_sign_and_custom_scale() -> None:
    cfg = ApolloControlMappingConfig(
        steering_sign=-1.0,
        steering_scale=0.02,
        throttle_scale=0.02,
        brake_scale=0.02,
    )

    command = map_apollo_control_dict_to_command(
        {"throttle": 100.0, "brake": 100.0, "steering_target": 100.0},
        cfg,
    )

    assert command.throttle == pytest.approx(1.0)
    assert command.brake == pytest.approx(1.0)
    assert command.steer == pytest.approx(-1.0)


def test_control_mapping_supports_alternate_steering_fields_and_flags() -> None:
    command = map_apollo_control_dict_to_command(
        {
            "throttle_percentage": 50.0,
            "brake_percentage": 0.0,
            "steering_rate": 20.0,
            "reverse": "true",
            "parking_brake": 1,
        }
    )

    assert command.throttle == pytest.approx(0.5)
    assert command.brake == pytest.approx(0.0)
    assert command.steer == pytest.approx(0.2)
    assert command.reverse is True
    assert command.hand_brake is True
    assert command.metadata["mapping"]["steering_key"] == "steering_rate"
