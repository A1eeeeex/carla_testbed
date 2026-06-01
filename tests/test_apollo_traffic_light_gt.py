from __future__ import annotations

from pathlib import Path

from carla_testbed.adapters.apollo.traffic_light_gt import (
    MockTrafficLightGTPublisher,
    TrafficLightMapping,
    build_traffic_light_gt_message_dict,
    find_traffic_light_mapping,
    iter_traffic_light_mappings,
    load_traffic_light_mappings,
    normalize_traffic_light_state,
)

FIXTURE = Path("configs/town01/traffic_lights.example.yaml")


def verified_mapping() -> TrafficLightMapping:
    return TrafficLightMapping(
        logical_id="verified_tl",
        carla_actor_id=123,
        carla_landmark_id="landmark_123",
        apollo_signal_id="signal_123",
        stop_line_id="stop_line_123",
        lane_ids=("lane_1", "lane_2"),
        default_state="RED",
    )


def test_load_fixture_and_find_mapping() -> None:
    config = load_traffic_light_mappings(FIXTURE)
    mappings = iter_traffic_light_mappings(config)

    assert config["schema_version"] == "apollo_traffic_light_gt.v1"
    assert len(mappings) >= 2
    by_logical_id = find_traffic_light_mapping(
        config, logical_id="town01_tl_red_stop_placeholder"
    )
    by_actor_id = find_traffic_light_mapping(
        config, carla_actor_id="placeholder:carla_actor_tl_green_go"
    )

    assert by_logical_id is not None
    assert by_logical_id.default_state == "RED"
    assert "traffic_light_red_stop" in by_logical_id.supported_scenarios
    assert by_actor_id is not None
    assert by_actor_id.default_state == "GREEN"


def test_state_normalization_supports_required_colors() -> None:
    assert normalize_traffic_light_state("RED") == "RED"
    assert normalize_traffic_light_state("yellow") == "YELLOW"
    assert normalize_traffic_light_state("Green") == "GREEN"
    assert normalize_traffic_light_state("not_a_carla_state") == "UNKNOWN"


def test_build_message_for_red_green_yellow_unknown() -> None:
    mapping = verified_mapping()

    red = build_traffic_light_gt_message_dict("RED", mapping, timestamp_sec=1.0, frame_id=10)
    green = build_traffic_light_gt_message_dict("GREEN", mapping, timestamp_sec=2.0, frame_id=20)
    yellow = build_traffic_light_gt_message_dict("YELLOW", mapping)
    unknown = build_traffic_light_gt_message_dict("UNKNOWN", mapping)

    assert red["signal_id"] == "signal_123"
    assert red["color"] == "RED"
    assert red["confidence"] == 1.0
    assert red["timestamp_sec"] == 1.0
    assert red["frame_id"] == 10
    assert green["color"] == "GREEN"
    assert yellow["color"] == "YELLOW"
    assert unknown["color"] == "UNKNOWN"
    assert "traffic_light_state_unknown" in unknown["warnings"]


def test_missing_mapping_warns_instead_of_silent_unknown_publish() -> None:
    message = build_traffic_light_gt_message_dict("GREEN", None)

    assert message["status"] == "warn"
    assert message["signal_id"] is None
    assert message["color"] == "GREEN"
    assert "missing_signal_mapping" in message["warnings"]


def test_missing_signal_mapping_fields_warn() -> None:
    message = build_traffic_light_gt_message_dict(
        "RED",
        {
            "logical_id": "bad_mapping",
            "carla_actor_id": "actor_1",
            "lane_ids": [],
        },
    )

    assert message["status"] == "warn"
    assert "missing_apollo_signal_id" in message["warnings"]
    assert "missing_stop_line_id" in message["warnings"]
    assert "missing_lane_ids" in message["warnings"]


def test_placeholder_signal_ids_are_not_treated_as_verified() -> None:
    config = load_traffic_light_mappings(FIXTURE)
    mapping = find_traffic_light_mapping(config, logical_id="town01_tl_red_stop_placeholder")

    message = build_traffic_light_gt_message_dict("RED", mapping)

    assert message["signal_id"] == "placeholder:apollo_signal_town01_red_stop"
    assert "apollo_signal_id_placeholder_unverified" in message["warnings"]
    assert "stop_line_id_placeholder_unverified" in message["warnings"]


def test_mock_publisher_records_messages_and_warnings() -> None:
    publisher = MockTrafficLightGTPublisher()
    message = publisher.publish_state("UNKNOWN", verified_mapping(), timestamp_sec=3.0, frame_id=30)

    assert publisher.publish_count == 1
    assert publisher.messages == [message]
    assert "traffic_light_state_unknown" in publisher.warnings
