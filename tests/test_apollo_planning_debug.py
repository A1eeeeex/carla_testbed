from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from tools.apollo10_cyber_bridge.planning_debug import (
    build_planning_debug_presence,
    build_prediction_obstacles_debug,
    build_trajectory_shape_debug,
)


def _point(x: float, y: float, theta: float, kappa: float, v: float, t: float) -> SimpleNamespace:
    return SimpleNamespace(
        path_point=SimpleNamespace(x=x, y=y, theta=theta, kappa=kappa),
        v=v,
        relative_time=t,
    )


def test_build_prediction_obstacles_debug_records_selected_future_speed() -> None:
    points = [
        SimpleNamespace(
            path_point=SimpleNamespace(x=100.0 - index, y=5.0, lane_id="0_0_2"),
            v=12.0,
            a=-0.5,
            relative_time=float(index),
        )
        for index in range(7)
    ]
    msg = SimpleNamespace(
        header=SimpleNamespace(timestamp_sec=1234.5, sequence_num=42),
        start_timestamp=1234.4,
        end_timestamp=1234.6,
        prediction_obstacle=[
            SimpleNamespace(
                perception_obstacle=SimpleNamespace(
                    id=147,
                    timestamp=35.0,
                    position=SimpleNamespace(x=100.0, y=5.0, z=0.0),
                    velocity=SimpleNamespace(x=-19.0, y=0.0, z=0.0),
                    theta=math.pi,
                ),
                timestamp=35.0,
                is_static=False,
                predicted_period=6.0,
                trajectory=[
                    SimpleNamespace(probability=0.2, trajectory_point=[]),
                    SimpleNamespace(probability=0.8, trajectory_point=points),
                ],
            )
        ],
    )

    debug = build_prediction_obstacles_debug(msg)

    assert debug["prediction_header_timestamp_sec"] == pytest.approx(1234.5)
    assert debug["prediction_header_sequence_num"] == 42
    assert debug["prediction_obstacle_count"] == 1
    obstacle = debug["prediction_obstacles"][0]
    assert obstacle["perception_id"] == 147
    assert obstacle["perception_speed_mps"] == pytest.approx(19.0)
    assert obstacle["perception_speed_3d_mps"] == pytest.approx(19.0)
    assert obstacle["trajectories_recorded_count"] == 2
    assert obstacle["trajectories_truncated"] is False
    assert obstacle["trajectory_summaries"][0] == {
        "index": 0,
        "probability": pytest.approx(0.2),
        "point_count": 0,
        "samples": [],
    }
    assert obstacle["trajectory_summaries"][1]["index"] == 1
    assert obstacle["trajectory_summaries"][1]["probability"] == pytest.approx(0.8)
    assert obstacle["trajectory_summaries"][1]["point_count"] == 7
    assert all(
        sample["v_mps"] == pytest.approx(12.0)
        for sample in obstacle["trajectory_summaries"][1]["samples"]
    )
    assert obstacle["selected_trajectory_index"] == 1
    assert obstacle["selected_trajectory_probability"] == pytest.approx(0.8)
    assert obstacle["selected_trajectory_point_count"] == 7
    assert [sample["relative_time_sec"] for sample in obstacle["selected_trajectory_samples"]] == [
        0.0,
        1.0,
        2.0,
        3.0,
        6.0,
    ]
    assert all(sample["v_mps"] == pytest.approx(12.0) for sample in obstacle["selected_trajectory_samples"])
    assert all(sample["lane_id"] == "0_0_2" for sample in obstacle["selected_trajectory_samples"])


def test_build_prediction_obstacles_debug_caps_per_obstacle_trajectories() -> None:
    msg = SimpleNamespace(
        prediction_obstacle=[
            SimpleNamespace(
                perception_obstacle=SimpleNamespace(),
                trajectory=[
                    SimpleNamespace(probability=float(index), trajectory_point=[])
                    for index in range(3)
                ],
            )
        ]
    )

    debug = build_prediction_obstacles_debug(
        msg,
        max_trajectories_per_obstacle=2,
    )

    obstacle = debug["prediction_obstacles"][0]
    assert obstacle["trajectory_count"] == 3
    assert obstacle["trajectories_recorded_count"] == 2
    assert obstacle["trajectories_truncated"] is True
    assert [item["index"] for item in obstacle["trajectory_summaries"]] == [0, 1]
    assert obstacle["selected_trajectory_index"] == 2


def test_build_trajectory_shape_debug_from_fake_points() -> None:
    points = [
        _point(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        _point(1.0, 0.0, 0.0, 0.12, 1.0, 0.1),
        _point(2.0, 0.2, 0.1, -0.20, 2.0, 0.2),
        _point(3.0, 0.4, 0.2, 0.01, 3.0, 0.3),
    ]

    debug = build_trajectory_shape_debug(points)

    assert debug["trajectory_kappa"]["count"] == 4
    assert debug["trajectory_kappa"]["max_abs"] == 0.2
    assert debug["trajectory_kappa_spike_count_abs_ge_0_05"] == 2
    assert debug["trajectory_kappa_spike_count_abs_ge_0_10"] == 2
    assert debug["trajectory_xy_step_m"]["count"] == 3
    assert debug["trajectory_first_segment_heading"] == 0.0
    assert debug["trajectory_first_theta_minus_first_segment_heading_rad"] == 0.0
    assert debug["trajectory_sample_points"][0]["index"] == 0
    assert debug["trajectory_sample_points"][-1]["index"] == 3


def test_build_trajectory_shape_debug_handles_dict_points_and_heading_wrap() -> None:
    points = [
        {"path_point": {"x": 0.0, "y": 0.0, "theta": math.pi - 0.01, "kappa": 0.0}, "v": 1.0},
        {"path_point": {"x": -1.0, "y": 0.0, "theta": -math.pi + 0.01, "kappa": 0.0}, "v": 1.0},
    ]

    debug = build_trajectory_shape_debug(points)

    assert debug["trajectory_theta_delta_abs"]["max_abs"] < 0.03
    assert debug["trajectory_first_segment_heading"] == math.pi
    assert abs(debug["trajectory_first_theta_minus_first_segment_heading_rad"] + 0.01) < 1e-9


def test_build_trajectory_shape_debug_reports_nonexpired_future_segment() -> None:
    points = [
        _point(0.0, 0.0, math.pi / 2.0, 0.0, 8.0, -0.2),
        _point(0.0, -1.0, math.pi / 2.0, 0.0, 8.5, -0.1),
        _point(0.0, 0.0, math.pi / 2.0, 0.0, 9.0, 0.0),
        _point(0.0, 1.0, math.pi / 2.0, 0.0, 9.5, 0.1),
        _point(0.0, 10.0, math.pi / 2.0, 0.0, 10.0, 1.0),
    ]

    debug = build_trajectory_shape_debug(points)

    assert debug["trajectory_expired_prefix_count"] == 2
    assert debug["trajectory_first_nonexpired_point_index"] == 2
    assert debug["trajectory_first_nonexpired_point_relative_time"] == 0.0
    assert debug["trajectory_first_nonexpired_point_theta"] == math.pi / 2.0
    assert debug["trajectory_first_nonexpired_point_v"] == pytest.approx(9.0)
    assert debug["trajectory_speed_at_1s_mps"] == pytest.approx(10.0)
    assert debug["trajectory_speed_at_1s_relative_time_sec"] == pytest.approx(1.0)
    assert debug["trajectory_speed_at_1s_point_index"] == 4
    assert debug["trajectory_future_first_segment_heading"] == math.pi / 2.0
    assert debug["trajectory_first_nonexpired_theta_minus_future_segment_heading_rad"] == 0.0
    assert debug["trajectory_future_lookahead_heading"] == math.pi / 2.0
    assert debug["trajectory_first_nonexpired_theta_minus_future_lookahead_heading_rad"] == 0.0
    assert debug["trajectory_first_nonexpired_remaining_point_count"] == 3
    assert [point["index"] for point in debug["trajectory_future_window_points"]] == [0, 1, 2, 3, 4]


def test_build_trajectory_shape_debug_reports_lookahead_separately_from_short_segment() -> None:
    points = [
        _point(0.0, -2.0, math.pi / 2.0, 0.0, 0.0, -0.2),
        _point(0.0, -1.0, math.pi / 2.0, 0.0, 0.0, -0.1),
        _point(0.0, 0.0, math.pi / 2.0, 0.0, 0.0, 0.0),
        _point(0.0, -0.01, math.pi / 2.0, 0.0, 0.0, 0.02),
        _point(0.0, 0.50, math.pi / 2.0, 0.0, 0.0, 0.30),
    ]

    debug = build_trajectory_shape_debug(points)

    assert debug["trajectory_future_first_segment_heading"] == pytest.approx(-math.pi / 2.0)
    assert abs(debug["trajectory_first_nonexpired_theta_minus_future_segment_heading_rad"]) > 3.0
    assert debug["trajectory_future_lookahead_point_index"] == 4
    assert debug["trajectory_future_lookahead_heading"] == math.pi / 2.0
    assert debug["trajectory_first_nonexpired_theta_minus_future_lookahead_heading_rad"] == 0.0
    assert [point["index"] for point in debug["trajectory_future_window_points"]] == [0, 1, 2, 3, 4]


def test_build_trajectory_shape_debug_empty_points() -> None:
    debug = build_trajectory_shape_debug(None)

    assert debug["trajectory_sample_points"] == []
    assert debug["trajectory_kappa"]["count"] == 0
    assert debug["trajectory_first_segment_heading"] is None


def test_build_planning_debug_presence_with_reference_line_and_routing() -> None:
    msg = SimpleNamespace(
        debug=SimpleNamespace(
            planning_data=SimpleNamespace(
                reference_line=[SimpleNamespace(length=120.0), SimpleNamespace(length=80.0)],
                routing=SimpleNamespace(
                    road=[
                        SimpleNamespace(
                            passage=[
                                SimpleNamespace(
                                    segment=[
                                        SimpleNamespace(id="lane_a", start_s=0.0, end_s=60.0),
                                        SimpleNamespace(id="lane_b", start_s=60.0, end_s=120.0),
                                    ]
                                )
                            ]
                        )
                    ]
                ),
            )
        )
    )

    presence = build_planning_debug_presence(msg)

    assert presence["planning_debug_debug_present"] is True
    assert presence["planning_debug_planning_data_present"] is True
    assert presence["planning_debug_reference_line_path"] == "debug.planning_data.reference_line"
    assert presence["planning_debug_reference_line_field_present"] is True
    assert presence["planning_debug_reference_line_count"] == 2
    assert presence["planning_debug_reference_line_lengths"] == [120.0, 80.0]
    assert presence["planning_debug_routing_path"] == "debug.planning_data.routing"
    assert presence["planning_debug_routing_field_present"] is True
    assert presence["planning_debug_routing_road_count"] == 1
    assert presence["planning_debug_routing_passage_count"] == 1
    assert presence["planning_debug_routing_segment_count"] == 2
    assert presence["planning_debug_diagnosis"] == "reference_line_and_routing_present"


def test_build_planning_debug_presence_separates_empty_reference_line_from_missing_path() -> None:
    msg = {
        "debug": {
            "planning_data": {
                "reference_line": [],
                "reference_path": [{"length": 42.0}],
                "routing": {
                    "road": [
                        {
                            "passage": [
                                {"segment": [{"id": "lane_a", "start_s": 0.0, "end_s": 100.0}]}
                            ]
                        }
                    ]
                },
            }
        }
    }

    presence = build_planning_debug_presence(msg)

    assert presence["planning_debug_planning_data_present"] is True
    assert presence["planning_debug_reference_line_field_present"] is True
    assert presence["planning_debug_reference_line_count"] == 0
    assert presence["planning_debug_routing_segment_count"] == 1
    assert presence["planning_debug_diagnosis"] == "routing_present_reference_line_empty"
    inventory = presence["planning_debug_field_inventory"]
    assert "reference_line" in inventory["planning_data_fields"]
    assert inventory["planning_data_repeated_field_counts"]["reference_line"] == 0
    assert inventory["planning_data_repeated_field_counts"]["reference_path"] == 1
    assert {
        "path": "debug.planning_data.reference_path",
        "repeated_count": 1,
        "field_name_match": True,
    } in inventory["reference_line_candidate_paths"]


def test_build_planning_debug_presence_inventory_handles_namespace_messages() -> None:
    msg = SimpleNamespace(
        debug=SimpleNamespace(
            planning_data=SimpleNamespace(
                reference_line=[],
                path=[SimpleNamespace(length=10.0)],
                routing=SimpleNamespace(road=[]),
            )
        )
    )

    presence = build_planning_debug_presence(msg)

    inventory = presence["planning_debug_field_inventory"]
    assert "debug" in inventory["top_level_fields"]
    assert "planning_data" in inventory["debug_fields"]
    assert "path" in inventory["planning_data_fields"]
    assert inventory["planning_data_repeated_field_counts"]["path"] == 1
    assert {
        "path": "debug.planning_data.path",
        "repeated_count": 1,
        "field_name_match": True,
    } in inventory["reference_line_candidate_paths"]
    path_summary = presence["planning_debug_path_candidate_summary"]
    assert path_summary["available"] is True
    assert path_summary["path_like_nonempty_candidate_count"] == 1
    assert path_summary["candidates"][0]["path"] == "debug.planning_data.path"
    assert path_summary["candidates"][0]["item_count"] == 1
    assert path_summary["candidates"][0]["item_summaries"][0]["scalar_fields"]["length"] == 10.0
    assert path_summary["candidates"][0]["item_summaries"][0]["point_sequence_candidates"] == []
    assert "diagnostic only" in path_summary["claim_boundary"]


def test_build_planning_debug_presence_summarizes_path_candidate_points() -> None:
    msg = SimpleNamespace(
        debug=SimpleNamespace(
            planning_data=SimpleNamespace(
                reference_line=[],
                path=[
                    SimpleNamespace(
                        name="regular_path",
                        path_point=[
                            SimpleNamespace(x=1.0, y=2.0, theta=0.1, kappa=0.01),
                            SimpleNamespace(x=2.0, y=2.1, theta=0.1, kappa=0.01),
                        ],
                    )
                ],
                routing=SimpleNamespace(road=[]),
            )
        )
    )

    presence = build_planning_debug_presence(msg)

    summary = presence["planning_debug_path_candidate_summary"]
    candidate = summary["candidates"][0]
    item = candidate["item_summaries"][0]
    assert summary["available"] is True
    assert candidate["path"] == "debug.planning_data.path"
    assert candidate["item_count"] == 1
    assert item["scalar_fields"]["name"] == "regular_path"
    assert item["sequence_counts"]["path_point"] == 2
    assert item["point_sequence_candidates"][0]["field"] == "path_point"
    assert item["point_sequence_candidates"][0]["sequence_count"] == 2
    assert item["point_sequence_candidates"][0]["point_like_count"] == 2
    assert item["point_sequence_candidates"][0]["first_point"] == {
        "x": 1.0,
        "y": 2.0,
        "theta": 0.1,
        "kappa": 0.01,
    }
    assert item["point_sequence_candidates"][0]["sample_points"] == [
        {"index": 0, "x": 1.0, "y": 2.0, "theta": 0.1, "kappa": 0.01},
        {"index": 1, "x": 2.0, "y": 2.1, "theta": 0.1, "kappa": 0.01},
    ]


def test_build_planning_debug_presence_keeps_all_four_path_stages() -> None:
    msg = SimpleNamespace(
        debug=SimpleNamespace(
            planning_data=SimpleNamespace(
                reference_line=[],
                path=[SimpleNamespace(name=f"path_stage_{index}") for index in range(4)],
                routing=SimpleNamespace(road=[]),
            )
        )
    )

    presence = build_planning_debug_presence(msg)

    items = presence["planning_debug_path_candidate_summary"]["candidates"][0][
        "item_summaries"
    ]
    assert [item["scalar_fields"]["name"] for item in items] == [
        "path_stage_0",
        "path_stage_1",
        "path_stage_2",
        "path_stage_3",
    ]


def test_build_planning_debug_presence_uses_path_geometry_fallback() -> None:
    msg = SimpleNamespace(
        debug=SimpleNamespace(
            planning_data=SimpleNamespace(
                reference_line=[],
                path=[
                    SimpleNamespace(
                        path_point=[
                            SimpleNamespace(x=0.0, y=0.0, theta=0.0, kappa=0.0),
                            SimpleNamespace(x=3.0, y=4.0, theta=0.0, kappa=0.0),
                        ],
                    ),
                    SimpleNamespace(
                        path_point=[
                            SimpleNamespace(x=10.0, y=10.0, theta=0.0, kappa=0.0),
                            SimpleNamespace(x=10.0, y=16.0, theta=0.0, kappa=0.0),
                        ],
                    ),
                ],
                routing=SimpleNamespace(
                    road=[
                        SimpleNamespace(
                            passage=[
                                SimpleNamespace(
                                    segment=[SimpleNamespace(id="lane_a", start_s=0.0, end_s=50.0)]
                                )
                            ]
                        )
                    ]
                ),
            )
        )
    )

    presence = build_planning_debug_presence(msg)

    assert presence["planning_debug_reference_line_field_present"] is True
    assert presence["planning_debug_path_fallback_used"] is True
    assert presence["planning_debug_path_item_count"] == 2
    assert presence["planning_debug_path_point_total"] == 4
    assert presence["planning_debug_reference_line_count"] == 2
    assert presence["planning_debug_reference_line_lengths"] == [5.0, 6.0]


def test_build_planning_debug_presence_reports_missing_planning_data() -> None:
    msg = SimpleNamespace(debug=SimpleNamespace())

    presence = build_planning_debug_presence(msg)

    assert presence["planning_debug_debug_present"] is True
    assert presence["planning_debug_planning_data_present"] is False
    assert presence["planning_debug_reference_line_path"] is None
    assert presence["planning_debug_reference_line_field_present"] is False
    assert presence["planning_debug_routing_field_present"] is False
    assert presence["planning_debug_diagnosis"] == "planning_data_missing"
