from __future__ import annotations

import math
from types import SimpleNamespace

from tools.apollo10_cyber_bridge.planning_debug import (
    build_planning_debug_presence,
    build_trajectory_shape_debug,
)


def _point(x: float, y: float, theta: float, kappa: float, v: float, t: float) -> SimpleNamespace:
    return SimpleNamespace(
        path_point=SimpleNamespace(x=x, y=y, theta=theta, kappa=kappa),
        v=v,
        relative_time=t,
    )


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


def test_build_planning_debug_presence_reports_missing_planning_data() -> None:
    msg = SimpleNamespace(debug=SimpleNamespace())

    presence = build_planning_debug_presence(msg)

    assert presence["planning_debug_debug_present"] is True
    assert presence["planning_debug_planning_data_present"] is False
    assert presence["planning_debug_reference_line_path"] is None
    assert presence["planning_debug_reference_line_field_present"] is False
    assert presence["planning_debug_routing_field_present"] is False
    assert presence["planning_debug_diagnosis"] == "planning_data_missing"
