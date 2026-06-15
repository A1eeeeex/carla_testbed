from __future__ import annotations

from carla_testbed.analysis.apollo_route_contract import build_lane_equivalence_town01


def _route_contract(carla_lanes: list[str], apollo_lanes: list[str]) -> dict:
    return {
        "status": "warn",
        "scenario_lane_namespace": "carla_waypoint",
        "apollo_lane_namespace": "apollo_hdmap",
        "lane_equivalence_status": "cross_namespace_unverified",
        "scenario_route_lane_sequence": carla_lanes,
        "apollo_routing_lane_sequence": apollo_lanes,
        "lane_equivalence": {"status": "cross_namespace_unverified"},
    }


def _row(
    *,
    carla_lane_key: str,
    nearest_lane_id: str,
    route_index: int,
    route_s: float,
    lateral_error_m: float = 0.01,
    heading_error_rad: float = 0.002,
) -> dict:
    road, lane = carla_lane_key.split(":", 1)
    return {
        "source": "apollo_hdmap_api",
        "status": "ok",
        "projection_status": "ok",
        "sample_type": "route",
        "route_index": route_index,
        "route_s": route_s,
        "carla_lane_key": carla_lane_key,
        "carla_lane_id": f"{road}:0:{lane}",
        "nearest_lane_id": nearest_lane_id,
        "projection_s": route_s,
        "projection_l": lateral_error_m,
        "lateral_error_m": lateral_error_m,
        "heading_error_rad": heading_error_rad,
        "lane_heading_at_s": 0.0,
    }


def test_id_different_but_geometry_equivalent_is_verified() -> None:
    report = build_lane_equivalence_town01(
        _route_contract(["117:1"], ["82:1"]),
        hdmap_projection_rows=[
            _row(carla_lane_key="117:1", nearest_lane_id="82_4_1", route_index=0, route_s=0.0),
            _row(carla_lane_key="117:1", nearest_lane_id="82_3_1", route_index=1, route_s=5.0),
            _row(carla_lane_key="117:1", nearest_lane_id="82_2_1", route_index=2, route_s=10.0),
        ],
    )

    pair = report["pairs"][0]
    assert report["status"] == "pass"
    assert pair["status"] == "verified"
    assert pair["classification"] == "verified_equivalent"
    assert pair["candidate_apollo_lane_id"] == "82:1"


def test_id_same_but_heading_high_is_heading_issue_not_pass() -> None:
    report = build_lane_equivalence_town01(
        _route_contract(["13:-1"], ["13:-1"]),
        hdmap_projection_rows=[
            _row(
                carla_lane_key="13:-1",
                nearest_lane_id="13_1_-1",
                route_index=index,
                route_s=float(index * 4.0),
                heading_error_rad=0.04,
            )
            for index in range(5)
        ],
    )

    pair = report["pairs"][0]
    assert report["status"] == "fail"
    assert pair["status"] == "failed"
    assert pair["classification"] == "heading_convention_mismatch"
    assert pair["heading_diagnosis"]["classification"] in {
        "curve_chord_vs_tangent",
        "heading_convention_mismatch",
    }


def test_boundary_sample_projecting_to_neighbor_is_boundary_ambiguous() -> None:
    report = build_lane_equivalence_town01(
        _route_contract(["83:1"], ["31:-1"]),
        hdmap_projection_rows=[
            _row(carla_lane_key="83:1", nearest_lane_id="37_4_1", route_index=0, route_s=0.0),
            _row(carla_lane_key="83:1", nearest_lane_id="31_1_-1", route_index=1, route_s=5.0),
            _row(carla_lane_key="83:1", nearest_lane_id="31_1_-1", route_index=2, route_s=10.0),
            _row(carla_lane_key="83:1", nearest_lane_id="31_1_-1", route_index=3, route_s=15.0),
        ],
    )

    pair = report["pairs"][0]
    assert report["status"] == "fail"
    assert pair["status"] == "ambiguous"
    assert pair["classification"] == "boundary_transition_ambiguous"
    assert pair["failed_samples"][0]["is_boundary_sample"] is True
    assert pair["failed_samples"][0]["boundary_transition_explains_mismatch"] is True


def test_mid_lane_mismatch_is_routing_not_claim_route() -> None:
    report = build_lane_equivalence_town01(
        _route_contract(["83:1"], ["31:-1"]),
        hdmap_projection_rows=[
            _row(carla_lane_key="83:1", nearest_lane_id="31_1_-1", route_index=0, route_s=0.0),
            _row(carla_lane_key="83:1", nearest_lane_id="37_4_1", route_index=1, route_s=5.0),
            _row(carla_lane_key="83:1", nearest_lane_id="37_4_1", route_index=2, route_s=10.0),
            _row(carla_lane_key="83:1", nearest_lane_id="31_1_-1", route_index=3, route_s=15.0),
        ],
    )

    pair = report["pairs"][0]
    assert report["status"] == "fail"
    assert pair["classification"] == "routing_not_claim_route"
    assert any(sample["is_boundary_sample"] is False for sample in pair["failed_samples"])


def test_missing_projection_rows_are_insufficient_data() -> None:
    report = build_lane_equivalence_town01(_route_contract(["117:1"], ["82:1"]))

    assert report["status"] == "insufficient_data"
    assert "apollo_hdmap_projection_rows" in report["missing_fields"]
    assert "verified_lane_equivalence_table" in report["missing_fields"]
