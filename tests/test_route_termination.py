from carla_testbed.runner.route_termination import evaluate_nominal_route_completion


def _metadata() -> dict:
    return {
        "capability_profile": "lane_keep",
        "front_actor_id": 0,
        "lead_profile": {"mode": "none"},
        "goal": {"x": 1.0, "y": 326.5},
        "claim_route_length_m": 241.3,
    }


def test_nominal_lane_keep_completes_near_goal_after_route_progress() -> None:
    result = evaluate_nominal_route_completion(
        _metadata(), ego_x=1.6, ego_y=325.9, route_s=240.9
    )

    assert result["eligible"] is True
    assert result["reached"] is True
    assert result["route_completion_ratio"] > 0.99


def test_nominal_lane_keep_does_not_complete_at_spatial_crossing_without_progress() -> None:
    result = evaluate_nominal_route_completion(
        _metadata(), ego_x=1.0, ego_y=326.5, route_s=100.0
    )

    assert result["reached"] is False


def test_follow_stop_is_not_eligible_for_nominal_route_termination() -> None:
    metadata = _metadata()
    metadata.update(
        {
            "capability_profile": "follow_stop",
            "front_actor_id": 42,
            "lead_profile": {"mode": "static"},
        }
    )

    result = evaluate_nominal_route_completion(
        metadata, ego_x=1.0, ego_y=326.5, route_s=241.3
    )

    assert result["eligible"] is False
    assert result["reached"] is False


def test_missing_claim_route_evidence_does_not_complete() -> None:
    metadata = _metadata()
    metadata.pop("claim_route_length_m")

    result = evaluate_nominal_route_completion(
        metadata, ego_x=1.0, ego_y=326.5, route_s=241.3
    )

    assert result["reached"] is False
    assert "route_length_m" in result["missing_fields"]
