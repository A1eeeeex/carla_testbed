from __future__ import annotations

import json

import yaml

from carla_testbed.utils.town01_route_health import finalize_town01_run


def test_finalizer_propagates_safety_exit_into_acceptance(tmp_path):
    run_dir = tmp_path / "apollo_lane_invasion"
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True)

    # The runtime summary can be optimistic before the route-health finalizer
    # sees the complete event stream. Safety evidence must still dominate.
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "success": True,
                "exit_reason": "LANE_INVASION",
                "lane_invasion_count": 1,
                "collision_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "effective.yaml").write_text(
        yaml.safe_dump({"run": {"profile_name": "town01_route_health_test"}}),
        encoding="utf-8",
    )
    (artifacts_dir / "scenario_metadata.json").write_text(
        json.dumps(
            {
                "route_id": "town01_test_route",
                "route_length_m": 200.0,
                "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
            }
        ),
        encoding="utf-8",
    )
    (artifacts_dir / "bridge_health_summary.json").write_text("{}", encoding="utf-8")
    (artifacts_dir / "cyber_bridge_stats.json").write_text(
        json.dumps({"routing_request_count": 1, "routing_success_count": 1}),
        encoding="utf-8",
    )
    (artifacts_dir / "direct_bridge_stats.json").write_text("{}", encoding="utf-8")

    finalized = finalize_town01_run(run_dir)

    assert finalized["success"] is False
    assert finalized["fail_reason"] == "LANE_INVASION"
    assert finalized["acceptance"]["success"] is False
    assert finalized["acceptance"]["failure_codes"][0] == "LANE_INVASION"
    assert finalized["acceptance"]["checks"]["safety"]["ok"] is False
