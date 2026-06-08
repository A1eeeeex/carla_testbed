from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.obstacle_gt_contract import (
    analyze_obstacle_gt_contract_records,
    analyze_obstacle_gt_contract_run_dir,
)


def _pedestrian(**updates: object) -> dict:
    payload = {
        "timestamp": 1.0,
        "ego_actor_id": "ego",
        "carla_actor_id": "walker_1",
        "apollo_perception_id": "walker_1",
        "object_type": "PEDESTRIAN",
        "is_ego": False,
        "frame_transform_checked": True,
        "theta_frame_checked": True,
        "position_frame_apollo_map": True,
        "length": 0.5,
        "width": 0.5,
        "height": 1.7,
        "velocity_source": "carla_actor_state",
        "velocity": {"x": 0.8, "y": 0.0, "z": 0.0},
        "dynamic": True,
        "tracking_time": 0.1,
    }
    payload.update(updates)
    return payload


def test_pedestrian_obstacle_section_passes_when_required() -> None:
    report = analyze_obstacle_gt_contract_records(
        [_pedestrian()],
        scenario_class="pedestrian_crossing",
        pedestrian_required=True,
    )

    assert report["status"] == "pass"
    assert report["pedestrians"]["required"] is True
    assert report["pedestrians"]["object_count"] == 1
    assert report["pedestrians"]["carla_actor_ids"] == ["walker_1"]


def test_pedestrian_required_missing_section_fails() -> None:
    report = analyze_obstacle_gt_contract_records(
        [
            {
                **_pedestrian(carla_actor_id="vehicle_1", apollo_perception_id="vehicle_1"),
                "object_type": "VEHICLE",
            }
        ],
        pedestrian_required=True,
    )

    assert report["status"] == "fail"
    assert "pedestrian_obstacle_section_missing" in report["errors"]
    assert report["pedestrians"]["status"] == "fail"


def test_pedestrian_dynamic_velocity_zero_filled_fails_when_required() -> None:
    report = analyze_obstacle_gt_contract_records(
        [
            _pedestrian(
                velocity_source="Detection3DArray_missing_velocity",
                velocity={"x": 0.0, "y": 0.0, "z": 0.0},
                actually_stationary=False,
            )
        ],
        pedestrian_required=True,
    )

    assert report["status"] == "fail"
    assert "velocity_source_missing_or_zero_filled" in report["warnings"]
    assert "pedestrian_dynamic_velocity_zero_filled" in report["errors"]


def test_run_dir_manifest_with_walkers_requires_pedestrian_obstacles(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "traffic_flow_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "traffic_flow_manifest.v1",
                "provider": "carla_walker_ai_controller",
                "enabled": True,
                "requested_walker_count": 1,
                "spawned_walker_count": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "obstacle_gt_contract.jsonl").write_text(
        json.dumps({"timestamp": 1.0, "published_obstacle_count": 0}) + "\n",
        encoding="utf-8",
    )

    report = analyze_obstacle_gt_contract_run_dir(run_dir, scenario_class="lane_keep")

    assert report["pedestrian_required"] is True
    assert report["status"] == "fail"
    assert "required_pedestrian_obstacle_missing" in report["errors"]
