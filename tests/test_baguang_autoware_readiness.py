from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.autoware.baguang_readiness import (
    BaguangAutowareReadinessConfig,
    check_baguang_autoware_readiness,
    load_baguang_autoware_suite,
    validate_baguang_autoware_suite,
    write_baguang_autoware_readiness_report,
)


def _write(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fake_carla_root(tmp_path: Path) -> Path:
    root = tmp_path / "CARLA_0.9.16"
    map_dir = root / "CarlaUE4" / "Content" / "Carla" / "Maps" / "straight_road_for_baguang"
    _write(map_dir / "straight_road_for_baguang.umap")
    _write(map_dir / "OpenDrive" / "straight_road_for_baguang.xodr")
    _write(map_dir / "Geometries" / "Road" / "Roads_Road.uasset")
    _write(map_dir / "Geometries" / "Sidewalk" / "Roads_Sidewalk.uasset")
    _write(map_dir / "Materials" / "Asphalt1_Road.uasset")
    _write(map_dir / "Textures" / "Asphalt1_Diff.uasset")
    return root


def _fake_autoware_map_root(tmp_path: Path) -> Path:
    root = tmp_path / "autoware_maps"
    _write(root / "point_cloud_maps" / "straight_road_for_baguang.pcd", "# .PCD v0.7\n")
    _write(
        root / "vector_maps" / "lanelet2" / "straight_road_for_baguang.osm",
        """
        <osm>
          <relation id="1">
            <tag k="type" v="lanelet"/>
            <tag k="subtype" v="road"/>
            <tag k="speed_limit" v="80"/>
          </relation>
        </osm>
        """,
    )
    return root


def test_baguang_autoware_suite_schema_loads() -> None:
    suite = load_baguang_autoware_suite("configs/scenarios/baguang_autoware_followstop_suite.yaml")
    validation = validate_baguang_autoware_suite(suite)

    assert suite["map"] == "straight_road_for_baguang"
    assert suite["scenario"]["scenario_id"] == "baguang_followstop_300m_80kph"
    assert suite["scenario"]["lead_distance_m"] == 300
    assert suite["scenario"]["current_best_demo_recording_profile"]["status"] == "experimental_verified_once"
    assert (
        suite["scenario"]["current_best_demo_recording_profile"]["evidence_run"]
        == "runs/baguang_autoware_80kph_followstop_demo_20260531_190757_timeout5_no_lane"
    )
    assert validation["errors"] == []


def test_readiness_passes_with_fake_complete_local_inputs(tmp_path: Path) -> None:
    cfg = BaguangAutowareReadinessConfig(
        carla_root=_fake_carla_root(tmp_path),
        autoware_map_root=_fake_autoware_map_root(tmp_path),
        out_dir=tmp_path / "out",
    )

    report = check_baguang_autoware_readiness(cfg)

    assert report["schema_version"] == "baguang_autoware_readiness.v1"
    assert report["status"] == "ready"
    assert report["blockers"] == []
    assert report["carla_map"]["visual_assets_available"] is True
    assert report["autoware_map"]["lanelet2_map_exists"] is True
    assert report["autoware_map"]["pointcloud_map_exists"] is True
    assert report["claim_boundary"]["readiness_proves_behavior"] is False
    online_cmd = report["next_commands"]["online_autoware_followstop"]
    assert "--override scenario.front_placement_mode=waypoint_ahead" in online_cmd
    assert "--override scenario.front_waypoint_ahead_m=300.0" in online_cmd
    assert "--override algo.autoware.goal.carla_goal_pose.x=-80.3" in online_cmd
    assert "--override run.ticks=860" in online_cmd
    assert "--override algo.autoware.planning_common_max_vel_mps=22.22" in online_cmd
    assert "--override algo.autoware.planning_common_nominal_max_acc_mps2=2.5" in online_cmd
    assert "--override algo.autoware.planning_common_nominal_min_acc_mps2=-3.0" in online_cmd
    assert "launch_goal_planner_module:=false" in online_cmd
    assert "--override algo.autoware.carla_control_bridge.longitudinal_mode=speed_feedback" in online_cmd
    assert "--override algo.autoware.carla_control_bridge.positive_accel_overshoot_action=coast_all" in online_cmd
    assert "--override algo.autoware.carla_control_bridge.speed_feedback_deadband_mps=0.5" in online_cmd
    assert "--override algo.autoware.carla_control_bridge.speed_feedback_transition_coast_sec=0.3" in online_cmd
    demo_cmd = report["next_commands"]["online_autoware_followstop_demo_recording"]
    assert demo_cmd.startswith("env -u SDL_VIDEODRIVER ")
    assert "--start-carla --carla-root /home/ubuntu/CARLA_0.9.16" in demo_cmd
    assert "--record dual_cam --record-dual-cam-third-person-only" in demo_cmd
    assert "--record-hud-mode driving" in demo_cmd
    assert "--rig-override events.lane_invasion=false" in demo_cmd
    assert "--override algo.autoware.carla_control_bridge.timeout_sec=5.0" in demo_cmd
    assert "video/dual_cam/demo_third_person.mp4" in report["claim_boundary"]["required_behavior_artifacts"]


def test_readiness_blocks_missing_autoware_lanelet_and_pointcloud(tmp_path: Path) -> None:
    cfg = BaguangAutowareReadinessConfig(
        carla_root=_fake_carla_root(tmp_path),
        autoware_map_root=tmp_path / "missing_maps",
    )

    report = check_baguang_autoware_readiness(cfg)

    assert report["status"] == "not_ready"
    assert any(item.startswith("autoware_map_root_missing:") for item in report["blockers"])
    assert any(item.startswith("autoware_lanelet2_map_missing:") for item in report["blockers"])
    assert any(item.startswith("autoware_pointcloud_map_missing:") for item in report["blockers"])


def test_readiness_blocks_missing_carla_visual_assets(tmp_path: Path) -> None:
    carla_root = tmp_path / "CARLA_0.9.16"
    map_dir = carla_root / "CarlaUE4" / "Content" / "Carla" / "Maps" / "straight_road_for_baguang"
    _write(map_dir / "straight_road_for_baguang.umap")
    _write(map_dir / "OpenDrive" / "straight_road_for_baguang.xodr")
    cfg = BaguangAutowareReadinessConfig(
        carla_root=carla_root,
        autoware_map_root=_fake_autoware_map_root(tmp_path),
    )

    report = check_baguang_autoware_readiness(cfg)

    assert report["status"] == "not_ready"
    assert "carla_map_visual_assets_missing" in report["blockers"]


def test_readiness_writer_and_cli_create_reports(tmp_path: Path) -> None:
    cfg = BaguangAutowareReadinessConfig(
        carla_root=_fake_carla_root(tmp_path),
        autoware_map_root=_fake_autoware_map_root(tmp_path),
        out_dir=tmp_path / "readiness",
    )
    report = check_baguang_autoware_readiness(cfg)

    outputs = write_baguang_autoware_readiness_report(report, cfg.out_dir)

    payload = json.loads(Path(outputs["readiness_json"]).read_text(encoding="utf-8"))
    markdown = Path(outputs["readiness_md"]).read_text(encoding="utf-8")
    assert payload["status"] == "ready"
    assert "does not prove Autoware follow-stop behavior" in markdown

    cli_out = tmp_path / "cli"
    result = subprocess.run(
        [
            sys.executable,
            "tools/check_baguang_autoware_readiness.py",
            "--carla-root",
            str(cfg.carla_root),
            "--autoware-map-root",
            str(cfg.autoware_map_root),
            "--out",
            str(cli_out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "status=ready" in result.stdout
    assert (cli_out / "baguang_autoware_readiness.json").exists()
