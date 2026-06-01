"""Readiness checks for the Baguang Autoware truth-input follow-stop demo.

The checker is deliberately offline: it inspects local files and suite schema
only. It does not import CARLA, ROS2, or Autoware runtime packages.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from carla_testbed.autoware.lanelet_speed import analyze_lanelet_speed_metadata

BAGUANG_AUTOWARE_READINESS_SCHEMA_VERSION = "baguang_autoware_readiness.v1"
BAGUANG_AUTOWARE_SUITE_SCHEMA_VERSION = "baguang_autoware_followstop_suite.v1"
DEFAULT_SUITE_PATH = Path("configs/scenarios/baguang_autoware_followstop_suite.yaml")
DEFAULT_CARLA_ROOT = Path("/home/ubuntu/CARLA_0.9.16")
DEFAULT_AUTOWARE_MAP_ROOT = Path("/home/ubuntu/autoware-contents/maps")
DEFAULT_OUTPUT_DIR = Path("runs/readiness/baguang_autoware_followstop")
DEFAULT_MAP_NAME = "straight_road_for_baguang"
DEFAULT_TARGET_SPEED_MPS = 80.0 / 3.6


@dataclass(frozen=True)
class BaguangAutowareReadinessConfig:
    suite_path: Path = DEFAULT_SUITE_PATH
    out_dir: Path = DEFAULT_OUTPUT_DIR
    carla_root: Path = DEFAULT_CARLA_ROOT
    autoware_map_root: Path = DEFAULT_AUTOWARE_MAP_ROOT
    map_name: str = DEFAULT_MAP_NAME
    target_speed_mps: float = DEFAULT_TARGET_SPEED_MPS


def load_baguang_autoware_suite(path: str | Path = DEFAULT_SUITE_PATH) -> dict[str, Any]:
    suite_path = Path(path).expanduser()
    with suite_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Baguang Autoware suite must be a mapping: {suite_path}")
    return payload


def check_baguang_autoware_readiness(
    config: BaguangAutowareReadinessConfig | None = None,
) -> dict[str, Any]:
    cfg = config or BaguangAutowareReadinessConfig()
    blockers: list[str] = []
    warnings: list[str] = []
    suite = _inspect_suite(cfg, blockers, warnings)
    carla_map = _inspect_carla_map(cfg, blockers, warnings)
    autoware_map = _inspect_autoware_map(cfg, blockers, warnings)
    status = "not_ready" if blockers else ("warn" if warnings else "ready")
    return {
        "schema_version": BAGUANG_AUTOWARE_READINESS_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "needs_local_carla": True,
        "needs_local_autoware": True,
        "map_name": cfg.map_name,
        "suite": suite,
        "carla_map": carla_map,
        "autoware_map": autoware_map,
        "blockers": blockers,
        "warnings": warnings,
        "claim_boundary": {
            "readiness_proves_behavior": False,
            "readiness_proves_natural_driving": False,
            "readiness_proves_autoware_sensor_pipeline": False,
            "required_behavior_artifacts": [
                "summary.json",
                "timeseries.csv",
                "analysis/autoware_control/autoware_control_diagnostics.json",
                "analysis/autoware_followstop/autoware_followstop_diagnostics.json",
                "artifacts/autoware_operator_view_status.json",
                "artifacts/autoware_rosbag_status.json",
                "rosbag2/autoware_demo",
                "video/rviz/autoware_rviz.mp4",
                "video/screen/carla_third_person_screen.mp4",
                "video/dual_cam/demo_third_person.mp4",
                "video/dual_cam/demo_third_person_hud.mp4",
                "video/dual_cam/demo_third_person_raw.mp4",
            ],
        },
        "next_commands": _next_commands(cfg),
    }


def write_baguang_autoware_readiness_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "baguang_autoware_readiness.json"
    md_path = output / "baguang_autoware_readiness.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_baguang_autoware_readiness_markdown(report), encoding="utf-8")
    return {"readiness_json": str(json_path), "readiness_md": str(md_path)}


def render_baguang_autoware_readiness_markdown(report: Mapping[str, Any]) -> str:
    suite = _as_mapping(report.get("suite"))
    carla_map = _as_mapping(report.get("carla_map"))
    autoware_map = _as_mapping(report.get("autoware_map"))
    lines = [
        "# Baguang Autoware Follow-Stop Readiness",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- map_name: `{report.get('map_name')}`",
        f"- suite: `{suite.get('path')}`",
        f"- CARLA map visual assets: `{carla_map.get('visual_assets_available')}`",
        f"- Autoware Lanelet2 map: `{autoware_map.get('lanelet2_map_exists')}`",
        f"- Autoware point cloud map: `{autoware_map.get('pointcloud_map_exists')}`",
        "",
        "## Blockers",
    ]
    blockers = list(report.get("blockers") or [])
    lines.extend(f"- {item}" for item in blockers) if blockers else lines.append("- none")
    lines.append("")
    lines.append("## Warnings")
    warnings = list(report.get("warnings") or [])
    lines.extend(f"- {item}" for item in warnings) if warnings else lines.append("- none")
    lines.append("")
    lines.append("## Boundary")
    lines.append("- This readiness report does not prove Autoware follow-stop behavior.")
    lines.append("- It only checks local files and suite contracts before an online CARLA/Autoware run.")
    lines.append("")
    lines.append("## Next Commands")
    commands = _as_mapping(report.get("next_commands"))
    for name, command in commands.items():
        lines.append(f"- `{name}`: `{command}`")
    return "\n".join(lines) + "\n"


def _inspect_suite(
    cfg: BaguangAutowareReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    path = Path(cfg.suite_path).expanduser()
    if not path.exists():
        blockers.append(f"suite_missing:{path}")
        return {"path": str(path), "exists": False, "status": "missing"}
    try:
        suite = load_baguang_autoware_suite(path)
    except Exception as exc:
        blockers.append(f"suite_invalid:{type(exc).__name__}:{exc}")
        return {"path": str(path), "exists": True, "status": "invalid", "error": str(exc)}

    validation = validate_baguang_autoware_suite(suite, expected_map=cfg.map_name)
    blockers.extend(f"suite_error:{item}" for item in validation["errors"])
    warnings.extend(f"suite_warning:{item}" for item in validation["warnings"])
    scenario = _as_mapping(suite.get("scenario"))
    return {
        "path": str(path),
        "exists": True,
        "status": "pass" if not validation["errors"] else "invalid",
        "schema_version": suite.get("schema_version"),
        "name": suite.get("name"),
        "map": suite.get("map"),
        "scenario_id": scenario.get("scenario_id"),
        "route_id": scenario.get("route_id"),
        "target_speed_kph": scenario.get("target_speed_kph"),
        "lead_distance_m": scenario.get("lead_distance_m"),
        "required_channels": list(scenario.get("required_channels") or []),
        "required_artifacts": list(scenario.get("required_artifacts") or []),
        "validation": validation,
    }


def validate_baguang_autoware_suite(
    suite: Mapping[str, Any],
    *,
    expected_map: str = DEFAULT_MAP_NAME,
) -> dict[str, list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if suite.get("schema_version") != BAGUANG_AUTOWARE_SUITE_SCHEMA_VERSION:
        errors.append(f"schema_version must be {BAGUANG_AUTOWARE_SUITE_SCHEMA_VERSION}")
    if suite.get("map") != expected_map:
        errors.append(f"map must be {expected_map}")
    mode = _as_mapping(suite.get("mode"))
    if mode.get("truth_input") is not True:
        errors.append("mode.truth_input must be true")
    if mode.get("stack") != "autoware":
        errors.append("mode.stack must be autoware")
    if mode.get("full_sensor_perception_reproduced") is True:
        errors.append("truth-input suite must not claim full sensor perception reproduction")
    scenario = _as_mapping(suite.get("scenario"))
    required = {
        "scenario_id",
        "route_id",
        "scenario_class",
        "map",
        "duration_s",
        "target_speed_kph",
        "lead_distance_m",
        "route_ref",
        "required_inputs",
        "required_channels",
        "required_artifacts",
        "success_criteria",
    }
    missing = sorted(required - set(scenario))
    if missing:
        errors.append(f"scenario missing fields: {', '.join(missing)}")
    if scenario.get("scenario_class") != "follow_stop_80kph":
        errors.append("scenario.scenario_class must be follow_stop_80kph")
    if scenario.get("map") != expected_map:
        errors.append(f"scenario.map must be {expected_map}")
    if float(scenario.get("lead_distance_m") or 0.0) < 295.0:
        errors.append("scenario.lead_distance_m must be about 300m")
    if float(scenario.get("target_speed_kph") or 0.0) < 75.0:
        errors.append("scenario.target_speed_kph must preserve the 80kph target")
    for field in ("required_inputs", "required_channels", "required_artifacts"):
        if not isinstance(scenario.get(field), list) or not scenario.get(field):
            errors.append(f"scenario.{field} must be a non-empty list")
    artifacts = {Path(str(item)).name for item in scenario.get("required_artifacts") or []}
    for artifact in (
        "manifest.json",
        "summary.json",
        "timeseries.csv",
        "autoware_control_diagnostics.json",
        "autoware_followstop_diagnostics.json",
        "autoware_operator_view_status.json",
        "autoware_rosbag_status.json",
        "autoware_rviz.mp4",
        "autoware_demo",
        "carla_third_person_screen.mp4",
    ):
        if artifact not in artifacts:
            errors.append(f"scenario.required_artifacts missing {artifact}")
    if suite.get("gate_role") != "diagnostic_gate":
        warnings.append("baguang follow-stop should remain diagnostic until online artifacts pass")
    return {"errors": errors, "warnings": warnings}


def _inspect_carla_map(
    cfg: BaguangAutowareReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    root = Path(cfg.carla_root).expanduser()
    map_dir = root / "CarlaUE4" / "Content" / "Carla" / "Maps" / cfg.map_name
    umap = map_dir / f"{cfg.map_name}.umap"
    xodr = map_dir / "OpenDrive" / f"{cfg.map_name}.xodr"
    geometry_count = _count_files(map_dir / "Geometries", "*.uasset")
    material_count = _count_files(map_dir / "Materials", "*.uasset")
    texture_count = _count_files(map_dir / "Textures", "*.uasset")
    visual_assets_available = geometry_count > 0 and material_count > 0 and texture_count > 0
    roads_road = bool(list((map_dir / "Geometries").glob("**/Roads_Road.uasset")))
    roads_sidewalk = bool(list((map_dir / "Geometries").glob("**/Roads_Sidewalk.uasset")))
    if not root.exists():
        blockers.append(f"carla_root_missing:{root}")
    if not map_dir.exists():
        blockers.append(f"carla_map_dir_missing:{map_dir}")
    if not umap.exists():
        blockers.append(f"carla_map_umap_missing:{umap}")
    if not xodr.exists():
        blockers.append(f"carla_map_xodr_missing:{xodr}")
    if not visual_assets_available:
        blockers.append("carla_map_visual_assets_missing")
    if not roads_road:
        warnings.append("carla_map_roads_road_asset_missing")
    if not roads_sidewalk:
        warnings.append("carla_map_roads_sidewalk_asset_missing")
    return {
        "root": str(root),
        "map_dir": str(map_dir),
        "map_dir_exists": map_dir.exists(),
        "umap_exists": umap.exists(),
        "xodr_exists": xodr.exists(),
        "geometry_uasset_count": geometry_count,
        "material_uasset_count": material_count,
        "texture_uasset_count": texture_count,
        "visual_assets_available": visual_assets_available,
        "roads_road_asset_exists": roads_road,
        "roads_sidewalk_asset_exists": roads_sidewalk,
    }


def _inspect_autoware_map(
    cfg: BaguangAutowareReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    root = Path(cfg.autoware_map_root).expanduser()
    pcd = root / "point_cloud_maps" / f"{cfg.map_name}.pcd"
    osm = root / "vector_maps" / "lanelet2" / f"{cfg.map_name}.osm"
    if not root.exists():
        blockers.append(f"autoware_map_root_missing:{root}")
    if not pcd.exists():
        blockers.append(f"autoware_pointcloud_map_missing:{pcd}")
    if not osm.exists():
        blockers.append(f"autoware_lanelet2_map_missing:{osm}")
    speed_report: dict[str, Any] | None = None
    if osm.exists():
        speed_report = analyze_lanelet_speed_metadata(
            osm,
            requested_max_vel_mps=float(cfg.target_speed_mps),
            map_name=cfg.map_name,
        )
        if speed_report.get("status") == "warn":
            warnings.extend(f"autoware_lanelet_speed:{item}" for item in speed_report.get("warnings") or [])
        elif speed_report.get("status") not in {"pass", "warn"}:
            warnings.append(f"autoware_lanelet_speed_status:{speed_report.get('status')}")
    return {
        "root": str(root),
        "root_exists": root.exists(),
        "pointcloud_map_path": str(pcd),
        "pointcloud_map_exists": pcd.exists(),
        "lanelet2_map_path": str(osm),
        "lanelet2_map_exists": osm.exists(),
        "speed_metadata": speed_report,
    }


def _next_commands(cfg: BaguangAutowareReadinessConfig) -> dict[str, str]:
    online_autoware_followstop = (
        "/home/ubuntu/miniconda3/envs/carla16/bin/python3 examples/run_followstop.py "
        "--config configs/io/examples/followstop_autoware.yaml "
        "--override run.map=straight_road_for_baguang "
        "--override run.ticks=860 "
        "--override algo.autoware.carla_map=straight_road_for_baguang "
        "--override algo.autoware.map_root=/home/ubuntu/autoware-contents/maps "
        "--override algo.autoware.map_path=/home/ubuntu/autoware-contents/maps "
        "--override scenario.ego_idx=0 "
        "--override scenario.auto_align_front_spawn=false "
        "--override scenario.front_placement_mode=waypoint_ahead "
        "--override scenario.front_waypoint_ahead_m=300.0 "
        "--override scenario.front_target_ahead_m=300.0 "
        "--override scenario.front_min_ahead_m=295.0 "
        "--override scenario.front_max_ahead_m=305.0 "
        "--override algo.autoware.goal.carla_goal_pose.x=-80.3 "
        "--override algo.autoware.goal.carla_goal_pose.y=-5.05 "
        "--override algo.autoware.goal.carla_goal_pose.z=0.0 "
        "--override algo.autoware.goal.carla_goal_pose.yaw_deg=179.97 "
        "--override algo.autoware.planning_common_max_vel_mps=22.22 "
        "--override algo.autoware.planning_common_nominal_max_acc_mps2=2.5 "
        "--override algo.autoware.planning_common_nominal_min_acc_mps2=-3.0 "
        "--override algo.autoware.planning_common_limit_max_acc_mps2=3.0 "
        "--override algo.autoware.planning_common_limit_min_acc_mps2=-4.0 "
        '--override "algo.autoware.launch_extra_args=launch_sensing:=false '
        "launch_perception:=false launch_localization:=false launch_planning:=true "
        "launch_control:=true launch_map:=true launch_vehicle:=true is_simulation:=true "
        "launch_system_monitor:=false launch_dummy_diag_publisher:=true "
        "diagnostic_graph_aggregator_graph_path:=/opt/autoware/share/autoware_launch/config/system/diagnostics/autoware-carla.yaml "
        'rviz:=false rviz_respawn:=false launch_goal_planner_module:=false" '
        "--override algo.autoware.carla_control_bridge.longitudinal_mode=speed_feedback "
        "--override algo.autoware.carla_control_bridge.positive_accel_overshoot_action=coast_all "
        "--override algo.autoware.carla_control_bridge.speed_feedback_gain=0.22 "
        "--override algo.autoware.carla_control_bridge.speed_feedback_brake_gain=0.28 "
        "--override algo.autoware.carla_control_bridge.speed_feedback_deadband_mps=0.5 "
        "--override algo.autoware.carla_control_bridge.speed_feedback_transition_coast_sec=0.3 "
        "--override algo.autoware.carla_control_bridge.speed_feedback_max_throttle_step=0.12 "
        "--override algo.autoware.carla_control_bridge.speed_feedback_max_brake_step=0.12 "
        "--override algo.autoware.carla_control_bridge.negative_accel_brake_speed_margin_mps=0.5"
    )
    return {
        "readiness": (
            "python tools/check_baguang_autoware_readiness.py "
            f"--suite {cfg.suite_path} --carla-root {cfg.carla_root} "
            f"--autoware-map-root {cfg.autoware_map_root} --out {cfg.out_dir}"
        ),
        "carla_spawn_smoke": (
            "/home/ubuntu/miniconda3/envs/carla16/bin/python3 "
            "tools/run_baguang_followstop_300m.py --map straight_road_for_baguang "
            "--lead-distance-m 300 --spectator"
        ),
        "online_autoware_followstop": online_autoware_followstop,
        "online_autoware_followstop_demo_recording": (
            "env -u SDL_VIDEODRIVER "
            f"{online_autoware_followstop} "
            "--start-carla --carla-root /home/ubuntu/CARLA_0.9.16 "
            "--run-dir runs/baguang_autoware_80kph_followstop_demo_$(date +%Y%m%d_%H%M%S) "
            "--record dual_cam --record-dual-cam-third-person-only "
            "--record-resolution 960x540 --record-fps 20 --record-hud-mode driving "
            "--carla-extra-args='-RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low' "
            "--rig minimal --rig-override events.lane_invasion=false "
            "--override algo.autoware.carla_control_bridge.timeout_sec=5.0"
        ),
    }


def _count_files(root: Path, pattern: str) -> int:
    if not root.exists():
        return 0
    return sum(1 for item in root.glob(f"**/{pattern}") if item.is_file())


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
