from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from carla_testbed.adapters.apollo.frame_transform import ApolloFrameTransform, Vector3, carla_point_to_apollo
from carla_testbed.adapters.apollo.vehicle_reference import VehicleReferenceConfig, load_vehicle_reference
from carla_testbed.analysis.apollo_hdmap_projection import (
    read_apollo_hdmap_projection,
    summarize_apollo_hdmap_projection,
)
from carla_testbed.analysis.localization_channel_stats import build_localization_channel_stats
from carla_testbed.analysis.channel_stats_normalizer import (
    channel_stats_candidate_paths,
    normalize_channel_stats_for_run,
)

REPORT_SCHEMA_VERSION = "apollo_localization_contract.v1"
LOCALIZATION_CHANNEL = "/apollo/localization/pose"
DEFAULT_MESSAGE_TYPE = "LocalizationEstimate"
DEFAULT_HEADING_WARN_RAD = 0.20
DEFAULT_HEADING_FAIL_RAD = 0.50
DEFAULT_LANE_HEADING_BLOCK_RAD = 0.35
DEFAULT_YAW_RATE_WARN_RAD_S = 0.30
DEFAULT_SPEED_WARN_MPS = 0.50
DEFAULT_SPEED_FAIL_MPS = 2.00
DEFAULT_LOCALIZATION_FRESH_HZ = 10.0
DEFAULT_MEASUREMENT_HEADER_DELTA_FAIL_MS = 1.0
DEFAULT_ORIENTATION_HEADING_DIFF_FAIL_RAD = 1.0e-3
DEFAULT_DUPLICATE_TIMESTAMP_WARN_RATIO = 0.01
DEFAULT_FRAME_TRANSFORM_PATH = Path("configs/town01/apollo_frame_transform.example.yaml")
DEFAULT_VERIFIED_VEHICLE_REFERENCE_PATH = Path("configs/vehicles/ego_vehicle_reference.verified.yaml")
DEFAULT_EXAMPLE_VEHICLE_REFERENCE_PATH = Path("configs/vehicles/ego_vehicle_reference.example.yaml")

LOCALIZATION_CONTRACT_STRONG_EVIDENCE_FIELDS = (
    "measurement_time",
    "localization_header_timestamp_sec",
    "localization_time_base",
    "localization_orientation_qx",
    "localization_orientation_qy",
    "localization_orientation_qz",
    "localization_orientation_qw",
    "heading_source",
    "orientation_convention",
)

ACCEPTANCE_CHECK_IDS = (
    "localization_channel_health",
    "sim_time_time_base",
    "frame_transform_configured",
    "position_uses_vrp",
    "source_to_published_reference_explained",
    "heading_from_transformed_forward",
    "rfu_to_enu_orientation",
    "decoded_orientation_consistency",
    "kinematics_frame_and_units",
    "chassis_speed_consistency",
    "lane_projection",
    "reference_line_projection",
    "acceleration_semantics",
    "uncertainty_status_policy",
    "natural_driving_gate_ready",
)

ALIASES = {
    "sim_time": ["sim_time", "sim_time_s", "ts_sec", "timestamp", "time"],
    "frame_id": ["frame_id", "frame", "carla_frame_id"],
    "localization_timestamp": ["localization_timestamp", "localization_timestamp_sec"],
    "chassis_timestamp": ["chassis_timestamp", "chassis_timestamp_sec", "chassis_header_timestamp_sec"],
    "planning_timestamp": ["planning_timestamp", "planning_timestamp_sec"],
    "planning_message_age_ms": ["planning_message_age_ms", "planning_age_ms"],
    "ego_heading": ["ego_heading", "heading"],
    "route_heading": ["route_heading"],
    "heading_error": ["heading_error", "heading_error_rad"],
    "lane_heading_error": ["heading_error_to_lane", "lane_heading_error", "reference_line_heading_error", "e_psi_rad"],
    "lane_heading_error_deg": ["e_psi_deg", "lane_heading_error_deg"],
    "ego_speed": ["ego_speed", "ego_speed_mps", "speed_mps"],
    "chassis_speed_mps": ["chassis_speed_mps", "chassis_speed", "vehicle_speed_mps"],
    "localization_speed_mps": ["localization_speed_mps", "localization_speed", "ego_speed", "ego_speed_mps", "speed_mps"],
    "velocity_norm_vs_chassis_speed_mps": [
        "velocity_norm_vs_chassis_speed_mps",
        "localization_chassis_speed_delta_mps",
    ],
    "localization_chassis_timestamp_delta_ms": ["localization_chassis_timestamp_delta_ms"],
    "ego_yaw_rate": [
        "ego_yaw_rate",
        "ego_yaw_rate_rad_s",
        "localization_yaw_rate_rad_s",
        "localization_angular_velocity_z_rad_s",
        "angular_velocity_z_rad_s",
    ],
    "heading_fd_yaw_rate": ["heading_fd_yaw_rate", "heading_fd_yaw_rate_rad_s"],
    "yaw_rate_vs_heading_fd_rad_s": ["yaw_rate_vs_heading_fd_rad_s", "yaw_rate_heading_fd_delta_rad_s"],
    "spawn_projection_error_m": ["spawn_projection_error_m"],
    "orientation_heading_diff": [
        "decoded_orientation_heading_diff_rad",
        "orientation_heading_diff",
        "orientation_heading_diff_rad",
    ],
    "self_reported_orientation_heading_diff": ["orientation_heading_diff", "orientation_heading_diff_rad"],
    "decoded_orientation_heading": ["decoded_orientation_heading"],
    "localization_heading": ["localization_heading"],
    "localization_orientation_qx": ["localization_orientation_qx", "orientation_qx", "qx"],
    "localization_orientation_qy": ["localization_orientation_qy", "orientation_qy", "qy"],
    "localization_orientation_qz": ["localization_orientation_qz", "orientation_qz", "qz"],
    "localization_orientation_qw": ["localization_orientation_qw", "orientation_qw", "qw"],
    "measurement_time": ["measurement_time", "localization_measurement_time"],
    "localization_header_timestamp_sec": ["localization_header_timestamp_sec", "localization_timestamp"],
    "localization_frame_id": ["localization_frame_id", "localization_header_frame_id", "header_frame_id"],
    "localization_sequence_num": ["localization_sequence_num"],
    "localization_carla_frame_id": ["localization_carla_frame_id", "direct_world_frame"],
    "localization_time_base": ["localization_time_base"],
    "heading_source": ["heading_source"],
    "vehicle_reference_confidence": ["vehicle_reference_confidence"],
    "vehicle_reference_hard_gate_eligible": [
        "vehicle_reference_hard_gate_eligible",
        "runtime_vehicle_reference_hard_gate_eligible",
    ],
    "orientation_convention": ["orientation_convention"],
    "angular_velocity_unit": ["angular_velocity_unit", "localization_angular_velocity_unit"],
    "acceleration": ["linear_acceleration", "linear_acceleration_x", "accel_mean_mps2", "ego_acceleration"],
    "linear_acceleration_available": ["linear_acceleration_available"],
    "linear_acceleration_vrf": ["linear_acceleration_vrf", "linear_acceleration_vrf_x"],
    "linear_acceleration_vrf_available": ["linear_acceleration_vrf_available"],
    "angular_velocity_vrf": ["angular_velocity_vrf", "angular_velocity_vrf_z"],
    "angular_velocity_vrf_available": ["angular_velocity_vrf_available"],
    "acceleration_source": ["acceleration_source", "linear_acceleration_source"],
    "uncertainty": ["uncertainty", "localization_uncertainty", "pose_uncertainty"],
    "uncertainty_policy": ["uncertainty_policy", "localization_uncertainty_policy"],
    "msf_status": ["msf_status", "localization_msf_status"],
    "msf_status_policy": ["msf_status_policy", "localization_msf_status_policy"],
    "sensor_status": ["sensor_status", "localization_sensor_status"],
    "sensor_status_policy": ["sensor_status_policy", "localization_sensor_status_policy"],
    "ego_x": ["ego_x", "x"],
    "ego_y": ["ego_y", "y"],
    "ego_z": ["ego_z", "z"],
    "route_cross_track_error_m": ["cross_track_error", "cross_track_error_m", "route_cross_track_error_m"],
    "route_s": ["route_s", "route_station_m"],
    "lane_inside": ["lane_inside"],
    "lane_dist_m": ["lane_dist_m", "lane_projection_distance_m"],
    "lane_lateral_error_m": ["e_y_m", "lane_lateral_error_m"],
}


def analyze_localization_contract(
    *,
    timeseries_rows: list[dict[str, Any]] | None = None,
    channel_stats: Mapping[str, Any] | None = None,
    route_health: Mapping[str, Any] | None = None,
    hdmap_projection_rows: list[dict[str, Any]] | None = None,
    frame_transform: ApolloFrameTransform | None = None,
    vehicle_reference: VehicleReferenceConfig | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(source or {})
    missing_fields: list[str] = []
    warnings: list[str] = []
    blocking_reasons: list[str] = []

    rows = timeseries_rows or []
    if not rows:
        missing_fields.append("timeseries")
        warnings.append("timeseries_missing")

    channel = _analyze_channel(channel_stats, rows, source, missing_fields, warnings, blocking_reasons)
    time = _analyze_time(rows, missing_fields, warnings)
    frame = _analyze_frame_transform(frame_transform, rows, missing_fields, warnings)
    reference_point = _analyze_reference_point(vehicle_reference, source, missing_fields, warnings, blocking_reasons)
    pose = _analyze_pose_consistency(rows, route_health, missing_fields, warnings)
    lane_projection = _analyze_lane_projection(rows, missing_fields, warnings)
    apollo_hdmap_projection = summarize_apollo_hdmap_projection(hdmap_projection_rows or [])
    hdmap_route_lateral_consistency = _analyze_hdmap_route_lateral_consistency(
        rows,
        hdmap_projection_rows or [],
    )
    if apollo_hdmap_projection["file_present"]:
        warnings.extend(str(item) for item in apollo_hdmap_projection.get("warnings", []) if item)
        blocking_reasons.extend(str(item) for item in apollo_hdmap_projection.get("blocking_reasons", []) if item)
    pose["heading_error_to_apollo_hdmap_lane_p95_rad"] = apollo_hdmap_projection.get("heading_error_p95_rad")
    pose["lateral_error_to_apollo_hdmap_lane_p95_m"] = apollo_hdmap_projection.get("lateral_error_p95_m")
    kinematics = _analyze_kinematics(rows, missing_fields, warnings)
    status = _analyze_status(rows, missing_fields, warnings)

    if route_health is None:
        warnings.append("route_health_missing")
        missing_fields.append("route_health")

    if time["measurement_time_source"] is None:
        missing_fields.append("measurement_time")
        warnings.append("measurement_time_missing")
    if time["measurement_header_delta_ms_p95"] is not None:
        if float(time["measurement_header_delta_ms_p95"]) > DEFAULT_MEASUREMENT_HEADER_DELTA_FAIL_MS:
            blocking_reasons.append("measurement_header_timestamp_mismatch")
    if channel.get("header_frame_id") != "map" and (
        rows or channel.get("publish_message_count") or channel.get("fresh_sample_count")
    ):
        blocking_reasons.append("localization_header_frame_id_not_map")
        if not channel.get("header_frame_id"):
            missing_fields.append("localization_header_frame_id")
    if pose.get("orientation_heading_diff_p95_rad") is not None:
        if float(pose["orientation_heading_diff_p95_rad"]) >= DEFAULT_ORIENTATION_HEADING_DIFF_FAIL_RAD:
            blocking_reasons.append("orientation_heading_diff_high")
    if pose.get("heading_source_truthful") is not True:
        warnings.append("heading_source_not_verified")
    if reference_point.get("vehicle_reference_hard_gate_eligible") is not True:
        missing_fields.append("vehicle_reference_hard_gate_eligible")
        warnings.append("vehicle_reference_not_claim_grade")
    if _duplicate_timestamp_ratio_claim_blocking(channel, source):
        warnings.append("duplicate_localization_timestamps_detected")
        blocking_reasons.append("duplicate_localization_timestamps_claim_grade_blocked")

    if pose["heading_error_to_route_p95_rad"] is not None:
        heading_p95 = float(pose["heading_error_to_route_p95_rad"])
        if heading_p95 >= DEFAULT_HEADING_FAIL_RAD:
            blocking_reasons.append("heading_error_to_route_high")
        elif heading_p95 >= DEFAULT_HEADING_WARN_RAD:
            warnings.append("heading_error_to_route_elevated")
    if pose["heading_error_to_lane_p95_rad"] is not None:
        lane_heading_p95 = float(pose["heading_error_to_lane_p95_rad"])
        if lane_heading_p95 >= DEFAULT_LANE_HEADING_BLOCK_RAD:
            blocking_reasons.append("heading_error_to_lane_high")
        elif lane_heading_p95 >= DEFAULT_HEADING_WARN_RAD:
            warnings.append("heading_error_to_lane_elevated")

    if kinematics["yaw_rate_vs_heading_fd_p95_rad_s"] is not None:
        yaw_rate_delta = float(kinematics["yaw_rate_vs_heading_fd_p95_rad_s"])
        if yaw_rate_delta >= DEFAULT_YAW_RATE_WARN_RAD_S:
            warnings.append("yaw_rate_vs_heading_fd_suspect")
    if kinematics["velocity_norm_vs_chassis_speed_p95_mps"] is not None:
        speed_delta = float(kinematics["velocity_norm_vs_chassis_speed_p95_mps"])
        if speed_delta >= DEFAULT_SPEED_FAIL_MPS:
            blocking_reasons.append("velocity_chassis_speed_mismatch_high")
        elif speed_delta >= DEFAULT_SPEED_WARN_MPS:
            warnings.append("velocity_chassis_speed_mismatch_elevated")

    verdict_status = _verdict_status(
        channel_status=channel["status"],
        blocking_reasons=blocking_reasons,
        warnings=warnings,
        missing_fields=missing_fields,
    )
    verdict = {
        "status": verdict_status,
        "blocking_reasons": sorted(set(blocking_reasons)),
    }

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": _first_text(source, "run_id"),
        "route_id": (
            _first_text(source, "route_id")
            or _route_health_route_id(route_health)
            or _first_row_value(rows, "route_id")
        ),
        "backend": _first_text(source, "backend", "backend_name"),
        "channel": channel,
        "time": time,
        "frame_transform": frame,
        "reference_point": reference_point,
        "pose_consistency": pose,
        "lane_projection": lane_projection,
        "kinematics": kinematics,
        "status": status,
        "status_verdict": verdict_status,
        "claim_grade": verdict_status == "pass",
        "acceptance_checklist": _build_acceptance_checklist(
            channel=channel,
            time=time,
            frame_transform=frame,
            reference_point=reference_point,
            pose_consistency=pose,
            lane_projection=lane_projection,
            kinematics=kinematics,
            status=status,
            verdict=verdict,
            route_health=route_health,
            apollo_hdmap_projection=apollo_hdmap_projection,
        ),
        "missing_fields": sorted(set(missing_fields)),
        "warnings": sorted(set(warnings)),
        "source": dict(source),
        "verdict": verdict,
        "apollo_hdmap_projection": apollo_hdmap_projection,
        "hdmap_route_lateral_consistency": hdmap_route_lateral_consistency,
        "interpretation_boundary": (
            "This report checks GT localization contract evidence only. It does not prove Apollo "
            "planning/control capability, and insufficient localization evidence should be resolved "
            "before attributing curve, junction, or traffic-light failures to Apollo behavior."
        ),
    }


def analyze_localization_contract_files(
    *,
    timeseries_path: str | Path | None = None,
    channel_stats_path: str | Path | None = None,
    route_health_path: str | Path | None = None,
    hdmap_projection_path: str | Path | None = None,
    frame_transform_path: str | Path | None = None,
    vehicle_reference_path: str | Path | None = None,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    explicit_timeseries_path = timeseries_path is not None
    source = _resolve_inputs(
        run_dir=Path(run_dir).expanduser() if run_dir else None,
        timeseries_path=Path(timeseries_path).expanduser() if timeseries_path else None,
        channel_stats_path=Path(channel_stats_path).expanduser() if channel_stats_path else None,
        route_health_path=Path(route_health_path).expanduser() if route_health_path else None,
        hdmap_projection_path=Path(hdmap_projection_path).expanduser() if hdmap_projection_path else None,
    )
    timeseries = _read_timeseries(source.get("timeseries_path"))
    primary_timeseries = list(timeseries)
    run_dir_path = source.get("run_dir")
    if run_dir_path is not None and source.get("channel_stats_path") is None:
        generated_stats = normalize_channel_stats_for_run(run_dir_path)
        if generated_stats is not None:
            normalized_output = generated_stats.get("_normalized_output_path")
            output_path = generated_stats.get("_output_path")
            source["channel_stats_path"] = Path(str(normalized_output or output_path))
            source["channel_stats_selection_reason"] = "generated_normalized_channel_stats_from_run_artifacts"
    if run_dir_path is not None and not explicit_timeseries_path:
        debug_timeseries_path = _find_first(
            run_dir_path,
            ["artifacts/debug_timeseries.csv", "debug_timeseries.csv"],
        )
        debug_timeseries = _read_timeseries(debug_timeseries_path)
        primary_score = _localization_contract_evidence_score(timeseries)
        debug_score = _localization_contract_evidence_score(debug_timeseries)
        if debug_score > primary_score:
            debug_timeseries = _merge_route_context_by_sim_time(
                debug_timeseries,
                primary_timeseries,
            )
            timeseries = debug_timeseries
            source["timeseries_path"] = debug_timeseries_path
            source["timeseries_selection_reason"] = "debug_timeseries_has_stronger_localization_contract_fields"
    channel_stats = _read_json_optional(source.get("channel_stats_path"))
    route_health = _read_json_optional(source.get("route_health_path"))
    hdmap_projection_rows = read_apollo_hdmap_projection(source.get("hdmap_projection_path"))
    frame_transform = _load_frame_transform_optional(frame_transform_path)
    vehicle_reference = load_vehicle_reference(vehicle_reference_path) if vehicle_reference_path else None
    metadata = _read_run_metadata(source.get("run_dir"), route_health)
    metadata.update({key: str(value) for key, value in source.items() if value is not None})
    for artifact_key in ("cyber_bridge_stats_path", "ros2_gt_live_stats_path"):
        if source.get(artifact_key):
            metadata[artifact_key] = str(source[artifact_key])
    cyber_bridge_stats = _read_json_optional(source.get("cyber_bridge_stats_path"))
    ros2_gt_live_stats = _read_json_optional(source.get("ros2_gt_live_stats_path"))
    if cyber_bridge_stats is not None:
        metadata["cyber_bridge_stats"] = cyber_bridge_stats
    if ros2_gt_live_stats is not None:
        metadata["ros2_gt_live_stats"] = ros2_gt_live_stats
    return analyze_localization_contract(
        timeseries_rows=timeseries,
        channel_stats=channel_stats,
        route_health=route_health,
        hdmap_projection_rows=hdmap_projection_rows,
        frame_transform=frame_transform,
        vehicle_reference=vehicle_reference,
        source=metadata,
    )


def write_localization_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "localization_contract_report.json"
    summary_path = output_dir / "localization_contract_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(localization_contract_summary_md(report), encoding="utf-8")
    return {
        "localization_contract_report": str(report_path),
        "localization_contract_summary": str(summary_path),
    }


def ensure_localization_contract_report(
    run_dir: str | Path,
    *,
    refresh: bool = False,
    frame_transform_path: str | Path | None = DEFAULT_FRAME_TRANSFORM_PATH,
    vehicle_reference_path: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    report_dir = root / "analysis" / "localization_contract"
    report_path = report_dir / "localization_contract_report.json"
    existing = _find_first(
        root,
        [
            "analysis/localization_contract/localization_contract_report.json",
            "localization_contract_report.json",
        ],
    )
    if existing is not None and not refresh:
        report = _read_json_optional(existing) or {}
        return {
            "status": "existing",
            "path": str(existing),
            "report_status": _report_status(report),
        }
    if existing is not None and not _has_localization_regeneration_inputs(root):
        report_dir.mkdir(parents=True, exist_ok=True)
        if existing.resolve() != report_path.resolve():
            report_path.write_text(existing.read_text(encoding="utf-8"), encoding="utf-8")
            summary = existing.parent / "localization_contract_summary.md"
            if summary.exists():
                (report_dir / "localization_contract_summary.md").write_text(
                    summary.read_text(encoding="utf-8"),
                    encoding="utf-8",
                )
        report = _read_json_optional(report_path if report_path.exists() else existing) or {}
        return {
            "status": "existing_report_copied",
            "path": str(report_path if report_path.exists() else existing),
            "report_status": _report_status(report),
            "source_report": str(existing),
        }

    vehicle_reference = vehicle_reference_path or _default_vehicle_reference_path()
    report = analyze_localization_contract_files(
        run_dir=root,
        frame_transform_path=frame_transform_path if _path_exists(frame_transform_path) else None,
        vehicle_reference_path=vehicle_reference,
    )
    outputs = write_localization_contract_report(report, report_dir)
    return {
        "status": "generated",
        "path": outputs["localization_contract_report"],
        "summary_path": outputs["localization_contract_summary"],
        "report_status": _report_status(report),
    }


def ensure_localization_contract_reports_for_root(
    root: str | Path,
    *,
    refresh: bool = False,
    frame_transform_path: str | Path | None = DEFAULT_FRAME_TRANSFORM_PATH,
    vehicle_reference_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    base = Path(root).expanduser()
    run_dirs = [base] if (base / "summary.json").exists() else sorted({path.parent for path in base.rglob("summary.json")})
    return [
        ensure_localization_contract_report(
            path,
            refresh=refresh,
            frame_transform_path=frame_transform_path,
            vehicle_reference_path=vehicle_reference_path,
        )
        for path in run_dirs
    ]


def localization_contract_summary_md(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    channel = _mapping(report.get("channel"))
    reference_point = _mapping(report.get("reference_point"))
    pose = _mapping(report.get("pose_consistency"))
    hdmap_projection = _mapping(report.get("apollo_hdmap_projection"))
    hdmap_route_lateral = _mapping(report.get("hdmap_route_lateral_consistency"))
    time = _mapping(report.get("time"))
    kinematics = _mapping(report.get("kinematics"))
    checklist = _mapping(report.get("acceptance_checklist"))
    checklist_statuses = [
        f"{check_id}={_mapping(check).get('status')}"
        for check_id, check in checklist.items()
    ]
    lines = [
        "# Apollo Localization Contract Summary",
        "",
        f"- Run ID: `{report.get('run_id')}`",
        f"- Route ID: `{report.get('route_id')}`",
        f"- Backend: `{report.get('backend')}`",
        f"- Verdict: `{verdict.get('status')}`",
        f"- Blocking reasons: `{', '.join(verdict.get('blocking_reasons') or []) or 'none'}`",
        f"- Channel status: `{channel.get('status')}`",
        f"- Header frame_id: `{channel.get('header_frame_id')}`",
        f"- Measurement/header delta p95 ms: `{time.get('measurement_header_delta_ms_p95')}`",
        f"- Localization publish Hz: `{channel.get('publish_hz')}`",
        f"- Localization fresh sample Hz: `{channel.get('fresh_sample_hz')}`",
        f"- Localization fresh/publish count: `{channel.get('fresh_sample_count')}` / `{channel.get('publish_message_count')}`",
        f"- Localization republish policy: `{channel.get('republish_policy') or 'none'}`",
        f"- Vehicle reference confidence: `{reference_point.get('confidence')}`",
        f"- Vehicle reference hard-gate eligible: `{reference_point.get('vehicle_reference_hard_gate_eligible')}`",
        f"- Position uses VRP: `{reference_point.get('position_uses_vrp')}`",
        f"- Heading source: `{pose.get('heading_source')}` truthful=`{pose.get('heading_source_truthful')}`",
        f"- Heading error to route p95 rad: `{pose.get('heading_error_to_route_p95_rad')}`",
        f"- Apollo HDMap projection status: `{hdmap_projection.get('status')}`",
        f"- Apollo HDMap projection claim-grade: `{hdmap_projection.get('claim_grade')}`",
        f"- Apollo HDMap projection heading p95 rad: `{hdmap_projection.get('heading_error_p95_rad')}`",
        f"- Apollo HDMap projection lateral p95 m: `{hdmap_projection.get('lateral_error_p95_m')}`",
        f"- HDMap lateral vs route cross-track consistency: `{hdmap_route_lateral.get('status')}`",
        f"- HDMap/route lateral alignment mode: `{hdmap_route_lateral.get('alignment_mode')}`",
        f"- HDMap/route lateral delta p95 m: `{hdmap_route_lateral.get('best_abs_delta_p95_m')}`",
        f"- HDMap/route lateral interpretation: `{hdmap_route_lateral.get('interpretation')}`",
        f"- Yaw-rate vs heading finite-difference p95 rad/s: `{kinematics.get('yaw_rate_vs_heading_fd_p95_rad_s')}`",
        f"- Acceptance checklist: `{', '.join(checklist_statuses) or 'not evaluated'}`",
        f"- Missing fields: `{', '.join(report.get('missing_fields') or []) or 'none'}`",
        f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
        "",
        str(report.get("interpretation_boundary") or ""),
        "",
    ]
    return "\n".join(lines)


def _build_acceptance_checklist(
    *,
    channel: Mapping[str, Any],
    time: Mapping[str, Any],
    frame_transform: Mapping[str, Any],
    reference_point: Mapping[str, Any],
    pose_consistency: Mapping[str, Any],
    lane_projection: Mapping[str, Any],
    kinematics: Mapping[str, Any],
    status: Mapping[str, Any],
    verdict: Mapping[str, Any],
    route_health: Mapping[str, Any] | None,
    apollo_hdmap_projection: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        "localization_channel_health": _check(
            question="/apollo/localization/pose exists, rate is reasonable, and timestamps are monotonic.",
            status=_channel_check_status(channel),
            evidence_fields=[
                "channel.publish_message_count",
                "channel.fresh_sample_count",
                "channel.publish_hz",
                "channel.fresh_sample_hz",
                "channel.max_gap_ms",
                "channel.timestamp_non_decreasing",
                "channel.timestamp_strictly_increasing",
                "channel.sequence_monotonic",
                "channel.stale_count",
                "channel.header_frame_id",
            ],
            reason=f"channel.status={channel.get('status')}",
        ),
        "sim_time_time_base": _check(
            question="Localization timestamps use sim_time and measurement_time is available.",
            status=_sim_time_check_status(time, status),
            evidence_fields=[
                "time.time_base",
                "time.header_timestamp_sec_source",
                "time.measurement_time_source",
                "time.measurement_header_delta_ms_p95",
                "status.measurement_time_available",
            ],
            reason=f"time_base={time.get('time_base')}; measurement_time_available={status.get('measurement_time_available')}",
        ),
        "frame_transform_configured": _check(
            question="CARLA world to Apollo map frame transform is explicitly configured.",
            status=(
                "pass"
                if frame_transform.get("uses_configured_transform") is True
                and frame_transform.get("y_flip_or_axis_mapping_declared") is True
                else "insufficient_data"
            ),
            evidence_fields=[
                "frame_transform.uses_configured_transform",
                "frame_transform.y_flip_or_axis_mapping_declared",
                "frame_transform.affine_self_consistency_roundtrip_error_m_max",
                "frame_transform.map_alignment_verified",
                "frame_transform.lane_projection_verified",
                "frame_transform.reference_line_verified",
            ],
            reason=(
                f"uses_configured_transform={frame_transform.get('uses_configured_transform')}; "
                f"axis_mapping_declared={frame_transform.get('y_flip_or_axis_mapping_declared')}"
            ),
        ),
        "position_uses_vrp": _check(
            question="Apollo position uses rear axle center / VRP, not raw CARLA actor origin.",
            status=_position_reference_check_status(reference_point),
            evidence_fields=[
                "reference_point.configured_apollo_reference_point",
                "reference_point.source_reference_mode",
                "reference_point.published_localization_reference_mode",
                "reference_point.reference_conversion_detected",
                "reference_point.origin_to_vrp_carla_m",
                "reference_point.position_uses_vrp",
                "reference_point.configured_vehicle_reference_confidence",
                "reference_point.position_uses_vrp_evidence",
            ],
            reason=(
                f"position_uses_vrp={reference_point.get('position_uses_vrp')}; "
                f"reference_path_status={reference_point.get('reference_path_status')}; "
                f"confidence={reference_point.get('configured_vehicle_reference_confidence')}"
            ),
        ),
        "source_to_published_reference_explained": _check(
            question="The source localization reference and published Apollo reference are explicitly explained.",
            status=_reference_explanation_status(reference_point),
            evidence_fields=[
                "reference_point.source_reference_mode",
                "reference_point.published_localization_reference_mode",
                "reference_point.reference_conversion_method",
            ],
            reason=(
                f"source={reference_point.get('source_reference_mode')}; "
                f"published={reference_point.get('published_localization_reference_mode')}; "
                f"method={reference_point.get('reference_conversion_method')}"
            ),
        ),
        "heading_from_transformed_forward": _check(
            question="Heading source is truthfully declared and consistent with the published orientation.",
            status=(
                "pass"
                if pose_consistency.get("heading_source_truthful") is True
                else "insufficient_data"
            ),
            evidence_fields=["pose_consistency.heading_source", "pose_consistency.heading_source_truthful"],
            reason=(
                f"heading_source={pose_consistency.get('heading_source')}; "
                f"truthful={pose_consistency.get('heading_source_truthful')}"
            ),
        ),
        "rfu_to_enu_orientation": _check(
            question="Orientation declares Apollo RFU-to-ENU convention.",
            status="pass" if pose_consistency.get("orientation_convention") == "RFU_to_ENU" else "insufficient_data",
            evidence_fields=["pose_consistency.orientation_convention"],
            reason=f"orientation_convention={pose_consistency.get('orientation_convention')}",
        ),
        "decoded_orientation_consistency": _check(
            question="Quaternion orientation independently decodes to the published heading.",
            status=_bounded_metric_status(
                pose_consistency.get("orientation_heading_diff_p95_rad"),
                warn_threshold=DEFAULT_ORIENTATION_HEADING_DIFF_FAIL_RAD / 2.0,
                fail_threshold=DEFAULT_ORIENTATION_HEADING_DIFF_FAIL_RAD,
            ),
            evidence_fields=[
                "pose_consistency.orientation_heading_diff_p95_rad",
                "pose_consistency.orientation_heading_diff_source",
                "pose_consistency.orientation_quaternion_available",
            ],
            reason=(
                f"orientation_heading_diff_p95_rad={pose_consistency.get('orientation_heading_diff_p95_rad')}; "
                f"source={pose_consistency.get('orientation_heading_diff_source')}"
            ),
        ),
        "kinematics_frame_and_units": _check(
            question="Velocity, acceleration, and angular velocity use declared Apollo-frame semantics and units.",
            status=_kinematics_frame_unit_status(kinematics),
            evidence_fields=[
                "kinematics.linear_velocity_frame",
                "kinematics.angular_velocity_unit",
                "kinematics.physical_linear_acceleration_available",
                "kinematics.vrf_fields_available",
            ],
            reason=(
                f"linear_velocity_frame={kinematics.get('linear_velocity_frame')}; "
                f"angular_velocity_unit={kinematics.get('angular_velocity_unit')}; "
                f"physical_linear_acceleration_available={kinematics.get('physical_linear_acceleration_available')}; "
                f"vrf_fields_available={kinematics.get('vrf_fields_available')}"
            ),
        ),
        "chassis_speed_consistency": _check(
            question="Localization speed is consistent with chassis speed.",
            status=_bounded_metric_status(
                kinematics.get("velocity_norm_vs_chassis_speed_p95_mps"),
                warn_threshold=DEFAULT_SPEED_WARN_MPS,
                fail_threshold=DEFAULT_SPEED_FAIL_MPS,
            ),
            evidence_fields=["kinematics.velocity_norm_vs_chassis_speed_p95_mps"],
            reason=f"velocity_norm_vs_chassis_speed_p95_mps={kinematics.get('velocity_norm_vs_chassis_speed_p95_mps')}",
        ),
        "lane_projection": _check(
            question="Ego pose has diagnostic lane projection evidence.",
            status=_lane_projection_status(lane_projection),
            evidence_fields=[
                "lane_projection.lane_projection_available",
                "lane_projection.lane_projection_inside_ratio",
                "lane_projection.lane_projection_distance_p95_m",
                "lane_projection.lane_heading_error_p95_rad",
            ],
            reason=(
                f"available={lane_projection.get('lane_projection_available')}; "
                f"inside_ratio={lane_projection.get('lane_projection_inside_ratio')}; "
                f"heading_p95={lane_projection.get('lane_heading_error_p95_rad')}"
            ),
        ),
        "reference_line_projection": _check(
            question="Apollo reference-line projection is explicitly verified.",
            status=_reference_line_projection_status(
                frame_transform,
                pose_consistency,
                route_health,
                apollo_hdmap_projection,
            ),
            evidence_fields=[
                "route_health",
                "pose_consistency.heading_error_to_route_p95_rad",
                "frame_transform.reference_line_verified",
                "apollo_hdmap_projection.status",
                "apollo_hdmap_projection.claim_grade",
                "apollo_hdmap_projection.heading_error_p95_rad",
                "apollo_hdmap_projection.lateral_error_p95_m",
                "pose_consistency.spawn_projection_error_m",
            ],
            reason=(
                f"route_health_present={route_health is not None}; "
                f"heading_error_to_route_p95_rad={pose_consistency.get('heading_error_to_route_p95_rad')}; "
                f"reference_line_verified={frame_transform.get('reference_line_verified')}; "
                f"apollo_hdmap_projection_status={apollo_hdmap_projection.get('status')}; "
                f"apollo_hdmap_projection_claim_grade={apollo_hdmap_projection.get('claim_grade')}; "
                f"spawn_projection_error_m={pose_consistency.get('spawn_projection_error_m')}"
            ),
        ),
        "acceleration_semantics": _check(
            question="Acceleration fields are not mistaken for claim-grade evidence when zero-filled.",
            status=_acceleration_semantics_status(kinematics),
            evidence_fields=[
                "kinematics.linear_acceleration_source",
                "kinematics.physical_linear_acceleration_available",
                "kinematics.linear_acceleration_claim_grade",
            ],
            reason=(
                f"source={kinematics.get('linear_acceleration_source')}; "
                f"physical_available={kinematics.get('physical_linear_acceleration_available')}; "
                f"claim_grade={kinematics.get('linear_acceleration_claim_grade')}"
            ),
        ),
        "uncertainty_status_policy": _check(
            question="Localization uncertainty and status fields are available or explicitly treated as policy gaps.",
            status=_uncertainty_status_policy(status),
            evidence_fields=[
                "status.uncertainty_available",
                "status.uncertainty_policy",
                "status.msf_status_available",
                "status.msf_status_policy",
                "status.sensor_status_available",
                "status.sensor_status_policy",
            ],
            reason=(
                f"uncertainty={status.get('uncertainty_available')}; "
                f"uncertainty_policy={status.get('uncertainty_policy')}; "
                f"msf_status={status.get('msf_status_available')}; "
                f"msf_status_policy={status.get('msf_status_policy')}; "
                f"sensor_status={status.get('sensor_status_available')}; "
                f"sensor_status_policy={status.get('sensor_status_policy')}"
            ),
        ),
        "natural_driving_gate_ready": _check(
            question="Localization evidence does not block natural-driving hard pass.",
            status=(
                "pass"
                if verdict.get("status") in {"pass", "warn"} and not verdict.get("blocking_reasons")
                else str(verdict.get("status") or "insufficient_data")
            ),
            evidence_fields=["verdict.status", "verdict.blocking_reasons"],
            reason=(
                f"verdict.status={verdict.get('status')}; "
                f"blocking_reasons={verdict.get('blocking_reasons') or []}"
            ),
        ),
    }


def _check(
    *,
    question: str,
    status: str,
    evidence_fields: list[str],
    reason: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "question": question,
        "evidence_fields": evidence_fields,
        "reason": reason,
    }


def _channel_check_status(channel: Mapping[str, Any]) -> str:
    status = str(channel.get("status") or "insufficient_data")
    if status not in {"pass", "warn"}:
        return status
    if channel.get("header_frame_id") != "map":
        return "fail"
    if channel.get("timestamp_non_decreasing") is not True or channel.get("sequence_monotonic") is False:
        return "fail"
    if not isinstance(channel.get("fresh_sample_count"), (int, float)) or float(channel.get("fresh_sample_count") or 0.0) <= 0:
        return "fail"
    return status


def _sim_time_check_status(time: Mapping[str, Any], status: Mapping[str, Any]) -> str:
    if time.get("time_base") != "sim_time":
        return "fail"
    if not time.get("header_timestamp_sec_source"):
        return "insufficient_data"
    if status.get("measurement_time_available") is not True:
        return "insufficient_data"
    delta = _number_or_none(time.get("measurement_header_delta_ms_p95"))
    if delta is not None and delta > DEFAULT_MEASUREMENT_HEADER_DELTA_FAIL_MS:
        return "fail"
    return "pass"


def _position_reference_check_status(reference_point: Mapping[str, Any]) -> str:
    if reference_point.get("reference_path_status") == "fail":
        return "fail"
    if reference_point.get("position_uses_vrp") is False:
        return "fail"
    if reference_point.get("position_uses_vrp") is not True:
        return "insufficient_data"
    if reference_point.get("vehicle_reference_hard_gate_eligible") is not True:
        return "insufficient_data"
    if reference_point.get("configured_vehicle_reference_confidence") == "assumed":
        return "warn"
    if reference_point.get("reference_path_status") == "warn":
        return "warn"
    return "pass"


def _reference_explanation_status(reference_point: Mapping[str, Any]) -> str:
    if reference_point.get("reference_path_status") == "fail":
        return "fail"
    if not reference_point.get("source_reference_mode") or not reference_point.get("published_localization_reference_mode"):
        return "insufficient_data"
    if reference_point.get("reference_path_status") == "warn":
        return "warn"
    return "pass"


def _kinematics_frame_unit_status(kinematics: Mapping[str, Any]) -> str:
    if kinematics.get("linear_velocity_frame") != "apollo_map":
        return "fail"
    if str(kinematics.get("angular_velocity_unit") or "unknown") == "unknown":
        return "warn"
    if str(kinematics.get("linear_acceleration_source") or kinematics.get("acceleration_source") or "") == "zero_filled":
        return "warn"
    if kinematics.get("vrf_fields_available") is not True:
        return "warn"
    return "pass"


def _lane_projection_status(lane_projection: Mapping[str, Any]) -> str:
    if lane_projection.get("lane_projection_available") is not True:
        return "insufficient_data"
    heading_status = _bounded_metric_status(
        lane_projection.get("lane_heading_error_p95_rad"),
        warn_threshold=DEFAULT_HEADING_WARN_RAD,
        fail_threshold=DEFAULT_HEADING_FAIL_RAD,
    )
    distance_status = _bounded_metric_status(
        lane_projection.get("lane_projection_distance_p95_m"),
        warn_threshold=1.0,
        fail_threshold=3.0,
    )
    statuses = {heading_status, distance_status}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def _reference_line_projection_status(
    frame_transform: Mapping[str, Any],
    pose_consistency: Mapping[str, Any],
    route_health: Mapping[str, Any] | None,
    apollo_hdmap_projection: Mapping[str, Any],
) -> str:
    projection_status = str(apollo_hdmap_projection.get("status") or "insufficient_data")
    if projection_status == "fail":
        return "fail"
    if apollo_hdmap_projection.get("claim_grade") is True:
        return "pass"
    if apollo_hdmap_projection.get("official_source_available") is True and projection_status == "warn":
        return "warn"
    if route_health is None:
        return "insufficient_data"
    if frame_transform.get("reference_line_verified") is not True:
        return "insufficient_data"
    route_status = _bounded_metric_status(
        pose_consistency.get("heading_error_to_route_p95_rad"),
        warn_threshold=DEFAULT_HEADING_WARN_RAD,
        fail_threshold=DEFAULT_HEADING_FAIL_RAD,
    )
    spawn_status = "pass" if pose_consistency.get("spawn_projection_error_m") is not None else "insufficient_data"
    statuses = {route_status, spawn_status}
    if "fail" in statuses:
        return "fail"
    if "insufficient_data" in statuses:
        return "insufficient_data"
    if "warn" in statuses:
        return "warn"
    return "pass"


def _acceleration_semantics_status(kinematics: Mapping[str, Any]) -> str:
    source = str(kinematics.get("linear_acceleration_source") or kinematics.get("acceleration_source") or "")
    if source == "zero_filled":
        return "warn"
    if kinematics.get("linear_acceleration_field_present") is not True:
        return "insufficient_data"
    if kinematics.get("linear_acceleration_claim_grade") is True:
        return "pass"
    return "warn"


def _uncertainty_status_policy(status: Mapping[str, Any]) -> str:
    available = [
        status.get("uncertainty_available"),
        status.get("msf_status_available"),
        status.get("sensor_status_available"),
    ]
    policies = [
        status.get("uncertainty_policy"),
        status.get("msf_status_policy"),
        status.get("sensor_status_policy"),
    ]
    if all(available):
        return "pass"
    if all(value or policy for value, policy in zip(available, policies)):
        return "warn"
    return "warn"


def _bounded_metric_status(value: Any, *, warn_threshold: float, fail_threshold: float) -> str:
    number = _number_or_none(value)
    if number is None:
        return "insufficient_data"
    absolute = abs(number)
    if absolute >= fail_threshold:
        return "fail"
    if absolute >= warn_threshold:
        return "warn"
    return "pass"


def _analyze_channel(
    channel_stats: Mapping[str, Any] | None,
    rows: list[dict[str, Any]],
    source: Mapping[str, Any],
    missing_fields: list[str],
    warnings: list[str],
    blocking_reasons: list[str],
) -> dict[str, Any]:
    result = build_localization_channel_stats(
        channel_stats=channel_stats,
        timeseries_rows=rows,
        bridge_stats=_mapping(source.get("cyber_bridge_stats")),
        min_fresh_hz=DEFAULT_LOCALIZATION_FRESH_HZ,
    )
    warnings.extend(str(item) for item in result.pop("_warnings", []) if item)
    blocking_reasons.extend(str(item) for item in result.pop("_blocking_reasons", []) if item)

    if result.get("channel_stats_source") is None:
        missing_fields.append("channel_stats")
    if result.get("fresh_sample_hz") is None:
        missing_fields.append("fresh_sample_hz")
    cyber_stats = _mapping(source.get("cyber_bridge_stats"))
    cyber_pose = _mapping(cyber_stats.get("last_pose_debug"))
    cyber_localization = _mapping(cyber_stats.get("localization"))
    result["header_frame_id"] = (
        _first_row_value(rows, "localization_frame_id")
        or cyber_pose.get("localization_frame_id")
        or cyber_localization.get("frame_id")
    )
    return result


def _analyze_time(
    rows: list[dict[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    chassis_delta_ms_direct = _p95_abs_values(rows, "localization_chassis_timestamp_delta_ms")
    chassis_delta = _p95_abs_delta(rows, "localization_timestamp", "chassis_timestamp")
    if chassis_delta is None:
        if chassis_delta_ms_direct is None:
            missing_fields.append("localization_timestamp_or_chassis_timestamp")
    planning_age = _p95_planning_age(rows)
    if planning_age is None:
        missing_fields.append("planning_timestamp_or_planning_message_age_ms")
    measurement_time_available = _has_any(rows, "measurement_time")
    if not measurement_time_available:
        warnings.append("measurement_time_field_missing")
    time_base = _first_row_value(rows, "localization_time_base") or "sim_time"
    measurement_header_delta_ms = _measurement_header_delta_ms_p95(rows)
    return {
        "time_base": time_base,
        "header_timestamp_sec_source": (
            "localization_header_timestamp_sec"
            if _has_any(rows, "localization_header_timestamp_sec")
            else ("localization_timestamp" if _has_any(rows, "localization_timestamp") else None)
        ),
        "measurement_time_source": "measurement_time" if measurement_time_available else None,
        "measurement_header_delta_ms_p95": measurement_header_delta_ms,
        "timestamp_chassis_delta_ms_p95": (
            chassis_delta_ms_direct if chassis_delta_ms_direct is not None else (chassis_delta * 1000.0 if chassis_delta is not None else None)
        ),
        "timestamp_planning_age_ms_p95": planning_age,
    }


def _analyze_frame_transform(
    frame_transform: ApolloFrameTransform | None,
    rows: list[dict[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    if frame_transform is None:
        missing_fields.append("frame_transform")
        warnings.append("frame_transform_missing")
        return {
            "source_frame": None,
            "target_frame": None,
            "transform_id": None,
            "uses_configured_transform": False,
            "y_flip_or_axis_mapping_declared": False,
            "affine_self_consistency_roundtrip_error_m_max": None,
            "roundtrip_error_m_max": None,
            "map_alignment_verified": False,
            "lane_projection_verified": False,
            "reference_line_verified": False,
        }
    reference_line_verified = False
    return {
        "source_frame": frame_transform.source_frame,
        "target_frame": frame_transform.target_frame,
        "transform_id": frame_transform.map_name,
        "uses_configured_transform": True,
        "y_flip_or_axis_mapping_declared": isinstance(frame_transform.y_flip, bool),
        "affine_self_consistency_roundtrip_error_m_max": _roundtrip_error_m_max(rows, frame_transform),
        "roundtrip_error_m_max": _roundtrip_error_m_max(rows, frame_transform),
        "map_alignment_verified": False,
        "lane_projection_verified": False,
        "reference_line_verified": reference_line_verified,
    }


def _analyze_reference_point(
    vehicle_reference: VehicleReferenceConfig | None,
    source: Mapping[str, Any],
    missing_fields: list[str],
    warnings: list[str],
    blocking_reasons: list[str],
) -> dict[str, Any]:
    cyber_stats = _mapping(source.get("cyber_bridge_stats"))
    ros2_stats = _mapping(source.get("ros2_gt_live_stats"))
    cyber_pose = _mapping(cyber_stats.get("last_pose_debug"))
    cyber_localization = _mapping(cyber_stats.get("localization"))
    ros2_reference = _mapping(ros2_stats.get("reference"))
    source_reference_mode = _optional_text(ros2_reference.get("localization_reference_mode"))
    published_reference_mode = _optional_text(
        cyber_pose.get("localization_reference_mode") or cyber_localization.get("reference_mode")
    )
    apollo_control_state_reference = _optional_text(
        cyber_pose.get("apollo_control_state_reference")
        or cyber_localization.get("apollo_control_state_reference")
        or ros2_reference.get("apollo_control_state_reference")
    )
    localization_back_offset_m = _number_or_none(
        cyber_pose.get("localization_back_offset_m") or cyber_localization.get("back_offset_m")
    )
    expected_offset = (
        _number_or_none(cyber_localization.get("expected_rear_axle_back_offset_m"))
        or _number_or_none(source.get("expected_rear_axle_back_offset_m"))
        or 1.4235
    )

    configured_reference = None
    configured_confidence = None
    configured_hard_gate_eligible = False
    origin_to_vrp = None
    actor_origin_definition = None
    if vehicle_reference is None:
        missing_fields.append("vehicle_reference")
        warnings.append("vehicle_reference_missing")
    else:
        configured_reference = vehicle_reference.apollo_reference_point
        configured_confidence = vehicle_reference.confidence
        configured_hard_gate_eligible = bool(vehicle_reference.hard_gate_eligible)
        actor_origin_definition = vehicle_reference.carla_actor_origin_definition
        origin_to_vrp = vehicle_reference.origin_to_vrp_carla.to_dict()
    if configured_confidence == "assumed":
        warnings.append("vehicle_reference_confidence_assumed")

    position_uses_vrp: bool | None = None
    evidence: list[str] = []
    reference_conversion_detected = False
    conversion_method = None
    reference_path_status = "insufficient_data"

    if source_reference_mode == "vehicle_origin" and published_reference_mode == "rear_axle":
        if localization_back_offset_m is not None and expected_offset is not None:
            if abs(localization_back_offset_m - expected_offset) <= 0.10:
                position_uses_vrp = True
                reference_conversion_detected = True
                conversion_method = "source_vehicle_origin_to_published_rear_axle"
                reference_path_status = "pass"
                evidence.extend(["ros2_gt_live_stats.reference", "cyber_bridge_stats.last_pose_debug"])
            else:
                position_uses_vrp = None
                reference_path_status = "warn"
                warnings.append("reference_conversion_back_offset_mismatch")
        else:
            position_uses_vrp = None
            reference_path_status = "warn"
            warnings.append("reference_conversion_back_offset_missing")
    elif published_reference_mode == "rear_axle":
        if localization_back_offset_m is not None:
            position_uses_vrp = True
            reference_conversion_detected = source_reference_mode == "vehicle_origin"
            conversion_method = "published_rear_axle_runtime_evidence"
            reference_path_status = "warn" if source_reference_mode is None else "pass"
            evidence.append("cyber_bridge_stats.last_pose_debug")
            if source_reference_mode is None:
                warnings.append("source_reference_mode_missing")
        else:
            position_uses_vrp = None
            reference_path_status = "warn"
            warnings.append("published_rear_axle_back_offset_missing")
    elif source_reference_mode == "vehicle_origin" and published_reference_mode == "vehicle_origin":
        position_uses_vrp = False
        reference_path_status = "fail"
        blocking_reasons.append("localization_position_not_vrp")
        evidence.extend(["ros2_gt_live_stats.reference", "cyber_bridge_stats.last_pose_debug"])
    elif source_reference_mode is None and published_reference_mode is None:
        position_uses_vrp = None
        reference_path_status = "insufficient_data"
        warnings.append("reference_runtime_artifacts_missing")
        missing_fields.append("runtime_reference_mode")
    else:
        position_uses_vrp = None
        reference_path_status = "warn"
        warnings.append("reference_path_ambiguous")

    runtime_confidence = (
        cyber_pose.get("vehicle_reference_confidence")
        or cyber_localization.get("vehicle_reference_confidence")
    )
    runtime_hard_gate = _bool_or_none(
        cyber_pose.get("vehicle_reference_hard_gate_eligible")
        if cyber_pose.get("vehicle_reference_hard_gate_eligible") is not None
        else cyber_localization.get("vehicle_reference_hard_gate_eligible")
    )
    vehicle_reference_hard_gate_eligible = bool(configured_hard_gate_eligible)
    if runtime_confidence not in {None, "", "verified"}:
        vehicle_reference_hard_gate_eligible = False
        warnings.append(f"vehicle_reference_confidence_{runtime_confidence}")
    if runtime_hard_gate is False:
        vehicle_reference_hard_gate_eligible = False

    return {
        "apollo_position_reference": configured_reference,
        "carla_actor_origin_definition": actor_origin_definition,
        "origin_to_vrp_carla_m": origin_to_vrp,
        "position_uses_vrp": position_uses_vrp,
        "confidence": configured_confidence,
        "vehicle_reference_hard_gate_eligible": vehicle_reference_hard_gate_eligible,
        "configured_apollo_reference_point": configured_reference,
        "configured_vehicle_reference_confidence": configured_confidence,
        "runtime_vehicle_reference_confidence": runtime_confidence,
        "runtime_vehicle_reference_hard_gate_eligible": runtime_hard_gate,
        "source_reference_mode": source_reference_mode,
        "published_localization_reference_mode": published_reference_mode,
        "apollo_control_state_reference": apollo_control_state_reference,
        "localization_back_offset_m": localization_back_offset_m,
        "expected_rear_axle_back_offset_m": expected_offset,
        "reference_conversion_detected": reference_conversion_detected,
        "reference_conversion_method": conversion_method,
        "position_uses_vrp_evidence": evidence,
        "reference_path_status": reference_path_status,
    }


def _analyze_pose_consistency(
    rows: list[dict[str, Any]],
    route_health: Mapping[str, Any] | None,
    missing_fields: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    orientation_diff_values, orientation_diff_source = _orientation_heading_diff_values(rows)
    orientation_diff = _percentile([abs(value) for value in orientation_diff_values], 0.95)
    if orientation_diff is None:
        fallback_orientation_diff = _p95_abs_values(rows, "self_reported_orientation_heading_diff")
        if fallback_orientation_diff is not None:
            orientation_diff = fallback_orientation_diff
            orientation_diff_source = "self_reported"
            warnings.append("orientation_diff_self_reported_without_quaternion_fields")
        else:
            missing_fields.append("orientation_heading_diff")
    heading_route = _p95_abs_values(rows, "heading_error")
    if heading_route is None:
        heading_route = _route_health_metric(route_health, "heading_error_p95_rad")
    if heading_route is None:
        missing_fields.append("heading_error")
    heading_lane = _lane_heading_error_p95(rows)
    if heading_lane is None:
        missing_fields.append("lane_heading_error")
    spawn_projection = _max_abs_values(rows, "spawn_projection_error_m")
    if spawn_projection is None:
        spawn_projection = _spawn_alignment_distance(route_health)
    if route_health is None:
        warnings.append("route_health_missing_for_pose_consistency")
    heading_source = _first_row_value(rows, "heading_source")
    truthful_sources = {
        "transformed_forward_vector",
        "odom_quaternion_yaw_after_frame_transform",
    }
    return {
        "heading_source": heading_source,
        "heading_source_truthful": heading_source in truthful_sources,
        "orientation_convention": _first_row_value(rows, "orientation_convention"),
        "orientation_quaternion_available": _has_orientation_quaternion(rows),
        "orientation_heading_diff_source": orientation_diff_source,
        "orientation_heading_diff_p95_rad": orientation_diff,
        "heading_error_to_route_p95_rad": heading_route,
        "heading_error_to_lane_p95_rad": heading_lane,
        "spawn_projection_error_m": spawn_projection,
    }


def _analyze_lane_projection(
    rows: list[dict[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    inside_values = [_bool_value(_value(row, "lane_inside")) for row in rows if _value(row, "lane_inside") not in {None, ""}]
    lane_dist = _p95_abs_values(rows, "lane_dist_m")
    lateral_error = _p95_abs_values(rows, "lane_lateral_error_m")
    heading_error = _lane_heading_error_p95(rows)
    available = bool(inside_values) or lane_dist is not None or lateral_error is not None or heading_error is not None
    if not available:
        missing_fields.append("lane_projection")
        return {
            "lane_projection_available": False,
            "lane_projection_inside_ratio": None,
            "lane_projection_distance_p95_m": None,
            "lane_lateral_error_p95_m": None,
            "lane_heading_error_p95_rad": None,
            "lane_projection_source": None,
            "reference_line_verified": False,
            "reference_line_note": "diagnostic lane projection is not Apollo reference-line verification",
        }
    return {
        "lane_projection_available": True,
        "lane_projection_inside_ratio": sum(1 for value in inside_values if value) / len(inside_values) if inside_values else None,
        "lane_projection_distance_p95_m": lane_dist,
        "lane_lateral_error_p95_m": lateral_error,
        "lane_heading_error_p95_rad": heading_error,
        "lane_projection_source": "bridge_debug_lane_projection",
        "reference_line_verified": False,
        "reference_line_note": "diagnostic lane projection is not Apollo reference-line verification",
    }


def _analyze_hdmap_route_lateral_consistency(
    rows: list[dict[str, Any]],
    hdmap_projection_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    route_samples: list[tuple[float, float, float | None]] = []
    for row in rows:
        timestamp = _number_or_none(_value(row, "sim_time"))
        cross_track = _number_or_none(_value(row, "route_cross_track_error_m"))
        route_s = _number_or_none(_value(row, "route_s"))
        if timestamp is None or cross_track is None:
            continue
        route_samples.append((timestamp, cross_track, route_s))
    route_samples.sort(key=lambda item: item[0])

    projection_samples: list[tuple[float, float]] = []
    for row in hdmap_projection_rows:
        if str(row.get("source") or "") != "apollo_hdmap_api":
            continue
        if str(row.get("status") or "").lower() != "ok":
            continue
        timestamp = _number_or_none(row.get("timestamp"))
        lateral = _number_or_none(row.get("lateral_error_m") if row.get("lateral_error_m") is not None else row.get("projection_l"))
        if timestamp is None or lateral is None:
            continue
        projection_samples.append((timestamp, lateral))
    projection_samples.sort(key=lambda item: item[0])

    if not route_samples or not projection_samples:
        return {
            "available": False,
            "status": "insufficient_data",
            "sample_count": 0,
            "alignment_mode": None,
            "best_abs_delta_p95_m": None,
            "best_abs_delta_max_m": None,
            "projection_lateral_p95_m": None,
            "route_cross_track_p95_m": None,
            "projection_lateral_start_m": None,
            "projection_lateral_end_m": None,
            "route_cross_track_start_m": None,
            "route_cross_track_end_m": None,
            "route_s_start_m": None,
            "route_s_end_m": None,
            "interpretation": "route_cross_track_or_projection_samples_missing",
        }

    pairs: list[tuple[float, float, float, float | None]] = []
    search_index = 0
    for timestamp, projection_lateral in projection_samples:
        while search_index + 1 < len(route_samples) and route_samples[search_index + 1][0] <= timestamp:
            search_index += 1
        candidates = [route_samples[search_index]]
        if search_index + 1 < len(route_samples):
            candidates.append(route_samples[search_index + 1])
        nearest = min(candidates, key=lambda item: abs(item[0] - timestamp))
        if abs(nearest[0] - timestamp) <= 0.10:
            pairs.append((timestamp, projection_lateral, nearest[1], nearest[2]))

    if not pairs:
        return {
            "available": False,
            "status": "insufficient_data",
            "sample_count": 0,
            "alignment_mode": None,
            "best_abs_delta_p95_m": None,
            "best_abs_delta_max_m": None,
            "projection_lateral_p95_m": None,
            "route_cross_track_p95_m": None,
            "projection_lateral_start_m": None,
            "projection_lateral_end_m": None,
            "route_cross_track_start_m": None,
            "route_cross_track_end_m": None,
            "route_s_start_m": None,
            "route_s_end_m": None,
            "interpretation": "timestamp_join_failed",
        }

    projection_values = [item[1] for item in pairs]
    route_values = [item[2] for item in pairs]
    same_sign_deltas = [projection - route for _, projection, route, _ in pairs]
    opposite_sign_deltas = [projection + route for _, projection, route, _ in pairs]
    same_p95 = _p95_abs_numbers(same_sign_deltas)
    opposite_p95 = _p95_abs_numbers(opposite_sign_deltas)
    same_max = max(abs(value) for value in same_sign_deltas)
    opposite_max = max(abs(value) for value in opposite_sign_deltas)

    if opposite_p95 is not None and (same_p95 is None or opposite_p95 <= same_p95):
        alignment_mode = "opposite_sign"
        best_p95 = opposite_p95
        best_max = opposite_max
    else:
        alignment_mode = "same_sign"
        best_p95 = same_p95
        best_max = same_max

    projection_p95 = _p95_abs_numbers(projection_values)
    route_p95 = _p95_abs_numbers(route_values)
    status = "pass" if best_p95 is not None and best_p95 <= 0.10 else "warn" if best_p95 is not None and best_p95 <= 0.30 else "fail"
    if status == "pass" and projection_p95 is not None and projection_p95 >= 0.50 and route_p95 is not None and route_p95 >= 0.50:
        interpretation = "hdmap_lateral_matches_route_cross_track_actual_lateral_drift"
    elif status == "pass":
        interpretation = "hdmap_lateral_matches_route_cross_track"
    else:
        interpretation = "hdmap_lateral_does_not_match_route_cross_track_check_transform_or_lane_equivalence"

    route_s_values = [item[3] for item in pairs if item[3] is not None]
    return {
        "available": True,
        "status": status,
        "sample_count": len(pairs),
        "alignment_mode": alignment_mode,
        "same_sign_abs_delta_p95_m": same_p95,
        "opposite_sign_abs_delta_p95_m": opposite_p95,
        "best_abs_delta_p95_m": best_p95,
        "best_abs_delta_max_m": best_max,
        "projection_lateral_p95_m": projection_p95,
        "route_cross_track_p95_m": route_p95,
        "projection_lateral_start_m": projection_values[0],
        "projection_lateral_end_m": projection_values[-1],
        "route_cross_track_start_m": route_values[0],
        "route_cross_track_end_m": route_values[-1],
        "route_s_start_m": route_s_values[0] if route_s_values else None,
        "route_s_end_m": route_s_values[-1] if route_s_values else None,
        "interpretation": interpretation,
    }


def _merge_route_context_by_sim_time(
    preferred_rows: list[dict[str, Any]],
    route_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not preferred_rows or not route_rows:
        return preferred_rows
    if _has_any(preferred_rows, "route_cross_track_error_m") and _has_any(preferred_rows, "route_s"):
        return preferred_rows

    route_context: list[tuple[float, Mapping[str, Any]]] = []
    for row in route_rows:
        timestamp = _number_or_none(_value(row, "sim_time"))
        if timestamp is not None:
            route_context.append((timestamp, row))
    route_context.sort(key=lambda item: item[0])
    if not route_context:
        return preferred_rows

    context_fields = (
        "route_id",
        "route_s",
        "cross_track_error",
        "cross_track_error_m",
        "route_cross_track_error_m",
        "route_heading",
        "heading_error",
    )
    merged: list[dict[str, Any]] = []
    search_index = 0
    for row in preferred_rows:
        updated = dict(row)
        timestamp = _number_or_none(_value(updated, "sim_time"))
        if timestamp is None:
            merged.append(updated)
            continue
        while search_index + 1 < len(route_context) and route_context[search_index + 1][0] <= timestamp:
            search_index += 1
        candidates = [route_context[search_index]]
        if search_index + 1 < len(route_context):
            candidates.append(route_context[search_index + 1])
        nearest_time, nearest_row = min(candidates, key=lambda item: abs(item[0] - timestamp))
        if abs(nearest_time - timestamp) <= 0.10:
            for field in context_fields:
                if updated.get(field) in {None, ""} and nearest_row.get(field) not in {None, ""}:
                    updated[field] = nearest_row.get(field)
        merged.append(updated)
    return merged


def _analyze_kinematics(
    rows: list[dict[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    localization_speed_available = _has_any(rows, "localization_speed_mps")
    chassis_speed_available = _has_any(rows, "chassis_speed_mps")
    velocity_delta = _p95_abs_values(rows, "velocity_norm_vs_chassis_speed_mps")
    if velocity_delta is None:
        velocity_delta = _p95_abs_delta(rows, "localization_speed_mps", "chassis_speed_mps")
    if velocity_delta is None:
        missing_fields.append("localization_speed_or_chassis_speed_mps")
    yaw_delta = _p95_abs_values(rows, "yaw_rate_vs_heading_fd_rad_s")
    if yaw_delta is None:
        yaw_delta = _p95_abs_delta(rows, "ego_yaw_rate", "heading_fd_yaw_rate")
    if yaw_delta is None:
        yaw_delta = _p95_abs_yaw_rate_vs_heading_finite_difference(rows)
    if yaw_delta is None:
        missing_fields.append("ego_yaw_rate_or_heading_fd_yaw_rate")
    angular_unit = _first_row_value(rows, "angular_velocity_unit")
    if not angular_unit:
        angular_unit = "unknown"
        warnings.append("angular_velocity_unit_unknown")
    elif str(angular_unit) == "unknown":
        warnings.append("angular_velocity_unit_unknown")
    acceleration_source = _first_row_value(rows, "acceleration_source")
    acceleration_source = _preferred_acceleration_source(rows) or acceleration_source
    if not acceleration_source and _has_any(rows, "acceleration"):
        acceleration_source = "unknown"
    linear_accel_field_present = _has_any(rows, "acceleration") or _bool_field_available(rows, "linear_acceleration_available")
    physical_linear_accel_available = bool(linear_accel_field_present and str(acceleration_source or "") not in {"zero_filled", "missing"})
    linear_accel_claim_grade = bool(physical_linear_accel_available and str(acceleration_source or "") in {"carla_actor", "finite_difference"})
    if str(acceleration_source or "") == "zero_filled":
        warnings.append("acceleration_zero_filled")
    linear_accel_vrf_present = _has_any(rows, "linear_acceleration_vrf") or _bool_field_available(rows, "linear_acceleration_vrf_available")
    angular_vrf_present = _has_any(rows, "angular_velocity_vrf") or _bool_field_available(rows, "angular_velocity_vrf_available")
    return {
        "linear_velocity_frame": "apollo_map",
        "localization_speed_mps_available": localization_speed_available,
        "chassis_speed_mps_available": chassis_speed_available,
        "velocity_norm_vs_chassis_speed_p95_mps": velocity_delta,
        "localization_chassis_timestamp_delta_ms_p95": _p95_abs_values(rows, "localization_chassis_timestamp_delta_ms"),
        "angular_velocity_unit": angular_unit,
        "ego_yaw_rate_available": _has_any(rows, "ego_yaw_rate"),
        "heading_finite_difference_yaw_rate_available": _has_heading_finite_difference(rows),
        "yaw_rate_vs_heading_fd_p95_rad_s": yaw_delta,
        "linear_acceleration_field_present": linear_accel_field_present,
        "linear_acceleration_source": acceleration_source or "missing",
        "physical_linear_acceleration_available": physical_linear_accel_available,
        "linear_acceleration_claim_grade": linear_accel_claim_grade,
        "acceleration_available": physical_linear_accel_available,
        "vrf_fields_available": linear_accel_vrf_present or angular_vrf_present,
        "vrf_axis_convention": "RFU" if (linear_accel_vrf_present or angular_vrf_present) else None,
        "vrf_claim_grade": bool(angular_vrf_present and str(angular_unit) == "rad_per_s"),
        "acceleration_source": acceleration_source,
        "linear_acceleration_vrf_field_present": linear_accel_vrf_present,
        "angular_velocity_vrf_field_present": angular_vrf_present,
        "linear_acceleration_vrf_available": _bool_field_available(rows, "linear_acceleration_vrf_available"),
        "angular_velocity_vrf_available": _bool_field_available(rows, "angular_velocity_vrf_available"),
    }


def _analyze_status(
    rows: list[dict[str, Any]],
    missing_fields: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    result = {
        "uncertainty_available": _has_any(rows, "uncertainty"),
        "uncertainty_policy": _first_row_value(rows, "uncertainty_policy"),
        "measurement_time_available": _has_any(rows, "measurement_time"),
        "msf_status_available": _has_any(rows, "msf_status"),
        "msf_status_policy": _first_row_value(rows, "msf_status_policy"),
        "sensor_status_available": _has_any(rows, "sensor_status"),
        "sensor_status_policy": _first_row_value(rows, "sensor_status_policy"),
    }
    policy_by_available_field = {
        "uncertainty_available": "uncertainty_policy",
        "msf_status_available": "msf_status_policy",
        "sensor_status_available": "sensor_status_policy",
    }
    for field, available in result.items():
        if not field.endswith("_available"):
            continue
        if not available:
            if policy_by_available_field.get(field) and result.get(policy_by_available_field[field]):
                warnings.append(f"{field.removesuffix('_available')}_explicit_policy_gap")
                continue
            missing_fields.append(field.replace("_available", ""))
    if not result["measurement_time_available"]:
        warnings.append("measurement_time_unavailable")
    return result


def _verdict_status(
    *,
    channel_status: str,
    blocking_reasons: list[str],
    warnings: list[str],
    missing_fields: list[str],
) -> str:
    if blocking_reasons or channel_status == "fail":
        return "fail"
    if "channel_stats" in missing_fields or "timeseries" in missing_fields:
        return "insufficient_data"
    if "measurement_time" in missing_fields or "vehicle_reference_hard_gate_eligible" in missing_fields:
        return "insufficient_data"
    if warnings or channel_status == "warn" or missing_fields:
        return "warn"
    return "pass"


def _resolve_inputs(
    *,
    run_dir: Path | None,
    timeseries_path: Path | None,
    channel_stats_path: Path | None,
    route_health_path: Path | None,
    hdmap_projection_path: Path | None,
) -> dict[str, Path | None]:
    source = {"run_dir": run_dir}
    if run_dir is not None:
        source["timeseries_path"] = timeseries_path or _find_first(
            run_dir,
            ["timeseries.csv", "timeseries.jsonl", "artifacts/timeseries.csv", "artifacts/timeseries.jsonl"],
        )
        source["channel_stats_path"] = channel_stats_path or _find_first(
            run_dir,
            [str(path.relative_to(run_dir)) for path in channel_stats_candidate_paths(run_dir)],
        )
        source["route_health_path"] = route_health_path or _find_first(
            run_dir,
            ["analysis/route_health/route_health.json", "route_health.json", "artifacts/route_health.json"],
        )
        source["hdmap_projection_path"] = hdmap_projection_path or _find_first(
            run_dir,
            [
                "artifacts/apollo_hdmap_projection.jsonl",
                "apollo_hdmap_projection.jsonl",
                "analysis/apollo_hdmap_projection/apollo_hdmap_projection.json",
            ],
        )
        source["cyber_bridge_stats_path"] = _find_first(
            run_dir,
            ["artifacts/cyber_bridge_stats.json", "cyber_bridge_stats.json"],
        )
        source["ros2_gt_live_stats_path"] = _find_first(
            run_dir,
            ["artifacts/ros2_gt_live_stats.json", "ros2_gt_live_stats.json"],
        )
    else:
        source["timeseries_path"] = timeseries_path
        source["channel_stats_path"] = channel_stats_path
        source["route_health_path"] = route_health_path
        source["hdmap_projection_path"] = hdmap_projection_path
        source["cyber_bridge_stats_path"] = None
        source["ros2_gt_live_stats_path"] = None
    return source


def _read_timeseries(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix.lower() == ".jsonl":
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
        return rows
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json_optional(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _has_localization_contract_evidence(rows: list[dict[str, Any]]) -> bool:
    return _localization_contract_evidence_score(rows) > 0


def _localization_contract_evidence_score(rows: list[dict[str, Any]]) -> int:
    """Score strong localization evidence without letting generic ego fields win.

    Main run timeseries often contains P0 ego fields such as ``ego_speed``. Those
    are useful for route-health, but they are not proof that the localization
    publisher emitted Apollo-like timestamps/orientation/reference metadata.
    """

    return sum(
        1
        for field in LOCALIZATION_CONTRACT_STRONG_EVIDENCE_FIELDS
        if _has_any(rows, field)
    )


def _load_frame_transform_optional(path: str | Path | None) -> ApolloFrameTransform | None:
    if path is None:
        return None
    payload = yaml.safe_load(Path(path).expanduser().read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"{path} must contain a YAML mapping")
    return ApolloFrameTransform.from_mapping(payload)


def _read_run_metadata(run_dir: Path | None, route_health: Mapping[str, Any] | None) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    if run_dir is None:
        if route_health and route_health.get("route_id"):
            metadata["route_id"] = route_health.get("route_id")
        return metadata
    for name in ("manifest.json", "summary.json"):
        payload = _read_json_optional(run_dir / name)
        if not payload:
            continue
        for key in ("run_id", "route_id", "backend", "backend_name"):
            if payload.get(key) and key not in metadata:
                metadata[key] = payload.get(key)
    if route_health and route_health.get("route_id") and "route_id" not in metadata:
        metadata["route_id"] = route_health.get("route_id")
    return metadata


def _find_first(root: Path, candidates: Iterable[str]) -> Path | None:
    for candidate in candidates:
        path = root / candidate
        if path.exists():
            return path
    return None


def _has_localization_regeneration_inputs(root: Path) -> bool:
    stats_path = _find_first(
        root,
        [
            "artifacts/cyber_bridge_stats.json",
            "cyber_bridge_stats.json",
            "artifacts/ros2_gt_live_stats.json",
            "ros2_gt_live_stats.json",
        ],
    )
    if stats_path is not None:
        return True
    timeseries_path = _find_first(
        root,
        [
            "artifacts/debug_timeseries.csv",
            "debug_timeseries.csv",
            "timeseries.csv",
            "timeseries.jsonl",
            "artifacts/timeseries.csv",
            "artifacts/timeseries.jsonl",
        ],
    )
    return _localization_contract_evidence_score(_read_timeseries(timeseries_path)) > 0


def _report_status(report: Mapping[str, Any]) -> str | None:
    verdict = report.get("verdict")
    if isinstance(verdict, Mapping) and verdict.get("status"):
        return str(verdict.get("status"))
    if report.get("status"):
        return str(report.get("status"))
    return None


def _path_exists(path: str | Path | None) -> bool:
    return path is not None and Path(path).expanduser().exists()


def _default_vehicle_reference_path() -> Path | None:
    for candidate in (DEFAULT_VERIFIED_VEHICLE_REFERENCE_PATH, DEFAULT_EXAMPLE_VEHICLE_REFERENCE_PATH):
        if candidate.exists():
            return candidate
    return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_text(mapping: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if value not in {None, ""}:
            return str(value)
    return None


def _route_health_route_id(route_health: Mapping[str, Any] | None) -> str | None:
    if isinstance(route_health, Mapping) and route_health.get("route_id"):
        return str(route_health.get("route_id"))
    return None


def _first_row_value(rows: list[dict[str, Any]], canonical_field: str) -> Any:
    for row in rows:
        value = _value(row, canonical_field)
        if value not in {None, ""}:
            return value
    return None


def _preferred_acceleration_source(rows: list[dict[str, Any]]) -> str | None:
    priority = {
        "finite_difference": 5,
        "carla_actor": 5,
        "measured": 5,
        "initial_sample_missing_previous_velocity": 2,
        "stale_timestamp_republish": 2,
        "unknown": 1,
        "zero_filled": 0,
        "missing": 0,
    }
    best_source: str | None = None
    best_score = -1
    for row in rows:
        value = _value(row, "acceleration_source")
        if value in {None, ""}:
            continue
        source = str(value)
        score = priority.get(source, 1)
        if score > best_score:
            best_source = source
            best_score = score
    return best_source


def _has_any(rows: list[dict[str, Any]], canonical_field: str) -> bool:
    return _first_row_value(rows, canonical_field) is not None


def _bool_field_available(rows: list[dict[str, Any]], canonical_field: str) -> bool:
    value = _first_row_value(rows, canonical_field)
    if isinstance(value, bool):
        return value
    if value in {None, ""}:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "available"}


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "inside"}


def _value(row: Mapping[str, Any], canonical_field: str) -> Any:
    for key in ALIASES.get(canonical_field, [canonical_field]):
        value = row.get(key)
        if value not in {None, ""}:
            return value
    return None


def _numeric_values(rows: list[dict[str, Any]], canonical_field: str) -> list[float]:
    values = []
    for row in rows:
        number = _number_or_none(_value(row, canonical_field))
        if number is not None:
            values.append(number)
    return values


def _p95_abs_values(rows: list[dict[str, Any]], canonical_field: str) -> float | None:
    return _percentile([abs(value) for value in _numeric_values(rows, canonical_field)], 0.95)


def _p95_abs_numbers(values: list[float]) -> float | None:
    return _percentile([abs(value) for value in values], 0.95)


def _lane_heading_error_p95(rows: list[dict[str, Any]]) -> float | None:
    rad_values = _numeric_values(rows, "lane_heading_error")
    if rad_values:
        return _p95_abs_numbers(rad_values)
    deg_values = _numeric_values(rows, "lane_heading_error_deg")
    if deg_values:
        return _p95_abs_numbers([math.radians(value) for value in deg_values])
    return None


def _has_orientation_quaternion(rows: list[dict[str, Any]]) -> bool:
    return all(
        _has_any(rows, field)
        for field in (
            "localization_orientation_qx",
            "localization_orientation_qy",
            "localization_orientation_qz",
            "localization_orientation_qw",
        )
    )


def _orientation_heading_diff_values(rows: list[dict[str, Any]]) -> tuple[list[float], str | None]:
    values: list[float] = []
    for row in rows:
        qx = _number_or_none(_value(row, "localization_orientation_qx"))
        qy = _number_or_none(_value(row, "localization_orientation_qy"))
        qz = _number_or_none(_value(row, "localization_orientation_qz"))
        qw = _number_or_none(_value(row, "localization_orientation_qw"))
        heading = _number_or_none(_value(row, "localization_heading"))
        if None in {qx, qy, qz, qw, heading}:
            continue
        decoded_heading = _decode_rfu_to_enu_heading_from_quat(qx, qy, qz, qw)
        values.append(_normalize_angle(decoded_heading - heading))
    return values, "decoded_quaternion" if values else None


def _decode_rfu_to_enu_heading_from_quat(qx: float, qy: float, qz: float, qw: float) -> float:
    # Apollo RFU forward axis is local +Y.
    fx, fy, _ = _rotate_vector_by_quat(qx, qy, qz, qw, 0.0, 1.0, 0.0)
    return math.atan2(fy, fx)


def _rotate_vector_by_quat(
    qx: float,
    qy: float,
    qz: float,
    qw: float,
    vx: float,
    vy: float,
    vz: float,
) -> tuple[float, float, float]:
    tx = 2.0 * (qy * vz - qz * vy)
    ty = 2.0 * (qz * vx - qx * vz)
    tz = 2.0 * (qx * vy - qy * vx)
    return (
        vx + qw * tx + (qy * tz - qz * ty),
        vy + qw * ty + (qz * tx - qx * tz),
        vz + qw * tz + (qx * ty - qy * tx),
    )


def _normalize_angle(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def _max_abs_values(rows: list[dict[str, Any]], canonical_field: str) -> float | None:
    values = [abs(value) for value in _numeric_values(rows, canonical_field)]
    return max(values) if values else None


def _p95_abs_delta(rows: list[dict[str, Any]], lhs_field: str, rhs_field: str) -> float | None:
    deltas = []
    for row in rows:
        lhs = _number_or_none(_value(row, lhs_field))
        rhs = _number_or_none(_value(row, rhs_field))
        if lhs is not None and rhs is not None:
            deltas.append(abs(lhs - rhs))
    return _percentile(deltas, 0.95)


def _measurement_header_delta_ms_p95(rows: list[dict[str, Any]]) -> float | None:
    deltas = []
    for row in rows:
        measurement = _number_or_none(_value(row, "measurement_time"))
        header = _number_or_none(_value(row, "localization_header_timestamp_sec"))
        if measurement is not None and header is not None:
            deltas.append(abs(measurement - header) * 1000.0)
    return _percentile(deltas, 0.95)


def _duplicate_timestamp_ratio_claim_blocking(channel: Mapping[str, Any], source: Mapping[str, Any]) -> bool:
    ratio = _number_or_none(channel.get("duplicate_timestamp_ratio"))
    if ratio is None or ratio < DEFAULT_DUPLICATE_TIMESTAMP_WARN_RATIO:
        return False
    return not _is_non_claim_debug_run(source)


def _is_non_claim_debug_run(source: Mapping[str, Any]) -> bool:
    if _bool_or_none(source.get("claim_grade")) is False:
        return True
    return any(_bool_or_none(source.get(key)) is True for key in ("non_claim_debug_run", "debug_run"))


def _has_heading_finite_difference(rows: list[dict[str, Any]]) -> bool:
    return _p95_abs_yaw_rate_vs_heading_finite_difference(rows) is not None


def _p95_abs_yaw_rate_vs_heading_finite_difference(rows: list[dict[str, Any]]) -> float | None:
    samples: list[tuple[float, float, float]] = []
    last_timestamp: float | None = None
    for row in rows:
        timestamp = _number_or_none(_value(row, "localization_header_timestamp_sec"))
        if timestamp is None:
            timestamp = _number_or_none(_value(row, "measurement_time"))
        heading = _number_or_none(_value(row, "localization_heading"))
        yaw_rate = _number_or_none(_value(row, "ego_yaw_rate"))
        if timestamp is None or heading is None or yaw_rate is None:
            continue
        # Repeated timestamp rows are stale republishes; they are channel
        # evidence, but not valid finite-difference samples.
        if last_timestamp is not None and abs(timestamp - last_timestamp) <= 1e-9:
            continue
        samples.append((timestamp, heading, yaw_rate))
        last_timestamp = timestamp
    if len(samples) < 2:
        return None

    deltas: list[float] = []
    prev_t, prev_heading, _ = samples[0]
    for timestamp, heading, yaw_rate in samples[1:]:
        dt = timestamp - prev_t
        if dt <= 1e-9:
            prev_t, prev_heading = timestamp, heading
            continue
        heading_fd_yaw_rate = _normalize_angle(heading - prev_heading) / dt
        deltas.append(abs(yaw_rate - heading_fd_yaw_rate))
        prev_t, prev_heading = timestamp, heading
    return _percentile(deltas, 0.95)


def _p95_planning_age(rows: list[dict[str, Any]]) -> float | None:
    ages = _numeric_values(rows, "planning_message_age_ms")
    if ages:
        return _percentile([abs(value) for value in ages], 0.95)
    deltas = []
    for row in rows:
        sim_time = _number_or_none(_value(row, "sim_time"))
        planning_timestamp = _number_or_none(_value(row, "planning_timestamp"))
        if sim_time is not None and planning_timestamp is not None:
            deltas.append(abs(sim_time - planning_timestamp) * 1000.0)
    return _percentile(deltas, 0.95)


def _route_health_metric(route_health: Mapping[str, Any] | None, field: str) -> float | None:
    if not isinstance(route_health, Mapping):
        return None
    run_metrics = route_health.get("run_metrics")
    if isinstance(run_metrics, Mapping):
        return _number_or_none(run_metrics.get(field))
    return None


def _spawn_alignment_distance(route_health: Mapping[str, Any] | None) -> float | None:
    if not isinstance(route_health, Mapping):
        return None
    geometry = route_health.get("route_geometry")
    if not isinstance(geometry, Mapping):
        return None
    spawn_alignment = geometry.get("spawn_alignment")
    if not isinstance(spawn_alignment, Mapping):
        return None
    return _number_or_none(spawn_alignment.get("distance_m"))


def _roundtrip_error_m_max(rows: list[dict[str, Any]], frame_transform: ApolloFrameTransform) -> float | None:
    points = []
    for row in rows:
        x = _number_or_none(_value(row, "ego_x"))
        y = _number_or_none(_value(row, "ego_y"))
        z = _number_or_none(_value(row, "ego_z")) or 0.0
        if x is not None and y is not None:
            points.append((x, y, z))
    if not points:
        return None
    # The inverse mirrors frame_transform.carla_point_to_apollo and is used only
    # as a self-consistency check for the configured affine transform.
    c = math.cos(frame_transform.yaw_rad)
    s = math.sin(frame_transform.yaw_rad)
    errors = []
    for x, y, z in points:
        apollo = carla_point_to_apollo(Vector3(x, y, z), frame_transform)
        ax = apollo.x
        ay = apollo.y
        az = apollo.z
        ux = (ax - frame_transform.tx) / frame_transform.scale
        uy = (ay - frame_transform.ty) / frame_transform.scale
        uz = (az - frame_transform.tz) / frame_transform.scale
        rx = ux * c + uy * s
        ry = -ux * s + uy * c
        carla_y = -ry if frame_transform.y_flip else ry
        errors.append(((rx - x) ** 2 + (carla_y - y) ** 2 + (uz - z) ** 2) ** 0.5)
    return max(errors) if errors else None


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _number_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in {None, ""}:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "available", "pass"}:
        return True
    if text in {"0", "false", "no", "n", "unavailable", "fail"}:
        return False
    return None


def _optional_text(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)
