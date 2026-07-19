from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import yaml

from algo.adapters.apollo import ApolloAdapter, _rewrite_apollo_lane_speed_limits
from carla_testbed.config.loader import load_config
from tools.apollo10_cyber_bridge.actuator_mapping import (
    ActuatorCalibration,
    load_actuator_calibration,
    resolve_calibration_path,
)


def test_apollo_lane_speed_limit_fill_preserves_existing_and_is_idempotent() -> None:
    map_text = """lane {
  id { id: \"with_limit\" }
  speed_limit: 12.5
}
lane {
  id { id: \"missing_limit\" }
}
"""

    patched, stats = _rewrite_apollo_lane_speed_limits(
        map_text,
        replace_existing_mps=None,
        fill_missing=True,
        missing_default_mps=23.61,
    )

    assert "speed_limit: 12.5" in patched
    assert "speed_limit: 23.610000000000" in patched
    assert stats == {
        "lane_count": 2,
        "explicit_speed_limit_count_before": 1,
        "missing_lane_speed_limit_count_before": 1,
        "inserted_missing_lane_speed_limit_count": 1,
        "existing_speed_limit_replaced_count": 0,
        "explicit_speed_limit_count_after": 2,
        "missing_lane_speed_limit_count_after": 0,
        "old_unique_speed_limits_mps": [12.5],
    }

    second_pass, second_stats = _rewrite_apollo_lane_speed_limits(
        patched,
        replace_existing_mps=None,
        fill_missing=True,
        missing_default_mps=23.61,
    )
    assert second_pass == patched
    assert second_stats["inserted_missing_lane_speed_limit_count"] == 0
    assert second_stats["missing_lane_speed_limit_count_after"] == 0


def test_apollo_lane_speed_limit_override_and_fill_are_scoped_to_lane_blocks() -> None:
    map_text = """header {
  speed_limit: 99.0
}
lane {
  speed_limit: 10.0
}
lane {
  id { id: \"missing\" }
}
"""

    patched, stats = _rewrite_apollo_lane_speed_limits(
        map_text,
        replace_existing_mps=20.0,
        fill_missing=True,
        missing_default_mps=15.0,
    )

    assert "speed_limit: 99.0" in patched
    assert "speed_limit: 20.000000000000" in patched
    assert "speed_limit: 15.000000000000" in patched
    assert stats["existing_speed_limit_replaced_count"] == 1
    assert stats["inserted_missing_lane_speed_limit_count"] == 1


def test_apollo_adapter_reports_missing_lane_speed_limit_fill(tmp_path: Path) -> None:
    map_file = tmp_path / "base_map.txt"
    original_text = """lane {
  id { id: \"existing\" }
  speed_limit: 12.5
}
lane {
  id { id: \"connector\" }
}
"""
    map_file.write_text(original_text, encoding="utf-8")
    bridge_cfg = tmp_path / "bridge.yaml"
    bridge_cfg.write_text(
        yaml.safe_dump({"bridge": {"map_file": str(map_file)}}),
        encoding="utf-8",
    )
    profile = {
        "algo": {
            "apollo": {
                "map_speed_limit": {
                    "enabled": False,
                    "fill_missing_lane_speed_limits": True,
                    "missing_lane_default_mps": 23.61,
                }
            }
        }
    }
    run_dir = tmp_path / "run"

    ApolloAdapter()._patch_apollo_map_speed_limit(profile, bridge_cfg, run_dir)

    report = json.loads(
        (run_dir / "artifacts" / "apollo_map_speed_limit_patch.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["ok"] is True
    assert report["patched"] is True
    assert report["lane_count"] == 2
    assert report["explicit_speed_limit_count_before"] == 1
    assert report["inserted_missing_lane_speed_limit_count"] == 1
    assert report["missing_lane_speed_limit_count_after"] == 0
    assert report["existing_speed_limit_replaced_count"] == 0
    assert Path(report["backup_file"]).read_text(encoding="utf-8") == original_text


def test_apollo_adapter_inherits_missing_lane_limit_from_clean_map(tmp_path: Path) -> None:
    map_file = tmp_path / "base_map.txt"
    map_file.write_text(
        """lane {
  speed_limit: 11.175972222222223
}
lane {
  id { id: \"connector\" }
}
""",
        encoding="utf-8",
    )
    bridge_cfg = tmp_path / "bridge.yaml"
    bridge_cfg.write_text(
        yaml.safe_dump({"bridge": {"map_file": str(map_file)}}),
        encoding="utf-8",
    )
    profile = {
        "algo": {
            "apollo": {
                "map_speed_limit": {
                    "enabled": False,
                    "fill_missing_lane_speed_limits": True,
                    "missing_lane_default_mps": "inherit_existing",
                }
            }
        }
    }
    run_dir = tmp_path / "run"

    ApolloAdapter()._patch_apollo_map_speed_limit(profile, bridge_cfg, run_dir)

    report = json.loads(
        (run_dir / "artifacts" / "apollo_map_speed_limit_patch.json").read_text(
            encoding="utf-8"
        )
    )
    assert map_file.read_text(encoding="utf-8").count("11.175972222222") == 2
    assert report["missing_lane_default_mps_effective"] == 11.175972222222223
    assert report["missing_lane_default_source"] == "minimum_positive_existing_lane_speed_limit"


def test_apollo_adapter_preserves_steering_percent_normalization_in_bridge_config(tmp_path: Path) -> None:
    profile = {
        "run": {
            "ego_id": "hero",
            "profile_name": "claim_probe",
            "claim_profile": True,
            "materialization_probe": True,
        },
        "runtime": {"carla": {"host": "127.0.0.1", "port": 2000}},
        "sim": {"map": "Town01"},
        "algo": {
            "apollo": {
                "apollo_root": "",
                "docker": {"enabled": False},
                "bridge": {
                    "map_file": "configs/io/maps/Town01/base_map.txt",
                    "localization_time_source": "cyber_time",
                    "obstacle_time_source": "cyber_time",
                    "obstacle_publish_rate_hz": 10.0,
                    "obstacle_publish_policy": "source_fresh",
                    "obstacle_alignment_policy": "wait_for_exact_source_time",
                    "artifact_async_write_enabled": True,
                    "artifact_async_queue_max_rows": 8192,
                    "artifact_async_queue_soft_limit_rows": 4096,
                    "artifact_flush_interval_s": 0.5,
                    "artifact_flush_max_pending_rows": 200,
                    "artifact_stats_flush_interval_s": 1.0,
                    "stage5_debug_artifact_sample_stride": 10,
                    "reference_debug_artifact_sample_stride": 9,
                    "control_debug_artifact_sample_stride": 7,
                    "control_debug_artifact_sample_strides": {
                        "apollo_control_raw": 1,
                    },
                    "claim_evidence_artifact_sample_stride": 3,
                    "localization_acceleration_source": "carla_feedback",
                    "carla_feedback": {"state_source": "ros_objects_json"},
                    "localization_acceleration_filter": {
                        "enabled": True,
                        "alpha": 0.35,
                        "max_abs_mps2": 4.0,
                        "max_delta_mps2": 1.0,
                    },
                    "localization_authored_initial_state_transition": {
                        "mode": "marker_zero_once",
                        "marker_path": (
                            "fixed_scene_gate_initial_state_materialization.json"
                        ),
                        "required_status": "pass",
                        "max_world_frame_delta": 2,
                        "max_sim_time_delta_s": 0.11,
                        "speed_tolerance_mps": 0.5,
                    },
                    "claim_grade": {
                        "enabled": True,
                        "stale_world_frame_policy": "skip",
                        "localization_publish_policy": "once_per_new_sim_frame",
                        "chassis_publish_policy": "once_per_new_sim_frame",
                    },
                },
                "transport_mode": "carla_direct",
                "direct_bridge": {
                    "control_apply_mode": "frame_flush_only",
                    "stale_world_frame_policy": "always_republish",
                },
                "control_mapping": {
                    "steer_scale": 0.25,
                    "steering_percent_normalization": "legacy_double_percent",
                    "throttle_brake_mutual_exclusion_enabled": True,
                    "throttle_brake_hysteresis_frames": 3,
                    "throttle_brake_min_command": 0.02,
                    "require_valid_planning_before_first_publish": True,
                    "require_exact_planning_match_before_first_publish": True,
                    "require_nonfallback_planning_before_first_publish": True,
                    "require_fixed_scene_handover_before_publish": True,
                },
            }
        },
    }

    ApolloAdapter().prepare(profile, tmp_path)

    bridge_cfg_path = tmp_path / "artifacts" / "apollo_bridge_effective.yaml"
    bridge_cfg = yaml.safe_load(bridge_cfg_path.read_text(encoding="utf-8"))
    bridge = bridge_cfg["bridge"]
    run_cfg = bridge_cfg["run"]
    control_mapping = bridge_cfg["bridge"]["control_mapping"]
    claim_grade = bridge_cfg["bridge"]["claim_grade"]
    assert run_cfg["profile_name"] == "claim_probe"
    assert run_cfg["claim_profile"] is True
    assert run_cfg["materialization_probe"] is True
    assert bridge["claim_profile"] is True
    assert bridge["materialization_probe"] is True
    assert bridge["localization_time_source"] == "cyber_time"
    assert bridge["obstacle_time_source"] == "cyber_time"
    assert bridge["obstacle_publish_rate_hz"] == 10.0
    assert bridge["obstacle_publish_policy"] == "source_fresh"
    assert bridge["obstacle_alignment_policy"] == "wait_for_exact_source_time"
    assert bridge["artifact_async_write_enabled"] is True
    assert bridge["artifact_async_queue_max_rows"] == 8192
    assert bridge["artifact_async_queue_soft_limit_rows"] == 4096
    assert bridge["artifact_flush_interval_s"] == 0.5
    assert bridge["artifact_flush_max_pending_rows"] == 200
    assert bridge["artifact_stats_flush_interval_s"] == 1.0
    assert bridge["stage5_debug_artifact_sample_stride"] == 10
    assert bridge["reference_debug_artifact_sample_stride"] == 9
    assert bridge["control_debug_artifact_sample_stride"] == 7
    assert bridge["control_debug_artifact_sample_strides"] == {
        "apollo_control_raw": 1,
    }
    assert bridge["claim_evidence_artifact_sample_stride"] == 3
    assert bridge["localization_acceleration_source"] == "carla_feedback"
    assert bridge["carla_feedback"]["state_source"] == "ros_objects_json"
    assert bridge["localization_acceleration_filter"] == {
        "enabled": True,
        "alpha": 0.35,
        "max_abs_mps2": 4.0,
        "max_delta_mps2": 1.0,
    }
    assert bridge["localization_authored_initial_state_transition"] == {
        "mode": "marker_zero_once",
        "marker_path": "fixed_scene_gate_initial_state_materialization.json",
        "required_status": "pass",
        "max_world_frame_delta": 2,
        "max_sim_time_delta_s": 0.11,
        "speed_tolerance_mps": 0.5,
    }
    assert control_mapping["steer_scale"] == 0.25
    assert control_mapping["steering_percent_normalization"] == "legacy_double_percent"
    assert control_mapping["throttle_brake_mutual_exclusion_enabled"] is True
    assert control_mapping["throttle_brake_hysteresis_frames"] == 3
    assert control_mapping["throttle_brake_min_command"] == 0.02
    assert control_mapping["require_valid_planning_before_first_publish"] is True
    assert control_mapping["require_exact_planning_match_before_first_publish"] is True
    assert control_mapping["require_nonfallback_planning_before_first_publish"] is True
    assert control_mapping["require_fixed_scene_handover_before_publish"] is True
    assert claim_grade["enabled"] is True
    assert claim_grade["stale_world_frame_policy"] == "skip"
    direct_bridge = bridge_cfg["algo"]["apollo"]["direct_bridge"]
    assert direct_bridge["control_apply_mode"] == "frame_flush_only"
    assert direct_bridge["stale_world_frame_policy"] == "always_republish"


def test_apollo_adapter_preserves_auto_localization_back_offset_for_bridge_resolution(tmp_path: Path) -> None:
    profile = {
        "run": {
            "ego_id": "hero",
            "profile_name": "phase1_vrp_auto_probe",
            "claim_profile": True,
        },
        "runtime": {"carla": {"host": "127.0.0.1", "port": 2000}},
        "sim": {"map": "straight_road_for_baguang"},
        "algo": {
            "apollo": {
                "apollo_root": "",
                "docker": {"enabled": False},
                "bridge": {
                    "localization_back_offset_m": "auto",
                    "vehicle_reference_path": "configs/vehicles/ego_vehicle_reference.verified.yaml",
                    "map_file": "${APOLLO_MAP_ROOT}/straight_road_for_baguang/base_map.txt",
                },
            }
        },
    }

    ApolloAdapter().prepare(profile, tmp_path)

    bridge_cfg = yaml.safe_load(
        (tmp_path / "artifacts" / "apollo_bridge_effective.yaml").read_text(encoding="utf-8")
    )
    bridge = bridge_cfg["bridge"]
    assert bridge["localization_back_offset_m"] == "auto"
    assert bridge["vehicle_reference_path"] == "configs/vehicles/ego_vehicle_reference.verified.yaml"


def test_apollo_adapter_materializes_cyber_clock_into_runtime_bridge_config(tmp_path: Path) -> None:
    profile = {
        "run": {
            "ego_id": "hero",
            "profile_name": "mock_clock_probe",
            "claim_profile": False,
        },
        "runtime": {"carla": {"host": "127.0.0.1", "port": 2000}},
        "sim": {"map": "Town01"},
        "algo": {
            "apollo": {
                "apollo_root": "",
                "docker": {"enabled": False},
                "bridge": {
                    "cyber_clock": {
                        "enabled": True,
                        "mode": "mock",
                        "channel": "/clock",
                        "publish_phase": "after_state",
                        "odom_queue_depth": 64,
                    }
                },
            }
        },
    }

    ApolloAdapter().prepare(profile, tmp_path)

    bridge_cfg = yaml.safe_load(
        (tmp_path / "artifacts" / "apollo_bridge_effective.yaml").read_text(encoding="utf-8")
    )
    assert bridge_cfg["bridge"]["cyber_clock"] == {
        "enabled": True,
        "mode": "mock",
        "channel": "/clock",
        "publish_phase": "after_state",
        "odom_queue_depth": 64,
    }


def test_baguang_atomic_clock_candidate_isolates_cpu_prediction_resource_mode() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_"
            "control_raw_full_capture_extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]

    assert apollo["prediction"]["compute_device"] == "cpu"
    assert apollo["bridge"]["carla_feedback"]["state_source"] == "ros_objects_json"
    assert apollo["bridge"]["front_obstacle_behavior"]["actor_probe_enabled"] is False
    assert config.scenario.params["gt"]["objects_hz"] == 20.0
    assert apollo["bridge"]["cyber_clock"] == {
        "enabled": True,
        "mode": "mock",
        "channel": "/clock",
        "publish_phase": "after_state",
        "odom_queue_depth": 64,
    }
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_route_only_claim_probe_keeps_artifact_io_out_of_gt_publish_hot_path() -> None:
    config_path = Path("configs/io/examples/town01_apollo_route_only_claim_probe.yaml")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    bridge = payload["algo"]["apollo"]["bridge"]
    assert bridge["artifact_async_queue_max_rows"] <= 2048
    assert bridge["artifact_async_queue_soft_limit_rows"] <= 1024
    assert bridge["artifact_flush_interval_s"] == 0.0
    assert bridge["artifact_flush_max_pending_rows"] == 0
    assert bridge["artifact_stats_flush_interval_s"] == 0.0
    assert bridge["control_debug_artifact_sample_stride"] >= 50
    assert bridge["reference_debug_artifact_sample_stride"] >= 10
    assert bridge["claim_evidence_artifact_sample_stride"] >= 10


def test_nominal_lane_keep_profile_keeps_artifact_io_out_of_gt_publish_hot_path() -> None:
    config_path = Path(
        "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    )
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    bridge = payload["algo"]["apollo"]["bridge"]
    apollo = payload["algo"]["apollo"]
    route_health = payload["scenario"]["route_health"]
    control_mapping = payload["algo"]["apollo"]["control_mapping"]
    control_lqr = payload["algo"]["apollo"]["control_lqr"]
    control_lon = payload["algo"]["apollo"]["control_lon"]
    map_speed_limit = payload["algo"]["apollo"]["map_speed_limit"]
    vehicle_param = payload["algo"]["apollo"]["vehicle_param"]

    assert bridge["artifact_flush_interval_s"] == 0.0
    assert bridge["artifact_flush_max_pending_rows"] == 0
    # Keep per-row artifact flushing out of the GT publish hot path, but allow
    # lightweight periodic stats materialization so the Phase 1 wrapper can read
    # routing/planning/control counters before bridge shutdown completes.
    assert bridge["artifact_stats_flush_interval_s"] == 2.0
    assert bridge["stage5_debug_artifact_sample_stride"] == 50
    assert bridge["reference_debug_artifact_sample_stride"] == 10
    assert bridge["control_debug_artifact_sample_stride"] == 50
    assert bridge["claim_evidence_artifact_sample_stride"] == 10
    assert bridge["localization_acceleration_source"] == "finite_difference"
    acceleration_filter = bridge["localization_acceleration_filter"]
    assert acceleration_filter["enabled"] is True
    assert acceleration_filter["alpha"] == 0.15
    assert acceleration_filter["max_abs_mps2"] == 0.8
    assert acceleration_filter["max_delta_mps2"] == 0.15
    assert acceleration_filter["nonnegative_speed_prediction_horizon_s"] == 0.1
    assert acceleration_filter["alpha"] < 0.5
    assert control_mapping["steer_sign"] == -1.0
    assert control_mapping["steer_scale"] == 1.0
    assert control_mapping["steering_percent_normalization"] == "single_percent_at_select"
    assert control_lqr["enabled"] is True
    assert control_lqr["enable_reverse_leadlag_compensation"] is False
    assert control_lqr["enable_look_ahead_back_control"] is False
    assert control_lqr["query_time_nearest_point_only"] is False
    assert control_lqr["minimum_speed_protection"] == 1.0
    assert control_lqr["matrix_q"] == [0.15, 0.0, 1.0, 0.0]
    assert control_lon["enabled"] is True
    assert vehicle_param == {
        "enabled": True,
        "auto_from_carla": False,
        "brake_deadzone": 0.05,
        "throttle_deadzone": 15.7,
    }
    assert map_speed_limit == {
        "enabled": False,
        "fill_missing_lane_speed_limits": True,
        "missing_lane_default_mps": "inherit_existing",
        "restore_original": False,
    }
    assert route_health["align_spawn_to_route_start"] is True
    planning = payload["algo"]["apollo"]["planning"]
    assert planning["enable_reference_line_stitching"] is False
    assert planning["enable_trajectory_stitcher"] is False
    assert "control_runtime" not in apollo


def test_town01_physical_candidate_inherits_reference_runtime_without_guards() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "town01_apollo_route_health_behavior_recovery_stitcher_physical_v1.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    mapping = apollo["control_mapping"]
    physical = mapping["physical"]

    assert apollo["bridge"]["obstacle_publish_rate_hz"] == 10.0
    assert apollo["planning"]["enable_trajectory_stitcher"] is True
    assert mapping["steer_sign"] == -1.0
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert mapping["actuator_mapping_mode"] == "physical"
    assert physical["apollo_max_steer_angle_deg"] == 30.0
    assert physical["allow_legacy_fallback"] is False
    assert physical["map_steering"] is True
    assert physical["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_speed_handover_physical_steering_candidate_is_isolated() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_handover_speed_compatibility_physical_steering_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    mapping = apollo["control_mapping"]
    physical = mapping["physical"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert apollo["bridge"]["obstacle_time_source"] == "cyber_time"
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["scene_preroll_planning_speed_gate_enabled"] is True
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 10
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert physical["allow_legacy_fallback"] is False
    assert physical["map_steering"] is True
    assert physical["map_steering_feedback"] is True
    assert physical["map_longitudinal"] is False
    assert physical["map_throttle"] is False
    assert physical["map_brake"] is False


def test_baguang_physical_steering_uses_tracked_vehicle_scoped_calibration(
    tmp_path: Path,
) -> None:
    calibration_relpath = (
        "configs/calibration/vehicles/vehicle.lincoln.mkz_2020/"
        "steering_front_wheel_v1.json"
    )
    profile_names = (
        "phase1_baguang_apollo_dynamic_coherent_simtime_accel_feasibility_"
        "trajectory_stitcher_physical_steering_candidate.yaml",
        "phase1_baguang_apollo_dynamic_handover_speed_compatibility_"
        "physical_steering_candidate.yaml",
        "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_"
        "physical_steering_fresh_gt_coherent_simtime_candidate.yaml",
    )

    for profile_name in profile_names:
        config = load_config(Path("configs/io/examples") / profile_name)
        physical = config.backend.params["legacy_algo"]["apollo"][
            "control_mapping"
        ]["physical"]
        assert physical["calibration_file"] == calibration_relpath

    calibration_path = resolve_calibration_path(
        calibration_relpath,
        repo_root=Path.cwd(),
        artifacts_dir=tmp_path,
    )
    assert calibration_path == (Path.cwd() / calibration_relpath).resolve()
    payload = json.loads(calibration_path.read_text(encoding="utf-8"))
    expected_rows = [
        {"target_front_wheel_angle_deg": 0.0, "carla_steer_cmd": 0.0},
        {
            "target_front_wheel_angle_deg": 13.167906522750854,
            "carla_steer_cmd": 0.2,
        },
        {
            "target_front_wheel_angle_deg": 13.172813653945923,
            "carla_steer_cmd": 0.2,
        },
        {
            "target_front_wheel_angle_deg": 25.141672134399414,
            "carla_steer_cmd": 0.4,
        },
        {
            "target_front_wheel_angle_deg": 25.150734901428223,
            "carla_steer_cmd": 0.4,
        },
        {
            "target_front_wheel_angle_deg": 36.46246862411499,
            "carla_steer_cmd": 0.6,
        },
        {
            "target_front_wheel_angle_deg": 36.4736704826355,
            "carla_steer_cmd": 0.6,
        },
        {
            "target_front_wheel_angle_deg": 47.526851654052734,
            "carla_steer_cmd": 0.8,
        },
        {
            "target_front_wheel_angle_deg": 47.534563064575195,
            "carla_steer_cmd": 0.8,
        },
        {
            "target_front_wheel_angle_deg": 58.18940734863281,
            "carla_steer_cmd": 1.0,
        },
        {
            "target_front_wheel_angle_deg": 58.67182540893555,
            "carla_steer_cmd": 1.0,
        },
    ]
    assert payload["steering"]["inverse"][
        "target_front_wheel_angle_deg_to_carla_cmd"
    ] == expected_rows
    assert payload["provenance"]["source_library_entry_id"] == "e969a34a5f43fd25"
    assert payload["provenance"]["source_calibration_sha256"] == (
        "2c2b09db185bf61d4258883de93bd9125fd3e34e28bb8ff24519ee0ea72ff0be"
    )

    calibration = load_actuator_calibration(calibration_path)
    baseline = ActuatorCalibration(
        {
            "schema_version": 1,
            "steering": {
                "inverse": {
                    "target_front_wheel_angle_deg_to_carla_cmd": expected_rows,
                }
            },
        }
    )
    for angle_tenths in range(-600, 601):
        angle_deg = angle_tenths / 10.0
        assert calibration.steering_cmd_for_angle(angle_deg) == (
            baseline.steering_cmd_for_angle(angle_deg)
        )

    status = calibration.status()
    assert status["loaded"] is True
    assert status["calibration_id"] == (
        "carla_0_9_16__vehicle.lincoln.mkz_2020__steering_front_wheel_v1"
    )
    assert status["vehicle_type_id"] == "vehicle.lincoln.mkz_2020"
    assert status["source_sha256"] == hashlib.sha256(
        calibration_path.read_bytes()
    ).hexdigest()
    assert status["steering_pairs"] == 11
    assert status["throttle_speed_bins"] == 0
    assert status["brake_speed_bins"] == 0


def test_baguang_speed_handover_fresh_gt_candidate_only_skips_cached_frames() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_handover_speed_compatibility_"
            "physical_steering_fresh_gt_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert bridge["obstacle_time_source"] == "cyber_time"
    assert bridge["claim_grade"] == {
        "enabled": True,
        "stale_world_frame_policy": "skip",
        "localization_publish_policy": "once_per_new_sim_frame",
        "chassis_publish_policy": "once_per_new_sim_frame",
    }
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["scene_preroll_planning_speed_gate_enabled"] is True
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 10
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_steering_feedback"] is True
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_extended_native_physical_fresh_gt_candidate_only_changes_gt_policy() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_"
            "drivetrain_ready_extended_opendrive_physical_steering_fresh_gt_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert bridge["claim_grade"] == {
        "enabled": True,
        "stale_world_frame_policy": "skip",
        "localization_publish_policy": "once_per_new_sim_frame",
        "chassis_publish_policy": "once_per_new_sim_frame",
    }
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["scene_preroll_drivetrain_gate_enabled"] is True
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_steering_feedback"] is True
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_fresh_gt_stats_flush_candidate_only_moves_periodic_stats_write() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_"
            "drivetrain_ready_extended_opendrive_physical_steering_fresh_gt_"
            "stats_flush_2s_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert bridge["artifact_stats_flush_interval_s"] == 2.0
    assert bridge["claim_grade"] == {
        "enabled": True,
        "stale_world_frame_policy": "skip",
        "localization_publish_policy": "once_per_new_sim_frame",
        "chassis_publish_policy": "once_per_new_sim_frame",
    }
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_steering_feedback"] is True
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_fresh_gt_low_capture_candidate_only_reduces_control_trace_sampling() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_"
            "drivetrain_ready_extended_opendrive_physical_steering_fresh_gt_"
            "low_capture_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert bridge["artifact_stats_flush_interval_s"] == 2.0
    assert bridge["control_debug_artifact_sample_stride"] == 10
    assert bridge["claim_grade"] == {
        "enabled": True,
        "stale_world_frame_policy": "skip",
        "localization_publish_policy": "once_per_new_sim_frame",
        "chassis_publish_policy": "once_per_new_sim_frame",
    }
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_steering_feedback"] is True
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_speed_compensated_steering_candidate_is_isolated_and_strict() -> None:
    baseline = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_"
            "control_raw_full_capture_extended_opendrive_candidate.yaml"
        )
    )
    candidate = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_"
            "control_raw_full_capture_extended_opendrive_"
            "speed_compensated_steering_candidate.yaml"
        )
    )
    baseline_apollo = baseline.backend.params["legacy_algo"]["apollo"]
    candidate_apollo = candidate.backend.params["legacy_algo"]["apollo"]
    baseline_fixed_scene = baseline.backend.params["legacy_runtime"][
        "fixed_scene_player"
    ]
    candidate_fixed_scene = candidate.backend.params["legacy_runtime"][
        "fixed_scene_player"
    ]

    normalized_candidate_apollo = copy.deepcopy(candidate_apollo)
    normalized_candidate_apollo["control_mapping"]["physical"][
        "calibration_file"
    ] = baseline_apollo["control_mapping"]["physical"]["calibration_file"]
    normalized_candidate_apollo["control_mapping"]["physical"].pop(
        "steering_speed_compensation_enabled"
    )
    assert normalized_candidate_apollo == baseline_apollo
    assert candidate_fixed_scene == baseline_fixed_scene

    mapping = candidate_apollo["control_mapping"]
    physical = mapping["physical"]
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["steering_percent_normalization"] == "single_percent_at_select"
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert physical["steering_speed_compensation_enabled"] is True
    assert physical["calibration_file"].endswith(
        "steering_front_wheel_speed_v2.json"
    )
    assert physical["map_steering"] is True
    assert physical["map_steering_feedback"] is True
    assert physical["map_longitudinal"] is False
    assert physical["allow_legacy_fallback"] is False
    assert candidate_fixed_scene["start_gate"] == "apollo_planning_ready"
    assert candidate_fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert candidate_fixed_scene["planning_ready_disallow_fallback"] is True
    assert candidate.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_fresh_gt_coherent_simtime_candidate_only_aligns_clock_domain() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_handover_speed_compatibility_"
            "physical_steering_fresh_gt_coherent_simtime_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert bridge["localization_time_source"] == "sim_time"
    assert bridge["obstacle_time_source"] == "localization_time"
    assert bridge["cyber_clock"] == {
        "enabled": True,
        "mode": "mock",
        "channel": "/clock",
    }
    assert bridge["claim_grade"] == {
        "enabled": True,
        "stale_world_frame_policy": "skip",
        "localization_publish_policy": "once_per_new_sim_frame",
        "chassis_publish_policy": "once_per_new_sim_frame",
    }
    assert fixed_scene["scene_preroll_planning_speed_gate_enabled"] is True
    assert fixed_scene["scene_preroll_planning_current_speed_tolerance_mps"] == 1.0
    assert fixed_scene["scene_preroll_planning_lookahead_speed_tolerance_mps"] == 2.0
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 10
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_steering_feedback"] is True
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_replan_confirmed_handover_candidate_keeps_strict_fallback_gate() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_handover_replan_confirmed_"
            "physical_steering_fresh_gt_coherent_simtime_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is True
    assert bridge["localization_time_source"] == "sim_time"
    assert bridge["obstacle_time_source"] == "localization_time"
    assert bridge["cyber_clock"]["mode"] == "mock"
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_authored_initial_speed_gate_candidate_is_explicit_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_"
            "physical_steering_fresh_gt_coherent_simtime_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["scene_preroll_materialize_initial_speeds_on_gate"] is True
    assert fixed_scene["scene_preroll_ego_handover_mode"] == "target_ready"
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is True
    assert bridge["localization_time_source"] == "sim_time"
    assert bridge["obstacle_time_source"] == "source_time"
    assert bridge["obstacle_publish_policy"] == "source_fresh"
    assert bridge["cyber_clock"]["mode"] == "mock"
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_authored_initial_speed_legacy_steering_ab_is_matched_and_quarantined() -> None:
    baseline = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_"
            "physical_steering_fresh_gt_coherent_simtime_candidate.yaml"
        )
    )
    candidate = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_"
            "legacy_steering_ab_candidate.yaml"
        )
    )
    baseline_apollo = baseline.backend.params["legacy_algo"]["apollo"]
    candidate_apollo = candidate.backend.params["legacy_algo"]["apollo"]
    baseline_fixed_scene = baseline.backend.params["legacy_runtime"][
        "fixed_scene_player"
    ]
    candidate_fixed_scene = candidate.backend.params["legacy_runtime"][
        "fixed_scene_player"
    ]

    normalized_candidate_apollo = copy.deepcopy(candidate_apollo)
    normalized_candidate_apollo["control_mapping"]["actuator_mapping_mode"] = (
        "physical"
    )
    assert normalized_candidate_apollo == baseline_apollo
    assert candidate_fixed_scene == baseline_fixed_scene

    mapping = candidate_apollo["control_mapping"]
    assert mapping["actuator_mapping_mode"] == "legacy"
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert candidate_fixed_scene["start_gate"] == "apollo_planning_ready"
    assert candidate_fixed_scene["planning_ready_disallow_fallback"] is True
    assert candidate_fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert candidate_fixed_scene["scene_preroll_ego_handover_mode"] == (
        "target_ready"
    )
    assert candidate.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_initial_state_obstacle_gate_is_truthful_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_obstacle_gate_"
            "extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    obstacle_gate = apollo["bridge"]["front_obstacle_behavior"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert obstacle_gate["mode"] == "scenario_initial_state_gate"
    assert obstacle_gate["actor_probe_enabled"] is True
    assert obstacle_gate["role_names"] == ["lead_vehicle", "front"]
    assert obstacle_gate["activation_marker_path"] == (
        "fixed_scene_obstacle_activation.json"
    )
    assert obstacle_gate["activation_required_status"] == "pass"
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["scene_preroll_materialize_initial_speeds_on_gate"] is True
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is True
    assert mapping["steer_scale"] == 1.0
    assert mapping["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_noninteractive_prediction_ab_is_native_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_gate_"
            "noninteractive_prediction_ab_extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert apollo["prediction"] == {"enable_interactive_tag": False}
    assert apollo["bridge"]["front_obstacle_behavior"]["mode"] == (
        "scenario_initial_state_gate"
    )
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_current_state_prediction_ab_is_native_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_gate_"
            "current_state_prediction_ab_extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert apollo["prediction"] == {
        "enable_interactive_tag": False,
        "vehicle_on_lane_caution_mode": "cost_move_sequence_current_state",
    }
    assert apollo["bridge"]["front_obstacle_behavior"]["mode"] == (
        "scenario_initial_state_gate"
    )
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_cut_in_current_state_prediction_ab_is_matched_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_"
            "cut_in_current_state_prediction_ab_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert apollo["prediction"] == {
        "enable_interactive_tag": False,
        "vehicle_on_lane_caution_mode": "cost_move_sequence_current_state",
    }
    assert apollo["bridge"]["front_obstacle_behavior"]["mode"] == "static_hold"
    assert apollo["bridge"]["obstacle_publish_policy"] == "source_fresh"
    assert apollo["bridge"]["cyber_clock"] == {
        "enabled": True,
        "mode": "mock",
        "channel": "/clock",
    }
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["scene_preroll_materialize_initial_speeds_on_gate"] is True
    assert fixed_scene["scene_preroll_ego_handover_mode"] == "target_ready"
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is True
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_current_state_prediction_carla_accel_is_gt_only_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_gate_"
            "current_state_prediction_carla_accel_extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert bridge["localization_acceleration_source"] == "carla_feedback"
    assert bridge["localization_acceleration_filter"] == {
        "enabled": False,
        "alpha": 1.0,
        "max_abs_mps2": 0.0,
        "max_delta_mps2": 0.0,
        "nonnegative_speed_prediction_horizon_s": 0.0,
    }
    assert bridge["front_obstacle_behavior"]["mode"] == "scenario_initial_state_gate"
    assert apollo["prediction"] == {
        "enable_interactive_tag": False,
        "vehicle_on_lane_caution_mode": "cost_move_sequence_current_state",
    }
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["scene_preroll_materialize_initial_speeds_on_gate"] is True
    assert fixed_scene["scene_preroll_ego_handover_mode"] == "target_ready"
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_current_speed_handover_keeps_strict_normal_continuity_gate() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_gate_"
            "current_state_prediction_carla_accel_current_speed_handover_"
            "extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["scene_preroll_planning_speed_gate_enabled"] is True
    assert fixed_scene["scene_preroll_planning_lookahead_speed_gate_enabled"] is False
    assert fixed_scene["scene_preroll_planning_current_speed_tolerance_mps"] == 1.0
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is False
    assert apollo["bridge"]["localization_acceleration_source"] == "carla_feedback"
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_initial_state_transition_is_one_shot_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_gate_"
            "current_state_prediction_carla_accel_initial_state_transition_"
            "extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert bridge["localization_acceleration_source"] == "carla_feedback"
    assert bridge["localization_acceleration_filter"]["enabled"] is False
    assert bridge["localization_authored_initial_state_transition"] == {
        "mode": "marker_zero_once",
        "marker_path": "fixed_scene_gate_initial_state_materialization.json",
        "required_status": "pass",
        "max_world_frame_delta": 2,
        "max_sim_time_delta_s": 0.11,
        "speed_tolerance_mps": 0.5,
    }
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_lookahead_speed_gate_enabled"] is False
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_initial_state_early_replan_is_single_variable_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_gate_"
            "current_state_prediction_carla_accel_initial_state_transition_"
            "early_replan_extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert apollo["planning"]["replan_longitudinal_distance_threshold"] == 0.5
    assert apollo["bridge"]["localization_authored_initial_state_transition"][
        "mode"
    ] == "marker_zero_once"
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_lookahead_speed_gate_enabled"] is False
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is False
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_handover_speed_tolerance_is_explicit_and_keeps_normal_gate() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_initial_state_gate_"
            "current_state_prediction_carla_accel_initial_state_transition_"
            "handover_speed_tolerance_1p0_extended_opendrive_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert apollo["planning"].get("replan_longitudinal_distance_threshold") is None
    assert fixed_scene["scene_preroll_speed_tolerance_mps"] == 1.0
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["scene_preroll_planning_current_speed_tolerance_mps"] == 1.0
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["scene_preroll_planning_lookahead_speed_gate_enabled"] is False
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is False
    assert apollo["control_mapping"]["steer_scale"] == 1.0
    assert apollo["control_mapping"]["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_destination_rule_ab_candidate_is_single_rule_and_quarantined() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_"
            "destination_rule_ab_candidate.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    planning = apollo["planning"]
    mapping = apollo["control_mapping"]
    fixed_scene = config.backend.params["legacy_runtime"]["fixed_scene_player"]

    assert planning["disable_destination_rule"] is True
    assert planning.get("disable_rule_based_stop_decider", False) is False
    assert planning.get("disable_traffic_light_rule", False) is False
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["scene_preroll_materialize_initial_speeds_on_gate"] is True
    assert fixed_scene["scene_preroll_ego_handover_mode"] == "target_ready"
    assert mapping["steer_scale"] == 1.0
    assert mapping["physical"]["map_longitudinal"] is False
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_town01_cybertime_stitcher_reference_candidate_is_explicit() -> None:
    config = load_config(
        Path(
            "configs/io/examples/"
            "town01_apollo_route_health_reference_runtime_cybertime_stitcher_v1.yaml"
        )
    )
    apollo = config.backend.params["legacy_algo"]["apollo"]
    bridge = apollo["bridge"]
    planning = apollo["planning"]
    mapping = apollo["control_mapping"]
    legacy_run = config.backend.params["legacy_run"]

    assert legacy_run["profile_name"] == (
        "town01_apollo_route_health_reference_runtime_cybertime_stitcher_v1"
    )
    assert config.run.claim_profile is False
    assert bridge["localization_time_source"] == "cyber_time"
    assert bridge["claim_grade"]["enabled"] is False
    assert planning["enable_trajectory_stitcher"] is True
    assert planning["enable_reference_line_stitching"] is False
    assert mapping["steer_sign"] == -1.0
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "legacy"
    assert config.assist_ledger["can_claim_unassisted_natural_driving"] is False


def test_baguang_native_prediction_physical_brake_candidate_is_isolated() -> None:
    baseline = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_"
            "control_raw_full_capture_extended_opendrive_native_prediction_candidate.yaml"
        )
    )
    candidate = load_config(
        Path(
            "configs/io/examples/"
            "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_"
            "control_raw_full_capture_extended_opendrive_native_prediction_"
            "physical_brake_candidate.yaml"
        )
    )
    baseline_apollo = baseline.backend.params["legacy_algo"]["apollo"]
    candidate_apollo = candidate.backend.params["legacy_algo"]["apollo"]
    expected_apollo = copy.deepcopy(baseline_apollo)
    expected_physical = expected_apollo["control_mapping"]["physical"]
    expected_physical.update(
        {
            "calibration_file": (
                "configs/calibration/vehicles/vehicle.lincoln.mkz_2020/"
                "steering_brake_v57_diagnostic_candidate.json"
            ),
            "allow_legacy_fallback": False,
            "map_steering": True,
            "map_steering_feedback": True,
            "map_longitudinal": True,
            "map_throttle": False,
            "map_brake": True,
        }
    )

    assert candidate_apollo == expected_apollo
    assert candidate.backend.params["legacy_runtime"] == baseline.backend.params[
        "legacy_runtime"
    ]
    mapping = candidate_apollo["control_mapping"]
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert candidate.assist_ledger["can_claim_unassisted_natural_driving"] is False

    calibration = load_actuator_calibration(
        Path(mapping["physical"]["calibration_file"])
    )
    status = calibration.status()
    assert status["calibration_id"].endswith(
        "steering_brake_v57_diagnostic_candidate"
    )
    assert status["steering_pairs"] == 11
    assert status["brake_speed_bins"] == 4
    assert status["brake_low_speed_hold_cmd"] == 0.0
    assert status["brake_low_speed_stop_cmd"] == 0.002
    assert status["brake_low_speed_rolling_sections"] == 1
    assert calibration.brake_mapping_for_decel(2.922, speed_mps=5.52)["cmd"] is not None
    assert calibration.brake_mapping_for_decel(2.14, speed_mps=3.35) == {
        "cmd": None,
        "source": "decel_below_calibrated_brake_range",
    }
