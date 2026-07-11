from __future__ import annotations

import json
from pathlib import Path

import yaml

from algo.adapters.apollo import ApolloAdapter, _rewrite_apollo_lane_speed_limits
from carla_testbed.config.loader import load_config


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
                    "obstacle_publish_rate_hz": 10.0,
                    "artifact_async_write_enabled": True,
                    "artifact_async_queue_max_rows": 8192,
                    "artifact_async_queue_soft_limit_rows": 4096,
                    "artifact_flush_interval_s": 0.5,
                    "artifact_flush_max_pending_rows": 200,
                    "artifact_stats_flush_interval_s": 1.0,
                    "stage5_debug_artifact_sample_stride": 10,
                    "reference_debug_artifact_sample_stride": 9,
                    "control_debug_artifact_sample_stride": 7,
                    "claim_evidence_artifact_sample_stride": 3,
                    "localization_acceleration_filter": {
                        "enabled": True,
                        "alpha": 0.35,
                        "max_abs_mps2": 4.0,
                        "max_delta_mps2": 1.0,
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
    assert bridge["obstacle_publish_rate_hz"] == 10.0
    assert bridge["artifact_async_write_enabled"] is True
    assert bridge["artifact_async_queue_max_rows"] == 8192
    assert bridge["artifact_async_queue_soft_limit_rows"] == 4096
    assert bridge["artifact_flush_interval_s"] == 0.5
    assert bridge["artifact_flush_max_pending_rows"] == 200
    assert bridge["artifact_stats_flush_interval_s"] == 1.0
    assert bridge["stage5_debug_artifact_sample_stride"] == 10
    assert bridge["reference_debug_artifact_sample_stride"] == 9
    assert bridge["control_debug_artifact_sample_stride"] == 7
    assert bridge["claim_evidence_artifact_sample_stride"] == 3
    assert bridge["localization_acceleration_filter"] == {
        "enabled": True,
        "alpha": 0.35,
        "max_abs_mps2": 4.0,
        "max_delta_mps2": 1.0,
    }
    assert control_mapping["steer_scale"] == 0.25
    assert control_mapping["steering_percent_normalization"] == "legacy_double_percent"
    assert control_mapping["throttle_brake_mutual_exclusion_enabled"] is True
    assert control_mapping["throttle_brake_hysteresis_frames"] == 3
    assert control_mapping["throttle_brake_min_command"] == 0.02
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
    assert control_lqr["minimum_speed_protection"] == 1.0
    assert control_lqr["matrix_q"] == [0.15, 0.0, 1.0, 0.0]
    assert control_lon["enabled"] is True
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
