from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import yaml

from tbio.backends.cyberrt import CyberRTBackend
from tbio.scripts.run import _load_config_with_extends


def test_longitudinal_candidate_declares_apollo_vehicle_param_override() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_longitudinal_calibration_candidate.yaml"
    )
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert payload["run"]["claim_profile"] is False
    vehicle_param = payload["algo"]["apollo"]["vehicle_param"]
    assert vehicle_param == {
        "enabled": True,
        "auto_from_carla": False,
        "brake_deadzone": 0.05,
        "throttle_deadzone": 15.7,
    }


def test_apollo_vehicle_param_override_reaches_runtime_textproto_shell() -> None:
    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "vehicle_param": {
                        "brake_deadzone": 0.05,
                        "throttle_deadzone": 15.7,
                    }
                }
            }
        }
    )

    effective = backend._sim_vehicle_param_cfg()
    shell = backend._docker_sim_vehicle_param_shell()

    assert effective["brake_deadzone"] == 0.05
    assert effective["throttle_deadzone"] == 15.7
    assert "brake_deadzone: 0.0500" in shell
    assert "throttle_deadzone: 15.7000" in shell


def test_baguang_followstop_accel4_candidate_is_scoped_and_materializes() -> None:
    path = Path(
        "configs/io/examples/"
        "phase1_baguang_apollo_dynamic_followstop_static_planning_ready_vehicle_accel4_candidate.yaml"
    )
    payload = _load_config_with_extends(path)

    # The compatibility CLI currently uses this inherited flag to dispatch the
    # existing Apollo runtime; the assist ledger remains the claim boundary.
    assert payload["run"]["claim_profile"] is True
    assert payload["assist_ledger"]["can_claim_unassisted_natural_driving"] is False
    assert payload["algo"]["apollo"]["vehicle_param"] == {
        "enabled": True,
        "auto_from_carla": False,
        "brake_deadzone": 0.05,
        "throttle_deadzone": 15.7,
        "max_acceleration": 4.0,
    }
    assert payload["runtime"]["fixed_scene_player"][
        "scene_preroll_ego_handover_mode"
    ] == "planning_ready"
    assert payload["runtime"]["fixed_scene_player"][
        "scene_preroll_materialize_initial_speeds_on_gate"
    ] is False

    backend = CyberRTBackend(payload)
    assert backend._sim_vehicle_param_cfg()["max_acceleration"] == 4.0
    assert "max_acceleration: 4.0000" in backend._docker_sim_vehicle_param_shell()


def test_baguang_compute_paced_accel4_candidate_changes_only_vehicle_capability() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_vehicle_accel4_candidate.yaml"
    )

    expected = deepcopy(baseline)
    expected["run"].update(
        {
            "id": "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_vehicle_accel4_candidate",
            "profile_name": "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_vehicle_accel4_candidate",
        }
    )
    expected["algo"]["apollo"]["vehicle_param"]["max_acceleration"] = 4.0
    expected["assist_ledger"]["can_claim_unassisted_natural_driving"] = False
    expected["assist_ledger"]["notes"] = candidate["assist_ledger"]["notes"]

    assert candidate == expected
    assert candidate["runtime"]["fixed_scene_player"]["start_gate"] == "apollo_planning_ready"
    assert candidate["runtime"]["fixed_scene_player"][
        "scene_preroll_planning_require_replan_anchor"
    ] is True
    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["physical"]["map_throttle"] is False
    assert mapping["physical"]["map_brake"] is False
    assert candidate["algo"]["apollo"]["bridge"][
        "localization_acceleration_source"
    ] == "carla_feedback"
    assert candidate["algo"]["apollo"]["bridge"][
        "localization_acceleration_filter"
    ]["enabled"] is False

    backend = CyberRTBackend(candidate)
    assert backend._sim_vehicle_param_cfg()["max_acceleration"] == 4.0
    assert "max_acceleration: 4.0000" in backend._docker_sim_vehicle_param_shell()


def test_baguang_compute_paced_current_state_prediction_candidate_is_isolated() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_vehicle_accel4_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_compute_paced_vehicle_accel4_current_state_prediction_extended_opendrive_candidate.yaml"
    )

    expected = deepcopy(baseline)
    expected["run"].update(
        {
            "id": "phase1_baguang_apollo_dynamic_compute_paced_vehicle_accel4_current_state_prediction_extended_opendrive_candidate",
            "profile_name": "phase1_baguang_apollo_dynamic_compute_paced_vehicle_accel4_current_state_prediction_extended_opendrive_candidate",
        }
    )
    expected["algo"]["apollo"]["prediction"][
        "vehicle_on_lane_caution_mode"
    ] = "cost_move_sequence_current_state"
    expected["assist_ledger"]["can_claim_unassisted_natural_driving"] = False
    expected["assist_ledger"]["notes"] = candidate["assist_ledger"]["notes"]

    assert candidate == expected

    apollo = candidate["algo"]["apollo"]
    fixed_scene = candidate["runtime"]["fixed_scene_player"]
    mapping = apollo["control_mapping"]
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is True
    assert apollo["vehicle_param"]["max_acceleration"] == 4.0
    assert apollo["prediction"]["enable_interactive_tag"] is False
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["steer_scale"] == 1.0
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False

    shell = CyberRTBackend(candidate)._prediction_obstacle_conf_overlay_shell()
    assert "COST_EVALUATOR" in shell
    assert "MOVE_SEQUENCE_PREDICTOR" in shell


def test_baguang_current_state_post_stability_handover_changes_only_replan_bit() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_compute_paced_vehicle_accel4_current_state_prediction_extended_opendrive_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_compute_paced_vehicle_accel4_current_state_prediction_post_stability_handover_extended_opendrive_candidate.yaml"
    )

    expected = deepcopy(baseline)
    expected["run"].update(
        {
            "id": "phase1_baguang_apollo_dynamic_compute_paced_vehicle_accel4_current_state_prediction_post_stability_handover_extended_opendrive_candidate",
            "profile_name": "phase1_baguang_apollo_dynamic_compute_paced_vehicle_accel4_current_state_prediction_post_stability_handover_extended_opendrive_candidate",
        }
    )
    expected["runtime"]["fixed_scene_player"][
        "planning_ready_require_initial_replan_anchor"
    ] = True
    expected["runtime"]["fixed_scene_player"][
        "scene_preroll_planning_require_replan_anchor"
    ] = False
    expected["assist_ledger"]["can_claim_unassisted_natural_driving"] = False
    expected["assist_ledger"]["notes"] = candidate["assist_ledger"]["notes"]

    assert candidate == expected

    fixed_scene = candidate["runtime"]["fixed_scene_player"]
    apollo = candidate["algo"]["apollo"]
    mapping = apollo["control_mapping"]
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["scene_preroll_planning_compatible_min_messages"] == 2
    assert fixed_scene["planning_ready_require_initial_replan_anchor"] is True
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is False
    assert fixed_scene["scene_preroll_planning_speed_gate_enabled"] is True
    assert apollo["prediction"]["vehicle_on_lane_caution_mode"] == (
        "cost_move_sequence_current_state"
    )
    assert apollo["vehicle_param"]["max_acceleration"] == 4.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["steer_scale"] == 1.0
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False


def test_baguang_dynamic_profiles_inherit_explicit_vehicle_param_override() -> None:
    expected = {
        "enabled": True,
        "auto_from_carla": False,
        "brake_deadzone": 0.05,
        "throttle_deadzone": 15.7,
    }
    for filename in (
        "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml",
        "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml",
    ):
        payload = _load_config_with_extends(Path("configs/io/examples") / filename)
        assert payload["algo"]["apollo"]["vehicle_param"] == expected


def test_baguang_minimal_authored_speed_gate_changes_only_setup_boundary() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_minimal_candidate.yaml"
    )

    expected_fixed_scene = dict(baseline["runtime"]["fixed_scene_player"])
    expected_fixed_scene.update(
        {
            "scene_preroll_materialize_initial_speeds_on_gate": True,
            "scene_preroll_speed_tolerance_mps": 1.0,
            "scene_preroll_planning_speed_gate_enabled": True,
            "scene_preroll_planning_current_speed_tolerance_mps": 1.0,
            "scene_preroll_planning_lookahead_speed_gate_enabled": False,
            "scene_preroll_planning_compatible_min_messages": 2,
            "scene_preroll_planning_require_replan_anchor": False,
        }
    )
    assert candidate["runtime"]["fixed_scene_player"] == expected_fixed_scene
    assert candidate["algo"] == baseline["algo"]
    assert candidate["run"]["claim_profile"] is True
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False


def test_baguang_early_apollo_handover_changes_only_setup_ownership() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml"
    )
    candidate = _load_config_with_extends(
        root / "phase1_baguang_apollo_dynamic_early_planning_handover_candidate.yaml"
    )

    expected_fixed_scene = dict(baseline["runtime"]["fixed_scene_player"])
    expected_fixed_scene.update(
        {
            "scene_preroll_ego_handover_mode": "planning_ready",
            "scene_preroll_planning_speed_gate_enabled": True,
            "scene_preroll_planning_current_speed_tolerance_mps": 1.0,
            "scene_preroll_planning_lookahead_speed_gate_enabled": False,
            "scene_preroll_planning_compatible_min_messages": 2,
            "scene_preroll_planning_require_replan_anchor": False,
        }
    )
    assert candidate["runtime"]["fixed_scene_player"] == expected_fixed_scene
    assert candidate["algo"] == baseline["algo"]
    assert candidate["run"]["claim_profile"] is True
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False


def test_baguang_minimal_transition_zeros_only_authored_jump_acceleration() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_minimal_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_minimal_transition_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["bridge"][
        "localization_authored_initial_state_transition"
    ] = {
        "mode": "marker_zero_once",
        "marker_path": "fixed_scene_gate_initial_state_materialization.json",
        "required_status": "pass",
        "max_world_frame_delta": 2,
        "max_sim_time_delta_s": 0.11,
        "speed_tolerance_mps": 0.5,
    }
    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["run"]["claim_profile"] is True
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False


def test_baguang_native_state_candidate_only_changes_startup_inputs() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_minimal_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    bridge = expected_algo["apollo"]["bridge"]
    bridge["localization_time_source"] = "sim_time"
    bridge["cyber_clock"] = {
        "enabled": True,
        "mode": "mock",
        "channel": "/clock",
    }
    bridge["localization_acceleration_source"] = "carla_feedback"
    bridge["localization_acceleration_filter"] = {
        "enabled": False,
        "alpha": 1.0,
        "max_abs_mps2": 0.0,
        "max_delta_mps2": 0.0,
        "nonnegative_speed_prediction_horizon_s": 0.0,
    }
    bridge["localization_authored_initial_state_transition"] = {
        "mode": "marker_zero_once",
        "marker_path": "fixed_scene_gate_initial_state_materialization.json",
        "required_status": "pass",
        "max_world_frame_delta": 2,
        "max_sim_time_delta_s": 0.11,
        "speed_tolerance_mps": 0.5,
    }
    expected_algo["apollo"]["prediction"] = {
        "enable_interactive_tag": False,
        "vehicle_on_lane_caution_mode": "cost_move_sequence_current_state",
    }

    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["run"]["claim_profile"] is True
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False


def test_baguang_drivetrain_ready_candidate_only_extends_setup_gate() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_drivetrain_ready_candidate.yaml"
    )

    expected_runtime = deepcopy(baseline["runtime"])
    expected_runtime["fixed_scene_player"].update(
        {
            "scene_preroll_drivetrain_gate_enabled": True,
            "scene_preroll_drivetrain_max_abs_acceleration_mps2": 2.0,
            "scene_preroll_ready_hold_ticks": 4,
        }
    )
    assert candidate["runtime"] == expected_runtime
    assert candidate["algo"] == baseline["algo"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False


def test_longitudinal_stitcher_candidate_only_enables_trajectory_stitching() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_longitudinal_stitcher_candidate.yaml"
    )
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    planning = payload["algo"]["apollo"]["planning"]
    assert planning["enable_reference_line_stitching"] is False
    assert planning["enable_trajectory_stitcher"] is True


def test_control_cadence_candidate_only_changes_bridge_timer_frequency() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_control_cadence_candidate.yaml"
    )
    payload = _load_config_with_extends(path)

    planning = payload["algo"]["apollo"]["planning"]
    bridge = payload["algo"]["apollo"]["carla_control_bridge"]
    assert planning["enable_reference_line_stitching"] is False
    assert planning["enable_trajectory_stitcher"] is True
    assert bridge["apply_hz"] == 40.0
    assert bridge["sync_to_world_tick"] is True


def test_baguang_control_state_cadence_candidate_only_aligns_apollo_control_period() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_authored_initial_speed_gate_native_state_drivetrain_ready_extended_opendrive_physical_steering_fresh_gt_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_extended_opendrive_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["control_runtime"] = {"control_interval_ms": 50}
    expected_algo["apollo"]["control_lqr"]["ts"] = 0.05

    # This inherited flag dispatches the compatibility runtime; it does not
    # override the explicit natural-driving claim boundary below.
    assert candidate["run"]["claim_profile"] is True
    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False

    backend = CyberRTBackend(candidate)
    assert "interval: 50" in backend._control_dag_overlay_shell()
    assert "--control_period=0.050" in backend._control_flags_overlay_shell()
    assert "ts: 0.0500" in backend._control_lqr_overlay_shell()


def test_baguang_control_cadence_stats_flush_candidate_only_moves_heavy_stats_off_hot_path() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_extended_opendrive_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_stats_flush_2s_extended_opendrive_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["bridge"]["artifact_stats_flush_interval_s"] = 2.0

    assert candidate["run"]["claim_profile"] is True
    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    claim_grade = candidate["algo"]["apollo"]["bridge"]["claim_grade"]
    assert claim_grade["enabled"] is True
    assert claim_grade["stale_world_frame_policy"] == "skip"
    assert claim_grade["localization_publish_policy"] == "once_per_new_sim_frame"
    assert claim_grade["chassis_publish_policy"] == "once_per_new_sim_frame"
    assert candidate["algo"]["apollo"]["bridge"]["publish_rate_hz"] == 20.0

    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_control_cadence_low_capture_candidate_only_samples_redundant_control_debug() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_stats_flush_2s_extended_opendrive_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_low_capture_extended_opendrive_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["bridge"]["control_debug_artifact_sample_stride"] = 10

    assert candidate["run"]["claim_profile"] is True
    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    bridge = candidate["algo"]["apollo"]["bridge"]
    assert bridge["publish_rate_hz"] == 20.0
    assert bridge["artifact_stats_flush_interval_s"] == 2.0
    assert bridge.get("claim_evidence_artifact_sample_stride", 1) == 1

    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_control_raw_full_capture_candidate_only_restores_canonical_raw_stream() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_low_capture_extended_opendrive_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_control_raw_full_capture_extended_opendrive_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["bridge"]["control_debug_artifact_sample_strides"] = {
        "apollo_control_raw": 1
    }

    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    bridge = candidate["algo"]["apollo"]["bridge"]
    assert bridge["control_debug_artifact_sample_stride"] == 10
    assert bridge["control_debug_artifact_sample_strides"] == {"apollo_control_raw": 1}
    assert bridge["claim_grade"]["stale_world_frame_policy"] == "skip"

    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_atomic_gt_clock_candidate_uses_tick_aligned_state_delivery() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_control_raw_full_capture_extended_opendrive_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["bridge"]["cyber_clock"] = {
        "enabled": True,
        "mode": "mock",
        "channel": "/clock",
        "publish_phase": "after_state",
        "odom_queue_depth": 64,
    }
    expected_algo["apollo"]["prediction"]["compute_device"] = "cpu"
    expected_algo["apollo"]["bridge"]["carla_feedback"] = {
        "state_source": "ros_objects_json"
    }
    expected_algo["apollo"]["bridge"]["front_obstacle_behavior"][
        "actor_probe_enabled"
    ] = False
    expected_scenario = deepcopy(baseline["scenario"])
    expected_scenario["gt"]["objects_hz"] = 20.0

    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == expected_scenario
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    apollo = candidate["algo"]["apollo"]
    assert apollo["prediction"]["compute_device"] == "cpu"
    assert apollo["bridge"]["carla_feedback"]["state_source"] == "ros_objects_json"
    assert apollo["bridge"]["front_obstacle_behavior"]["actor_probe_enabled"] is False
    assert candidate["scenario"]["gt"]["objects_hz"] == 20.0
    assert apollo["control_runtime"]["control_interval_ms"] == 50
    assert apollo["control_lqr"]["ts"] == 0.05
    assert apollo["bridge"]["control_debug_artifact_sample_strides"] == {
        "apollo_control_raw": 1
    }
    mapping = apollo["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False


def test_baguang_native_prediction_candidate_restores_native_mode_and_exact_gt_alignment() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["prediction"]["vehicle_on_lane_caution_mode"] = ""
    expected_algo["apollo"]["bridge"]["obstacle_alignment_policy"] = (
        "wait_for_exact_source_time"
    )
    expected_runtime = deepcopy(baseline["runtime"])
    expected_runtime["fixed_scene_player"][
        "scene_preroll_planning_require_replan_anchor"
    ] = True

    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == expected_runtime
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    apollo = candidate["algo"]["apollo"]
    prediction = apollo["prediction"]
    assert prediction["compute_device"] == "cpu"
    assert prediction["enable_interactive_tag"] is False
    assert prediction["vehicle_on_lane_caution_mode"] == ""

    backend = CyberRTBackend(candidate)
    prediction_overlay = backend._prediction_obstacle_conf_overlay_shell()
    assert "python3 -c" not in prediction_overlay
    assert "prediction_conf.pb.txt.carla_testbed.bak" in prediction_overlay

    mapping = apollo["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False
    assert candidate["runtime"]["fixed_scene_player"][
        "scene_preroll_planning_require_replan_anchor"
    ] is True


def test_baguang_native_prediction_compute_paced_candidate_only_slows_wall_clock() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_candidate.yaml"
    )

    expected_run = deepcopy(baseline["run"])
    expected_run.update(
        {
            "id": "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_candidate",
            "profile_name": "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_compute_paced_candidate",
            "wall_time_pacing": {
                "enabled": True,
                "target_interval_s": 0.25,
                "max_sleep_s": 0.25,
            },
        }
    )

    assert candidate["run"] == expected_run
    assert candidate["algo"] == baseline["algo"]
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    fixed_scene = candidate["runtime"]["fixed_scene_player"]
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["scene_preroll_planning_require_replan_anchor"] is True

    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False


def test_baguang_native_prediction_brake_scale_candidate_is_longitudinally_isolated() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_atomic_gt_clock_control_raw_full_capture_extended_opendrive_native_prediction_brake_scale_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_mapping = expected_algo["apollo"]["control_mapping"]
    expected_mapping["brake_scale"] = 0.38
    expected_mapping["brake_deadzone"] = 0.01

    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["physical"]["map_throttle"] is False
    assert mapping["physical"]["map_brake"] is False
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False


def test_baguang_followstop_brake_scale_candidate_is_longitudinally_isolated() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_followstop_static_planning_ready_portable_physical_steering_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_followstop_static_planning_ready_brake_scale_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_mapping = expected_algo["apollo"]["control_mapping"]
    expected_mapping["brake_scale"] = 0.38
    expected_mapping["brake_deadzone"] = 0.01

    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    fixed_scene = candidate["runtime"]["fixed_scene_player"]
    assert fixed_scene["start_gate"] == "apollo_planning_ready"
    assert fixed_scene["planning_ready_min_trajectory_points"] == 2
    assert fixed_scene["planning_ready_disallow_fallback"] is True
    assert fixed_scene["scene_preroll_materialize_initial_speeds_on_gate"] is False
    assert fixed_scene["scene_preroll_ego_handover_mode"] == "planning_ready"

    mapping = candidate["algo"]["apollo"]["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_steering"] is True
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["physical"]["map_throttle"] is False
    assert mapping["physical"]["map_brake"] is False
    assert mapping["force_zero_steer_output_enabled"] is False
    assert mapping["straight_lane_zero_steer_enabled"] is False
    assert mapping["low_speed_steer_guard_enabled"] is False
    assert mapping["low_speed_sustained_guard_enabled"] is False
    assert mapping["sustained_lateral_guard_enabled"] is False
    assert mapping["trajectory_contract_lateral_guard_enabled"] is False


def test_baguang_cutoff_preserved_candidate_only_changes_lqr_cutoff() -> None:
    root = Path("configs/io/examples")
    baseline = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_control_raw_full_capture_extended_opendrive_candidate.yaml"
    )
    candidate = _load_config_with_extends(
        root
        / "phase1_baguang_apollo_dynamic_control_state_cadence_aligned_control_raw_full_capture_extended_opendrive_cutoff_preserved_candidate.yaml"
    )

    expected_algo = deepcopy(baseline["algo"])
    expected_algo["apollo"]["control_lqr"]["cutoff_freq"] = 2

    assert candidate["algo"] == expected_algo
    assert candidate["runtime"] == baseline["runtime"]
    assert candidate["scenario"] == baseline["scenario"]
    assert candidate["assist_ledger"]["can_claim_unassisted_natural_driving"] is False

    apollo = candidate["algo"]["apollo"]
    assert apollo["control_runtime"]["control_interval_ms"] == 50
    assert apollo["control_lqr"]["ts"] == 0.05
    assert apollo["control_lqr"]["cutoff_freq"] == 2
    assert apollo["bridge"]["control_debug_artifact_sample_strides"] == {
        "apollo_control_raw": 1
    }

    mapping = apollo["control_mapping"]
    assert mapping["steer_scale"] == 1.0
    assert mapping["steer_sign"] == -1.0
    assert mapping["actuator_mapping_mode"] == "physical"
    assert mapping["physical"]["map_longitudinal"] is False
    assert mapping["physical"].get("steering_speed_compensation_enabled", False) is False

    backend = CyberRTBackend(candidate)
    lqr_overlay = backend._control_lqr_overlay_shell()
    assert "ts: 0.0500" in lqr_overlay
    assert "cutoff_freq: 2" in lqr_overlay


def test_cybertime_candidate_only_changes_localization_time_source() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_longitudinal_cybertime_candidate.yaml"
    )
    payload = _load_config_with_extends(path)

    planning = payload["algo"]["apollo"]["planning"]
    bridge = payload["algo"]["apollo"]["bridge"]
    vehicle_param = payload["algo"]["apollo"]["vehicle_param"]
    assert planning["enable_reference_line_stitching"] is False
    assert planning["enable_trajectory_stitcher"] is True
    assert bridge["localization_time_source"] == "cyber_time"
    assert vehicle_param["brake_deadzone"] == 0.05


def test_cybertime_republish_candidate_is_explicitly_diagnostic_only() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_longitudinal_cybertime_republish_candidate.yaml"
    )
    payload = _load_config_with_extends(path)

    bridge = payload["algo"]["apollo"]["bridge"]
    claim_grade = bridge["claim_grade"]
    assert payload["run"]["claim_profile"] is False
    assert payload["assist_ledger"]["can_claim_unassisted_natural_driving"] is False
    assert claim_grade == {
        "enabled": False,
        "stale_world_frame_policy": "republish_debug",
        "localization_publish_policy": "allow_cached_republish",
        "chassis_publish_policy": "allow_cached_republish",
    }


def test_replan_threshold_candidate_is_explicitly_diagnostic() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_longitudinal_replan_threshold_candidate.yaml"
    )
    payload = _load_config_with_extends(path)
    assert payload["run"]["claim_profile"] is False
    assert payload["algo"]["apollo"]["planning"]["replan_longitudinal_distance_threshold"] == 3.5

    backend = CyberRTBackend(
        {
            "algo": {
                "apollo": {
                    "planning": {"replan_longitudinal_distance_threshold": 3.5}
                }
            }
        }
    )
    shell = backend._planning_flags_overlay_shell()
    assert "--replan_longitudinal_distance_threshold=3.500" in shell


def test_reference_stitching_probe_does_not_change_trajectory_stitching() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_longitudinal_reference_stitching_probe.yaml"
    )
    payload = _load_config_with_extends(path)
    planning = payload["algo"]["apollo"]["planning"]
    assert payload["run"]["claim_profile"] is False
    assert planning["enable_reference_line_stitching"] is True
    assert planning["enable_trajectory_stitcher"] is True


def test_simtime_reference_stitching_candidate_is_explicitly_diagnostic() -> None:
    path = Path(
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_longitudinal_simtime_reference_stitching_candidate.yaml"
    )
    payload = _load_config_with_extends(path)
    planning = payload["algo"]["apollo"]["planning"]
    bridge = payload["algo"]["apollo"]["bridge"]
    assert payload["run"]["claim_profile"] is False
    assert payload["assist_ledger"]["can_claim_unassisted_natural_driving"] is False
    assert bridge["localization_time_source"] == "sim_time"
    assert planning["enable_reference_line_stitching"] is True
    assert planning["enable_trajectory_stitcher"] is True
