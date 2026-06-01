from pathlib import Path

import yaml

from algo.adapters.autoware import AutowareAdapter


def _make_autoware_map_root(root: Path, town: str = "Town01") -> Path:
    (root / "point_cloud_maps").mkdir(parents=True)
    (root / "vector_maps" / "lanelet2").mkdir(parents=True)
    (root / "point_cloud_maps" / f"{town}.pcd").write_text("pcd placeholder\n")
    (root / "vector_maps" / "lanelet2" / f"{town}.osm").write_text("<osm />\n")
    return root


def test_autoware_map_root_expands_environment(monkeypatch, tmp_path):
    map_root = _make_autoware_map_root(tmp_path / "maps")
    monkeypatch.setenv("AUTOWARE_MAP_ROOT", str(map_root))

    root, town = AutowareAdapter()._infer_map_root(
        {
            "map_root": "${AUTOWARE_MAP_ROOT}",
            "map_path": "${AUTOWARE_MAP_ROOT}",
            "carla_map": "Town01",
        }
    )

    assert root == map_root.resolve()
    assert town == "Town01"


def test_followstop_autoware_enables_native_control_bridge():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    autoware_cfg = cfg["algo"]["autoware"]
    bridge_cfg = cfg["algo"]["autoware"]["carla_control_bridge"]
    control_log_cfg = cfg["record"]["control_log"]

    assert autoware_cfg["rmw_implementation"] == "rmw_cyclonedds_cpp"
    runner = Path("examples/run_followstop.py").read_text()
    assert bridge_cfg["enabled"] is True
    assert bridge_cfg["control_type"] == "autoware_control"
    assert bridge_cfg["control_topic"] == "/control/command/control_cmd"
    assert bridge_cfg["accel_throttle_gain"] == 0.3
    assert bridge_cfg["longitudinal_mode"] == "open_loop"
    assert bridge_cfg["speed_source"] == "carla"
    assert bridge_cfg["speed_feedback_gain"] == 0.25
    assert bridge_cfg["velocity_status_topic"] == "/vehicle/status/velocity_status"
    assert bridge_cfg["positive_accel_brake_inhibit_enabled"] is False
    assert bridge_cfg["accel_feedforward_overspeed_taper_mps"] == 0.0
    assert bridge_cfg["positive_accel_brake_inhibit_speed_margin_mps"] == 0.5
    assert bridge_cfg["positive_accel_overshoot_action"] == "throttle"
    assert bridge_cfg["positive_accel_min_throttle"] == 0.0
    assert bridge_cfg["positive_accel_min_throttle_speed_mps"] == 5.0
    assert bridge_cfg["negative_accel_brake_speed_margin_mps"] == -1.0
    assert bridge_cfg["speed_feedback_transition_coast_sec"] == 0.0
    assert bridge_cfg["speed_feedback_max_throttle_step"] == 1.0
    assert bridge_cfg["speed_feedback_max_brake_step"] == 1.0
    assert "/planning/mission_planning/route" in control_log_cfg["extra_topics"]
    assert "/planning/scenario_planning/trajectory" in control_log_cfg["extra_topics"]
    assert "/planning/scenario_planning/velocity_smoother/trajectory" in control_log_cfg["extra_topics"]
    assert "/planning/trajectory" in control_log_cfg["extra_topics"]
    assert "/planning/scenario_planning/max_velocity" in control_log_cfg["extra_topics"]
    assert "/planning/scenario_planning/max_velocity_default" in control_log_cfg["extra_topics"]
    assert "/planning/scenario_planning/current_max_velocity" in control_log_cfg["extra_topics"]
    assert "/planning/scenario_planning/max_velocity_candidates" in control_log_cfg["extra_topics"]
    assert "/planning/scenario_planning/velocity_limit" in control_log_cfg["extra_topics"]
    assert "/control/trajectory_follower/control_cmd" in control_log_cfg["extra_topics"]
    assert "/control/trajectory_follower/longitudinal/diagnostic" in control_log_cfg["extra_topics"]
    assert "/control/trajectory_follower/longitudinal/slope_angle" in control_log_cfg["extra_topics"]
    assert "/control/trajectory_follower/longitudinal/stop_reason" in control_log_cfg["extra_topics"]
    extra_topic_types = control_log_cfg["extra_topic_types"]
    assert extra_topic_types["/planning/planning_factors/obstacle_stop"] == (
        "autoware_internal_planning_msgs/msg/PlanningFactorArray"
    )
    assert extra_topic_types["/planning/scenario_planning/status/stop_reasons"] == (
        "tier4_planning_msgs/msg/StopReasonArray"
    )
    assert extra_topic_types["/control/trajectory_follower/longitudinal/stop_reason"] == (
        "tier4_planning_msgs/msg/StopReasonArray"
    )
    assert "extra_topic_types.get(extra_topic)" in runner
    assert "/localization/kinematic_state" in control_log_cfg["extra_topics"]
    assert "/vehicle/status/velocity_status" in control_log_cfg["extra_topics"]
    assert "/vehicle/status/control_mode" in control_log_cfg["extra_topics"]
    assert "/vehicle/status/gear_status" in control_log_cfg["extra_topics"]
    assert "/system/operation_mode/state" in control_log_cfg["extra_topics"]
    assert "/system/fail_safe/mrm_state" in control_log_cfg["extra_topics"]
    assert "/diagnostics" in control_log_cfg["extra_topics"]
    assert "/diagnostics_agg" in control_log_cfg["extra_topics"]


def test_followstop_autoware_declares_demo_recording_defaults():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    record_cfg = cfg["record"]
    runner = Path("examples/run_followstop.py").read_text()

    assert record_cfg["visual"]["modes"] == ["dual_cam"]
    assert record_cfg["visual"]["dual_cam_third_person_only"] is True
    assert record_cfg["rosbag"]["enable"] is True
    assert record_cfg["rosbag"]["out"] == "rosbag2/autoware_demo"
    assert record_cfg["autoware_operator_view"]["enabled"] is True
    assert record_cfg["autoware_operator_view"]["capture_region"] == "1280x720+0,0"
    assert 'stack_cfg != "autoware"' in runner


def test_followstop_autoware_requires_localization_before_route_with_retry():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    goal_cfg = cfg["algo"]["autoware"]["goal"]

    assert goal_cfg["require_localization_before_route"] is True
    assert goal_cfg["localization_wait_s"] > 0
    assert goal_cfg["tf_wait_s"] > 0
    assert goal_cfg["localization_retry_wait_s"] > 0
    assert goal_cfg["tf_retry_wait_s"] > 0
    assert goal_cfg["route_ready_wait_s"] >= (
        goal_cfg["localization_wait_s"]
        + goal_cfg["tf_wait_s"]
        + goal_cfg["localization_retry_wait_s"]
        + goal_cfg["tf_retry_wait_s"]
    )
    assert goal_cfg["tick_while_waiting_for_route"] is True
    assert goal_cfg["route_wait_tick_timeout_s"] > 0
    assert goal_cfg["route_wait_tick_sleep_s"] > 0
    assert goal_cfg["engage_retry_timeout_s"] > 0
    assert goal_cfg["engage_retry_period_s"] > 0
    assert goal_cfg["engage_ready_topics"] == ["/planning/trajectory"]
    assert goal_cfg["engage_ready_wait_s"] > 0
    assert goal_cfg["engage_ready_probe_timeout_s"] > 0
    assert goal_cfg["require_engage_ready_topics"] is True


def test_followstop_autoware_starts_bridge_after_scenario_spawn():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    autoware_cfg = cfg["algo"]["autoware"]
    run_cfg = cfg["run"]
    carla_cfg = cfg["runtime"]["carla"]
    runner = Path("examples/run_followstop.py").read_text()

    assert autoware_cfg["start_after_scenario_spawn"] is True
    assert carla_cfg["launch_with_map"] is False
    assert carla_cfg["bootstrap_stability_window_s"] > 0
    assert run_cfg["post_spawn_visibility_ticks"] > 0
    assert run_cfg["require_post_spawn_actor_visibility"] is True
    assert "adapter_start_deferred_until_scenario_spawn" in runner
    assert "adapter_start_after_scenario_spawn" in runner
    assert "scenario_actor_visibility_ok" in runner
    assert "scenario_actor_visibility_failed" in runner


def test_followstop_autoware_uses_carla_diagnostics_contract():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    launch_args = cfg["algo"]["autoware"]["launch_extra_args"]

    assert "is_simulation:=true" in launch_args
    assert "launch_system_monitor:=false" in launch_args
    assert "launch_dummy_diag_publisher:=true" in launch_args
    assert "autoware-carla.yaml" in launch_args


def test_followstop_autoware_declares_planning_max_vel_override_default_disabled():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    autoware_cfg = cfg["algo"]["autoware"]
    adapter = Path("algo/adapters/autoware.py").read_text()
    compose = Path("algo/baselines/autoware/docker/compose.yaml").read_text()
    script = Path("algo/baselines/autoware/docker/entrypoint.sh").read_text()

    assert autoware_cfg["planning_common_max_vel_mps"] is None
    assert autoware_cfg["planning_common_nominal_max_acc_mps2"] is None
    assert autoware_cfg["planning_common_nominal_min_acc_mps2"] is None
    assert autoware_cfg["planning_common_limit_max_acc_mps2"] is None
    assert autoware_cfg["planning_common_limit_min_acc_mps2"] is None
    assert "planning_common_max_vel_mps" in adapter
    assert "planning_common_nominal_max_acc_mps2" in adapter
    assert "planning_common_nominal_min_acc_mps2" in adapter
    assert "planning_common_limit_max_acc_mps2" in adapter
    assert "planning_common_limit_min_acc_mps2" in adapter
    assert "AUTOWARE_PLANNING_COMMON_MAX_VEL_MPS" in compose
    assert "AUTOWARE_PLANNING_COMMON_NOMINAL_MAX_ACC_MPS2" in compose
    assert "AUTOWARE_PLANNING_COMMON_NOMINAL_MIN_ACC_MPS2" in compose
    assert "AUTOWARE_PLANNING_COMMON_LIMIT_MAX_ACC_MPS2" in compose
    assert "AUTOWARE_PLANNING_COMMON_LIMIT_MIN_ACC_MPS2" in compose
    assert "AUTOWARE_PLANNING_COMMON_PARAM_PATH" in compose
    assert "apply_planning_common_max_vel_override" in script
    assert "autoware_planning_common_override.json" in script
    assert "common.param.before_max_vel_override.yaml" in script
    assert "common.param.after_max_vel_override.yaml" in script
    assert '"requested_values"' in script
    assert '"original_values"' in script


def test_followstop_autoware_declares_velocity_limit_probe_default_disabled():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    probe_cfg = cfg["algo"]["autoware"]["velocity_limit_probe"]
    adapter = Path("algo/adapters/autoware.py").read_text()
    compose = Path("algo/baselines/autoware/docker/compose.yaml").read_text()
    script = Path("algo/baselines/autoware/docker/entrypoint.sh").read_text()

    assert probe_cfg["enabled"] is False
    assert probe_cfg["topic"] == "/planning/scenario_planning/max_velocity_default"
    assert probe_cfg["max_velocity_mps"] is None
    assert "velocity_limit_probe" in adapter
    assert "AUTOWARE_VELOCITY_LIMIT_PROBE_ENABLED" in adapter
    assert "AUTOWARE_VELOCITY_LIMIT_PROBE_MAX_VELOCITY_MPS" in compose
    assert "velocity_limit_publisher.py" in script
    assert "start_velocity_limit_probe" in script
    assert "DurabilityPolicy.TRANSIENT_LOCAL" in Path(
        "tbio/ros2/tools/velocity_limit_publisher.py"
    ).read_text()
    control_logger = Path("tbio/ros2/tools/control_logger.py").read_text()
    assert '"max_velocity"' in control_logger
    assert '"sender"' in control_logger


def test_followstop_autoware_pregoal_warmup_actively_ticks_carla():
    runner = Path("examples/run_followstop.py").read_text()

    assert "pre-goal CARLA warmup" in runner
    assert "no other tick owner before harness.run()" in runner
    assert "world.tick()" in runner
    assert "route_ready_wait_s" in runner
    assert "engage_retry_timeout_s" in runner
    assert "engage_ready_topics" in runner
    assert "require_engage_ready_topics" in runner
    assert "_start_carla_tick_pump" in runner
    assert "started CARLA tick pump while waiting for Autoware localization/tf/route" in runner


def test_followstop_acceptance_keeps_safety_failures_authoritative():
    runner = Path("examples/run_followstop.py").read_text()

    assert "safety_failures_from_summary" in runner
    assert '"safety"' in runner
    assert 'summary_data["success"] = acceptance["success"]' in runner


def test_followstop_autoware_enables_control_health_acceptance_gate():
    cfg = yaml.safe_load(Path("configs/io/examples/followstop_autoware.yaml").read_text())
    control_health = cfg["acceptance"]["control_health"]
    runner = Path("examples/run_followstop.py").read_text()

    assert control_health["enabled"] is True
    assert control_health["fail_on_missing"] is True
    assert control_health["max_applied_throttle_brake_switch_count"] <= 10
    assert "applied_control_health_from_timeseries" in runner
    assert "control_health_failures_from_check" in runner
    assert "CONTROL_HEALTH_APPLIED_OSCILLATION" in Path(
        "carla_testbed/evaluation/acceptance.py"
    ).read_text()


def test_autoware_entrypoint_stages_local_map_projector_for_carla_maps():
    script = Path("algo/baselines/autoware/docker/entrypoint.sh").read_text()

    assert "map_projector_info.yaml" in script
    assert "local_x" in script
    assert "local_y" in script
    assert "projector_type: Local" in script


def test_autoware_gt_bridge_publishes_minimal_planning_inputs():
    bridge = Path("algo/baselines/autoware/gt/gt_bridge_node.py").read_text()

    assert "/perception/occupancy_grid_map/map" in bridge
    assert "/perception/obstacle_segmentation/pointcloud" in bridge
    assert "/perception/traffic_light_recognition/traffic_signals" in bridge
    assert "/initialpose3d" in bridge
    assert "/sensing/imu/imu_data" in bridge
    assert "OccupancyGrid" in bridge
    assert "PointCloud2" in bridge
    assert "TrafficLightGroupArray" in bridge
    assert "PredictedPath" in bridge
    assert "def _build_constant_velocity_predicted_path" in bridge
    assert "obj.kinematics.predicted_paths = [" in bridge
    assert "PoseWithCovarianceStamped" in bridge
    assert "Imu" in bridge
    assert "cloud.header.frame_id = self.base_frame" in bridge
    assert "wait_for_tick(0.2)" in bridge
    assert 'self.create_publisher(Clock, "/clock", qos)' in bridge
    assert "self.world.get_snapshot().timestamp.elapsed_seconds" in bridge
    assert "def _sync_world_snapshot" in bridge
    assert "self.world.wait_for_tick" in bridge
    assert "self._publish_clock(now)" in bridge
    assert "autoware_steer_sign" in bridge
    assert "self.autoware_steer_sign * float(ctrl.steer) * self.max_steer_angle" in bridge
    assert "odom.twist.twist = carla_to_ros_twist(ego.get_velocity())" not in bridge
    assert "odom.twist.twist = vehicle_frame_twist(longitudinal_v, lateral_v, heading_rate)" in bridge


def test_autoware_entrypoint_uses_same_steering_contract_for_control_and_feedback():
    compose = Path("algo/baselines/autoware/docker/compose.yaml").read_text()
    script = Path("algo/baselines/autoware/docker/entrypoint.sh").read_text()

    assert "AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_OVERSPEED_TAPER_MPS" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_ENABLED" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_OVERSHOOT_ACTION" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_MIN_THROTTLE_SPEED_MPS" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_NEGATIVE_ACCEL_BRAKE_SPEED_MARGIN_MPS" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_TRANSITION_COAST_SEC" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_THROTTLE_STEP" in compose
    assert "AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_MAX_BRAKE_STEP" in compose
    assert "--max-steer-angle \"$AUTOWARE_CONTROL_BRIDGE_MAX_STEER_ANGLE\"" in script
    assert "--autoware-steer-sign \"$AUTOWARE_CONTROL_BRIDGE_STEER_SIGN\"" in script
    assert "--accel-throttle-gain \"$AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN\"" in script
    assert "--longitudinal-mode \"$AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE\"" in script
    assert "--speed-feedback-gain \"$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_GAIN\"" in script
    assert "--speed-feedback-brake-gain \"$AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_BRAKE_GAIN\"" in script
    assert "--accel-feedforward-overspeed-taper-mps" in script
    assert "--positive-accel-brake-inhibit-min-accel-mps2" in script
    assert "--positive-accel-brake-inhibit-speed-margin-mps" in script
    assert "--positive-accel-overshoot-action" in script
    assert "--positive-accel-min-throttle" in script
    assert "--positive-accel-min-throttle-speed-mps" in script
    assert "--negative-accel-brake-speed-margin-mps" in script
    assert "--speed-feedback-transition-coast-sec" in script
    assert "--speed-feedback-max-throttle-step" in script
    assert "--speed-feedback-max-brake-step" in script
    assert "--speed-source \"$AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE\"" in script
    assert "--velocity-status-topic \"$AUTOWARE_CONTROL_BRIDGE_VELOCITY_STATUS_TOPIC\"" in script


def test_autoware_entrypoint_passes_use_sim_time_to_launch():
    adapter = Path("algo/adapters/autoware.py").read_text()
    script = Path("algo/baselines/autoware/docker/entrypoint.sh").read_text()

    assert 'env.setdefault("USE_SIM_TIME", "true"' in adapter
    assert "USE_SIM_TIME={env.get('USE_SIM_TIME','')}" in adapter
    assert "use_sim_time:=${USE_SIM_TIME:-true}" in script
    assert '"${sim_time_arg[@]}"' in script


def test_autoware_entrypoint_relays_velocity_smoother_trajectory_to_control_topic():
    compose = Path("algo/baselines/autoware/docker/compose.yaml").read_text()
    script = Path("algo/baselines/autoware/docker/entrypoint.sh").read_text()

    assert "AUTOWARE_TRAJECTORY_RELAY_ENABLED" in compose
    assert "/planning/scenario_planning/velocity_smoother/trajectory" in script
    assert "/planning/trajectory" in script
    assert "carla_testbed_trajectory_relay" in script


def test_autoware_topic_logger_preserves_trajectory_summary():
    logger = Path("tbio/ros2/tools/control_logger.py").read_text()
    runner = Path("examples/run_followstop.py").read_text()

    assert "_node_name_for_topic(topic)" in logger
    assert "control_logger_{safe}" in logger
    assert "trajectory_point_count" in logger
    assert "trajectory_velocity_mps" in logger
    assert '"velocity": getattr(lon, "velocity", None)' in logger
    assert "velocity_report" in logger
    assert "_diagnostic_status_summary" in logger
    assert 'out["status"] = _diagnostic_status_summary' in logger
    assert '"pose": _pose_summary' in logger
    assert 'out["twist"] = _twist_summary' in logger
    assert 'elif hasattr(msg, "stamp")' in logger
    assert 'if hasattr(msg, "control_time")' in logger
    assert "_has_non_header_payload(out)" in logger
    assert "analyze_autoware_control_run" in runner
    assert "analysis\" / \"autoware_control" in runner
    assert '"steering_tire_angle"' in logger
    assert '"is_autoware_control_enabled"' in logger
    assert '"behavior"' in logger
    assert 'out["data"] = list(getattr(msg, "data", []) or [])' in logger
    assert "first_stop_velocity_indices" in logger
    assert "dense_near_field_count = 80" in logger
    assert "range(min(len(pts), dense_near_field_count))" in logger
    assert "first_nonzero_velocity_index" in logger
    assert "trajectory_points_sample" in logger


def test_autoware_adapter_exports_acceleration_throttle_gain():
    adapter = Path("algo/adapters/autoware.py").read_text()
    bridge = Path("algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py").read_text()

    assert "AUTOWARE_CONTROL_BRIDGE_ACCEL_THROTTLE_GAIN" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_LONGITUDINAL_MODE" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_GAIN" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_SPEED_SOURCE" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_ACCEL_FEEDFORWARD_OVERSPEED_TAPER_MPS" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_BRAKE_INHIBIT_ENABLED" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_POSITIVE_ACCEL_OVERSHOOT_ACTION" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_NEGATIVE_ACCEL_BRAKE_SPEED_MARGIN_MPS" in adapter
    assert "AUTOWARE_CONTROL_BRIDGE_SPEED_FEEDBACK_TRANSITION_COAST_SEC" in adapter
    assert "accel_throttle_gain" in adapter
    assert "self.accel_throttle_gain" in bridge
    assert "max(float(accel), 0.0) * self.accel_throttle_gain" in bridge
    assert "accel_feedforward_overspeed_taper_mps" in bridge
    assert "accel_feedforward *= clamp(taper, 0.0, 1.0)" in bridge
    assert "positive_accel_brake_inhibit_enabled" in bridge
    assert "positive_accel_overshoot_action" in bridge
    assert 'self.positive_accel_overshoot_action == "coast"' in bridge
    assert 'self.positive_accel_overshoot_action == "coast_all"' in bridge
    assert "positive_accel_min_throttle" in bridge
    assert "positive_accel_min_throttle_speed_mps" in bridge
    assert "ctrl.throttle = max(float(ctrl.throttle), self.positive_accel_min_throttle)" in bridge
    assert "negative_accel_brake_speed_margin_mps" in bridge
    assert "def _negative_accel_brake_allowed" in bridge
    assert "speed_feedback_transition_coast_sec" in bridge
    assert "_apply_speed_feedback_debounce" in bridge
    assert '"speed_feedback"' in bridge
    assert "speed_error = float(target_speed) - current_speed" in bridge
    assert "target_speed={self._fmt_optional(target_speed)}" in bridge
    assert "VelocityReport" in bridge
    assert "ros_velocity_status" in bridge


def test_autoware_control_bridge_resolves_final_throttle_brake_conflict():
    bridge = Path("algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py").read_text()

    assert "def _resolve_throttle_brake_conflict" in bridge
    assert "self._resolve_throttle_brake_conflict(ctrl)" in bridge
    assert "CARLA never receives simultaneous throttle and brake" in bridge
    assert "_throttle_brake_conflict_resolved_count" in bridge
    assert "release the opposite actuator immediately" in bridge


def test_autoware_control_bridge_supports_coast_all_positive_accel_overspeed():
    bridge = Path("algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py").read_text()

    assert 'choices=["throttle", "coast", "coast_all"]' in bridge
    assert "coast_positive_overspeed" in bridge
    assert "positive acceleration with target velocity below ego" in bridge
