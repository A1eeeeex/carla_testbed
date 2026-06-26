from __future__ import annotations

import csv
import builtins
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

import yaml

import tools.analyze_carla_startup_probe_corpus as startup_probe_corpus
import tools.inspect_town01_live_route_progress as town01_live_progress_probe
import tools.analyze_town01_junction_startup_lineage as town01_startup_lineage
import tools.run_apollo_actuator_semantic_suite as apollo_semantic_suite
import tools.run_town01_capability_online_chain as capability_online_chain
import tools.run_town01_capability_online_pack as capability_online_pack
from tools.calibration_pipeline_common import evaluate_capture_validity, render_capture_validity_markdown
import tools.render_calibration_baseline_reference_summary as calibration_baseline_summary_renderer
import tools.run_unified_calibration_pipeline as unified_calibration_pipeline

from tbio.carla.launcher import (
    CarlaLauncher,
    _candidate_carla_ports,
    _carla_pids_matching_port_arg,
    _port_likely_owned_by_carla,
)
from tbio.carla.launch_policy import resolve_carla_launch_policy
from tbio.backends.cyberrt import CyberRTBackend
from carla_testbed.utils.town01_route_health import (
    _compute_curve_semantic_window,
    _compute_curve_tracking_health_summary,
    _compute_direct_metric_consistency,
    _compute_longitudinal_tracking_health_summary,
    _compute_planning_trajectory_type_summary,
    _compute_route_metrics,
    _compute_planning_control_alignment,
    _build_state_timeline,
    _planning_nonzero_ratio_check,
    _extract_carla_bootstrap_summary,
    _extract_command_materialization_summary,
    evaluate_runtime_contract,
    write_platform_report,
    finalize_town01_run,
    load_route_corpus,
    route_id_from_metadata,
    select_route_ids,
)
from tools.run_town01_route_health import (
    CAPABILITY_PROFILE_DEFAULT_SUBSETS,
    CarlaWorldReadyError,
    CAPABILITY_PROFILE_OVERRIDE_PRESET_FILES,
    GET_WORLD_RETRY_DELAY_S,
    _analyze_batch,
    _build_parser,
    _build_base_overrides,
    _cleanup_carla_processes,
    _default_capability_profile_overrides,
    _compose_prestart_carla_extra_args,
    _compose_prestart_carla_env_overrides,
    _connect_world,
    _display_probe_monitor_count,
    _flags_for_run,
    _invoke_run,
    _load_overrides_file,
    _load_route_ids_file,
    _memory_preflight_snapshot,
    _overrides_for_flags,
    _preview_runtime_contract,
    _prestart_carla,
    _publish_outputs,
    _resolve_recommended_subset,
    _resolve_route_ids,
    _ros_sourced_env,
    _startup_probe_attempt_rows,
    _startup_probe_failure_family,
    _startup_probe_payload,
    _startup_retry_decision,
    _startup_retry_policy_payload,
)
from tools.run_unified_calibration_pipeline import (
    _emit_replay_input_contract_artifacts,
    _fallback_capture_entry_payload,
    _replay_input_contract_markdown,
    _replay_input_contract_payload,
    _resolve_baseline_reference_details,
    _valid_capture_entry_payload,
)
from tools.run_town01_route_health import _discover_batch_runs, _resolve_run_dir
from tools.probe_carla_startup_modes import MODE_PRESETS, _classify_attempt
from tools.probe_carla_handshake_trigger import _classify_handshake_effect, _write_report
from tools.analyze_town01_capability_subsets import render_report as render_capability_subset_report
from tools.analyze_town01_capability_progress import build_capability_progress_rows
from tools.analyze_town01_capability_review_manifest import build_manifest_review_rows, render_report as render_capability_review_manifest_report
from tools.run_town01_capability_missing_history_packs import (
    build_missing_history_entries,
    render_runbook as render_missing_history_runbook,
)
from tools.run_town01_capability_online_pack import (
    _effective_launch_attempts,
    _default_runtime_flags_for_capability,
    _build_parser as _build_online_pack_parser,
    build_online_command,
    estimate_online_time_budget,
    resolve_startup_profile_sequence,
)
from tools.run_town01_capability_online_chain import (
    _build_parser as _build_online_chain_parser,
    build_online_chain_plan,
    estimate_online_chain_time_budget,
)
from tools.run_town01_capability_review_packs import (
    _build_parser as _build_review_pack_parser,
    build_capability_review_plan,
    freeze_capability_review_plan,
    render_runbook,
)
from tools.refresh_town01_route_corpus_health import _aggregate_route_evidence, _updated_route


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _load_bridge_unit_test_module():
    bridge_path = Path.cwd() / "tools" / "apollo10_cyber_bridge" / "bridge.py"
    module_name = "bridge_unit_test_stubbed"

    fake_rclpy = types.ModuleType("rclpy")
    fake_google_mod = types.ModuleType("google")
    fake_google_protobuf_mod = types.ModuleType("google.protobuf")
    fake_empty_pb2_mod = types.ModuleType("google.protobuf.empty_pb2")
    fake_node_mod = types.ModuleType("rclpy.node")
    fake_exec_mod = types.ModuleType("rclpy.executors")
    fake_qos_mod = types.ModuleType("rclpy.qos")
    fake_nav_msg_mod = types.ModuleType("nav_msgs.msg")
    fake_std_msg_mod = types.ModuleType("std_msgs.msg")
    fake_ackermann_msg_mod = types.ModuleType("ackermann_msgs.msg")
    fake_geom_msg_mod = types.ModuleType("geometry_msgs.msg")
    fake_vision_msg_mod = types.ModuleType("vision_msgs.msg")
    fake_viz_msg_mod = types.ModuleType("visualization_msgs.msg")

    class _FakeNode:
        pass

    fake_node_mod.Node = _FakeNode
    fake_exec_mod.MultiThreadedExecutor = object
    fake_qos_mod.QoSHistoryPolicy = object
    fake_qos_mod.QoSProfile = object
    fake_qos_mod.QoSReliabilityPolicy = object
    fake_qos_mod.qos_profile_sensor_data = object()
    fake_nav_msg_mod.Odometry = object
    fake_std_msg_mod.String = object
    fake_ackermann_msg_mod.AckermannDriveStamped = object
    fake_geom_msg_mod.Twist = object
    fake_std_msg_mod.Float32MultiArray = object
    fake_vision_msg_mod.Detection3DArray = object
    fake_viz_msg_mod.MarkerArray = object

    spec = importlib.util.spec_from_file_location(module_name, bridge_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "rclpy": fake_rclpy,
            "google": fake_google_mod,
            "google.protobuf": fake_google_protobuf_mod,
            "google.protobuf.empty_pb2": fake_empty_pb2_mod,
            "rclpy.node": fake_node_mod,
            "rclpy.executors": fake_exec_mod,
            "rclpy.qos": fake_qos_mod,
            "nav_msgs.msg": fake_nav_msg_mod,
            "std_msgs.msg": fake_std_msg_mod,
            "ackermann_msgs.msg": fake_ackermann_msg_mod,
            "geometry_msgs.msg": fake_geom_msg_mod,
            "vision_msgs.msg": fake_vision_msg_mod,
            "visualization_msgs.msg": fake_viz_msg_mod,
            module_name: module,
        },
    ):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


def _load_bridge_unit_test_module_without_ros2():
    bridge_path = Path.cwd() / "tools" / "apollo10_cyber_bridge" / "bridge.py"
    module_name = "bridge_unit_test_no_ros2"

    fake_carla_mod = types.ModuleType("carla")
    fake_carla_mod.Client = object
    fake_carla_mod.VehicleControl = object
    fake_carla_mod.World = object
    fake_carla_mod.Vehicle = object
    fake_carla_mod.Actor = object
    fake_carla_mod.Location = object
    fake_carla_mod.Rotation = object
    fake_carla_mod.Transform = object

    real_import = builtins.__import__
    blocked_roots = {
        "rclpy",
        "nav_msgs",
        "std_msgs",
        "ackermann_msgs",
        "geometry_msgs",
        "vision_msgs",
        "visualization_msgs",
    }

    def _patched_import(name, globals=None, locals=None, fromlist=(), level=0):
        root = str(name or "").split(".", 1)[0]
        if root in blocked_roots:
            raise ImportError(f"blocked_for_direct_no_ros2_test:{name}")
        return real_import(name, globals, locals, fromlist, level)

    spec = importlib.util.spec_from_file_location(module_name, bridge_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    with mock.patch("builtins.__import__", side_effect=_patched_import), mock.patch.dict(
        sys.modules,
        {
            "carla": fake_carla_mod,
            module_name: module,
        },
    ):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


class Town01RouteHealthMainlineTests(unittest.TestCase):
    def assertHostPythonExecIsPortable(self, value: str, msg: str | None = None) -> None:
        self.assertIn(
            value,
            {"${CARLA16_PYTHON}", "/home/ubuntu/miniconda3/envs/carla16/bin/python3"},
            msg,
        )

    def test_town01_route_health_profiles_use_rear_axle_localization_back_offset(self) -> None:
        for rel_path in (
            "configs/io/examples/town01_apollo_route_health.yaml",
            "configs/io/examples/town01_apollo_route_health_relaxed.yaml",
            "configs/io/examples/town01_apollo_route_health_minimal.yaml",
            "configs/io/examples/town01_apollo_route_health_strict.yaml",
        ):
            cfg = yaml.safe_load((Path.cwd() / rel_path).read_text(encoding="utf-8"))
            self.assertAlmostEqual(
                float(cfg["algo"]["apollo"]["bridge"]["localization_back_offset_m"]),
                1.4235,
                places=4,
                msg=rel_path,
            )

    def test_canonical_configs_use_planning_ready_control_by_default(self) -> None:
        for rel_path in (
            "configs/io/examples/town01_apollo_route_health.yaml",
            "configs/io/examples/town01_apollo_route_health_relaxed.yaml",
        ):
            cfg = yaml.safe_load((Path.cwd() / rel_path).read_text(encoding="utf-8"))
            docker_cfg = cfg["algo"]["apollo"]["docker"]
            self.assertTrue(docker_cfg["defer_control_until_planning_ready"], rel_path)
            self.assertEqual(docker_cfg["control_start_gate"], "planning_ready", rel_path)
            self.assertEqual(docker_cfg["deferred_control_start_mode"], "dag", rel_path)
            self.assertTrue(docker_cfg["deferred_control_start_async"], rel_path)
            self.assertFalse(docker_cfg["deferred_control_disable_bvar_dump"], rel_path)
            self.assertEqual(docker_cfg["control_planning_ready_min_nonempty_count"], 1, rel_path)
            self.assertEqual(docker_cfg["control_planning_ready_min_sequence_num"], 5, rel_path)
            self.assertHostPythonExecIsPortable(docker_cfg["host_python_exec"], rel_path)
        probe_cfg = yaml.safe_load(
            (Path.cwd() / "configs/io/examples/town01_apollo_route_health_relaxed_probe.yaml").read_text(
                encoding="utf-8"
            )
        )
        self.assertTrue(probe_cfg["algo"]["apollo"]["docker"]["defer_control_until_planning_ready"])
        self.assertEqual(probe_cfg["algo"]["apollo"]["docker"]["control_start_gate"], "planning_ready")
        self.assertEqual(probe_cfg["algo"]["apollo"]["docker"]["deferred_control_start_mode"], "dag")
        self.assertTrue(probe_cfg["algo"]["apollo"]["docker"]["deferred_control_start_async"])
        self.assertTrue(probe_cfg["algo"]["apollo"]["docker"]["deferred_control_disable_bvar_dump"])
        self.assertTrue(probe_cfg["algo"]["apollo"]["docker"]["control_require_chassis_ready"])
        self.assertEqual(probe_cfg["algo"]["apollo"]["docker"]["control_chassis_ready_min_count"], 20)
        self.assertHostPythonExecIsPortable(probe_cfg["algo"]["apollo"]["docker"]["host_python_exec"])
        self.assertEqual(probe_cfg["run"]["ticks"], 320)
        self.assertEqual(probe_cfg["run"]["post_fail_steps"], 120)

    def test_planning_ready_docker_profiles_declare_async_deferred_control_start(self) -> None:
        for path in sorted((Path.cwd() / "configs" / "io" / "examples").glob("*.yaml")):
            cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            docker_cfg = (((cfg.get("algo") or {}).get("apollo") or {}).get("docker") or {})
            if docker_cfg.get("control_start_gate") != "planning_ready":
                continue
            self.assertTrue(
                docker_cfg.get("deferred_control_start_async"),
                str(path.relative_to(Path.cwd())),
            )

    def test_mainline_configs_disable_dreamview_by_default(self) -> None:
        for rel_path in (
            "configs/io/examples/town01_apollo_route_health.yaml",
            "configs/io/examples/town01_apollo_route_health_relaxed.yaml",
            "configs/io/examples/town01_apollo_route_health_minimal.yaml",
        ):
            cfg = yaml.safe_load((Path.cwd() / rel_path).read_text(encoding="utf-8"))
            dreamview_cfg = cfg["algo"]["apollo"]["dreamview"]
            self.assertFalse(dreamview_cfg["enabled"], rel_path)
            self.assertFalse(dreamview_cfg["auto_start"], rel_path)

    def test_baseline_recovery_config_tracks_old_097_semantics(self) -> None:
        cfg = yaml.safe_load(
            (Path.cwd() / "configs/io/examples/town01_apollo_route_health_baseline_recovery.yaml").read_text(
                encoding="utf-8"
            )
        )
        docker_cfg = cfg["algo"]["apollo"]["docker"]
        dreamview_cfg = cfg["algo"]["apollo"]["dreamview"]
        routing_cfg = cfg["algo"]["apollo"]["routing"]
        planning_cfg = cfg["algo"]["apollo"]["planning"]
        self.assertEqual(cfg["run"]["ticks"], 320)
        self.assertEqual(cfg["run"]["post_fail_steps"], 120)
        self.assertTrue(dreamview_cfg["enabled"])
        self.assertTrue(dreamview_cfg["auto_start"])
        self.assertTrue(docker_cfg["defer_control_until_planning_ready"])
        self.assertEqual(docker_cfg["control_start_gate"], "planning_ready")
        self.assertEqual(docker_cfg["deferred_control_start_mode"], "dag")
        self.assertFalse(docker_cfg["deferred_control_disable_bvar_dump"])
        self.assertEqual(
            docker_cfg["control_runtime_overlay_source_dirs"],
            [
                "/opt/apollo/neo/.codex_backup/20260331_004431/control_runtime_refresh/control_component",
                "/opt/apollo/neo/.codex_backup/20260331_004431/control_runtime_refresh/submodules",
                "/opt/apollo/neo/.codex_backup/20260330_170640/control_runtime",
                "/opt/apollo/neo/.codex_backup/20260330_170358/common_msgs/control_msgs",
            ],
        )
        self.assertHostPythonExecIsPortable(docker_cfg["host_python_exec"])
        self.assertFalse(routing_cfg["defer_long_goal_until_route_debug_ready"])
        self.assertFalse(planning_cfg["acc_only_mode"])
        self.assertFalse(planning_cfg["longitudinal_only_pipeline"])
        self.assertFalse(planning_cfg["longitudinal_only_keep_lane_follow_path"])

    def test_host_bridge_python_exec_resolves_env_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            python_path = Path(tmpdir) / "python"
            python_path.write_text("", encoding="utf-8")
            backend = CyberRTBackend(
                {"algo": {"apollo": {"docker": {"host_python_exec": "${CARLA16_PYTHON}"}}}}
            )
            with mock.patch.dict("os.environ", {"CARLA16_PYTHON": str(python_path)}):
                self.assertEqual(backend._host_bridge_python_exec(), str(python_path))

    def test_host_bridge_python_exec_defaults_carla16_placeholder_to_current_python(self) -> None:
        backend = CyberRTBackend(
            {"algo": {"apollo": {"docker": {"host_python_exec": "${CARLA16_PYTHON}"}}}}
        )
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(backend._host_bridge_python_exec(), sys.executable)

    def test_behavior_recovery_config_extends_baseline_recovery_for_long_behavior_eval(self) -> None:
        cfg = yaml.safe_load(
            (Path.cwd() / "configs/io/examples/town01_apollo_route_health_behavior_recovery.yaml").read_text(
                encoding="utf-8"
            )
        )
        docker_cfg = cfg["algo"]["apollo"]["docker"]
        self.assertEqual(cfg["run"]["ticks"], 700)
        self.assertEqual(cfg["run"]["post_fail_steps"], 300)
        self.assertTrue(cfg["algo"]["apollo"]["dreamview"]["enabled"])
        self.assertTrue(docker_cfg["defer_control_until_planning_ready"])
        self.assertEqual(docker_cfg["control_start_gate"], "planning_ready")
        self.assertEqual(docker_cfg["deferred_control_start_mode"], "dag")
        self.assertFalse(docker_cfg["deferred_control_disable_bvar_dump"])
        self.assertEqual(
            docker_cfg["control_runtime_overlay_source_dirs"],
            [
                "/opt/apollo/neo/.codex_backup/20260331_004431/control_runtime_refresh/control_component",
                "/opt/apollo/neo/.codex_backup/20260331_004431/control_runtime_refresh/submodules",
                "/opt/apollo/neo/.codex_backup/20260330_170640/control_runtime",
                "/opt/apollo/neo/.codex_backup/20260330_170358/common_msgs/control_msgs",
            ],
        )

    def test_direct_candidate_config_explicitly_uses_cyber_direct_without_ros2_runtime(self) -> None:
        cfg = yaml.safe_load(
            (
                Path.cwd()
                / "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
            ).read_text(encoding="utf-8")
        )
        apollo_cfg = cfg["algo"]["apollo"]
        direct_cfg = apollo_cfg["direct_bridge"]
        self.assertEqual(apollo_cfg["transport_mode"], "carla_direct")
        self.assertEqual(direct_cfg["route_command_mode"], "cyber_direct")
        self.assertTrue(direct_cfg["require_no_ros2_runtime"])
        self.assertFalse(apollo_cfg["dreamview"]["enabled"])
        self.assertFalse(apollo_cfg["dreamview"]["auto_start"])
        self.assertFalse(apollo_cfg["carla_control_bridge"]["enabled"])
        self.assertTrue(cfg["runtime"]["carla"]["disable_native_ros2_arg"])

    def test_run_parser_uses_slow_host_carla_defaults(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["run", "--route-id", "town01_rh_spawn068_goal068"])
        self.assertEqual(args.carla_launch_attempts, 1)
        self.assertEqual(args.carla_world_ready_timeout_sec, 180.0)
        self.assertEqual(args.carla_retry_delay_sec, 2.0)
        self.assertEqual(args.carla_no_retry_failure_families, "")
        self.assertFalse(args.carla_launcher_auto_recovery)

    def test_run_parser_supports_nonretry_failure_families(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-no-retry-failure-families",
                "rpc_ready_followup_missing_eof_alive,rpc_not_ready_no_listener",
            ]
        )
        self.assertEqual(
            args.carla_no_retry_failure_families,
            "rpc_ready_followup_missing_eof_alive,rpc_not_ready_no_listener",
        )

    def test_run_parser_supports_stage6_reference_line_flags(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--enable-stage6-reference-line",
                "--clear-lane-follow-cache-on-new-command",
            ]
        )
        self.assertTrue(args.enable_stage6_reference_line)
        self.assertTrue(args.stage6_clear_lane_follow_cache_on_new_command)
        self.assertFalse(args.stage6_reference_line_generation_guard)

    def test_run_parser_supports_carla_extra_args(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-extra-args",
                "-windowed -ResX=960 -ResY=540",
            ]
        )
        self.assertEqual(args.carla_extra_args, "-windowed -ResX=960 -ResY=540")

    def test_run_parser_supports_capability_profile(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--capability-profile",
                "traffic_light_actual",
            ]
        )
        self.assertEqual(args.capability_profile, "traffic_light_actual")

    def test_online_chain_parser_defaults_to_followstop_aligned_startup_profile(self) -> None:
        parser = capability_online_chain._build_parser()
        args = parser.parse_args([])
        self.assertEqual(args.startup_profile, "render_offscreen_no_ros2")

    def test_docker_runtime_check_dual_timeout_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "runtime_check_timeout_sec": 15.0,
                            "runtime_check_fallback_timeout_sec": 5.0,
                            "docker": {
                                "auto_install_runtime_deps": False,
                            },
                        }
                    }
                }
            )
            first_exc = subprocess.TimeoutExpired(
                cmd="mainboard --help",
                timeout=15.0,
                output="",
                stderr="",
            )
            fallback_exc = subprocess.TimeoutExpired(
                cmd="ldd $(command -v mainboard)",
                timeout=5.0,
                output="",
                stderr="",
            )
            with mock.patch.object(backend, "_docker_module_exec_user", return_value="1000:1000"), mock.patch.object(
                backend, "_docker_modules_prefix", return_value="set -o pipefail; export APOLLO_ROOT=/apollo"
            ), mock.patch.object(
                backend,
                "_docker_exec",
                side_effect=[first_exc, fallback_exc],
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    backend._docker_ensure_runtime_deps(artifacts)
            self.assertIn("fallback dependency probe also timed out", str(ctx.exception))
            log_text = (artifacts / "apollo_mainboard_runtime_check.log").read_text(encoding="utf-8")
            self.assertIn("returncode: timeout", log_text)
            self.assertIn("fallback_timeout_sec: 5.0", log_text)
            self.assertIn("fallback_returncode: timeout", log_text)

    def test_runtime_module_status_lines_ignore_launcher_shells(self) -> None:
        backend = CyberRTBackend({})
        raw = "\n".join(
            [
                "80143 bash -lc source /apollo/cyber/setup.bash; mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT",
                "80253 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT",
                "80260 /bin/bash -lc pgrep -af 'modules/control/control_component/dag/control.dag' || true",
            ]
        )
        self.assertEqual(
            backend._runtime_module_status_lines(raw),
            [
                "80253 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT"
            ],
        )

    def test_source_prefix_skips_ros2_setup_for_direct_no_ros2_runtime(self) -> None:
        backend = CyberRTBackend(
            {
                "algo": {
                    "apollo": {
                        "transport_mode": "carla_direct",
                        "direct_bridge": {
                            "require_no_ros2_runtime": True,
                        },
                    }
                }
            }
        )
        with mock.patch.object(backend, "_apollo_root", return_value=Path("/tmp/apollo/src")), mock.patch.object(
            backend,
            "_pb_root",
            return_value=Path("/tmp/pb"),
        ), mock.patch.object(
            backend,
            "_host_ros2_setup_script",
            return_value="/opt/ros/humble/setup.bash",
        ), mock.patch.object(
            backend,
            "_apollo_internal_debug_shell_exports",
            return_value="",
        ):
            prefix = backend._source_prefix()
        self.assertNotIn("source /opt/ros/humble/setup.bash", prefix)

    def test_source_prefix_keeps_ros2_setup_for_ros2_gt(self) -> None:
        backend = CyberRTBackend({"algo": {"apollo": {"transport_mode": "ros2_gt"}}})
        with mock.patch.object(backend, "_apollo_root", return_value=Path("/tmp/apollo/src")), mock.patch.object(
            backend,
            "_pb_root",
            return_value=Path("/tmp/pb"),
        ), mock.patch.object(
            backend,
            "_host_ros2_setup_script",
            return_value="/opt/ros/humble/setup.bash",
        ), mock.patch.object(
            backend,
            "_apollo_internal_debug_shell_exports",
            return_value="",
        ):
            prefix = backend._source_prefix()
        self.assertIn("source /opt/ros/humble/setup.bash", prefix)

    def test_bridge_module_can_import_without_ros2_when_direct_transport_is_used(self) -> None:
        module = _load_bridge_unit_test_module_without_ros2()
        self.assertFalse(module.ROS2_RUNTIME_IMPORT_OK)
        self.assertIn("blocked_for_direct_no_ros2_test", module.ROS2_RUNTIME_IMPORT_ERROR)

    def test_extract_command_materialization_summary_prefers_explicit_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            _write_json(
                artifacts / "command_materialization_summary.json",
                {
                    "command_path_stage": "routing_not_sent",
                    "first_divergence_layer": "command_path",
                    "first_divergence_reason": "startup_delay",
                },
            )
            extracted = _extract_command_materialization_summary(artifacts)
        self.assertEqual(extracted["command_materialization_stage"], "routing_not_sent")
        self.assertEqual(extracted["command_materialization_layer"], "command_path")
        self.assertEqual(extracted["command_materialization_reason"], "startup_delay")
        self.assertEqual(extracted["command_materialization"]["command_path_stage"], "routing_not_sent")

    def test_docker_control_start_gate_normalizes_route_established_aliases(self) -> None:
        backend = CyberRTBackend(
            {
                "algo": {
                    "apollo": {
                        "docker": {
                            "control_start_gate": "routing_ready",
                        }
                    }
                }
            }
        )
        self.assertEqual(backend._docker_control_start_gate(), "route_established")

    def test_docker_deferred_control_start_mode_defaults_to_launch(self) -> None:
        backend = CyberRTBackend({})
        self.assertEqual(backend._docker_deferred_control_start_mode(), "launch")

    def test_docker_deferred_control_start_mode_normalizes_aliases(self) -> None:
        dag_backend = CyberRTBackend(
            {"algo": {"apollo": {"docker": {"deferred_control_start_mode": "mainboard"}}}}
        )
        launch_backend = CyberRTBackend(
            {"algo": {"apollo": {"docker": {"deferred_control_start_mode": "cyber_launch"}}}}
        )
        self.assertEqual(dag_backend._docker_deferred_control_start_mode(), "dag")
        self.assertEqual(launch_backend._docker_deferred_control_start_mode(), "launch")

    def test_docker_disable_deferred_control_bvar_dump_defaults_false(self) -> None:
        backend = CyberRTBackend({})
        self.assertFalse(backend._docker_disable_deferred_control_bvar_dump())

    def test_docker_disable_deferred_control_bvar_dump_respects_config(self) -> None:
        backend = CyberRTBackend(
            {"algo": {"apollo": {"docker": {"deferred_control_disable_bvar_dump": False}}}}
        )
        self.assertFalse(backend._docker_disable_deferred_control_bvar_dump())

    def test_docker_control_require_chassis_ready_defaults_false(self) -> None:
        backend = CyberRTBackend({})
        self.assertFalse(backend._docker_control_require_chassis_ready())

    def test_docker_control_runtime_overlay_source_dirs_defaults_empty(self) -> None:
        backend = CyberRTBackend({})
        self.assertEqual(backend._docker_control_runtime_overlay_source_dirs(), [])

    def test_docker_control_runtime_overlay_source_dirs_respects_order(self) -> None:
        backend = CyberRTBackend(
            {
                "algo": {
                    "apollo": {
                        "docker": {
                            "control_runtime_overlay_source_dirs": [
                                "/tmp/first",
                                " /tmp/second ",
                                "",
                            ]
                        }
                    }
                }
            }
        )
        self.assertEqual(
            backend._docker_control_runtime_overlay_source_dirs(),
            ["/tmp/first", "/tmp/second"],
        )

    def test_docker_control_chassis_ready_min_count_defaults_one(self) -> None:
        backend = CyberRTBackend({})
        self.assertEqual(backend._docker_control_chassis_ready_min_count(), 1)

    def test_docker_deferred_control_bvar_env_prefix_defaults_to_unset(self) -> None:
        backend = CyberRTBackend({})
        self.assertEqual(
            backend._docker_deferred_control_bvar_env_prefix(),
            "unset APOLLO_DISABLE_BVAR_DUMP; ",
        )

    def test_docker_deferred_control_bvar_env_prefix_can_unset(self) -> None:
        backend = CyberRTBackend(
            {"algo": {"apollo": {"docker": {"deferred_control_disable_bvar_dump": False}}}}
        )
        self.assertEqual(
            backend._docker_deferred_control_bvar_env_prefix(),
            "unset APOLLO_DISABLE_BVAR_DUMP; ",
        )

    def test_bridge_runtime_preflight_writes_success_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend({})
            completed = subprocess.CompletedProcess(
                args=["bash", "-lc", "preflight"],
                returncode=0,
                stdout=(
                    "setup noise\n"
                    '{"bridge_runtime_import_ok": true, '
                    '"bridge_runtime_preflight_status": "bridge_runtime_ready", '
                    '"bridge_runtime_import_error": null, '
                    '"bridge_runtime_dependency_probe_status": "bridge_runtime_ready", '
                    '"bridge_runtime_missing_shared_libs": [], '
                    '"bridge_runtime_missing_python_modules": []}\n'
                ),
                stderr="",
            )
            with mock.patch("tbio.backends.cyberrt.subprocess.run", return_value=completed):
                payload = backend._run_bridge_runtime_preflight(
                    artifacts=artifacts,
                    source_prefix="",
                    python_exec="python3",
                    docker_container=None,
                )
            self.assertTrue(payload["bridge_runtime_import_ok"])
            self.assertEqual(payload["bridge_runtime_preflight_status"], "bridge_runtime_ready")
            self.assertEqual(payload["bridge_runtime_dependency_probe_status"], "bridge_runtime_ready")
            written = json.loads((artifacts / "bridge_runtime_preflight.json").read_text(encoding="utf-8"))
            self.assertEqual(written["bridge_runtime_preflight_status"], "bridge_runtime_ready")
            self.assertEqual(written["bridge_runtime_command_returncode"], 0)

    def test_bridge_runtime_preflight_records_import_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend({})
            completed = subprocess.CompletedProcess(
                args=["bash", "-lc", "preflight"],
                returncode=1,
                stdout="",
                stderr="ImportError: libcudart.so.11.0: cannot open shared object file\n",
            )
            with mock.patch("tbio.backends.cyberrt.subprocess.run", return_value=completed):
                payload = backend._run_bridge_runtime_preflight(
                    artifacts=artifacts,
                    source_prefix="",
                    python_exec="python3",
                    docker_container=None,
                )
            self.assertFalse(payload["bridge_runtime_import_ok"])
            self.assertEqual(payload["bridge_runtime_preflight_status"], "bridge_runtime_import_failed")
            self.assertIn("libcudart.so.11.0", payload["bridge_runtime_import_error"])
            self.assertIn("libcudart.so.11.0", payload["bridge_runtime_stderr_tail"])
            self.assertEqual(payload["bridge_runtime_dependency_probe_status"], "bridge_runtime_cuda_runtime_failed")
            self.assertIn("libcudart.so.11.0", payload["bridge_runtime_missing_shared_libs"])

    def test_bridge_runtime_preflight_parser_preserves_dependency_details(self) -> None:
        payload = CyberRTBackend._parse_bridge_runtime_preflight_output(
            stdout=(
                '{"bridge_runtime_import_ok": false, '
                '"bridge_runtime_preflight_status": "bridge_runtime_import_failed", '
                '"bridge_runtime_import_error": "ImportError: libcudart.so.11.0: cannot open shared object file", '
                '"bridge_runtime_dependency_probe_status": "bridge_runtime_cuda_runtime_failed", '
                '"bridge_runtime_missing_shared_libs": ["libcudart.so.11.0"], '
                '"bridge_runtime_missing_python_modules": [], '
                '"bridge_runtime_pythonpath_head": ["/tmp/a"], '
                '"bridge_runtime_ld_library_path_head": ["/tmp/b"]}'
            ),
            stderr="",
            returncode=0,
        )
        self.assertEqual(payload["bridge_runtime_dependency_probe_status"], "bridge_runtime_cuda_runtime_failed")
        self.assertEqual(payload["bridge_runtime_missing_shared_libs"], ["libcudart.so.11.0"])
        self.assertEqual(payload["bridge_runtime_pythonpath_head"], ["/tmp/a"])
        self.assertEqual(payload["bridge_runtime_ld_library_path_head"], ["/tmp/b"])

    def test_docker_sync_host_libs_reuses_repo_cache_between_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            artifacts_a = repo_root / "run_a" / "artifacts"
            artifacts_b = repo_root / "run_b" / "artifacts"
            artifacts_a.mkdir(parents=True)
            artifacts_b.mkdir(parents=True)
            backend = CyberRTBackend({})
            backend.repo_root = repo_root
            backend._docker_container_name = "apollo_neo_dev_10.0.0_pkg"
            entries = [
                {
                    "path": "/usr/local/lib/libbvar.so.1",
                    "basename": "libbvar.so.1",
                    "resolved_path": "/usr/local/lib/libbvar.so.1.0",
                    "resolved_basename": "libbvar.so.1.0",
                    "is_symlink": True,
                    "symlink_target": "libbvar.so.1.0",
                }
            ]
            docker_list = subprocess.CompletedProcess(
                args=["docker", "exec"],
                returncode=0,
                stdout=json.dumps(entries),
                stderr="",
            )

            def _fake_cp(cmd: list[str]) -> None:
                source = str(cmd[2]).split(":", 1)[1]
                dest = Path(str(cmd[3]))
                dest.mkdir(parents=True, exist_ok=True)
                (dest / Path(source).name).write_text("fake-lib", encoding="utf-8")

            with mock.patch.object(backend, "_docker_exec", return_value=docker_list), mock.patch(
                "tbio.backends.cyberrt.subprocess.check_call",
                side_effect=_fake_cp,
            ) as mock_cp:
                first_dir = backend._docker_sync_host_libs(artifacts_a)
            self.assertIsNotNone(first_dir)
            self.assertEqual(mock_cp.call_count, 1)
            first_status = json.loads((artifacts_a / "apollo_docker_libs_cache_status.json").read_text(encoding="utf-8"))
            self.assertFalse(first_status["cache_reused"])
            self.assertEqual(first_status["newly_copied_count"], 1)

            with mock.patch.object(backend, "_docker_exec", return_value=docker_list), mock.patch(
                "tbio.backends.cyberrt.subprocess.check_call"
            ) as mock_cp:
                second_dir = backend._docker_sync_host_libs(artifacts_b)
            self.assertEqual(second_dir, first_dir)
            mock_cp.assert_not_called()
            second_status = json.loads((artifacts_b / "apollo_docker_libs_cache_status.json").read_text(encoding="utf-8"))
            self.assertTrue(second_status["cache_reused"])
            self.assertEqual(second_status["newly_copied_count"], 0)
            self.assertEqual(second_status["used_resolved_paths_count"], 1)

    def test_dreamview_runtime_snapshot_tracks_enable_and_record_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "dreamview": {
                                "enabled": True,
                                "auto_start": True,
                                "record": {"enabled": False},
                            }
                        }
                    }
                }
            )
            backend._ensure_dreamview_capture_state(artifacts)
            snapshot = json.loads(
                (artifacts / "dreamview_runtime_config_snapshot.json").read_text(encoding="utf-8")
            )
            self.assertTrue(snapshot["enabled"])
            self.assertTrue(snapshot["auto_start"])
            self.assertFalse(snapshot["record_enabled"])

    def test_dreamview_region_from_dict_accepts_offset_field_names(self) -> None:
        backend = CyberRTBackend({})

        self.assertEqual(
            backend._dreamview_region_from_dict(
                {"width": 1280, "height": 720, "offset_x": 10, "offset_y": 20}
            ),
            (1280, 720, 10, 20),
        )
        self.assertEqual(
            backend._dreamview_region_from_dict({"width": 640, "height": 360, "x": 0, "y": 0}),
            (640, 360, 0, 0),
        )

    def test_stop_dreamview_recording_disabled_marks_disabled_without_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "dreamview": {
                                "enabled": True,
                                "auto_start": True,
                                "record": {"enabled": False},
                            }
                        }
                    }
                }
            )
            backend._stop_dreamview_recording(artifacts)
            status = json.loads((artifacts / "dreamview_recording_status.json").read_text(encoding="utf-8"))
            snapshot = json.loads(
                (artifacts / "dreamview_runtime_config_snapshot.json").read_text(encoding="utf-8")
            )
            self.assertEqual(status["recording_status"], "disabled")
            self.assertEqual(status["failure_types"], [])
            self.assertTrue(snapshot["enabled"])
            self.assertFalse(snapshot["record_enabled"])

    def test_maybe_capture_channels_uses_container_cli_prefix_not_host_bridge_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "docker": {"container": "apollo_neo_dev_10.0.0_pkg"},
                            "healthcheck": {"probe_channels": True},
                        }
                    }
                }
            )
            backend._docker_container_name = "apollo_neo_dev_10.0.0_pkg"
            backend._runtime_source_prefix = (
                "export APOLLO_ROOT=/home/ubuntu/Apollo10.0/application-core/.aem/envroot/opt/apollo/neo/src"
            )
            with mock.patch.object(
                backend,
                "_docker_exec",
                return_value=subprocess.CompletedProcess(
                    args=["docker", "exec"],
                    returncode=0,
                    stdout="/apollo/planning\n/apollo/control\n",
                    stderr="",
                ),
            ) as mock_exec:
                payload = backend._maybe_capture_channels(artifacts)
            self.assertTrue(payload["ok"])
            called_cmd = mock_exec.call_args.args[0]
            self.assertIn("export APOLLO_ROOT=/apollo", called_cmd)
            self.assertNotIn("/home/ubuntu/Apollo10.0", called_cmd)
            self.assertIn("cyber_channel list", called_cmd)
            self.assertIn("/apollo/control", (artifacts / "cyber_channel_list.txt").read_text(encoding="utf-8"))

    def test_bridge_planning_runtime_counters_support_nested_planning_stats(self) -> None:
        counters = CyberRTBackend._bridge_planning_runtime_counters(
            {
                "routing_request_count": 1,
                "routing_success_count": 1,
                "planning": {
                    "msg_count": 3038,
                    "nonempty_trajectory_count": 2547,
                    "last_planning_header_sequence_num": 3043,
                    "last_trajectory_point_count": 111,
                    "first_nonempty_ts_sec": 1775037248.7323787,
                },
            }
        )
        self.assertEqual(counters["planning_messages_received"], 3038)
        self.assertEqual(counters["planning_nonempty_trajectory_count"], 2547)
        self.assertEqual(counters["last_planning_header_sequence_num"], 3043)
        self.assertEqual(counters["last_trajectory_point_count"], 111)
        self.assertAlmostEqual(counters["planning_first_nonempty_ts_sec"], 1775037248.7323787)

    def test_docker_probe_route_established_gate_waits_for_routing_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "docker": {
                                "control_start_gate": "route_established",
                                "control_planning_ready_timeout_sec": 60.0,
                                "control_planning_ready_poll_sec": 0.5,
                            }
                        }
                    }
                }
            )
            backend._deferred_control_overall_start_sec = 100.0
            with mock.patch("tbio.backends.cyberrt.time.time", return_value=101.0), mock.patch.object(
                backend,
                "_read_stats",
                return_value=(
                    True,
                    0.1,
                    {
                        "routing_request_count": 1,
                        "routing_success_count": 1,
                        "planning_nonempty_trajectory_count": 0,
                        "planning_messages_received": 0,
                    },
                ),
            ):
                status, error = backend._docker_probe_planning_ready_before_control(artifacts)
            self.assertEqual(status, "ready")
            self.assertIsNone(error)
            wait_log = (artifacts / "apollo_control_route_established_wait.log").read_text(encoding="utf-8")
            self.assertIn("control_start_gate=route_established", wait_log)
            self.assertIn("\"routing_success_count\": 1", wait_log)
            self.assertIn("\"route_established_at\": 101.0", wait_log)

    def test_docker_probe_planning_ready_gate_waits_for_chassis_when_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "docker": {
                                "control_start_gate": "planning_ready",
                                "control_require_chassis_ready": True,
                                "control_chassis_ready_min_count": 5,
                                "control_planning_ready_timeout_sec": 60.0,
                                "control_planning_ready_poll_sec": 0.5,
                            }
                        }
                    }
                }
            )
            backend._deferred_control_overall_start_sec = 100.0
            with mock.patch("tbio.backends.cyberrt.time.time", return_value=101.0), mock.patch.object(
                backend,
                "_read_stats",
                return_value=(
                    True,
                    0.1,
                    {
                        "routing_request_count": 1,
                        "routing_success_count": 1,
                        "planning": {
                            "msg_count": 8,
                            "nonempty_trajectory_count": 3,
                            "last_planning_header_sequence_num": 8,
                            "last_trajectory_point_count": 111,
                        },
                        "chassis_count": 2,
                    },
                ),
            ):
                status, error = backend._docker_probe_planning_ready_before_control(artifacts)
            self.assertEqual(status, "waiting")
            self.assertIsNone(error)
            wait_log = (artifacts / "apollo_control_planning_ready_wait.log").read_text(encoding="utf-8")
            self.assertIn("status=waiting_for_chassis_ready", wait_log)
            self.assertIn("control_chassis_ready_min_count=5", wait_log)
            self.assertIn("current_chassis_count=2", wait_log)

    def test_docker_probe_planning_ready_gate_allows_ready_when_chassis_threshold_met(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "docker": {
                                "control_start_gate": "planning_ready",
                                "control_require_chassis_ready": True,
                                "control_chassis_ready_min_count": 5,
                                "control_planning_ready_timeout_sec": 60.0,
                                "control_planning_ready_poll_sec": 0.5,
                            }
                        }
                    }
                }
            )
            backend._deferred_control_overall_start_sec = 100.0
            with mock.patch("tbio.backends.cyberrt.time.time", return_value=101.0), mock.patch.object(
                backend,
                "_read_stats",
                return_value=(
                    True,
                    0.1,
                    {
                        "routing_request_count": 1,
                        "routing_success_count": 1,
                        "planning": {
                            "msg_count": 8,
                            "nonempty_trajectory_count": 3,
                            "last_planning_header_sequence_num": 8,
                            "last_trajectory_point_count": 111,
                        },
                        "chassis_count": 6,
                    },
                ),
            ):
                status, error = backend._docker_probe_planning_ready_before_control(artifacts)
            self.assertEqual(status, "ready")
            self.assertIsNone(error)
            wait_log = (artifacts / "apollo_control_planning_ready_wait.log").read_text(encoding="utf-8")
            self.assertIn("status=ready", wait_log)
            self.assertIn("chassis_ready=True", wait_log)

    def test_docker_start_control_module_rejects_launcher_shell_false_positive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "docker": {
                                "container": "apollo_neo_dev_10.0.0_pkg",
                            }
                        }
                    }
                }
            )
            launcher_only = subprocess.CompletedProcess(
                args=["docker", "exec"],
                returncode=0,
                stdout=(
                    "80143 bash -lc source /apollo/cyber/setup.bash; "
                    "mainboard -d modules/control/control_component/dag/control.dag "
                    "-p control -s CYBER_DEFAULT\n"
                ),
                stderr="",
            )
            empty_log = subprocess.CompletedProcess(
                args=["docker", "exec"],
                returncode=0,
                stdout="",
                stderr="",
            )
            docker_exec_results = [launcher_only] * 10 + [empty_log, empty_log]
            with (
                mock.patch("tbio.backends.cyberrt.subprocess.run", return_value=launcher_only),
                mock.patch("tbio.backends.cyberrt.time.sleep"),
                mock.patch.object(backend, "_docker_module_exec_user", return_value="1000:1000"),
                mock.patch.object(backend, "_docker_modules_prefix", return_value="set -o pipefail"),
                mock.patch.object(backend, "_docker_exec", side_effect=docker_exec_results),
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    backend._docker_start_control_module(artifacts)
            self.assertIn("control process was not found", str(ctx.exception))
            status_text = (artifacts / "apollo_control_deferred_status.log").read_text(encoding="utf-8")
            self.assertIn("runtime_filtered_lines: []", status_text)
            self.assertIn("ignored_launcher_lines", status_text)
            log_text = (artifacts / "apollo_control_deferred_start.log").read_text(encoding="utf-8")
            self.assertIn("control_running_after_probe: False", log_text)

    def test_docker_start_control_module_records_survival_probe_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "artifacts": {"dir": str(artifacts)},
                    "algo": {
                        "apollo": {
                            "docker": {
                                "container": "apollo_neo_dev_10.0.0_pkg",
                                "deferred_control_start_mode": "launch",
                                "deferred_control_disable_bvar_dump": True,
                            }
                        }
                    },
                }
            )
            running_status = subprocess.CompletedProcess(
                args=["docker", "exec"],
                returncode=0,
                stdout=(
                    "78367 mainboard -d modules/control/control_component/dag/control.dag "
                    "-p control -s CYBER_DEFAULT\n"
                ),
                stderr="",
            )
            empty_log = subprocess.CompletedProcess(
                args=["docker", "exec"],
                returncode=0,
                stdout="",
                stderr="",
            )
            survival_payload = {
                "control_started_pid_seen": True,
                "control_survived_5s": False,
                "control_survived_10s": False,
                "control_present_after_first_nonzero_planning": False,
            }
            with (
                mock.patch(
                    "tbio.backends.cyberrt.subprocess.run",
                    return_value=subprocess.CompletedProcess(
                        args=["docker", "exec"],
                        returncode=0,
                        stdout="",
                        stderr="",
                    ),
                ),
                mock.patch("tbio.backends.cyberrt.time.sleep"),
                mock.patch.object(backend, "_docker_module_exec_user", return_value="1000:1000"),
                mock.patch.object(backend, "_docker_modules_prefix", return_value="set -o pipefail"),
                mock.patch.object(backend, "_docker_exec", side_effect=[running_status, empty_log, empty_log]),
                mock.patch.object(
                    backend,
                    "_docker_probe_deferred_control_survival",
                    return_value=survival_payload,
                ) as mock_survival,
            ):
                backend._docker_start_control_module(artifacts)
            mock_survival.assert_called_once()
            trace_rows = [
                json.loads(line)
                for line in (artifacts / "apollo_backend_startup_trace.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(trace_rows[0]["step"], "deferred_control_start_begin")
            self.assertEqual(trace_rows[0]["preferred_deferred_control_start_mode"], "launch")
            self.assertTrue(trace_rows[0]["deferred_control_disable_bvar_dump"])
            self.assertTrue(trace_rows[0]["deferred_control_bvar_env_disabled"])
            self.assertEqual(trace_rows[-1]["step"], "deferred_control_start_done")
            self.assertEqual(trace_rows[-1]["actual_deferred_control_start_mode"], "launch")
            self.assertTrue(trace_rows[-1]["deferred_control_disable_bvar_dump"])
            self.assertTrue(trace_rows[-1]["deferred_control_bvar_env_disabled"])
            self.assertFalse(trace_rows[-1]["control_survived_5s"])
            self.assertFalse(trace_rows[-1]["control_survived_10s"])
            self.assertFalse(trace_rows[-1]["control_present_after_first_nonzero_planning"])
            log_text = (artifacts / "apollo_control_deferred_start.log").read_text(encoding="utf-8")
            self.assertIn("APOLLO_DISABLE_BVAR_DUMP=1", log_text)
            self.assertIn("pgrep -f 'modules/control/control_component/dag/control[.]dag'", log_text)
            self.assertNotIn("pgrep -f 'modules/control/control_component/dag/control.dag'", log_text)

    def test_on_sim_tick_schedules_deferred_control_start_without_blocking_tick(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "artifacts": {"dir": str(artifacts)},
                    "algo": {
                        "apollo": {
                            "docker": {
                                "container": "apollo_neo_dev_10.0.0_pkg",
                                "control_start_gate": "planning_ready",
                                "deferred_control_start_async": True,
                            }
                        }
                    },
                }
            )
            backend._deferred_control_pending = True
            backend._deferred_control_started = False
            backend._deferred_control_last_poll_sec = -999.0
            started = threading.Event()
            release = threading.Event()

            def slow_start(_artifacts: Path) -> None:
                started.set()
                self.assertTrue(release.wait(timeout=2.0))

            with (
                mock.patch.object(backend, "_docker_probe_planning_ready_before_control", return_value=("ready", None)),
                mock.patch.object(backend, "_docker_start_control_module", side_effect=slow_start) as mock_start,
            ):
                begin = time.perf_counter()
                backend.on_sim_tick(frame_id=10, timestamp=1.0, step=3)
                elapsed = time.perf_counter() - begin
                self.assertLess(elapsed, 0.5)
                self.assertTrue(started.wait(timeout=1.0))
                self.assertTrue(backend._deferred_control_start_in_progress)
                self.assertTrue(backend._deferred_control_pending)
                release.set()
                self.assertIsNotNone(backend._deferred_control_start_thread)
                backend._deferred_control_start_thread.join(timeout=2.0)

            mock_start.assert_called_once_with(artifacts)
            self.assertTrue(backend._deferred_control_started)
            self.assertFalse(backend._deferred_control_pending)
            self.assertFalse(backend._deferred_control_start_in_progress)
            trace_rows = [
                json.loads(line)
                for line in (artifacts / "apollo_backend_startup_trace.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(trace_rows[0]["step"], "deferred_control_start_async_scheduled")
            self.assertEqual(trace_rows[-1]["step"], "deferred_control_start_async_done")

    def test_on_sim_tick_records_deferred_control_async_start_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "artifacts": {"dir": str(artifacts)},
                    "algo": {
                        "apollo": {
                            "docker": {
                                "container": "apollo_neo_dev_10.0.0_pkg",
                                "control_start_gate": "planning_ready",
                                "deferred_control_start_async": True,
                            }
                        }
                    },
                }
            )
            backend._deferred_control_pending = True
            backend._deferred_control_started = False
            backend._deferred_control_last_poll_sec = -999.0

            with (
                mock.patch.object(backend, "_docker_probe_planning_ready_before_control", return_value=("ready", None)),
                mock.patch.object(
                    backend,
                    "_docker_start_control_module",
                    side_effect=RuntimeError("control start failed"),
                ),
                mock.patch("builtins.print"),
            ):
                backend.on_sim_tick(frame_id=11, timestamp=1.1, step=4)
                self.assertIsNotNone(backend._deferred_control_start_thread)
                backend._deferred_control_start_thread.join(timeout=2.0)

            self.assertFalse(backend._deferred_control_started)
            self.assertFalse(backend._deferred_control_pending)
            self.assertFalse(backend._deferred_control_start_in_progress)
            self.assertIn("control start failed", backend._deferred_control_failure or "")
            self.assertIn("control start failed", backend._deferred_control_start_async_failure or "")
            trace_rows = [
                json.loads(line)
                for line in (artifacts / "apollo_backend_startup_trace.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(trace_rows[-1]["step"], "deferred_control_start_async_failed")

    def test_evaluate_capture_validity_surfaces_startup_stuck_family(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "lat_straight_track__02"
            artifacts = run_dir / "artifacts"
            artifacts.mkdir(parents=True)
            _write_jsonl(
                artifacts / "startup_stage_timeline.jsonl",
                [
                    {"stage": "run_initialized", "ts_sec": 1.0},
                    {"stage": "carla_launch_start", "ts_sec": 2.0},
                    {"stage": "carla_wait_ready_ok", "ts_sec": 55.0},
                    {"stage": "carla_get_world_start", "ts_sec": 63.0},
                ],
            )
            _write_json(
                artifacts / "carla_world_ready_summary.json",
                {
                    "status": "carla_wait_ready_ok",
                    "final_town": None,
                },
            )
            detail = evaluate_capture_validity(
                capture_id="lat_straight_track",
                raw_path=None,
                exit_code=124,
                policy={"require_summary": False},
                summary_path=run_dir / "summary.json",
                metadata_path=artifacts / "scenario_metadata.json",
                log_path=run_dir.parent / "logs" / "lat_straight_track.log",
            )
            self.assertEqual(detail["startup_stage_last"], "carla_get_world_start")
            self.assertEqual(detail["startup_stage_count"], 4)
            self.assertEqual(detail["startup_highest_confirmed_stage"], "CARLA_GET_WORLD_STARTED")
            self.assertEqual(detail["startup_blocker_family"], "scene_timeout_during_carla_get_world")
            self.assertEqual(detail["carla_world_ready_status"], "carla_wait_ready_ok")
            self.assertFalse(detail["world_ready_reached"])
            markdown = render_capture_validity_markdown(
                {
                    "capture_count": 1,
                    "valid_capture_count": 0,
                    "invalid_capture_count": 1,
                    "suite_recoverable": False,
                    "aggregate_coverage_ok": False,
                    "aggregate_coverage_reasons": [],
                    "captures": [detail],
                }
            )
            self.assertIn("CARLA_GET_WORLD_STARTED", markdown)
            self.assertIn("scene_timeout_during_carla_get_world", markdown)

    def test_capability_profile_defaults_target_focus_or_proxy_pack(self) -> None:
        self.assertEqual(CAPABILITY_PROFILE_DEFAULT_SUBSETS["lane_keep"], "lane_keep_focus_pack")
        self.assertEqual(CAPABILITY_PROFILE_DEFAULT_SUBSETS["curve_lane_follow"], "curve_lane_follow_proxy_pack")
        self.assertEqual(CAPABILITY_PROFILE_DEFAULT_SUBSETS["junction_traverse"], "junction_traverse_proxy_pack")
        self.assertEqual(CAPABILITY_PROFILE_DEFAULT_SUBSETS["traffic_light_actual"], "traffic_light_proxy_pack")

    def test_resolve_recommended_subset_prefers_explicit_subset(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--capability-profile",
                "curve_lane_follow",
                "--recommended-subset",
                "curve_lane_follow_pair_gap_queue",
            ]
        )
        self.assertEqual(_resolve_recommended_subset(args), "curve_lane_follow_pair_gap_queue")

    def test_resolve_recommended_subset_uses_proxy_or_focus_pack_default(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--capability-profile",
                "junction_traverse",
            ]
        )
        self.assertEqual(_resolve_recommended_subset(args), "junction_traverse_proxy_pack")

    def test_run_parser_supports_route_ids_file(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-ids-file",
                "/tmp/routes.json",
            ]
        )
        self.assertEqual(args.route_ids_file, "/tmp/routes.json")

    def test_run_parser_supports_overrides_file(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--overrides-file",
                "/tmp/overrides.txt",
            ]
        )
        self.assertEqual(args.overrides_file, "/tmp/overrides.txt")

    def test_load_route_ids_file_supports_json_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "route_ids.json"
            path.write_text(json.dumps({"route_ids": ["route_a", "route_b", "route_a", ""]}), encoding="utf-8")
            self.assertEqual(_load_route_ids_file(path), ["route_a", "route_b"])

    def test_load_route_ids_file_supports_json_list_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "route_ids.json"
            path.write_text(json.dumps(["route_a", "route_b", "route_a", ""]), encoding="utf-8")
            self.assertEqual(_load_route_ids_file(path), ["route_a", "route_b"])

    def test_load_overrides_file_supports_text_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "overrides.txt"
            path.write_text(
                "\n".join(
                    [
                        "algo.apollo.planning.lane_follow_only_scenario=false",
                        "# comment",
                        "algo.apollo.traffic_light.policy=carla_actual",
                        "algo.apollo.traffic_light.policy=carla_actual",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                _load_overrides_file(path),
                [
                    "algo.apollo.planning.lane_follow_only_scenario=false",
                    "algo.apollo.traffic_light.policy=carla_actual",
                ],
            )

    def test_default_capability_profile_overrides_load_from_repo_presets(self) -> None:
        overrides = _default_capability_profile_overrides("traffic_light_actual")
        self.assertIn("algo.apollo.traffic_light.policy=carla_actual", overrides)
        self.assertIn("algo.apollo.planning.disable_traffic_light_rule=false", overrides)
        self.assertEqual(
            CAPABILITY_PROFILE_OVERRIDE_PRESET_FILES["traffic_light_actual"].name,
            "town01_route_health_traffic_light_actual.overrides.txt",
        )

    def test_build_base_overrides_prefers_overrides_file_over_capability_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "overrides.txt"
            path.write_text(
                "algo.apollo.planning.lane_follow_only_scenario=false\n"
                "algo.apollo.traffic_light.policy=carla_actual\n",
                encoding="utf-8",
            )
            parser = _build_parser()
            args = parser.parse_args(
                [
                    "run",
                    "--capability-profile",
                    "lane_keep",
                    "--overrides-file",
                    str(path),
                ]
            )
            overrides = _build_base_overrides(args)
            self.assertIn("algo.apollo.planning.lane_follow_only_scenario=false", overrides)
            self.assertIn("algo.apollo.traffic_light.policy=carla_actual", overrides)
            self.assertNotIn("algo.apollo.planning.disable_traffic_light_rule=true", overrides)

    def test_invoke_run_applies_cli_overrides_after_capability_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            completed = subprocess.CompletedProcess(args=[], returncode=0)
            with mock.patch("tools.run_town01_route_health._cleanup_runtime_processes"), mock.patch(
                "tools.run_town01_route_health._ros_sourced_env", side_effect=lambda env: env
            ), mock.patch("tools.run_town01_route_health.subprocess.run", return_value=completed) as run_mock:
                rc = _invoke_run(
                    run_dir=root / "run",
                    route_id="town01_rh_spawn097_goal046",
                    corpus_path=root / "corpus.json",
                    config_path=root / "config.yaml",
                    flags={},
                    comparison_label="demo_showcase",
                    ticks=420,
                    base_overrides=[],
                    run_overrides=["algo.apollo.dreamview.record.enabled=false"],
                    final_overrides=["algo.apollo.dreamview.record.enabled=true"],
                )

            self.assertEqual(rc, 0)
            cmd = list(run_mock.call_args.args[0])
            override_values = [cmd[index + 1] for index, item in enumerate(cmd[:-1]) if item == "--override"]
            self.assertIn("algo.apollo.dreamview.record.enabled=false", override_values)
            self.assertEqual(override_values[-1], "algo.apollo.dreamview.record.enabled=true")

    def test_resolve_route_ids_prefers_route_ids_file_before_subset_sampling(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "route_ids.json"
            path.write_text(json.dumps({"route_ids": ["route_a", "route_b"]}), encoding="utf-8")
            parser = _build_parser()
            args = parser.parse_args(
                [
                    "run",
                    "--capability-profile",
                    "curve_lane_follow",
                    "--route-ids-file",
                    str(path),
                ]
            )
            self.assertEqual(_resolve_route_ids({"recommended_subsets": {}}, args), ["route_a", "route_b"])

    def test_build_capability_review_plan_uses_default_subsets(self) -> None:
        corpus = {
            "recommended_subsets": {
                "lane_keep_focus_pack": ["lane_a", "lane_b"],
                "curve_lane_follow_proxy_pack": ["curve_a", "curve_anchor"],
                "junction_traverse_proxy_pack": ["junction_a", "junction_anchor"],
                "traffic_light_proxy_pack": ["traffic_a", "traffic_anchor"],
            }
        }
        entries = build_capability_review_plan(
            corpus=corpus,
            capability_profiles=[
                "lane_keep",
                "curve_lane_follow",
                "junction_traverse",
                "traffic_light_actual",
            ],
            pack_kind="default",
            sample_size=4,
            ticks=700,
            batch_root_parent=Path("/tmp/town01_capability_review"),
        )
        self.assertEqual([entry["subset_name"] for entry in entries], [
            "lane_keep_focus_pack",
            "curve_lane_follow_proxy_pack",
            "junction_traverse_proxy_pack",
            "traffic_light_proxy_pack",
        ])
        self.assertEqual(entries[1]["route_ids"], ["curve_a", "curve_anchor"])
        self.assertIn("--recommended-subset", entries[2]["command"])
        self.assertIn("junction_traverse_proxy_pack", entries[2]["command"])

    def test_review_pack_parser_supports_missing_history_outputs(self) -> None:
        parser = _build_review_pack_parser()
        args = parser.parse_args(
            [
                "--missing-history-runbook-output",
                "/tmp/missing_history_runbook.md",
                "--missing-history-manifest-output",
                "/tmp/missing_history_manifest.json",
                "--missing-history-summary-output",
                "/tmp/missing_history_summary.csv",
            ]
        )
        self.assertEqual(str(args.missing_history_runbook_output), "/tmp/missing_history_runbook.md")
        self.assertEqual(str(args.missing_history_manifest_output), "/tmp/missing_history_manifest.json")
        self.assertEqual(str(args.missing_history_summary_output), "/tmp/missing_history_summary.csv")

    def test_build_capability_review_plan_allows_pack_override(self) -> None:
        corpus = {
            "recommended_subsets": {
                "curve_lane_follow_focus_pack": ["curve_left", "curve_right"],
                "curve_lane_follow_proxy_pack": ["curve_left", "curve_proxy"],
                "curve_lane_follow_seed_pack": ["curve_left"],
            }
        }
        entries = build_capability_review_plan(
            corpus=corpus,
            capability_profiles=["curve_lane_follow"],
            pack_kind="focus_pack",
            sample_size=2,
            ticks=500,
            batch_root_parent=Path("/tmp/town01_capability_review"),
            extra_args=["--enable-guard"],
        )
        self.assertEqual(entries[0]["subset_name"], "curve_lane_follow_focus_pack")
        self.assertEqual(entries[0]["route_ids"], ["curve_left", "curve_right"])
        self.assertIn("--enable-guard", entries[0]["command"])

        seed_entries = build_capability_review_plan(
            corpus=corpus,
            capability_profiles=["curve_lane_follow"],
            pack_kind="seed_pack",
            sample_size=2,
            ticks=500,
            batch_root_parent=Path("/tmp/town01_capability_review"),
        )
        self.assertEqual(seed_entries[0]["subset_name"], "curve_lane_follow_seed_pack")
        self.assertEqual(seed_entries[0]["route_ids"], ["curve_left"])

        proxy_entries = build_capability_review_plan(
            corpus=corpus,
            capability_profiles=["curve_lane_follow"],
            pack_kind="proxy_pack",
            sample_size=2,
            ticks=500,
            batch_root_parent=Path("/tmp/town01_capability_review"),
        )
        self.assertEqual(proxy_entries[0]["subset_name"], "curve_lane_follow_proxy_pack")
        self.assertEqual(proxy_entries[0]["route_ids"], ["curve_left", "curve_proxy"])

    def test_review_pack_parser_supports_proxy_pack_kind(self) -> None:
        parser = _build_review_pack_parser()
        args = parser.parse_args(["--pack-kind", "proxy_pack"])
        self.assertEqual(args.pack_kind, "proxy_pack")

    def test_freeze_capability_review_plan_writes_route_id_files_and_frozen_commands(self) -> None:
        entries = [
            {
                "capability_profile": "junction_traverse",
                "subset_name": "junction_traverse_history_gap_queue",
                "route_ids": ["route_left", "route_right"],
                "route_count": 2,
                "sample_size": 4,
                "ticks": 700,
                "batch_root": "/tmp/batch",
                "comparison_label": "junction_traverse__junction_traverse_history_gap_queue",
                "extra_args": ["--enable-guard"],
                "subset_command_argv": ["python3", "tools/run_town01_route_health.py", "run"],
                "subset_command": "python3 tools/run_town01_route_health.py run --recommended-subset junction_traverse_history_gap_queue",
                "command_argv": ["python3", "tools/run_town01_route_health.py", "run"],
                "command": "python3 tools/run_town01_route_health.py run --recommended-subset junction_traverse_history_gap_queue",
            }
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            frozen = freeze_capability_review_plan(
                entries,
                output_root=Path(tmpdir) / "route_ids",
                overrides_root=Path(tmpdir) / "overrides",
            )
            self.assertEqual(len(frozen), 1)
            route_ids_file = Path(frozen[0]["route_ids_file"])
            self.assertTrue(route_ids_file.exists())
            self.assertEqual(json.loads(route_ids_file.read_text(encoding="utf-8"))["route_ids"], ["route_left", "route_right"])
            overrides_file = Path(frozen[0]["overrides_file"])
            self.assertTrue(overrides_file.exists())
            overrides_text = overrides_file.read_text(encoding="utf-8")
            self.assertIn("scenario.route_health.spawn_reject_junction=false", overrides_text)
            self.assertTrue(str(frozen[0]["preset_overrides_file"]).endswith("town01_route_health_junction_traverse.overrides.txt"))
            self.assertIn("--route-ids-file", frozen[0]["frozen_command"])
            self.assertIn("--overrides-file", frozen[0]["frozen_command"])
            self.assertIn("--enable-guard", frozen[0]["frozen_command"])
            self.assertEqual(frozen[0]["effective_sample_size"], 2)

    def test_render_capability_review_runbook_includes_commands_and_routes(self) -> None:
        report = render_runbook(
            entries=[
                {
                    "capability_profile": "traffic_light_actual",
                    "subset_name": "traffic_light_history_gap_queue",
                    "route_ids": ["town01_rh_spawn129_goal051", "town01_rh_spawn219_goal052"],
                    "route_count": 2,
                    "sample_size": 4,
                    "ticks": 700,
                    "batch_root": "/tmp/batch",
                    "comparison_label": "traffic_light_actual__traffic_light_history_gap_queue",
                    "route_ids_file": "/tmp/routes.json",
                    "preset_overrides_file": "/repo/configs/io/examples/town01_route_health_traffic_light_actual.overrides.txt",
                    "overrides_file": "/tmp/traffic_light.overrides.txt",
                    "effective_overrides": [
                        "algo.apollo.planning.disable_traffic_light_rule=false",
                        "algo.apollo.traffic_light.policy=carla_actual",
                    ],
                    "effective_sample_size": 2,
                    "subset_command_argv": ["python3", "tools/run_town01_route_health.py"],
                    "subset_command": "python3 tools/run_town01_route_health.py run --capability-profile traffic_light_actual --recommended-subset traffic_light_history_gap_queue --overrides-file /tmp/traffic_light.overrides.txt",
                    "frozen_command_argv": ["python3", "tools/run_town01_route_health.py"],
                    "frozen_command": "python3 tools/run_town01_route_health.py run --capability-profile traffic_light_actual --route-ids-file /tmp/routes.json --overrides-file /tmp/traffic_light.overrides.txt",
                    "command_argv": ["python3", "tools/run_town01_route_health.py"],
                    "command": "python3 tools/run_town01_route_health.py run --capability-profile traffic_light_actual --route-ids-file /tmp/routes.json --overrides-file /tmp/traffic_light.overrides.txt",
                }
            ],
            corpus_path=Path("/tmp/town01_route_corpus.json"),
            pack_kind="default",
            sample_size=4,
            ticks=700,
        )
        self.assertIn("traffic_light_actual", report)
        self.assertIn("traffic_light_history_gap_queue", report)
        self.assertIn("town01_rh_spawn129_goal051, town01_rh_spawn219_goal052", report)
        self.assertIn("/repo/configs/io/examples/town01_route_health_traffic_light_actual.overrides.txt", report)
        self.assertIn("/tmp/traffic_light.overrides.txt", report)
        self.assertIn("--capability-profile traffic_light_actual --route-ids-file /tmp/routes.json --overrides-file /tmp/traffic_light.overrides.txt", report)
        self.assertIn("--capability-profile traffic_light_actual --recommended-subset traffic_light_history_gap_queue --overrides-file /tmp/traffic_light.overrides.txt", report)

    def test_build_manifest_review_rows_marks_history_partial_with_pass(self) -> None:
        manifest = {
            "entries": [
                {
                    "capability_profile": "lane_keep",
                    "subset_name": "lane_keep_focus_pack",
                    "route_ids": ["lane_a", "lane_b"],
                    "route_ids_file": "/tmp/lane_keep.route_ids.json",
                    "comparison_label": "lane_keep__lane_keep_focus_pack",
                    "batch_root": "/tmp/nonexistent-batch",
                    "effective_sample_size": 2,
                }
            ]
        }
        corpus = {
            "routes": [
                {
                    "route_id": "lane_a",
                    "health_tags": ["route_health_pass"],
                    "route_length_m": 200.0,
                    "road_transition_count": 0,
                    "lane_transition_count": 0,
                    "goal_heading_delta_deg": 0.0,
                },
                {
                    "route_id": "lane_b",
                    "health_tags": ["route_health_candidate"],
                    "route_length_m": 205.0,
                    "road_transition_count": 0,
                    "lane_transition_count": 0,
                    "goal_heading_delta_deg": 0.0,
                },
            ]
        }
        comparison_rows = [
            {
                "route_id": "lane_a",
                "route_health_label": "route_health_pass",
                "route_completion_ratio": "0.98",
                "route_distance_achieved_m": "190.0",
                "comparison_label": "lane_keep_old_a",
            },
            {
                "route_id": "lane_b",
                "route_health_label": "route_health_candidate",
                "route_completion_ratio": "0.75",
                "route_distance_achieved_m": "150.0",
                "comparison_label": "lane_keep_old_b",
            },
        ]
        historical_rows = [
            {
                "route_id": "lane_a",
                "route_health_label": "route_health_pass",
                "route_completion_ratio": 0.98,
                "route_distance_achieved_m": 190.0,
                "comparison_label": "lane_keep_semantic_a",
                "semantic::lane_keep_candidate": True,
            }
        ]
        rows = build_manifest_review_rows(manifest, corpus, comparison_rows, historical_rows)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pack_readiness"], "history_partial_with_pass")
        self.assertEqual(rows[0]["history_reviewed_count"], 1)
        self.assertEqual(rows[0]["missing_history_routes"], ["lane_b"])

    def test_render_capability_review_manifest_report_includes_missing_routes(self) -> None:
        report = render_capability_review_manifest_report(
            [
                {
                    "capability_profile": "curve_lane_follow",
                    "subset_name": "curve_lane_follow_history_gap_queue",
                    "comparison_label": "curve_lane_follow__curve_lane_follow_history_gap_queue",
                    "batch_root": "/tmp/batch",
                    "route_ids_file": "/tmp/curve.route_ids.json",
                    "route_count": 2,
                    "effective_sample_size": 2,
                    "batch_observed_route_count": 0,
                    "batch_reviewed_count": 0,
                    "batch_pass_count": 0,
                    "history_observed_route_count": 0,
                    "history_reviewed_count": 0,
                    "history_pass_count": 0,
                    "missing_batch_routes": ["curve_left", "curve_right"],
                    "missing_history_routes": ["curve_left", "curve_right"],
                    "pack_readiness": "no_evidence",
                    "route_rows": [
                        {
                            "route_id": "curve_left",
                            "corpus_status": "unreviewed",
                            "geometry_class": "standard_bend",
                            "direction_class": "left_bend",
                            "batch_status": "missing",
                            "batch_label": "none",
                            "batch_completion": 0.0,
                            "batch_comparison_label": "none",
                            "history_status": "missing",
                            "history_label": "none",
                            "history_completion": 0.0,
                            "history_comparison_label": "none",
                            "current_best_label": "none",
                            "current_best_completion": 0.0,
                            "current_best_comparison_label": "none",
                        }
                    ],
                }
            ],
            manifest_path=Path("/tmp/manifest.json"),
            comparison_path=Path("/tmp/comparison.csv"),
            corpus_path=Path("/tmp/corpus.json"),
        )
        self.assertIn("curve_lane_follow_history_gap_queue", report)
        self.assertIn("missing_batch=`curve_left, curve_right`", report)
        self.assertIn("pack_readiness: `no_evidence`", report)

    def test_build_missing_history_entries_writes_frozen_route_ids_and_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_overrides = Path(tmpdir) / "source.overrides.txt"
            source_overrides.write_text(
                "algo.apollo.planning.lane_follow_only_scenario=false\n"
                "algo.apollo.traffic_light.policy=carla_actual\n",
                encoding="utf-8",
            )
            entries = build_missing_history_entries(
                [
                    {
                        "capability_profile": "traffic_light_actual",
                        "subset_name": "traffic_light_history_gap_queue",
                        "comparison_label": "traffic_light_actual__traffic_light_history_gap_queue",
                        "pack_readiness": "no_evidence",
                        "missing_history_routes": ["tl_left", "tl_right"],
                        "preset_overrides_file": "/repo/configs/io/examples/town01_route_health_traffic_light_actual.overrides.txt",
                        "overrides_file": str(source_overrides),
                        "ticks": 700,
                        "extra_args": ["--enable-guard"],
                    }
                ],
                route_ids_root=Path(tmpdir) / "route_ids",
                overrides_root=Path(tmpdir) / "overrides",
                batch_root_parent=Path(tmpdir) / "runs",
            )
            self.assertEqual(len(entries), 1)
            route_ids_file = Path(entries[0]["route_ids_file"])
            self.assertTrue(route_ids_file.exists())
            self.assertEqual(json.loads(route_ids_file.read_text(encoding="utf-8"))["route_ids"], ["tl_left", "tl_right"])
            seed_route_ids_file = Path(entries[0]["seed_route_ids_file"])
            self.assertTrue(seed_route_ids_file.exists())
            self.assertEqual(json.loads(seed_route_ids_file.read_text(encoding="utf-8"))["route_ids"], ["tl_left"])
            self.assertEqual(entries[0]["seed_route_id"], "tl_left")
            overrides_file = Path(entries[0]["overrides_file"])
            self.assertTrue(overrides_file.exists())
            self.assertIn("algo.apollo.traffic_light.policy=carla_actual", overrides_file.read_text(encoding="utf-8"))
            self.assertIn("__missing_history", entries[0]["comparison_label"])
            self.assertIn("__missing_history__seed", entries[0]["seed_comparison_label"])
            self.assertIn("--route-ids-file", entries[0]["seed_command"])
            self.assertIn("--overrides-file", entries[0]["seed_command"])
            self.assertIn("--route-ids-file", entries[0]["command"])
            self.assertIn("--overrides-file", entries[0]["command"])
            self.assertIn("--enable-guard", entries[0]["command"])

    def test_render_missing_history_runbook_includes_frozen_command(self) -> None:
        report = render_missing_history_runbook(
            [
                {
                    "capability_profile": "curve_lane_follow",
                    "source_subset_name": "curve_lane_follow_history_gap_queue",
                    "source_comparison_label": "curve_lane_follow__curve_lane_follow_history_gap_queue",
                    "source_pack_readiness": "no_evidence",
                    "missing_history_routes": ["curve_left", "curve_right"],
                    "seed_route_id": "curve_left",
                    "seed_route_ids_file": "/tmp/curve__seed.route_ids.json",
                    "seed_batch_root": "/tmp/curve_seed_batch",
                    "route_ids_file": "/tmp/curve.route_ids.json",
                    "preset_overrides_file": "/repo/configs/io/examples/town01_route_health_curve_lane_follow.overrides.txt",
                    "source_overrides_file": "/tmp/source_curve.overrides.txt",
                    "overrides_file": "/tmp/curve.overrides.txt",
                    "effective_overrides": ["algo.apollo.planning.lane_follow_only_scenario=true"],
                    "batch_root": "/tmp/batch",
                    "seed_command": "python3 tools/run_town01_route_health.py run --capability-profile curve_lane_follow --route-ids-file /tmp/curve__seed.route_ids.json --overrides-file /tmp/curve.overrides.txt",
                    "command": "python3 tools/run_town01_route_health.py run --capability-profile curve_lane_follow --route-ids-file /tmp/curve.route_ids.json --overrides-file /tmp/curve.overrides.txt",
                }
            ],
            source_manifest=Path("/tmp/review_manifest.json"),
            source_report=Path("/tmp/review_report.md"),
        )
        self.assertIn("Town01 Capability Missing-History Runbook", report)
        self.assertIn("curve_lane_follow_history_gap_queue", report)
        self.assertIn("curve_left, curve_right", report)
        self.assertIn("seed_route_id: `curve_left`", report)
        self.assertIn("/tmp/curve__seed.route_ids.json", report)
        self.assertIn("/tmp/curve.route_ids.json", report)
        self.assertIn("--capability-profile curve_lane_follow --route-ids-file /tmp/curve__seed.route_ids.json --overrides-file /tmp/curve.overrides.txt", report)
        self.assertIn("--capability-profile curve_lane_follow --route-ids-file /tmp/curve.route_ids.json --overrides-file /tmp/curve.overrides.txt", report)

    def test_render_missing_history_runbook_includes_seed_command(self) -> None:
        report = render_missing_history_runbook(
            [
                {
                    "capability_profile": "junction_traverse",
                    "source_subset_name": "junction_traverse_history_gap_queue",
                    "source_comparison_label": "junction_traverse__junction_traverse_history_gap_queue",
                    "source_pack_readiness": "no_evidence",
                    "missing_history_routes": ["junction_left", "junction_right"],
                    "seed_route_id": "junction_left",
                    "seed_route_ids_file": "/tmp/junction__seed.route_ids.json",
                    "seed_batch_root": "/tmp/junction_seed_batch",
                    "route_ids_file": "/tmp/junction.route_ids.json",
                    "preset_overrides_file": "/repo/configs/io/examples/town01_route_health_junction_traverse.overrides.txt",
                    "source_overrides_file": "/tmp/source_junction.overrides.txt",
                    "overrides_file": "/tmp/junction.overrides.txt",
                    "effective_overrides": ["algo.apollo.planning.lane_follow_only_scenario=false"],
                    "batch_root": "/tmp/junction_batch",
                    "seed_command": "python3 tools/run_town01_route_health.py run --capability-profile junction_traverse --route-ids-file /tmp/junction__seed.route_ids.json --overrides-file /tmp/junction.overrides.txt",
                    "command": "python3 tools/run_town01_route_health.py run --capability-profile junction_traverse --route-ids-file /tmp/junction.route_ids.json --overrides-file /tmp/junction.overrides.txt",
                }
            ],
            source_manifest=Path("/tmp/review_manifest.json"),
            source_report=Path("/tmp/review_report.md"),
        )
        self.assertIn("seed_route_id: `junction_left`", report)
        self.assertIn("/tmp/junction__seed.route_ids.json", report)
        self.assertIn("--capability-profile junction_traverse --route-ids-file /tmp/junction__seed.route_ids.json --overrides-file /tmp/junction.overrides.txt", report)

    def test_online_pack_parser_defaults_to_seed_followstop_aligned_startup(self) -> None:
        parser = _build_online_pack_parser()
        args = parser.parse_args(["curve_lane_follow"])
        self.assertEqual(args.capability_profile, "curve_lane_follow")
        self.assertEqual(args.mode, "seed")
        self.assertIsNone(args.config)
        self.assertEqual(args.route_id, "")
        self.assertEqual(args.startup_profile, "render_offscreen_no_ros2")
        self.assertEqual(args.carla_launch_attempts, 1)
        self.assertEqual(args.carla_world_ready_timeout_sec, 180.0)
        self.assertEqual(args.carla_retry_delay_sec, 2.0)
        self.assertIsNone(args.enable_lateral)
        self.assertIsNone(args.enable_guard)
        self.assertFalse(args.dry_run)

    def test_online_pack_parser_supports_route_id_override(self) -> None:
        parser = _build_online_pack_parser()
        args = parser.parse_args(
            ["traffic_light_actual", "--mode", "seed", "--route-id", "town01_rh_spawn219_goal063"]
        )
        self.assertEqual(args.route_id, "town01_rh_spawn219_goal063")

    def test_online_pack_parser_supports_lateral_and_guard_flags(self) -> None:
        parser = _build_online_pack_parser()
        args = parser.parse_args(["junction_traverse", "--enable-lateral", "--enable-guard"])
        self.assertTrue(args.enable_lateral)
        self.assertTrue(args.enable_guard)

    def test_online_pack_parser_supports_explicit_config(self) -> None:
        parser = _build_online_pack_parser()
        args = parser.parse_args(
            ["lane_keep", "--config", "/tmp/town01_apollo_route_health_relaxed_probe.yaml"]
        )
        self.assertEqual(
            str(args.config),
            "/tmp/town01_apollo_route_health_relaxed_probe.yaml",
        )

    def test_online_pack_parser_supports_post_fail_steps_override(self) -> None:
        parser = _build_online_pack_parser()
        args = parser.parse_args(["lane_keep", "--post-fail-steps", "120"])
        self.assertEqual(args.post_fail_steps, 120)

    def test_default_runtime_flags_for_capability_enable_lateral_for_claim_profiles(self) -> None:
        self.assertEqual(
            _default_runtime_flags_for_capability("curve_lane_follow"),
            {"enable_lateral": True, "enable_guard": True},
        )
        self.assertEqual(
            _default_runtime_flags_for_capability("traffic_light_actual"),
            {"enable_lateral": True, "enable_guard": True},
        )
        self.assertEqual(
            _default_runtime_flags_for_capability("lane_keep"),
            {"enable_lateral": True, "enable_guard": True},
        )

    def test_resolve_startup_profile_sequence_supports_adaptive_probe_fallback(self) -> None:
        self.assertEqual(resolve_startup_profile_sequence("default"), ["default"])
        self.assertEqual(resolve_startup_profile_sequence("render_offscreen_no_ros2"), ["render_offscreen_no_ros2"])
        self.assertEqual(resolve_startup_profile_sequence("lowres_low_quality"), ["lowres_low_quality"])
        self.assertEqual(resolve_startup_profile_sequence("render_offscreen"), ["render_offscreen"])
        self.assertEqual(resolve_startup_profile_sequence("lowres_no_ros"), ["lowres_no_ros"])
        self.assertEqual(
            resolve_startup_profile_sequence("adaptive"),
            ["render_offscreen_no_ros2", "render_offscreen", "lowres_no_ros"],
        )

    def test_build_online_command_for_seed_uses_lowres_low_quality_ros2_flags(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "lane_keep",
                "source_subset_name": "lane_keep_focus_pack",
                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                "seed_comparison_label": "lane_keep__seed",
                "route_ids_file": "/tmp/lane.full.route_ids.json",
                "comparison_label": "lane_keep__full",
                "overrides_file": "/tmp/lane.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="lowres_low_quality",
            ticks_override=None,
            launch_attempts=2,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=2.0,
            stop_carla_on_exit=True,
        )
        joined = " ".join(argv)
        self.assertIn("--carla-extra-args -windowed -ResX=960 -ResY=540 -quality-level=Low", joined)
        self.assertIn("--carla-force-fresh-start", joined)
        self.assertNotIn("--carla-disable-native-ros2-arg", joined)

    def test_build_online_command_for_seed_uses_render_offscreen_no_ros2_flags(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "lane_keep",
                "source_subset_name": "lane_keep_focus_pack",
                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                "seed_comparison_label": "lane_keep__seed",
                "route_ids_file": "/tmp/lane.full.route_ids.json",
                "comparison_label": "lane_keep__full",
                "overrides_file": "/tmp/lane.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="render_offscreen_no_ros2",
            ticks_override=None,
            launch_attempts=2,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=2.0,
            stop_carla_on_exit=True,
        )
        joined = " ".join(argv)
        self.assertIn("--carla-extra-args -RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low", joined)
        self.assertIn("--carla-force-headless-env", joined)
        self.assertIn("--carla-disable-native-ros2-arg", joined)
        self.assertIn("--carla-force-fresh-start", joined)

    def test_build_online_command_for_seed_uses_render_offscreen_flags(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "lane_keep",
                "source_subset_name": "lane_keep_focus_pack",
                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                "seed_comparison_label": "lane_keep__seed",
                "route_ids_file": "/tmp/lane.full.route_ids.json",
                "comparison_label": "lane_keep__full",
                "overrides_file": "/tmp/lane.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="render_offscreen",
            ticks_override=None,
            launch_attempts=2,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=2.0,
            stop_carla_on_exit=True,
        )
        joined = " ".join(argv)
        self.assertIn("--carla-extra-args -RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low", joined)
        self.assertIn("--carla-force-headless-env", joined)
        self.assertIn("--carla-force-fresh-start", joined)
        self.assertNotIn("--carla-disable-native-ros2-arg", joined)

    def test_build_online_command_for_seed_uses_lowres_no_ros_flags(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "curve_lane_follow",
                "source_subset_name": "curve_lane_follow_proxy_pack",
                "seed_route_ids_file": "/tmp/curve.seed.route_ids.json",
                "seed_comparison_label": "curve_lane_follow__curve_lane_follow_proxy_pack__missing_history__seed",
                "route_ids_file": "/tmp/curve.full.route_ids.json",
                "comparison_label": "curve_lane_follow__curve_lane_follow_proxy_pack__missing_history",
                "overrides_file": "/tmp/curve.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="lowres_no_ros",
            ticks_override=None,
            launch_attempts=4,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=5.0,
            stop_carla_on_exit=True,
        )
        joined = " ".join(argv)
        self.assertIn("--route-ids-file /tmp/curve.seed.route_ids.json", joined)
        self.assertIn("--comparison-label curve_lane_follow__curve_lane_follow_proxy_pack__missing_history__seed__manual_online", joined)
        self.assertIn("--carla-launch-attempts 4", joined)
        self.assertIn("--carla-force-fresh-start", joined)
        self.assertIn("--carla-disable-native-ros2-arg", joined)
        self.assertIn("--carla-extra-args -windowed -ResX=960 -ResY=540 -quality-level=Low", joined)
        self.assertIn("--carla-retry-delay-sec 5.0", joined)
        self.assertIn("--stop-carla-on-exit", joined)

    def test_lowres_no_ros_effective_launch_attempts_respects_requested_value(self) -> None:
        self.assertEqual(_effective_launch_attempts("default", 2), 2)
        self.assertEqual(_effective_launch_attempts("lowres_no_ros", 2), 2)
        argv = build_online_command(
            {
                "capability_profile": "lane_keep",
                "source_subset_name": "lane_keep_focus_pack",
                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                "seed_comparison_label": "lane_keep__seed",
                "route_ids_file": "/tmp/lane.full.route_ids.json",
                "comparison_label": "lane_keep__full",
                "overrides_file": "/tmp/lane.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="lowres_no_ros",
            ticks_override=None,
            launch_attempts=2,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=2.0,
            stop_carla_on_exit=True,
        )
        self.assertIn("--carla-launch-attempts 2", " ".join(argv))

    def test_build_online_command_for_full_default_startup_omits_lowres_flags(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "junction_traverse",
                "source_subset_name": "junction_traverse_proxy_pack",
                "seed_route_ids_file": "/tmp/junction.seed.route_ids.json",
                "seed_comparison_label": "junction_traverse__junction_traverse_proxy_pack__missing_history__seed",
                "route_ids_file": "/tmp/junction.full.route_ids.json",
                "comparison_label": "junction_traverse__junction_traverse_proxy_pack__missing_history",
                "overrides_file": "/tmp/junction.overrides.txt",
                "ticks": 800,
                "extra_args": ["--enable-guard"],
            },
            mode="full",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="default",
            ticks_override=900,
            launch_attempts=3,
            world_ready_timeout_sec=180.0,
            retry_delay_sec=7.5,
            stop_carla_on_exit=False,
            enable_lateral=False,
            enable_guard=False,
        )
        joined = " ".join(argv)
        self.assertIn("--route-ids-file /tmp/junction.full.route_ids.json", joined)
        self.assertIn("--comparison-label junction_traverse__junction_traverse_proxy_pack__missing_history__manual_online", joined)
        self.assertIn("--carla-launch-attempts 3", joined)
        self.assertIn("--carla-world-ready-timeout-sec 180.0", joined)
        self.assertIn("--carla-retry-delay-sec 7.5", joined)
        self.assertIn("--ticks 900", joined)
        self.assertIn("--enable-guard", joined)
        self.assertNotIn("--carla-force-fresh-start", joined)
        self.assertNotIn("--carla-disable-native-ros2-arg", joined)
        self.assertNotIn("--carla-extra-args", joined)

    def test_build_online_command_route_id_override_uses_single_route_without_route_ids_file(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "traffic_light_actual",
                "source_subset_name": "traffic_light_proxy_pack",
                "seed_route_ids_file": "/tmp/traffic.seed.route_ids.json",
                "seed_comparison_label": "traffic_light_actual__traffic_light_proxy_pack__missing_history__seed",
                "route_ids_file": "/tmp/traffic.full.route_ids.json",
                "comparison_label": "traffic_light_actual__traffic_light_proxy_pack__missing_history",
                "overrides_file": "/tmp/traffic.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="default",
            ticks_override=None,
            launch_attempts=4,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=5.0,
            stop_carla_on_exit=True,
            route_id_override="town01_rh_spawn219_goal063",
        )
        joined = " ".join(argv)
        self.assertIn("--route-id town01_rh_spawn219_goal063", joined)
        self.assertNotIn("--route-ids-file", joined)
        self.assertIn("traffic_light_actual__adhoc__town01_rh_spawn219_goal063__seed__manual_online", joined)

    def test_build_online_command_for_seed_can_forward_lateral_runtime_flags(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "junction_traverse",
                "source_subset_name": "junction_traverse_proxy_pack",
                "seed_route_ids_file": "/tmp/junction.seed.route_ids.json",
                "seed_comparison_label": "junction_traverse__junction_traverse_proxy_pack__missing_history__seed",
                "route_ids_file": "/tmp/junction.full.route_ids.json",
                "comparison_label": "junction_traverse__junction_traverse_proxy_pack__missing_history",
                "overrides_file": "/tmp/junction.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="default",
            ticks_override=None,
            launch_attempts=4,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=5.0,
            stop_carla_on_exit=True,
            enable_lateral=True,
            enable_guard=True,
        )
        joined = " ".join(argv)
        self.assertIn("--enable-lateral", joined)
        self.assertIn("--enable-guard", joined)

    def test_build_online_command_can_forward_explicit_config(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "lane_keep",
                "source_subset_name": "lane_keep_focus_pack",
                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                "seed_comparison_label": "lane_keep__seed",
                "route_ids_file": "/tmp/lane.full.route_ids.json",
                "comparison_label": "lane_keep__full",
                "overrides_file": "/tmp/lane.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="render_offscreen_no_ros2",
            config_path=Path("/tmp/town01_apollo_route_health_relaxed_probe.yaml"),
            ticks_override=None,
            launch_attempts=1,
            world_ready_timeout_sec=180.0,
            retry_delay_sec=2.0,
            stop_carla_on_exit=True,
        )
        joined = " ".join(argv)
        self.assertIn("--config /tmp/town01_apollo_route_health_relaxed_probe.yaml", joined)

    def test_build_online_command_can_forward_post_fail_steps_override(self) -> None:
        argv = build_online_command(
            {
                "capability_profile": "lane_keep",
                "source_subset_name": "lane_keep_focus_pack",
                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                "seed_comparison_label": "lane_keep__seed",
                "route_ids_file": "/tmp/lane.full.route_ids.json",
                "comparison_label": "lane_keep__full",
                "overrides_file": "/tmp/lane.overrides.txt",
                "ticks": 700,
                "extra_args": [],
            },
            mode="seed",
            batch_root_parent=Path("/tmp/manual_online"),
            comparison_label_suffix="manual_online",
            startup_profile="render_offscreen_no_ros2",
            ticks_override=320,
            post_fail_steps_override=120,
            launch_attempts=1,
            world_ready_timeout_sec=180.0,
            retry_delay_sec=2.0,
            stop_carla_on_exit=True,
        )
        joined = " ".join(argv)
        self.assertIn("--override run.post_fail_steps=120", joined)

    def test_estimate_online_time_budget_uses_startup_and_nominal_runtime(self) -> None:
        budget = estimate_online_time_budget(
            {
                "capability_profile": "traffic_light_actual",
                "seed_route_id": "town01_rh_spawn129_goal051",
                "missing_history_routes": [
                    "town01_rh_spawn129_goal051",
                    "town01_rh_spawn097_goal046",
                ],
                "ticks": 700,
            },
            mode="full",
            ticks_override=None,
            launch_attempts=4,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=5.0,
        )
        self.assertEqual(budget["route_count"], 2)
        self.assertEqual(budget["ticks"], 700)
        self.assertEqual(budget["nominal_route_runtime_sec"], 35.0)
        self.assertEqual(budget["nominal_batch_runtime_sec"], 70.0)
        self.assertEqual(budget["startup_budget_sec"], 375.0)
        self.assertEqual(budget["total_budget_sec"], 445.0)
        self.assertEqual(budget["total_budget_min"], 7.4)

    def test_estimate_online_time_budget_route_id_override_forces_single_route(self) -> None:
        budget = estimate_online_time_budget(
            {
                "capability_profile": "curve_lane_follow",
                "seed_route_id": "town01_rh_spawn217_goal048",
                "missing_history_routes": [
                    "town01_rh_spawn119_goal059",
                    "town01_rh_spawn213_goal059",
                ],
                "ticks": 700,
            },
            mode="full",
            ticks_override=None,
            launch_attempts=4,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=5.0,
            route_id_override="town01_rh_spawn119_goal059",
        )
        self.assertEqual(budget["route_count"], 1)
        self.assertEqual(budget["nominal_batch_runtime_sec"], 35.0)
        self.assertEqual(budget["total_budget_sec"], 410.0)

    def test_online_chain_parser_defaults_to_three_capabilities_seed_followstop_aligned_startup(self) -> None:
        parser = _build_online_chain_parser()
        args = parser.parse_args([])
        self.assertEqual(args.capability_profiles, [])
        self.assertEqual(args.step, [])
        self.assertEqual(args.mode, "seed")
        self.assertIsNone(args.config)
        self.assertEqual(args.startup_profile, "render_offscreen_no_ros2")
        self.assertEqual(args.carla_launch_attempts, 1)
        self.assertEqual(args.carla_world_ready_timeout_sec, 180.0)
        self.assertEqual(args.carla_retry_delay_sec, 2.0)
        self.assertTrue(args.progress)
        self.assertEqual(args.progress_update_sec, 5.0)
        self.assertTrue(args.auto_probe_early_stop)
        self.assertEqual(args.early_stop_planning_no_control_sec, 20.0)
        self.assertEqual(args.early_stop_min_step_elapsed_sec, 15.0)
        self.assertEqual(args.early_stop_min_planning_nonempty, 80)
        self.assertEqual(args.early_stop_max_speed_mps, 0.2)
        self.assertTrue(args.prewarm_carla)
        self.assertEqual(args.prewarm_carla_launch_attempts, 2)
        self.assertIsNone(args.enable_lateral)
        self.assertIsNone(args.enable_guard)
        self.assertFalse(args.keep_carla_alive_at_end)

    def test_online_chain_parser_supports_lateral_and_guard_flags(self) -> None:
        parser = _build_online_chain_parser()
        args = parser.parse_args(["--enable-lateral", "--enable-guard"])
        self.assertTrue(args.enable_lateral)
        self.assertTrue(args.enable_guard)

    def test_online_chain_parser_supports_explicit_config_and_progress_toggle(self) -> None:
        parser = _build_online_chain_parser()
        args = parser.parse_args(
            [
                "--config",
                "/tmp/town01_apollo_route_health_relaxed_probe.yaml",
                "--no-progress",
                "--progress-update-sec",
                "2.5",
            ]
        )
        self.assertEqual(str(args.config), "/tmp/town01_apollo_route_health_relaxed_probe.yaml")
        self.assertFalse(args.progress)
        self.assertEqual(args.progress_update_sec, 2.5)

    def test_online_chain_parser_supports_post_fail_steps_override(self) -> None:
        parser = _build_online_chain_parser()
        args = parser.parse_args(["--post-fail-steps", "120"])
        self.assertEqual(args.post_fail_steps, 120)

    def test_online_chain_parser_supports_ordered_route_steps(self) -> None:
        parser = _build_online_chain_parser()
        args = parser.parse_args(
            [
                "--step",
                "curve_lane_follow:town01_rh_spawn119_goal059",
                "--step",
                "traffic_light_actual:town01_rh_spawn179_goal063",
            ]
        )
        self.assertEqual(
            args.step,
            [
                "curve_lane_follow:town01_rh_spawn119_goal059",
                "traffic_light_actual:town01_rh_spawn179_goal063",
            ],
        )

    def test_build_online_chain_plan_reuses_carla_after_first_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "capability_profile": "curve_lane_follow",
                                "source_subset_name": "curve_lane_follow_proxy_pack",
                                "seed_route_id": "curve_seed",
                                "seed_route_ids_file": "/tmp/curve.seed.route_ids.json",
                                "seed_comparison_label": "curve__seed",
                                "route_ids_file": "/tmp/curve.full.route_ids.json",
                                "comparison_label": "curve__full",
                                "overrides_file": "/tmp/curve.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["curve_seed"],
                            },
                            {
                                "capability_profile": "junction_traverse",
                                "source_subset_name": "junction_traverse_proxy_pack",
                                "seed_route_id": "junction_seed",
                                "seed_route_ids_file": "/tmp/junction.seed.route_ids.json",
                                "seed_comparison_label": "junction__seed",
                                "route_ids_file": "/tmp/junction.full.route_ids.json",
                                "comparison_label": "junction__full",
                                "overrides_file": "/tmp/junction.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["junction_seed"],
                            },
                            {
                                "capability_profile": "traffic_light_actual",
                                "source_subset_name": "traffic_light_proxy_pack",
                                "seed_route_id": "traffic_seed",
                                "seed_route_ids_file": "/tmp/traffic.seed.route_ids.json",
                                "seed_comparison_label": "traffic__seed",
                                "route_ids_file": "/tmp/traffic.full.route_ids.json",
                                "comparison_label": "traffic__full",
                                "overrides_file": "/tmp/traffic.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["traffic_seed", "traffic_peer"],
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            plan = build_online_chain_plan(
                manifest_path=manifest_path,
                route_steps=[],
                capability_profiles=["curve_lane_follow", "junction_traverse", "traffic_light_actual"],
                mode="seed",
                config_path=None,
                batch_root_parent=Path("/tmp/online_chain"),
                comparison_label_suffix="manual_online_chain",
                startup_profile="lowres_no_ros",
                ticks_override=None,
                post_fail_steps_override=None,
                launch_attempts=4,
                world_ready_timeout_sec=90.0,
                retry_delay_sec=5.0,
                keep_carla_alive_at_end=False,
                enable_lateral=True,
                enable_guard=True,
            )
        self.assertEqual(len(plan), 3)
        self.assertTrue(plan[0]["force_fresh_start"])
        self.assertFalse(plan[1]["force_fresh_start"])
        self.assertFalse(plan[2]["force_fresh_start"])
        self.assertFalse(plan[0]["stop_carla_on_exit"])
        self.assertFalse(plan[1]["stop_carla_on_exit"])
        self.assertTrue(plan[2]["stop_carla_on_exit"])
        self.assertIn("--carla-force-fresh-start", plan[0]["command"])
        self.assertNotIn("--carla-force-fresh-start", plan[1]["command"])
        self.assertNotIn("--carla-force-fresh-start", plan[2]["command"])
        self.assertNotIn("--stop-carla-on-exit", plan[0]["command"])
        self.assertNotIn("--stop-carla-on-exit", plan[1]["command"])
        self.assertIn("--stop-carla-on-exit", plan[2]["command"])
        self.assertIn("--enable-lateral", plan[0]["command"])
        self.assertIn("--enable-guard", plan[1]["command"])
        self.assertTrue(plan[0]["enable_lateral"])
        self.assertTrue(plan[1]["enable_lateral"])
        self.assertTrue(plan[2]["enable_lateral"])

    def test_build_online_chain_plan_reuses_prewarmed_carla_from_first_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "capability_profile": "lane_keep",
                                "source_subset_name": "lane_keep_focus_pack",
                                "seed_route_id": "lane_seed",
                                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                                "seed_comparison_label": "lane__seed",
                                "route_ids_file": "/tmp/lane.full.route_ids.json",
                                "comparison_label": "lane__full",
                                "overrides_file": "/tmp/lane.overrides.txt",
                                "ticks": 320,
                                "missing_history_routes": ["lane_seed"],
                            },
                            {
                                "capability_profile": "junction_traverse",
                                "source_subset_name": "junction_proxy_pack",
                                "seed_route_id": "junction_seed",
                                "seed_route_ids_file": "/tmp/junction.seed.route_ids.json",
                                "seed_comparison_label": "junction__seed",
                                "route_ids_file": "/tmp/junction.full.route_ids.json",
                                "comparison_label": "junction__full",
                                "overrides_file": "/tmp/junction.overrides.txt",
                                "ticks": 320,
                                "missing_history_routes": ["junction_seed"],
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            plan = build_online_chain_plan(
                manifest_path=manifest_path,
                route_steps=[("lane_keep", "town01_rh_spawn097_goal046"), ("junction_traverse", "town01_rh_spawn031_goal056")],
                capability_profiles=[],
                mode="seed",
                config_path=None,
                batch_root_parent=Path("/tmp/online_chain"),
                comparison_label_suffix="manual_online_chain",
                startup_profile="render_offscreen_no_ros2",
                ticks_override=None,
                post_fail_steps_override=None,
                launch_attempts=1,
                world_ready_timeout_sec=180.0,
                retry_delay_sec=2.0,
                keep_carla_alive_at_end=False,
                prewarmed_carla=True,
            )
        self.assertEqual(len(plan), 2)
        self.assertFalse(plan[0]["force_fresh_start"])
        self.assertFalse(plan[1]["force_fresh_start"])
        self.assertTrue(plan[0]["prewarmed_carla"])
        self.assertNotIn("--carla-force-fresh-start", plan[0]["command"])

    def test_build_online_chain_plan_force_fresh_start_first_step_for_render_offscreen(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "capability_profile": "lane_keep",
                                "source_subset_name": "lane_keep_focus_pack",
                                "seed_route_id": "lane_seed",
                                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                                "seed_comparison_label": "lane__seed",
                                "route_ids_file": "/tmp/lane.full.route_ids.json",
                                "comparison_label": "lane__full",
                                "overrides_file": "/tmp/lane.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["lane_seed"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            plan = build_online_chain_plan(
                manifest_path=manifest_path,
                route_steps=[("lane_keep", None)],
                capability_profiles=[],
                mode="seed",
                config_path=None,
                batch_root_parent=Path("/tmp/online_chain"),
                comparison_label_suffix="manual_online_chain",
                startup_profile="render_offscreen",
                ticks_override=None,
                post_fail_steps_override=None,
                launch_attempts=2,
                world_ready_timeout_sec=90.0,
                retry_delay_sec=2.0,
                keep_carla_alive_at_end=False,
            )
        self.assertEqual(len(plan), 1)
        self.assertTrue(plan[0]["force_fresh_start"])
        self.assertIn("--carla-force-fresh-start", plan[0]["command"])

    def test_build_online_chain_plan_force_fresh_start_first_step_for_render_offscreen_no_ros2(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "capability_profile": "lane_keep",
                                "source_subset_name": "lane_keep_focus_pack",
                                "seed_route_id": "lane_seed",
                                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                                "seed_comparison_label": "lane__seed",
                                "route_ids_file": "/tmp/lane.full.route_ids.json",
                                "comparison_label": "lane__full",
                                "overrides_file": "/tmp/lane.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["lane_seed"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            plan = build_online_chain_plan(
                manifest_path=manifest_path,
                route_steps=[("lane_keep", None)],
                capability_profiles=[],
                mode="seed",
                config_path=None,
                batch_root_parent=Path("/tmp/online_chain"),
                comparison_label_suffix="manual_online_chain",
                startup_profile="render_offscreen_no_ros2",
                ticks_override=None,
                post_fail_steps_override=None,
                launch_attempts=2,
                world_ready_timeout_sec=90.0,
                retry_delay_sec=2.0,
                keep_carla_alive_at_end=False,
            )
        self.assertEqual(len(plan), 1)
        self.assertTrue(plan[0]["force_fresh_start"])
        self.assertIn("--carla-force-fresh-start", plan[0]["command"])
        self.assertIn("--carla-disable-native-ros2-arg", plan[0]["command"])

    def test_build_prewarm_route_health_args_tracks_render_offscreen_no_ros2_profile(self) -> None:
        args = capability_online_chain._build_prewarm_route_health_args(
            startup_profile="render_offscreen_no_ros2",
            launch_attempts=2,
            world_ready_timeout_sec=180.0,
            retry_delay_sec=2.0,
        )
        self.assertEqual(args.startup_profile, "render_offscreen_no_ros2")
        self.assertEqual(args.carla_launch_attempts, 2)
        self.assertTrue(args.carla_force_fresh_start)
        self.assertTrue(args.carla_force_headless_env)
        self.assertTrue(args.carla_disable_native_ros2_arg)
        self.assertIn("-RenderOffScreen", args.carla_extra_args)

    def test_build_online_chain_plan_supports_route_id_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "capability_profile": "curve_lane_follow",
                                "source_subset_name": "curve_lane_follow_proxy_pack",
                                "seed_route_id": "curve_seed",
                                "seed_route_ids_file": "/tmp/curve.seed.route_ids.json",
                                "seed_comparison_label": "curve__seed",
                                "route_ids_file": "/tmp/curve.full.route_ids.json",
                                "comparison_label": "curve__full",
                                "overrides_file": "/tmp/curve.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["curve_seed"],
                            },
                            {
                                "capability_profile": "traffic_light_actual",
                                "source_subset_name": "traffic_light_proxy_pack",
                                "seed_route_id": "traffic_seed",
                                "seed_route_ids_file": "/tmp/traffic.seed.route_ids.json",
                                "seed_comparison_label": "traffic__seed",
                                "route_ids_file": "/tmp/traffic.full.route_ids.json",
                                "comparison_label": "traffic__full",
                                "overrides_file": "/tmp/traffic.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["traffic_seed", "traffic_peer"],
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            plan = build_online_chain_plan(
                manifest_path=manifest_path,
                route_steps=[
                    ("curve_lane_follow", "town01_rh_spawn119_goal059"),
                    ("traffic_light_actual", "town01_rh_spawn179_goal063"),
                ],
                capability_profiles=[],
                mode="seed",
                config_path=None,
                batch_root_parent=Path("/tmp/online_chain"),
                comparison_label_suffix="manual_online_chain",
                startup_profile="default",
                ticks_override=None,
                post_fail_steps_override=None,
                launch_attempts=2,
                world_ready_timeout_sec=90.0,
                retry_delay_sec=2.0,
                keep_carla_alive_at_end=False,
                enable_lateral=None,
                enable_guard=None,
            )
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[0]["route_id"], "town01_rh_spawn119_goal059")
        self.assertEqual(plan[1]["route_id"], "town01_rh_spawn179_goal063")
        self.assertIn("--route-id town01_rh_spawn119_goal059", plan[0]["command"])
        self.assertIn("--route-id town01_rh_spawn179_goal063", plan[1]["command"])
        self.assertNotIn("--route-ids-file", plan[0]["command"])
        self.assertFalse(plan[0]["stop_carla_on_exit"])
        self.assertTrue(plan[1]["stop_carla_on_exit"])
        self.assertTrue(plan[0]["enable_lateral"])
        self.assertTrue(plan[1]["enable_lateral"])

    def test_build_online_chain_plan_forwards_explicit_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            config_path = Path(tmpdir) / "recovery.yaml"
            config_path.write_text(
                "run:\n  ticks: 320\n  post_fail_steps: 120\n",
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "capability_profile": "lane_keep",
                                "source_subset_name": "lane_keep_focus_pack",
                                "seed_route_id": "lane_seed",
                                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                                "seed_comparison_label": "lane__seed",
                                "route_ids_file": "/tmp/lane.full.route_ids.json",
                                "comparison_label": "lane__full",
                                "overrides_file": "/tmp/lane.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["lane_seed"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            plan = build_online_chain_plan(
                manifest_path=manifest_path,
                route_steps=[("lane_keep", "town01_rh_spawn097_goal046")],
                capability_profiles=[],
                mode="seed",
                config_path=config_path,
                batch_root_parent=Path("/tmp/online_chain"),
                comparison_label_suffix="manual_online_chain",
                startup_profile="render_offscreen_no_ros2",
                ticks_override=None,
                post_fail_steps_override=120,
                launch_attempts=1,
                world_ready_timeout_sec=180.0,
                retry_delay_sec=2.0,
                keep_carla_alive_at_end=False,
            )
        self.assertIn(
            f"--config {config_path}",
            plan[0]["command"],
        )
        self.assertIn("--ticks 320", plan[0]["command"])
        self.assertEqual(plan[0]["ticks"], 320)
        self.assertIn("--override run.post_fail_steps=120", plan[0]["command"])

    def test_build_online_chain_plan_forwards_extra_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "capability_profile": "lane_keep",
                                "source_subset_name": "lane_keep_focus_pack",
                                "seed_route_id": "lane_seed",
                                "seed_route_ids_file": "/tmp/lane.seed.route_ids.json",
                                "seed_comparison_label": "lane__seed",
                                "route_ids_file": "/tmp/lane.full.route_ids.json",
                                "comparison_label": "lane__full",
                                "overrides_file": "/tmp/lane.overrides.txt",
                                "ticks": 700,
                                "missing_history_routes": ["lane_seed"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            plan = build_online_chain_plan(
                manifest_path=manifest_path,
                route_steps=[("lane_keep", "town01_rh_spawn097_goal046")],
                capability_profiles=[],
                mode="seed",
                config_path=None,
                batch_root_parent=Path("/tmp/online_chain"),
                comparison_label_suffix="manual_online_chain",
                startup_profile="render_offscreen_no_ros2",
                ticks_override=None,
                post_fail_steps_override=None,
                launch_attempts=1,
                world_ready_timeout_sec=180.0,
                retry_delay_sec=2.0,
                keep_carla_alive_at_end=False,
                extra_overrides=[
                    "algo.apollo.dreamview.enabled=false",
                    "algo.apollo.direct_bridge.poll_hz=20.0",
                ],
                carla_ignore_memory_preflight=True,
            )
        self.assertIn("--override algo.apollo.dreamview.enabled=false", plan[0]["command"])
        self.assertIn("--override algo.apollo.direct_bridge.poll_hz=20.0", plan[0]["command"])
        self.assertIn("--carla-ignore-memory-preflight", plan[0]["command"])

    def test_online_chain_derive_live_phase_prefers_control_pending_after_planning(self) -> None:
        phase = capability_online_chain._derive_live_phase(
            {
                "routing_request_count": 1,
                "planning_nonempty_trajectory_count": 12,
                "control_tx_count": 0,
                "deferred_control_pending": True,
                "control_running": False,
            }
        )
        self.assertEqual(phase, "control_pending")

    def test_online_chain_derive_live_phase_treats_direct_apply_as_control_output(self) -> None:
        phase = capability_online_chain._derive_live_phase(
            {
                "routing_request_count": 1,
                "planning_nonempty_trajectory_count": 12,
                "control_tx_count": 0,
                "direct_control_apply_count": 3,
                "deferred_control_pending": True,
                "control_running": False,
            }
        )
        self.assertEqual(phase, "control_output")

    def test_online_chain_derive_live_phase_surfaces_startup_failure_family_before_routing_wait(self) -> None:
        phase = capability_online_chain._derive_live_phase(
            {
                "carla_startup_status": "rpc_ready",
                "carla_startup_failure_family": "rpc_ready_followup_missing_eof_alive",
                "routing_request_count": 0,
                "planning_nonempty_trajectory_count": 0,
                "control_tx_count": 0,
            }
        )
        self.assertEqual(phase, "startup_rpc_ready_followup_missing_eof_alive")

    def test_online_chain_render_progress_line_includes_eta_and_phase(self) -> None:
        line = capability_online_chain._render_progress_line(
            item={"step_index": 1, "chain_step_count": 2},
            step_elapsed_sec=15.0,
            chain_elapsed_sec=30.0,
            step_budget_sec=60.0,
            chain_budget_sec=180.0,
            snapshot={
                "phase": "planning_wait",
                "manifest_status": "running",
                "route_health_label": "chain_not_alive",
                "summary_status": "provisional",
                "scenario_progress_current": 160,
                "scenario_progress_total": 320,
                "scenario_progress_fraction": 0.5,
                "scenario_progress_remaining_sec": 8.0,
                "routing_request_count": 1,
                "planning_nonempty_trajectory_count": 8,
                "control_tx_count": 0,
                "speed_mps": 0.4,
            },
        )
        self.assertIn("eta_step=", line)
        self.assertIn("eta_chain=", line)
        self.assertIn("phase=planning_wait", line)
        self.assertIn("ticks=160/320", line)
        self.assertIn("eta_scene=00:08", line)
        self.assertIn("routing=1", line)
        self.assertIn("planning=8", line)

    def test_online_chain_render_progress_line_prefers_control_output_for_direct_apply(self) -> None:
        line = capability_online_chain._render_progress_line(
            item={"step_index": 1, "chain_step_count": 1},
            step_elapsed_sec=15.0,
            chain_elapsed_sec=15.0,
            step_budget_sec=60.0,
            chain_budget_sec=60.0,
            snapshot={
                "phase": "control_output",
                "manifest_status": "running",
                "routing_request_count": 1,
                "planning_nonempty_trajectory_count": 8,
                "control_tx_count": 0,
                "direct_control_apply_count": 4,
                "deferred_control_pending": True,
                "speed_mps": 1.2,
            },
        )
        self.assertIn("control=output", line)
        self.assertIn("direct_apply=4", line)
        self.assertNotIn("control=pending", line)

    def test_direct_metric_consistency_flags_short_apply_metric_mismatch(self) -> None:
        consistency = _compute_direct_metric_consistency(
            {"transport_mode": "carla_direct"},
            {"route_distance_achieved_m": 185.0, "route_completion_ratio": 0.84},
            27.7,
            {
                "apply_count": 14,
                "window_status": "short_apply_window",
                "apply_frame_span": 14,
                "max_speed_mps": 0.59,
                "max_throttle": 1.0,
            },
        )

        self.assertEqual(consistency["status"], "short_apply_metric_mismatch")
        self.assertIn("summary_speed_exceeds_direct_apply_speed", consistency["reasons"])
        self.assertIn("route_distance_too_large_for_short_direct_apply_window", consistency["reasons"])

    def test_direct_metric_consistency_keeps_long_apply_low_speed_consistent(self) -> None:
        consistency = _compute_direct_metric_consistency(
            {"transport_mode": "carla_direct"},
            {"route_distance_achieved_m": 33.8, "route_completion_ratio": 0.14},
            3.67,
            {
                "apply_count": 311,
                "window_status": "observable_apply_window",
                "apply_frame_span": 311,
                "max_speed_mps": 3.67,
                "max_throttle": 1.0,
            },
        )

        self.assertEqual(consistency["status"], "consistent")
        self.assertEqual(consistency["reasons"], [])

    def test_online_chain_extract_followstop_progress_reads_last_progress_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_root = Path(tmpdir)
            log_path = batch_root / "case" / "artifacts" / "followstop_child.stdout.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(
                "[progress] 16/320 (5%), est remaining 15.2s\n"
                "noise\n"
                "[progress] 304/320 (95%), est remaining 0.8s\n",
                encoding="utf-8",
            )
            snapshot = capability_online_chain._extract_followstop_progress(batch_root)
        self.assertEqual(snapshot["scenario_progress_current"], 304)
        self.assertEqual(snapshot["scenario_progress_total"], 320)
        self.assertAlmostEqual(snapshot["scenario_progress_fraction"], 0.95, places=3)
        self.assertAlmostEqual(snapshot["scenario_progress_remaining_sec"], 0.8, places=3)

    def test_online_chain_extract_followstop_progress_prefers_current_redirected_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_root = Path(tmpdir)
            stale_log = batch_root / "zz_old_run" / "artifacts" / "followstop_child.stdout.log"
            stale_log.parent.mkdir(parents=True, exist_ok=True)
            stale_log.write_text("[progress] 399/420 (95%), est remaining 1.0s\n", encoding="utf-8")

            effective_run_dir = batch_root / "aa_current_run__02"
            effective_run_dir.mkdir(parents=True, exist_ok=True)
            planned_run_dir = batch_root / "aa_current_run"
            planned_log = planned_run_dir / "artifacts" / "followstop_child.stdout.log"
            planned_log.parent.mkdir(parents=True, exist_ok=True)
            planned_log.write_text("[progress] 3/160 (1%), est remaining 45.0s\n", encoding="utf-8")
            (planned_run_dir / "RUN_DIR_REDIRECT.txt").write_text(str(effective_run_dir), encoding="utf-8")
            (batch_root / "LATEST.txt").write_text(str(effective_run_dir), encoding="utf-8")

            snapshot = capability_online_chain._collect_step_live_snapshot(
                {
                    "command_argv": [
                        "python3",
                        "/tmp/run_town01_route_health.py",
                        "--batch-root",
                        str(batch_root),
                    ]
                }
            )

        self.assertEqual(snapshot["scenario_progress_current"], 3)
        self.assertEqual(snapshot["scenario_progress_total"], 160)
        self.assertIn("aa_current_run", snapshot["scenario_progress_source"])
        self.assertNotIn("zz_old_run", snapshot["scenario_progress_source"])

    def test_collect_step_live_snapshot_exposes_carla_startup_probe_before_internal_run_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_root = Path(tmpdir)
            _write_json(
                batch_root / "carla_boot" / "carla_startup_probe.json",
                {
                    "status": "rpc_ready",
                    "rpc_ready": True,
                    "world_ready": False,
                    "failure_family": "rpc_ready_followup_missing_eof_alive",
                },
            )
            item = {
                "command_argv": [
                    "python3",
                    "/tmp/run_town01_route_health.py",
                    "--batch-root",
                    str(batch_root),
                ]
            }
            snapshot = capability_online_chain._collect_step_live_snapshot(item)
        self.assertEqual(snapshot["carla_startup_status"], "rpc_ready")
        self.assertTrue(snapshot["carla_startup_rpc_ready"])
        self.assertFalse(snapshot["carla_startup_world_ready"])
        self.assertEqual(snapshot["carla_startup_failure_family"], "rpc_ready_followup_missing_eof_alive")
        self.assertEqual(snapshot["phase"], "startup_rpc_ready_followup_missing_eof_alive")

    def test_collect_step_live_snapshot_prefers_finalized_summary_over_stale_provisional(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_root = Path(tmpdir)
            effective_run_dir = batch_root / "current_run"
            effective_run_dir.mkdir(parents=True, exist_ok=True)
            (batch_root / "LATEST.txt").write_text(str(effective_run_dir), encoding="utf-8")
            _write_json(
                effective_run_dir / "summary.provisional.json",
                {
                    "summary_status": "provisional",
                    "route_health_label": "chain_not_alive",
                },
            )
            _write_json(
                effective_run_dir / "summary.json",
                {
                    "summary_status": "finalized",
                    "route_health_label": "route_established_but_behavior_unhealthy",
                },
            )
            _write_json(
                effective_run_dir / "artifacts" / "cyber_bridge_stats.json",
                {
                    "routing_request_count": 1,
                    "control_tx_count": 2418,
                    "last_measured_control": {"speed_mps": 0.0},
                    "planning": {"nonempty_trajectory_count": 319},
                },
            )
            item = {
                "command_argv": [
                    "python3",
                    "/tmp/run_town01_route_health.py",
                    "--batch-root",
                    str(batch_root),
                ]
            }
            snapshot = capability_online_chain._collect_step_live_snapshot(item)

        self.assertEqual(snapshot["summary_status"], "finalized")
        self.assertEqual(
            snapshot["route_health_label"],
            "route_established_but_behavior_unhealthy",
        )
        self.assertEqual(snapshot["phase"], "control_output")
        self.assertEqual(snapshot["control_tx_count"], 2418)

    def test_route_health_state_timeline_prefers_sim_time_over_wall_timestamp(self) -> None:
        timeline, failure_stage = _build_state_timeline(
            routing_rows=[
                {
                    "timestamp": 1_782_415_622.5,
                    "sim_time_sec": 122.1,
                    "routing_request_sent": True,
                }
            ],
            planning_rows=[
                {
                    "timestamp": 1_782_415_624.9,
                    "sim_time_sec": 124.5,
                    "trajectory_point_count": 70,
                }
            ],
            control_events=[
                {
                    "timestamp": 1_782_415_625.0,
                    "sim_time_sec": 124.6,
                    "control_used_planning_trajectory": True,
                }
            ],
            debug_rows=[
                {
                    "ts_sec": 114.1,
                    "map_x": 0.0,
                    "map_y": 0.0,
                    "speed_mps": 0.0,
                },
                {
                    "ts_sec": 125.0,
                    "map_x": 6.0,
                    "map_y": 0.0,
                    "speed_mps": 2.0,
                },
            ],
            route_metrics={"route_completion_ratio": 0.5, "final_goal_distance_m": 100.0},
            routing_success_count=1,
        )

        by_state = {item["state"]: item for item in timeline}
        latency = (
            by_state["ROUTE_ESTABLISHED"]["entered_ts_sec"]
            - by_state["CARLA_READY"]["entered_ts_sec"]
        )
        self.assertAlmostEqual(latency, 8.0, places=3)
        self.assertLess(latency, 45.0)
        self.assertEqual(failure_stage, "CRUISE_ACTIVE")

    def test_planning_nonzero_ratio_uses_post_route_established_window(self) -> None:
        planning_rows = [
            {"sim_time_sec": 110.0, "trajectory_point_count": 0},
            {"sim_time_sec": 110.1, "trajectory_point_count": 0},
            {"sim_time_sec": 110.2, "trajectory_point_count": 0},
            {"sim_time_sec": 110.3, "trajectory_point_count": 0},
            {"sim_time_sec": 118.3, "trajectory_point_count": 120},
            {"sim_time_sec": 118.4, "trajectory_point_count": 125},
        ]
        planning_summary = {
            "total_messages_received": 6,
            "messages_with_nonzero_trajectory_points": 2,
        }

        check = _planning_nonzero_ratio_check(
            planning_rows,
            planning_summary,
            route_established_ts=118.0,
        )

        self.assertEqual(check["source"], "post_route_established_planning_messages")
        self.assertEqual(check["numerator"], 2)
        self.assertEqual(check["denominator"], 2)
        self.assertEqual(check["all_messages_numerator"], 2)
        self.assertEqual(check["all_messages_denominator"], 6)
        self.assertEqual(check["actual"], 1.0)

    def test_estimate_online_chain_time_budget_counts_startup_once(self) -> None:
        budget = estimate_online_chain_time_budget(
            [
                {"capability_profile": "curve_lane_follow", "route_count": 1, "nominal_batch_runtime_sec": 35.0},
                {"capability_profile": "junction_traverse", "route_count": 1, "nominal_batch_runtime_sec": 35.0},
                {"capability_profile": "traffic_light_actual", "route_count": 2, "nominal_batch_runtime_sec": 70.0},
            ],
            launch_attempts=4,
            world_ready_timeout_sec=90.0,
            retry_delay_sec=5.0,
        )
        self.assertEqual(budget["capability_count"], 3)
        self.assertEqual(budget["route_count"], 4)
        self.assertEqual(budget["startup_budget_sec"], 375.0)
        self.assertEqual(budget["nominal_batch_runtime_sec"], 140.0)
        self.assertEqual(budget["total_budget_sec"], 515.0)
        self.assertEqual(budget["total_budget_min"], 8.6)

    def test_estimate_online_time_budget_prefers_config_ticks_over_manifest_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "probe.yaml"
            config_path.write_text("run:\n  ticks: 320\n", encoding="utf-8")
            budget = capability_online_pack.estimate_online_time_budget(
                {
                    "capability_profile": "lane_keep",
                    "ticks": 700,
                    "seed_route_id": "lane_seed",
                },
                mode="seed",
                startup_profile="render_offscreen_no_ros2",
                config_path=config_path,
                ticks_override=None,
                launch_attempts=1,
                world_ready_timeout_sec=180.0,
                retry_delay_sec=2.0,
            )
        self.assertEqual(budget["ticks"], 320)
        self.assertEqual(budget["nominal_batch_runtime_sec"], 16.0)

    def test_online_chain_probe_early_stop_condition_requires_planning_alive_and_control_silent(self) -> None:
        should_stop = capability_online_chain._probe_early_stop_condition(
            item={"route_count": 1},
            snapshot={
                "bridge_runtime_preflight_status": "bridge_runtime_ready",
                "routing_request_count": 1,
                "planning_nonempty_trajectory_count": 120,
                "control_tx_count": 0,
                "phase": "control_pending",
                "speed_mps": 0.0,
            },
            step_elapsed_sec=32.0,
            min_step_elapsed_sec=15.0,
            min_planning_nonempty=80,
            max_speed_mps=0.2,
        )
        self.assertTrue(should_stop)
        should_not_stop = capability_online_chain._probe_early_stop_condition(
            item={"route_count": 1},
            snapshot={
                "bridge_runtime_preflight_status": "bridge_runtime_ready",
                "routing_request_count": 1,
                "planning_nonempty_trajectory_count": 120,
                "control_tx_count": 3,
                "phase": "control_output",
                "speed_mps": 1.4,
            },
            step_elapsed_sec=32.0,
            min_step_elapsed_sec=15.0,
            min_planning_nonempty=80,
            max_speed_mps=0.2,
        )
        self.assertFalse(should_not_stop)

    def test_run_parser_supports_disabling_auto_ros2_prestart_arg(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-disable-native-ros2-arg",
            ]
        )
        self.assertTrue(args.carla_disable_native_ros2_arg)

    def test_build_corpus_parser_supports_disabling_auto_ros2_prestart_arg(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "build-corpus",
                "--start-carla",
                "--carla-disable-native-ros2-arg",
            ]
        )
        self.assertTrue(args.carla_disable_native_ros2_arg)

    def test_run_parser_supports_force_fresh_carla_start(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-force-fresh-start",
            ]
        )
        self.assertTrue(args.carla_force_fresh_start)

    def test_build_corpus_parser_supports_force_fresh_carla_start(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "build-corpus",
                "--start-carla",
                "--carla-force-fresh-start",
            ]
        )
        self.assertTrue(args.carla_force_fresh_start)

    def test_run_parser_supports_force_sdl_x11_no_xrandr(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-force-sdl-x11-no-xrandr",
            ]
        )
        self.assertTrue(args.carla_force_sdl_x11_no_xrandr)

    def test_run_parser_supports_force_headless_env(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-force-headless-env",
            ]
        )
        self.assertTrue(args.carla_force_headless_env)

    def test_build_corpus_parser_supports_force_sdl_x11_no_xrandr(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "build-corpus",
                "--start-carla",
                "--carla-force-sdl-x11-no-xrandr",
            ]
        )
        self.assertTrue(args.carla_force_sdl_x11_no_xrandr)

    def test_build_corpus_parser_supports_force_headless_env(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "build-corpus",
                "--start-carla",
                "--carla-force-headless-env",
            ]
        )
        self.assertTrue(args.carla_force_headless_env)

    def test_run_parser_supports_carla_display_override(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-display-override",
                ":99",
                "--carla-xauthority-override",
                "/tmp/xauth",
            ]
        )
        self.assertEqual(args.carla_display_override, ":99")
        self.assertEqual(args.carla_xauthority_override, "/tmp/xauth")

    def test_build_corpus_parser_supports_carla_display_override(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "build-corpus",
                "--start-carla",
                "--carla-display-override",
                ":99",
                "--carla-xauthority-override",
                "/tmp/xauth",
            ]
        )
        self.assertEqual(args.carla_display_override, ":99")
        self.assertEqual(args.carla_xauthority_override, "/tmp/xauth")

    def test_compose_prestart_carla_extra_args_defaults_to_ros2(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["run", "--route-id", "town01_rh_spawn068_goal068"])
        self.assertEqual(_compose_prestart_carla_extra_args(args), "--ros2")

    def test_compose_prestart_carla_extra_args_can_disable_auto_ros2(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-disable-native-ros2-arg",
                "--carla-extra-args",
                "-windowed -ResX=960 -ResY=540 -quality-level=Low",
            ]
        )
        self.assertEqual(
            _compose_prestart_carla_extra_args(args),
            "-windowed -ResX=960 -ResY=540 -quality-level=Low",
        )

    def test_build_base_overrides_disable_native_ros2_arg_propagates_to_effective_config(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-disable-native-ros2-arg",
            ]
        )
        overrides = _build_base_overrides(args)
        self.assertIn("runtime.carla.disable_native_ros2_arg=true", overrides)
        self.assertIn("carla.disable_native_ros2_arg=true", overrides)
        self.assertIn('runtime.carla.extra_args=""', overrides)
        self.assertIn('carla.extra_args=""', overrides)

    def test_launch_policy_disable_native_ros2_arg_strips_ros2_even_in_ros2_native_mode(self) -> None:
        policy = resolve_carla_launch_policy(
            {
                "io": {"mode": "ros2_native"},
                "scenario": {"publish_ros2_native": False},
                "runtime": {
                    "carla": {
                        "start": False,
                        "host": "localhost",
                        "port": 2000,
                        "town": "Town01",
                        "disable_native_ros2_arg": True,
                        "extra_args": "",
                    }
                },
                "carla": {"extra_args": "--ros2"},
                "run": {"map": "Town01"},
            }
        )
        self.assertFalse(policy.need_ros2_native)
        self.assertEqual(policy.extra_args, "")

    def test_compose_prestart_carla_env_overrides_defaults_include_malloc_arena_cap(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["run", "--route-id", "town01_rh_spawn068_goal068"])
        self.assertEqual(_compose_prestart_carla_env_overrides(args), {"MALLOC_ARENA_MAX": "2"})

    def test_compose_prestart_carla_env_overrides_can_force_x11_without_xrandr(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-force-sdl-x11-no-xrandr",
            ]
        )
        self.assertEqual(
            _compose_prestart_carla_env_overrides(args),
            {
                "MALLOC_ARENA_MAX": "2",
                "SDL_VIDEODRIVER": "x11",
                "SDL_VIDEO_X11_REQUIRE_XRANDR": "0",
            },
        )

    def test_compose_prestart_carla_env_overrides_can_force_pure_headless(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-force-headless-env",
            ]
        )
        self.assertEqual(
            _compose_prestart_carla_env_overrides(args),
            {
                "MALLOC_ARENA_MAX": "2",
                "DISPLAY": "",
                "WAYLAND_DISPLAY": "",
                "XAUTHORITY": "",
                "SDL_AUDIODRIVER": "dummy",
            },
        )

    def test_compose_prestart_carla_env_overrides_can_set_display_and_xauthority(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-display-override",
                ":99",
                "--carla-xauthority-override",
                "/tmp/xauth",
            ]
        )
        self.assertEqual(
            _compose_prestart_carla_env_overrides(args),
            {
                "MALLOC_ARENA_MAX": "2",
                "DISPLAY": ":99",
                "XAUTHORITY": "/tmp/xauth",
            },
        )

    def test_memory_preflight_snapshot_warns_when_swap_disabled(self) -> None:
        with (
            mock.patch("tools.run_town01_route_health._read_meminfo_mb", return_value={"MemAvailable": 11264.0, "MemTotal": 15360.0}),
            mock.patch("tools.run_town01_route_health._host_swap_enabled", return_value=False),
        ):
            snapshot = _memory_preflight_snapshot()
        self.assertEqual(snapshot["status"], "warn_no_swap")
        self.assertFalse(snapshot["blocked"])
        self.assertFalse(snapshot["host_swap_enabled"])

    def test_memory_preflight_snapshot_blocks_low_available_memory(self) -> None:
        with (
            mock.patch("tools.run_town01_route_health._read_meminfo_mb", return_value={"MemAvailable": 4096.0, "MemTotal": 15360.0}),
            mock.patch("tools.run_town01_route_health._host_swap_enabled", return_value=True),
        ):
            snapshot = _memory_preflight_snapshot()
        self.assertEqual(snapshot["status"], "blocked_available_memory_below_min")
        self.assertTrue(snapshot["blocked"])

    def test_prestart_carla_allows_low_memory_when_reusing_existing_town01_session(self) -> None:
        class FakeLauncher:
            instances: list["FakeLauncher"] = []

            def __init__(self, *args, **kwargs):
                self.kwargs = dict(kwargs)
                self.reused = True
                FakeLauncher.instances.append(self)

            def start(self):
                return None

            def wait_ready(self, *args, **kwargs):
                return True

            def diagnostics_snapshot(self, *args, **kwargs):
                return {
                    "process_alive": True,
                    "target_port_snapshot": [],
                    "latest_server_log_tail": [],
                }

            def stop(self):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            carla_root = Path(tmpdir) / "CARLA"
            carla_root.mkdir(parents=True, exist_ok=True)
            parser = _build_parser()
            args = parser.parse_args(
                [
                    "run",
                    "--route-id",
                    "town01_rh_spawn097_goal046",
                    "--carla-root",
                    str(carla_root),
                    "--carla-launch-attempts",
                    "1",
                ]
            )
            fake_world = mock.Mock()
            fake_world.get_map.return_value.name = "Town01"
            with (
                mock.patch("tbio.carla.launcher.CarlaLauncher", FakeLauncher),
                mock.patch(
                    "tools.run_town01_route_health._memory_preflight_snapshot",
                    return_value={
                        "status": "blocked_available_memory_below_min",
                        "blocked": True,
                        "host_swap_enabled": False,
                        "available_memory_mb_before_start": 4235.855,
                        "total_memory_mb": 15677.488,
                        "min_available_memory_mb_required": 8192.0,
                    },
                ),
                mock.patch(
                    "tools.run_town01_route_health._probe_existing_carla_world",
                    return_value={"ready": True, "town": "Town01", "error": ""},
                ),
                mock.patch("tools.run_town01_route_health._connect_world", return_value=(object(), fake_world)),
            ):
                launcher = _prestart_carla(args, Path(tmpdir) / "batch")
                probe = json.loads(
                    (Path(tmpdir) / "batch" / "carla_boot" / "carla_startup_probe.json").read_text(encoding="utf-8")
                )
        self.assertIs(launcher, FakeLauncher.instances[0])
        self.assertEqual(probe["memory_preflight"]["status"], "warn_low_available_memory_reuse_existing_session")
        self.assertFalse(probe["memory_preflight"]["blocked"])
        self.assertTrue(probe["memory_preflight"]["reuse_existing_session_ready"])

    def test_prestart_carla_can_ignore_memory_preflight_for_technical_probe(self) -> None:
        class FakeLauncher:
            instances: list["FakeLauncher"] = []

            def __init__(self, *args, **kwargs):
                self.kwargs = dict(kwargs)
                self.reused = False
                FakeLauncher.instances.append(self)

            def start(self):
                return None

            def wait_ready(self, *args, **kwargs):
                return True

            def diagnostics_snapshot(self, *args, **kwargs):
                return {
                    "process_alive": True,
                    "target_port_snapshot": [],
                    "latest_server_log_tail": [],
                }

            def stop(self):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            carla_root = Path(tmpdir) / "CARLA"
            carla_root.mkdir(parents=True, exist_ok=True)
            parser = _build_parser()
            args = parser.parse_args(
                [
                    "run",
                    "--route-id",
                    "town01_rh_spawn097_goal046",
                    "--carla-root",
                    str(carla_root),
                    "--carla-launch-attempts",
                    "1",
                    "--carla-ignore-memory-preflight",
                    "--carla-force-fresh-start",
                ]
            )
            fake_world = mock.Mock()
            fake_world.get_map.return_value.name = "Town01"
            with (
                mock.patch("tbio.carla.launcher.CarlaLauncher", FakeLauncher),
                mock.patch(
                    "tools.run_town01_route_health._memory_preflight_snapshot",
                    return_value={
                        "status": "blocked_available_memory_below_min",
                        "blocked": True,
                        "host_swap_enabled": False,
                        "available_memory_mb_before_start": 4096.0,
                        "total_memory_mb": 15677.488,
                        "min_available_memory_mb_required": 8192.0,
                    },
                ),
                mock.patch("tools.run_town01_route_health._connect_world", return_value=(object(), fake_world)),
            ):
                launcher = _prestart_carla(args, Path(tmpdir) / "batch")
                probe = json.loads(
                    (Path(tmpdir) / "batch" / "carla_boot" / "carla_startup_probe.json").read_text(encoding="utf-8")
                )
        self.assertIs(launcher, FakeLauncher.instances[0])
        self.assertEqual(probe["memory_preflight"]["status"], "warn_memory_preflight_ignored")
        self.assertFalse(probe["memory_preflight"]["blocked"])
        self.assertTrue(probe["memory_preflight"]["ignored"])
        self.assertEqual(probe["memory_preflight"]["ignored_for"], "technical_probe")
        self.assertEqual(probe["memory_preflight"]["original_status"], "blocked_available_memory_below_min")

    def test_extract_carla_bootstrap_summary_reads_probe_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            probe_path = run_dir / "carla_boot" / "carla_startup_probe.json"
            probe_path.parent.mkdir(parents=True, exist_ok=True)
            probe_path.write_text(
                json.dumps(
                    {
                        "memory_preflight": {
                            "status": "warn_no_swap",
                            "blocked": False,
                            "host_swap_enabled": False,
                            "available_memory_mb_before_start": 11264.0,
                        },
                        "attempts": [
                            {
                                "status": "world_ready",
                                "world_ready": True,
                                "launcher_diagnostics": {
                                    "bootstrap_stability_elapsed_s": 61.2,
                                    "process_survived_stability_window": True,
                                    "carla_process_rss_mb": 2450.5,
                                    "latest_server_log_tail": ["ready"],
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            summary = _extract_carla_bootstrap_summary(run_dir)
        self.assertEqual(summary["carla_bootstrap_status"], "bootstrap_alive_then_world_ready")
        self.assertEqual(summary["memory_preflight_status"], "warn_no_swap")
        self.assertFalse(summary["host_swap_enabled"])
        self.assertAlmostEqual(summary["available_memory_mb_before_start"], 11264.0)

    def test_startup_probe_failure_family_identifies_rpc_ready_sig11_bridge(self) -> None:
        attempt = {
            "status": "world_not_ready",
            "rpc_ready": True,
            "launcher_diagnostics": {
                "process_alive": False,
                "latest_server_log_tail": [
                    "ERROR: session 0 : error retrieving stream id : End of file ",
                    "ERROR: Primary server: failed to read header: End of file ",
                    "Signal 11 caught.",
                ],
                "target_port_snapshot": [
                    {"port": 2000, "open": False},
                    {"port": 2001, "open": False},
                    {"port": 2002, "open": False},
                ],
            },
        }
        self.assertEqual(
            _startup_probe_failure_family(attempt),
            "rpc_ready_world_not_ready_eof_sig11_exit",
        )

    def test_startup_probe_failure_family_identifies_rpc_ready_eof_exit_without_sig11(self) -> None:
        attempt = {
            "status": "world_not_ready",
            "rpc_ready": True,
            "launcher_diagnostics": {
                "process_alive": False,
                "latest_server_log_tail": [
                    "ERROR: session 0 : error retrieving stream id : End of file ",
                    "ERROR: Primary server: failed to read header: End of file ",
                ],
                "target_port_snapshot": [
                    {"port": 2000, "open": False},
                    {"port": 2001, "open": False},
                    {"port": 2002, "open": False},
                ],
            },
        }
        self.assertEqual(
            _startup_probe_failure_family(attempt),
            "rpc_ready_world_not_ready_eof_exit",
        )

    def test_startup_probe_failure_family_identifies_rpc_ready_followup_missing(self) -> None:
        attempt = {
            "status": "rpc_ready",
            "rpc_ready": True,
            "world_ready": False,
            "launcher_diagnostics": {
                "process_alive": None,
                "latest_server_log_tail": [],
                "target_port_snapshot": [
                    {"port": 2000, "open": True},
                    {"port": 2001, "open": True},
                    {"port": 2002, "open": True},
                ],
            },
        }
        self.assertEqual(_startup_probe_failure_family(attempt), "rpc_ready_followup_missing")

    def test_startup_probe_failure_family_identifies_ports_open_eof_alive(self) -> None:
        attempt = {
            "status": "rpc_not_ready",
            "rpc_ready": False,
            "launcher_diagnostics": {
                "process_alive": True,
                "latest_server_log_tail": [
                    "ERROR: session 0 : error retrieving stream id : End of file ",
                    "ERROR: Primary server: failed to read header: End of file ",
                ],
                "target_port_snapshot": [
                    {"port": 2000, "open": True},
                    {"port": 2001, "open": True},
                    {"port": 2002, "open": True},
                ],
            },
        }
        self.assertEqual(
            _startup_probe_failure_family(attempt),
            "rpc_not_ready_ports_open_eof_alive",
        )

    def test_startup_probe_failure_family_identifies_early_exit(self) -> None:
        attempt = {
            "status": "rpc_not_ready",
            "rpc_ready": False,
            "launcher_diagnostics": {
                "process_alive": False,
                "latest_server_log_tail": [],
                "target_port_snapshot": [
                    {"port": 2000, "open": False},
                    {"port": 2001, "open": False},
                    {"port": 2002, "open": False},
                ],
            },
        }
        self.assertEqual(_startup_probe_failure_family(attempt), "rpc_not_ready_early_exit")

    def test_startup_probe_failure_family_identifies_request_exit_alive_without_listener(self) -> None:
        attempt = {
            "status": "rpc_not_ready",
            "rpc_ready": False,
            "launcher_diagnostics": {
                "process_alive": True,
                "latest_server_log_tail": [
                    "FUnixPlatformMisc::RequestExitWithStatus",
                    "FUnixPlatformMisc::RequestExit",
                ],
                "target_port_snapshot": [
                    {"port": 2000, "open": False},
                    {"port": 2001, "open": False},
                    {"port": 2002, "open": False},
                ],
            },
        }
        self.assertEqual(
            _startup_probe_failure_family(attempt),
            "rpc_not_ready_request_exit_alive",
        )

    def test_display_probe_monitor_count_falls_back_to_xrandr_output_head(self) -> None:
        self.assertEqual(
            _display_probe_monitor_count(
                {
                    "xrandr_output_head": [
                        "xrandr: Failed to get size of gamma for output default",
                        "Monitors: 1",
                        " 0: +default 1280/339x720/190+0+0  default",
                    ]
                }
            ),
            1,
        )

    def test_startup_probe_attempt_rows_normalize_legacy_world_ready_payload(self) -> None:
        rows = _startup_probe_attempt_rows(
            {
                "status": "world_ready",
                "rpc_ready": True,
                "world_ready": True,
                "launcher_diagnostics": {
                    "display_probe": {
                        "display": ":0",
                        "xrandr_output_head": ["Monitors: 0"],
                    }
                },
            }
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attempt"], 1)
        self.assertEqual(rows[0]["failure_family"], "world_ready")
        self.assertEqual(
            rows[0]["launcher_diagnostics"]["display_probe"]["monitor_count"],
            0,
        )

    def test_startup_probe_attempt_rows_fill_missing_failure_family_for_attempts(self) -> None:
        rows = _startup_probe_attempt_rows(
            {
                "attempts": [
                    {
                        "attempt": 2,
                        "status": "world_not_ready",
                        "rpc_ready": True,
                        "world_ready": False,
                        "launcher_diagnostics": {
                            "process_alive": False,
                            "latest_server_log_tail": [
                                "ERROR: session 0 : error retrieving stream id : End of file ",
                                "ERROR: Primary server: failed to read header: End of file ",
                            ],
                            "display_probe": {
                                "xrandr_output_head": ["Monitors: 1"],
                            },
                        },
                    }
                ]
            }
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attempt"], 2)
        self.assertEqual(rows[0]["failure_family"], "rpc_ready_world_not_ready_eof_exit")
        self.assertEqual(
            rows[0]["launcher_diagnostics"]["display_probe"]["monitor_count"],
            1,
        )

    def test_startup_probe_payload_promotes_final_attempt_fields(self) -> None:
        payload = _startup_probe_payload(
            attempt_rows=[
                {
                    "attempt": 1,
                    "status": "world_not_ready",
                    "rpc_ready": True,
                    "world_ready": False,
                    "failure_family": "rpc_ready_followup_missing_eof_alive",
                },
                {
                    "attempt": 2,
                    "status": "world_ready",
                    "rpc_ready": True,
                    "world_ready": True,
                    "failure_family": "world_ready",
                },
            ],
            memory_preflight={"status": "ok"},
            bootstrap_policy={"startup_profile": "render_offscreen_no_ros2"},
            retry_policy={"max_attempts": 2},
        )
        self.assertEqual(payload["final_attempt"], 2)
        self.assertEqual(payload["status"], "world_ready")
        self.assertTrue(payload["rpc_ready"])
        self.assertTrue(payload["world_ready"])
        self.assertEqual(payload["failure_family"], "world_ready")
        self.assertEqual(payload["attempts"][0]["failure_family"], "rpc_ready_followup_missing_eof_alive")

    def test_startup_probe_payload_normalizes_legacy_single_attempt_shape(self) -> None:
        payload = _startup_probe_payload(
            attempt_rows=[
                {
                    "status": "world_ready",
                    "rpc_ready": True,
                    "world_ready": True,
                }
            ],
            memory_preflight={},
            bootstrap_policy={},
            retry_policy={},
        )
        self.assertEqual(payload["final_attempt"], 1)
        self.assertEqual(payload["status"], "world_ready")
        self.assertTrue(payload["rpc_ready"])
        self.assertTrue(payload["world_ready"])
        self.assertEqual(payload["failure_family"], "world_ready")

    def test_startup_retry_policy_payload_tracks_nonretry_families(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-launch-attempts",
                "3",
                "--carla-retry-delay-sec",
                "7",
                "--carla-no-retry-failure-families",
                "rpc_ready_followup_missing_eof_alive",
            ]
        )
        self.assertEqual(
            _startup_retry_policy_payload(args),
            {
                "max_attempts": 3,
                "retry_delay_sec": 7.0,
                "no_retry_failure_families": ["rpc_ready_followup_missing_eof_alive"],
            },
        )

    def test_startup_retry_decision_blocks_configured_nonretry_family(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-launch-attempts",
                "4",
                "--carla-no-retry-failure-families",
                "rpc_ready_followup_missing_eof_alive",
            ]
        )
        retry_allowed, reason = _startup_retry_decision(
            {
                "status": "world_not_ready",
                "rpc_ready": True,
                "world_ready": False,
                "failure_family": "rpc_ready_followup_missing_eof_alive",
            },
            attempt=1,
            max_attempts=4,
            args=args,
        )
        self.assertFalse(retry_allowed)
        self.assertEqual(reason, "nonretry_family:rpc_ready_followup_missing_eof_alive")

    def test_startup_retry_decision_allows_retryable_world_not_ready_family(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--carla-launch-attempts",
                "4",
                "--carla-no-retry-failure-families",
                "rpc_ready_followup_missing_eof_alive",
            ]
        )
        retry_allowed, reason = _startup_retry_decision(
            {
                "status": "world_not_ready",
                "rpc_ready": True,
                "world_ready": False,
                "failure_family": "rpc_ready_world_not_ready_eof_alive",
            },
            attempt=1,
            max_attempts=4,
            args=args,
        )
        self.assertTrue(retry_allowed)
        self.assertEqual(reason, "retry_allowed")

    def test_prestart_carla_stops_launcher_before_final_ready_failure(self) -> None:
        class FakeLauncher:
            instances: list["FakeLauncher"] = []

            def __init__(self, *args, **kwargs):
                self.stop_calls = 0
                self.reused = False
                FakeLauncher.instances.append(self)

            def start(self):
                return None

            def wait_ready(self, *args, **kwargs):
                return False

            def diagnostics_snapshot(self, *args, **kwargs):
                return {
                    "process_alive": True,
                    "target_port_snapshot": [],
                    "latest_server_log_tail": [],
                }

            def stop(self):
                self.stop_calls += 1

        with tempfile.TemporaryDirectory() as tmpdir:
            carla_root = Path(tmpdir) / "CARLA"
            carla_root.mkdir(parents=True, exist_ok=True)
            parser = _build_parser()
            args = parser.parse_args(
                [
                    "run",
                    "--route-id",
                    "town01_rh_spawn097_goal046",
                    "--carla-root",
                    str(carla_root),
                    "--carla-launch-attempts",
                    "1",
                ]
            )
            with (
                mock.patch("tbio.carla.launcher.CarlaLauncher", FakeLauncher),
                mock.patch(
                    "tools.run_town01_route_health._memory_preflight_snapshot",
                    return_value={
                        "status": "ok",
                        "blocked": False,
                        "host_swap_enabled": True,
                        "available_memory_mb_before_start": 12000.0,
                        "total_memory_mb": 16000.0,
                        "min_available_memory_mb_required": 8192.0,
                    },
                ),
            ):
                with self.assertRaises(SystemExit):
                    _prestart_carla(args, Path(tmpdir) / "batch")
        self.assertEqual(len(FakeLauncher.instances), 1)
        self.assertEqual(FakeLauncher.instances[0].stop_calls, 1)

    def test_prestart_carla_defaults_launcher_auto_recovery_off(self) -> None:
        class FakeLauncher:
            instances: list["FakeLauncher"] = []

            def __init__(self, *args, **kwargs):
                self.kwargs = dict(kwargs)
                self.reused = True
                FakeLauncher.instances.append(self)

            def start(self):
                return None

            def wait_ready(self, *args, **kwargs):
                return True

            def diagnostics_snapshot(self, *args, **kwargs):
                return {
                    "process_alive": True,
                    "target_port_snapshot": [],
                    "latest_server_log_tail": [],
                }

            def stop(self):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            carla_root = Path(tmpdir) / "CARLA"
            carla_root.mkdir(parents=True, exist_ok=True)
            parser = _build_parser()
            args = parser.parse_args(
                [
                    "run",
                    "--route-id",
                    "town01_rh_spawn097_goal046",
                    "--carla-root",
                    str(carla_root),
                    "--carla-launch-attempts",
                    "1",
                ]
            )
            fake_world = mock.Mock()
            fake_world.get_map.return_value.name = "Town01"
            with (
                mock.patch("tbio.carla.launcher.CarlaLauncher", FakeLauncher),
                mock.patch(
                    "tools.run_town01_route_health._memory_preflight_snapshot",
                    return_value={
                        "status": "ok",
                        "blocked": False,
                        "host_swap_enabled": True,
                        "available_memory_mb_before_start": 12000.0,
                        "total_memory_mb": 16000.0,
                        "min_available_memory_mb_required": 8192.0,
                    },
                ),
                mock.patch("tools.run_town01_route_health._connect_world", return_value=(object(), fake_world)),
            ):
                launcher = _prestart_carla(args, Path(tmpdir) / "batch")
        self.assertIs(launcher, FakeLauncher.instances[0])
        self.assertFalse(FakeLauncher.instances[0].kwargs["enable_auto_recovery"])

    def test_prestart_carla_retryable_world_not_ready_cleans_processes_before_retry(self) -> None:
        class FakeLauncher:
            instances: list["FakeLauncher"] = []

            def __init__(self, *args, **kwargs):
                self.stop_calls = 0
                self.reused = False
                FakeLauncher.instances.append(self)

            def start(self):
                return None

            def wait_ready(self, *args, **kwargs):
                return True

            def diagnostics_snapshot(self, *args, **kwargs):
                return {
                    "process_alive": True,
                    "target_port_snapshot": [],
                    "latest_server_log_tail": [],
                }

            def stop(self):
                self.stop_calls += 1

        with tempfile.TemporaryDirectory() as tmpdir:
            carla_root = Path(tmpdir) / "CARLA"
            carla_root.mkdir(parents=True, exist_ok=True)
            parser = _build_parser()
            args = parser.parse_args(
                [
                    "run",
                    "--route-id",
                    "town01_rh_spawn097_goal046",
                    "--carla-root",
                    str(carla_root),
                    "--carla-launch-attempts",
                    "2",
                ]
            )
            fake_world = mock.Mock()
            fake_world.get_map.return_value.name = "Town01"
            with (
                mock.patch("tbio.carla.launcher.CarlaLauncher", FakeLauncher),
                mock.patch(
                    "tools.run_town01_route_health._memory_preflight_snapshot",
                    return_value={
                        "status": "ok",
                        "blocked": False,
                        "host_swap_enabled": True,
                        "available_memory_mb_before_start": 12000.0,
                        "total_memory_mb": 16000.0,
                        "min_available_memory_mb_required": 8192.0,
                    },
                ),
                mock.patch(
                    "tools.run_town01_route_health._connect_world",
                    side_effect=[RuntimeError("world not ready"), (object(), fake_world)],
                ),
                mock.patch("tools.run_town01_route_health._cleanup_carla_processes") as cleanup_mock,
                mock.patch("tools.run_town01_route_health.time.sleep", return_value=None),
            ):
                launcher = _prestart_carla(args, Path(tmpdir) / "batch")
        self.assertIs(launcher, FakeLauncher.instances[-1])
        self.assertEqual(len(FakeLauncher.instances), 2)
        self.assertEqual(FakeLauncher.instances[0].stop_calls, 1)
        cleanup_mock.assert_called_once()

    def test_valid_capture_entry_payload_requires_summary_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "capture_case"
            run_dir.mkdir(parents=True, exist_ok=True)
            payload = _valid_capture_entry_payload(str(run_dir))
            self.assertTrue(payload["exists"])
            self.assertFalse(payload["summary_exists"])
            self.assertFalse(payload["artifacts_dir_exists"])
            self.assertFalse(payload["ready"])
            (run_dir / "summary.json").write_text("{}", encoding="utf-8")
            (run_dir / "artifacts").mkdir()
            payload = _valid_capture_entry_payload(str(run_dir))
            self.assertTrue(payload["ready"])

    def test_fallback_capture_entry_payload_ready_tracks_file_existence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Path(tmpdir) / "capture.yaml"
            cfg.write_text("mode: demo\n", encoding="utf-8")
            payload = _fallback_capture_entry_payload(str(cfg))
            self.assertTrue(payload["exists"])
            self.assertTrue(payload["ready"])

    def test_replay_input_contract_payload_uses_ready_valid_capture_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ready_run_dir = Path(tmpdir) / "ready_capture"
            ready_run_dir.mkdir(parents=True, exist_ok=True)
            (ready_run_dir / "summary.json").write_text("{}", encoding="utf-8")
            (ready_run_dir / "artifacts").mkdir()
            payload = _replay_input_contract_payload(
                capture_summary={
                    "captures": [
                        {
                            "capture_id": "ready_case",
                            "capture_valid": True,
                            "run_dir": str(ready_run_dir),
                        }
                    ]
                },
                valid_capture_run_dirs=[str(ready_run_dir)],
                replay_cfg={"capture_configs": []},
            )
            self.assertEqual(payload["replay_source_mode"], "suite_valid_capture_run_dirs")
            self.assertTrue(payload["selected_source_ready"])
            self.assertTrue(payload["valid_capture_entries"][0]["ready"])

    def test_replay_input_contract_payload_marks_missing_fallback_as_not_ready(self) -> None:
        payload = _replay_input_contract_payload(
            capture_summary={"captures": []},
            valid_capture_run_dirs=[],
            replay_cfg={"capture_configs": ["/tmp/definitely_missing_capture_config.yaml"]},
        )
        self.assertEqual(payload["replay_source_mode"], "replay_config_capture_configs")
        self.assertFalse(payload["selected_source_ready"])
        self.assertFalse(payload["fallback_capture_entries"][0]["ready"])

    def test_replay_input_contract_markdown_surfaces_ready_fields(self) -> None:
        markdown = _replay_input_contract_markdown(
            {
                "replay_source_mode": "suite_valid_capture_run_dirs",
                "selected_source_ready": True,
                "valid_capture_count": 1,
                "invalid_capture_count": 1,
                "valid_capture_entries": [
                    {
                        "run_dir": "/tmp/ready",
                        "exists": True,
                        "summary_exists": True,
                        "artifacts_dir_exists": True,
                        "ready": True,
                    }
                ],
                "invalid_capture_entries": [
                    {
                        "capture_id": "bad_case",
                        "run_dir": "/tmp/bad",
                        "run_dir_exists": True,
                        "summary_exists": False,
                    }
                ],
                "fallback_capture_entries": [
                    {
                        "capture_config": "cfg.yaml",
                        "resolved_path": "/tmp/cfg.yaml",
                        "exists": True,
                        "ready": True,
                    }
                ],
            }
        )
        self.assertIn("selected_source_ready: `True`", markdown)
        self.assertIn("ready=`True`", markdown)

    def test_emit_replay_input_contract_artifacts_writes_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "artifacts"
            target.mkdir(parents=True, exist_ok=True)
            ready_run_dir = Path(tmpdir) / "ready_capture"
            ready_run_dir.mkdir(parents=True, exist_ok=True)
            (ready_run_dir / "summary.json").write_text("{}", encoding="utf-8")
            (ready_run_dir / "artifacts").mkdir()
            payload = _emit_replay_input_contract_artifacts(
                artifact_targets=[target],
                suite_manifest={
                    "capture_summary": {
                        "captures": [
                            {
                                "capture_id": "ready_case",
                                "capture_valid": True,
                                "run_dir": str(ready_run_dir),
                            }
                        ]
                    }
                },
                replay_cfg={"capture_configs": []},
            )
            self.assertTrue((target / "replay_input_contract.json").exists())
            self.assertTrue((target / "replay_input_contract.md").exists())
            self.assertTrue(payload["selected_source_ready"])
            self.assertEqual(payload["valid_capture_count"], 1)

    def test_emit_replay_input_contract_artifacts_supports_capture_only_fallback_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "artifacts"
            target.mkdir(parents=True, exist_ok=True)
            fallback_cfg = Path(tmpdir) / "capture.yaml"
            fallback_cfg.write_text("mode: semantic\n", encoding="utf-8")
            payload = _emit_replay_input_contract_artifacts(
                artifact_targets=[target],
                suite_manifest={
                    "capture_summary": {
                        "captures": [
                            {
                                "capture_id": "bad_case",
                                "capture_valid": False,
                                "run_dir": str(Path(tmpdir) / "bad_capture"),
                                "summary_path": str(Path(tmpdir) / "bad_capture" / "summary.json"),
                            }
                        ]
                    }
                },
                replay_cfg={"capture_configs": [str(fallback_cfg)]},
            )
            self.assertEqual(payload["replay_source_mode"], "replay_config_capture_configs")
            self.assertTrue(payload["selected_source_ready"])
            self.assertEqual(payload["invalid_capture_count"], 1)

    def test_startup_probe_flatten_row_surfaces_retry_policy_fields(self) -> None:
        row = startup_probe_corpus._flatten_attempt_row(
            Path("/tmp/fake_run/carla_boot/carla_startup_probe.json"),
            {
                "attempt": 1,
                "status": "rpc_ready",
                "rpc_ready": True,
                "world_ready": False,
                "failure_family": "rpc_ready_followup_missing_eof_alive",
                "retry_eligible": False,
                "retry_decision_reason": "nonretry_family:rpc_ready_followup_missing_eof_alive",
                "_retry_policy": {
                    "max_attempts": 4,
                    "retry_delay_sec": 5.0,
                    "no_retry_failure_families": ["rpc_ready_followup_missing_eof_alive"],
                },
                "launcher_diagnostics": {
                    "display_probe": {"display": ":0", "monitor_count": 1},
                    "launch_records": [
                        {
                            "command": ["bash", "/tmp/CarlaUE4.sh", "-carla-map=Town01", "--ros2"],
                            "display": ":0",
                        }
                    ],
                },
            },
        )
        self.assertTrue(row["probe_retry_policy_present"])
        self.assertEqual(row["retry_max_attempts"], 4)
        self.assertEqual(row["retry_delay_sec"], 5.0)
        self.assertEqual(row["retry_no_retry_families"], "rpc_ready_followup_missing_eof_alive")
        self.assertFalse(row["retry_eligible"])
        self.assertEqual(
            row["retry_decision_reason"],
            "nonretry_family:rpc_ready_followup_missing_eof_alive",
        )

    def test_startup_probe_report_surfaces_retry_policy_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "startup_report.md"
            rows = [
                startup_probe_corpus._flatten_attempt_row(
                    Path(tmpdir) / "policy_run" / "carla_boot" / "carla_startup_probe.json",
                    {
                        "attempt": 1,
                        "status": "rpc_ready",
                        "rpc_ready": True,
                        "world_ready": False,
                        "failure_family": "rpc_ready_followup_missing_eof_alive",
                        "retry_eligible": False,
                        "retry_decision_reason": "nonretry_family:rpc_ready_followup_missing_eof_alive",
                        "_retry_policy": {
                            "max_attempts": 4,
                            "retry_delay_sec": 5.0,
                            "no_retry_failure_families": ["rpc_ready_followup_missing_eof_alive"],
                        },
                        "launcher_diagnostics": {
                            "display_probe": {"display": ":0", "monitor_count": 1},
                            "launch_records": [
                                {
                                    "command": ["bash", "/tmp/CarlaUE4.sh", "-carla-map=Town01", "--ros2"],
                                    "display": ":0",
                                }
                            ],
                        },
                    },
                ),
                startup_probe_corpus._flatten_attempt_row(
                    Path(tmpdir) / "legacy_run" / "carla_boot" / "carla_startup_probe.json",
                    {
                        "attempt": 1,
                        "status": "world_ready",
                        "rpc_ready": True,
                        "world_ready": True,
                        "failure_family": "world_ready",
                        "launcher_diagnostics": {
                            "display_probe": {"display": ":0", "monitor_count": 1},
                            "launch_records": [
                                {
                                    "command": ["bash", "/tmp/CarlaUE4.sh", "-carla-map=Town01", "--ros2"],
                                    "display": ":0",
                                }
                            ],
                        },
                    },
                ),
            ]
            startup_probe_corpus._write_report(report_path, rows, "Startup Retry Coverage Test")
            text = report_path.read_text(encoding="utf-8")
        self.assertIn("## Retry Policy Coverage", text)
        self.assertIn("recorded `retry_policy` probe count: `1`", text)
        self.assertIn("## Latest Recorded Retry Decisions", text)
        self.assertIn("nonretry_family:rpc_ready_followup_missing_eof_alive", text)

    def test_town01_junction_startup_lineage_main_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            anchor_summary = root / "anchor_batch" / "junction_traverse__focus_left_turn" / "anchor_run" / "summary.json"
            _write_json(anchor_summary, {"route_id": "town01_rh_spawn181_goal066"})
            _write_json(
                anchor_summary.parent.parent / "carla_boot" / "carla_startup_probe.json",
                {
                    "attempts": [
                        {
                            "attempt": 1,
                            "status": "world_not_ready",
                            "rpc_ready": True,
                            "world_ready": False,
                            "failure_family": "rpc_ready_world_not_ready_eof_alive",
                            "launcher_diagnostics": {
                                "process_alive": True,
                                "rpc_handshake_ready": True,
                                "target_port_snapshot": [
                                    {"port": 2000, "open": True},
                                    {"port": 2001, "open": True},
                                    {"port": 2002, "open": True},
                                ],
                            },
                        },
                        {
                            "attempt": 2,
                            "status": "world_ready",
                            "rpc_ready": True,
                            "world_ready": True,
                            "failure_family": "world_ready",
                            "launcher_diagnostics": {
                                "process_alive": True,
                                "rpc_handshake_ready": True,
                                "target_port_snapshot": [
                                    {"port": 2000, "open": True},
                                    {"port": 2001, "open": True},
                                    {"port": 2002, "open": True},
                                ],
                            },
                        },
                    ]
                },
            )
            fresh_probe = root / "fresh_run" / "carla_boot" / "carla_startup_probe.json"
            _write_json(
                fresh_probe,
                {
                    "attempts": [
                        {
                            "attempt": 1,
                            "status": "rpc_ready",
                            "rpc_ready": True,
                            "world_ready": False,
                            "failure_family": "rpc_ready_followup_missing_eof_alive",
                            "launcher_diagnostics": {
                                "process_alive": True,
                                "rpc_handshake_ready": True,
                                "target_port_snapshot": [
                                    {"port": 2000, "open": True},
                                    {"port": 2001, "open": True},
                                    {"port": 2002, "open": True},
                                ],
                            },
                        }
                    ]
                },
            )
            output_md = root / "contrast.md"
            output_json = root / "contrast.json"
            argv = [
                "analyze_town01_junction_startup_lineage.py",
                "--anchor-summary",
                str(anchor_summary),
                "--fresh-probe",
                str(fresh_probe),
                "--output-md",
                str(output_md),
                "--output-json",
                str(output_json),
            ]
            with mock.patch.object(sys, "argv", argv):
                rc = town01_startup_lineage.main()
            self.assertEqual(rc, 0)
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["anchor"]["route_id"], "town01_rh_spawn181_goal066")
            self.assertEqual(payload["anchor"]["startup"]["lineage_class"], "world_ready_after_retry")
            self.assertEqual(
                payload["anchor"]["startup"]["pre_world_ready_failure_families"],
                ["rpc_ready_world_not_ready_eof_alive"],
            )
            self.assertEqual(
                payload["fresh_probe"]["startup"]["final_failure_family"],
                "rpc_ready_followup_missing_eof_alive",
            )
            self.assertIn("world_ready_after_retry", output_md.read_text(encoding="utf-8"))
            self.assertIn("rpc_ready_followup_missing_eof_alive", output_md.read_text(encoding="utf-8"))

    def test_render_calibration_baseline_reference_summary_main_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "baseline_reference.json"
            _write_json(
                input_path,
                {
                    "source": "pipeline_library_entry",
                    "exists": True,
                    "same_as_candidate": False,
                    "resolution_mode": "library_index",
                    "requested_calibration_file": "",
                    "requested_library_entry_id": "entry_a",
                    "library_index_entry_found": True,
                    "library_entry_map": "Town01",
                },
            )
            output_json = root / "baseline_reference_summary.json"
            output_md = root / "baseline_reference_summary.md"
            argv = [
                "render_calibration_baseline_reference_summary.py",
                "--input",
                str(input_path),
                "--output-json",
                str(output_json),
                "--output-md",
                str(output_md),
            ]
            with mock.patch.object(sys, "argv", argv):
                rc = calibration_baseline_summary_renderer.main()
            self.assertEqual(rc, 0)
            summary = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(summary["requested_library_entry_id"], "entry_a")
            self.assertEqual(summary["resolution_mode"], "library_index")
            self.assertTrue(summary["library_index_entry_found"])
            self.assertIn("requested_library_entry_id: `entry_a`", output_md.read_text(encoding="utf-8"))

    def test_inspect_town01_live_route_progress_tolerates_missing_debug_timeseries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "junction_traverse__town01_rh_spawn219_goal062__02"
            artifacts = run_dir / "artifacts"
            artifacts.mkdir(parents=True)
            sibling_run_dir = root / "junction_traverse__town01_rh_spawn219_goal062"
            sibling_artifacts = sibling_run_dir / "artifacts"
            sibling_artifacts.mkdir(parents=True)
            _write_json(
                run_dir / "summary.json",
                {
                    "summary_status": "finalized",
                    "comparison_label": "junction_traverse_liveattempt",
                    "route_id": "town01_rh_spawn219_goal062",
                    "route_health": {
                        "available": False,
                        "route_established": False,
                        "route_completion_percentage": None,
                        "route_distance_achieved_m": None,
                    },
                    "acceptance": {
                        "success": False,
                        "fail_reason": "ROUTING_REQUEST_COUNT",
                        "failure_stage": "MAP_READY -> ROUTING_READY",
                        "checks": {
                            "routing_request_count": {"actual": 0},
                            "planning_nonzero_ratio": {"actual": 0.0},
                            "control_used_planning_ratio": {"actual": 0.0},
                            "route_establishment_latency_sec": {"actual": None},
                        },
                    },
                    "dreamview_recording": {
                        "recording_status": "failed",
                    },
                },
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "route_id": "town01_rh_spawn219_goal062",
                    "comparison_label": "junction_traverse_liveattempt",
                },
            )
            _write_json(
                artifacts / "startup_stage.json",
                {
                    "stage": "scenario_build_start",
                    "ts_sec": 123.4,
                },
            )
            _write_json(
                artifacts / "carla_world_ready_summary.json",
                {
                    "status": "world_ready",
                    "final_town": "Town01",
                },
            )
            _write_jsonl(
                artifacts / "startup_stage_timeline.jsonl",
                [
                    {"stage": "carla_get_world_ok", "current_town": "Town01"},
                    {"stage": "adapter_start_exception", "error": "Apollo modules start timed out"},
                    {"stage": "scenario_build_start"},
                ],
            )
            _write_jsonl(
                artifacts / "apollo_backend_startup_trace.jsonl",
                [
                    {"step": "docker_start_modules_begin", "timeout_sec": 90.0},
                    {"step": "docker_start_modules_timeout", "timeout_sec": 90.0},
                ],
            )
            (sibling_artifacts / "followstop_child.stderr.log").write_text(
                "\n".join(
                    [
                        "  File \"/home/ubuntu/carla_testbed/examples/run_followstop.py\", line 2179, in main",
                        "  File \"/home/ubuntu/carla_testbed/carla_testbed/scenarios/town01_route_health.py\", line 689, in build",
                        "    ego.set_simulate_physics(True)",
                        "RuntimeError: time-out of 30000ms while waiting for the simulator",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            report_path = root / "probe.md"
            json_output_path = root / "probe.json"
            argv = [
                "inspect_town01_live_route_progress.py",
                "--run-dir",
                str(run_dir),
                "--report",
                str(report_path),
                "--json-output",
                str(json_output_path),
            ]
            stdout = io.StringIO()
            with mock.patch.object(sys, "argv", argv), mock.patch("sys.stdout", stdout):
                rc = town01_live_progress_probe.main()
            self.assertEqual(rc, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["route_id"], "town01_rh_spawn219_goal062")
            self.assertFalse(payload["startup_snapshot"]["debug_timeseries_available"])
            self.assertEqual(payload["startup_snapshot"]["summary_fail_reason"], "ROUTING_REQUEST_COUNT")
            self.assertEqual(payload["startup_snapshot"]["carla_world_ready_status"], "world_ready")
            self.assertEqual(
                payload["blocker_analysis"]["blocker_summary"],
                "world_ready_reached -> apollo_modules_start_timeout -> scenario_build_simulator_timeout -> routing_request_count_zero -> planning_nonzero_ratio_zero -> debug_timeseries_missing",
            )
            self.assertEqual(
                payload["blocker_analysis"]["root_blocker_family"],
                "post_world_ready_apollo_modules_start_timeout",
            )
            self.assertTrue(payload["blocker_analysis"]["scenario_build_simulator_timeout"])
            self.assertIn(
                "simulator_rpc_timeout_30000ms",
                payload["blocker_analysis"]["followstop_stderr_signatures"],
            )
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("debug_timeseries_available: `False`", report_text)
            self.assertIn("summary_fail_reason: `ROUTING_REQUEST_COUNT`", report_text)
            self.assertIn("root_blocker_family: `post_world_ready_apollo_modules_start_timeout`", report_text)
            self.assertIn("blocker_summary: `world_ready_reached -> apollo_modules_start_timeout", report_text)
            json_payload = json.loads(json_output_path.read_text(encoding="utf-8"))
            self.assertEqual(
                json_payload["blocker_analysis"]["evidence_paths"]["followstop_stderr"],
                str(sibling_artifacts / "followstop_child.stderr.log"),
            )

    def test_inspect_town01_live_route_progress_detects_mainboard_runtime_timeout_family(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "junction_traverse__town01_rh_spawn219_goal062__02"
            artifacts = run_dir / "artifacts"
            artifacts.mkdir(parents=True)
            sibling_run_dir = root / "junction_traverse__town01_rh_spawn219_goal062"
            sibling_artifacts = sibling_run_dir / "artifacts"
            sibling_artifacts.mkdir(parents=True)
            _write_json(
                run_dir / "summary.json",
                {
                    "summary_status": "finalized",
                    "comparison_label": "junction_traverse_liveattempt",
                    "route_id": "town01_rh_spawn219_goal062",
                    "route_health": {
                        "available": False,
                        "route_established": False,
                        "route_completion_percentage": None,
                        "route_distance_achieved_m": None,
                    },
                    "acceptance": {
                        "success": False,
                        "fail_reason": "ROUTING_REQUEST_COUNT",
                        "failure_stage": "MAP_READY -> ROUTING_READY",
                        "checks": {
                            "routing_request_count": {"actual": 0},
                            "planning_nonzero_ratio": {"actual": 0.0},
                            "control_used_planning_ratio": {"actual": 0.0},
                            "route_establishment_latency_sec": {"actual": None},
                        },
                    },
                    "dreamview_recording": {
                        "recording_status": "failed",
                    },
                },
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "route_id": "town01_rh_spawn219_goal062",
                    "comparison_label": "junction_traverse_liveattempt",
                },
            )
            _write_json(
                artifacts / "startup_stage.json",
                {
                    "stage": "scenario_build_start",
                    "ts_sec": 240.0,
                },
            )
            _write_json(
                artifacts / "carla_world_ready_summary.json",
                {
                    "status": "world_ready",
                    "final_town": "Town01",
                },
            )
            _write_jsonl(
                artifacts / "startup_stage_timeline.jsonl",
                [
                    {"stage": "carla_get_world_ok", "current_town": "Town01", "ts_sec": 100.0},
                    {"stage": "adapter_prepare_done", "ts_sec": 104.5},
                    {"stage": "adapter_start_exception", "error": "Apollo modules start timed out", "ts_sec": 225.0},
                    {"stage": "scenario_build_start", "ts_sec": 240.0},
                ],
            )
            _write_jsonl(
                artifacts / "apollo_backend_startup_trace.jsonl",
                [
                    {"step": "docker_start_modules_begin", "timeout_sec": 90.0, "ts_sec": 110.0},
                    {"step": "docker_start_modules_timeout", "timeout_sec": 90.0, "ts_sec": 219.0},
                ],
            )
            (artifacts / "apollo_mainboard_runtime_check.log").write_text(
                "\n".join(
                    [
                        "timeout_sec: 15.0",
                        "returncode: timeout",
                        "--- stdout ---",
                        "",
                        "--- stderr ---",
                        "",
                        "",
                        "=== fallback ldd probe ===",
                        "fallback_returncode: timeout",
                        "--- stdout ---",
                        "",
                        "--- stderr ---",
                        "",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_start.log").write_text(
                "\n".join(
                    [
                        "timeout_sec: 90.0",
                        "returncode: timeout",
                        "--- stdout ---",
                        "",
                        "--- stderr ---",
                        "",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (sibling_artifacts / "followstop_child.stderr.log").write_text(
                "\n".join(
                    [
                        "  File \"/home/ubuntu/carla_testbed/examples/run_followstop.py\", line 2179, in main",
                        "  File \"/home/ubuntu/carla_testbed/carla_testbed/scenarios/town01_route_health.py\", line 689, in build",
                        "    ego.set_simulate_physics(True)",
                        "RuntimeError: time-out of 30000ms while waiting for the simulator, make sure the simulator is ready and connected to localhost:2000",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            report_path = root / "probe.md"
            json_output_path = root / "probe.json"
            argv = [
                "inspect_town01_live_route_progress.py",
                "--run-dir",
                str(run_dir),
                "--report",
                str(report_path),
                "--json-output",
                str(json_output_path),
            ]
            stdout = io.StringIO()
            with mock.patch.object(sys, "argv", argv), mock.patch("sys.stdout", stdout):
                rc = town01_live_progress_probe.main()
            self.assertEqual(rc, 0)
            payload = json.loads(stdout.getvalue())
            blocker = payload["blocker_analysis"]
            self.assertEqual(
                blocker["blocker_summary"],
                "world_ready_reached -> apollo_mainboard_runtime_check_timeout -> apollo_modules_start_timeout -> scenario_build_simulator_timeout -> routing_request_count_zero -> planning_nonzero_ratio_zero -> debug_timeseries_missing",
            )
            self.assertEqual(
                blocker["root_blocker_family"],
                "post_world_ready_apollo_mainboard_runtime_timeout",
            )
            self.assertEqual(
                blocker["apollo_start_subfamily"],
                "mainboard_runtime_check_timeout_before_module_output",
            )
            self.assertTrue(blocker["apollo_mainboard_runtime_check_timeout"])
            self.assertTrue(blocker["apollo_mainboard_fallback_ldd_timeout"])
            self.assertEqual(blocker["apollo_modules_start_log_returncode"], "timeout")
            self.assertFalse(blocker["apollo_modules_start_stdout_nonempty"])
            self.assertFalse(blocker["apollo_modules_start_stderr_nonempty"])
            self.assertEqual(
                blocker["timing_summary"],
                {
                    "world_ready_to_adapter_prepare_done_sec": 4.5,
                    "adapter_prepare_done_to_apollo_modules_timeout_sec": 114.5,
                    "apollo_modules_timeout_to_scenario_build_start_sec": 21.0,
                },
            )
            self.assertIn(
                "inspect Apollo mainboard/runtime stall",
                blocker["recommended_next_focus"],
            )
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn(
                "root_blocker_family: `post_world_ready_apollo_mainboard_runtime_timeout`",
                report_text,
            )
            self.assertIn(
                "apollo_start_subfamily: `mainboard_runtime_check_timeout_before_module_output`",
                report_text,
            )
            self.assertIn("apollo_mainboard_runtime_check_timeout: `True`", report_text)
            json_payload = json.loads(json_output_path.read_text(encoding="utf-8"))
            self.assertEqual(
                json_payload["blocker_analysis"]["evidence_paths"]["apollo_mainboard_runtime_check_log"],
                str(artifacts / "apollo_mainboard_runtime_check.log"),
            )

    def test_inspect_town01_live_route_progress_detects_behavior_unhealthy_family(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "junction_traverse__town01_rh_spawn219_goal062__02"
            artifacts = run_dir / "artifacts"
            artifacts.mkdir(parents=True)
            _write_json(
                run_dir / "summary.json",
                {
                    "summary_status": "finalized",
                    "comparison_label": "junction_traverse_liveattempt",
                    "route_id": "town01_rh_spawn219_goal062",
                    "route_health": {
                        "available": True,
                        "route_established": True,
                        "route_distance_achieved_m": 32.5106,
                        "route_completion_ratio": 0.1303,
                        "route_completion_percentage": 0.1303,
                        "route_length_target_m": 249.4261,
                        "final_goal_distance_m": 216.9698,
                        "label": "route_established_but_behavior_unhealthy",
                    },
                    "acceptance": {
                        "success": False,
                        "fail_reason": "ROUTE_DISTANCE_ACHIEVED_M",
                        "failure_stage": "CRUISE_ACTIVE",
                        "failure_codes": [
                            "ROUTE_DISTANCE_ACHIEVED_M",
                            "ROUTE_COMPLETION_RATIO",
                            "MAX_SPEED_MPS",
                        ],
                        "checks": {
                            "routing_request_count": {"actual": 2, "threshold": 1, "ok": True},
                            "route_establishment_latency_sec": {"actual": 28.4, "threshold": 45.0, "ok": True},
                            "planning_nonzero_ratio": {"actual": 0.83, "threshold": 0.8, "ok": True},
                            "control_used_planning_ratio": {"actual": 0.999, "threshold": 0.8, "ok": True},
                            "route_distance_achieved_m": {"actual": 32.5106, "threshold": 140.0, "ok": False},
                            "route_completion_ratio": {"actual": 0.1303, "threshold": 0.55, "ok": False},
                            "max_speed_mps": {"actual": 1.96, "threshold": 5.0, "ok": False},
                            "low_speed_creep_duration_sec": {"actual": 0.0, "threshold": 2.0, "ok": True},
                            "lateral_metrics": {
                                "available": True,
                                "raw_steer_nonzero_ratio": 0.9993,
                                "commanded_steer_nonzero_ratio": 0.0,
                                "force_zero_steer_applied_count": 11223,
                                "guard_trigger_ratio": 0.0,
                            },
                            "planning_trajectory_type_summary": {
                                "summary_status": "finalized",
                                "path_fallback_count": 661,
                                "persistent_path_fallback_at_end": True,
                                "last_trajectory_type": "PATH_FALLBACK",
                            },
                        },
                    },
                    "dreamview_recording": {
                        "recording_status": "failed",
                    },
                },
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "route_id": "town01_rh_spawn219_goal062",
                    "comparison_label": "junction_traverse_liveattempt",
                },
            )
            _write_json(
                artifacts / "startup_stage.json",
                {
                    "stage": "scenario_build_done",
                    "ts_sec": 240.0,
                },
            )
            _write_json(
                artifacts / "carla_world_ready_summary.json",
                {
                    "status": "world_ready",
                    "final_town": "Town01",
                },
            )
            _write_jsonl(
                artifacts / "startup_stage_timeline.jsonl",
                [
                    {"stage": "carla_get_world_ok", "current_town": "Town01", "ts_sec": 100.0},
                    {"stage": "adapter_prepare_done", "ts_sec": 100.8},
                    {"stage": "scenario_build_start", "ts_sec": 120.0},
                    {"stage": "scenario_build_done", "ts_sec": 130.0},
                ],
            )
            _write_jsonl(
                artifacts / "apollo_backend_startup_trace.jsonl",
                [
                    {"step": "docker_prepare_begin", "ts_sec": 100.1},
                    {"step": "start_return", "ts_sec": 104.0},
                ],
            )
            (artifacts / "apollo_mainboard_runtime_check.log").write_text(
                "\n".join(
                    [
                        "timeout_sec: 15.0",
                        "returncode: 0",
                        "--- stdout ---",
                        "",
                        "--- stderr ---",
                        "",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_start.log").write_text(
                "\n".join(
                    [
                        "timeout_sec: 90.0",
                        "returncode: 0",
                        "--- stdout ---",
                        "routing up",
                        "--- stderr ---",
                        "",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "debug_timeseries.csv").write_text(
                "\n".join(
                    [
                        "ts_sec,speed_mps,apollo_desired_steer,commanded_steer,commanded_throttle,commanded_brake,planning_lateral_latest_sequence_num,planning_lateral_latest_point_count,trajectory_contract_lateral_guard_applied",
                        "1774761305.26,1.31,0.00009,0.0,0.2355,0.0,2777,70,False",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            report_path = root / "probe.md"
            json_output_path = root / "probe.json"
            argv = [
                "inspect_town01_live_route_progress.py",
                "--run-dir",
                str(run_dir),
                "--report",
                str(report_path),
                "--json-output",
                str(json_output_path),
            ]
            stdout = io.StringIO()
            with mock.patch.object(sys, "argv", argv), mock.patch("sys.stdout", stdout):
                rc = town01_live_progress_probe.main()
            self.assertEqual(rc, 0)
            payload = json.loads(stdout.getvalue())
            blocker = payload["blocker_analysis"]
            self.assertEqual(
                blocker["root_blocker_family"],
                "route_established_but_behavior_unhealthy",
            )
            self.assertEqual(
                blocker["blocker_summary"],
                "world_ready_reached -> route_established -> route_distance_below_threshold -> route_completion_below_threshold -> max_speed_below_threshold -> persistent_path_fallback_at_end -> ended_in_path_fallback -> commanded_steer_zero_while_raw_nonzero",
            )
            self.assertEqual(blocker["terminal_blocker_family"], "commanded_steer_zero_while_raw_nonzero")
            self.assertEqual(blocker["path_fallback_count"], 661)
            self.assertTrue(blocker["persistent_path_fallback_at_end"])
            self.assertTrue(blocker["commanded_steer_zero_while_raw_nonzero"])
            self.assertIn(
                "post-routing persistent path-fallback and zero-steer masking",
                blocker["recommended_next_focus"],
            )
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("root_blocker_family: `route_established_but_behavior_unhealthy`", report_text)
            self.assertIn("persistent_path_fallback_at_end: `True`", report_text)
            self.assertIn("commanded_steer_zero_while_raw_nonzero: `True`", report_text)
            json_payload = json.loads(json_output_path.read_text(encoding="utf-8"))
            self.assertEqual(
                json_payload["blocker_analysis"]["acceptance_failure_codes"],
                ["ROUTE_DISTANCE_ACHIEVED_M", "ROUTE_COMPLETION_RATIO", "MAX_SPEED_MPS"],
            )

    def test_apollo_semantic_suite_run_logged_times_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            log_path = root / "scene.log"
            with self.assertRaises(TimeoutError):
                apollo_semantic_suite._run_logged(
                    [
                        sys.executable,
                        "-c",
                        "import time; time.sleep(2.0)",
                    ],
                    log_path=log_path,
                    cwd=root,
                    label="unit timeout",
                    heartbeat_sec=10.0,
                    timeout_sec=0.5,
                )
            self.assertTrue(log_path.exists())
            self.assertIn("python", log_path.read_text(encoding="utf-8"))

    def test_unified_calibration_run_suite_stage_forwards_max_scenes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pipeline_dir = root / "pipeline"
            config_path = root / "pipeline.yaml"
            config_path.write_text("pipeline: {}\n", encoding="utf-8")
            pipeline_cfg = {
                "config_path": str(config_path),
                "pipeline": {
                    "suite": {
                        "apollo_profile": "configs/io/examples/apollo_semantic_suite_base.yaml",
                        "scene_timeout_sec": 75.0,
                        "max_scenes": 1,
                    }
                },
            }
            seen: dict[str, object] = {}

            def _fake_run_logged(cmd, *, log_path, cwd, env):
                seen["cmd"] = list(cmd)
                suite_dir = pipeline_dir / "suite"
                suite_dir.mkdir(parents=True, exist_ok=True)
                _write_json(suite_dir / "suite_manifest.json", {"status": "completed", "capture_summary": {}})

            with mock.patch.object(unified_calibration_pipeline, "_run_logged", side_effect=_fake_run_logged):
                suite_dir, manifest = unified_calibration_pipeline._run_suite_stage(
                    pipeline_dir=pipeline_dir,
                    pipeline_cfg=pipeline_cfg,
                    mode="capture_only",
                    resume=False,
                    env={},
                )

            self.assertEqual(suite_dir, pipeline_dir / "suite")
            self.assertEqual(manifest["status"], "completed")
            cmd = seen["cmd"]
            self.assertIsInstance(cmd, list)
            self.assertIn("--max-scenes", cmd)
            self.assertEqual(cmd[cmd.index("--max-scenes") + 1], "1")
            self.assertIn("--scene-timeout-sec", cmd)

    def test_unified_calibration_main_capture_only_manifest_surfaces_replay_contract_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pipeline_dir = tmp_path / "unified_pipeline_run"
            fallback_cfg = tmp_path / "semantic_capture.yaml"
            fallback_cfg.write_text("mode: semantic\n", encoding="utf-8")
            args = types.SimpleNamespace(
                capture_only=True,
                infer_only=False,
                validate_only=False,
                full=False,
                config="unused.yaml",
                output_dir=str(pipeline_dir),
                suite_dir="",
                resume=False,
                candidate_calibration_file="",
                baseline_calibration_file="",
            )
            suite_manifest = {
                "capture_summary": {
                    "valid_capture_count": 0,
                    "invalid_capture_count": 1,
                    "captures": [
                        {
                            "capture_id": "bad_case",
                            "capture_valid": False,
                            "run_dir": str(tmp_path / "bad_capture"),
                            "summary_path": str(tmp_path / "bad_capture" / "summary.json"),
                        }
                    ],
                }
            }
            pipeline_cfg = {
                "config_path": str(tmp_path / "pipeline.yaml"),
                "pipeline": {
                    "suite": {},
                    "baseline": {},
                    "replay_validation": {"capture_configs": [str(fallback_cfg)]},
                    "tracking_validation": {},
                    "compatibility": {"protected_configs": []},
                },
            }
            comparison_payload = {
                "recommended_for_default": False,
                "status": "insufficient_data",
                "improved_metrics": [],
                "regressed_metrics": [],
            }
            with (
                mock.patch.object(unified_calibration_pipeline, "parse_args", return_value=args),
                mock.patch.object(unified_calibration_pipeline, "_build_config", return_value=pipeline_cfg),
                mock.patch.object(unified_calibration_pipeline, "_child_env", return_value={}),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "_artifact_targets",
                    return_value=[pipeline_dir / "artifacts"],
                ),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "_run_suite_stage",
                    return_value=(pipeline_dir / "suite", suite_manifest),
                ),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "compare_validation_payloads",
                    return_value=comparison_payload,
                ),
                mock.patch.object(unified_calibration_pipeline, "write_comparison_artifacts"),
            ):
                rc = unified_calibration_pipeline.main()
            self.assertEqual(rc, 0)
            manifest = json.loads(
                (pipeline_dir / "artifacts" / "unified_calibration_manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                manifest["replay_input_contract_summary"]["replay_source_mode"],
                "replay_config_capture_configs",
            )
            self.assertTrue(manifest["replay_input_contract_summary"]["selected_source_ready"])
            self.assertEqual(manifest["replay_input_contract_summary"]["invalid_capture_count"], 1)
            self.assertTrue((pipeline_dir / "artifacts" / "replay_input_contract.json").exists())

    def test_unified_calibration_main_capture_only_manifest_surfaces_baseline_reference_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pipeline_dir = tmp_path / "unified_pipeline_run"
            library_dir = tmp_path / "calibration_library"
            entry_dir = library_dir / "entry_a"
            entry_dir.mkdir(parents=True, exist_ok=True)
            calibration_path = entry_dir / "carla_actuator_calibration.json"
            metadata_path = entry_dir / "metadata.json"
            calibration_path.write_text("{}", encoding="utf-8")
            metadata_path.write_text("{}", encoding="utf-8")
            _write_json(
                library_dir / "index.json",
                {
                    "entries": [
                        {
                            "entry_id": "entry_a",
                            "request_fingerprint": "fingerprint_a",
                            "map": "Town01",
                            "calibration_path": str(calibration_path),
                            "metadata_path": str(metadata_path),
                        }
                    ]
                },
            )
            args = types.SimpleNamespace(
                capture_only=True,
                infer_only=False,
                validate_only=False,
                full=False,
                config="unused.yaml",
                output_dir=str(pipeline_dir),
                suite_dir="",
                resume=False,
                candidate_calibration_file="",
                baseline_calibration_file="",
            )
            suite_manifest = {"capture_summary": {"valid_capture_count": 0, "invalid_capture_count": 0, "captures": []}}
            pipeline_cfg = {
                "config_path": str(tmp_path / "pipeline.yaml"),
                "pipeline": {
                    "suite": {"calibration_library_dir": str(library_dir)},
                    "baseline": {
                        "library_dir": str(library_dir),
                        "library_entry_id": "entry_a",
                    },
                    "replay_validation": {"capture_configs": []},
                    "tracking_validation": {},
                    "compatibility": {"protected_configs": []},
                },
            }
            comparison_payload = {
                "recommended_for_default": False,
                "status": "insufficient_data",
                "improved_metrics": [],
                "regressed_metrics": [],
            }
            with (
                mock.patch.object(unified_calibration_pipeline, "parse_args", return_value=args),
                mock.patch.object(unified_calibration_pipeline, "_build_config", return_value=pipeline_cfg),
                mock.patch.object(unified_calibration_pipeline, "_child_env", return_value={}),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "_artifact_targets",
                    return_value=[pipeline_dir / "artifacts"],
                ),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "_run_suite_stage",
                    return_value=(pipeline_dir / "suite", suite_manifest),
                ),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "compare_validation_payloads",
                    return_value=comparison_payload,
                ),
                mock.patch.object(unified_calibration_pipeline, "write_comparison_artifacts"),
            ):
                rc = unified_calibration_pipeline.main()
            self.assertEqual(rc, 0)
            manifest = json.loads(
                (pipeline_dir / "artifacts" / "unified_calibration_manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["baseline_reference_summary"]["source"], "pipeline_library_entry")
            self.assertTrue(manifest["baseline_reference_summary"]["exists"])
            self.assertEqual(manifest["baseline_reference_summary"]["resolution_mode"], "library_index")
            self.assertEqual(manifest["baseline_reference_summary"]["requested_library_entry_id"], "entry_a")
            self.assertTrue(manifest["baseline_reference_summary"]["library_index_entry_found"])
            self.assertEqual(manifest["baseline_reference_summary"]["library_entry_map"], "Town01")
            self.assertTrue((pipeline_dir / "artifacts" / "baseline_reference_summary.json").exists())
            self.assertTrue((pipeline_dir / "artifacts" / "baseline_reference_summary.md").exists())

    def test_unified_calibration_main_capture_only_failure_still_writes_baseline_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pipeline_dir = tmp_path / "unified_pipeline_run"
            suite_dir = pipeline_dir / "suite"
            suite_dir.mkdir(parents=True, exist_ok=True)
            _write_json(
                suite_dir / "suite_manifest.json",
                {"capture_summary": {"valid_capture_count": 0, "invalid_capture_count": 0, "captures": []}},
            )
            library_dir = tmp_path / "calibration_library"
            entry_dir = library_dir / "entry_a"
            entry_dir.mkdir(parents=True, exist_ok=True)
            calibration_path = entry_dir / "carla_actuator_calibration.json"
            metadata_path = entry_dir / "metadata.json"
            calibration_path.write_text("{}", encoding="utf-8")
            metadata_path.write_text("{}", encoding="utf-8")
            _write_json(
                library_dir / "index.json",
                {
                    "entries": [
                        {
                            "entry_id": "entry_a",
                            "request_fingerprint": "fingerprint_a",
                            "map": "Town01",
                            "calibration_path": str(calibration_path),
                            "metadata_path": str(metadata_path),
                        }
                    ]
                },
            )
            args = types.SimpleNamespace(
                capture_only=True,
                infer_only=False,
                validate_only=False,
                full=False,
                config="unused.yaml",
                output_dir=str(pipeline_dir),
                suite_dir="",
                resume=False,
                candidate_calibration_file="",
                baseline_calibration_file="",
            )
            pipeline_cfg = {
                "config_path": str(tmp_path / "pipeline.yaml"),
                "pipeline": {
                    "suite": {"calibration_library_dir": str(library_dir)},
                    "baseline": {
                        "library_dir": str(library_dir),
                        "library_entry_id": "entry_a",
                    },
                    "replay_validation": {"capture_configs": []},
                    "tracking_validation": {},
                    "compatibility": {"protected_configs": []},
                },
            }
            comparison_payload = {
                "recommended_for_default": False,
                "status": "insufficient_data",
                "improved_metrics": [],
                "regressed_metrics": [],
            }
            with (
                mock.patch.object(unified_calibration_pipeline, "parse_args", return_value=args),
                mock.patch.object(unified_calibration_pipeline, "_build_config", return_value=pipeline_cfg),
                mock.patch.object(unified_calibration_pipeline, "_child_env", return_value={}),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "_artifact_targets",
                    return_value=[pipeline_dir / "artifacts"],
                ),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "_run_suite_stage",
                    side_effect=subprocess.CalledProcessError(1, ["suite_cmd"]),
                ),
                mock.patch.object(
                    unified_calibration_pipeline,
                    "compare_validation_payloads",
                    return_value=comparison_payload,
                ),
                mock.patch.object(unified_calibration_pipeline, "write_comparison_artifacts"),
            ):
                rc = unified_calibration_pipeline.main()
            self.assertEqual(rc, 1)
            manifest = json.loads(
                (pipeline_dir / "artifacts" / "unified_calibration_manifest.json").read_text(encoding="utf-8")
            )
            self.assertTrue(manifest["suite_stage_failed"])
            self.assertEqual(manifest["suite_stage_returncode"], 1)
            self.assertEqual(manifest["baseline_reference_summary"]["requested_library_entry_id"], "entry_a")
            self.assertTrue((pipeline_dir / "artifacts" / "baseline_reference_summary.json").exists())
            self.assertTrue((pipeline_dir / "artifacts" / "baseline_reference_summary.md").exists())

    def test_resolve_baseline_reference_details_uses_library_index_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            library_dir = Path(tmpdir) / "calibration_library"
            entry_dir = library_dir / "entry_a"
            entry_dir.mkdir(parents=True, exist_ok=True)
            calibration_path = entry_dir / "carla_actuator_calibration.json"
            metadata_path = entry_dir / "metadata.json"
            calibration_path.write_text("{}", encoding="utf-8")
            metadata_path.write_text("{}", encoding="utf-8")
            _write_json(
                library_dir / "index.json",
                {
                    "entries": [
                        {
                            "entry_id": "entry_a",
                            "request_fingerprint": "fingerprint_a",
                            "map": "Town01",
                            "calibration_path": str(calibration_path),
                            "metadata_path": str(metadata_path),
                        }
                    ]
                },
            )
            details = _resolve_baseline_reference_details(
                {
                    "pipeline": {
                        "suite": {"calibration_library_dir": str(library_dir)},
                        "baseline": {
                            "library_dir": str(library_dir),
                            "library_entry_id": "entry_a",
                        },
                    }
                },
                types.SimpleNamespace(baseline_calibration_file=""),
            )
        self.assertEqual(details["source"], "pipeline_library_entry")
        self.assertEqual(details["resolution_mode"], "library_index")
        self.assertEqual(details["resolved_calibration_path"], str(calibration_path))
        self.assertTrue(details["library_index_exists"])
        self.assertTrue(details["library_index_entry_found"])
        self.assertEqual(details["library_entry_request_fingerprint"], "fingerprint_a")
        self.assertEqual(details["library_entry_map"], "Town01")
        self.assertEqual(details["library_entry_metadata_path"], str(metadata_path))

    def test_resolve_baseline_reference_details_falls_back_to_conventional_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            library_dir = Path(tmpdir) / "calibration_library"
            conventional_path = library_dir / "entry_missing" / "carla_actuator_calibration.json"
            _write_json(library_dir / "index.json", {"entries": []})
            details = _resolve_baseline_reference_details(
                {
                    "pipeline": {
                        "suite": {"calibration_library_dir": str(library_dir)},
                        "baseline": {
                            "library_dir": str(library_dir),
                            "library_entry_id": "entry_missing",
                        },
                    }
                },
                types.SimpleNamespace(baseline_calibration_file=""),
            )
        self.assertEqual(details["source"], "pipeline_library_entry")
        self.assertEqual(details["resolution_mode"], "library_conventional_path")
        self.assertEqual(details["resolved_calibration_path"], str(conventional_path.resolve()))
        self.assertTrue(details["library_index_exists"])
        self.assertFalse(details["library_index_entry_found"])
        self.assertEqual(details["requested_library_entry_id"], "entry_missing")

    def test_flags_for_run_auto_enables_stage6_reference_line_when_cache_clear_requested(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(
            [
                "run",
                "--route-id",
                "town01_rh_spawn068_goal068",
                "--clear-lane-follow-cache-on-new-command",
            ]
        )
        flags = _flags_for_run(args)
        self.assertTrue(flags["enable_stage6_reference_line"])
        self.assertTrue(flags["stage6_clear_lane_follow_cache_on_new_command"])
        self.assertFalse(flags["stage6_reference_line_generation_guard"])

    def test_overrides_for_flags_include_stage6_reference_line_settings(self) -> None:
        flags = {
            "enable_lateral": True,
            "enable_guard": True,
            "enable_lateral_debug": False,
            "enable_recording": False,
            "enable_strict_observation": False,
            "enable_stage6_reference_line": True,
            "stage6_clear_lane_follow_cache_on_new_command": True,
            "stage6_reference_line_generation_guard": False,
        }
        overrides = _overrides_for_flags(
            flags,
            corpus_path=Path("/tmp/town01_route_corpus.json"),
            route_id="town01_rh_spawn219_goal046",
            ticks=700,
            comparison_label="stage6_probe",
        )
        self.assertIn("run.claim_profile=true", overrides)
        self.assertIn("run.materialization_probe=true", overrides)
        self.assertIn("algo.apollo.stage6_reference_line.enabled=true", overrides)
        self.assertIn(
            "algo.apollo.stage6_reference_line.clear_lane_follow_cache_on_new_command=true",
            overrides,
        )
        self.assertIn(
            "algo.apollo.stage6_reference_line.reference_line_generation_guard=false",
            overrides,
        )

    def test_overrides_for_flags_toggle_planning_modes_with_lateral_flag(self) -> None:
        lateral_flags = {
            "enable_lateral": True,
            "enable_guard": True,
            "enable_lateral_debug": False,
            "enable_recording": False,
            "enable_strict_observation": False,
            "enable_stage6_reference_line": False,
            "stage6_clear_lane_follow_cache_on_new_command": True,
            "stage6_reference_line_generation_guard": False,
        }
        lateral_overrides = _overrides_for_flags(
            lateral_flags,
            corpus_path=Path("/tmp/town01_route_corpus.json"),
            route_id="town01_rh_spawn213_goal059",
            ticks=700,
            comparison_label="curve_probe",
        )
        self.assertIn("algo.apollo.control_mapping.force_zero_steer_output=false", lateral_overrides)
        self.assertIn("algo.apollo.planning.acc_only_mode=false", lateral_overrides)
        self.assertIn("algo.apollo.planning.longitudinal_only_pipeline=false", lateral_overrides)
        self.assertIn(
            "algo.apollo.planning.longitudinal_only_keep_lane_follow_path=false",
            lateral_overrides,
        )

        longitudinal_flags = dict(lateral_flags)
        longitudinal_flags["enable_lateral"] = False
        longitudinal_overrides = _overrides_for_flags(
            longitudinal_flags,
            corpus_path=Path("/tmp/town01_route_corpus.json"),
            route_id="town01_rh_spawn219_goal063",
            ticks=700,
            comparison_label="traffic_probe",
        )
        self.assertIn(
            "algo.apollo.control_mapping.force_zero_steer_output=true",
            longitudinal_overrides,
        )
        self.assertIn("algo.apollo.planning.acc_only_mode=true", longitudinal_overrides)
        self.assertIn("algo.apollo.planning.longitudinal_only_pipeline=true", longitudinal_overrides)
        self.assertIn(
            "algo.apollo.planning.longitudinal_only_keep_lane_follow_path=true",
            longitudinal_overrides,
        )

    def test_cleanup_carla_processes_issues_term_then_kill(self) -> None:
        commands: list[str] = []

        def _fake_run(argv: list[str], **_: object) -> mock.Mock:
            commands.append(" ".join(str(item) for item in argv))
            return mock.Mock()

        with (
            mock.patch("tools.run_town01_route_health.subprocess.run", side_effect=_fake_run),
            mock.patch("tools.run_town01_route_health.time.sleep"),
        ):
            _cleanup_carla_processes()

        self.assertEqual(len(commands), 4)
        self.assertIn("pkill -TERM -f CarlaUE4-Linux-Shipping", commands[0])
        self.assertIn("pkill -TERM -f CarlaUE4.sh", commands[1])
        self.assertIn("pkill -KILL -f CarlaUE4-Linux-Shipping", commands[2])
        self.assertIn("pkill -KILL -f CarlaUE4.sh", commands[3])

    def test_ros_sourced_env_timeout_falls_back_to_base_env(self) -> None:
        with (
            mock.patch("tools.run_town01_route_health._ROS_SOURCED_ENV", None),
            mock.patch("tools.run_town01_route_health.subprocess.run", side_effect=subprocess.TimeoutExpired("bash", 10.0)),
        ):
            self.assertEqual(_ros_sourced_env({"A": "1"}), {"A": "1"})

    def test_evaluate_runtime_contract_marks_curve_longitudinal_shell_as_misconfigured(self) -> None:
        contract = evaluate_runtime_contract(
            "curve_lane_follow",
            {
                "enable_lateral": False,
                "force_zero_steer_output": True,
                "effective_acc_only_mode": True,
                "effective_longitudinal_only_pipeline": True,
                "effective_longitudinal_only_keep_lane_follow_path": True,
                "localization_reference_mode": "vehicle_origin",
            },
        )
        self.assertEqual(contract["status"], "misconfigured")
        self.assertTrue(contract["requires_true_lateral"])
        self.assertIn("enable_lateral=false", contract["blockers"])
        self.assertIn("acc_only_mode=true", contract["blockers"])
        self.assertIn("localization_reference_mode=vehicle_origin", contract["blockers"])

    def test_evaluate_runtime_contract_marks_lane_keep_fair_lateral_canary_as_aligned(self) -> None:
        contract = evaluate_runtime_contract(
            "lane_keep",
            {
                "enable_lateral": True,
                "force_zero_steer_output": False,
                "effective_acc_only_mode": False,
                "effective_longitudinal_only_pipeline": False,
                "effective_longitudinal_only_keep_lane_follow_path": False,
                "localization_reference_mode": "rear_axle",
            },
        )
        self.assertEqual(contract["status"], "aligned")
        self.assertFalse(contract["requires_true_lateral"])
        self.assertTrue(contract["requested_true_lateral"])

    def test_preview_runtime_contract_detects_traffic_misconfiguration_and_curve_alignment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "mainline.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "run": {"ticks": 700},
                        "algo": {
                            "apollo": {
                                "bridge": {
                                    "localization_back_offset_m": 0.0,
                                },
                                "planning": {
                                    "acc_only_mode": True,
                                    "longitudinal_only_pipeline": True,
                                    "longitudinal_only_keep_lane_follow_path": True,
                                },
                                "control_mapping": {
                                    "force_zero_steer_output": True,
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            misconfigured = _preview_runtime_contract(
                config_path,
                base_overrides=[],
                flags={
                    "enable_lateral": False,
                    "enable_guard": True,
                    "enable_lateral_debug": False,
                    "enable_recording": False,
                    "enable_strict_observation": False,
                    "enable_stage6_reference_line": False,
                    "stage6_clear_lane_follow_cache_on_new_command": False,
                    "stage6_reference_line_generation_guard": False,
                },
                corpus_path=Path("/tmp/town01_route_corpus.json"),
                route_id="town01_rh_spawn179_goal063",
                ticks=700,
                comparison_label="traffic_probe",
                capability_profile="traffic_light_actual",
            )
            self.assertEqual(misconfigured["runtime_contract"]["status"], "misconfigured")
            self.assertEqual(misconfigured["runtime_mode"]["localization_reference_mode"], "vehicle_origin")

            aligned = _preview_runtime_contract(
                config_path,
                base_overrides=["algo.apollo.bridge.localization_back_offset_m=1.4235"],
                flags={
                    "enable_lateral": True,
                    "enable_guard": True,
                    "enable_lateral_debug": False,
                    "enable_recording": False,
                    "enable_strict_observation": False,
                    "enable_stage6_reference_line": False,
                    "stage6_clear_lane_follow_cache_on_new_command": False,
                    "stage6_reference_line_generation_guard": False,
                },
                corpus_path=Path("/tmp/town01_route_corpus.json"),
                route_id="town01_rh_spawn217_goal048",
                ticks=700,
                comparison_label="curve_probe",
                capability_profile="curve_lane_follow",
            )
            self.assertEqual(aligned["runtime_contract"]["status"], "aligned")
            self.assertFalse(aligned["runtime_mode"]["effective_acc_only_mode"])
            self.assertEqual(aligned["runtime_mode"]["localization_reference_mode"], "rear_axle")

    def test_route_id_from_metadata_legacy_fields(self) -> None:
        route_id = route_id_from_metadata({"used_spawn_idx": 68, "goal_trace_index": 68})
        self.assertEqual(route_id, "town01_rh_spawn068_goal068")

    def test_select_route_ids_prefers_mainline_subset_and_is_stable(self) -> None:
        corpus = {
            "routes": [
                {"route_id": "a", "recommended_uses": ["mainline_regression"]},
                {"route_id": "b", "recommended_uses": ["mainline_regression"]},
                {"route_id": "c", "recommended_uses": []},
                {"route_id": "d", "recommended_uses": []},
            ]
        }
        selected = select_route_ids(corpus, sample_size=3, sample_seed=7)
        self.assertEqual(selected[:2], ["a", "b"])
        self.assertEqual(selected, select_route_ids(corpus, sample_size=3, sample_seed=7))

    def test_select_route_ids_supports_recommended_subset(self) -> None:
        corpus = {
            "routes": [
                {
                    "route_id": "ls1",
                    "recommended_uses": ["mainline_regression", "lateral_smoke_candidate"],
                    "route_length_m": 230.0,
                    "road_transition_count": 0,
                    "lane_transition_count": 0,
                    "spawn_lane": {"road_id": 8, "lane_id": 1},
                },
                {
                    "route_id": "ls2",
                    "recommended_uses": ["lateral_smoke_candidate"],
                    "route_length_m": 231.0,
                    "road_transition_count": 0,
                    "lane_transition_count": 0,
                    "spawn_lane": {"road_id": 15, "lane_id": -1},
                },
                {"route_id": "other", "recommended_uses": [], "route_length_m": 300.0},
            ],
            "recommended_subsets": {"lateral_smoke_candidate": ["ls1", "ls2"]},
        }
        selected = select_route_ids(
            corpus,
            sample_size=2,
            sample_seed=7,
            recommended_subset="lateral_smoke_candidate",
        )
        self.assertEqual(selected, ["ls1", "ls2"])

    def test_select_route_ids_supports_repeat_verified_subset(self) -> None:
        corpus = {
            "routes": [
                {"route_id": "r1", "recommended_uses": ["guarded_lateral_repeat_verified_smoke"]},
                {"route_id": "r2", "recommended_uses": ["guarded_lateral_repeat_verified_smoke"]},
                {"route_id": "other", "recommended_uses": []},
            ],
            "recommended_subsets": {"guarded_lateral_repeat_verified_smoke": ["r1", "r2"]},
        }
        selected = select_route_ids(
            corpus,
            sample_size=2,
            sample_seed=7,
            recommended_subset="guarded_lateral_repeat_verified_smoke",
        )
        self.assertEqual(selected, ["r1", "r2"])

    def test_select_route_ids_does_not_pad_outside_explicit_recommended_subset(self) -> None:
        corpus = {
            "routes": [
                {"route_id": "a", "recommended_uses": ["guarded_lateral_first_wave_smoke"]},
                {"route_id": "b", "recommended_uses": ["guarded_lateral_first_wave_smoke"]},
                {"route_id": "c", "recommended_uses": []},
                {"route_id": "d", "recommended_uses": []},
            ],
            "recommended_subsets": {"guarded_lateral_first_wave_smoke": ["a", "b"]},
        }
        selected = select_route_ids(
            corpus,
            sample_size=4,
            sample_seed=7,
            recommended_subset="guarded_lateral_first_wave_smoke",
        )
        self.assertEqual(selected, ["a", "b"])

    def test_load_route_corpus_builds_guarded_lateral_first_wave_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "pass1",
                                "recommended_uses": ["lateral_smoke_candidate"],
                                "health_tags": ["empirically_reviewed", "guarded_lateral_runtime_ok", "route_health_pass"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "candidate1",
                                "recommended_uses": ["lateral_smoke_candidate"],
                                "health_tags": ["empirically_reviewed", "guarded_lateral_runtime_ok", "route_health_candidate"],
                                "route_length_m": 231.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 15, "lane_id": -1},
                            },
                            {
                                "route_id": "pressure",
                                "recommended_uses": ["lateral_smoke_candidate", "guarded_lateral_first_wave_smoke"],
                                "health_tags": ["empirically_reviewed", "recoverable_fallback_pressure"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 9, "lane_id": 1},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertIn("guarded_lateral_first_wave_smoke", by_id["pass1"]["recommended_uses"])
            self.assertIn("guarded_lateral_first_wave_smoke", by_id["candidate1"]["recommended_uses"])
            self.assertNotIn("guarded_lateral_first_wave_smoke", by_id["pressure"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["guarded_lateral_first_wave_smoke"], ["pass1", "candidate1"])

    def test_load_route_corpus_builds_guarded_lateral_repeat_verified_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "repeat1",
                                "recommended_uses": ["guarded_lateral_first_wave_smoke"],
                                "health_tags": [
                                    "empirically_reviewed",
                                    "guarded_lateral_runtime_ok",
                                    "guarded_lateral_repeat_verified",
                                    "route_health_pass",
                                ],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "firstwave_only",
                                "recommended_uses": ["guarded_lateral_first_wave_smoke"],
                                "health_tags": ["empirically_reviewed", "guarded_lateral_runtime_ok", "route_health_candidate"],
                                "route_length_m": 231.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 15, "lane_id": -1},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertIn("guarded_lateral_repeat_verified_smoke", by_id["repeat1"]["recommended_uses"])
            self.assertNotIn("guarded_lateral_repeat_verified_smoke", by_id["firstwave_only"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["guarded_lateral_repeat_verified_smoke"], ["repeat1"])

    def test_load_route_corpus_backfills_lateral_smoke_candidate_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "ls1",
                                "recommended_uses": ["mainline_regression"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "other",
                                "recommended_uses": [],
                                "route_length_m": 310.0,
                                "road_transition_count": 2,
                                "lane_transition_count": 1,
                                "spawn_lane": {"road_id": 1, "lane_id": 1},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            self.assertIn("lateral_smoke_candidate", corpus["routes"][0]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["lateral_smoke_candidate"], ["ls1"])

    def test_load_route_corpus_excludes_persistent_fallback_route_from_lateral_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "bad",
                                "recommended_uses": ["lateral_smoke_candidate"],
                                "health_tags": ["persistent_path_fallback_risk"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "good",
                                "recommended_uses": [],
                                "health_tags": ["route_health_candidate", "guarded_lateral_runtime_ok"],
                                "route_length_m": 230.5,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 15, "lane_id": -1},
                            },
                        ],
                        "recommended_subsets": {"lateral_smoke_candidate": ["bad", "good"]},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertNotIn("lateral_smoke_candidate", by_id["bad"]["recommended_uses"])
            self.assertIn("lateral_smoke_candidate", by_id["good"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["lateral_smoke_candidate"], ["good"])

    def test_load_route_corpus_excludes_recoverable_fallback_pressure_route_from_lateral_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "pressure",
                                "recommended_uses": ["lateral_smoke_candidate"],
                                "health_tags": ["recoverable_fallback_pressure"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "good",
                                "recommended_uses": [],
                                "health_tags": ["route_health_candidate", "guarded_lateral_runtime_ok"],
                                "route_length_m": 230.5,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 15, "lane_id": -1},
                            },
                        ],
                        "recommended_subsets": {"lateral_smoke_candidate": ["pressure", "good"]},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertNotIn("lateral_smoke_candidate", by_id["pressure"]["recommended_uses"])
            self.assertIn("lateral_smoke_candidate", by_id["good"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["lateral_smoke_candidate"], ["good"])

    def test_load_route_corpus_excludes_repeat_instability_route_from_guarded_subsets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "unstable",
                                "recommended_uses": [
                                    "lateral_smoke_candidate",
                                    "guarded_lateral_first_wave_smoke",
                                    "guarded_lateral_repeat_verified_smoke",
                                ],
                                "health_tags": ["repeat_instability_risk"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "good",
                                "recommended_uses": [
                                    "lateral_smoke_candidate",
                                    "guarded_lateral_first_wave_smoke",
                                    "guarded_lateral_repeat_verified_smoke",
                                ],
                                "health_tags": [
                                    "empirically_reviewed",
                                    "guarded_lateral_runtime_ok",
                                    "guarded_lateral_repeat_verified",
                                    "route_health_pass",
                                ],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                        ],
                        "recommended_subsets": {
                            "lateral_smoke_candidate": ["unstable", "good"],
                            "guarded_lateral_first_wave_smoke": ["unstable", "good"],
                            "guarded_lateral_repeat_verified_smoke": ["unstable", "good"],
                        },
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertNotIn("lateral_smoke_candidate", by_id["unstable"]["recommended_uses"])
            self.assertNotIn("guarded_lateral_first_wave_smoke", by_id["unstable"]["recommended_uses"])
            self.assertNotIn("guarded_lateral_repeat_verified_smoke", by_id["unstable"]["recommended_uses"])
            self.assertIn("guarded_lateral_first_wave_smoke", by_id["good"]["recommended_uses"])
            self.assertIn("guarded_lateral_repeat_verified_smoke", by_id["good"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["guarded_lateral_first_wave_smoke"], ["good"])
            self.assertEqual(corpus["recommended_subsets"]["guarded_lateral_repeat_verified_smoke"], ["good"])

    def test_load_route_corpus_excludes_current_time_smaller_precursor_route_from_guarded_subsets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "precursor",
                                "recommended_uses": [
                                    "lateral_smoke_candidate",
                                    "guarded_lateral_first_wave_smoke",
                                    "guarded_lateral_repeat_verified_smoke",
                                ],
                                "health_tags": ["current_time_smaller_precursor_risk"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "good",
                                "recommended_uses": [
                                    "lateral_smoke_candidate",
                                    "guarded_lateral_first_wave_smoke",
                                    "guarded_lateral_repeat_verified_smoke",
                                ],
                                "health_tags": [
                                    "empirically_reviewed",
                                    "guarded_lateral_runtime_ok",
                                    "guarded_lateral_repeat_verified",
                                    "route_health_pass",
                                ],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                        ],
                        "recommended_subsets": {
                            "lateral_smoke_candidate": ["precursor", "good"],
                            "guarded_lateral_first_wave_smoke": ["precursor", "good"],
                            "guarded_lateral_repeat_verified_smoke": ["precursor", "good"],
                        },
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertNotIn("lateral_smoke_candidate", by_id["precursor"]["recommended_uses"])
            self.assertNotIn("guarded_lateral_first_wave_smoke", by_id["precursor"]["recommended_uses"])
            self.assertNotIn("guarded_lateral_repeat_verified_smoke", by_id["precursor"]["recommended_uses"])
            self.assertIn("guarded_lateral_first_wave_smoke", by_id["good"]["recommended_uses"])
            self.assertIn("guarded_lateral_repeat_verified_smoke", by_id["good"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["guarded_lateral_first_wave_smoke"], ["good"])
            self.assertEqual(corpus["recommended_subsets"]["guarded_lateral_repeat_verified_smoke"], ["good"])

    def test_load_route_corpus_excludes_relapse_heavy_route_from_lateral_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "relapse",
                                "recommended_uses": ["lateral_smoke_candidate"],
                                "health_tags": ["relapse_heavy_after_recovery"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "good",
                                "recommended_uses": [],
                                "health_tags": ["route_health_candidate", "guarded_lateral_runtime_ok"],
                                "route_length_m": 230.5,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 15, "lane_id": -1},
                            },
                        ],
                        "recommended_subsets": {"lateral_smoke_candidate": ["relapse", "good"]},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertNotIn("lateral_smoke_candidate", by_id["relapse"]["recommended_uses"])
            self.assertIn("lateral_smoke_candidate", by_id["good"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["lateral_smoke_candidate"], ["good"])

    def test_load_route_corpus_does_not_readd_empirically_reviewed_route_to_lateral_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "reviewed_bad",
                                "recommended_uses": [],
                                "health_tags": ["empirically_reviewed", "deprioritized_lateral_smoke"],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 8, "lane_id": 1},
                            },
                            {
                                "route_id": "new_auto",
                                "recommended_uses": [],
                                "health_tags": [],
                                "route_length_m": 230.0,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_lane": {"road_id": 15, "lane_id": -1},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertNotIn("lateral_smoke_candidate", by_id["reviewed_bad"]["recommended_uses"])
            self.assertIn("lateral_smoke_candidate", by_id["new_auto"]["recommended_uses"])
            self.assertEqual(corpus["recommended_subsets"]["lateral_smoke_candidate"], ["new_auto"])

    def test_load_route_corpus_builds_capability_candidate_subsets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "town01_rh_spawn097_goal046",
                                "recommended_uses": [],
                                "health_tags": [
                                    "empirically_reviewed",
                                    "guarded_lateral_runtime_ok",
                                    "guarded_lateral_repeat_verified",
                                    "route_health_candidate",
                                    "route_health_pass",
                                ],
                                "route_length_m": 230.0,
                                "spawn_idx": 97,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "town01_rh_spawn217_goal046",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 230.0,
                                "spawn_idx": 217,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "town01_rh_spawn119_goal046",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 230.0,
                                "spawn_idx": 119,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "town01_rh_spawn183_goal046",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 230.0,
                                "spawn_idx": 183,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "town01_rh_spawn219_goal046",
                                "recommended_uses": [],
                                "health_tags": [
                                    "empirically_reviewed",
                                    "repeat_instability_risk",
                                    "reference_line_provider_bridge_risk",
                                    "reroute_propagation_frame_bridge_risk",
                                    "persistent_path_fallback_risk",
                                ],
                                "route_length_m": 230.0,
                                "spawn_idx": 219,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "town01_rh_spawn217_goal048",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 217,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 45.0},
                            },
                            {
                                "route_id": "town01_rh_spawn119_goal059",
                                "recommended_uses": [],
                                "route_length_m": 250.0,
                                "spawn_idx": 119,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 55.0},
                            },
                            {
                                "route_id": "town01_rh_spawn183_goal061",
                                "recommended_uses": [],
                                "route_length_m": 250.0,
                                "spawn_idx": 183,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 35.0},
                            },
                            {
                                "route_id": "town01_rh_spawn176_goal061",
                                "recommended_uses": [],
                                "route_length_m": 250.0,
                                "spawn_idx": 176,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 36.0},
                            },
                            {
                                "route_id": "town01_rh_spawn219_goal048",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 219,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 52.0},
                            },
                            {
                                "route_id": "town01_rh_spawn179_goal063",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 179,
                                "road_transition_count": 4,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "town01_rh_spawn217_goal052",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 217,
                                "road_transition_count": 2,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "town01_rh_spawn119_goal062",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 119,
                                "road_transition_count": 2,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "town01_rh_spawn183_goal064",
                                "recommended_uses": [],
                                "route_length_m": 250.0,
                                "spawn_idx": 183,
                                "road_transition_count": 4,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "town01_rh_spawn219_goal052",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 219,
                                "road_transition_count": 2,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertEqual(by_id["town01_rh_spawn097_goal046"]["goal_abs_heading_delta_deg"], 0.0)
            self.assertEqual(by_id["town01_rh_spawn217_goal048"]["goal_abs_heading_delta_deg"], 45.0)
            self.assertEqual(by_id["town01_rh_spawn179_goal063"]["goal_abs_heading_delta_deg"], 90.0)
            self.assertEqual(by_id["town01_rh_spawn219_goal052"]["goal_abs_heading_delta_deg"], 90.0)
            self.assertIn("lane_keep_candidate", by_id["town01_rh_spawn097_goal046"]["recommended_uses"])
            self.assertIn("lane_keep_first_wave_smoke", by_id["town01_rh_spawn097_goal046"]["recommended_uses"])
            self.assertIn("curve_lane_follow_candidate", by_id["town01_rh_spawn217_goal048"]["recommended_uses"])
            self.assertIn("curve_lane_follow_first_wave_smoke", by_id["town01_rh_spawn217_goal048"]["recommended_uses"])
            self.assertIn("junction_traverse_candidate", by_id["town01_rh_spawn179_goal063"]["recommended_uses"])
            self.assertIn("junction_traverse_first_wave_smoke", by_id["town01_rh_spawn179_goal063"]["recommended_uses"])
            self.assertIn("junction_traverse_candidate", by_id["town01_rh_spawn219_goal052"]["recommended_uses"])
            self.assertNotIn("junction_traverse_first_wave_smoke", by_id["town01_rh_spawn219_goal052"]["recommended_uses"])
            self.assertIn("traffic_light_candidate", by_id["town01_rh_spawn179_goal063"]["recommended_uses"])
            self.assertIn("traffic_light_first_wave_smoke", by_id["town01_rh_spawn179_goal063"]["recommended_uses"])
            self.assertIn("traffic_light_candidate", by_id["town01_rh_spawn219_goal052"]["recommended_uses"])
            self.assertNotIn("traffic_light_first_wave_smoke", by_id["town01_rh_spawn219_goal052"]["recommended_uses"])
            self.assertEqual(
                corpus["recommended_subsets"]["lane_keep_first_wave_smoke"],
                [
                    "town01_rh_spawn097_goal046",
                    "town01_rh_spawn119_goal046",
                    "town01_rh_spawn183_goal046",
                    "town01_rh_spawn217_goal046",
                ],
            )
            self.assertEqual(
                corpus["recommended_subsets"]["curve_lane_follow_first_wave_smoke"],
                [
                    "town01_rh_spawn217_goal048",
                    "town01_rh_spawn119_goal059",
                    "town01_rh_spawn176_goal061",
                    "town01_rh_spawn183_goal061",
                ],
            )
            self.assertEqual(
                corpus["recommended_subsets"]["junction_traverse_first_wave_smoke"],
                [
                    "town01_rh_spawn183_goal064",
                    "town01_rh_spawn119_goal062",
                    "town01_rh_spawn179_goal063",
                    "town01_rh_spawn217_goal052",
                ],
            )
            self.assertEqual(
                corpus["recommended_subsets"]["traffic_light_first_wave_smoke"],
                [
                    "town01_rh_spawn119_goal062",
                    "town01_rh_spawn179_goal063",
                    "town01_rh_spawn217_goal052",
                    "town01_rh_spawn183_goal064",
                ],
            )

    def test_load_route_corpus_prefers_direction_diverse_first_wave_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "curve_left_gentle",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 10,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 28.0},
                            },
                            {
                                "route_id": "curve_right_standard",
                                "recommended_uses": [],
                                "route_length_m": 245.0,
                                "spawn_idx": 11,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -40.0},
                            },
                            {
                                "route_id": "curve_left_sharp",
                                "recommended_uses": [],
                                "route_length_m": 255.0,
                                "spawn_idx": 12,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 58.0},
                            },
                            {
                                "route_id": "curve_right_sharp",
                                "recommended_uses": [],
                                "route_length_m": 260.0,
                                "spawn_idx": 13,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -62.0},
                            },
                            {
                                "route_id": "j_left_cross",
                                "recommended_uses": [],
                                "route_length_m": 245.0,
                                "spawn_idx": 21,
                                "road_transition_count": 4,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "j_right_cross",
                                "recommended_uses": [],
                                "route_length_m": 246.0,
                                "spawn_idx": 22,
                                "road_transition_count": 4,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -90.0},
                            },
                            {
                                "route_id": "j_left_multi",
                                "recommended_uses": [],
                                "route_length_m": 280.0,
                                "spawn_idx": 23,
                                "road_transition_count": 3,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "j_right_simple",
                                "recommended_uses": [],
                                "route_length_m": 242.0,
                                "spawn_idx": 24,
                                "road_transition_count": 2,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -90.0},
                            },
                            {
                                "route_id": "tl_left_cross_long",
                                "recommended_uses": [],
                                "route_length_m": 265.0,
                                "spawn_idx": 31,
                                "road_transition_count": 4,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "tl_right_cross_compact",
                                "recommended_uses": [],
                                "route_length_m": 240.0,
                                "spawn_idx": 32,
                                "road_transition_count": 4,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -90.0},
                            },
                            {
                                "route_id": "tl_left_turn",
                                "recommended_uses": [],
                                "route_length_m": 238.0,
                                "spawn_idx": 33,
                                "road_transition_count": 2,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 90.0},
                            },
                            {
                                "route_id": "tl_right_turn",
                                "recommended_uses": [],
                                "route_length_m": 239.0,
                                "spawn_idx": 34,
                                "road_transition_count": 2,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -90.0},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertEqual(by_id["curve_right_standard"]["curve_lane_follow_direction_class"], "right_bend")
            self.assertEqual(by_id["j_left_cross"]["junction_traverse_direction_class"], "left_turn")
            self.assertEqual(by_id["tl_right_turn"]["traffic_light_direction_class"], "signalized_right_turn")

            curve_ids = corpus["recommended_subsets"]["curve_lane_follow_first_wave_smoke"]
            junction_ids = corpus["recommended_subsets"]["junction_traverse_first_wave_smoke"]
            traffic_ids = corpus["recommended_subsets"]["traffic_light_first_wave_smoke"]
            curve_contrast_ids = corpus["recommended_subsets"]["curve_lane_follow_contrast_queue"]

            curve_dirs = {by_id[route_id]["curve_lane_follow_direction_class"] for route_id in curve_ids}
            junction_dirs = {by_id[route_id]["junction_traverse_direction_class"] for route_id in junction_ids}
            traffic_dirs = {by_id[route_id]["traffic_light_direction_class"] for route_id in traffic_ids}

            self.assertIn("left_bend", curve_dirs)
            self.assertIn("right_bend", curve_dirs)
            self.assertIn("left_turn", junction_dirs)
            self.assertIn("right_turn", junction_dirs)
            self.assertIn("signalized_left_turn", traffic_dirs)
            self.assertIn("signalized_right_turn", traffic_dirs)
            self.assertIn("curve_right_sharp", curve_contrast_ids)
            self.assertIn("curve_right_standard", curve_contrast_ids)
            self.assertIn("curve_left_sharp", curve_contrast_ids)

    def test_load_route_corpus_builds_lane_keep_next_review_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "lane_pass",
                                "recommended_uses": [],
                                "health_tags": ["empirically_reviewed", "route_health_pass"],
                                "route_length_m": 230.0,
                                "spawn_idx": 1,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "lane_candidate",
                                "recommended_uses": [],
                                "health_tags": ["route_health_candidate"],
                                "route_length_m": 230.0,
                                "spawn_idx": 2,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "lane_unreviewed",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 230.0,
                                "spawn_idx": 3,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "lane_unreviewed_late",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 236.0,
                                "spawn_idx": 5,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                            {
                                "route_id": "lane_risky",
                                "recommended_uses": [],
                                "health_tags": ["repeat_instability_risk"],
                                "route_length_m": 230.0,
                                "spawn_idx": 4,
                                "road_transition_count": 0,
                                "lane_transition_count": 0,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 0.0},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertEqual(
                corpus["recommended_subsets"]["lane_keep_next_review_queue"],
                ["lane_unreviewed", "lane_unreviewed_late", "lane_candidate"],
            )
            self.assertEqual(
                set(corpus["recommended_subsets"]["lane_keep_contrast_queue"]),
                {"lane_pass", "lane_candidate", "lane_unreviewed", "lane_unreviewed_late"},
            )
            self.assertEqual(
                set(corpus["recommended_subsets"]["lane_keep_review_pack"]),
                {"lane_pass", "lane_candidate", "lane_unreviewed", "lane_unreviewed_late"},
            )
            self.assertEqual(
                corpus["recommended_subsets"]["lane_keep_review_priority_queue"],
                ["lane_unreviewed", "lane_unreviewed_late", "lane_candidate", "lane_pass"],
            )
            self.assertEqual(
                corpus["recommended_subsets"]["lane_keep_focus_pack"],
                ["lane_unreviewed", "lane_unreviewed_late", "lane_pass"],
            )
            self.assertEqual(
                corpus["recommended_subsets"]["lane_keep_history_gap_queue"],
                ["lane_unreviewed", "lane_unreviewed_late"],
            )
            self.assertIn("lane_keep_next_review_queue", by_id["lane_unreviewed"]["recommended_uses"])
            self.assertIn("lane_keep_next_review_queue", by_id["lane_unreviewed_late"]["recommended_uses"])
            self.assertIn("lane_keep_next_review_queue", by_id["lane_candidate"]["recommended_uses"])
            self.assertIn("lane_keep_contrast_queue", by_id["lane_pass"]["recommended_uses"])
            self.assertIn("lane_keep_contrast_queue", by_id["lane_candidate"]["recommended_uses"])
            self.assertIn("lane_keep_review_pack", by_id["lane_pass"]["recommended_uses"])
            self.assertIn("lane_keep_review_pack", by_id["lane_candidate"]["recommended_uses"])
            self.assertIn("lane_keep_review_pack", by_id["lane_unreviewed"]["recommended_uses"])
            self.assertIn("lane_keep_review_pack", by_id["lane_unreviewed_late"]["recommended_uses"])
            self.assertIn("lane_keep_review_priority_queue", by_id["lane_pass"]["recommended_uses"])
            self.assertIn("lane_keep_review_priority_queue", by_id["lane_candidate"]["recommended_uses"])
            self.assertIn("lane_keep_review_priority_queue", by_id["lane_unreviewed"]["recommended_uses"])
            self.assertIn("lane_keep_review_priority_queue", by_id["lane_unreviewed_late"]["recommended_uses"])
            self.assertIn("lane_keep_focus_pack", by_id["lane_pass"]["recommended_uses"])
            self.assertNotIn("lane_keep_focus_pack", by_id["lane_candidate"]["recommended_uses"])
            self.assertIn("lane_keep_focus_pack", by_id["lane_unreviewed"]["recommended_uses"])
            self.assertIn("lane_keep_focus_pack", by_id["lane_unreviewed_late"]["recommended_uses"])
            self.assertIn("lane_keep_history_gap_queue", by_id["lane_unreviewed"]["recommended_uses"])
            self.assertIn("lane_keep_history_gap_queue", by_id["lane_unreviewed_late"]["recommended_uses"])
            self.assertNotIn("lane_keep_history_gap_queue", by_id["lane_pass"]["recommended_uses"])
            self.assertNotIn("lane_keep_next_review_queue", by_id["lane_pass"]["recommended_uses"])
            self.assertNotIn("lane_keep_next_review_queue", by_id["lane_risky"]["recommended_uses"])
            self.assertNotIn("lane_keep_review_pack", by_id["lane_risky"]["recommended_uses"])
            self.assertNotIn("lane_keep_review_priority_queue", by_id["lane_risky"]["recommended_uses"])
            self.assertNotIn("lane_keep_focus_pack", by_id["lane_risky"]["recommended_uses"])
            self.assertNotIn("lane_keep_history_gap_queue", by_id["lane_risky"]["recommended_uses"])

    def test_load_route_corpus_prioritizes_uncovered_shape_in_curve_next_review_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            corpus_path = Path(tmpdir) / "town01_route_corpus.json"
            corpus_path.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "route_id": "curve_std_right",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 240.0,
                                "spawn_idx": 10,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -45.0},
                            },
                            {
                                "route_id": "curve_std_left",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 240.0,
                                "spawn_idx": 11,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 40.0},
                            },
                            {
                                "route_id": "curve_sharp_left",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 240.0,
                                "spawn_idx": 12,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 60.0},
                            },
                            {
                                "route_id": "curve_sharp_right",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 240.0,
                                "spawn_idx": 13,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": -60.0},
                            },
                            {
                                "route_id": "curve_gentle_left",
                                "recommended_uses": [],
                                "health_tags": ["unreviewed"],
                                "route_length_m": 240.0,
                                "spawn_idx": 14,
                                "road_transition_count": 1,
                                "lane_transition_count": 1,
                                "spawn_pose": {"yaw_deg": 0.0},
                                "goal_pose": {"yaw_deg": 25.0},
                            },
                        ],
                        "recommended_subsets": {},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            corpus = load_route_corpus(corpus_path)
            by_id = {row["route_id"]: row for row in corpus["routes"]}
            self.assertEqual(
                corpus["recommended_subsets"]["curve_lane_follow_next_review_queue"][0],
                "curve_sharp_right",
            )
            self.assertIn("curve_lane_follow_next_review_queue", by_id["curve_sharp_right"]["recommended_uses"])
            self.assertNotIn("curve_lane_follow_first_wave_smoke", by_id["curve_sharp_right"]["recommended_uses"])

    def test_refresh_overlay_deprioritizes_empirically_bad_lateral_route(self) -> None:
        route = {
            "route_id": "bad_lat",
            "recommended_uses": ["mainline_regression", "lateral_smoke_candidate"],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.35,
            "best_lateral_distance_m": 80.1,
            "has_persistent_path_fallback": False,
            "has_guarded_lateral_runtime_ok": False,
            "has_route_health_candidate": False,
            "has_route_health_pass": False,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("empirically_reviewed", updated["health_tags"])
        self.assertIn("deprioritized_lateral_smoke", updated["health_tags"])
        self.assertNotIn("lateral_smoke_candidate", updated["recommended_uses"])

    def test_build_capability_progress_rows_summarizes_first_wave_statuses(self) -> None:
        corpus = {
            "routes": [
                {
                    "route_id": "lane_pass",
                    "health_tags": ["empirically_reviewed", "route_health_pass"],
                    "recommended_uses": ["lane_keep_candidate", "lane_keep_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 0.0},
                },
                {
                    "route_id": "lane_unreviewed",
                    "health_tags": ["unreviewed"],
                    "recommended_uses": ["lane_keep_candidate", "lane_keep_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 0.0},
                },
                {
                    "route_id": "junction_risky",
                    "health_tags": ["empirically_reviewed", "reference_line_provider_bridge_risk"],
                    "recommended_uses": ["junction_traverse_candidate"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -90.0},
                },
                {
                    "route_id": "junction_new",
                    "health_tags": ["unreviewed"],
                    "recommended_uses": ["junction_traverse_candidate", "junction_traverse_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 90.0},
                },
            ],
            "recommended_subsets": {
                "lane_keep_candidate": ["lane_pass", "lane_unreviewed"],
                "lane_keep_first_wave_smoke": ["lane_pass", "lane_unreviewed"],
                "lane_keep_next_review_queue": ["lane_unreviewed"],
                "lane_keep_contrast_queue": ["lane_pass", "lane_unreviewed"],
                "lane_keep_review_pack": ["lane_pass", "lane_unreviewed"],
                "lane_keep_review_priority_queue": ["lane_unreviewed", "lane_pass"],
                "lane_keep_focus_pack": ["lane_unreviewed", "lane_pass"],
                "curve_lane_follow_candidate": [],
                "curve_lane_follow_first_wave_smoke": [],
                "curve_lane_follow_next_review_queue": [],
                "curve_lane_follow_contrast_queue": [],
                "curve_lane_follow_review_pack": [],
                "curve_lane_follow_review_priority_queue": [],
                "curve_lane_follow_focus_pack": [],
                "junction_traverse_candidate": ["junction_risky", "junction_new"],
                "junction_traverse_first_wave_smoke": ["junction_new"],
                "junction_traverse_next_review_queue": ["junction_new"],
                "junction_traverse_contrast_queue": ["junction_new"],
                "junction_traverse_review_pack": ["junction_new"],
                "junction_traverse_review_priority_queue": ["junction_new"],
                "junction_traverse_focus_pack": ["junction_new"],
                "traffic_light_candidate": [],
                "traffic_light_first_wave_smoke": [],
                "traffic_light_next_review_queue": [],
                "traffic_light_contrast_queue": [],
                "traffic_light_review_pack": [],
                "traffic_light_review_priority_queue": [],
                "traffic_light_focus_pack": [],
            },
        }
        comparison_rows = [
            {
                "route_id": "lane_pass",
                "comparison_label": "lane_batch",
                "route_health_label": "route_health_pass",
                "route_completion_ratio": "0.91",
                "route_distance_achieved_m": "180.0",
            },
            {
                "route_id": "junction_risky",
                "comparison_label": "junction_batch",
                "route_health_label": "route_health_candidate",
                "route_completion_ratio": "0.41",
                "route_distance_achieved_m": "80.0",
            },
        ]
        rows = build_capability_progress_rows(corpus, comparison_rows)
        by_capability = {row["capability"]: row for row in rows}
        self.assertEqual(by_capability["lane_keep"]["readiness"], "validated")
        self.assertEqual(by_capability["lane_keep"]["first_wave_pass_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["first_wave_unreviewed_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["first_wave_direction_coverage"], "straight")
        self.assertEqual(by_capability["lane_keep"]["first_wave_shape_coverage"], "short_straight:straight")
        self.assertEqual(by_capability["lane_keep"]["uncovered_candidate_shape_coverage"], "none")
        self.assertEqual(by_capability["lane_keep"]["next_review_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["next_review_unreviewed_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["next_review_shape_coverage"], "short_straight:straight")
        self.assertEqual(by_capability["lane_keep"]["contrast_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["contrast_pass_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["contrast_unreviewed_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["contrast_shape_coverage"], "short_straight:straight")
        self.assertEqual(by_capability["lane_keep"]["review_pack_subset"], "lane_keep_review_pack")
        self.assertEqual(by_capability["lane_keep"]["review_pack_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["review_priority_subset"], "lane_keep_review_priority_queue")
        self.assertEqual(by_capability["lane_keep"]["review_priority_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["focus_pack_subset"], "lane_keep_focus_pack")
        self.assertEqual(by_capability["lane_keep"]["focus_pack_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["focus_pack_readiness"], "pass_anchor_present")
        self.assertEqual(by_capability["lane_keep"]["focus_pack_shape_coverage"], "short_straight:straight")
        self.assertEqual(by_capability["lane_keep"]["focus_pack_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["missing_focus_pack_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["review_priority_shape_coverage"], "short_straight:straight")
        self.assertEqual(by_capability["lane_keep"]["review_priority_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["missing_review_priority_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["review_pack_readiness"], "pass_anchor_present")
        self.assertEqual(by_capability["lane_keep"]["review_pack_shape_coverage"], "short_straight:straight")
        self.assertEqual(by_capability["lane_keep"]["review_pack_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["pair_gap_subset"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["pair_gap_count"], 0)
        self.assertEqual(by_capability["lane_keep"]["pair_gap_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["candidate_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["missing_contrast_pair_coverage"], "n/a")
        self.assertEqual(by_capability["lane_keep"]["missing_review_pack_pair_coverage"], "n/a")
        self.assertEqual(by_capability["junction_traverse"]["readiness"], "discovery")
        self.assertEqual(by_capability["junction_traverse"]["first_wave_unreviewed_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["first_wave_direction_coverage"], "left_turn")
        self.assertEqual(by_capability["junction_traverse"]["first_wave_shape_coverage"], "simple_turn:left_turn")
        self.assertEqual(
            by_capability["junction_traverse"]["candidate_pair_coverage"],
            "simple_turn:left_turn vs right_turn",
        )
        self.assertEqual(by_capability["junction_traverse"]["first_wave_pair_coverage"], "none")
        self.assertEqual(by_capability["junction_traverse"]["uncovered_candidate_shape_coverage"], "simple_turn:right_turn")
        self.assertEqual(by_capability["junction_traverse"]["next_review_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["next_review_unreviewed_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["next_review_shape_coverage"], "simple_turn:left_turn")
        self.assertEqual(by_capability["junction_traverse"]["next_review_pair_coverage"], "none")
        self.assertEqual(by_capability["junction_traverse"]["contrast_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["contrast_unreviewed_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["contrast_shape_coverage"], "simple_turn:left_turn")
        self.assertEqual(by_capability["junction_traverse"]["contrast_pair_coverage"], "none")
        self.assertEqual(by_capability["junction_traverse"]["review_pack_subset"], "junction_traverse_review_pack")
        self.assertEqual(by_capability["junction_traverse"]["review_pack_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["review_priority_subset"], "junction_traverse_review_priority_queue")
        self.assertEqual(by_capability["junction_traverse"]["review_priority_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["focus_pack_subset"], "junction_traverse_focus_pack")
        self.assertEqual(by_capability["junction_traverse"]["focus_pack_count"], 1)
        self.assertEqual(by_capability["junction_traverse"]["focus_pack_readiness"], "pair_fragment")
        self.assertEqual(by_capability["junction_traverse"]["focus_pack_shape_coverage"], "simple_turn:left_turn")
        self.assertEqual(by_capability["junction_traverse"]["focus_pack_pair_coverage"], "none")
        self.assertEqual(
            by_capability["junction_traverse"]["missing_focus_pack_pair_coverage"],
            "simple_turn:left_turn vs right_turn",
        )
        self.assertEqual(by_capability["junction_traverse"]["review_priority_shape_coverage"], "simple_turn:left_turn")
        self.assertEqual(by_capability["junction_traverse"]["review_priority_pair_coverage"], "none")
        self.assertEqual(
            by_capability["junction_traverse"]["missing_review_priority_pair_coverage"],
            "simple_turn:left_turn vs right_turn",
        )
        self.assertEqual(by_capability["junction_traverse"]["review_pack_readiness"], "pair_incomplete")
        self.assertEqual(by_capability["junction_traverse"]["review_pack_shape_coverage"], "simple_turn:left_turn")
        self.assertEqual(by_capability["junction_traverse"]["review_pack_pair_coverage"], "none")
        self.assertEqual(by_capability["junction_traverse"]["pair_gap_subset"], "junction_traverse_pair_gap_queue")
        self.assertEqual(by_capability["junction_traverse"]["pair_gap_count"], 0)
        self.assertEqual(by_capability["junction_traverse"]["pair_gap_pair_coverage"], "none")
        self.assertEqual(
            by_capability["junction_traverse"]["missing_first_wave_pair_coverage"],
            "simple_turn:left_turn vs right_turn",
        )
        self.assertEqual(
            by_capability["junction_traverse"]["missing_contrast_pair_coverage"],
            "simple_turn:left_turn vs right_turn",
        )
        self.assertEqual(
            by_capability["junction_traverse"]["missing_review_pack_pair_coverage"],
            "simple_turn:left_turn vs right_turn",
        )
        self.assertEqual(by_capability["junction_traverse"]["excluded_candidate_rows"][0]["route_id"], "junction_risky")
        self.assertEqual(by_capability["junction_traverse"]["excluded_candidate_rows"][0]["status"], "risky")

    def test_build_capability_progress_rows_marks_no_semantic_history_when_none_exists(self) -> None:
        corpus = {
            "routes": [
                {
                    "route_id": "curve_left",
                    "health_tags": ["unreviewed"],
                    "recommended_uses": ["curve_lane_follow_candidate", "curve_lane_follow_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 25.0},
                },
                {
                    "route_id": "curve_right",
                    "health_tags": ["unreviewed"],
                    "recommended_uses": ["curve_lane_follow_candidate", "curve_lane_follow_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -25.0},
                },
            ],
            "recommended_subsets": {
                "lane_keep_candidate": [],
                "lane_keep_first_wave_smoke": [],
                "lane_keep_next_review_queue": [],
                "lane_keep_contrast_queue": [],
                "lane_keep_review_pack": [],
                "lane_keep_review_priority_queue": [],
                "lane_keep_focus_pack": [],
                "curve_lane_follow_candidate": ["curve_left", "curve_right"],
                "curve_lane_follow_first_wave_smoke": ["curve_left", "curve_right"],
                "curve_lane_follow_next_review_queue": ["curve_left", "curve_right"],
                "curve_lane_follow_contrast_queue": ["curve_left", "curve_right"],
                "curve_lane_follow_review_pack": ["curve_left", "curve_right"],
                "curve_lane_follow_review_priority_queue": ["curve_left", "curve_right"],
                "curve_lane_follow_focus_pack": ["curve_left", "curve_right"],
                "curve_lane_follow_pair_gap_queue": [],
                "junction_traverse_candidate": [],
                "junction_traverse_first_wave_smoke": [],
                "junction_traverse_next_review_queue": [],
                "junction_traverse_contrast_queue": [],
                "junction_traverse_review_pack": [],
                "junction_traverse_review_priority_queue": [],
                "junction_traverse_focus_pack": [],
                "junction_traverse_pair_gap_queue": [],
                "traffic_light_candidate": [],
                "traffic_light_first_wave_smoke": [],
                "traffic_light_next_review_queue": [],
                "traffic_light_contrast_queue": [],
                "traffic_light_review_pack": [],
                "traffic_light_review_priority_queue": [],
                "traffic_light_focus_pack": [],
                "traffic_light_pair_gap_queue": [],
            },
        }
        rows = build_capability_progress_rows(corpus, [], [])
        by_capability = {row["capability"]: row for row in rows}
        self.assertEqual(by_capability["curve_lane_follow"]["history_readiness"], "no_semantic_history")
        self.assertEqual(by_capability["curve_lane_follow"]["historical_semantic_run_count"], 0)
        self.assertEqual(by_capability["curve_lane_follow"]["historical_semantic_route_count"], 0)
        self.assertEqual(by_capability["curve_lane_follow"]["historical_focus_pack_run_count"], 0)
        self.assertEqual(by_capability["curve_lane_follow"]["historical_focus_pack_missing_count"], 2)
        self.assertEqual(
            by_capability["curve_lane_follow"]["historical_focus_pack_missing_routes"],
            "curve_left, curve_right",
        )

    def test_build_capability_progress_rows_marks_semantic_history_with_pass(self) -> None:
        corpus = {
            "routes": [
                {
                    "route_id": "lane_anchor",
                    "health_tags": ["empirically_reviewed", "route_health_pass"],
                    "recommended_uses": ["lane_keep_candidate", "lane_keep_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 0.0},
                },
                {
                    "route_id": "lane_probe",
                    "health_tags": ["unreviewed"],
                    "recommended_uses": ["lane_keep_candidate", "lane_keep_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 0.0},
                },
            ],
            "recommended_subsets": {
                "lane_keep_candidate": ["lane_anchor", "lane_probe"],
                "lane_keep_first_wave_smoke": ["lane_anchor", "lane_probe"],
                "lane_keep_next_review_queue": ["lane_probe"],
                "lane_keep_contrast_queue": ["lane_anchor", "lane_probe"],
                "lane_keep_review_pack": ["lane_anchor", "lane_probe"],
                "lane_keep_review_priority_queue": ["lane_probe", "lane_anchor"],
                "lane_keep_focus_pack": ["lane_probe", "lane_anchor"],
                "curve_lane_follow_candidate": [],
                "curve_lane_follow_first_wave_smoke": [],
                "curve_lane_follow_next_review_queue": [],
                "curve_lane_follow_contrast_queue": [],
                "curve_lane_follow_review_pack": [],
                "curve_lane_follow_review_priority_queue": [],
                "curve_lane_follow_focus_pack": [],
                "curve_lane_follow_pair_gap_queue": [],
                "junction_traverse_candidate": [],
                "junction_traverse_first_wave_smoke": [],
                "junction_traverse_next_review_queue": [],
                "junction_traverse_contrast_queue": [],
                "junction_traverse_review_pack": [],
                "junction_traverse_review_priority_queue": [],
                "junction_traverse_focus_pack": [],
                "junction_traverse_pair_gap_queue": [],
                "traffic_light_candidate": [],
                "traffic_light_first_wave_smoke": [],
                "traffic_light_next_review_queue": [],
                "traffic_light_contrast_queue": [],
                "traffic_light_review_pack": [],
                "traffic_light_review_priority_queue": [],
                "traffic_light_focus_pack": [],
                "traffic_light_pair_gap_queue": [],
            },
        }
        historical_summary_rows = [
            {
                "route_id": "lane_anchor",
                "comparison_label": "lane_keep_hist_pass",
                "route_health_label": "route_health_pass",
                "route_completion_ratio": 0.98,
                "route_distance_achieved_m": 201.0,
                "semantic::lane_keep_candidate": True,
            },
            {
                "route_id": "lane_probe",
                "comparison_label": "lane_keep_hist_probe",
                "route_health_label": "",
                "route_completion_ratio": 0.44,
                "route_distance_achieved_m": 90.0,
                "semantic::lane_keep_candidate": True,
            },
        ]
        rows = build_capability_progress_rows(corpus, [], historical_summary_rows)
        by_capability = {row["capability"]: row for row in rows}
        self.assertEqual(by_capability["lane_keep"]["history_readiness"], "semantic_history_with_pass")
        self.assertEqual(by_capability["lane_keep"]["historical_semantic_run_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["historical_semantic_route_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["historical_semantic_pass_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["historical_semantic_unreviewed_count"], 1)
        self.assertEqual(by_capability["lane_keep"]["historical_focus_pack_run_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["historical_focus_pack_route_count"], 2)
        self.assertEqual(by_capability["lane_keep"]["historical_focus_pack_missing_count"], 0)
        self.assertEqual(by_capability["lane_keep"]["historical_focus_pack_missing_routes"], "none")

    def test_build_capability_progress_rows_derives_runtime_semantic_assessment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            curve_summary = root / "curve" / "summary.json"
            junction_summary = root / "junction" / "summary.json"
            traffic_summary = root / "traffic" / "summary.json"
            _write_json(curve_summary, {"route_id": "curve_seed"})
            _write_json(junction_summary, {"route_id": "junction_seed"})
            _write_json(traffic_summary, {"route_id": "traffic_seed"})
            _write_csv(
                curve_summary.with_name("timeseries.csv"),
                [{"dbg_ref_wp_is_junction": False, "dbg_target_wp_is_junction": False}],
            )
            _write_csv(
                junction_summary.with_name("timeseries.csv"),
                [{"dbg_ref_wp_is_junction": False, "dbg_target_wp_is_junction": False}],
            )
            _write_csv(
                traffic_summary.with_name("timeseries.csv"),
                [{"dbg_ref_wp_is_junction": False, "dbg_target_wp_is_junction": False}],
            )
            _write_csv(
                curve_summary.parent / "artifacts" / "debug_timeseries.csv",
                [
                    {
                        "commanded_steer_pre_lateral_guards": 0.001,
                        "measured_steer": 0.0,
                        "force_zero_steer_applied": True,
                        "measured_brake": 0.0,
                        "terminal_stop_hold_active": False,
                        "traffic_light_policy": "ignore",
                    }
                ],
            )
            _write_csv(
                junction_summary.parent / "artifacts" / "debug_timeseries.csv",
                [
                    {
                        "commanded_steer_pre_lateral_guards": 0.0,
                        "measured_steer": 0.0,
                        "force_zero_steer_applied": False,
                        "measured_brake": 0.1,
                        "terminal_stop_hold_active": False,
                        "traffic_light_policy": "ignore",
                    }
                ],
            )
            _write_csv(
                traffic_summary.parent / "artifacts" / "debug_timeseries.csv",
                [
                    {
                        "commanded_steer_pre_lateral_guards": 0.0,
                        "measured_steer": 0.0,
                        "force_zero_steer_applied": False,
                        "measured_brake": 0.2,
                        "terminal_stop_hold_active": False,
                        "traffic_light_policy": "carla_actual",
                    }
                ],
            )
            _write_json(
                curve_summary.parent / "artifacts" / "bridge_health_summary.finalized.json",
                {"traffic_light_policy": "ignore", "terminal_stop_hold_engaged_count": 0},
            )
            _write_json(
                junction_summary.parent / "artifacts" / "bridge_health_summary.finalized.json",
                {"traffic_light_policy": "ignore", "terminal_stop_hold_engaged_count": 0},
            )
            _write_json(
                traffic_summary.parent / "artifacts" / "bridge_health_summary.finalized.json",
                {"traffic_light_policy": "carla_actual", "terminal_stop_hold_engaged_count": 0},
            )
            corpus = {
                "routes": [
                    {
                        "route_id": "curve_seed",
                        "health_tags": ["empirically_reviewed"],
                        "recommended_uses": [
                            "curve_lane_follow_candidate",
                            "curve_lane_follow_proxy_pack",
                            "curve_lane_follow_seed_pack",
                        ],
                        "spawn_pose": {"yaw_deg": 0.0},
                        "goal_pose": {"yaw_deg": 60.0},
                    },
                    {
                        "route_id": "junction_seed",
                        "health_tags": ["empirically_reviewed"],
                        "recommended_uses": [
                            "junction_traverse_candidate",
                            "junction_traverse_proxy_pack",
                            "junction_traverse_seed_pack",
                        ],
                        "spawn_pose": {"yaw_deg": 0.0},
                        "goal_pose": {"yaw_deg": 90.0},
                    },
                    {
                        "route_id": "traffic_seed",
                        "health_tags": ["empirically_reviewed"],
                        "recommended_uses": [
                            "traffic_light_candidate",
                            "traffic_light_proxy_pack",
                            "traffic_light_seed_pack",
                        ],
                        "spawn_pose": {"yaw_deg": 0.0},
                        "goal_pose": {"yaw_deg": -90.0},
                    },
                ],
                "recommended_subsets": {
                    "lane_keep_candidate": [],
                    "lane_keep_first_wave_smoke": [],
                    "lane_keep_next_review_queue": [],
                    "lane_keep_contrast_queue": [],
                    "lane_keep_review_pack": [],
                    "lane_keep_review_priority_queue": [],
                    "lane_keep_focus_pack": [],
                    "lane_keep_proxy_pack": [],
                    "lane_keep_seed_pack": [],
                    "lane_keep_history_gap_queue": [],
                    "curve_lane_follow_candidate": ["curve_seed"],
                    "curve_lane_follow_first_wave_smoke": ["curve_seed"],
                    "curve_lane_follow_next_review_queue": ["curve_seed"],
                    "curve_lane_follow_contrast_queue": ["curve_seed"],
                    "curve_lane_follow_review_pack": ["curve_seed"],
                    "curve_lane_follow_review_priority_queue": ["curve_seed"],
                    "curve_lane_follow_focus_pack": ["curve_seed"],
                    "curve_lane_follow_proxy_pack": ["curve_seed"],
                    "curve_lane_follow_seed_pack": ["curve_seed"],
                    "curve_lane_follow_history_gap_queue": [],
                    "curve_lane_follow_pair_gap_queue": [],
                    "junction_traverse_candidate": ["junction_seed"],
                    "junction_traverse_first_wave_smoke": ["junction_seed"],
                    "junction_traverse_next_review_queue": ["junction_seed"],
                    "junction_traverse_contrast_queue": ["junction_seed"],
                    "junction_traverse_review_pack": ["junction_seed"],
                    "junction_traverse_review_priority_queue": ["junction_seed"],
                    "junction_traverse_focus_pack": ["junction_seed"],
                    "junction_traverse_proxy_pack": ["junction_seed"],
                    "junction_traverse_seed_pack": ["junction_seed"],
                    "junction_traverse_history_gap_queue": [],
                    "junction_traverse_pair_gap_queue": [],
                    "traffic_light_candidate": ["traffic_seed"],
                    "traffic_light_first_wave_smoke": ["traffic_seed"],
                    "traffic_light_next_review_queue": ["traffic_seed"],
                    "traffic_light_contrast_queue": ["traffic_seed"],
                    "traffic_light_review_pack": ["traffic_seed"],
                    "traffic_light_review_priority_queue": ["traffic_seed"],
                    "traffic_light_focus_pack": ["traffic_seed"],
                    "traffic_light_proxy_pack": ["traffic_seed"],
                    "traffic_light_seed_pack": ["traffic_seed"],
                    "traffic_light_history_gap_queue": [],
                    "traffic_light_pair_gap_queue": [],
                },
            }
            historical_summary_rows = [
                {
                    "route_id": "curve_seed",
                    "comparison_label": "curve_hist",
                    "route_health_label": "route_established_but_behavior_unhealthy",
                    "route_completion_ratio": 0.36,
                    "route_distance_achieved_m": 111.0,
                    "summary_path": str(curve_summary),
                    "semantic::curve_lane_follow_candidate": True,
                },
                {
                    "route_id": "junction_seed",
                    "comparison_label": "junction_hist",
                    "route_health_label": "route_established_but_behavior_unhealthy",
                    "route_completion_ratio": 0.49,
                    "route_distance_achieved_m": 158.0,
                    "summary_path": str(junction_summary),
                    "semantic::junction_traverse_candidate": True,
                },
                {
                    "route_id": "traffic_seed",
                    "comparison_label": "traffic_hist",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": 0.61,
                    "route_distance_achieved_m": 146.0,
                    "summary_path": str(traffic_summary),
                    "semantic::traffic_light_candidate": True,
                },
            ]
            rows = build_capability_progress_rows(corpus, [], historical_summary_rows)
            by_capability = {row["capability"]: row for row in rows}
            self.assertEqual(by_capability["curve_lane_follow"]["semantic_observability"], "indirect_runtime")
            self.assertEqual(
                by_capability["curve_lane_follow"]["semantic_evidence_level"],
                "curve_behavior_observed_but_unhealthy",
            )
            self.assertEqual(by_capability["curve_lane_follow"]["semantic_pass_confidence"], "not_proven")
            self.assertIn(
                "measured steering stayed near zero",
                by_capability["curve_lane_follow"]["semantic_note"],
            )
            self.assertEqual(by_capability["curve_lane_follow"]["runtime_commanded_steer_intent_frame_count"], 1)
            self.assertEqual(by_capability["curve_lane_follow"]["runtime_measured_steer_nonzero_frame_count"], 0)
            self.assertEqual(by_capability["junction_traverse"]["semantic_observability"], "route_only")
            self.assertEqual(
                by_capability["junction_traverse"]["semantic_evidence_level"],
                "route_progress_without_junction_proof",
            )
            self.assertEqual(by_capability["junction_traverse"]["runtime_junction_frame_count"], 0)
            self.assertEqual(by_capability["traffic_light_actual"]["semantic_observability"], "policy_runtime")
            self.assertEqual(
                by_capability["traffic_light_actual"]["semantic_evidence_level"],
                "traffic_policy_observed_candidate",
            )
            self.assertEqual(by_capability["traffic_light_actual"]["runtime_traffic_light_policy"], "carla_actual")

    def test_build_capability_progress_rows_treats_external_stack_junction_signals_as_indirect_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            junction_summary = root / "junction" / "summary.json"
            _write_json(junction_summary, {"route_id": "junction_seed"})
            _write_csv(
                junction_summary.with_name("timeseries.csv"),
                [
                    {
                        "control_source": "external_stack",
                        "dbg_ref_wp_is_junction": False,
                        "dbg_target_wp_is_junction": False,
                        "dbg_target_wp_lane_id": "",
                        "dbg_target_wp_road_id": "",
                        "dbg_target_wp_section_id": "",
                    }
                ],
            )
            _write_csv(
                junction_summary.parent / "artifacts" / "debug_timeseries.csv",
                [
                    {
                        "commanded_steer_pre_lateral_guards": "0.0",
                        "measured_steer": "0.2",
                        "measured_brake": "0.0",
                        "force_zero_steer_applied": False,
                        "terminal_stop_hold_active": False,
                        "traffic_light_policy": "",
                    }
                ],
            )
            _write_jsonl(
                junction_summary.parent / "artifacts" / "stage5_apollo_reference_line_debug.jsonl",
                [
                    {
                        "route_segment_count": 22,
                        "create_route_segments_status": "ready",
                        "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
                        "current_lane_id": "8_1_1",
                    }
                ],
            )
            _write_jsonl(
                junction_summary.parent / "artifacts" / "planning_topic_debug.jsonl",
                [
                    {
                        "lane_id_first": "8_1_1",
                        "target_lane_id_first": "8_1_1",
                        "routing_road_count": 22,
                        "routing_segment_count": 22,
                    }
                ],
            )
            corpus = {
                "routes": [
                    {
                        "route_id": "junction_seed",
                        "health_tags": ["empirically_reviewed", "junction_traverse_semantic_history"],
                        "recommended_uses": ["junction_traverse_candidate", "junction_traverse_first_wave_smoke"],
                        "junction_traverse_geometry_class": "cross_junction_turn",
                        "junction_traverse_direction_class": "left_turn",
                        "spawn_pose": {"yaw_deg": 0.0},
                        "goal_pose": {"yaw_deg": 90.0},
                    }
                ],
                "recommended_subsets": {
                    "lane_keep_candidate": [],
                    "lane_keep_first_wave_smoke": [],
                    "lane_keep_next_review_queue": [],
                    "lane_keep_contrast_queue": [],
                    "lane_keep_review_pack": [],
                    "lane_keep_review_priority_queue": [],
                    "lane_keep_focus_pack": [],
                    "lane_keep_proxy_pack": [],
                    "lane_keep_seed_pack": [],
                    "lane_keep_history_gap_queue": [],
                    "curve_lane_follow_candidate": [],
                    "curve_lane_follow_first_wave_smoke": [],
                    "curve_lane_follow_next_review_queue": [],
                    "curve_lane_follow_contrast_queue": [],
                    "curve_lane_follow_review_pack": [],
                    "curve_lane_follow_review_priority_queue": [],
                    "curve_lane_follow_focus_pack": [],
                    "curve_lane_follow_proxy_pack": [],
                    "curve_lane_follow_seed_pack": [],
                    "curve_lane_follow_history_gap_queue": [],
                    "curve_lane_follow_pair_gap_queue": [],
                    "junction_traverse_candidate": ["junction_seed"],
                    "junction_traverse_first_wave_smoke": ["junction_seed"],
                    "junction_traverse_next_review_queue": ["junction_seed"],
                    "junction_traverse_contrast_queue": ["junction_seed"],
                    "junction_traverse_review_pack": ["junction_seed"],
                    "junction_traverse_review_priority_queue": ["junction_seed"],
                    "junction_traverse_focus_pack": ["junction_seed"],
                    "junction_traverse_proxy_pack": ["junction_seed"],
                    "junction_traverse_seed_pack": ["junction_seed"],
                    "junction_traverse_history_gap_queue": [],
                    "junction_traverse_pair_gap_queue": [],
                    "traffic_light_candidate": [],
                    "traffic_light_first_wave_smoke": [],
                    "traffic_light_next_review_queue": [],
                    "traffic_light_contrast_queue": [],
                    "traffic_light_review_pack": [],
                    "traffic_light_review_priority_queue": [],
                    "traffic_light_focus_pack": [],
                    "traffic_light_proxy_pack": [],
                    "traffic_light_seed_pack": [],
                    "traffic_light_history_gap_queue": [],
                    "traffic_light_pair_gap_queue": [],
                },
            }
            historical_summary_rows = [
                {
                    "route_id": "junction_seed",
                    "comparison_label": "junction_hist",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": 0.60,
                    "route_distance_achieved_m": 150.0,
                    "summary_path": str(junction_summary),
                    "semantic::junction_traverse_candidate": True,
                }
            ]
            rows = build_capability_progress_rows(corpus, [], historical_summary_rows)
            junction_row = next(row for row in rows if row["capability"] == "junction_traverse")
            self.assertEqual(junction_row["semantic_observability"], "indirect_runtime")
            self.assertEqual(
                junction_row["semantic_evidence_level"],
                "junction_behavior_supported_via_external_runtime",
            )
            self.assertEqual(junction_row["semantic_pass_confidence"], "partial")
            self.assertEqual(junction_row["runtime_external_control_frame_count"], 1)
            self.assertEqual(junction_row["runtime_harness_target_wp_frame_count"], 0)
            self.assertEqual(
                junction_row["runtime_harness_junction_flag_reliability"],
                "harness_external_stack_targetless_unreliable",
            )
            self.assertEqual(junction_row["runtime_apollo_route_segment_ready_frame_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_route_segment_multi_frame_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_current_lane_ids"], "8_1_1")
            self.assertEqual(junction_row["runtime_apollo_lane_transition_count"], 0)
            self.assertEqual(junction_row["runtime_apollo_lane_road_transition_count"], 0)
            self.assertEqual(junction_row["runtime_apollo_planning_first_routing_road_count"], 22)
            self.assertEqual(junction_row["runtime_apollo_planning_last_routing_road_count"], 22)
            self.assertEqual(junction_row["runtime_apollo_planning_max_routing_road_count"], 22)
            self.assertEqual(junction_row["runtime_apollo_planning_multi_road_frame_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_planning_first_routing_segment_count"], 22)
            self.assertEqual(junction_row["runtime_apollo_planning_last_routing_segment_count"], 22)
            self.assertEqual(junction_row["runtime_apollo_planning_max_routing_segment_count"], 22)
            self.assertIn("routing preview spanned up to 22 roads", junction_row["semantic_note"])
            self.assertIn("lane ids never transitioned across roads", junction_row["semantic_note"])
            self.assertIn("no target waypoint was exposed", junction_row["semantic_note"])

    def test_build_capability_progress_rows_prefers_junction_anchor_with_apollo_lane_transition(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            transition_summary = root / "junction181" / "summary.json"
            _write_json(transition_summary, {"route_id": "town01_rh_spawn181_goal066"})
            _write_csv(
                transition_summary.with_name("timeseries.csv"),
                [
                    {
                        "control_source": "external_stack",
                        "dbg_ref_wp_is_junction": False,
                        "dbg_target_wp_is_junction": False,
                        "dbg_target_wp_lane_id": "",
                        "dbg_target_wp_road_id": "",
                        "dbg_target_wp_section_id": "",
                    }
                ],
            )
            _write_csv(
                transition_summary.parent / "artifacts" / "debug_timeseries.csv",
                [
                    {
                        "commanded_steer_pre_lateral_guards": "0.0",
                        "measured_steer": "0.2",
                        "measured_brake": "0.0",
                        "force_zero_steer_applied": False,
                        "terminal_stop_hold_active": False,
                        "traffic_light_policy": "",
                    }
                ],
            )
            _write_jsonl(
                transition_summary.parent / "artifacts" / "stage5_apollo_reference_line_debug.jsonl",
                [
                    {
                        "route_segment_count": 22,
                        "create_route_segments_status": "ready",
                        "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
                        "current_lane_id": "11_1_-1",
                    },
                    {
                        "route_segment_count": 22,
                        "create_route_segments_status": "ready",
                        "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
                        "current_lane_id": "8_1_1",
                    }
                ],
            )
            _write_jsonl(
                transition_summary.parent / "artifacts" / "planning_topic_debug.jsonl",
                [
                    {
                        "lane_id_first": "11_1_-1",
                        "target_lane_id_first": "11_1_-1",
                        "routing_road_count": 2,
                        "routing_segment_count": 2,
                    },
                    {
                        "lane_id_first": "8_1_1",
                        "target_lane_id_first": "8_1_1",
                        "routing_road_count": 27,
                        "routing_segment_count": 2,
                    },
                ],
            )
            _write_json(
                transition_summary.parent / "carla_boot" / "carla_startup_probe.json",
                {
                    "attempts": [
                        {
                            "attempt": 1,
                            "status": "world_not_ready",
                            "rpc_ready": True,
                            "world_ready": False,
                            "failure_family": "rpc_ready_world_not_ready_eof_alive",
                            "retry_eligible": True,
                            "retry_decision_reason": "retry_allowed",
                        },
                        {
                            "attempt": 2,
                            "status": "world_ready",
                            "rpc_ready": True,
                            "world_ready": True,
                            "failure_family": "world_ready",
                            "retry_eligible": False,
                            "retry_decision_reason": "world_ready",
                        },
                    ]
                },
            )

            completion_summary = root / "junction219" / "summary.json"
            _write_json(completion_summary, {"route_id": "town01_rh_spawn219_goal062"})
            _write_csv(
                completion_summary.with_name("timeseries.csv"),
                [
                    {
                        "control_source": "external_stack",
                        "dbg_ref_wp_is_junction": False,
                        "dbg_target_wp_is_junction": False,
                        "dbg_target_wp_lane_id": "",
                        "dbg_target_wp_road_id": "",
                        "dbg_target_wp_section_id": "",
                    }
                ],
            )
            _write_csv(
                completion_summary.parent / "artifacts" / "debug_timeseries.csv",
                [
                    {
                        "commanded_steer_pre_lateral_guards": "0.0",
                        "measured_steer": "0.2",
                        "measured_brake": "0.0",
                        "force_zero_steer_applied": False,
                        "terminal_stop_hold_active": False,
                        "traffic_light_policy": "",
                    }
                ],
            )
            _write_jsonl(
                completion_summary.parent / "artifacts" / "stage5_apollo_reference_line_debug.jsonl",
                [
                    {
                        "route_segment_count": 22,
                        "create_route_segments_status": "ready",
                        "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
                        "current_lane_id": "8_1_1",
                    }
                ],
            )
            _write_jsonl(
                completion_summary.parent / "artifacts" / "planning_topic_debug.jsonl",
                [
                    {
                        "lane_id_first": "8_1_1",
                        "target_lane_id_first": "8_1_1",
                        "routing_road_count": 22,
                        "routing_segment_count": 2,
                    }
                ],
            )

            corpus = {
                "routes": [
                    {
                        "route_id": "town01_rh_spawn181_goal066",
                        "health_tags": ["empirically_reviewed", "junction_traverse_semantic_history"],
                        "recommended_uses": ["junction_traverse_candidate", "junction_traverse_focus_pack"],
                        "junction_traverse_geometry_class": "multi_stage_turn",
                        "junction_traverse_direction_class": "left_turn",
                        "spawn_pose": {"yaw_deg": 0.0},
                        "goal_pose": {"yaw_deg": 90.0},
                    },
                    {
                        "route_id": "town01_rh_spawn219_goal062",
                        "health_tags": ["unreviewed"],
                        "recommended_uses": ["junction_traverse_candidate", "junction_traverse_proxy_pack"],
                        "junction_traverse_geometry_class": "cross_junction_turn",
                        "junction_traverse_direction_class": "left_turn",
                        "spawn_pose": {"yaw_deg": 0.0},
                        "goal_pose": {"yaw_deg": 90.0},
                    },
                ],
                "recommended_subsets": {
                    "lane_keep_candidate": [],
                    "lane_keep_first_wave_smoke": [],
                    "lane_keep_next_review_queue": [],
                    "lane_keep_contrast_queue": [],
                    "lane_keep_review_pack": [],
                    "lane_keep_review_priority_queue": [],
                    "lane_keep_focus_pack": [],
                    "lane_keep_proxy_pack": [],
                    "lane_keep_seed_pack": [],
                    "lane_keep_history_gap_queue": [],
                    "curve_lane_follow_candidate": [],
                    "curve_lane_follow_first_wave_smoke": [],
                    "curve_lane_follow_next_review_queue": [],
                    "curve_lane_follow_contrast_queue": [],
                    "curve_lane_follow_review_pack": [],
                    "curve_lane_follow_review_priority_queue": [],
                    "curve_lane_follow_focus_pack": [],
                    "curve_lane_follow_proxy_pack": [],
                    "curve_lane_follow_seed_pack": [],
                    "curve_lane_follow_history_gap_queue": [],
                    "curve_lane_follow_pair_gap_queue": [],
                    "junction_traverse_candidate": [
                        "town01_rh_spawn181_goal066",
                        "town01_rh_spawn219_goal062",
                    ],
                    "junction_traverse_first_wave_smoke": [
                        "town01_rh_spawn181_goal066",
                        "town01_rh_spawn219_goal062",
                    ],
                    "junction_traverse_next_review_queue": [
                        "town01_rh_spawn181_goal066",
                        "town01_rh_spawn219_goal062",
                    ],
                    "junction_traverse_contrast_queue": [
                        "town01_rh_spawn181_goal066",
                        "town01_rh_spawn219_goal062",
                    ],
                    "junction_traverse_review_pack": [
                        "town01_rh_spawn181_goal066",
                        "town01_rh_spawn219_goal062",
                    ],
                    "junction_traverse_review_priority_queue": [
                        "town01_rh_spawn181_goal066",
                        "town01_rh_spawn219_goal062",
                    ],
                    "junction_traverse_focus_pack": ["town01_rh_spawn181_goal066"],
                    "junction_traverse_proxy_pack": ["town01_rh_spawn219_goal062"],
                    "junction_traverse_seed_pack": ["town01_rh_spawn181_goal066"],
                    "junction_traverse_history_gap_queue": [],
                    "junction_traverse_pair_gap_queue": [],
                    "traffic_light_candidate": [],
                    "traffic_light_first_wave_smoke": [],
                    "traffic_light_next_review_queue": [],
                    "traffic_light_contrast_queue": [],
                    "traffic_light_review_pack": [],
                    "traffic_light_review_priority_queue": [],
                    "traffic_light_focus_pack": [],
                    "traffic_light_proxy_pack": [],
                    "traffic_light_seed_pack": [],
                    "traffic_light_history_gap_queue": [],
                    "traffic_light_pair_gap_queue": [],
                },
            }
            historical_summary_rows = [
                {
                    "route_id": "town01_rh_spawn181_goal066",
                    "comparison_label": "junction_focus_left_turn",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": 0.5629831306770491,
                    "route_distance_achieved_m": 178.8,
                    "summary_path": str(transition_summary),
                    "semantic::junction_traverse_candidate": True,
                },
                {
                    "route_id": "town01_rh_spawn219_goal062",
                    "comparison_label": "junction_cross_left_turn",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": 0.6023592740592978,
                    "route_distance_achieved_m": 150.3,
                    "summary_path": str(completion_summary),
                    "semantic::junction_traverse_candidate": True,
                },
            ]

            rows = build_capability_progress_rows(corpus, [], historical_summary_rows)
            junction_row = next(row for row in rows if row["capability"] == "junction_traverse")
            self.assertEqual(junction_row["semantic_best_route_id"], "town01_rh_spawn181_goal066")
            self.assertEqual(junction_row["semantic_observability"], "direct_runtime")
            self.assertEqual(
                junction_row["semantic_evidence_level"],
                "junction_behavior_supported_via_apollo_lane_transition",
            )
            self.assertEqual(junction_row["semantic_pass_confidence"], "supported")
            self.assertEqual(junction_row["runtime_apollo_current_lane_ids"], "11_1_-1, 8_1_1")
            self.assertEqual(junction_row["runtime_apollo_current_lane_transition_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_current_lane_road_transition_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_planning_lane_ids"], "11_1_-1, 8_1_1")
            self.assertEqual(junction_row["runtime_apollo_planning_target_lane_ids"], "11_1_-1, 8_1_1")
            self.assertEqual(junction_row["runtime_apollo_lane_transition_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_target_lane_transition_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_lane_road_transition_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_target_lane_road_transition_count"], 1)
            self.assertEqual(junction_row["runtime_apollo_planning_first_routing_road_count"], 2)
            self.assertEqual(junction_row["runtime_apollo_planning_last_routing_road_count"], 27)
            self.assertEqual(junction_row["runtime_apollo_planning_max_routing_road_count"], 27)
            self.assertEqual(junction_row["runtime_apollo_planning_multi_road_frame_count"], 2)
            self.assertTrue(junction_row["startup_probe_present"])
            self.assertEqual(junction_row["startup_probe_attempt_count"], 2)
            self.assertEqual(junction_row["startup_probe_first_world_ready_attempt"], 2)
            self.assertTrue(junction_row["startup_probe_recovered_after_retry"])
            self.assertEqual(junction_row["startup_probe_lineage_class"], "world_ready_after_retry")
            self.assertEqual(
                junction_row["startup_probe_pre_world_ready_failure_families"],
                "rpc_ready_world_not_ready_eof_alive",
            )
            self.assertEqual(junction_row["startup_probe_final_status"], "world_ready")
            self.assertEqual(junction_row["startup_probe_final_failure_family"], "world_ready")
            self.assertTrue(junction_row["startup_probe_final_world_ready"])
            self.assertFalse(junction_row["startup_probe_retry_policy_present"])
            self.assertEqual(junction_row["startup_probe_final_retry_decision_reason"], "world_ready")
            self.assertIn("reference-line current lane ids", junction_row["semantic_note"])
            self.assertIn("transitioned across distinct roads", junction_row["semantic_note"])
            self.assertIn("routing preview expanded from 2 to 27 roads", junction_row["semantic_note"])

    def test_build_capability_progress_rows_counts_proxy_pack_history_outside_candidate_subset(self) -> None:
        corpus = {
            "routes": [
                {
                    "route_id": "curve_seed",
                    "health_tags": ["unreviewed"],
                    "recommended_uses": ["curve_lane_follow_candidate", "curve_lane_follow_first_wave_smoke"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 25.0},
                },
                {
                    "route_id": "curve_proxy",
                    "health_tags": ["empirically_reviewed", "curve_lane_follow_semantic_history"],
                    "recommended_uses": ["curve_lane_follow_proxy_pack"],
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 60.0},
                },
            ],
            "recommended_subsets": {
                "lane_keep_candidate": [],
                "lane_keep_first_wave_smoke": [],
                "lane_keep_next_review_queue": [],
                "lane_keep_contrast_queue": [],
                "lane_keep_review_pack": [],
                "lane_keep_review_priority_queue": [],
                "lane_keep_focus_pack": [],
                "lane_keep_proxy_pack": [],
                "lane_keep_seed_pack": [],
                "lane_keep_history_gap_queue": [],
                "curve_lane_follow_candidate": ["curve_seed"],
                "curve_lane_follow_first_wave_smoke": ["curve_seed"],
                "curve_lane_follow_next_review_queue": ["curve_seed"],
                "curve_lane_follow_contrast_queue": ["curve_seed"],
                "curve_lane_follow_review_pack": ["curve_seed"],
                "curve_lane_follow_review_priority_queue": ["curve_seed"],
                "curve_lane_follow_focus_pack": ["curve_seed"],
                "curve_lane_follow_proxy_pack": ["curve_seed", "curve_proxy"],
                "curve_lane_follow_seed_pack": ["curve_seed"],
                "curve_lane_follow_history_gap_queue": ["curve_seed"],
                "curve_lane_follow_pair_gap_queue": [],
                "junction_traverse_candidate": [],
                "junction_traverse_first_wave_smoke": [],
                "junction_traverse_next_review_queue": [],
                "junction_traverse_contrast_queue": [],
                "junction_traverse_review_pack": [],
                "junction_traverse_review_priority_queue": [],
                "junction_traverse_focus_pack": [],
                "junction_traverse_proxy_pack": [],
                "junction_traverse_seed_pack": [],
                "junction_traverse_history_gap_queue": [],
                "junction_traverse_pair_gap_queue": [],
                "traffic_light_candidate": [],
                "traffic_light_first_wave_smoke": [],
                "traffic_light_next_review_queue": [],
                "traffic_light_contrast_queue": [],
                "traffic_light_review_pack": [],
                "traffic_light_review_priority_queue": [],
                "traffic_light_focus_pack": [],
                "traffic_light_proxy_pack": [],
                "traffic_light_seed_pack": [],
                "traffic_light_history_gap_queue": [],
                "traffic_light_pair_gap_queue": [],
            },
        }
        historical_summary_rows = [
            {
                "route_id": "curve_proxy",
                "comparison_label": "curve_proxy_hist",
                "route_health_label": "route_health_candidate",
                "route_completion_ratio": 0.73,
                "route_distance_achieved_m": 150.0,
                "semantic::curve_lane_follow_candidate": True,
            }
        ]
        rows = build_capability_progress_rows(corpus, [], historical_summary_rows)
        by_capability = {row["capability"]: row for row in rows}
        self.assertEqual(by_capability["curve_lane_follow"]["historical_proxy_pack_run_count"], 1)
        self.assertEqual(by_capability["curve_lane_follow"]["historical_proxy_pack_route_count"], 1)
        self.assertEqual(by_capability["curve_lane_follow"]["historical_proxy_pack_candidate_count"], 1)
        self.assertEqual(by_capability["curve_lane_follow"]["historical_proxy_pack_missing_count"], 1)
        self.assertEqual(
            by_capability["curve_lane_follow"]["historical_proxy_pack_missing_routes"],
            "curve_seed",
        )

    def test_capability_subset_report_includes_pair_coverage(self) -> None:
        corpus = {
            "routes": [
                {
                    "route_id": "curve_left",
                    "recommended_uses": ["curve_lane_follow_candidate", "curve_lane_follow_first_wave_smoke"],
                    "route_length_m": 210.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 60.0},
                },
                {
                    "route_id": "curve_right",
                    "recommended_uses": ["curve_lane_follow_candidate", "curve_lane_follow_contrast_queue"],
                    "route_length_m": 210.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -60.0},
                },
            ],
            "recommended_subsets": {
                "curve_lane_follow_candidate": ["curve_left", "curve_right"],
                "curve_lane_follow_first_wave_smoke": ["curve_left"],
                "curve_lane_follow_next_review_queue": ["curve_right"],
                "curve_lane_follow_contrast_queue": ["curve_left", "curve_right"],
                "curve_lane_follow_review_pack": ["curve_left", "curve_right"],
                "curve_lane_follow_review_priority_queue": ["curve_right", "curve_left"],
                "curve_lane_follow_focus_pack": ["curve_right", "curve_left"],
                "curve_lane_follow_seed_pack": ["curve_left"],
                "lane_keep_candidate": [],
                "lane_keep_first_wave_smoke": [],
                "lane_keep_next_review_queue": [],
                "lane_keep_contrast_queue": [],
                "lane_keep_review_pack": [],
                "lane_keep_review_priority_queue": [],
                "lane_keep_focus_pack": [],
                "lane_keep_seed_pack": [],
                "junction_traverse_candidate": [],
                "junction_traverse_first_wave_smoke": [],
                "junction_traverse_next_review_queue": [],
                "junction_traverse_contrast_queue": [],
                "junction_traverse_review_pack": [],
                "junction_traverse_review_priority_queue": [],
                "junction_traverse_focus_pack": [],
                "junction_traverse_seed_pack": [],
                "traffic_light_candidate": [],
                "traffic_light_first_wave_smoke": [],
                "traffic_light_next_review_queue": [],
                "traffic_light_contrast_queue": [],
                "traffic_light_review_pack": [],
                "traffic_light_review_priority_queue": [],
                "traffic_light_focus_pack": [],
                "traffic_light_seed_pack": [],
            },
        }
        report = render_capability_subset_report(corpus, Path("/tmp/corpus.json"))
        self.assertIn("pair_candidates=`sharp_bend:left_bend vs right_bend`", report)
        self.assertIn("- next_review_pair_coverage: `none`", report)
        self.assertIn("- review_priority_pair_coverage: `sharp_bend:left_bend vs right_bend`", report)
        self.assertIn("- focus_pack_pair_coverage: `sharp_bend:left_bend vs right_bend`", report)
        self.assertIn("- missing_focus_pack_pair_coverage: `none`", report)
        self.assertIn("- seed_pack_pair_coverage: `none`", report)
        self.assertIn("- missing_seed_pack_pair_coverage: `sharp_bend:left_bend vs right_bend`", report)
        self.assertIn("- contrast_pair_coverage: `sharp_bend:left_bend vs right_bend`", report)
        self.assertIn("- missing_contrast_pair_coverage: `none`", report)
        self.assertIn("- review_pack_pair_coverage: `sharp_bend:left_bend vs right_bend`", report)
        self.assertIn("- missing_review_pack_pair_coverage: `none`", report)

    def test_load_route_corpus_derives_curve_pair_gap_queue(self) -> None:
        payload = {
            "routes": [
                {
                    "route_id": "curve_standard_left",
                    "spawn_idx": 1,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 45.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_standard_right",
                    "spawn_idx": 2,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -45.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_sharp_left",
                    "spawn_idx": 3,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 60.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_sharp_right",
                    "spawn_idx": 4,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -60.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_gentle_left",
                    "spawn_idx": 5,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 25.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_gentle_right",
                    "spawn_idx": 6,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -25.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
            ],
            "recommended_subsets": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "corpus.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            corpus = load_route_corpus(path)
        subsets = corpus["recommended_subsets"]
        contrast_ids = subsets["curve_lane_follow_contrast_queue"]
        review_pack_ids = subsets["curve_lane_follow_review_pack"]
        review_priority_ids = subsets["curve_lane_follow_review_priority_queue"]
        focus_pack_ids = subsets["curve_lane_follow_focus_pack"]
        seed_pack_ids = subsets["curve_lane_follow_seed_pack"]
        pair_gap_ids = subsets["curve_lane_follow_pair_gap_queue"]
        history_gap_ids = subsets["curve_lane_follow_history_gap_queue"]
        self.assertEqual(len(contrast_ids), 4)
        self.assertEqual(len(review_pack_ids), 6)
        self.assertEqual(len(review_priority_ids), 6)
        self.assertEqual(len(focus_pack_ids), 2)
        self.assertEqual(len(seed_pack_ids), 1)
        self.assertEqual(len(history_gap_ids), 2)
        self.assertEqual(len(pair_gap_ids), 2)
        contrast_geometries = {
            corpus["routes"][idx]["curve_lane_follow_geometry_class"]
            for idx, route in enumerate(corpus["routes"])
            if route["route_id"] in contrast_ids
        }
        pair_gap_geometries = {
            corpus["routes"][idx]["curve_lane_follow_geometry_class"]
            for idx, route in enumerate(corpus["routes"])
            if route["route_id"] in pair_gap_ids
        }
        self.assertEqual(len(contrast_geometries), 2)
        self.assertEqual(len(pair_gap_geometries), 1)
        self.assertTrue(contrast_geometries.isdisjoint(pair_gap_geometries))
        self.assertEqual(
            set(contrast_ids).union(pair_gap_ids),
            {
                "curve_standard_left",
                "curve_standard_right",
                "curve_sharp_left",
                "curve_sharp_right",
                "curve_gentle_left",
                "curve_gentle_right",
            },
        )
        self.assertEqual(set(review_pack_ids), set(contrast_ids).union(pair_gap_ids))
        self.assertEqual(set(focus_pack_ids), {"curve_standard_left", "curve_standard_right"})
        self.assertEqual(seed_pack_ids, ["curve_standard_left"])
        self.assertEqual(set(history_gap_ids), {"curve_standard_left", "curve_standard_right"})

    def test_load_route_corpus_prefers_shape_diverse_curve_candidate_queue(self) -> None:
        payload = {
            "routes": [
                {
                    "route_id": "curve_standard_left_a",
                    "spawn_idx": 1,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 45.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_standard_left_b",
                    "spawn_idx": 2,
                    "route_length_m": 242.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 45.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_standard_right_a",
                    "spawn_idx": 3,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -45.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_standard_right_b",
                    "spawn_idx": 4,
                    "route_length_m": 242.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -45.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_sharp_left_a",
                    "spawn_idx": 5,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 60.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_sharp_left_b",
                    "spawn_idx": 6,
                    "route_length_m": 242.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 60.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_sharp_right_a",
                    "spawn_idx": 7,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -60.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_sharp_right_b",
                    "spawn_idx": 8,
                    "route_length_m": 242.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -60.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_gentle_left",
                    "spawn_idx": 9,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 25.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_gentle_right",
                    "spawn_idx": 10,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -25.0},
                    "health_tags": [],
                    "recommended_uses": [],
                },
            ],
            "recommended_subsets": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "corpus.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            corpus = load_route_corpus(path)
        subsets = corpus["recommended_subsets"]
        candidate_ids = subsets["curve_lane_follow_candidate"]
        first_wave_ids = subsets["curve_lane_follow_first_wave_smoke"]
        route_by_id = {route["route_id"]: route for route in corpus["routes"]}
        candidate_shapes = {
            (
                route_by_id[route_id]["curve_lane_follow_geometry_class"],
                route_by_id[route_id]["curve_lane_follow_direction_class"],
            )
            for route_id in candidate_ids
        }
        first_wave_shapes = {
            (
                route_by_id[route_id]["curve_lane_follow_geometry_class"],
                route_by_id[route_id]["curve_lane_follow_direction_class"],
            )
            for route_id in first_wave_ids
        }
        self.assertEqual(len(candidate_ids), 8)
        self.assertIn(("gentle_bend", "left_bend"), candidate_shapes)
        self.assertIn(("gentle_bend", "right_bend"), candidate_shapes)
        self.assertGreaterEqual(len(candidate_shapes), 6)
        self.assertTrue(set(first_wave_ids).issubset(set(candidate_ids)))
        self.assertEqual(len(first_wave_ids), 4)
        self.assertEqual(len(first_wave_shapes), 4)

    def test_load_route_corpus_prefers_semantic_history_anchor_for_seed_and_proxy_pack(self) -> None:
        payload = {
            "routes": [
                {
                    "route_id": "curve_left_semantic",
                    "spawn_idx": 1,
                    "route_length_m": 240.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 45.0},
                    "health_tags": ["empirically_reviewed", "curve_lane_follow_semantic_history"],
                    "recommended_uses": [],
                },
                {
                    "route_id": "curve_right_semantic",
                    "spawn_idx": 2,
                    "route_length_m": 245.0,
                    "road_transition_count": 1,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": -45.0},
                    "health_tags": ["empirically_reviewed", "curve_lane_follow_semantic_history"],
                    "recommended_uses": [],
                },
                {
                    "route_id": "junction_reviewed_unrelated",
                    "spawn_idx": 3,
                    "route_length_m": 250.0,
                    "road_transition_count": 3,
                    "lane_transition_count": 1,
                    "spawn_pose": {"yaw_deg": 0.0},
                    "goal_pose": {"yaw_deg": 90.0},
                    "health_tags": ["empirically_reviewed", "route_health_candidate"],
                    "recommended_uses": [],
                },
            ],
            "empirical_health_summary": {
                "curve_left_semantic": {
                    "curve_lane_follow_semantic_best_label": "route_established_but_behavior_unhealthy",
                    "curve_lane_follow_semantic_best_completion": 0.36,
                    "curve_lane_follow_semantic_best_comparison_label": "curve_left_hist",
                },
                "curve_right_semantic": {
                    "curve_lane_follow_semantic_best_label": "route_established_but_behavior_unhealthy",
                    "curve_lane_follow_semantic_best_completion": 0.44,
                    "curve_lane_follow_semantic_best_comparison_label": "curve_right_hist",
                },
            },
            "recommended_subsets": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "corpus.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            corpus = load_route_corpus(path)
        subsets = corpus["recommended_subsets"]
        self.assertEqual(subsets["curve_lane_follow_seed_pack"], ["curve_right_semantic"])
        self.assertEqual(
            subsets["curve_lane_follow_proxy_pack"],
            ["curve_right_semantic", "curve_left_semantic"],
        )
        self.assertEqual(subsets["curve_lane_follow_history_gap_queue"], [])
        self.assertNotIn("junction_reviewed_unrelated", subsets["curve_lane_follow_proxy_pack"])

    def test_refresh_overlay_marks_recoverable_fallback_pressure_route(self) -> None:
        route = {
            "route_id": "pressure_lat",
            "recommended_uses": ["mainline_regression", "lateral_smoke_candidate"],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.34,
            "best_lateral_distance_m": 80.1,
            "has_persistent_path_fallback": False,
            "has_recoverable_fallback_pressure": True,
            "has_guarded_lateral_runtime_ok": False,
            "has_route_health_candidate": False,
            "has_route_health_pass": False,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("empirically_reviewed", updated["health_tags"])
        self.assertIn("recoverable_fallback_pressure", updated["health_tags"])
        self.assertIn("deprioritized_lateral_smoke", updated["health_tags"])
        self.assertNotIn("lateral_smoke_candidate", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_first_wave_smoke", updated["recommended_uses"])

    def test_refresh_overlay_marks_relapse_heavy_route(self) -> None:
        route = {
            "route_id": "relapse_lat",
            "recommended_uses": ["mainline_regression", "lateral_smoke_candidate", "guarded_lateral_first_wave_smoke"],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.34,
            "best_lateral_distance_m": 80.1,
            "has_persistent_path_fallback": False,
            "has_recoverable_fallback_pressure": False,
            "has_relapse_heavy_after_recovery": True,
            "has_guarded_lateral_runtime_ok": False,
            "has_route_health_candidate": False,
            "has_route_health_pass": False,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("empirically_reviewed", updated["health_tags"])
        self.assertIn("relapse_heavy_after_recovery", updated["health_tags"])
        self.assertIn("deprioritized_lateral_smoke", updated["health_tags"])
        self.assertNotIn("lateral_smoke_candidate", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_first_wave_smoke", updated["recommended_uses"])

    def test_refresh_overlay_promotes_guarded_lateral_first_wave_smoke(self) -> None:
        route = {
            "route_id": "good_lat",
            "recommended_uses": ["mainline_regression", "lateral_smoke_candidate"],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.62,
            "best_lateral_distance_m": 142.6,
            "has_persistent_path_fallback": False,
            "has_recoverable_fallback_pressure": False,
            "has_guarded_lateral_runtime_ok": True,
            "has_route_health_candidate": True,
            "has_route_health_pass": False,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("empirically_reviewed", updated["health_tags"])
        self.assertIn("guarded_lateral_first_wave_smoke", updated["recommended_uses"])

    def test_refresh_overlay_promotes_repeat_verified_smoke(self) -> None:
        route = {
            "route_id": "great_lat",
            "recommended_uses": ["mainline_regression", "lateral_smoke_candidate"],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.97,
            "best_lateral_distance_m": 224.1,
            "has_persistent_path_fallback": False,
            "has_recoverable_fallback_pressure": False,
            "has_relapse_heavy_after_recovery": False,
            "has_guarded_lateral_runtime_ok": True,
            "has_guarded_lateral_repeat_verified": True,
            "has_route_health_candidate": True,
            "has_route_health_pass": True,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("guarded_lateral_repeat_verified", updated["health_tags"])
        self.assertIn("guarded_lateral_repeat_verified_smoke", updated["recommended_uses"])

    def test_refresh_overlay_marks_capability_semantic_history_tags(self) -> None:
        route = {
            "route_id": "cap_hist",
            "recommended_uses": ["lane_keep_candidate", "curve_lane_follow_candidate"],
            "health_tags": [],
        }
        evidence = {
            "has_lane_keep_semantic_history": True,
            "has_curve_lane_follow_semantic_history": True,
            "has_junction_traverse_semantic_history": False,
            "has_traffic_light_semantic_history": True,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("lane_keep_semantic_history", updated["health_tags"])
        self.assertIn("curve_lane_follow_semantic_history", updated["health_tags"])
        self.assertIn("traffic_light_semantic_history", updated["health_tags"])
        self.assertNotIn("junction_traverse_semantic_history", updated["health_tags"])

    def test_refresh_overlay_marks_repeat_instability_risk(self) -> None:
        route = {
            "route_id": "repeat_unstable",
            "recommended_uses": [
                "mainline_regression",
                "lateral_smoke_candidate",
                "guarded_lateral_first_wave_smoke",
                "guarded_lateral_repeat_verified_smoke",
            ],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.72,
            "best_lateral_distance_m": 165.9,
            "has_persistent_path_fallback": False,
            "has_recoverable_fallback_pressure": False,
            "has_relapse_heavy_after_recovery": False,
            "has_repeat_instability_risk": True,
            "has_guarded_lateral_runtime_ok": True,
            "has_guarded_lateral_repeat_verified": False,
            "has_route_health_candidate": True,
            "has_route_health_pass": False,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("repeat_instability_risk", updated["health_tags"])
        self.assertIn("deprioritized_lateral_smoke", updated["health_tags"])
        self.assertNotIn("lateral_smoke_candidate", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_first_wave_smoke", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_repeat_verified_smoke", updated["recommended_uses"])

    def test_refresh_overlay_marks_current_time_smaller_precursor_risk(self) -> None:
        route = {
            "route_id": "timebase_risk",
            "recommended_uses": [
                "mainline_regression",
                "lateral_smoke_candidate",
                "guarded_lateral_first_wave_smoke",
                "guarded_lateral_repeat_verified_smoke",
            ],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.72,
            "best_lateral_distance_m": 165.9,
            "has_persistent_path_fallback": False,
            "has_recoverable_fallback_pressure": False,
            "has_relapse_heavy_after_recovery": False,
            "has_repeat_instability_risk": False,
            "has_current_time_smaller_precursor_risk": True,
            "has_guarded_lateral_runtime_ok": True,
            "has_guarded_lateral_repeat_verified": False,
            "has_route_health_candidate": True,
            "has_route_health_pass": False,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("current_time_smaller_precursor_risk", updated["health_tags"])
        self.assertIn("deprioritized_lateral_smoke", updated["health_tags"])
        self.assertNotIn("lateral_smoke_candidate", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_first_wave_smoke", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_repeat_verified_smoke", updated["recommended_uses"])

    def test_refresh_overlay_marks_reroute_propagation_frame_bridge_risk(self) -> None:
        route = {
            "route_id": "reroute_bridge_risk",
            "recommended_uses": [
                "mainline_regression",
                "lateral_smoke_candidate",
                "guarded_lateral_first_wave_smoke",
                "guarded_lateral_repeat_verified_smoke",
            ],
            "health_tags": [],
        }
        evidence = {
            "best_lateral_completion": 0.72,
            "best_lateral_distance_m": 165.9,
            "has_persistent_path_fallback": False,
            "has_recoverable_fallback_pressure": False,
            "has_relapse_heavy_after_recovery": False,
            "has_repeat_instability_risk": False,
            "has_current_time_smaller_precursor_risk": False,
            "has_reference_line_provider_bridge_risk": False,
            "has_reroute_propagation_frame_bridge_risk": True,
            "has_guarded_lateral_runtime_ok": True,
            "has_guarded_lateral_repeat_verified": False,
            "has_route_health_candidate": True,
            "has_route_health_pass": False,
        }
        updated = _updated_route(route, evidence)
        self.assertIn("reroute_propagation_frame_bridge_risk", updated["health_tags"])
        self.assertIn("deprioritized_lateral_smoke", updated["health_tags"])
        self.assertNotIn("lateral_smoke_candidate", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_first_wave_smoke", updated["recommended_uses"])
        self.assertNotIn("guarded_lateral_repeat_verified_smoke", updated["recommended_uses"])

    def test_refresh_overlay_preserves_existing_empirical_labels_without_new_evidence(self) -> None:
        route = {
            "route_id": "persist_bad",
            "recommended_uses": ["mainline_regression", "calibration_compare_candidate"],
            "health_tags": ["empirically_reviewed", "persistent_path_fallback_risk", "deprioritized_lateral_smoke"],
        }
        updated = _updated_route(route, None)
        self.assertEqual(set(updated["health_tags"]), set(route["health_tags"]))
        self.assertEqual(updated["recommended_uses"], route["recommended_uses"])

    def test_publish_outputs_can_stay_batch_local(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            batch_root = tmp_root / "runs" / "batch1"
            batch_root.mkdir(parents=True, exist_ok=True)
            rows = [
                {
                    "run_dir": str(batch_root / "mainline__town01_rh_spawn097_goal046__02"),
                    "route_id": "town01_rh_spawn097_goal046",
                    "comparison_label": "mainline",
                    "summary_status": "finalized",
                    "profile_name": "test",
                    "route_health_label": "route_health_pass",
                    "failure_stage": "",
                    "planning_nonzero_ratio": 0.9,
                    "control_used_planning_ratio": 0.99,
                    "route_completion_ratio": 0.75,
                    "route_distance_achieved_m": 170.0,
                    "final_goal_distance_m": 57.0,
                    "path_fallback_count": 0,
                    "max_consecutive_path_fallback_length": 0,
                    "final_normal_tail_length": 2000,
                    "persistent_path_fallback_at_end": False,
                    "guard_trigger_ratio": 0.0,
                    "raw_steer_saturated_ratio": 0.0,
                }
            ]
            with mock.patch("tools.run_town01_route_health.REPO_ROOT", tmp_root):
                _publish_outputs(batch_root, rows, publish_repo=False)
            self.assertTrue((batch_root / "artifacts" / "town01_route_health_platform_comparison.csv").exists())
            self.assertFalse((tmp_root / "artifacts" / "town01_route_health_platform_comparison.csv").exists())

    def test_platform_report_excludes_stage6_experimental_rows_from_answers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            normal_dir = tmp_root / "mainline_run"
            experimental_dir = tmp_root / "stage6_probe_run"
            for run_dir in (normal_dir, experimental_dir):
                _write_json(
                    run_dir / "summary.json",
                    {
                        "acceptance": {"failure_codes": [], "checks": {"lateral_metrics": {}}},
                        "planning_control_alignment": {"live_control_stops_before_first_nonzero_planning": False},
                        "planning_trajectory_type_summary": {
                            "persistent_path_fallback_at_end": False,
                            "recovered_after_path_fallback": False,
                        },
                        "state_machine": {"states_reached": ["CRUISE_ACTIVE"]},
                        "runtime_mode": {"enable_lateral": True},
                    },
                )
            report_path = tmp_root / "platform_report.md"
            write_platform_report(
                [
                    {
                        "route_id": "mainline_route",
                        "comparison_label": "guarded_smoke",
                        "run_dir": str(normal_dir),
                        "route_completion_ratio": "0.55",
                        "route_distance_achieved_m": "120.0",
                        "control_used_planning_ratio": "0.99",
                        "raw_steer_saturated_ratio": "0.0",
                        "guard_trigger_ratio": "0.0",
                        "persistent_path_fallback_at_end": "False",
                        "success": True,
                        "finalized_from_event_stream": True,
                        "summary_written_successfully": True,
                        "enable_lateral": True,
                        "enable_guard": True,
                    },
                    {
                        "route_id": "experimental_route",
                        "comparison_label": "guarded_lateral_stage6_cache_clear_probe",
                        "run_dir": str(experimental_dir),
                        "route_completion_ratio": "0.95",
                        "route_distance_achieved_m": "220.0",
                        "control_used_planning_ratio": "0.99",
                        "raw_steer_saturated_ratio": "0.0",
                        "guard_trigger_ratio": "0.0",
                        "persistent_path_fallback_at_end": "False",
                        "success": True,
                        "finalized_from_event_stream": True,
                        "summary_written_successfully": True,
                        "enable_lateral": True,
                        "enable_guard": True,
                        "stage6_clear_lane_follow_cache_on_new_command": True,
                        "effective_stage6_clear_lane_follow_cache_on_new_command": True,
                    },
                ],
                report_path,
            )
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("- run_count: `1`", report_text)
            self.assertIn("- experimental_probe_rows_excluded_from_answers: `1`", report_text)
            self.assertIn("route=`mainline_route`", report_text)
            self.assertNotIn("route=`experimental_route`", report_text)

    def test_empirical_guarded_lateral_runtime_ok_requires_completion_floor(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "low_completion",
                    "comparison_label": "guarded",
                    "route_completion_ratio": "0.34",
                    "route_distance_achieved_m": "80.1",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.0",
                    "guard_trigger_ratio": "0.0",
                    "route_health_label": "route_established_but_behavior_unhealthy",
                },
                {
                    "route_id": "good_completion",
                    "comparison_label": "guarded",
                    "route_completion_ratio": "0.62",
                    "route_distance_achieved_m": "142.6",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.0",
                    "guard_trigger_ratio": "0.0",
                    "route_health_label": "route_health_candidate",
                },
            ]
        )
        self.assertFalse(evidence["low_completion"]["has_guarded_lateral_runtime_ok"])
        self.assertTrue(evidence["good_completion"]["has_guarded_lateral_runtime_ok"])

    def test_empirical_recoverable_fallback_pressure_requires_large_fallback_and_low_completion(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "pressure",
                    "comparison_label": "guarded",
                    "route_completion_ratio": "0.34",
                    "route_distance_achieved_m": "80.1",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "path_fallback_count": "529",
                    "route_health_label": "route_established_but_behavior_unhealthy",
                },
                {
                    "route_id": "recovers_well",
                    "comparison_label": "guarded",
                    "route_completion_ratio": "0.62",
                    "route_distance_achieved_m": "142.6",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "path_fallback_count": "94",
                    "route_health_label": "route_health_candidate",
                },
            ]
        )
        self.assertTrue(evidence["pressure"]["has_recoverable_fallback_pressure"])
        self.assertEqual(evidence["pressure"]["max_lateral_path_fallback_count"], 529)
        self.assertFalse(evidence["recovers_well"]["has_recoverable_fallback_pressure"])

    def test_empirical_relapse_heavy_after_recovery_requires_long_relapse_and_short_tail(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "relapse_heavy",
                    "comparison_label": "guarded",
                    "route_completion_ratio": "0.34",
                    "route_distance_achieved_m": "80.1",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "path_fallback_count_after_first_recovery": "528",
                    "first_relapse_cluster_length": "31",
                    "final_normal_tail_length": "2",
                },
                {
                    "route_id": "stable_after_relapse",
                    "comparison_label": "guarded",
                    "route_completion_ratio": "0.65",
                    "route_distance_achieved_m": "150.3",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "path_fallback_count_after_first_recovery": "6",
                    "first_relapse_cluster_length": "4",
                    "final_normal_tail_length": "1749",
                },
            ]
        )
        self.assertTrue(evidence["relapse_heavy"]["has_relapse_heavy_after_recovery"])
        self.assertFalse(evidence["stable_after_relapse"]["has_relapse_heavy_after_recovery"])

    def test_empirical_repeat_verified_requires_two_strong_guarded_labels(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "repeat_ok",
                    "comparison_label": "guarded_a",
                    "route_health_label": "route_health_pass",
                    "route_completion_ratio": "0.80",
                    "route_distance_achieved_m": "172.9",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.00",
                    "guard_trigger_ratio": "0.00",
                    "path_fallback_count_after_first_recovery": "0",
                    "final_normal_tail_length": "2400",
                },
                {
                    "route_id": "repeat_ok",
                    "comparison_label": "guarded_b",
                    "route_health_label": "route_health_pass",
                    "route_completion_ratio": "0.97",
                    "route_distance_achieved_m": "224.1",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.00",
                    "guard_trigger_ratio": "0.00",
                    "path_fallback_count_after_first_recovery": "0",
                    "final_normal_tail_length": "2500",
                },
                {
                    "route_id": "single_ok",
                    "comparison_label": "guarded_only",
                    "route_health_label": "route_health_pass",
                    "route_completion_ratio": "0.96",
                    "route_distance_achieved_m": "220.0",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.00",
                    "guard_trigger_ratio": "0.00",
                    "path_fallback_count_after_first_recovery": "0",
                    "final_normal_tail_length": "2300",
                },
            ]
        )
        self.assertTrue(evidence["repeat_ok"]["has_guarded_lateral_repeat_verified"])
        self.assertEqual(sorted(evidence["repeat_ok"]["guarded_repeat_ok_labels"]), ["guarded_a", "guarded_b"])
        self.assertFalse(evidence["single_ok"]["has_guarded_lateral_repeat_verified"])

    def test_empirical_repeat_instability_requires_good_and_bad_distinct_labels(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "repeat_unstable",
                    "comparison_label": "guarded_smoke",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": "0.65",
                    "route_distance_achieved_m": "150.3",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.00",
                    "guard_trigger_ratio": "0.00",
                    "path_fallback_count_after_first_recovery": "6",
                    "final_normal_tail_length": "1749",
                },
                {
                    "route_id": "repeat_unstable",
                    "comparison_label": "guarded_repeat",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": "0.72",
                    "route_distance_achieved_m": "165.9",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "True",
                    "path_fallback_count_after_first_recovery": "367",
                    "final_normal_tail_length": "0",
                },
                {
                    "route_id": "single_bad",
                    "comparison_label": "guarded_only",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": "0.72",
                    "route_distance_achieved_m": "165.9",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "True",
                    "path_fallback_count_after_first_recovery": "367",
                    "final_normal_tail_length": "0",
                },
            ]
        )
        self.assertTrue(evidence["repeat_unstable"]["has_repeat_instability_risk"])
        self.assertEqual(sorted(evidence["repeat_unstable"]["guarded_runtime_ok_labels"]), ["guarded_smoke"])
        self.assertEqual(sorted(evidence["repeat_unstable"]["guarded_unstable_labels"]), ["guarded_repeat"])
        self.assertFalse(evidence["single_bad"]["has_repeat_instability_risk"])

    def test_empirical_current_time_smaller_precursor_risk_requires_positive_precursor_window(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "precursor_risk",
                    "comparison_label": "guarded_repeat",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": "0.72",
                    "route_distance_achieved_m": "165.9",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "True",
                    "first_path_fallback_precurrent_time_smaller_normal_count": "10",
                    "first_path_fallback_precurrent_time_smaller_rel_min_max": "0.07984504699707032",
                },
                {
                    "route_id": "not_precursor_risk",
                    "comparison_label": "guarded_smoke",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": "0.65",
                    "route_distance_achieved_m": "150.3",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "first_path_fallback_precurrent_time_smaller_normal_count": "1",
                    "first_path_fallback_precurrent_time_smaller_rel_min_max": "-1.33",
                },
            ]
        )
        self.assertTrue(evidence["precursor_risk"]["has_current_time_smaller_precursor_risk"])
        self.assertFalse(evidence["not_precursor_risk"]["has_current_time_smaller_precursor_risk"])

    def test_empirical_reroute_propagation_frame_bridge_risk_requires_explicit_bridge_flag(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "bridge_risk",
                    "comparison_label": "guarded_repeat",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": "0.72",
                    "route_distance_achieved_m": "165.9",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "first_path_fallback_bridge_occurs_on_reroute_propagation_frame": "True",
                },
                {
                    "route_id": "not_bridge_risk",
                    "comparison_label": "guarded_smoke",
                    "route_health_label": "route_health_candidate",
                    "route_completion_ratio": "0.65",
                    "route_distance_achieved_m": "150.3",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "first_path_fallback_bridge_occurs_on_reroute_propagation_frame": "False",
                },
            ]
        )
        self.assertTrue(evidence["bridge_risk"]["has_reroute_propagation_frame_bridge_risk"])
        self.assertFalse(evidence["not_bridge_risk"]["has_reroute_propagation_frame_bridge_risk"])

    def test_empirical_evidence_skips_stage6_experimental_rows_by_default(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "stage6_probe_route",
                    "comparison_label": "stage6_probe",
                    "route_completion_ratio": "0.71",
                    "route_distance_achieved_m": "163.5",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.0",
                    "guard_trigger_ratio": "0.0",
                    "stage6_clear_lane_follow_cache_on_new_command": "True",
                    "effective_stage6_clear_lane_follow_cache_on_new_command": "True",
                    "route_health_label": "route_health_candidate",
                }
            ]
        )
        self.assertEqual(evidence, {})

    def test_empirical_evidence_can_include_stage6_experimental_rows_explicitly(self) -> None:
        evidence = _aggregate_route_evidence(
            [
                {
                    "route_id": "stage6_probe_route",
                    "comparison_label": "stage6_probe",
                    "route_completion_ratio": "0.71",
                    "route_distance_achieved_m": "163.5",
                    "enable_lateral": "True",
                    "enable_guard": "True",
                    "persistent_path_fallback_at_end": "False",
                    "control_used_planning_ratio": "0.99",
                    "raw_steer_saturated_ratio": "0.0",
                    "guard_trigger_ratio": "0.0",
                    "stage6_clear_lane_follow_cache_on_new_command": "True",
                    "effective_stage6_clear_lane_follow_cache_on_new_command": "True",
                    "route_health_label": "route_health_candidate",
                }
            ],
            include_stage6_experimental=True,
        )
        self.assertTrue(evidence["stage6_probe_route"]["has_guarded_lateral_runtime_ok"])

    def test_planning_trajectory_type_summary_tracks_final_path_fallback_suffix(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {"planning_header_sequence_num": 1, "trajectory_type": "NORMAL", "timestamp": 1.0, "trajectory_point_count": 111},
                {"planning_header_sequence_num": 2, "trajectory_type": "PATH_FALLBACK", "timestamp": 2.0, "trajectory_point_count": 69},
                {"planning_header_sequence_num": 3, "trajectory_type": "NORMAL", "timestamp": 3.0, "trajectory_point_count": 118},
                {"planning_header_sequence_num": 4, "trajectory_type": "PATH_FALLBACK", "timestamp": 4.0, "trajectory_point_count": 69},
                {"planning_header_sequence_num": 5, "trajectory_type": "PATH_FALLBACK", "timestamp": 5.0, "trajectory_point_count": 69},
            ]
        )
        self.assertEqual(summary["first_path_fallback_seq"], 2)
        self.assertTrue(summary["recovered_after_path_fallback"])
        self.assertTrue(summary["persistent_path_fallback_at_end"])
        self.assertEqual(summary["final_path_fallback_suffix_start_seq"], 4)
        self.assertEqual(summary["final_path_fallback_suffix_length"], 2)
        self.assertEqual(summary["last_normal_before_final_path_fallback_seq"], 3)
        self.assertEqual(summary["first_recovery_after_path_fallback_seq"], 3)
        self.assertEqual(summary["first_path_fallback_cluster_length"], 1)
        self.assertEqual(summary["max_consecutive_path_fallback_length"], 2)
        self.assertEqual(summary["final_normal_tail_length"], 0)

    def test_planning_trajectory_type_summary_tracks_recovery_and_final_normal_tail(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {"planning_header_sequence_num": 10, "trajectory_type": "NORMAL", "timestamp": 10.0, "trajectory_point_count": 111},
                {"planning_header_sequence_num": 11, "trajectory_type": "PATH_FALLBACK", "timestamp": 11.0, "trajectory_point_count": 69},
                {"planning_header_sequence_num": 12, "trajectory_type": "NORMAL", "timestamp": 12.0, "trajectory_point_count": 118},
                {"planning_header_sequence_num": 13, "trajectory_type": "NORMAL", "timestamp": 13.0, "trajectory_point_count": 117},
            ]
        )
        self.assertEqual(summary["path_fallback_count"], 1)
        self.assertEqual(summary["first_recovery_after_path_fallback_seq"], 12)
        self.assertEqual(summary["first_path_fallback_cluster_length"], 1)
        self.assertEqual(summary["max_consecutive_path_fallback_length"], 1)
        self.assertEqual(summary["final_path_fallback_suffix_length"], 0)
        self.assertEqual(summary["final_normal_tail_length"], 2)
        self.assertFalse(summary["persistent_path_fallback_at_end"])
        self.assertTrue(summary["recovered_after_path_fallback"])
        self.assertFalse(summary["reentered_path_fallback_after_recovery"])
        self.assertEqual(summary["path_fallback_count_after_first_recovery"], 0)

    def test_planning_trajectory_type_summary_tracks_relapse_after_recovery(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {"planning_header_sequence_num": 20, "trajectory_type": "NORMAL", "timestamp": 20.0, "trajectory_point_count": 111, "trajectory_total_path_length": 40.0, "first_trajectory_point_v": 4.0},
                {"planning_header_sequence_num": 21, "trajectory_type": "PATH_FALLBACK", "timestamp": 21.0, "trajectory_point_count": 69, "trajectory_total_path_length": 5.0, "first_trajectory_point_v": 4.2},
                {"planning_header_sequence_num": 22, "trajectory_type": "NORMAL", "timestamp": 22.0, "trajectory_point_count": 118, "trajectory_total_path_length": 36.0, "first_trajectory_point_v": 4.1},
                {"planning_header_sequence_num": 23, "trajectory_type": "PATH_FALLBACK", "timestamp": 23.0, "trajectory_point_count": 69, "trajectory_total_path_length": 3.6, "first_trajectory_point_v": 4.3, "replan_reason": "the distance between matched point and actual position is too large. Replan is triggered. lon_diff = 2.50"},
                {"planning_header_sequence_num": 24, "trajectory_type": "PATH_FALLBACK", "timestamp": 24.0, "trajectory_point_count": 69, "trajectory_total_path_length": 3.5, "first_trajectory_point_v": 4.2},
                {"planning_header_sequence_num": 25, "trajectory_type": "NORMAL", "timestamp": 25.0, "trajectory_point_count": 117, "trajectory_total_path_length": 34.0, "first_trajectory_point_v": 4.0},
            ]
        )
        self.assertEqual(summary["first_path_fallback_prev_normal_seq"], 20)
        self.assertAlmostEqual(summary["first_path_fallback_prev_normal_first_point_v"], 4.0)
        self.assertAlmostEqual(summary["first_path_fallback_prev_normal_total_path_length"], 40.0)
        self.assertAlmostEqual(summary["first_path_fallback_first_point_v"], 4.2)
        self.assertAlmostEqual(summary["first_path_fallback_total_path_length"], 5.0)
        self.assertEqual(summary["first_path_fallback_cluster_length"], 1)
        self.assertAlmostEqual(summary["first_path_fallback_path_length_collapse_ratio"], 0.125)
        self.assertIsNone(summary["first_path_fallback_trigger_seq"])
        self.assertIsNone(summary["first_path_fallback_trigger_seq_gap"])
        self.assertIsNone(summary["first_path_fallback_trigger_first_point_v"])
        self.assertIsNone(summary["first_path_fallback_trigger_total_path_length"])
        self.assertIsNone(summary["first_path_fallback_trigger_lon_diff"])
        self.assertEqual(summary["first_recovery_after_path_fallback_seq"], 22)
        self.assertEqual(summary["first_relapse_after_recovery_seq"], 23)
        self.assertEqual(summary["first_relapse_cluster_length"], 2)
        self.assertAlmostEqual(summary["first_relapse_path_length_collapse_ratio"], 0.1)
        self.assertAlmostEqual(summary["first_relapse_after_recovery_lon_diff"], 2.5)
        self.assertAlmostEqual(summary["first_relapse_prev_normal_first_point_v"], 4.1)
        self.assertAlmostEqual(summary["first_relapse_prev_normal_total_path_length"], 36.0)
        self.assertAlmostEqual(summary["first_relapse_after_recovery_first_point_v"], 4.3)
        self.assertAlmostEqual(summary["first_relapse_after_recovery_total_path_length"], 3.6)
        self.assertTrue(summary["reentered_path_fallback_after_recovery"])
        self.assertEqual(summary["path_fallback_count_after_first_recovery"], 2)
        self.assertEqual(summary["max_consecutive_path_fallback_after_first_recovery"], 2)
        self.assertEqual(summary["normal_count_after_first_recovery"], 2)
        self.assertEqual(summary["final_normal_tail_length"], 1)

    def test_planning_trajectory_type_summary_uses_recent_trigger_context_for_first_fallback(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {"planning_header_sequence_num": 30, "trajectory_type": "NORMAL", "timestamp": 30.0, "trajectory_point_count": 111, "trajectory_total_path_length": 22.0, "first_trajectory_point_v": 1.4, "trajectory_relative_time_min_sec": -0.15},
                {"planning_header_sequence_num": 31, "trajectory_type": "NORMAL", "timestamp": 31.0, "trajectory_point_count": 111, "trajectory_total_path_length": 22.3, "first_trajectory_point_v": 1.35, "trajectory_relative_time_min_sec": 0.08, "replan_reason": "replan for current time smaller than the previous trajectory's first time."},
                {"planning_header_sequence_num": 32, "trajectory_type": "PATH_FALLBACK", "timestamp": 32.0, "trajectory_point_count": 69, "trajectory_total_path_length": 3.0, "first_trajectory_point_v": 1.2, "trajectory_relative_time_min_sec": 0.06},
            ]
        )
        self.assertEqual(summary["first_path_fallback_seq"], 32)
        self.assertEqual(summary["first_path_fallback_trigger_seq"], 31)
        self.assertEqual(summary["first_path_fallback_trigger_seq_gap"], 1)
        self.assertEqual(summary["first_path_fallback_trigger_reason_family"], "current_time_smaller")
        self.assertEqual(summary["first_path_fallback_trigger_reason_streak_length"], 1)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_normal_count"], 1)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_normal_seq_start"], 31)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_normal_seq_end"], 31)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_rel_min_min"], 0.08)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_rel_min_max"], 0.08)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_first_point_v_min"], 1.35)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_first_point_v_max"], 1.35)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_point_count_min"], 111)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_point_count_max"], 111)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_path_length_min"], 22.3)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_path_length_max"], 22.3)
        self.assertEqual(
            summary["first_path_fallback_trigger_replan_reason"],
            "replan for current time smaller than the previous trajectory's first time.",
        )
        self.assertIsNone(summary["first_path_fallback_lon_diff"])
        self.assertIsNone(summary["first_path_fallback_trigger_lon_diff"])
        self.assertAlmostEqual(summary["first_path_fallback_trigger_relative_time_min_sec"], 0.08)
        self.assertAlmostEqual(summary["first_path_fallback_prev_normal_relative_time_min_sec"], 0.08)
        self.assertAlmostEqual(summary["first_path_fallback_trigger_first_point_v"], 1.35)
        self.assertAlmostEqual(summary["first_path_fallback_trigger_total_path_length"], 22.3)

    def test_compute_curve_tracking_health_summary_tracks_high_steer_before_matched_point_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            _write_jsonl(
                artifacts / "apollo_control_raw.jsonl",
                [
                    {
                        "ts_sec": 30.0,
                        "selected_steering_field": "steering_target",
                        "apollo_control_raw": {
                            "control_header_sequence_num": 10,
                            "debug_input_trajectory_header_sequence_num": 60,
                            "steering_target": 10.0,
                        },
                    },
                    {
                        "ts_sec": 31.0,
                        "selected_steering_field": "steering_target",
                        "apollo_control_raw": {
                            "control_header_sequence_num": 11,
                            "debug_input_trajectory_header_sequence_num": 72,
                            "steering_target": 96.0,
                        },
                    },
                    {
                        "ts_sec": 32.0,
                        "selected_steering_field": "steering_target",
                        "apollo_control_raw": {
                            "control_header_sequence_num": 12,
                            "debug_input_trajectory_header_sequence_num": 80,
                            "steering_target": 100.0,
                        },
                    },
                ],
            )
            _write_jsonl(
                artifacts / "control_decode_debug.jsonl",
                [
                    {
                        "ts_sec": 103.1,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.01,
                            "debug_simple_lat_heading_error_rad": 0.005,
                            "debug_simple_lat_target_point_kappa": 0.05,
                        },
                        "output_to_carla": {
                            "mapped_carla_steer_cmd": 0.02,
                            "target_front_wheel_angle_deg": 1.0,
                        },
                    },
                    {
                        "ts_sec": 103.2,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.08,
                            "debug_simple_lat_heading_error_rad": 0.04,
                            "debug_simple_lat_target_point_kappa": 0.15,
                        },
                        "output_to_carla": {
                            "mapped_carla_steer_cmd": 0.10,
                            "target_front_wheel_angle_deg": 6.0,
                        },
                    },
                    {
                        "ts_sec": 104.0,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.12,
                            "debug_simple_lat_heading_error_rad": 0.08,
                            "debug_simple_lat_target_point_kappa": 0.20,
                        },
                        "output_to_carla": {
                            "mapped_carla_steer_cmd": 0.12,
                            "target_front_wheel_angle_deg": 8.2,
                        },
                    },
                ],
            )

            summary = _compute_curve_tracking_health_summary(
                artifacts,
                {
                    "path_fallback_count": 3,
                    "persistent_path_fallback_at_end": True,
                    "path_fallback_count_after_first_recovery": 2,
                    "first_path_fallback_trigger_reason_family": "current_time_smaller",
                    "first_relapse_after_recovery_reason_family": "matched_point_too_large",
                    "first_relapse_after_recovery_seq": 75,
                },
            )

            self.assertAlmostEqual(summary["apollo_raw_steer_abs_p95"], 99.6)
            self.assertAlmostEqual(summary["apollo_raw_steer_abs_p99"], 99.92)
            self.assertAlmostEqual(summary["apollo_raw_steer_over95_ratio"], 2.0 / 3.0)
            self.assertEqual(summary["first_high_steer_seq"], 72)
            self.assertEqual(summary["first_matched_point_too_large_seq"], 75)
            self.assertTrue(summary["high_steer_before_first_matched_point_too_large"])
            self.assertAlmostEqual(summary["apollo_simple_lat_lateral_error_abs_p95"], 0.116)
            self.assertAlmostEqual(summary["apollo_simple_lat_lateral_error_abs_p99"], 0.1192)
            self.assertAlmostEqual(summary["apollo_simple_lat_heading_error_abs_p95"], 0.076)
            self.assertAlmostEqual(summary["apollo_simple_lat_heading_error_abs_p99"], 0.0792)
            self.assertAlmostEqual(summary["apollo_simple_lat_target_point_kappa_abs_p95"], 0.195)
            self.assertAlmostEqual(summary["apollo_simple_lat_target_point_kappa_abs_p99"], 0.199)
            self.assertAlmostEqual(summary["mapped_carla_steer_cmd_abs_p95"], 0.118)
            self.assertAlmostEqual(summary["mapped_carla_steer_cmd_abs_p99"], 0.1196)
            self.assertAlmostEqual(summary["target_front_wheel_angle_deg_abs_p95"], 7.98)
            self.assertAlmostEqual(summary["target_front_wheel_angle_deg_abs_p99"], 8.156)
            self.assertEqual(summary["path_fallback_count"], 3)
            self.assertTrue(summary["persistent_path_fallback_at_end"])
            self.assertEqual(summary["path_fallback_count_after_first_recovery"], 2)

    def test_compute_longitudinal_tracking_health_summary_tracks_speed_and_overlap(self) -> None:
        summary = _compute_longitudinal_tracking_health_summary(
            {
                "algo": {
                    "apollo": {
                        "routing": {"target_speed_mps": 8.8},
                        "planning": {"default_cruise_speed_mps": 18.0},
                    }
                }
            },
            [
                {
                    "speed_mps": 5.0,
                    "mapped_throttle_cmd": 0.40,
                    "mapped_brake_cmd": 0.0,
                    "apollo_acceleration_mps2": 0.6,
                },
                {
                    "speed_mps": 6.0,
                    "mapped_throttle_cmd": 1.0,
                    "mapped_brake_cmd": 0.10,
                    "apollo_acceleration_mps2": 1.1,
                },
                {
                    "speed_mps": 7.0,
                    "mapped_throttle_cmd": 0.70,
                    "mapped_brake_cmd": 0.20,
                    "apollo_acceleration_mps2": 1.5,
                },
            ],
        )

        self.assertAlmostEqual(summary["routing_target_speed_mps"], 8.8)
        self.assertAlmostEqual(summary["planning_default_cruise_speed_mps"], 18.0)
        self.assertAlmostEqual(summary["speed_abs_p95"], 6.9)
        self.assertAlmostEqual(summary["speed_ratio_to_target_p95"], 6.9 / 8.8)
        self.assertAlmostEqual(summary["throttle_p95"], 0.97)
        self.assertAlmostEqual(summary["brake_p95"], 0.19)
        self.assertAlmostEqual(summary["throttle_brake_overlap_ratio"], 2.0 / 3.0)
        self.assertAlmostEqual(summary["acc_cmd_p95"], 1.46)
        self.assertAlmostEqual(summary["speed_gain_limited_ratio"], 1.0 / 3.0)
        self.assertFalse(summary["persistent_low_speed_at_end"])

    def test_planning_trajectory_type_summary_counts_trigger_reason_streak(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {"planning_header_sequence_num": 40, "trajectory_type": "NORMAL", "timestamp": 40.0, "trajectory_point_count": 111, "trajectory_total_path_length": 18.0, "first_trajectory_point_v": 1.0, "trajectory_relative_time_min_sec": 0.05, "replan_reason": "replan for current time smaller than the previous trajectory's first time."},
                {"planning_header_sequence_num": 41, "trajectory_type": "NORMAL", "timestamp": 41.0, "trajectory_point_count": 111, "trajectory_total_path_length": 18.1, "first_trajectory_point_v": 1.0, "trajectory_relative_time_min_sec": 0.06, "replan_reason": "replan for current time smaller than the previous trajectory's first time."},
                {"planning_header_sequence_num": 42, "trajectory_type": "PATH_FALLBACK", "timestamp": 42.0, "trajectory_point_count": 69, "trajectory_total_path_length": 3.2, "first_trajectory_point_v": 1.2, "trajectory_relative_time_min_sec": 0.07, "replan_reason": "replan for current time smaller than the previous trajectory's first time."},
            ]
        )
        self.assertEqual(summary["first_path_fallback_trigger_seq"], 42)
        self.assertEqual(summary["first_path_fallback_trigger_reason_family"], "current_time_smaller")
        self.assertEqual(summary["first_path_fallback_trigger_reason_streak_length"], 3)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_normal_count"], 2)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_normal_seq_start"], 40)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_normal_seq_end"], 41)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_rel_min_min"], 0.05)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_rel_min_max"], 0.06)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_first_point_v_min"], 1.0)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_first_point_v_max"], 1.0)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_point_count_min"], 111)
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_point_count_max"], 111)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_path_length_min"], 18.0)
        self.assertAlmostEqual(summary["first_path_fallback_precurrent_time_smaller_path_length_max"], 18.1)

    def test_planning_trajectory_type_summary_has_no_precursor_window_for_non_current_time_smaller_fallback(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {"planning_header_sequence_num": 50, "trajectory_type": "NORMAL", "timestamp": 50.0, "trajectory_point_count": 111, "trajectory_total_path_length": 29.0, "first_trajectory_point_v": 2.3, "trajectory_relative_time_min_sec": -1.12},
                {"planning_header_sequence_num": 51, "trajectory_type": "NORMAL", "timestamp": 51.0, "trajectory_point_count": 111, "trajectory_total_path_length": 30.1, "first_trajectory_point_v": 2.4, "trajectory_relative_time_min_sec": -0.98},
                {"planning_header_sequence_num": 52, "trajectory_type": "PATH_FALLBACK", "timestamp": 52.0, "trajectory_point_count": 69, "trajectory_total_path_length": 1.4, "first_trajectory_point_v": 2.6, "trajectory_relative_time_min_sec": 0.07, "replan_reason": "the distance between matched point and actual position is too large. Replan is triggered. lon_diff = 2.64"},
            ]
        )
        self.assertEqual(summary["first_path_fallback_trigger_reason_family"], "matched_point_too_large")
        self.assertEqual(summary["first_path_fallback_precurrent_time_smaller_normal_count"], 0)
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_normal_seq_start"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_normal_seq_end"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_rel_min_min"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_rel_min_max"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_first_point_v_min"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_first_point_v_max"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_point_count_min"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_point_count_max"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_path_length_min"])
        self.assertIsNone(summary["first_path_fallback_precurrent_time_smaller_path_length_max"])

    def test_planning_trajectory_type_summary_tracks_unknown_empty_previous_bridge(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {
                    "planning_header_sequence_num": 369,
                    "trajectory_type": "NORMAL",
                    "timestamp": 369.0,
                    "is_replan": True,
                    "replan_reason": "replan for current time smaller than the previous trajectory's first time.",
                    "trajectory_point_count": 118,
                    "trajectory_total_path_length": 30.0,
                    "trajectory_total_time": 7.1,
                    "first_trajectory_point_v": 1.42,
                    "routing_segment_count": 1,
                    "reference_line_count": 0,
                },
                {
                    "planning_header_sequence_num": 370,
                    "trajectory_type": "UNKNOWN",
                    "timestamp": 370.0,
                    "is_replan": False,
                    "trajectory_point_count": 0,
                    "trajectory_total_path_length": 0.0,
                    "trajectory_total_time": 0.0,
                    "routing_segment_count": 25,
                    "reference_line_count": 0,
                },
                {
                    "planning_header_sequence_num": 371,
                    "trajectory_type": "NORMAL",
                    "timestamp": 371.0,
                    "is_replan": True,
                    "trajectory_point_count": 111,
                    "trajectory_total_path_length": 20.443340653108876,
                    "trajectory_total_time": 7.1,
                    "first_trajectory_point_v": 1.068,
                    "replan_reason": "replan for empty previous trajectory.",
                    "routing_segment_count": 25,
                    "reference_line_count": 0,
                },
                {
                    "planning_header_sequence_num": 372,
                    "trajectory_type": "NORMAL",
                    "timestamp": 372.0,
                    "trajectory_point_count": 111,
                    "trajectory_total_path_length": 20.44344794345958,
                    "first_trajectory_point_v": 1.068,
                    "trajectory_relative_time_min_sec": 0.0712,
                    "replan_reason": "replan for current time smaller than the previous trajectory's first time.",
                },
                {
                    "planning_header_sequence_num": 381,
                    "trajectory_type": "PATH_FALLBACK",
                    "timestamp": 381.0,
                    "trajectory_point_count": 69,
                    "trajectory_total_path_length": 2.97906123077045,
                    "first_trajectory_point_v": 1.248,
                    "trajectory_relative_time_min_sec": 0.0654,
                    "replan_reason": "replan for current time smaller than the previous trajectory's first time.",
                },
            ],
            route_debug_rows=[
                {
                    "planning_header_sequence_num": 369,
                    "planning_header_timestamp_sec": 1774492625.451,
                    "route_segment_count": 1,
                    "route_segment_total_length": 30.000000195004887,
                    "create_route_segments_status": "ready",
                    "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
                    "lane_follow_map_inconsistent": False,
                    "reference_line_provider_status": "trajectory_nonzero_debug_missing",
                    "planning_empty_reason_guess": "",
                    "path_end_like_condition": False,
                    "current_lane_id": "8_1_1",
                    "last_reroute_timestamp": 1774492611.5680852,
                },
                {
                    "planning_header_sequence_num": 370,
                    "planning_header_timestamp_sec": 1774492625.595805,
                    "route_segment_count": 25,
                    "route_segment_total_length": 1540.071459746437,
                    "create_route_segments_status": "ready",
                    "lane_follow_map_status": "route_segments_present_reference_line_missing",
                    "lane_follow_map_inconsistent": True,
                    "reference_line_provider_status": "failed",
                    "planning_empty_reason_guess": "reference_line_missing",
                    "path_end_like_condition": True,
                    "current_lane_id": None,
                    "last_reroute_timestamp": 1774492625.387454,
                },
                {
                    "planning_header_sequence_num": 381,
                    "planning_header_timestamp_sec": 1774492626.428751,
                    "route_segment_count": 25,
                    "route_segment_total_length": 1540.071459746437,
                    "create_route_segments_status": "ready",
                    "lane_follow_map_status": "trajectory_nonzero_reference_line_debug_missing",
                    "lane_follow_map_inconsistent": False,
                    "reference_line_provider_status": "trajectory_nonzero_debug_missing",
                    "planning_empty_reason_guess": "",
                    "path_end_like_condition": False,
                    "current_lane_id": "8_1_1",
                    "last_reroute_timestamp": 1774492625.387454,
                },
            ],
        )
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_seq"], 369)
        self.assertTrue(summary["first_path_fallback_bridge_prev_normal_is_replan"])
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_replan_reason_family"], "current_time_smaller")
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_point_count"], 118)
        self.assertAlmostEqual(summary["first_path_fallback_bridge_prev_normal_first_point_v"], 1.42)
        self.assertAlmostEqual(summary["first_path_fallback_bridge_prev_normal_total_path_length"], 30.0)
        self.assertAlmostEqual(summary["first_path_fallback_bridge_prev_normal_total_time"], 7.1)
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_routing_segment_count"], 1)
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_reference_line_count"], 0)
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_route_segment_count"], 1)
        self.assertAlmostEqual(
            summary["first_path_fallback_bridge_prev_normal_route_segment_total_length"],
            30.000000195004887,
        )
        self.assertEqual(
            summary["first_path_fallback_bridge_prev_normal_lane_follow_map_status"],
            "trajectory_nonzero_reference_line_debug_missing",
        )
        self.assertEqual(
            summary["first_path_fallback_bridge_prev_normal_reference_line_provider_status"],
            "trajectory_nonzero_debug_missing",
        )
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_planning_empty_reason_guess"], "")
        self.assertFalse(summary["first_path_fallback_bridge_prev_normal_path_end_like_condition"])
        self.assertEqual(summary["first_path_fallback_bridge_prev_normal_current_lane_id"], "8_1_1")
        self.assertAlmostEqual(
            summary["first_path_fallback_bridge_prev_normal_last_reroute_timestamp"],
            1774492611.5680852,
        )
        self.assertEqual(summary["first_path_fallback_bridge_route_segment_count_delta_from_prev_normal"], 24)
        self.assertAlmostEqual(
            summary["first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal"],
            1510.071459551432,
        )
        self.assertTrue(summary["first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal"])
        self.assertTrue(summary["first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready"])
        self.assertEqual(summary["first_path_fallback_bridge_unknown_seq"], 370)
        self.assertEqual(summary["first_path_fallback_bridge_unknown_seq_gap_to_fallback"], 11)
        self.assertFalse(summary["first_path_fallback_bridge_unknown_is_replan"])
        self.assertEqual(summary["first_path_fallback_bridge_unknown_point_count"], 0)
        self.assertAlmostEqual(summary["first_path_fallback_bridge_unknown_total_path_length"], 0.0)
        self.assertAlmostEqual(summary["first_path_fallback_bridge_unknown_total_time"], 0.0)
        self.assertEqual(summary["first_path_fallback_bridge_unknown_routing_segment_count"], 25)
        self.assertEqual(summary["first_path_fallback_bridge_unknown_reference_line_count"], 0)
        self.assertEqual(summary["first_path_fallback_bridge_unknown_route_segment_count"], 25)
        self.assertAlmostEqual(summary["first_path_fallback_bridge_unknown_route_segment_total_length"], 1540.071459746437)
        self.assertEqual(summary["first_path_fallback_bridge_unknown_create_route_segments_status"], "ready")
        self.assertEqual(
            summary["first_path_fallback_bridge_unknown_lane_follow_map_status"],
            "route_segments_present_reference_line_missing",
        )
        self.assertTrue(summary["first_path_fallback_bridge_unknown_lane_follow_map_inconsistent"])
        self.assertEqual(summary["first_path_fallback_bridge_unknown_reference_line_provider_status"], "failed")
        self.assertEqual(summary["first_path_fallback_bridge_unknown_planning_empty_reason_guess"], "reference_line_missing")
        self.assertTrue(summary["first_path_fallback_bridge_unknown_path_end_like_condition"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_current_lane_id"])
        self.assertAlmostEqual(
            summary["first_path_fallback_bridge_unknown_last_reroute_timestamp"],
            1774492625.387454,
        )
        self.assertAlmostEqual(
            summary["first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec"],
            0.20835113525390625,
            places=6,
        )
        self.assertAlmostEqual(
            summary["first_path_fallback_bridge_unknown_reroute_delta_sec_from_prev_normal"],
            13.819368839263916,
        )
        self.assertTrue(summary["first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal"])
        self.assertTrue(summary["first_path_fallback_bridge_occurs_on_reroute_propagation_frame"])
        self.assertEqual(
            summary["first_path_fallback_bridge_likely_codepath_family"],
            "reroute_landing_reference_line_empty_output",
        )
        self.assertEqual(
            summary["first_path_fallback_bridge_likely_provider_failure_site"],
            "create_reference_line_new_command_empty_output",
        )
        self.assertEqual(summary["first_path_fallback_bridge_empty_previous_seq"], 371)
        self.assertEqual(summary["first_path_fallback_bridge_empty_previous_seq_gap_from_unknown"], 1)
        self.assertEqual(summary["first_path_fallback_bridge_empty_previous_seq_gap_to_fallback"], 10)
        self.assertEqual(summary["first_path_fallback_bridge_empty_previous_point_count"], 111)
        self.assertAlmostEqual(summary["first_path_fallback_bridge_empty_previous_first_point_v"], 1.068)
        self.assertAlmostEqual(
            summary["first_path_fallback_bridge_empty_previous_total_path_length"],
            20.443340653108876,
        )
        self.assertAlmostEqual(summary["first_path_fallback_bridge_empty_previous_total_time"], 7.1)
        self.assertEqual(summary["first_path_fallback_bridge_empty_previous_routing_segment_count"], 25)
        self.assertEqual(summary["first_path_fallback_bridge_empty_previous_reference_line_count"], 0)
        self.assertEqual(
            summary["first_path_fallback_bridge_empty_previous_replan_reason"],
            "replan for empty previous trajectory.",
        )
        self.assertAlmostEqual(
            summary["first_path_fallback_trigger_last_reroute_timestamp"],
            1774492625.387454,
        )
        self.assertAlmostEqual(
            summary["first_path_fallback_trigger_planning_header_age_from_last_reroute_sec"],
            1.0412969589233398,
            places=6,
        )

    def test_planning_trajectory_type_summary_ignores_distant_unknown_bridge(self) -> None:
        summary = _compute_planning_trajectory_type_summary(
            [
                {"planning_header_sequence_num": 10, "trajectory_type": "UNKNOWN", "timestamp": 10.0, "trajectory_point_count": 0},
                {
                    "planning_header_sequence_num": 11,
                    "trajectory_type": "NORMAL",
                    "timestamp": 11.0,
                    "trajectory_point_count": 111,
                    "trajectory_total_path_length": 20.0,
                    "first_trajectory_point_v": 1.0,
                    "replan_reason": "replan for empty previous trajectory.",
                },
                {
                    "planning_header_sequence_num": 50,
                    "trajectory_type": "NORMAL",
                    "timestamp": 50.0,
                    "trajectory_point_count": 111,
                    "trajectory_total_path_length": 29.0,
                    "first_trajectory_point_v": 2.3,
                    "trajectory_relative_time_min_sec": -1.12,
                },
                {
                    "planning_header_sequence_num": 51,
                    "trajectory_type": "NORMAL",
                    "timestamp": 51.0,
                    "trajectory_point_count": 111,
                    "trajectory_total_path_length": 30.1,
                    "first_trajectory_point_v": 2.4,
                    "trajectory_relative_time_min_sec": -0.98,
                },
                {
                    "planning_header_sequence_num": 52,
                    "trajectory_type": "PATH_FALLBACK",
                    "timestamp": 52.0,
                    "trajectory_point_count": 69,
                    "trajectory_total_path_length": 1.4,
                    "first_trajectory_point_v": 2.6,
                    "trajectory_relative_time_min_sec": 0.07,
                    "replan_reason": "the distance between matched point and actual position is too large. Replan is triggered. lon_diff = 2.64",
                },
            ],
            route_debug_rows=[
                {
                    "planning_header_sequence_num": 10,
                    "route_segment_count": 25,
                    "route_segment_total_length": 1540.0,
                    "lane_follow_map_status": "route_segments_present_reference_line_missing",
                    "reference_line_provider_status": "failed",
                    "planning_empty_reason_guess": "reference_line_missing",
                    "path_end_like_condition": True,
                    "current_lane_id": None,
                }
            ],
        )
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_seq"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_route_segment_count"])
        self.assertIsNone(summary["first_path_fallback_bridge_empty_previous_seq"])
        self.assertIsNone(summary["first_path_fallback_bridge_empty_previous_total_path_length"])
        self.assertIsNone(summary["first_path_fallback_bridge_prev_normal_seq"])
        self.assertIsNone(summary["first_path_fallback_bridge_prev_normal_is_replan"])
        self.assertIsNone(summary["first_path_fallback_bridge_prev_normal_replan_reason_family"])
        self.assertIsNone(summary["first_path_fallback_bridge_prev_normal_total_time"])
        self.assertIsNone(summary["first_path_fallback_bridge_prev_normal_routing_segment_count"])
        self.assertIsNone(summary["first_path_fallback_bridge_prev_normal_reference_line_count"])
        self.assertIsNone(summary["first_path_fallback_bridge_prev_normal_route_segment_count"])
        self.assertIsNone(summary["first_path_fallback_bridge_route_segment_count_delta_from_prev_normal"])
        self.assertIsNone(summary["first_path_fallback_bridge_route_segment_total_length_delta_from_prev_normal"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_current_lane_dropped_from_prev_normal"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_reference_line_failed_with_route_segments_ready"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_is_replan"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_total_time"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_routing_segment_count"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_reference_line_count"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_planning_header_age_from_last_reroute_sec"])
        self.assertIsNone(summary["first_path_fallback_bridge_unknown_last_reroute_timestamp_changed_from_prev_normal"])
        self.assertIsNone(summary["first_path_fallback_bridge_occurs_on_reroute_propagation_frame"])
        self.assertIsNone(summary["first_path_fallback_bridge_likely_codepath_family"])
        self.assertIsNone(summary["first_path_fallback_bridge_likely_provider_failure_site"])
        self.assertIsNone(summary["first_path_fallback_bridge_empty_previous_total_time"])
        self.assertIsNone(summary["first_path_fallback_bridge_empty_previous_routing_segment_count"])
        self.assertIsNone(summary["first_path_fallback_bridge_empty_previous_reference_line_count"])
        self.assertIsNone(summary["first_path_fallback_trigger_last_reroute_timestamp"])
        self.assertIsNone(summary["first_path_fallback_trigger_planning_header_age_from_last_reroute_sec"])

    def test_finalize_town01_run_builds_finalized_summary_from_event_streams(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health_relaxed",
                    "comparison_label": "legacy_relaxed",
                    "max_speed_mps": 6.5,
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "run": {
                            "ticks": 700,
                            "profile_name": "town01_route_health_relaxed",
                            "comparison_label": "legacy_relaxed",
                        },
                        "algo": {
                            "apollo": {
                                "carla_to_apollo": {"tx": 0.0, "ty": 0.0, "tz": 0.0, "yaw_deg": 0.0},
                                "control_mapping": {
                                    "force_zero_steer_output": True,
                                    "straight_lane_zero_steer_enabled": False,
                                    "low_speed_steer_guard_enabled": True,
                                    "low_speed_sustained_saturation_guard_enabled": True,
                                    "sustained_lateral_guard_enabled": True,
                                    "trajectory_contract_lateral_guard_enabled": True,
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 68,
                    "goal_trace_index": 68,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                    "spawn_lane": {"road_id": 1, "section_id": 0, "lane_id": 1},
                    "goal_lane": {"road_id": 2, "section_id": 0, "lane_id": 1},
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {})
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_json(
                artifacts / "direct_bridge_stats.json",
                {"control_apply_count": 7, "control_apply_fail_count": 1},
            )
            _write_jsonl(
                artifacts / "direct_bridge_control_apply.jsonl",
                [
                    {
                        "ts_sec": 201.0,
                        "frame_id": 38395,
                        "source": "pending",
                        "throttle": 0.45,
                        "brake": 0.0,
                        "speed_mps": 0.0,
                    },
                    {
                        "ts_sec": 201.2,
                        "frame_id": 38396,
                        "source": "pending",
                        "throttle": 0.94,
                        "brake": 0.0,
                        "speed_mps": 0.01,
                    },
                ],
            )
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 100.0, "routing_request_sent": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 101.0,
                        "planning_header_timestamp_sec": 100.9,
                        "planning_header_sequence_num": 1,
                        "trajectory_point_count": 10,
                        "trajectory_relative_time_min_sec": 0.0,
                        "trajectory_relative_time_max_sec": 8.0,
                        "planning_message_parsed_successfully": True,
                    }
                ],
            )
            _write_jsonl(
                artifacts / "control_trajectory_consume_debug_live.jsonl",
                [
                    {
                        "timestamp": 101.1,
                        "latest_planning_msg_timestamp": 101.0,
                        "latest_planning_msg_age_ms": 100.0,
                        "latest_planning_msg_sequence_num": 1,
                        "latest_planning_trajectory_point_count": 10,
                        "trajectory_first_point_relative_time": 0.0,
                        "trajectory_last_point_relative_time": 8.0,
                        "control_now_relative_to_planning_header_sec": 0.2,
                        "control_used_planning_trajectory": True,
                        "control_used_cached_trajectory": False,
                        "control_had_no_trajectory": False,
                        "auto_drive_mode": "auto",
                        "engage_state": "ready",
                    }
                ],
            )
            (artifacts / "apollo_control.INFO").write_text("", encoding="utf-8")
            _write_jsonl(
                artifacts / "bridge_control_decode.jsonl",
                [
                    {
                        "ts_sec": 101.1,
                        "raw_steer": 0.1,
                        "commanded_steer": 0.1,
                        "force_zero_steer_applied": False,
                    }
                ],
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 100.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "mapped_throttle_cmd": 0.0,
                        "mapped_brake_cmd": 0.0,
                        "apollo_acceleration_mps2": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 110.0,
                        "map_x": 195.0,
                        "map_y": 0.0,
                        "speed_mps": 6.5,
                        "mapped_throttle_cmd": 0.1,
                        "mapped_brake_cmd": 0.0,
                        "apollo_acceleration_mps2": 0.2,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertEqual(finalized["summary_status"], "finalized")
            self.assertTrue(finalized["finalized_from_event_stream"])
            self.assertEqual(finalized["route_id"], "town01_rh_spawn068_goal068")
            self.assertEqual(finalized["route_health_label"], "route_health_pass")
            self.assertTrue(finalized["acceptance"]["success"])
            self.assertEqual(finalized["final_goal_distance_m"], 5.0)
            self.assertEqual(finalized["bridge_runtime_preflight_status"], "unknown")
            self.assertIsNone(finalized["bridge_runtime_import_ok"])
            self.assertIsNone(finalized["bridge_runtime_import_error"])
            self.assertTrue(finalized["bridge_stats_materialized"])
            self.assertTrue(finalized["routing_materialized"])
            self.assertTrue(finalized["planning_materialized"])
            self.assertEqual(finalized["routing_first_request_at"], 100.0)
            self.assertEqual(finalized["planning_first_message_at"], 101.0)
            self.assertEqual(finalized["materialization_status"], "planning_control_materialized")
            self.assertEqual(finalized["planning_control_alignment"]["first_nonzero_planning_seq"], 1)
            self.assertEqual(finalized["planning_control_alignment"]["last_live_control_planning_seq"], 1)
            self.assertFalse(finalized["planning_control_alignment"]["live_control_stops_before_first_nonzero_planning"])
            self.assertEqual(finalized["control_handoff_status"], "control_consuming_with_nonzero_planning")
            self.assertEqual(finalized["planning_route_seen_at"], 100.0)
            self.assertEqual(finalized["planning_first_nonempty_at"], 101.0)
            self.assertEqual(finalized["control_first_consume_at"], 101.1)
            self.assertEqual(finalized["control_consume_row_count"], 1)
            self.assertAlmostEqual(finalized["control_handoff_latency_sec"], 0.1, places=3)
            self.assertTrue(finalized["apollo_control_process_alive"])
            self.assertFalse(finalized["apollo_control_log_nonempty"])
            self.assertEqual(finalized["apollo_control_raw_row_count"], 0)
            self.assertFalse(finalized["control_zero_hold_only"])
            self.assertEqual(finalized["control_last_input_trajectory_seq"], 1)
            self.assertEqual(finalized["control_last_input_trajectory_point_count"], 10)
            self.assertEqual(finalized["control_first_nonzero_planning_seen_by_control_at"], 101.1)
            self.assertEqual(finalized["control_last_output_ts"], 101.1)
            self.assertIsNone(finalized["control_output_zero_hold_ratio"])
            self.assertFalse(finalized["control_final_process_alive"])
            self.assertEqual(finalized["runtime_mode"]["enable_lateral"], False)
            self.assertEqual(finalized["runtime_mode"]["enable_guard"], True)
            self.assertEqual(finalized["runtime_mode"]["force_zero_steer_output"], True)
            self.assertEqual(finalized["runtime_mode"]["run_ticks"], 700)
            self.assertEqual(finalized["raw_steer_saturated_ratio"], 0.0)
            self.assertEqual(finalized["guard_trigger_ratio"], 0.0)
            self.assertEqual(finalized["path_fallback_count"], 0)
            self.assertEqual(finalized["first_recovery_after_path_fallback_seq"], None)
            self.assertEqual(finalized["first_relapse_prev_normal_first_point_v"], None)
            self.assertEqual(finalized["first_relapse_prev_normal_total_path_length"], None)
            self.assertEqual(finalized["first_relapse_after_recovery_first_point_v"], None)
            self.assertEqual(finalized["first_relapse_after_recovery_total_path_length"], None)
            self.assertEqual(finalized["max_consecutive_path_fallback_length"], 0)
            self.assertEqual(finalized["final_path_fallback_suffix_length"], 0)
            self.assertEqual(finalized["final_normal_tail_length"], 0)
            self.assertFalse(finalized["persistent_path_fallback_at_end"])
            self.assertEqual(finalized["route_health"]["raw_steer_saturated_ratio"], 0.0)
            self.assertEqual(finalized["route_health"]["guard_trigger_ratio"], 0.0)
            self.assertEqual(finalized["route_health"]["path_fallback_count"], 0)
            self.assertEqual(finalized["route_health"]["first_recovery_after_path_fallback_seq"], None)
            self.assertEqual(finalized["route_health"]["first_relapse_prev_normal_first_point_v"], None)
            self.assertEqual(finalized["route_health"]["first_relapse_prev_normal_total_path_length"], None)
            self.assertEqual(finalized["route_health"]["first_relapse_after_recovery_first_point_v"], None)
            self.assertEqual(finalized["route_health"]["first_relapse_after_recovery_total_path_length"], None)
            self.assertEqual(finalized["route_health"]["max_consecutive_path_fallback_length"], 0)
            self.assertEqual(finalized["route_health"]["final_normal_tail_length"], 0)
            scenario_meta = json.loads((artifacts / "scenario_metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(scenario_meta["enable_lateral"], False)
            self.assertEqual(scenario_meta["enable_guard"], True)
            self.assertEqual(scenario_meta["force_zero_steer_output"], True)
            self.assertEqual(scenario_meta["run_ticks"], 700)
            self.assertEqual(
                finalized["planning_control_alignment"]["last_live_control_input_trajectory_header_sequence_num"],
                None,
            )
            self.assertEqual(
                finalized["planning_control_alignment"]["last_live_control_candidate_trajectory_header_sequence_num"],
                None,
            )
            self.assertFalse(finalized["planning_control_alignment"]["exact_match_seen"])
            self.assertTrue((artifacts / "town01_route_health_state_timeline.jsonl").exists())
            self.assertTrue((artifacts / "planning_control_alignment.finalized.json").exists())
            self.assertTrue((artifacts / "control_handoff_summary.json").exists())
            self.assertTrue((run_dir / "summary.provisional.json").exists())

    def test_finalize_town01_run_writes_curve_tracking_health_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health_relaxed",
                    "comparison_label": "curve_tracking_health",
                    "max_speed_mps": 6.5,
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "run": {
                            "ticks": 700,
                            "profile_name": "town01_route_health_relaxed",
                            "comparison_label": "curve_tracking_health",
                        },
                        "algo": {
                            "apollo": {
                                "planning": {"default_cruise_speed_mps": 18.0},
                                "routing": {"target_speed_mps": 8.8},
                                "control_mapping": {
                                    "force_zero_steer_output": False,
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 217,
                    "goal_trace_index": 48,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {})
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 100.0, "routing_request_sent": True, "routing_success": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 101.0,
                        "planning_header_timestamp_sec": 100.9,
                        "planning_header_sequence_num": 70,
                        "trajectory_type": "NORMAL",
                        "trajectory_point_count": 111,
                        "trajectory_total_path_length": 40.0,
                        "first_trajectory_point_v": 4.0,
                        "planning_message_parsed_successfully": True,
                    },
                    {
                        "timestamp": 102.0,
                        "planning_header_timestamp_sec": 101.9,
                        "planning_header_sequence_num": 71,
                        "trajectory_type": "PATH_FALLBACK",
                        "trajectory_point_count": 69,
                        "trajectory_total_path_length": 5.0,
                        "first_trajectory_point_v": 4.1,
                        "planning_message_parsed_successfully": True,
                    },
                    {
                        "timestamp": 103.0,
                        "planning_header_timestamp_sec": 102.9,
                        "planning_header_sequence_num": 72,
                        "trajectory_type": "NORMAL",
                        "trajectory_point_count": 118,
                        "trajectory_total_path_length": 38.0,
                        "first_trajectory_point_v": 4.0,
                        "planning_message_parsed_successfully": True,
                    },
                    {
                        "timestamp": 104.0,
                        "planning_header_timestamp_sec": 103.9,
                        "planning_header_sequence_num": 73,
                        "trajectory_type": "PATH_FALLBACK",
                        "trajectory_point_count": 69,
                        "trajectory_total_path_length": 3.6,
                        "first_trajectory_point_v": 4.2,
                        "replan_reason": "the distance between matched point and actual position is too large. Replan is triggered. lon_diff = 2.50",
                        "planning_message_parsed_successfully": True,
                    },
                    {
                        "timestamp": 105.0,
                        "planning_header_timestamp_sec": 104.9,
                        "planning_header_sequence_num": 74,
                        "trajectory_type": "NORMAL",
                        "trajectory_point_count": 117,
                        "trajectory_total_path_length": 34.0,
                        "first_trajectory_point_v": 4.0,
                        "planning_message_parsed_successfully": True,
                    },
                ],
            )
            _write_jsonl(
                artifacts / "control_trajectory_consume_debug_live.jsonl",
                [
                    {
                        "timestamp": 105.1,
                        "latest_planning_msg_timestamp": 105.0,
                        "latest_planning_msg_age_ms": 100.0,
                        "latest_planning_msg_sequence_num": 74,
                        "latest_planning_trajectory_point_count": 117,
                        "trajectory_first_point_relative_time": 0.0,
                        "trajectory_last_point_relative_time": 8.0,
                        "control_now_relative_to_planning_header_sec": 0.2,
                        "control_used_planning_trajectory": True,
                        "control_used_cached_trajectory": False,
                        "control_had_no_trajectory": False,
                        "auto_drive_mode": "auto",
                        "engage_state": "ready",
                    }
                ],
            )
            _write_jsonl(
                artifacts / "apollo_control_raw.jsonl",
                [
                    {
                        "ts_sec": 103.1,
                        "selected_steering_field": "steering_target",
                        "apollo_control_raw": {
                            "control_header_sequence_num": 1,
                            "debug_input_trajectory_header_sequence_num": 72,
                            "steering_target": 10.0,
                        },
                    },
                    {
                        "ts_sec": 103.2,
                        "selected_steering_field": "steering_target",
                        "apollo_control_raw": {
                            "control_header_sequence_num": 2,
                            "debug_input_trajectory_header_sequence_num": 72,
                            "steering_target": 96.0,
                        },
                    },
                    {
                        "ts_sec": 104.2,
                        "selected_steering_field": "steering_target",
                        "apollo_control_raw": {
                            "control_header_sequence_num": 3,
                            "debug_input_trajectory_header_sequence_num": 73,
                            "steering_target": 100.0,
                        },
                    },
                ],
            )
            _write_jsonl(
                artifacts / "control_decode_debug.jsonl",
                [
                    {
                        "ts_sec": 103.1,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.01,
                            "debug_simple_lat_heading_error_rad": 0.005,
                            "debug_simple_lat_target_point_kappa": 0.05,
                        },
                        "output_to_carla": {
                            "mapped_carla_steer_cmd": 0.02,
                            "target_front_wheel_angle_deg": 1.0,
                        },
                    },
                    {
                        "ts_sec": 103.2,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.08,
                            "debug_simple_lat_heading_error_rad": 0.04,
                            "debug_simple_lat_target_point_kappa": 0.15,
                        },
                        "output_to_carla": {
                            "mapped_carla_steer_cmd": 0.10,
                            "target_front_wheel_angle_deg": 6.0,
                        },
                    },
                    {
                        "ts_sec": 104.0,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.12,
                            "debug_simple_lat_heading_error_rad": 0.08,
                            "debug_simple_lat_target_point_kappa": 0.20,
                        },
                        "output_to_carla": {
                            "mapped_carla_steer_cmd": 0.12,
                            "target_front_wheel_angle_deg": 8.2,
                        },
                    },
                ],
            )
            (artifacts / "apollo_control.INFO").write_text("", encoding="utf-8")
            _write_jsonl(
                artifacts / "bridge_control_decode.jsonl",
                [
                    {
                        "ts_sec": 105.1,
                        "raw_steer": 0.1,
                        "commanded_steer": 0.1,
                        "force_zero_steer_applied": False,
                    }
                ],
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 100.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "mapped_throttle_cmd": 0.0,
                        "mapped_brake_cmd": 0.0,
                        "apollo_acceleration_mps2": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 110.0,
                        "map_x": 195.0,
                        "map_y": 0.0,
                        "speed_mps": 6.5,
                        "mapped_throttle_cmd": 0.1,
                        "mapped_brake_cmd": 0.0,
                        "apollo_acceleration_mps2": 0.2,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            curve_summary = json.loads(
                (artifacts / "curve_tracking_health_summary.finalized.json").read_text(encoding="utf-8")
            )
            semantic_window = json.loads(
                (artifacts / "curve_semantic_window.finalized.json").read_text(encoding="utf-8")
            )
            longitudinal_summary = json.loads(
                (artifacts / "longitudinal_tracking_health_summary.finalized.json").read_text(encoding="utf-8")
            )
            self.assertAlmostEqual(curve_summary["apollo_raw_steer_abs_p95"], 99.6)
            self.assertAlmostEqual(curve_summary["apollo_raw_steer_abs_p99"], 99.92)
            self.assertAlmostEqual(curve_summary["apollo_raw_steer_over95_ratio"], 2.0 / 3.0)
            self.assertEqual(curve_summary["first_high_steer_seq"], 72)
            self.assertAlmostEqual(curve_summary["first_high_steer_at"], 103.2)
            self.assertEqual(curve_summary["first_matched_point_too_large_seq"], 73)
            self.assertTrue(curve_summary["high_steer_before_first_matched_point_too_large"])
            self.assertAlmostEqual(curve_summary["apollo_simple_lat_lateral_error_abs_p95"], 0.116)
            self.assertAlmostEqual(curve_summary["apollo_simple_lat_heading_error_abs_p95"], 0.076)
            self.assertAlmostEqual(curve_summary["apollo_simple_lat_target_point_kappa_abs_p95"], 0.195)
            self.assertAlmostEqual(curve_summary["mapped_carla_steer_cmd_abs_p95"], 0.118)
            self.assertAlmostEqual(curve_summary["target_front_wheel_angle_deg_abs_p95"], 7.98)
            self.assertEqual(curve_summary["path_fallback_count_after_first_recovery"], 1)
            self.assertFalse(curve_summary["persistent_path_fallback_at_end"])
            self.assertEqual(semantic_window["semantic_window_anchor_kind"], "matched_point_too_large")
            self.assertEqual(semantic_window["semantic_window_anchor_seq"], 73)
            self.assertAlmostEqual(semantic_window["semantic_window_anchor_at"], 104.0)
            self.assertEqual(semantic_window["first_matched_point_too_large_seq"], 73)
            self.assertAlmostEqual(semantic_window["first_matched_point_too_large_at"], 104.0)
            self.assertAlmostEqual(semantic_window["simple_lat_lateral_error_abs_p95_before_failure"], 0.116)
            self.assertAlmostEqual(semantic_window["simple_lat_heading_error_abs_p95_before_failure"], 0.076)
            self.assertAlmostEqual(semantic_window["target_point_kappa_abs_p95_before_failure"], 0.195)
            self.assertAlmostEqual(semantic_window["mapped_carla_steer_cmd_abs_p95_before_failure"], 0.118)
            self.assertAlmostEqual(semantic_window["simple_lat_lateral_error_abs_p95_before_anchor"], 0.116)
            self.assertAlmostEqual(semantic_window["simple_lat_heading_error_abs_p95_before_anchor"], 0.076)
            self.assertAlmostEqual(semantic_window["target_point_kappa_abs_p95_before_anchor"], 0.195)
            self.assertAlmostEqual(semantic_window["mapped_carla_steer_cmd_abs_p95_before_anchor"], 0.118)
            self.assertTrue(semantic_window["high_steer_before_first_matched_point_too_large"])
            self.assertFalse(semantic_window["persistent_path_fallback_at_end"])
            self.assertAlmostEqual(longitudinal_summary["routing_target_speed_mps"], 8.8)
            self.assertAlmostEqual(longitudinal_summary["planning_default_cruise_speed_mps"], 18.0)
            self.assertAlmostEqual(longitudinal_summary["speed_abs_p95"], 6.175)
            self.assertAlmostEqual(longitudinal_summary["speed_ratio_to_target_p95"], 6.175 / 8.8)
            self.assertAlmostEqual(longitudinal_summary["throttle_p95"], 0.095)
            self.assertAlmostEqual(longitudinal_summary["brake_p95"], 0.0)
            self.assertAlmostEqual(longitudinal_summary["throttle_brake_overlap_ratio"], 0.0)
            self.assertAlmostEqual(longitudinal_summary["acc_cmd_p95"], 0.19)
            self.assertAlmostEqual(longitudinal_summary["speed_gain_limited_ratio"], 0.0)
            self.assertFalse(longitudinal_summary["persistent_low_speed_at_end"])

            self.assertAlmostEqual(finalized["apollo_raw_steer_abs_p95"], 99.6)
            self.assertAlmostEqual(finalized["apollo_raw_steer_abs_p99"], 99.92)
            self.assertAlmostEqual(finalized["apollo_raw_steer_over95_ratio"], 2.0 / 3.0)
            self.assertAlmostEqual(finalized["apollo_simple_lat_lateral_error_abs_p95"], 0.116)
            self.assertAlmostEqual(finalized["apollo_simple_lat_heading_error_abs_p95"], 0.076)
            self.assertAlmostEqual(finalized["apollo_simple_lat_target_point_kappa_abs_p95"], 0.195)
            self.assertAlmostEqual(finalized["mapped_carla_steer_cmd_abs_p95"], 0.118)
            self.assertAlmostEqual(finalized["target_front_wheel_angle_deg_abs_p95"], 7.98)
            self.assertAlmostEqual(finalized["routing_target_speed_mps"], 8.8)
            self.assertAlmostEqual(finalized["planning_default_cruise_speed_mps"], 18.0)
            self.assertAlmostEqual(finalized["speed_abs_p95"], 6.175)
            self.assertAlmostEqual(finalized["speed_ratio_to_target_p95"], 6.175 / 8.8)
            self.assertAlmostEqual(finalized["throttle_p95"], 0.095)
            self.assertAlmostEqual(finalized["brake_p95"], 0.0)
            self.assertAlmostEqual(finalized["throttle_brake_overlap_ratio"], 0.0)
            self.assertAlmostEqual(finalized["acc_cmd_p95"], 0.19)
            self.assertFalse(finalized["persistent_low_speed_at_end"])
            self.assertAlmostEqual(finalized["first_high_steer_at"], 103.2)
            self.assertEqual(finalized["first_matched_point_too_large_seq"], 73)
            self.assertEqual(finalized["semantic_window_anchor_kind"], "matched_point_too_large")
            self.assertEqual(finalized["semantic_window_anchor_seq"], 73)
            self.assertAlmostEqual(finalized["semantic_window_anchor_at"], 104.0)
            self.assertAlmostEqual(finalized["first_matched_point_too_large_at"], 104.0)
            self.assertAlmostEqual(finalized["simple_lat_lateral_error_abs_p95_before_failure"], 0.116)
            self.assertAlmostEqual(finalized["simple_lat_heading_error_abs_p95_before_failure"], 0.076)
            self.assertAlmostEqual(finalized["target_point_kappa_abs_p95_before_failure"], 0.195)
            self.assertAlmostEqual(finalized["mapped_carla_steer_cmd_abs_p95_before_failure"], 0.118)
            self.assertAlmostEqual(finalized["simple_lat_lateral_error_abs_p95_before_anchor"], 0.116)
            self.assertAlmostEqual(finalized["simple_lat_heading_error_abs_p95_before_anchor"], 0.076)
            self.assertAlmostEqual(finalized["target_point_kappa_abs_p95_before_anchor"], 0.195)
            self.assertAlmostEqual(finalized["mapped_carla_steer_cmd_abs_p95_before_anchor"], 0.118)
            self.assertEqual(finalized["first_high_steer_seq"], 72)
            self.assertEqual(finalized["first_matched_point_too_large_seq"], 73)
            self.assertTrue(finalized["high_steer_before_first_matched_point_too_large"])
            self.assertAlmostEqual(finalized["route_health"]["apollo_raw_steer_abs_p95"], 99.6)
            self.assertAlmostEqual(finalized["route_health"]["apollo_raw_steer_abs_p99"], 99.92)
            self.assertAlmostEqual(finalized["route_health"]["apollo_raw_steer_over95_ratio"], 2.0 / 3.0)
            self.assertAlmostEqual(finalized["route_health"]["apollo_simple_lat_lateral_error_abs_p95"], 0.116)
            self.assertAlmostEqual(finalized["route_health"]["apollo_simple_lat_heading_error_abs_p95"], 0.076)
            self.assertAlmostEqual(finalized["route_health"]["apollo_simple_lat_target_point_kappa_abs_p95"], 0.195)
            self.assertAlmostEqual(finalized["route_health"]["mapped_carla_steer_cmd_abs_p95"], 0.118)
            self.assertAlmostEqual(finalized["route_health"]["target_front_wheel_angle_deg_abs_p95"], 7.98)
            self.assertEqual(finalized["route_health"]["first_high_steer_seq"], 72)
            self.assertAlmostEqual(finalized["route_health"]["first_high_steer_at"], 103.2)
            self.assertEqual(finalized["route_health"]["first_matched_point_too_large_seq"], 73)
            self.assertEqual(finalized["route_health"]["semantic_window_anchor_kind"], "matched_point_too_large")
            self.assertEqual(finalized["route_health"]["semantic_window_anchor_seq"], 73)
            self.assertAlmostEqual(finalized["route_health"]["semantic_window_anchor_at"], 104.0)
            self.assertTrue(finalized["route_health"]["high_steer_before_first_matched_point_too_large"])

    def test_finalize_town01_run_recovers_lateral_runtime_from_effective_cfg_when_flags_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health_curve_runtime_contract_repair",
                    "comparison_label": "curve_runtime_contract_repair",
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "run": {
                            "ticks": 420,
                            "profile_name": "town01_apollo_route_health_curve_runtime_contract_repair",
                            "comparison_label": "curve_runtime_contract_repair",
                            "capability_profile": "curve_lane_follow",
                        },
                        "algo": {
                            "apollo": {
                                "bridge": {"localization_back_offset_m": 1.4235},
                                "map_contract": {"fail_fast_on_high_risk_mismatch": False},
                                "planning": {
                                    "acc_only_mode": False,
                                    "longitudinal_only_pipeline": False,
                                    "longitudinal_only_keep_lane_follow_path": False,
                                },
                                "control_mapping": {
                                    "force_zero_steer_output": False,
                                    "low_speed_steer_guard_enabled": True,
                                    "low_speed_sustained_saturation_guard_enabled": True,
                                    "sustained_lateral_guard_enabled": True,
                                    "trajectory_contract_lateral_guard_enabled": True,
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 217,
                    "goal_trace_index": 48,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                    "enable_lateral": False,
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {})
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 100.0, "routing_request_sent": True, "routing_success": True}],
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 100.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    }
                ],
            )

            finalized = finalize_town01_run(run_dir)
            scenario_meta = json.loads((artifacts / "scenario_metadata.json").read_text(encoding="utf-8"))

            self.assertTrue(finalized["runtime_mode"]["enable_lateral"])
            self.assertTrue(scenario_meta["enable_lateral"])
            self.assertEqual(finalized["runtime_contract"]["status"], "aligned")
            self.assertEqual(finalized["runtime_contract"]["blockers"], [])

    def test_compute_curve_semantic_window_falls_back_to_first_high_steer_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir)
            _write_jsonl(
                artifacts_dir / "control_decode_debug.jsonl",
                [
                    {
                        "ts_sec": 200.0,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.01,
                            "debug_simple_lat_heading_error_rad": 0.002,
                            "debug_simple_lat_target_point_kappa": 0.01,
                        },
                        "raw_control_msg_dump": {
                            "debug_input_trajectory_header_sequence_num": 699,
                        },
                        "output_to_carla": {"mapped_carla_steer_cmd": 0.001},
                    },
                    {
                        "ts_sec": 204.0,
                        "parsed_control": {
                            "debug_simple_lat_lateral_error_m": 0.03,
                            "debug_simple_lat_heading_error_rad": 0.005,
                            "debug_simple_lat_target_point_kappa": 0.04,
                        },
                        "raw_control_msg_dump": {
                            "debug_input_trajectory_header_sequence_num": 700,
                        },
                        "output_to_carla": {"mapped_carla_steer_cmd": 0.002},
                    },
                ],
            )
            semantic_window = _compute_curve_semantic_window(
                artifacts_dir,
                planning_rows=[],
                planning_trajectory_type_summary={"persistent_path_fallback_at_end": True},
                curve_tracking_health_summary={
                    "first_high_steer_seq": 700,
                    "first_high_steer_at": 204.0,
                    "first_matched_point_too_large_seq": None,
                    "high_steer_before_first_matched_point_too_large": None,
                },
            )
            self.assertEqual(semantic_window["semantic_window_anchor_kind"], "first_high_steer")
            self.assertEqual(semantic_window["semantic_window_anchor_seq"], 700)
            self.assertAlmostEqual(semantic_window["semantic_window_anchor_at"], 204.0)
            self.assertIsNone(semantic_window["first_matched_point_too_large_seq"])
            self.assertAlmostEqual(semantic_window["simple_lat_lateral_error_abs_p95_before_anchor"], 0.029)
            self.assertAlmostEqual(semantic_window["simple_lat_heading_error_abs_p95_before_anchor"], 0.00485)
            self.assertAlmostEqual(semantic_window["target_point_kappa_abs_p95_before_anchor"], 0.0385)
            self.assertAlmostEqual(semantic_window["mapped_carla_steer_cmd_abs_p95_before_anchor"], 0.00195)
            self.assertTrue(semantic_window["persistent_path_fallback_at_end"])

    def test_finalize_town01_run_surfaces_bridge_runtime_import_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health",
                    "comparison_label": "bridge_runtime_failed",
                },
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 97,
                    "goal_trace_index": 46,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(
                artifacts / "bridge_runtime_preflight.json",
                {
                    "bridge_runtime_import_ok": False,
                    "bridge_runtime_preflight_status": "bridge_runtime_import_failed",
                    "bridge_runtime_import_error": "ImportError: libcudart.so.11.0",
                    "bridge_runtime_dependency_probe_status": "bridge_runtime_cuda_runtime_failed",
                    "bridge_runtime_missing_shared_libs": ["libcudart.so.11.0"],
                    "bridge_runtime_missing_python_modules": [],
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {})
            _write_json(artifacts / "cyber_bridge_stats.json", {})
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 100.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "false",
                        "terminal_stop_hold_active": "false",
                    }
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertEqual(finalized["bridge_runtime_preflight_status"], "bridge_runtime_import_failed")
            self.assertFalse(finalized["bridge_runtime_import_ok"])
            self.assertIn("libcudart.so.11.0", finalized["bridge_runtime_import_error"])
            self.assertEqual(finalized["bridge_runtime_dependency_probe_status"], "bridge_runtime_cuda_runtime_failed")
            self.assertEqual(finalized["bridge_runtime_missing_shared_libs"], ["libcudart.so.11.0"])
            self.assertFalse(finalized["routing_materialized"])
            self.assertFalse(finalized["planning_materialized"])
            self.assertEqual(finalized["materialization_status"], "bridge_runtime_import_failed")
            self.assertEqual(finalized["acceptance"]["fail_reason"], "BRIDGE_RUNTIME_IMPORT")
            self.assertEqual(finalized["route_health_label"], "route_not_established")

    def test_finalize_town01_run_classifies_planning_ready_control_not_consuming(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health",
                    "comparison_label": "control_regressed",
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "algo": {
                            "apollo": {
                                "docker": {
                                    "defer_control_until_planning_ready": False,
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 97,
                    "goal_trace_index": 46,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(
                artifacts / "bridge_health_summary.json",
                {
                    "planning_first_nonempty_ts_sec": 101.5,
                },
            )
            _write_json(
                artifacts / "command_materialization_summary.json",
                {
                    "command_path_stage": "planning_ready_no_control_rx",
                    "first_divergence_layer": "control_materialization",
                    "first_divergence_reason": "control_message_pending",
                },
            )
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_json(
                artifacts / "direct_bridge_stats.json",
                {"control_apply_count": 7, "control_apply_fail_count": 1},
            )
            _write_jsonl(
                artifacts / "direct_bridge_control_apply.jsonl",
                [
                    {
                        "ts_sec": 201.0,
                        "frame_id": 38395,
                        "source": "pending",
                        "throttle": 0.45,
                        "brake": 0.0,
                        "speed_mps": 0.0,
                    },
                    {
                        "ts_sec": 201.2,
                        "frame_id": 38396,
                        "source": "pending",
                        "throttle": 0.94,
                        "brake": 0.0,
                        "speed_mps": 0.01,
                    },
                ],
            )
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 101.0, "routing_request_sent": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 101.5,
                        "planning_header_timestamp_sec": 101.4,
                        "planning_header_sequence_num": 5,
                        "trajectory_point_count": 111,
                        "trajectory_relative_time_min_sec": 0.0,
                        "trajectory_relative_time_max_sec": 8.0,
                        "planning_message_parsed_successfully": True,
                    }
                ],
            )
            (artifacts / "control_trajectory_consume_debug_live.jsonl").write_text("", encoding="utf-8")
            (artifacts / "apollo_control.INFO").write_text("", encoding="utf-8")
            (artifacts / "apollo_modules_status.log").write_text(
                "runtime_filtered_lines: [\"123 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control_planning_ready_wait.log").write_text(
                "\n".join(
                    [
                        "status=ready",
                        json.dumps(
                            {
                                "ts": 101.4,
                                "route_seen_at": 101.2,
                                "planning_nonempty_trajectory_count": 1,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 101.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 111.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertEqual(finalized["route_health_label"], "route_established_but_no_control_progress")
            self.assertEqual(finalized["control_handoff_status"], "planning_ready_control_not_consuming")
            self.assertEqual(finalized["planning_route_seen_at"], 101.2)
            self.assertEqual(finalized["planning_first_nonempty_at"], 101.5)
            self.assertIsNone(finalized["control_first_consume_at"])
            self.assertEqual(finalized["control_consume_row_count"], 0)
            self.assertIsNone(finalized["control_handoff_latency_sec"])
            self.assertTrue(finalized["apollo_control_process_alive"])
            self.assertFalse(finalized["apollo_control_log_nonempty"])
            self.assertEqual(finalized["apollo_control_raw_row_count"], 0)
            self.assertFalse(finalized["control_zero_hold_only"])
            self.assertEqual(finalized["control_last_input_trajectory_seq"], 0)
            self.assertEqual(finalized["control_last_input_trajectory_point_count"], 0)
            self.assertIsNone(finalized["control_first_nonzero_planning_seen_by_control_at"])
            self.assertIsNone(finalized["control_last_output_ts"])
            self.assertIsNone(finalized["control_output_zero_hold_ratio"])
            self.assertFalse(finalized["control_final_process_alive"])
            self.assertFalse(finalized["control_final_modules_status"]["control_present"])
            self.assertEqual(finalized["direct_control_apply_count"], 7)
            self.assertEqual(finalized["direct_control_apply_fail_count"], 1)
            self.assertEqual(finalized["direct_control_apply_window_status"], "short_apply_window")
            self.assertEqual(finalized["direct_control_first_apply_frame"], 38395)
            self.assertEqual(finalized["direct_control_last_apply_frame"], 38396)
            self.assertEqual(finalized["direct_control_apply_frame_span"], 2)
            self.assertEqual(finalized["direct_control_apply_max_throttle"], 0.94)
            self.assertEqual(finalized["direct_metric_consistency_status"], "not_applicable")
            self.assertTrue((artifacts / "direct_control_apply_summary.finalized.json").exists())
            self.assertTrue((artifacts / "direct_metric_consistency.finalized.json").exists())

    def test_finalize_town01_run_classifies_control_output_stopped_before_nonzero_planning(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health",
                    "comparison_label": "control_zero_hold",
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "algo": {
                            "apollo": {
                                "docker": {
                                    "defer_control_until_planning_ready": False,
                                    "control_start_gate": "route_established",
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 97,
                    "goal_trace_index": 46,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(
                artifacts / "bridge_health_summary.json",
                {
                    "planning_first_nonempty_ts_sec": 102.0,
                },
            )
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 101.0, "routing_request_sent": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 101.5,
                        "planning_header_timestamp_sec": 101.4,
                        "planning_header_sequence_num": 1,
                        "trajectory_point_count": 0,
                        "planning_message_parsed_successfully": True,
                    },
                    {
                        "timestamp": 102.0,
                        "planning_header_timestamp_sec": 101.9,
                        "planning_header_sequence_num": 2,
                        "trajectory_point_count": 111,
                        "planning_message_parsed_successfully": True,
                    },
                ],
            )
            _write_jsonl(
                artifacts / "control_trajectory_consume_debug_live.jsonl",
                [
                    {
                        "timestamp": 101.98,
                        "latest_planning_msg_timestamp": 101.95,
                        "latest_planning_msg_age_ms": 20.0,
                        "latest_planning_msg_sequence_num": 1,
                        "control_input_trajectory_header_sequence_num": None,
                        "control_input_candidate_source": "missing",
                        "effective_planning_source": "latest_known_fallback",
                        "latest_planning_trajectory_point_count": 0,
                        "control_used_planning_trajectory": False,
                        "control_used_cached_trajectory": False,
                        "control_had_no_trajectory": False,
                    }
                ],
            )
            _write_jsonl(
                artifacts / "apollo_control_raw.jsonl",
                [
                    {
                        "ts_sec": 101.97,
                        "apollo_control_raw": {
                            "throttle": 0.0,
                            "brake": 15.0,
                            "steering_target": 0.0,
                            "debug_input_trajectory_header_sequence_num": 0,
                        },
                    },
                    {
                        "ts_sec": 101.98,
                        "apollo_control_raw": {
                            "throttle": 0.0,
                            "brake": 15.0,
                            "steering_target": 0.0,
                            "debug_input_trajectory_header_sequence_num": 0,
                        },
                    },
                ],
            )
            (artifacts / "apollo_control.INFO").write_text(
                "Control waiting for first nonempty planning trajectory before enabling closed-loop control.\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_status.log").write_text(
                "runtime_filtered_lines: [\"123 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_status_final.log").write_text(
                "runtime_filtered_lines: [\"123 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control_route_established_wait.log").write_text(
                "\n".join(
                    [
                        "status=ready",
                        json.dumps(
                            {
                                "ts": 101.1,
                                "route_established_at": 101.1,
                                "routing_request_count": 1,
                                "routing_success_count": 1,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 101.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 111.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertEqual(finalized["route_health_label"], "route_established_but_no_control_progress")
            self.assertEqual(finalized["control_handoff_status"], "control_output_stopped_before_nonzero_planning")
            self.assertEqual(finalized["planning_route_seen_at"], 101.1)
            self.assertEqual(finalized["planning_first_nonempty_at"], 102.0)
            self.assertEqual(finalized["control_consume_row_count"], 1)
            self.assertTrue(finalized["apollo_control_process_alive"])
            self.assertEqual(finalized["apollo_control_raw_row_count"], 2)
            self.assertTrue(finalized["control_zero_hold_only"])
            self.assertEqual(finalized["control_last_input_trajectory_seq"], 0)
            self.assertEqual(finalized["control_last_input_trajectory_point_count"], 0)
            self.assertIsNone(finalized["control_first_nonzero_planning_seen_by_control_at"])
            self.assertEqual(finalized["control_last_output_ts"], 101.98)
            self.assertEqual(finalized["control_output_zero_hold_ratio"], 1.0)
            self.assertTrue(finalized["control_final_process_alive"])
            self.assertTrue(finalized["control_final_modules_status"]["control_present"])

    def test_finalize_town01_run_treats_missing_final_status_as_output_stopped_not_process_died(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health",
                    "comparison_label": "control_output_stopped_missing_final_status",
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "algo": {
                            "apollo": {
                                "docker": {
                                    "defer_control_until_planning_ready": False,
                                    "control_start_gate": "none",
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 97,
                    "goal_trace_index": 46,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {"planning_first_nonempty_ts_sec": 102.0})
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 101.0, "routing_request_sent": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 101.5,
                        "planning_header_timestamp_sec": 101.4,
                        "planning_header_sequence_num": 1,
                        "trajectory_point_count": 0,
                        "planning_message_parsed_successfully": True,
                    },
                    {
                        "timestamp": 102.0,
                        "planning_header_timestamp_sec": 101.9,
                        "planning_header_sequence_num": 2,
                        "trajectory_point_count": 111,
                        "planning_message_parsed_successfully": True,
                    },
                ],
            )
            _write_jsonl(
                artifacts / "control_trajectory_consume_debug_live.jsonl",
                [
                    {
                        "timestamp": 101.98,
                        "latest_planning_msg_timestamp": 101.95,
                        "latest_planning_msg_age_ms": 20.0,
                        "latest_planning_msg_sequence_num": 1,
                        "control_input_candidate_source": "missing",
                        "effective_planning_source": "latest_known_fallback",
                        "latest_planning_trajectory_point_count": 0,
                        "control_used_planning_trajectory": False,
                    }
                ],
            )
            _write_jsonl(
                artifacts / "apollo_control_raw.jsonl",
                [
                    {
                        "ts_sec": 101.97,
                        "apollo_control_raw": {
                            "throttle": 0.0,
                            "brake": 15.0,
                            "steering_target": 0.0,
                            "debug_input_trajectory_header_sequence_num": 0,
                        },
                    },
                    {
                        "ts_sec": 101.98,
                        "apollo_control_raw": {
                            "throttle": 0.0,
                            "brake": 15.0,
                            "steering_target": 0.0,
                            "debug_input_trajectory_header_sequence_num": 0,
                        },
                    },
                ],
            )
            (artifacts / "apollo_control.INFO").write_text(
                "Control waiting for first nonempty planning trajectory before enabling closed-loop control.\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_status.log").write_text(
                "runtime_filtered_lines: [\"123 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 101.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 111.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertEqual(finalized["route_health_label"], "route_established_but_no_control_progress")
            self.assertEqual(finalized["control_handoff_status"], "control_output_stopped_before_nonzero_planning")
            self.assertTrue(finalized["apollo_control_process_alive"])
            self.assertFalse(finalized["control_final_modules_status"]["exists"])
            self.assertFalse(finalized["control_final_process_alive"])

    def test_finalize_town01_run_detects_control_started_then_died_before_any_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health",
                    "comparison_label": "control_started_then_died_before_any_output",
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "algo": {
                            "apollo": {
                                "docker": {
                                    "defer_control_until_planning_ready": False,
                                    "control_start_gate": "route_established",
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 97,
                    "goal_trace_index": 46,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {"planning_first_nonempty_ts_sec": 102.0})
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 101.0, "routing_request_sent": True, "routing_success": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 101.5,
                        "planning_header_timestamp_sec": 101.4,
                        "planning_header_sequence_num": 1,
                        "trajectory_point_count": 0,
                        "planning_message_parsed_successfully": True,
                    },
                    {
                        "timestamp": 102.0,
                        "planning_header_timestamp_sec": 101.9,
                        "planning_header_sequence_num": 2,
                        "trajectory_point_count": 111,
                        "planning_message_parsed_successfully": True,
                    },
                ],
            )
            (artifacts / "apollo_control.INFO").write_text(
                "Control init, starting ...\nControl default driving action is START\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_status.log").write_text(
                "runtime_filtered_lines: [\"123 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_status_final.log").write_text(
                "runtime_filtered_lines: [\"456 mainboard -d modules/planning/planning_component/dag/planning.dag -p planning -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control_route_established_wait.log").write_text(
                "\n".join(
                    [
                        "status=ready",
                        json.dumps(
                            {
                                "ts": 101.1,
                                "route_established_at": 101.1,
                                "routing_request_count": 1,
                                "routing_success_count": 1,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            _write_json(
                artifacts / "apollo_control_deferred_survival.json",
                {
                    "control_started_pid_seen": True,
                    "control_survived_5s": False,
                    "control_survived_10s": False,
                    "first_nonzero_planning_seen_at": 102.0,
                    "control_present_after_first_nonzero_planning": False,
                    "control_present_at_end": False,
                    "samples": [],
                },
            )
            (artifacts / "apollo_control_deferred_start.log").write_text(
                "\n".join(
                    [
                        "preferred_start_mode: launch",
                        "actual_start_mode: launch",
                        "launch_log: /apollo_workspace/log/control.deferred.123.launch.log",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control_deferred_launch.log").write_text(
                "Segmentation fault\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control.INFO").write_text(
                "E0401 00:00:00.000000 1 control_component.cc:328] Chassis msg is not ready!\n"
                "I0401 00:00:00.010000 1 control_component.cc:111] Control waiting for first nonempty planning trajectory before enabling closed-loop control.\n"
                "E0401 00:00:00.020000 1 control_component.cc:328] Chassis msg is not ready!\n",
                encoding="utf-8",
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 101.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 111.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertEqual(finalized["route_health_label"], "route_established_but_no_control_progress")
            self.assertEqual(finalized["control_handoff_status"], "control_started_then_died_before_any_output")
            self.assertTrue(finalized["control_started_pid_seen"])
            self.assertFalse(finalized["control_survived_5s"])
            self.assertFalse(finalized["control_survived_10s"])
            self.assertFalse(finalized["control_present_after_first_nonzero_planning"])
            self.assertFalse(finalized["control_final_process_alive"])
            self.assertEqual(finalized["apollo_control_raw_row_count"], 0)
            self.assertEqual(finalized["control_consume_row_count"], 0)
            self.assertTrue(finalized["control_crash_detected"])
            self.assertEqual(finalized["control_crash_reason"], "segfault")
            self.assertEqual(finalized["control_deferred_start_mode"], "launch")
            self.assertEqual(
                finalized["control_deferred_log_path"],
                str(artifacts / "apollo_control_deferred_launch.log"),
            )
            self.assertFalse(finalized["control_output_materialized"])
            self.assertEqual(finalized["apollo_control_chassis_not_ready_count"], 2)
            self.assertEqual(finalized["apollo_control_waiting_for_planning_count"], 1)

    def test_finalize_town01_run_prefers_not_consuming_when_control_survived_without_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(
                run_dir / "summary.json",
                {
                    "profile_name": "town01_apollo_route_health",
                    "comparison_label": "control_survived_without_output",
                },
            )
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "algo": {
                            "apollo": {
                                "docker": {
                                    "defer_control_until_planning_ready": True,
                                    "control_start_gate": "planning_ready",
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 97,
                    "goal_trace_index": 46,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {"planning_first_nonempty_ts_sec": 102.0})
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 101.0, "routing_request_sent": True, "routing_success": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 102.0,
                        "planning_header_timestamp_sec": 101.9,
                        "planning_header_sequence_num": 2,
                        "trajectory_point_count": 111,
                        "planning_message_parsed_successfully": True,
                    },
                ],
            )
            (artifacts / "apollo_modules_status.log").write_text(
                "runtime_filtered_lines: [\"123 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_status_final.log").write_text(
                "runtime_filtered_lines: [\"456 mainboard -d modules/planning/planning_component/dag/planning.dag -p planning -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control_planning_ready_wait.log").write_text(
                "\n".join(
                    [
                        "status=ready",
                        json.dumps(
                            {
                                "ts": 101.1,
                                "route_seen_at": 101.0,
                                "route_established_at": 101.0,
                                "planning_nonempty_trajectory_count": 1,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            _write_json(
                artifacts / "apollo_control_deferred_survival.json",
                {
                    "control_started_pid_seen": True,
                    "control_survived_5s": True,
                    "control_survived_10s": True,
                    "first_nonzero_planning_seen_at": 102.0,
                    "control_present_after_first_nonzero_planning": True,
                    "control_present_at_end": True,
                    "samples": [],
                },
            )
            (artifacts / "apollo_control_deferred_start.log").write_text(
                "\n".join(
                    [
                        "preferred_start_mode: launch",
                        "actual_start_mode: launch",
                        "launch_log: /apollo_workspace/log/control.deferred.123.launch.log",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control_deferred_launch.log").write_text(
                "src/tcmalloc.cc:333] Attempt to free invalid pointer 0x1234\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control.INFO").write_text(
                "E0401 00:00:00.000000 1 control_component.cc:328] Chassis msg is not ready!\n"
                "E0401 00:00:00.010000 1 control_component.cc:328] Chassis msg is not ready!\n",
                encoding="utf-8",
            )
            _write_json(
                artifacts / "command_materialization_summary.json",
                {
                    "command_path_stage": "planning_ready_no_control_rx",
                    "first_divergence_layer": "control_materialization",
                    "first_divergence_reason": "control_message_pending",
                },
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 101.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 111.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertEqual(finalized["control_handoff_status"], "planning_ready_control_not_consuming")
            self.assertEqual(finalized["command_materialization_stage"], "planning_ready_no_control_rx")
            self.assertEqual(finalized["command_materialization_layer"], "control_materialization")
            self.assertEqual(finalized["command_materialization_reason"], "control_message_pending")
            self.assertTrue(finalized["control_started_pid_seen"])
            self.assertTrue(finalized["control_survived_5s"])
            self.assertTrue(finalized["control_survived_10s"])
            self.assertTrue(finalized["control_present_after_first_nonzero_planning"])
            self.assertFalse(finalized["control_final_process_alive"])
            self.assertEqual(finalized["control_crash_reason"], "tcmalloc_invalid_free")
            self.assertFalse(finalized["control_output_materialized"])
            self.assertEqual(finalized["apollo_control_chassis_not_ready_count"], 2)

    def test_finalize_town01_run_classifies_tcmalloc_invalid_free_control_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            artifacts = run_dir / "artifacts"

            _write_json(run_dir / "summary.json", {"profile_name": "town01_apollo_route_health"})
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump({"algo": {"apollo": {"docker": {"control_start_gate": "planning_ready"}}}}),
                encoding="utf-8",
            )
            _write_json(
                artifacts / "scenario_metadata.json",
                {
                    "used_spawn_idx": 97,
                    "goal_trace_index": 46,
                    "spawn": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "goal": {"x": 200.0, "y": 0.0, "z": 0.0},
                    "route_length_m": 200.0,
                },
            )
            _write_json(artifacts / "bridge_health_summary.json", {"planning_first_nonempty_ts_sec": 102.0})
            _write_json(artifacts / "cyber_bridge_stats.json", {"routing_request_count": 1, "routing_success_count": 1})
            _write_jsonl(
                artifacts / "routing_event_debug.jsonl",
                [{"timestamp": 101.0, "routing_request_sent": True, "routing_success": True}],
            )
            _write_jsonl(
                artifacts / "planning_topic_debug.jsonl",
                [
                    {
                        "timestamp": 102.0,
                        "planning_header_timestamp_sec": 101.9,
                        "planning_header_sequence_num": 2,
                        "trajectory_point_count": 111,
                        "planning_message_parsed_successfully": True,
                    },
                ],
            )
            (artifacts / "apollo_modules_status.log").write_text(
                "runtime_filtered_lines: [\"123 mainboard -d modules/control/control_component/dag/control.dag -p control -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_modules_status_final.log").write_text(
                "runtime_filtered_lines: [\"456 mainboard -d modules/planning/planning_component/dag/planning.dag -p planning -s CYBER_DEFAULT\"]\n",
                encoding="utf-8",
            )
            _write_json(
                artifacts / "apollo_control_deferred_survival.json",
                {
                    "control_started_pid_seen": True,
                    "control_survived_5s": False,
                    "control_survived_10s": False,
                    "control_present_after_first_nonzero_planning": False,
                },
            )
            (artifacts / "apollo_control_deferred_start.log").write_text(
                "\n".join(
                    [
                        "preferred_start_mode: dag",
                        "actual_start_mode: dag",
                        "direct_log: /apollo_workspace/log/control.deferred.123.tb.log",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "apollo_control_deferred_mainboard.log").write_text(
                "src/tcmalloc.cc:333] Attempt to free invalid pointer 0x1234\n",
                encoding="utf-8",
            )
            _write_csv(
                artifacts / "debug_timeseries.csv",
                [
                    {
                        "ts_sec": 101.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                    {
                        "ts_sec": 111.0,
                        "map_x": 0.0,
                        "map_y": 0.0,
                        "speed_mps": 0.0,
                        "front_obstacle_gap_lon_m": 100.0,
                        "routing_established": "true",
                        "terminal_stop_hold_active": "false",
                    },
                ],
            )

            finalized = finalize_town01_run(run_dir)

            self.assertTrue(finalized["control_crash_detected"])
            self.assertEqual(finalized["control_crash_reason"], "tcmalloc_invalid_free")
            self.assertEqual(finalized["control_deferred_start_mode"], "dag")
            self.assertEqual(
                finalized["control_deferred_log_path"],
                str(artifacts / "apollo_control_deferred_mainboard.log"),
            )

    def test_alignment_detects_live_control_stopping_before_first_nonzero(self) -> None:
        planning_rows = [
            {
                "timestamp": 100.00,
                "planning_header_sequence_num": 0,
                "trajectory_point_count": 0,
                "planning_message_parsed_successfully": True,
            },
            {
                "timestamp": 100.05,
                "planning_header_sequence_num": 1,
                "trajectory_point_count": 12,
                "planning_message_parsed_successfully": True,
            },
        ]
        live_control_rows = [
            {
                "timestamp": 100.015,
                "latest_planning_msg_sequence_num": 0,
                "latest_planning_trajectory_point_count": 0,
            }
        ]
        control_events = [
            {"event_type": "control_success", "timestamp": 100.015},
            {"event_type": "control_no_trajectory", "timestamp": 100.08},
        ]
        alignment = _compute_planning_control_alignment(planning_rows, live_control_rows, control_events)
        self.assertTrue(alignment["live_control_stops_before_first_nonzero_planning"])
        self.assertEqual(alignment["first_nonzero_planning_seq"], 1)
        self.assertEqual(alignment["last_live_control_planning_seq"], 0)
        self.assertEqual(alignment["control_no_trajectory_after_first_nonzero"], 1)
        self.assertIsNone(alignment["last_live_control_candidate_trajectory_header_sequence_num"])
        self.assertFalse(alignment["exact_candidate_match_seen"])

    def test_compute_route_metrics_uses_map_space_goal_candidate_when_y_axis_is_mirrored(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            (run_dir / "effective.yaml").write_text(
                yaml.safe_dump(
                    {
                        "algo": {
                            "apollo": {
                                "carla_to_apollo": {
                                    "tx": 0.0,
                                    "ty": 0.0,
                                    "tz": 0.0,
                                    "yaw_deg": 0.0,
                                    "auto_calib": False,
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            scenario_meta = {
                "spawn": {"x": 396.6375732421875, "y": 318.38226318359375, "z": 0.28530004620552063},
                "goal": {"x": 396.2877197265625, "y": 88.3827133178711, "z": 0.0},
                "route_length_m": 229.99964983584644,
                "spawn_lane": {"road_id": 8, "section_id": 0, "lane_id": -1},
                "goal_lane": {"road_id": 8, "section_id": 0, "lane_id": -1},
            }
            debug_rows = [
                {"map_x": "396.6375717257836", "map_y": "-319.80576318359294", "speed_mps": "1.96"},
                {"map_x": "395.7", "map_y": "-315.0", "speed_mps": "2.5"},
                {"map_x": "394.72645962620453", "map_y": "-310.13501212259564", "speed_mps": "0.0"},
            ]
            metrics = _compute_route_metrics(run_dir, {}, scenario_meta, debug_rows)
            self.assertTrue(metrics["available"])
            self.assertGreater(metrics["route_distance_achieved_m"], 9.0)
            self.assertGreater(metrics["route_completion_ratio"], 0.03)
            self.assertLess(metrics["final_goal_distance_m"], metrics["start_goal_distance_m"])


class CarlaLauncherTests(unittest.TestCase):
    def test_candidate_carla_ports_cover_rpc_triplet(self) -> None:
        self.assertEqual(_candidate_carla_ports(2000), [2000, 2001, 2002])

    def test_carla_pids_matching_port_arg_filters_by_rpc_port(self) -> None:
        output = "\n".join(
            [
                "101 /home/ubuntu/CARLA_0.9.16/CarlaUE4/Binaries/Linux/CarlaUE4-Linux-Shipping CarlaUE4 -carla-rpc-port=2000 --ros2",
                "102 bash /home/ubuntu/CARLA_0.9.16/CarlaUE4.sh -carla-rpc-port=2000 --ros2",
                "103 /home/ubuntu/CARLA_0.9.16/CarlaUE4/Binaries/Linux/CarlaUE4-Linux-Shipping CarlaUE4 -carla-rpc-port=3000 --ros2",
            ]
        )
        with mock.patch("tbio.carla.launcher.subprocess.check_output", return_value=output):
            self.assertEqual(_carla_pids_matching_port_arg(2000), [101, 102])

    def test_port_likely_owned_by_carla_checks_port_owner_not_global_pgrep(self) -> None:
        with (
            mock.patch("tbio.carla.launcher._ss_lines_for_port", return_value=['LISTEN 0 5 *:2001 users:(("python",pid=123,fd=7))']),
            mock.patch("tbio.carla.launcher._port_owner_pid", return_value=123),
            mock.patch("tbio.carla.launcher._pid_cmdline", return_value="python some_other_server.py"),
        ):
            self.assertFalse(_port_likely_owned_by_carla(2001))

    def test_cleanup_stale_carla_ports_deduplicates_same_pid_across_triplet(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
            )
            with (
                mock.patch("tbio.carla.launcher._carla_pid_on_port", side_effect=[111, 111, None]),
                mock.patch("tbio.carla.launcher._carla_pids_matching_port_arg", return_value=[111]),
                mock.patch("tbio.carla.launcher._pid_cmdline", return_value="/fake/CarlaUE4-Linux-Shipping"),
                mock.patch("tbio.carla.launcher._terminate_pid", return_value=True) as terminate_mock,
                mock.patch("tbio.carla.launcher.time.sleep"),
            ):
                launcher._cleanup_stale_carla_ports()
            terminate_mock.assert_called_once_with(111)

    def test_cleanup_stale_carla_ports_kills_non_listening_matching_port_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
            )
            with (
                mock.patch("tbio.carla.launcher._carla_pid_on_port", side_effect=[None, None, None]),
                mock.patch("tbio.carla.launcher._carla_pids_matching_port_arg", return_value=[222]),
                mock.patch("tbio.carla.launcher._pid_cmdline", return_value="/fake/CarlaUE4-Linux-Shipping -carla-rpc-port=2000"),
                mock.patch("tbio.carla.launcher._terminate_pid", return_value=True) as terminate_mock,
                mock.patch("tbio.carla.launcher.time.sleep"),
            ):
                launcher._cleanup_stale_carla_ports()
            terminate_mock.assert_called_once_with(222)

    def test_force_fresh_start_skips_reuse_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                force_fresh_start=True,
            )
            proc = mock.Mock()
            proc.pid = 999
            with (
                mock.patch("tbio.carla.launcher._client_ok", return_value=True) as client_ok_mock,
                mock.patch.object(launcher, "_cleanup_stale_carla_ports") as cleanup_mock,
                mock.patch("tbio.carla.launcher._is_port_open", return_value=False),
                mock.patch.object(launcher, "_build_env", return_value={}),
                mock.patch.object(launcher, "_command", return_value=["fake-carla"]),
                mock.patch("tbio.carla.launcher.open", mock.mock_open()),
                mock.patch("tbio.carla.launcher.subprocess.Popen", return_value=proc),
            ):
                launcher.start()
            client_ok_mock.assert_not_called()
            cleanup_mock.assert_called_once()
            self.assertFalse(launcher.reused)

    def test_wait_ready_retries_once_with_render_offscreen_after_early_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            exited_proc = mock.Mock()
            exited_proc.poll.return_value = 1
            exited_proc.returncode = 1
            running_proc = mock.Mock()
            running_proc.poll.return_value = None
            launcher.proc = exited_proc

            def fake_start() -> None:
                launcher.proc = running_proc

            with (
                mock.patch.object(launcher, "start", side_effect=fake_start) as start_mock,
                mock.patch("tbio.carla.launcher._client_ok", return_value=True),
                mock.patch("tbio.carla.launcher._is_port_open", return_value=True),
                mock.patch("tbio.carla.launcher.time.sleep"),
            ):
                self.assertTrue(launcher.wait_ready(timeout_s=1.0, poll_s=0.0))
            self.assertTrue(launcher._render_offscreen_retry_used)
            self.assertTrue(launcher._render_offscreen_forced)
            start_mock.assert_called_once()

    def test_wait_ready_retries_once_with_lowres_window_after_vulkan_oom(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            exited_proc = mock.Mock()
            exited_proc.poll.return_value = 139
            exited_proc.returncode = 139
            running_proc = mock.Mock()
            running_proc.poll.return_value = None
            launcher.proc = exited_proc

            def fake_start() -> None:
                launcher.proc = running_proc

            with (
                mock.patch.object(launcher, "start", side_effect=fake_start) as start_mock,
                mock.patch.object(
                    launcher,
                    "_tail_file",
                    side_effect=[["Out of memory on Vulkan; MemoryTypeIndex=2, AllocSize=512.000MB"], []],
                ),
                mock.patch("tbio.carla.launcher._client_ok", return_value=True),
                mock.patch("tbio.carla.launcher._is_port_open", return_value=True),
                mock.patch("tbio.carla.launcher.time.sleep"),
            ):
                self.assertTrue(launcher.wait_ready(timeout_s=1.0, poll_s=0.0))
            self.assertTrue(launcher._vulkan_oom_windowed_retry_used)
            self.assertTrue(launcher._vulkan_oom_windowed_recovery_applied)
            self.assertFalse(launcher._render_offscreen_retry_used)
            start_mock.assert_called_once()

    def test_wait_ready_retries_once_with_lowres_low_quality_after_listener_dead_hang(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            initial_proc = mock.Mock()
            initial_proc.poll.return_value = None
            recovered_proc = mock.Mock()
            recovered_proc.poll.return_value = None
            launcher.proc = initial_proc
            launcher._launch_records.append({"mode": "launch"})

            def fake_restart() -> None:
                launcher._lowres_low_quality_recovery_applied = True
                launcher._lowres_low_quality_retry_used = True
                launcher.proc = recovered_proc
                launcher._launch_records.append({"mode": "launch", "recovery": "lowres_low_quality"})

            with (
                mock.patch.object(launcher, "_hung_startup_retry_after_sec", return_value=0.0),
                mock.patch(
                    "tbio.carla.launcher._is_port_open",
                    side_effect=[False, False, False, True, True, True],
                ),
                mock.patch.object(launcher, "_launch_tail_contains_stream_eof", return_value=False),
                mock.patch.object(
                    launcher,
                    "_restart_with_lowres_low_quality_recovery",
                    side_effect=fake_restart,
                ) as restart_mock,
                mock.patch("tbio.carla.launcher._client_ok", return_value=True),
                mock.patch("tbio.carla.launcher.time.sleep"),
            ):
                self.assertTrue(launcher.wait_ready(timeout_s=1.0, poll_s=0.0))
        self.assertTrue(launcher._lowres_low_quality_retry_used)
        self.assertTrue(launcher._lowres_low_quality_recovery_applied)
        restart_mock.assert_called_once()

    def test_command_prefers_render_offscreen_for_monitorless_display(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            with mock.patch.object(
                launcher,
                "_probe_display_state",
                return_value={"available": True, "monitor_count": 0, "display": ":0"},
            ):
                env = launcher._build_env()
            cmd = launcher._command(env)
            self.assertIn("-RenderOffScreen", cmd)
            self.assertNotIn("-windowed", cmd)

    def test_command_omits_carla_map_when_town_not_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="",
                extra_args="-RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            with mock.patch.object(
                launcher,
                "_probe_display_state",
                return_value={"available": True, "monitor_count": 1, "display": ":0"},
            ):
                env = launcher._build_env()
            cmd = launcher._command(env)
            self.assertNotIn("-carla-map=Town01", cmd)
            self.assertNotIn("-carla-map=", cmd)
            self.assertIn("-RenderOffScreen", cmd)

    def test_build_env_for_explicit_render_offscreen_keeps_display_path_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="-RenderOffScreen -quality-level=Low",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            with mock.patch.object(
                launcher,
                "_probe_display_state",
                return_value={"available": True, "monitor_count": 1, "display": ":0"},
            ):
                env = launcher._build_env()
            self.assertNotEqual(env.get("SDL_VIDEODRIVER"), "offscreen")
            self.assertEqual(env.get("DISPLAY"), ":0")

    def test_build_env_can_force_pure_headless_even_if_display_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="-RenderOffScreen -quality-level=Low",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={
                    "DISPLAY": "",
                    "WAYLAND_DISPLAY": "",
                    "XAUTHORITY": "",
                },
            )
            with mock.patch.object(launcher, "_probe_display_state") as probe_mock:
                env = launcher._build_env()
            self.assertEqual(env.get("DISPLAY"), "")
            self.assertEqual(env.get("WAYLAND_DISPLAY"), "")
            self.assertIsNone(env.get("SDL_VIDEODRIVER"))
            probe_mock.assert_not_called()

    def test_wait_ready_monitorless_lowres_hang_retries_with_render_offscreen(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            running_proc = mock.Mock()
            running_proc.poll.return_value = None
            recovered_proc = mock.Mock()
            recovered_proc.poll.return_value = None
            launcher.proc = running_proc
            launcher._display_probe = {"available": True, "monitor_count": 0, "display": ":0"}
            launcher._launch_records.append(
                {
                    "mode": "launch",
                    "command": [
                        "bash",
                        "/tmp/fake_carla_root/CarlaUE4.sh",
                        "-windowed",
                        "-ResX=960",
                        "-ResY=540",
                        "-quality-level=Low",
                    ],
                }
            )

            def fake_start() -> None:
                launcher.proc = recovered_proc
                launcher._launch_records.append({"mode": "launch", "command": ["-RenderOffScreen"]})

            with (
                mock.patch.object(launcher, "_hung_startup_retry_after_sec", return_value=0.0),
                mock.patch("tbio.carla.launcher._is_port_open", side_effect=[False, False, False, True, True, True]),
                mock.patch("tbio.carla.launcher._client_ok", return_value=True),
                mock.patch("tbio.carla.launcher.time.sleep"),
                mock.patch.object(launcher, "start", side_effect=fake_start) as start_mock,
            ):
                self.assertTrue(launcher.wait_ready(timeout_s=1.0, poll_s=0.0))
            self.assertTrue(launcher._render_offscreen_retry_used)
            self.assertTrue(launcher._render_offscreen_forced)
            start_mock.assert_called_once()

    def test_wait_ready_render_offscreen_launch_does_not_retry_back_into_lowres(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="-RenderOffScreen -quality-level=Low",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            running_proc = mock.Mock()
            running_proc.poll.return_value = None
            launcher.proc = running_proc
            launcher._display_probe = {"available": True, "monitor_count": 0, "display": ":0"}
            launcher._launch_records.append(
                {
                    "mode": "launch",
                    "command": [
                        "bash",
                        "/tmp/fake_carla_root/CarlaUE4.sh",
                        "-RenderOffScreen",
                        "-quality-level=Low",
                    ],
                }
            )

            with (
                mock.patch.object(launcher, "_hung_startup_retry_after_sec", return_value=0.0),
                mock.patch("tbio.carla.launcher._is_port_open", return_value=False),
                mock.patch("tbio.carla.launcher.time.sleep"),
                mock.patch.object(launcher, "_restart_with_lowres_low_quality_recovery") as lowres_restart_mock,
            ):
                self.assertFalse(launcher.wait_ready(timeout_s=1.0, poll_s=0.0))
            lowres_restart_mock.assert_not_called()

    def test_wait_ready_fails_fast_on_ports_open_eof_alive_hang(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
            )
            running_proc = mock.Mock()
            running_proc.poll.return_value = None
            launcher.proc = running_proc
            launcher._launch_records.append({"mode": "launch"})

            with (
                mock.patch.object(launcher, "_hung_startup_retry_after_sec", return_value=0.0),
                mock.patch("tbio.carla.launcher._is_port_open", return_value=True),
                mock.patch.object(launcher, "_launch_tail_contains_stream_eof", return_value=True),
                mock.patch.object(launcher, "diagnose_tail", return_value="[carla] diag"),
                mock.patch("tbio.carla.launcher._client_ok", return_value=False),
                mock.patch("tbio.carla.launcher.time.sleep"),
            ):
                self.assertFalse(launcher.wait_ready(timeout_s=1.0, poll_s=0.0))
            self.assertEqual(
                launcher._launch_records[-1].get("early_fail_reason"),
                "rpc_handshake_dead_with_eof_alive",
            )

    def test_wait_ready_without_auto_recovery_does_not_fail_fast_on_ports_open_eof_alive_hang(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={"DISPLAY": ":0"},
                enable_auto_recovery=False,
            )
            running_proc = mock.Mock()
            running_proc.poll.return_value = None
            launcher.proc = running_proc
            launcher._launch_records.append({"mode": "launch"})
            timeline = iter([0.0, 0.0, 0.01, 0.01, 0.02, 0.02])
            with (
                mock.patch("tbio.carla.launcher.time.time", side_effect=lambda: next(timeline)),
                mock.patch.object(launcher, "_hung_startup_retry_after_sec", return_value=0.0),
                mock.patch("tbio.carla.launcher._is_port_open", return_value=True),
                mock.patch.object(launcher, "_launch_tail_contains_stream_eof", return_value=True),
                mock.patch("tbio.carla.launcher._client_ok", return_value=False),
                mock.patch("tbio.carla.launcher.time.sleep"),
            ):
                self.assertFalse(launcher.wait_ready(timeout_s=0.02, poll_s=0.0))
            self.assertIsNone(launcher._launch_records[-1].get("early_fail_reason"))

    def test_stop_sweeps_port_arg_matched_pids_even_without_open_ports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
            )
            with (
                mock.patch("tbio.carla.launcher._carla_pids_matching_port_arg", return_value=[333, 444]),
                mock.patch("tbio.carla.launcher._carla_pid_on_port", return_value=None),
                mock.patch("tbio.carla.launcher._terminate_pid", return_value=True) as terminate_mock,
            ):
                launcher.stop()
            self.assertEqual(
                [call.args[0] for call in terminate_mock.call_args_list],
                [333, 444],
            )

    def test_diagnostics_snapshot_includes_live_tail_and_process_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
            )
            launcher._server_log.write_text("a\nb\nc\n", encoding="utf-8")
            launcher._ue_log.parent.mkdir(parents=True, exist_ok=True)
            launcher._ue_log.write_text("ue1\nue2\n", encoding="utf-8")
            proc = mock.Mock()
            proc.pid = 4321
            proc.poll.return_value = None
            launcher.proc = proc

            with (
                mock.patch("tbio.carla.launcher._is_port_open", return_value=False),
                mock.patch("tbio.carla.launcher._ss_lines_for_port", return_value=[]),
                mock.patch("tbio.carla.launcher._client_ok", return_value=False),
            ):
                snapshot = launcher.diagnostics_snapshot()

            self.assertEqual(snapshot["process_pid"], 4321)
            self.assertTrue(snapshot["process_alive"])
            self.assertEqual(snapshot["latest_server_log_tail"], ["a", "b", "c"])
            self.assertEqual(snapshot["latest_ue_log_tail"], ["ue1", "ue2"])
            self.assertIn("bootstrap_stability_window_s", snapshot)
            self.assertIn("memory_guard_enabled", snapshot)
            self.assertIn("launcher_log_contains_end_of_file", snapshot)

    def test_diagnostics_snapshot_can_skip_rpc_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
            )
            with (
                mock.patch("tbio.carla.launcher._is_port_open", return_value=False),
                mock.patch("tbio.carla.launcher._ss_lines_for_port", return_value=[]),
                mock.patch("tbio.carla.launcher._client_ok") as client_ok_mock,
            ):
                snapshot = launcher.diagnostics_snapshot(probe_rpc=False)
            self.assertIsNone(snapshot["rpc_handshake_ready"])
            client_ok_mock.assert_not_called()

    def test_build_env_sets_malloc_arena_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
            )
            env = launcher._build_env()
            self.assertEqual(env["MALLOC_ARENA_MAX"], "2")

    def test_command_includes_nosound(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="-RenderOffScreen",
                foreground=False,
                run_dir=Path(tmpdir),
            )
            cmd = launcher._command({"SDL_VIDEODRIVER": "offscreen"})
            self.assertIn("-nosound", cmd)

    def test_start_wraps_launch_in_user_scope_memory_guard_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="-RenderOffScreen",
                foreground=False,
                run_dir=Path(tmpdir),
                use_systemd_scope_memory_guard=True,
                memory_high="9G",
                memory_max="10G",
            )
            fake_proc = mock.Mock()
            fake_proc.pid = 4567
            fake_proc.poll.return_value = None
            with (
                mock.patch("tbio.carla.launcher._client_ok", return_value=False),
                mock.patch("tbio.carla.launcher._is_port_open", return_value=False),
                mock.patch.object(launcher, "_cleanup_stale_carla_ports"),
                mock.patch("tbio.carla.launcher.shutil.which", return_value="/usr/bin/systemd-run"),
                mock.patch.dict("tbio.carla.launcher.os.environ", {"XDG_RUNTIME_DIR": "/run/user/1000"}, clear=False),
                mock.patch("tbio.carla.launcher.subprocess.run") as subprocess_run_mock,
                mock.patch("tbio.carla.launcher.subprocess.Popen", return_value=fake_proc) as popen_mock,
            ):
                launcher.start()
                self.assertTrue(launcher._systemd_scope_unit)
                launcher.stop()
            subprocess_run_mock.assert_called()
            launched_cmd = popen_mock.call_args.args[0]
            self.assertEqual(launched_cmd[0], "systemd-run")
            self.assertIn("MemoryHigh=9G", launched_cmd)
            self.assertIn("MemoryMax=10G", launched_cmd)

    def test_build_env_records_display_probe_even_with_explicit_sdl_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            launcher = CarlaLauncher(
                carla_root=Path("/tmp/fake_carla_root"),
                host="127.0.0.1",
                port=2000,
                town="Town01",
                extra_args="",
                foreground=False,
                run_dir=Path(tmpdir),
                env_overrides={
                    "DISPLAY": ":99",
                    "SDL_VIDEODRIVER": "x11",
                    "SDL_VIDEO_X11_REQUIRE_XRANDR": "0",
                },
            )
            with mock.patch.object(
                launcher,
                "_probe_display_state",
                return_value={"available": True, "monitor_count": 1, "display": ":99"},
            ) as probe_mock:
                env = launcher._build_env()
            self.assertEqual(env["DISPLAY"], ":99")
            self.assertEqual(env["SDL_VIDEODRIVER"], "x11")
            self.assertEqual(launcher._display_probe, {"available": True, "monitor_count": 1, "display": ":99"})
            probe_mock.assert_called_once()

    def test_classify_attempt_passive_eof_bridge(self) -> None:
        row = {
            "status": "passive_wait_complete",
            "probe_style": "passive",
            "launcher_diagnostics": {
                "target_port_snapshot": [{"port": 2000, "open": True, "ss_lines": ["listener"]}],
                "latest_server_log_tail": [
                    "ERROR: session 0 : error retrieving stream id : End of file ",
                    "ERROR: Primary server: failed to read header: End of file ",
                ],
                "launch_records": [],
                "rpc_handshake_ready": None,
            },
        }
        self.assertEqual(_classify_attempt(row), "passive_eof_bridge")

    def test_classify_attempt_uses_canonical_failure_family_for_active_probe(self) -> None:
        row = {
            "status": "world_not_ready",
            "probe_style": "active",
            "rpc_ready": True,
            "launcher_diagnostics": {
                "process_alive": True,
                "latest_server_log_tail": [],
                "target_port_snapshot": [
                    {"port": 2000, "open": True, "ss_lines": ["listener"]},
                    {"port": 2001, "open": True, "ss_lines": ["listener"]},
                    {"port": 2002, "open": True, "ss_lines": ["listener"]},
                ],
            },
        }
        self.assertEqual(_classify_attempt(row), "rpc_ready_world_not_ready_alive_no_eof")

    def test_classify_handshake_effect_marks_post_handshake_eof(self) -> None:
        pre_tail = ['[init] sg.PostProcessQuality = "0"']
        post_tail = [
            '[init] sg.PostProcessQuality = "0"',
            'ERROR: session 0 : error retrieving stream id : End of file ',
        ]
        self.assertEqual(
            _classify_handshake_effect(pre_tail, post_tail, post_ports_open=True),
            "handshake_triggered_eof_bridge",
        )

    def test_startup_mode_presets_include_no_ros2_controls(self) -> None:
        self.assertEqual(MODE_PRESETS["render_offscreen_no_ros2"]["extra_args"], "-RenderOffScreen")
        self.assertEqual(
            MODE_PRESETS["lowres_low_quality_no_ros2"]["extra_args"],
            "-windowed -ResX=960 -ResY=540 -quality-level=Low",
        )

    def test_handshake_report_handles_preflight_failure_without_pre_post(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "probe_report.md"
            _write_report(
                report_path,
                {
                    "mode": "lowres_low_quality",
                    "extra_args": "--ros2 -windowed -ResX=960 -ResY=540 -quality-level=Low",
                    "python_executable": "/usr/bin/python3",
                    "python_version": "3.13.0",
                    "passive_wait_sec": 60.0,
                    "post_handshake_wait_sec": 5.0,
                    "handshake_status": "preflight_import_failed",
                    "handshake_error": "ModuleNotFoundError('carla wheel missing')",
                },
            )
            text = report_path.read_text(encoding="utf-8")
            self.assertIn("preflight_import_failed", text)
            self.assertIn("/usr/bin/python3", text)


class ApolloBridgeSteeringFieldTests(unittest.TestCase):
    def test_select_steering_field_normalizes_percentage_fields(self) -> None:
        module = _load_bridge_unit_test_module()
        bridge_cls = module.ApolloGtBridge

        class _FakeBridge:
            physical_steer_field_priority = ["steering_target", "steering_percentage", "steering", "steering_rate"]

            @staticmethod
            def _coerce_float(value, default, field, source):
                try:
                    return float(value)
                except Exception:
                    return float(default)

        field, value = bridge_cls._select_steering_field(
            _FakeBridge(),
            {"steering_target": 1.0},
            physical_mode=False,
        )
        self.assertEqual(field, "steering_target")
        self.assertAlmostEqual(value, 0.01)

        field, value = bridge_cls._select_steering_field(
            _FakeBridge(),
            {"steering_percentage": -25.0},
            physical_mode=True,
        )
        self.assertEqual(field, "steering_percentage")
        self.assertAlmostEqual(value, -0.25)

    def test_select_steering_field_can_pin_legacy_double_percent_policy(self) -> None:
        module = _load_bridge_unit_test_module()
        bridge_cls = module.ApolloGtBridge

        class _FakeBridge:
            physical_steer_field_priority = ["steering_target", "steering_percentage", "steering", "steering_rate"]
            steering_percent_normalization = "legacy_double_percent"

            @staticmethod
            def _coerce_float(value, default, field, source):
                try:
                    return float(value)
                except Exception:
                    return float(default)

        field, value = bridge_cls._select_steering_field(
            _FakeBridge(),
            {"steering_target": 10.0},
            physical_mode=False,
        )
        self.assertEqual(field, "steering_target")
        self.assertAlmostEqual(value, 0.001)

    def test_normalize_steering_command_only_clamps_selected_normalized_value(self) -> None:
        module = _load_bridge_unit_test_module()
        bridge_cls = module.ApolloGtBridge

        self.assertAlmostEqual(
            bridge_cls._normalize_steering_command(1.0, physical_mode=False),
            1.0,
        )
        self.assertAlmostEqual(
            bridge_cls._normalize_steering_command(1.0, physical_mode=True),
            1.0,
        )
        self.assertAlmostEqual(
            bridge_cls._normalize_steering_command(-0.25, physical_mode=True),
            -0.25,
        )
        self.assertAlmostEqual(
            bridge_cls._normalize_steering_command(2.0, physical_mode=False),
            1.0,
        )

    def test_steering_normalization_mode_is_self_describing(self) -> None:
        module = _load_bridge_unit_test_module()

        self.assertEqual(
            module.steering_normalization_mode_impl("steering_target"),
            "single_percent_at_select",
        )
        self.assertEqual(
            module.steering_normalization_mode_impl(
                "steering_target",
                percent_normalization_mode="legacy_double_percent",
            ),
            "legacy_double_percent",
        )
        self.assertEqual(
            module.steering_normalization_mode_impl("steering_percentage"),
            "single_percent_at_select",
        )
        self.assertEqual(
            module.steering_normalization_mode_impl("steering"),
            "field_value_clamp_only",
        )

    def test_legacy_steering_target_is_normalized_once_before_scale(self) -> None:
        module = _load_bridge_unit_test_module()
        bridge_cls = module.ApolloGtBridge

        class _FakeBridge:
            physical_steer_field_priority = ["steering_target", "steering_percentage", "steering", "steering_rate"]

            throttle_scale = 1.0
            brake_scale = 1.0
            steer_scale = 0.25
            steer_sign = 1.0
            brake_deadzone = 0.0
            physical_allow_legacy_fallback = True
            physical_apollo_max_steer_angle_deg = 8.203
            physical_apollo_max_accel_mps2 = 3.0
            physical_apollo_max_decel_mps2 = 8.0
            physical_use_top_level_acceleration = False
            physical_use_lon_debug = False
            physical_steer_field_priority = ["steering_target", "steering_percentage", "steering", "steering_rate"]
            physical_acceleration_field_priority = []

            @staticmethod
            def _coerce_float(value, default, field, source):
                try:
                    return float(value)
                except Exception:
                    return float(default)

            @staticmethod
            def _apply_zero_hold(throttle_cmd, brake_cmd, now_sec):
                return throttle_cmd

        fake = _FakeBridge()
        field, selected = bridge_cls._select_steering_field(
            fake,
            {"steering_target": 10.0},
            physical_mode=False,
        )
        raw_steer = bridge_cls._normalize_steering_command(selected, physical_mode=False)
        mapped = module.legacy_map_base_controls_impl(
            raw_throttle=0.0,
            raw_brake=0.0,
            raw_steer=raw_steer,
            now_sec=1.0,
            config=module.ControlMappingConfig(
                throttle_scale=1.0,
                brake_scale=1.0,
                steer_scale=0.25,
                steer_sign=1.0,
                brake_deadzone=0.0,
                throttle_brake_mutual_exclusion_enabled=True,
                throttle_brake_hysteresis_frames=2,
                throttle_brake_min_command=0.01,
                physical_allow_legacy_fallback=True,
                physical_apollo_max_steer_angle_deg=8.203,
                physical_apollo_max_accel_mps2=3.0,
                physical_apollo_max_decel_mps2=8.0,
                physical_use_top_level_acceleration=False,
                physical_use_lon_debug=False,
                physical_steer_field_priority=tuple(fake.physical_steer_field_priority),
                physical_acceleration_field_priority=(),
            ),
            apply_zero_hold=fake._apply_zero_hold,
        )

        self.assertEqual(field, "steering_target")
        self.assertAlmostEqual(selected, 0.10)
        self.assertAlmostEqual(raw_steer, 0.10)
        self.assertAlmostEqual(mapped["mapped_carla_steer_cmd"], 0.025)

    def test_extract_road_section_lane_metadata_tracks_junction_membership(self) -> None:
        module = _load_bridge_unit_test_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            map_path = Path(tmpdir) / "base_map.txt"
            map_path.write_text(
                "\n".join(
                    [
                        "road {",
                        "  id {",
                        '    id: "11"',
                        "  }",
                        "  section {",
                        "    id {",
                        '      id: "1"',
                        "    }",
                        "    lane_id {",
                        '      id: "11_1_-1"',
                        "    }",
                        "    lane_id {",
                        '      id: "11_2_-1"',
                        "    }",
                        "  }",
                        "  junction_id {",
                        '    id: "195"',
                        "  }",
                        "}",
                        "road {",
                        "  id {",
                        '    id: "8"',
                        "  }",
                        "  section {",
                        "    id {",
                        '      id: "1"',
                        "    }",
                        "    lane_id {",
                        '      id: "8_1_1"',
                        "    }",
                        "  }",
                        "}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            metadata = module._extract_road_section_lane_metadata(map_path)
        self.assertEqual(metadata["11_1_-1"]["road_id"], "11")
        self.assertEqual(metadata["11_1_-1"]["section_id"], "1")
        self.assertEqual(metadata["11_1_-1"]["junction_id"], "195")
        self.assertTrue(metadata["11_1_-1"]["is_junction"])
        self.assertEqual(metadata["8_1_1"]["road_id"], "8")
        self.assertEqual(metadata["8_1_1"]["section_id"], "1")
        self.assertEqual(metadata["8_1_1"]["junction_id"], "")
        self.assertFalse(metadata["8_1_1"]["is_junction"])

    def test_planning_lane_metadata_fields_preserve_fallback_tokens_and_map_hits(self) -> None:
        module = _load_bridge_unit_test_module()
        metadata = {
            "11_1_-1": {
                "road_id": "11",
                "section_id": "1",
                "junction_id": "195",
                "is_junction": True,
            }
        }
        fields = module._planning_lane_metadata_fields(
            current_lane_id="11_1_-1",
            lane_id_first="11_1_-1",
            target_lane_id_first="8_1_1",
            lane_metadata=metadata,
        )
        self.assertEqual(fields["current_lane_road_id"], "11")
        self.assertEqual(fields["current_lane_section_id"], "1")
        self.assertEqual(fields["current_lane_junction_id"], "195")
        self.assertTrue(fields["current_lane_is_junction"])
        self.assertEqual(fields["current_lane_metadata_source"], "map_lane_metadata")
        self.assertEqual(fields["target_lane_road_id"], "8")
        self.assertEqual(fields["target_lane_section_id"], "1")
        self.assertIsNone(fields["target_lane_junction_id"])
        self.assertIsNone(fields["target_lane_is_junction"])
        self.assertEqual(fields["target_lane_metadata_source"], "lane_id_fallback")


class Town01WorldConnectTests(unittest.TestCase):
    def test_connect_world_retries_get_world_before_success(self) -> None:
        class FakeMap:
            name = "Carla/Maps/Town01"

        class FakeWorld:
            def get_map(self):
                return FakeMap()

        class FakeClient:
            def __init__(self):
                self.calls = 0

            def get_world(self):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("world still loading")
                return FakeWorld()

        class FakeManager:
            def __init__(self, *args, **kwargs):
                self.timeout = 30.0

            def create_client(self):
                return fake_client

        fake_client = FakeClient()
        fake_pkg = types.ModuleType("carla_testbed.sim")
        fake_mod = types.ModuleType("carla_testbed.sim.carla_client")
        fake_mod.CarlaClientManager = FakeManager
        sleep_mock = mock.Mock()
        with (
            mock.patch.dict(sys.modules, {"carla_testbed.sim": fake_pkg, "carla_testbed.sim.carla_client": fake_mod}),
            mock.patch("tools.run_town01_route_health.time.sleep", sleep_mock),
        ):
            client, world = _connect_world("127.0.0.1", 2000, timeout_s=5.0, poll_s=0.0)
        self.assertIs(client, fake_client)
        self.assertEqual(fake_client.calls, 2)
        sleep_mock.assert_any_call(GET_WORLD_RETRY_DELAY_S)
        self.assertEqual(world.get_map().name, "Carla/Maps/Town01")

    def test_connect_world_loads_town01_when_current_world_differs(self) -> None:
        class FakeMap:
            def __init__(self, name: str):
                self.name = name

        class FakeWorld:
            def __init__(self, name: str):
                self._map = FakeMap(name)

            def get_map(self):
                return self._map

        class FakeClient:
            def __init__(self):
                self.timeout_values: list[float] = []

            def get_world(self):
                return FakeWorld("Carla/Maps/Town02")

            def load_world(self, town: str):
                self.loaded_town = town
                return FakeWorld(f"Carla/Maps/{town}")

            def set_timeout(self, value: float):
                self.timeout_values.append(value)

        class FakeManager:
            def __init__(self, *args, **kwargs):
                self.timeout = 30.0

            def create_client(self):
                return fake_client

        fake_client = FakeClient()
        fake_pkg = types.ModuleType("carla_testbed.sim")
        fake_mod = types.ModuleType("carla_testbed.sim.carla_client")
        fake_mod.CarlaClientManager = FakeManager
        with (
            mock.patch.dict(sys.modules, {"carla_testbed.sim": fake_pkg, "carla_testbed.sim.carla_client": fake_mod}),
            mock.patch("tools.run_town01_route_health.time.sleep"),
        ):
            _client, world = _connect_world("127.0.0.1", 2000, timeout_s=5.0, poll_s=0.0)
        self.assertEqual(fake_client.loaded_town, "Town01")
        self.assertEqual(len(fake_client.timeout_values), 3)
        self.assertLessEqual(fake_client.timeout_values[0], 5.0)
        self.assertGreater(fake_client.timeout_values[0], 0.0)
        self.assertLessEqual(fake_client.timeout_values[1], 5.0)
        self.assertGreater(fake_client.timeout_values[1], 0.0)
        self.assertEqual(fake_client.timeout_values[2], 30.0)
        self.assertEqual(world.get_map().name, "Carla/Maps/Town01")

    def test_connect_world_loads_town01_when_get_world_never_stabilizes(self) -> None:
        class FakeMap:
            def __init__(self, name: str):
                self.name = name

        class FakeWorld:
            def __init__(self, name: str):
                self._map = FakeMap(name)

            def get_map(self):
                return self._map

        class FakeClient:
            def __init__(self):
                self.timeout_values: list[float] = []
                self.get_world_calls = 0
                self.loaded_town = None

            def get_world(self):
                self.get_world_calls += 1
                raise RuntimeError("world handshake still timing out")

            def load_world(self, town: str):
                self.loaded_town = town
                return FakeWorld(f"Carla/Maps/{town}")

            def set_timeout(self, value: float):
                self.timeout_values.append(value)

        class FakeManager:
            def __init__(self, *args, **kwargs):
                self.timeout = 30.0

            def create_client(self):
                return fake_client

        fake_client = FakeClient()
        fake_pkg = types.ModuleType("carla_testbed.sim")
        fake_mod = types.ModuleType("carla_testbed.sim.carla_client")
        fake_mod.CarlaClientManager = FakeManager
        with (
            mock.patch.dict(sys.modules, {"carla_testbed.sim": fake_pkg, "carla_testbed.sim.carla_client": fake_mod}),
            mock.patch("tools.run_town01_route_health.time.sleep"),
        ):
            _client, world = _connect_world("127.0.0.1", 2000, timeout_s=5.0, poll_s=0.0)
        self.assertEqual(fake_client.get_world_calls, 3)
        self.assertEqual(fake_client.loaded_town, "Town01")
        self.assertEqual(len(fake_client.timeout_values), 5)
        for value in fake_client.timeout_values[:4]:
            self.assertLessEqual(value, 5.0)
            self.assertGreater(value, 0.0)
        self.assertEqual(fake_client.timeout_values[4], 30.0)
        self.assertEqual(world.get_map().name, "Carla/Maps/Town01")

    def test_connect_world_fail_fast_surfaces_eof_alive_details(self) -> None:
        class FakeClient:
            def __init__(self):
                self.timeout_values: list[float] = []
                self.get_world_calls = 0

            def get_world(self):
                self.get_world_calls += 1
                raise RuntimeError("world handshake still timing out")

            def set_timeout(self, value: float):
                self.timeout_values.append(value)

        class FakeManager:
            def __init__(self, *args, **kwargs):
                self.timeout = 30.0

            def create_client(self):
                return fake_client

        class FakeClock:
            def __init__(self):
                self.value = 0.0

            def time(self):
                self.value += 5.0
                return self.value

        fake_client = FakeClient()
        fake_clock = FakeClock()
        fake_pkg = types.ModuleType("carla_testbed.sim")
        fake_mod = types.ModuleType("carla_testbed.sim.carla_client")
        fake_mod.CarlaClientManager = FakeManager
        startup_diag = {
            "process_alive": True,
            "target_port_snapshot": [
                {"port": 2000, "open": True},
                {"port": 2001, "open": True},
            ],
            "latest_server_log_tail": [
                "ERROR: Primary server: failed to read header: End of file ",
                "RequestExitWithStatus(1)",
            ],
        }
        with (
            mock.patch.dict(sys.modules, {"carla_testbed.sim": fake_pkg, "carla_testbed.sim.carla_client": fake_mod}),
            mock.patch("tools.run_town01_route_health.time.sleep"),
            mock.patch("tools.run_town01_route_health.time.time", side_effect=fake_clock.time),
        ):
            with self.assertRaises(CarlaWorldReadyError) as ctx:
                _connect_world(
                    "127.0.0.1",
                    2000,
                    timeout_s=30.0,
                    poll_s=0.0,
                    startup_diagnostics=lambda: startup_diag,
                    fail_fast_on_bad_world_after_s=8.0,
                )
        self.assertEqual(fake_client.get_world_calls, 1)
        details = ctx.exception.details
        self.assertEqual(details["connect_world_fail_fast_reason"], "rpc_ready_world_not_ready_eof_request_exit_alive")
        self.assertTrue(details["launcher_alive_after_failure"])
        self.assertTrue(details["launcher_log_contains_end_of_file"])
        self.assertTrue(details["launcher_log_contains_request_exit"])
        self.assertEqual(details["connect_world_get_world_attempt_count"], 1)


class Town01BatchRecoveryTests(unittest.TestCase):
    def test_resolve_run_dir_follows_redirect(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = root / "label__route"
            effective = root / "label__route__02"
            base.mkdir(parents=True, exist_ok=True)
            effective.mkdir(parents=True, exist_ok=True)
            (base / "RUN_DIR_REDIRECT.txt").write_text(str(effective) + "\n", encoding="utf-8")
            self.assertEqual(_resolve_run_dir(base), effective.resolve())

    def test_discover_batch_runs_recovers_from_empty_manifest_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_root = Path(tmpdir)
            (batch_root / "artifacts").mkdir(parents=True, exist_ok=True)
            base = batch_root / "mainline__town01_rh_spawn068_goal068"
            effective = batch_root / "mainline__town01_rh_spawn068_goal068__02"
            base.mkdir(parents=True, exist_ok=True)
            effective.mkdir(parents=True, exist_ok=True)
            (base / "RUN_DIR_REDIRECT.txt").write_text(str(effective) + "\n", encoding="utf-8")
            rows = _discover_batch_runs(batch_root)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["comparison_label"], "mainline")
            self.assertEqual(rows[0]["route_id"], "town01_rh_spawn068_goal068")
            self.assertEqual(Path(rows[0]["effective_run_dir"]), effective.resolve())

    def test_analyze_batch_reconciles_stale_running_manifest_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_root = Path(tmpdir)
            artifacts_dir = batch_root / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            base = batch_root / "junction_traverse__town01_rh_spawn031_goal056"
            effective = batch_root / "junction_traverse__town01_rh_spawn031_goal056__02"
            base.mkdir(parents=True, exist_ok=True)
            effective.mkdir(parents=True, exist_ok=True)
            (base / "RUN_DIR_REDIRECT.txt").write_text(str(effective) + "\n", encoding="utf-8")
            (effective / "summary.json").write_text("{}\n", encoding="utf-8")
            _write_json(
                artifacts_dir / "town01_route_health_run_manifest.json",
                {
                    "created_at": "2026-03-27T15:11:00",
                    "runs": [
                        {
                            "run_index": 1,
                            "comparison_label": "junction_traverse",
                            "route_id": "town01_rh_spawn031_goal056",
                            "run_dir": str(base),
                            "status": "running",
                            "returncode": None,
                        }
                    ],
                },
            )
            finalized = {
                "summary_status": "finalized",
                "finalized_from_event_stream": True,
                "summary_written_successfully": True,
            }
            row = {
                "run_dir": str(effective),
                "route_id": "town01_rh_spawn031_goal056",
                "comparison_label": "junction_traverse",
                "summary_status": "finalized",
                "finalized_from_event_stream": True,
                "summary_written_successfully": True,
            }
            with (
                mock.patch("tools.run_town01_route_health.finalize_town01_run", return_value=finalized),
                mock.patch("tools.run_town01_route_health.collect_run_row", return_value=row),
                mock.patch("tools.run_town01_route_health._publish_outputs"),
            ):
                rows = _analyze_batch(batch_root, publish_repo=False)

            self.assertEqual(len(rows), 1)
            manifest = json.loads((artifacts_dir / "town01_route_health_run_manifest.json").read_text(encoding="utf-8"))
            item = manifest["runs"][0]
            self.assertEqual(item["status"], "completed_from_analysis")
            self.assertIsNone(item["returncode"])
            self.assertEqual(item["effective_run_dir"], str(effective.resolve()))
            self.assertEqual(item["summary_path"], str((effective / "summary.json").resolve()))
            self.assertEqual(item["summary_status"], "finalized")
            self.assertTrue(item["finalized_from_event_stream"])
            self.assertTrue(item["summary_written_successfully"])
            self.assertIn("analyzed_at", item)


if __name__ == "__main__":
    unittest.main()
