from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
import types
from unittest import mock

sys.modules.setdefault("carla", types.SimpleNamespace(Client=object))

from carla_testbed.sim.bringup import connect_world_with_retry
from carla_testbed.utils.recording_manifest import build_recording_manifest
from carla_testbed.utils.town01_route_health import (
    canonical_demo_steps,
    canonical_comparison_route_ids,
    canonical_comparison_set,
    capability_layer_summary,
    default_canonical_comparison_set_path,
    default_corpus_path,
    default_corpus_report_path,
    default_random_regression_pool_path,
    random_regression_route_ids,
    random_regression_steps,
)
from tools.apollo10_cyber_bridge.bridge_observer import build_bridge_observer_summary
from tools.apollo10_cyber_bridge.bridge_policy import build_bridge_policy_summary
from tools.apollo10_cyber_bridge.ingress_egress import (
    build_bridge_transport_summary,
    build_ingress_egress_summary,
)
from tools.analyze_town01_capability_progress import _best_current_runtime_row
from tools.run_town01_capability_review_packs import _build_parser as _build_review_pack_parser


class _FakeMap:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeWorld:
    def __init__(self, town: str) -> None:
        self._town = town

    def get_map(self) -> _FakeMap:
        return _FakeMap(f"/Game/Carla/Maps/{self._town}")


class _FakeClient:
    def __init__(self) -> None:
        self.timeout_values = []
        self._world = _FakeWorld("Town02")

    def get_world(self) -> _FakeWorld:
        return self._world

    def load_world(self, town: str) -> _FakeWorld:
        self._world = _FakeWorld(str(town))
        return self._world

    def set_timeout(self, value: float) -> None:
        self.timeout_values.append(float(value))


class FirstWaveStructureTests(unittest.TestCase):
    def test_default_town01_corpus_paths_use_tracked_assets(self) -> None:
        repo_root = Path("/tmp/carla_testbed")
        self.assertEqual(
            default_corpus_path(repo_root),
            repo_root / "carla_testbed" / "assets" / "routes" / "town01" / "town01_route_corpus.json",
        )
        self.assertEqual(
            default_corpus_report_path(repo_root),
            repo_root / "carla_testbed" / "assets" / "routes" / "town01" / "town01_route_corpus_report.md",
        )
        self.assertEqual(
            default_canonical_comparison_set_path(repo_root),
            repo_root
            / "carla_testbed"
            / "assets"
            / "routes"
            / "town01"
            / "town01_canonical_comparison_set.json",
        )
        self.assertEqual(
            default_random_regression_pool_path(repo_root),
            repo_root
            / "carla_testbed"
            / "assets"
            / "routes"
            / "town01"
            / "town01_random_regression_pool_20260416.json",
        )

    def test_connect_world_with_retry_returns_session_state_and_trace(self) -> None:
        fake_client = _FakeClient()
        callback_rows = []

        with mock.patch(
            "carla_testbed.sim.carla_client.CarlaClientManager.create_client",
            return_value=fake_client,
        ):
            result = connect_world_with_retry(
                host="127.0.0.1",
                port=2000,
                target_town="Town01",
                root=tempfile.gettempdir(),
                attempt_callback=lambda phase, payload: callback_rows.append((phase, dict(payload))),
            )

        self.assertEqual(result.current_town_before_load, "Town02")
        self.assertEqual(result.final_town, "Town01")
        self.assertEqual(result.session_state["target_town"], "Town01")
        self.assertEqual(result.session_state["final_town"], "Town01")
        self.assertTrue(result.startup_trace)
        self.assertTrue(any(row["phase"] == "get_world_attempt_ok" for row in result.startup_trace))
        self.assertTrue(any(row["phase"] == "load_world_attempt_ok" for row in result.startup_trace))
        self.assertTrue(any(phase == "client_connect_attempt_ok" for phase, _ in callback_rows))

    def test_canonical_comparison_set_is_fixed_five_route_baseline(self) -> None:
        self.assertEqual(
            canonical_comparison_route_ids(),
            (
                "town01_rh_spawn097_goal046",
                "town01_rh_spawn217_goal046",
                "town01_rh_spawn031_goal056",
                "town01_rh_spawn217_goal048",
                "town01_rh_spawn213_goal059",
            ),
        )
        self.assertEqual(
            [item["comparison_key"] for item in canonical_comparison_set()],
            ["lane097", "lane217", "junction031", "curve217", "curve213"],
        )
        self.assertEqual(
            [item["showcase_group"] for item in canonical_comparison_set()],
            ["core", "core", "core", "curve_diagnostic", "curve_diagnostic"],
        )
        self.assertEqual(
            canonical_demo_steps(),
            (
                "lane_keep:town01_rh_spawn097_goal046",
                "lane_keep:town01_rh_spawn217_goal046",
                "junction_traverse:town01_rh_spawn031_goal056",
            ),
        )
        self.assertEqual(
            canonical_demo_steps(include_curve_diagnostic=True),
            (
                "lane_keep:town01_rh_spawn097_goal046",
                "lane_keep:town01_rh_spawn217_goal046",
                "junction_traverse:town01_rh_spawn031_goal056",
                "curve_lane_follow:town01_rh_spawn217_goal048",
                "curve_lane_follow:town01_rh_spawn213_goal059",
            ),
        )
        self.assertEqual(
            random_regression_route_ids(),
            (
                "town01_rh_spawn183_goal044",
                "town01_rh_spawn213_goal048",
                "town01_rh_spawn144_goal082",
                "town01_rh_spawn178_goal077",
                "town01_rh_spawn176_goal061",
                "town01_rh_spawn177_goal052",
            ),
        )
        self.assertEqual(
            random_regression_steps(),
            (
                "lane_keep:town01_rh_spawn183_goal044",
                "lane_keep:town01_rh_spawn213_goal048",
                "junction_traverse:town01_rh_spawn144_goal082",
                "junction_traverse:town01_rh_spawn178_goal077",
                "curve_lane_follow:town01_rh_spawn176_goal061",
                "curve_lane_follow:town01_rh_spawn177_goal052",
            ),
        )

    def test_capability_layer_summary_collapses_to_three_layers(self) -> None:
        ready = capability_layer_summary(
            route_health_label="route_health_candidate",
            runtime_contract_status="aligned",
            control_handoff_status="control_consuming_with_nonzero_planning",
        )
        self.assertEqual(ready["engineering_gate"], "pass")
        self.assertEqual(ready["capability_evidence"], "positive")
        self.assertEqual(ready["promotion_candidate"], "ready")

        blocked = capability_layer_summary(
            route_health_label="route_established_but_behavior_unhealthy",
            runtime_contract_status="aligned",
            control_handoff_status="planning_ready_control_not_consuming",
        )
        self.assertEqual(blocked["engineering_gate"], "aligned_runtime_only")
        self.assertEqual(blocked["capability_evidence"], "behavior_unhealthy")
        self.assertEqual(blocked["promotion_candidate"], "not_ready")

    def test_bridge_policy_summary_marks_bridge_policy_source(self) -> None:
        summary = build_bridge_policy_summary(
            goal_validity_last={"fallback_reason": "invalid_goal_fallback_ahead", "fallback_applied": True},
            stats={
                "lateral_guard_reason_counts": {"sustained_raw_saturation": 2},
                "lateral_guard_trigger_count": 2,
                "lateral_guard_apply_count": 1,
                "trajectory_contract_lateral_guard_apply_count": 3,
            },
        )
        self.assertEqual(summary["source"], "bridge_policy")
        self.assertEqual(summary["goal_fallback"]["source"], "bridge_policy")
        self.assertTrue(summary["goal_fallback"]["fallback_applied"])
        self.assertEqual(summary["lateral_guard"]["apply_count"], 1)
        self.assertEqual(summary["trajectory_contract_guard"]["apply_count"], 3)

    def test_bridge_observer_and_ingress_egress_summaries_are_explicit(self) -> None:
        observer = build_bridge_observer_summary(
            planning_reader_enabled=True,
            planning_channel="/apollo/planning",
            planning_message_type="ADCTrajectory",
            timing={"timing_source_rule": "odom_header", "tick_owner": "harness"},
            health_summary_path="/tmp/bridge_health_summary.json",
            planning_topic_debug_summary_path="/tmp/planning_topic_debug_summary.json",
            planning_route_segment_debug_path="/tmp/planning_route_segment_debug.jsonl",
            apollo_map_runtime_debug_path="/tmp/apollo_map_runtime_debug.json",
            apollo_reference_line_debug_path="/tmp/apollo_reference_line_debug.jsonl",
            apollo_route_segment_debug_path="/tmp/apollo_route_segment_debug.jsonl",
            control_trajectory_consume_live_path="/tmp/control_trajectory_consume_live.jsonl",
            routing_event_debug_path="/tmp/routing_event_debug.jsonl",
            reroute_decision_debug_path="/tmp/reroute_decision_debug.jsonl",
            goal_validity_debug_path="/tmp/goal_validity_debug.jsonl",
            lateral_guard_debug_path="/tmp/lateral_guard_debug.jsonl",
        )
        ingress_egress = build_ingress_egress_summary(
            transport_mode="carla_direct",
            gt_source="carla_world_snapshot_direct",
            control_apply_path="bridge_direct_actor_apply",
            tick_owner="runner_harness_world_tick",
            ros_ego_id="hero",
            odom_topic="/carla/hero/odom",
            objects3d_topic="/carla/hero/objects3d",
            objects_markers_topic="/carla/hero/objects_markers",
            objects_json_topic="/carla/hero/objects_json",
            control_out_topic="/tb/ego/control_cmd",
            control_out_type="direct",
            localization_channel="/apollo/localization/pose",
            chassis_channel="/apollo/canbus/chassis",
            obstacles_channel="/apollo/perception/obstacles",
            control_channel="/apollo/control",
            planning_channel="/apollo/planning",
            routing_request_channel="/apollo/raw_routing_request",
            routing_response_channel="/apollo/routing_response",
            lane_follow_channel="/apollo/external_command/lane_follow",
            action_channel="/apollo/external_command/action",
            traffic_light_channel="/apollo/perception/traffic_light",
        )
        transport = build_bridge_transport_summary(
            transport_mode="carla_direct",
            gt_source="carla_world_snapshot_direct",
            control_apply_path="bridge_direct_actor_apply",
            tick_owner="runner_harness_world_tick",
            bridge_is_tick_owner=False,
            ros2_gt_enabled=False,
            uses_ros2_gt=False,
            uses_ros2_control_bridge=False,
            requires_ros2_reexec=False,
            route_command_mode="cyber_direct",
            route_command_path="cyber_direct_bridge_command_path",
            control_out_type="direct",
            direct_bridge={"ego_role_name": "hero", "poll_hz": 20.0},
        )
        self.assertEqual(observer["source"], "bridge_observer")
        self.assertEqual(observer["artifact_paths"]["health_summary_path"], "/tmp/bridge_health_summary.json")
        self.assertEqual(ingress_egress["source"], "ingress_egress")
        self.assertEqual(ingress_egress["transport_mode"], "carla_direct")
        self.assertEqual(ingress_egress["gt_source"], "carla_world_snapshot_direct")
        self.assertEqual(ingress_egress["ros2"]["control_out_type"], "direct")
        self.assertEqual(ingress_egress["cyber"]["planning_channel"], "/apollo/planning")
        self.assertEqual(transport["source"], "bridge_transport")
        self.assertEqual(transport["transport_mode"], "carla_direct")
        self.assertFalse(transport["uses_ros2_gt"])
        self.assertFalse(transport["uses_ros2_control_bridge"])
        self.assertEqual(transport["route_command_mode"], "cyber_direct")
        self.assertEqual(transport["direct_bridge"]["ego_role_name"], "hero")

    def test_review_pack_defaults_now_follow_batch_artifacts(self) -> None:
        parser = _build_review_pack_parser()
        args = parser.parse_args(["--batch-root-parent", "/tmp/town01_review_batch"])
        self.assertIsNone(args.output)
        self.assertIsNone(args.manifest_output)
        self.assertIsNone(args.review_report_output)
        self.assertIsNone(args.review_summary_output)
        self.assertIsNone(args.missing_history_runbook_output)

    def test_recording_manifest_tracks_expected_and_actual_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            actual_dir = root / "video" / "dual_cam" / "raw_tp"
            actual_dir.mkdir(parents=True)
            payload = build_recording_manifest(
                mode="full",
                expected_outputs=[actual_dir],
                actual_outputs=[actual_dir],
                required_for_acceptance=False,
                status="completed",
            )
        self.assertEqual(payload["mode"], "full")
        self.assertEqual(payload["status"], "completed")
        self.assertFalse(payload["required_for_acceptance"])
        self.assertTrue(payload["expected_outputs"][0]["exists"])
        self.assertTrue(payload["actual_outputs"][0]["exists"])

    def test_current_runtime_row_prefers_aligned_curve_sample_with_semantic_anchor(self) -> None:
        rows = [
            {
                "route_health_label": "route_established_but_behavior_unhealthy",
                "runtime_contract_status": "aligned",
                "control_handoff_status": "control_consuming_with_nonzero_planning",
                "semantic_window_anchor_kind": "",
                "persistent_path_fallback_at_end": True,
                "route_distance_achieved_m": 102.7,
                "route_completion_ratio": 0.42,
                "comparison_label": "older_long",
            },
            {
                "route_health_label": "route_established_but_behavior_unhealthy",
                "runtime_contract_status": "aligned",
                "control_handoff_status": "control_consuming_with_nonzero_planning",
                "semantic_window_anchor_kind": "first_high_steer",
                "persistent_path_fallback_at_end": False,
                "route_distance_achieved_m": 56.2,
                "route_completion_ratio": 0.20,
                "comparison_label": "current_short",
            },
        ]

        best = _best_current_runtime_row("curve_lane_follow", rows)
        self.assertEqual(best["comparison_label"], "current_short")


if __name__ == "__main__":
    unittest.main()
