from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from argparse import Namespace

from tools.run_town01_transport_ab import (
    _ab_batch_roots,
    _build_command,
    _load_runtime_scope,
    _resolve_startup_profiles,
    _selected_steps,
)


class Town01TransportAbTests(unittest.TestCase):
    def test_selected_steps_follow_tracked_route_sets(self) -> None:
        canonical = _selected_steps("canonical")
        self.assertEqual(
            canonical,
            [
                "lane_keep:town01_rh_spawn097_goal046",
                "lane_keep:town01_rh_spawn217_goal046",
                "junction_traverse:town01_rh_spawn031_goal056",
            ],
        )
        random_pool = _selected_steps("random")
        self.assertEqual(len(random_pool), 6)
        self.assertTrue(all(item.startswith(("lane_keep:", "junction_traverse:", "curve_lane_follow:")) for item in random_pool))
        self.assertEqual(len(_selected_steps("full")), 9)

    def test_build_command_keeps_transport_ab_flags_explicit(self) -> None:
        steps = ["lane_keep:town01_rh_spawn097_goal046", "junction_traverse:town01_rh_spawn031_goal056"]
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = _build_command(
                python_exec="python3",
                config_path=Path("/tmp/candidate.yaml"),
                steps=steps,
                startup_profile="render_offscreen_no_ros2",
                world_ready_timeout_sec=180.0,
                launch_attempts=1,
                ticks=420,
                post_fail_steps=120,
                progress_update_sec=3.0,
                batch_root_parent=Path(tmpdir),
                no_prewarm_carla=True,
                keep_carla_alive_at_end=True,
                continue_on_failure=True,
                continue_initial_step_on_failure=True,
                comparison_label_suffix="transport_ab_direct_candidate",
                extra_overrides=["algo.apollo.dreamview.enabled=false"],
                carla_ignore_memory_preflight=True,
                enable_guard=True,
                dry_run=True,
            )
        text = " ".join(cmd)
        self.assertIn("--enable-lateral", text)
        self.assertIn("--enable-guard", text)
        self.assertIn("--no-prewarm-carla", text)
        self.assertIn("--carla-ignore-memory-preflight", text)
        self.assertIn("--keep-carla-alive-at-end", text)
        self.assertIn("--continue-on-failure", text)
        self.assertIn("--continue-initial-step-on-failure", text)
        self.assertIn("--override algo.apollo.dreamview.enabled=false", text)
        self.assertIn("--comparison-label-suffix transport_ab_direct_candidate", text)
        self.assertIn("--dry-run", text)
        self.assertIn("--step lane_keep:town01_rh_spawn097_goal046", text)
        self.assertIn("--step junction_traverse:town01_rh_spawn031_goal056", text)

    def test_build_command_can_disable_guard_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = _build_command(
                python_exec="python3",
                config_path=Path("/tmp/candidate.yaml"),
                steps=["lane_keep:town01_rh_spawn097_goal046"],
                startup_profile="render_offscreen_no_ros2",
                world_ready_timeout_sec=180.0,
                launch_attempts=1,
                ticks=420,
                post_fail_steps=120,
                progress_update_sec=3.0,
                batch_root_parent=Path(tmpdir),
                no_prewarm_carla=False,
                keep_carla_alive_at_end=False,
                continue_on_failure=False,
                continue_initial_step_on_failure=False,
                comparison_label_suffix="transport_ab_direct_candidate",
                extra_overrides=[],
                carla_ignore_memory_preflight=False,
                enable_guard=False,
                dry_run=True,
            )

        self.assertNotIn("--enable-guard", cmd)

    def test_ab_batch_roots_keep_baseline_and_candidate_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            baseline, candidate = _ab_batch_roots(Path(tmpdir) / "ab")

        self.assertEqual(baseline.name, "baseline")
        self.assertEqual(candidate.name, "candidate")
        self.assertEqual(baseline.parent, candidate.parent)

    def test_transport_ab_defaults_use_fair_per_chain_startup_profiles(self) -> None:
        baseline, candidate = _resolve_startup_profiles(
            Namespace(
                startup_profile=None,
                baseline_startup_profile="render_offscreen",
                candidate_startup_profile="render_offscreen_no_ros2",
            )
        )

        self.assertEqual(baseline, "render_offscreen")
        self.assertEqual(candidate, "render_offscreen_no_ros2")

    def test_transport_ab_legacy_startup_profile_overrides_both_chains(self) -> None:
        baseline, candidate = _resolve_startup_profiles(
            Namespace(
                startup_profile="render_offscreen_no_ros2",
                baseline_startup_profile="render_offscreen",
                candidate_startup_profile="render_offscreen_no_ros2",
            )
        )

        self.assertEqual(baseline, "render_offscreen_no_ros2")
        self.assertEqual(candidate, "render_offscreen_no_ros2")

    def test_load_runtime_scope_reads_direct_candidate_topology(self) -> None:
        scope = _load_runtime_scope(
            Path(
                "/home/ubuntu/carla_testbed/configs/io/examples/"
                "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
            )
        )
        self.assertEqual(scope["transport_mode"], "carla_direct")
        self.assertFalse(scope["uses_ros2_gt"])
        self.assertFalse(scope["uses_ros2_control_bridge"])
        self.assertFalse(scope["requires_ros2_reexec"])
        self.assertEqual(scope["route_command_mode"], "cyber_direct")
        self.assertTrue(scope["require_no_ros2_runtime"])
        self.assertFalse(scope["dreamview_enabled"])
        self.assertTrue(scope["disable_native_ros2_arg"])


if __name__ == "__main__":
    unittest.main()
