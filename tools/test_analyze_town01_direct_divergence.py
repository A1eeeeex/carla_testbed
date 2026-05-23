from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.analyze_town01_direct_divergence import build_report, write_json, write_markdown


def _write_run(root: Path, name: str, summary_updates: dict, direct_apply_rows: list[dict] | None = None) -> Path:
    run_dir = root / name
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    summary = {
        "route_id": "town01_rh_spawn183_goal044",
        "transport_mode": "carla_direct",
        "runtime_contract": {"status": "aligned"},
        "control_handoff_status": "control_consuming_with_nonzero_planning",
        "route_health_label": "route_established_but_behavior_unhealthy",
        "materialization_status": "planning_control_materialized",
        "route_distance_achieved_m": 100.0,
        "route_completion_ratio": 0.5,
        "max_speed_mps": 7.0,
        "planning_nonzero_ratio": 0.8,
        "control_used_planning_ratio": 0.99,
        "routing_request_count": 2,
        "routing_success_count": 2,
        "direct_control_apply_count": 300,
        "direct_control_apply_window_status": "observable_apply_window",
        "direct_control_apply_frame_span": 300,
        "direct_control_apply_max_speed_mps": 7.0,
        "direct_control_apply_max_throttle": 1.0,
    }
    summary.update(summary_updates)
    (run_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (artifacts / "direct_bridge_stats.json").write_text(
        json.dumps(
            {
                "control_apply_count": summary.get("direct_control_apply_count"),
                "control_apply_frame_span": summary.get("direct_control_apply_frame_span"),
                "control_apply_max_speed_mps": summary.get("direct_control_apply_max_speed_mps"),
                "control_apply_max_throttle": summary.get("direct_control_apply_max_throttle"),
                "snapshot_count": 400,
                "ego_discovery_fail_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "command_materialization_summary.json").write_text(
        json.dumps({"command_path_stage": "materialized"}),
        encoding="utf-8",
    )
    if direct_apply_rows is not None:
        (artifacts / "direct_bridge_control_apply.jsonl").write_text(
            "\n".join(json.dumps(row) for row in direct_apply_rows) + "\n",
            encoding="utf-8",
        )
    (run_dir / "timeseries.csv").write_text(
        "frame,v_mps,cmd_throttle,applied_throttle,applied_brake,applied_steer\n"
        "1,1.0,0.5,0.0,0.0,0.0\n"
        "2,8.0,0.7,0.0,0.0,0.0\n",
        encoding="utf-8",
    )
    return run_dir


class AnalyzeTown01DirectDivergenceTests(unittest.TestCase):
    def test_detects_short_apply_metric_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            left = _write_run(root, "left", {})
            right = _write_run(
                root,
                "right",
                {
                    "route_distance_achieved_m": 185.0,
                    "max_speed_mps": 27.0,
                    "direct_control_apply_count": 14,
                    "direct_control_apply_window_status": "short_apply_window",
                    "direct_control_apply_frame_span": 14,
                    "direct_control_apply_max_speed_mps": 0.5,
                },
                direct_apply_rows=[
                    {"frame_id": 10, "throttle": 0.0, "brake": 0.15, "steer": 0.0, "speed_mps": 0.0},
                    {"frame_id": 23, "throttle": 1.0, "brake": 0.0, "steer": 0.0, "speed_mps": 0.5},
                ],
            )

            report = build_report(left, "single_success", right, "batch_short_window")

            self.assertEqual(report["comparison"]["verdict"], "right_direct_short_apply_metric_mismatch")
            self.assertEqual(report["right"]["direct_apply_frame_span"], 14)
            self.assertEqual(report["right"]["direct_apply_max_speed_mps"], 0.5)

    def test_detects_long_apply_lower_distance(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = _write_run(
                root,
                "baseline",
                {
                    "transport_mode": "ros2_gt",
                    "route_distance_achieved_m": 73.0,
                    "max_speed_mps": 6.7,
                    "direct_control_apply_count": "",
                    "direct_control_apply_frame_span": "",
                    "direct_control_apply_max_speed_mps": "",
                },
            )
            direct = _write_run(
                root,
                "direct",
                {
                    "route_distance_achieved_m": 34.0,
                    "max_speed_mps": 3.7,
                    "direct_control_apply_count": 311,
                    "direct_control_apply_frame_span": 311,
                    "direct_control_apply_max_speed_mps": 3.7,
                },
            )

            report = build_report(baseline, "ros2_gt", direct, "carla_direct")

            self.assertEqual(report["comparison"]["verdict"], "right_direct_long_apply_lower_distance")
            self.assertLess(report["comparison"]["distance_delta_m"], -5.0)

    def test_runtime_interruption_overrides_distance_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = _write_run(
                root,
                "baseline",
                {
                    "transport_mode": "ros2_gt",
                    "route_distance_achieved_m": 73.0,
                    "max_speed_mps": 6.7,
                    "direct_control_apply_count": "",
                    "direct_control_apply_frame_span": "",
                    "direct_control_apply_max_speed_mps": "",
                },
            )
            direct = _write_run(
                root,
                "direct",
                {
                    "route_distance_achieved_m": 10.0,
                    "max_speed_mps": 3.7,
                    "direct_control_apply_count": 80,
                    "direct_control_apply_frame_span": 80,
                    "direct_control_apply_max_speed_mps": 3.7,
                },
            )
            log_dir = direct.parent
            (log_dir / "followstop_child.stderr.log").write_text(
                "RuntimeError: time-out of 30000ms while waiting for the simulator, make sure the simulator is ready and connected to localhost:2000\n"
                "world.tick()\n",
                encoding="utf-8",
            )

            report = build_report(baseline, "ros2_gt", direct, "carla_direct")

            self.assertEqual(report["comparison"]["verdict"], "right_runtime_interrupted")
            self.assertEqual(report["right"]["runtime_interruption_reason"], "simulator_world_tick_timeout")

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            left = _write_run(root, "left", {})
            right = _write_run(root, "right", {"route_distance_achieved_m": 120.0})
            report = build_report(left, "left", right, "right")
            json_out = root / "report.json"
            md_out = root / "report.md"

            write_json(json_out, report)
            write_markdown(md_out, report)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["comparison"]["verdict"], "right_positive_distance")
            self.assertIn("Town01 Direct Divergence Report", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
