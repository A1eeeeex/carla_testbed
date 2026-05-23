from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.analyze_town01_transport_ab import build_rows, summarize_rows, write_csv, write_markdown


def _write_summary(root: Path, route_id: str, payload: dict) -> Path:
    run_dir = root / route_id / "run__02"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "route_id": route_id,
        "runtime_contract": {"status": "aligned"},
        "control_handoff_status": "control_consuming_with_nonzero_planning",
        "route_health_label": "route_established_but_behavior_unhealthy",
        "routing_success_count": 1,
        "transport_mode": "ros2_gt",
        "route_distance_achieved_m": 10.0,
        "max_speed_mps": 3.0,
    }
    summary.update(payload)
    path = run_dir / "summary.json"
    path.write_text(json.dumps(summary), encoding="utf-8")
    return path


class AnalyzeTown01TransportAbTests(unittest.TestCase):
    def test_build_rows_classifies_positive_and_short_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(baseline, "town01_rh_spawn097_goal046", {"route_distance_achieved_m": 50.0})
            _write_summary(
                candidate,
                "town01_rh_spawn097_goal046",
                {
                    "transport_mode": "carla_direct",
                    "route_distance_achieved_m": 5.0,
                    "direct_control_apply_count": 5,
                    "direct_control_apply_window_status": "short_apply_window",
                    "direct_control_apply_frame_span": 5,
                    "direct_control_apply_max_speed_mps": 3.0,
                },
            )
            _write_summary(baseline, "town01_rh_spawn217_goal046", {"route_distance_achieved_m": 70.0})
            _write_summary(
                candidate,
                "town01_rh_spawn217_goal046",
                {
                    "transport_mode": "carla_direct",
                    "route_distance_achieved_m": 100.0,
                    "direct_control_apply_count": 300,
                    "direct_control_apply_window_status": "observable_apply_window",
                },
            )

            rows = build_rows([baseline], [candidate])
            verdicts = {row["route_id"]: row["verdict"] for row in rows}

            self.assertEqual(
                verdicts["town01_rh_spawn097_goal046"],
                "candidate_inconclusive_short_apply_window",
            )
            self.assertEqual(verdicts["town01_rh_spawn217_goal046"], "candidate_positive_distance")
            summary = summarize_rows(rows)
            self.assertEqual(summary["candidate_positive_count"], 1)
            self.assertEqual(summary["candidate_inconclusive_count"], 1)
            self.assertEqual(summary["decision_status"], "candidate_inconclusive")

    def test_writes_csv_and_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(baseline, "route_a", {"route_distance_achieved_m": 10.0})
            _write_summary(
                candidate,
                "route_a",
                {"transport_mode": "carla_direct", "route_distance_achieved_m": 20.0},
            )

            rows = build_rows([baseline], [candidate])
            csv_out = root / "report.csv"
            md_out = root / "report.md"
            write_csv(csv_out, rows)
            write_markdown(md_out, rows, summarize_rows(rows))

            self.assertIn("route_a", csv_out.read_text(encoding="utf-8"))
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Transport A/B Summary", text)
            self.assertIn("## Decision", text)

    def test_route_id_filter_reports_only_requested_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(baseline, "route_a", {"route_distance_achieved_m": 10.0})
            _write_summary(candidate, "route_a", {"transport_mode": "carla_direct"})
            _write_summary(baseline, "route_b", {"route_distance_achieved_m": 10.0})
            _write_summary(candidate, "route_b", {"transport_mode": "carla_direct"})

            rows = build_rows([baseline], [candidate], route_ids=["route_b", "route_missing"])

            self.assertEqual([row["route_id"] for row in rows], ["route_b", "route_missing"])
            self.assertEqual(rows[1]["verdict"], "baseline_missing")

    def test_summary_marks_partial_pending_rerun_with_pending_routes(self) -> None:
        rows = [
            {"route_id": "route_ok", "verdict": "candidate_positive_distance"},
            {"route_id": "route_missing", "verdict": "candidate_missing"},
            {
                "route_id": "route_timeout",
                "verdict": "candidate_inconclusive_runtime_interrupted",
            },
        ]

        summary = summarize_rows(rows)

        self.assertEqual(summary["decision_status"], "partial_pending_rerun")
        self.assertEqual(summary["pending_rerun_routes"], ["route_timeout", "route_missing"])
        self.assertIn("route_ok", summary["candidate_positive_routes"])

    def test_summary_marks_negative_before_partial(self) -> None:
        rows = [
            {"route_id": "route_bad", "verdict": "candidate_negative_control"},
            {"route_id": "route_missing", "verdict": "candidate_missing"},
        ]

        summary = summarize_rows(rows)

        self.assertEqual(summary["decision_status"], "candidate_negative")
        self.assertEqual(summary["candidate_negative_routes"], ["route_bad"])

    def test_gt_ingress_timeout_is_inconclusive_not_transport_negative(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(
                baseline,
                "town01_rh_spawn183_goal044",
                {"route_distance_achieved_m": 52.0, "routing_success_count": 1},
            )
            _write_summary(
                candidate,
                "town01_rh_spawn183_goal044",
                {
                    "transport_mode": "carla_direct",
                    "control_handoff_status": "control_process_missing",
                    "route_health_label": "route_not_established",
                    "routing_success_count": 0,
                    "direct_control_apply_count": 0,
                    "direct_control_apply_window_status": "no_direct_apply",
                    "materialization_status": "bridge_alive_no_routing",
                    "command_materialization_layer": "gt_ingress",
                    "command_materialization_stage": "waiting_for_first_odometry",
                    "command_materialization_reason": (
                        "connect_failed:time-out of 5000ms while waiting for the simulator"
                    ),
                },
            )

            rows = build_rows([baseline], [candidate])

            self.assertEqual(rows[0]["verdict"], "candidate_inconclusive_gt_ingress")
            self.assertEqual(
                rows[0]["candidate_command_materialization_layer"],
                "gt_ingress",
            )

    def test_short_apply_metric_mismatch_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(
                baseline,
                "town01_rh_spawn183_goal044",
                {"route_distance_achieved_m": 40.0, "max_speed_mps": 5.0},
            )
            _write_summary(
                candidate,
                "town01_rh_spawn183_goal044",
                {
                    "transport_mode": "carla_direct",
                    "route_distance_achieved_m": 185.0,
                    "max_speed_mps": 27.0,
                    "direct_control_apply_count": 14,
                    "direct_control_apply_window_status": "short_apply_window",
                    "direct_control_apply_frame_span": 14,
                    "direct_control_apply_max_speed_mps": 0.5,
                },
            )

            rows = build_rows([baseline], [candidate])

            self.assertEqual(
                rows[0]["verdict"],
                "candidate_inconclusive_short_apply_metric_mismatch",
            )
            self.assertEqual(rows[0]["candidate_direct_apply_frame_span"], 14)
            self.assertEqual(rows[0]["distance_quality"], "ok")

    def test_negative_route_distance_is_flagged_without_overriding_speed_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(
                baseline,
                "town01_rh_spawn178_goal077",
                {"route_distance_achieved_m": -1.8, "max_speed_mps": 5.0},
            )
            _write_summary(
                candidate,
                "town01_rh_spawn178_goal077",
                {
                    "transport_mode": "carla_direct",
                    "route_distance_achieved_m": -3.6,
                    "max_speed_mps": 6.3,
                    "direct_control_apply_count": 307,
                    "direct_control_apply_window_status": "observable_apply_window",
                    "direct_control_apply_frame_span": 307,
                    "direct_control_apply_max_speed_mps": 6.3,
                },
            )

            rows = build_rows([baseline], [candidate])

            self.assertEqual(rows[0]["distance_quality"], "baseline_negative+candidate_negative")
            self.assertEqual(rows[0]["verdict"], "candidate_positive_speed")

    def test_world_tick_timeout_is_inconclusive_not_distance_negative(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(
                baseline,
                "town01_rh_spawn176_goal061",
                {"route_distance_achieved_m": 60.0, "max_speed_mps": 5.2},
            )
            candidate_summary = _write_summary(
                candidate,
                "town01_rh_spawn176_goal061",
                {
                    "transport_mode": "carla_direct",
                    "route_distance_achieved_m": 4.7,
                    "max_speed_mps": 5.1,
                    "direct_control_apply_count": 45,
                    "direct_control_apply_window_status": "observable_apply_window",
                    "direct_control_apply_frame_span": 46,
                    "materialization_status": "planning_control_materialized",
                },
            )
            log_dir = candidate_summary.parents[1] / "pre_redirect_run" / "artifacts"
            log_dir.mkdir(parents=True)
            (log_dir / "followstop_child.stderr.log").write_text(
                "RuntimeError: time-out of 30000ms while waiting for the simulator\n"
                "  File \"carla_testbed/sim/tick.py\", line 40, in tick_world\n"
                "    snapshot = world.tick()\n",
                encoding="utf-8",
            )

            rows = build_rows([baseline], [candidate])

            self.assertEqual(rows[0]["verdict"], "candidate_inconclusive_runtime_interrupted")
            self.assertEqual(rows[0]["candidate_invalid_reason"], "simulator_world_tick_timeout")


if __name__ == "__main__":
    unittest.main()
