from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.analyze_town01_direct_curve_recovery import (
    build_recovery_payload,
    classify_recovery,
    find_latest_candidate_root,
    find_latest_candidate_roots,
    write_recovery_json,
    write_recovery_markdown,
)


def _write_summary(root: Path, route_id: str, payload: dict) -> Path:
    run_dir = root / route_id / f"{route_id}__direct_random_curve_pair_recovery_420"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "route_id": route_id,
        "runtime_contract": {"status": "aligned"},
        "control_handoff_status": "control_consuming_with_nonzero_planning",
        "route_health_label": "route_established_but_behavior_unhealthy",
        "routing_success_count": 1,
        "transport_mode": "carla_direct",
        "route_distance_achieved_m": 80.0,
        "max_speed_mps": 6.0,
        "direct_control_apply_count": 300,
        "direct_control_apply_window_status": "observable_apply_window",
        "direct_control_apply_frame_span": 300,
        "direct_metric_consistency_status": "consistent",
    }
    summary.update(payload)
    path = run_dir / "summary.json"
    path.write_text(json.dumps(summary), encoding="utf-8")
    return path


class AnalyzeTown01DirectCurveRecoveryTests(unittest.TestCase):
    def test_find_latest_candidate_root_prefers_matching_routes_and_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs = Path(tmpdir)
            old_root = runs / "town01_capability_online_chain_20260522_010000"
            new_root = runs / "town01_capability_online_chain_20260522_020000"
            _write_summary(old_root, "town01_rh_spawn176_goal061", {"route_distance_achieved_m": 70.0})
            _write_summary(new_root, "town01_rh_spawn177_goal052", {"route_distance_achieved_m": 90.0})

            found = find_latest_candidate_root(
                runs,
                route_ids=["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
                label_hint="direct_random_curve_pair_recovery_420",
            )

            self.assertEqual(found, new_root.resolve())

    def test_build_recovery_payload_marks_positive_pair(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate = root / "candidate"
            _write_summary(
                baseline,
                "town01_rh_spawn176_goal061",
                {"transport_mode": "ros2_gt", "route_distance_achieved_m": 50.0},
            )
            _write_summary(
                baseline,
                "town01_rh_spawn177_goal052",
                {"transport_mode": "ros2_gt", "route_distance_achieved_m": 50.0},
            )
            _write_summary(candidate, "town01_rh_spawn176_goal061", {"route_distance_achieved_m": 70.0})
            _write_summary(candidate, "town01_rh_spawn177_goal052", {"route_distance_achieved_m": 75.0})

            payload = build_recovery_payload(
                baseline_root=baseline,
                candidate_root=candidate,
                route_ids=["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
            )

            self.assertEqual(payload["recovery_verdict"], "recovery_positive")
            self.assertEqual(payload["summary"]["candidate_positive_count"], 2)

    def test_build_recovery_payload_merges_split_candidate_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline = root / "baseline"
            candidate_a = root / "town01_capability_online_chain_20260522_180001"
            candidate_b = root / "town01_capability_online_chain_20260522_180002"
            _write_summary(
                baseline,
                "town01_rh_spawn176_goal061",
                {"transport_mode": "ros2_gt", "route_distance_achieved_m": 50.0},
            )
            _write_summary(
                baseline,
                "town01_rh_spawn177_goal052",
                {"transport_mode": "ros2_gt", "route_distance_achieved_m": 50.0},
            )
            _write_summary(candidate_a, "town01_rh_spawn176_goal061", {"route_distance_achieved_m": 70.0})
            _write_summary(candidate_b, "town01_rh_spawn177_goal052", {"route_distance_achieved_m": 75.0})

            payload = build_recovery_payload(
                baseline_root=baseline,
                candidate_root=[candidate_a, candidate_b],
                route_ids=["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
            )

            self.assertEqual(payload["recovery_verdict"], "recovery_positive")
            self.assertEqual(len(payload["candidate_roots"]), 2)

    def test_find_latest_candidate_roots_returns_split_roots_per_route(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs = Path(tmpdir)
            root_a = runs / "town01_capability_online_chain_20260522_180001"
            root_b = runs / "town01_capability_online_chain_20260522_180002"
            _write_summary(root_a, "town01_rh_spawn176_goal061", {"route_distance_achieved_m": 70.0})
            _write_summary(root_b, "town01_rh_spawn177_goal052", {"route_distance_achieved_m": 90.0})

            found = find_latest_candidate_roots(
                runs,
                route_ids=["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
                label_hint="direct_random_curve_pair_recovery_420",
            )

            self.assertEqual(found, [root_a.resolve(), root_b.resolve()])

    def test_classify_recovery_marks_missing_candidate_incomplete(self) -> None:
        rows = [{"verdict": "candidate_missing"}]
        summary = {
            "route_count": 1,
            "verdict_counts": {"candidate_missing": 1},
            "candidate_negative_count": 0,
            "candidate_positive_count": 0,
            "candidate_inconclusive_count": 0,
            "candidate_aligned_count": 0,
            "candidate_control_consuming_count": 0,
        }

        self.assertEqual(classify_recovery(rows, summary), "recovery_incomplete_candidate_missing")

    def test_writes_recovery_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "recovery_verdict": "recovery_inconclusive",
                "baseline_root": "baseline",
                "candidate_root": "candidate",
                "route_ids": ["a"],
                "summary": {
                    "route_count": 1,
                    "candidate_aligned_count": 1,
                    "candidate_control_consuming_count": 1,
                    "candidate_positive_count": 0,
                    "candidate_negative_count": 0,
                    "candidate_inconclusive_count": 1,
                    "verdict_counts": {"candidate_inconclusive": 1},
                },
                "rows": [
                    {
                        "route_id": "a",
                        "baseline_distance_m": 1.0,
                        "candidate_distance_m": 2.0,
                        "distance_delta_m": 1.0,
                        "candidate_invalid_reason": "",
                        "verdict": "candidate_inconclusive",
                    }
                ],
            }
            json_out = root / "report.json"
            md_out = root / "report.md"

            write_recovery_json(json_out, payload)
            write_recovery_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["recovery_verdict"], "recovery_inconclusive")
            self.assertIn("Town01 Direct Curve Recovery Assessment", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
