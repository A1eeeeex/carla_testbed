#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.town01_postprocess import postprocess_town01_goal


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline postprocess for Town01 A/B, route-health, calibration, and goal-audit evidence."
    )
    parser.add_argument("--hard-gate-batch", type=Path, help="A/B batch root for 097/217/031 hard gates.")
    parser.add_argument("--curve-batch", type=Path, help="A/B batch root for curve217/curve213 diagnostics.")
    parser.add_argument("--random-batch", type=Path, help="A/B batch root for random regression pool.")
    parser.add_argument("--calibration-profile", type=Path, default=Path("configs/calibration/control_actuation.yaml"))
    parser.add_argument("--natural-driving-report", type=Path, help="Optional natural_driving_report.json.")
    parser.add_argument("--demo-recording", type=Path, help="Optional town01_demo_recording_inspection.json.")
    parser.add_argument("--refresh-route-health", action="store_true", help="Rebuild route-health reports even if present.")
    parser.add_argument("--min-trial-duration-s", type=float, default=0.1)
    parser.add_argument("--cadence-ratio-min", type=float, default=0.8)
    parser.add_argument("--out", type=Path, required=True, help="Output postprocess directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not any([args.hard_gate_batch, args.curve_batch, args.random_batch]):
        print("at least one of --hard-gate-batch, --curve-batch, or --random-batch is required", file=sys.stderr)
        return 2
    result = postprocess_town01_goal(
        out_dir=args.out,
        hard_gate_batch=args.hard_gate_batch,
        curve_batch=args.curve_batch,
        random_batch=args.random_batch,
        calibration_profile=args.calibration_profile,
        natural_driving_report=args.natural_driving_report,
        demo_recording=args.demo_recording,
        refresh_route_health=args.refresh_route_health,
        min_trial_duration_s=args.min_trial_duration_s,
        cadence_ratio_min=args.cadence_ratio_min,
    )
    print(
        json.dumps(
            {
                "status": result.get("status"),
                "outputs": result.get("outputs"),
                "audit_status": (result.get("audit") or {}).get("status"),
                "missing_evidence": (result.get("audit") or {}).get("missing_evidence") or [],
                "missing_evidence_count": len((result.get("audit") or {}).get("missing_evidence") or []),
                "next_actions": (result.get("audit") or {}).get("next_actions") or [],
                "next_action_commands": (result.get("audit") or {}).get("next_action_commands") or {},
                "calibration_status": ((result.get("calibration") or {}).get("report") or {}).get("status"),
                "calibration_promotion_allowed": ((result.get("calibration") or {}).get("report") or {}).get(
                    "promotion_allowed"
                ),
                "natural_driving_status": (result.get("natural_driving") or {}).get("status"),
                "natural_driving_problem_count": len(
                    (result.get("natural_driving") or {}).get("problem_run_details") or []
                ),
                "natural_driving_problem_details": (
                    (result.get("natural_driving") or {}).get("problem_run_details") or []
                )[:8],
                "curve_pair_status": (result.get("curve_pair") or {}).get("status"),
                "ab_report_count": len(result.get("ab_reports") or []),
                "route_health_existing_count": len((result.get("route_health") or {}).get("existing") or []),
                "route_health_generated_count": len((result.get("route_health") or {}).get("generated") or []),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
