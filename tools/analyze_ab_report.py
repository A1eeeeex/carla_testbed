#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.transport_ab import analyze_ab_manifest, check_ab_report_requirements, write_ab_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze a Town01 baseline/direct A/B batch from run artifacts.")
    parser.add_argument("--batch-root", type=Path, help="Batch root containing ab_manifest.json.")
    parser.add_argument("--manifest", type=Path, help="Explicit ab_manifest.json path.")
    parser.add_argument("--out", type=Path, required=True, help="Output directory for ab_report.* files.")
    parser.add_argument(
        "--require-hard-gate-pass",
        action="store_true",
        help="Return non-zero unless lane097/lane217/junction031 all pass the hard-gate summary.",
    )
    parser.add_argument(
        "--require-steering-normalization-mode",
        help="Return non-zero unless every run reports exactly this steering normalization mode.",
    )
    parser.add_argument(
        "--require-direct-control-apply-mode",
        help="Return non-zero unless every carla_direct run reports this direct control apply mode.",
    )
    parser.add_argument(
        "--require-direct-stale-world-frame-policy",
        help="Return non-zero unless every carla_direct run reports this stale-world-frame policy.",
    )
    parser.add_argument(
        "--require-direct-transport-contract-aligned",
        action="store_true",
        help="Return non-zero unless every carla_direct run has observed transport contract status aligned.",
    )
    parser.add_argument(
        "--require-direct-bridge-cadence-ratio-min",
        type=float,
        help="Return non-zero unless every carla_direct comparison has bridge loc/chassis cadence ratio at least this value.",
    )
    parser.add_argument(
        "--require-route-curve-p1-complete",
        action="store_true",
        help="Return non-zero unless curve diagnostic runs have complete per-frame P1 route-curve artifact evidence.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = args.manifest or ((args.batch_root / "ab_manifest.json") if args.batch_root else None)
    if manifest is None:
        print("--manifest or --batch-root is required", file=sys.stderr)
        return 2
    report = analyze_ab_manifest(manifest, batch_root=args.batch_root)
    outputs = write_ab_report(args.out, report)
    requirement_check = check_ab_report_requirements(
        report,
        require_hard_gate_pass=bool(args.require_hard_gate_pass),
        required_steering_mode=args.require_steering_normalization_mode,
        required_direct_control_apply_mode=args.require_direct_control_apply_mode,
        required_direct_stale_world_frame_policy=args.require_direct_stale_world_frame_policy,
        require_direct_transport_contract_aligned=bool(args.require_direct_transport_contract_aligned),
        required_direct_bridge_cadence_ratio_min=args.require_direct_bridge_cadence_ratio_min,
        require_route_curve_p1_complete=bool(args.require_route_curve_p1_complete),
    )
    print(
        json.dumps(
            {
                "outputs": outputs,
                "verdict": report.get("verdict"),
                "requirement_check": requirement_check,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if requirement_check["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
