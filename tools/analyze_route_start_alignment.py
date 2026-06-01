#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.route_start_alignment import (  # noqa: E402
    analyze_route_start_alignment_run_dir,
    write_route_start_alignment_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Town01 route-start/spawn alignment from existing run artifacts."
    )
    parser.add_argument("--run-dir", required=True, type=Path, help="Run directory.")
    parser.add_argument(
        "--out",
        type=Path,
        help="Output directory. Defaults to <run-dir>/analysis/route_start_alignment.",
    )
    parser.add_argument(
        "--fail-on-status",
        default="",
        help="Comma-separated statuses that should return non-zero.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out_dir = args.out or args.run_dir / "analysis" / "route_start_alignment"
    report = analyze_route_start_alignment_run_dir(args.run_dir)
    outputs = write_route_start_alignment_report(report, out_dir)
    payload = {
        "status": report.get("status"),
        "reason": report.get("reason"),
        "run_id": report.get("run_id"),
        "route_id": report.get("route_id"),
        "hypotheses": report.get("hypotheses") or [],
        "missing_inputs": report.get("missing_inputs") or [],
        "outputs": outputs,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    return 2 if payload["status"] in fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main())
