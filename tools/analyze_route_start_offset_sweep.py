#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.route_start_offset_sweep import (  # noqa: E402
    analyze_route_start_offset_sweep,
    write_route_start_offset_sweep_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate Town01 route-start offset probes without starting CARLA/Apollo."
    )
    parser.add_argument("--source-report", required=True, help="Baseline/source natural_driving_report.json.")
    parser.add_argument(
        "--probe-report",
        action="append",
        required=True,
        help="Probe natural_driving_report.json. Repeat for each tested offset/run.",
    )
    parser.add_argument("--scenario-id", help="Scenario id to compare, e.g. lane_keep_097.")
    parser.add_argument("--out", required=True, help="Output directory for route_start_offset_sweep_report.*.")
    parser.add_argument(
        "--fail-on-status",
        default="fail,insufficient_data",
        help="Comma-separated report statuses that return non-zero.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_route_start_offset_sweep(
        args.source_report,
        args.probe_report,
        scenario_id=args.scenario_id,
    )
    outputs = write_route_start_offset_sweep_report(report, args.out)
    payload = {
        "status": report.get("status"),
        "reason": report.get("reason"),
        "scenario_id": report.get("scenario_id"),
        "summary": report.get("summary"),
        "outputs": outputs,
        "claim_boundary": report.get("claim_boundary"),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    return 1 if report.get("status") in fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main())
