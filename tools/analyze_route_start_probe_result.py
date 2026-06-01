#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.natural_driving_probe_plan import (  # noqa: E402
    evaluate_route_start_probe_result_from_files,
    write_route_start_probe_result,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare source and probe natural-driving reports for route-start alignment evidence."
    )
    parser.add_argument("--source-report", required=True, help="Source natural_driving_report.json.")
    parser.add_argument("--probe-report", required=True, help="Probe natural_driving_report.json.")
    parser.add_argument("--out", required=True, help="Output directory for route_start_probe_result.* files.")
    parser.add_argument("--scenario-id", help="Optional scenario_id to compare.")
    parser.add_argument(
        "--fail-on-status",
        default="negative,insufficient_data",
        help="Comma-separated statuses that return non-zero.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = evaluate_route_start_probe_result_from_files(
        args.source_report,
        args.probe_report,
        scenario_id=args.scenario_id,
    )
    outputs = write_route_start_probe_result(result, args.out)
    payload = {
        "status": result.get("status"),
        "reason": result.get("reason"),
        "scenario_id": result.get("scenario_id"),
        "outputs": outputs,
        "claim_boundary": result.get("claim_boundary"),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    return 1 if result.get("status") in fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main())
