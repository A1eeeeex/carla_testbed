#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.route_curve_artifact_gap import (  # noqa: E402
    analyze_route_curve_artifact_gap,
    write_route_curve_artifact_gap_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check whether a Town01 run has per-frame P1 route-curve semantics artifacts."
    )
    parser.add_argument("--timeseries", required=True, help="Input timeseries.csv.")
    parser.add_argument("--summary", help="Optional summary.json with summary-level Apollo semantics.")
    parser.add_argument("--out", required=True, help="Output directory for route_curve_artifact_gap_report.json.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_route_curve_artifact_gap(args.timeseries, summary_json=args.summary)
    outputs = write_route_curve_artifact_gap_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report["status"],
                "failure_reason": report.get("failure_reason"),
                "report": outputs["route_curve_artifact_gap_report"],
                "summary": outputs["summary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
