#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_lateral_semantics import (  # noqa: E402
    analyze_apollo_lateral_semantics,
    analyze_apollo_lateral_semantics_run_dir,
    write_apollo_lateral_semantics_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo lateral semantics across kappa, target/matched point, and steer fields."
    )
    parser.add_argument("--run-dir", help="Run directory with timeseries and optional route-health artifacts.")
    parser.add_argument("--timeseries", help="Input timeseries.csv/jsonl.")
    parser.add_argument("--route-health", help="Optional route_health.json.")
    parser.add_argument("--planning-debug", help="Optional planning debug csv/json/jsonl.")
    parser.add_argument("--source-steer-summary", help="Optional source steer summary JSON.")
    parser.add_argument("--kappa-audit-summary", help="Optional kappa audit summary JSON.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.run_dir:
        report = analyze_apollo_lateral_semantics_run_dir(args.run_dir)
    else:
        report = analyze_apollo_lateral_semantics(
            timeseries=args.timeseries,
            route_health=args.route_health,
            planning_debug=args.planning_debug,
            source_steer_summary=args.source_steer_summary,
            kappa_audit_summary=args.kappa_audit_summary,
        )
    outputs = write_apollo_lateral_semantics_report(report, args.out)
    print(
        json.dumps(
            {
                "status": (report.get("verdict") or {}).get("status"),
                "suspected_layer": report.get("suspected_layer"),
                "confidence": report.get("confidence"),
                "report": outputs["apollo_lateral_semantics_report"],
                "summary": outputs["apollo_lateral_semantics_summary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
