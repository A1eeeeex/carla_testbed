#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.localization_contract import (  # noqa: E402
    analyze_localization_contract_files,
    write_localization_contract_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo GT localization contract evidence from run artifacts."
    )
    parser.add_argument("--run-dir", help="Run directory to auto-discover timeseries/channel stats/route health.")
    parser.add_argument("--timeseries", help="timeseries.csv or timeseries.jsonl.")
    parser.add_argument("--channel-stats", help="channel_stats.json.")
    parser.add_argument("--route-health", help="route_health.json.")
    parser.add_argument("--hdmap-projection", help="Optional artifacts/apollo_hdmap_projection.jsonl from Apollo HDMap API.")
    parser.add_argument("--frame-transform", help="Apollo frame transform YAML.")
    parser.add_argument("--vehicle-reference", help="Vehicle reference YAML.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_localization_contract_files(
        run_dir=args.run_dir,
        timeseries_path=args.timeseries,
        channel_stats_path=args.channel_stats,
        route_health_path=args.route_health,
        hdmap_projection_path=args.hdmap_projection,
        frame_transform_path=args.frame_transform,
        vehicle_reference_path=args.vehicle_reference,
    )
    outputs = write_localization_contract_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report["verdict"]["status"],
                "run_id": report.get("run_id"),
                "route_id": report.get("route_id"),
                "report": outputs["localization_contract_report"],
                "summary": outputs["localization_contract_summary"],
                "blocking_reasons": report["verdict"].get("blocking_reasons", []),
                "missing_fields": report.get("missing_fields", []),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report["verdict"]["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
