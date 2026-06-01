#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_channel_health import (  # noqa: E402
    analyze_apollo_channel_health_files,
    write_apollo_channel_health_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo natural-driving channel health from channel_stats.json."
    )
    parser.add_argument(
        "--config",
        default="configs/algorithms/apollo_natural_driving_channels.yaml",
        help="Channel health config YAML.",
    )
    parser.add_argument("--stats", required=True, help="Input channel_stats.json.")
    parser.add_argument(
        "--scenario-class",
        help="Scenario class, e.g. lane_keep or traffic_light_red_stop.",
    )
    parser.add_argument("--out", required=True, help="Output directory for apollo_channel_health_report.json.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_apollo_channel_health_files(
        args.config,
        args.stats,
        scenario_class=args.scenario_class,
    )
    outputs = write_apollo_channel_health_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report["status"],
                "scenario_class": report.get("scenario_class"),
                "report": outputs["apollo_channel_health_report"],
                "missing_required_channels": report.get("missing_required_channels", []),
                "missing_optional_channels": report.get("missing_optional_channels", []),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
