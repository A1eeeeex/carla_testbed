#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_shadow_mode import (  # noqa: E402
    analyze_apollo_shadow_mode_timeseries,
    write_apollo_shadow_mode_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo Town01 shadow-mode planning/control outputs from timeseries.csv."
    )
    parser.add_argument("--timeseries", required=True, help="Input shadow-mode timeseries.csv.")
    parser.add_argument(
        "--summary",
        help="Optional summary.json with summary-derived Apollo lateral semantic fields.",
    )
    parser.add_argument("--out", required=True, help="Output directory for apollo_shadow_mode_report.json.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_apollo_shadow_mode_timeseries(args.timeseries, summary_json=args.summary)
    outputs = write_apollo_shadow_mode_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report["status"],
                "failure_reason": report.get("failure_reason"),
                "report": outputs["apollo_shadow_mode_report"],
                "summary": outputs["summary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
