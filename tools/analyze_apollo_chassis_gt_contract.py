#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.chassis_gt_contract import (  # noqa: E402
    analyze_chassis_gt_contract_files,
    write_chassis_gt_contract_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo GT chassis contract evidence from run artifacts."
    )
    parser.add_argument("--run-dir", help="Run directory to auto-discover timeseries/channel stats.")
    parser.add_argument("--timeseries", help="timeseries.csv or timeseries.jsonl path.")
    parser.add_argument("--channel-stats", help="channel_stats.json path.")
    parser.add_argument("--summary", help="summary.json path.")
    parser.add_argument("--manifest", help="manifest.json path.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_chassis_gt_contract_files(
        run_dir=args.run_dir,
        timeseries_path=args.timeseries,
        channel_stats_path=args.channel_stats,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    outputs = write_chassis_gt_contract_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "claim_grade": report.get("claim_grade"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "missing_fields": report.get("missing_fields") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
