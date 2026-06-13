#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.control_attribution import (  # noqa: E402
    analyze_control_attribution,
    analyze_control_attribution_run_dir,
    write_control_attribution_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Attribute raw/mapped/applied/vehicle-response control-chain breakpoints."
    )
    parser.add_argument("--run-dir", help="Run directory with summary/manifest/config and control traces.")
    parser.add_argument("--timeseries", help="Input timeseries.csv/jsonl or control trace.")
    parser.add_argument("--summary", help="Optional summary.json for run metadata.")
    parser.add_argument("--manifest", help="Optional manifest.json for run metadata.")
    parser.add_argument("--config", help="Optional resolved config yaml for control mapping metadata.")
    parser.add_argument("--control-handoff", help="Optional apollo_control_handoff_report.json.")
    parser.add_argument("--bridge-health", help="Optional bridge_health_summary.json.")
    parser.add_argument("--cyber-bridge-stats", help="Optional cyber_bridge_stats.json.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.run_dir:
        report = analyze_control_attribution_run_dir(args.run_dir)
    else:
        if not args.timeseries:
            raise SystemExit("--timeseries is required unless --run-dir is provided")
        report = analyze_control_attribution(
            args.timeseries,
            summary_json=args.summary,
            manifest_json=args.manifest,
            config_yaml=args.config,
            control_handoff_json=args.control_handoff,
            bridge_health_json=args.bridge_health,
            cyber_bridge_stats_json=args.cyber_bridge_stats,
        )
    outputs = write_control_attribution_report(report, args.out)
    print(
        json.dumps(
            {
                "status": (report.get("verdict") or {}).get("status"),
                "dominant_breakpoint": (report.get("attribution") or {}).get("dominant_breakpoint"),
                "report": outputs["control_attribution_report"],
                "summary": outputs["control_attribution_summary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
