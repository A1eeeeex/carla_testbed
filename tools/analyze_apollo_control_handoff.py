#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_control_handoff import (  # noqa: E402
    analyze_apollo_control_handoff,
    write_apollo_control_handoff_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo control process/channel/bridge/apply/vehicle-response handoff evidence."
    )
    parser.add_argument("--run-dir", help="Run artifact directory to auto-discover inputs.")
    parser.add_argument("--summary", help="summary.json path.")
    parser.add_argument("--manifest", help="manifest.json path.")
    parser.add_argument("--timeseries", help="timeseries.csv or timeseries.jsonl path.")
    parser.add_argument("--cyber-bridge-stats", help="cyber_bridge_stats.json path.")
    parser.add_argument("--planning-summary", help="planning_topic_debug_summary.json path.")
    parser.add_argument("--control-health", help="control_health_report.json path.")
    parser.add_argument("--control-attribution", help="control_attribution_report.json path.")
    parser.add_argument("--control-logs-dir", help="Directory containing control bridge/control process logs.")
    parser.add_argument("--out", required=True, help="Output directory for apollo_control_handoff_report.*")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_apollo_control_handoff(
        run_dir=args.run_dir,
        summary=args.summary,
        manifest=args.manifest,
        timeseries=args.timeseries,
        cyber_bridge_stats=args.cyber_bridge_stats,
        planning_summary=args.planning_summary,
        control_health=args.control_health,
        control_attribution=args.control_attribution,
        control_logs_dir=args.control_logs_dir,
    )
    outputs = write_apollo_control_handoff_report(report, args.out)
    print(
        json.dumps(
            {
                "verdict": report.get("verdict"),
                "failure_stage": report.get("failure_stage"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("verdict") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
