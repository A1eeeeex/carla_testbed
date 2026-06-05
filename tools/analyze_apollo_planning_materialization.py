#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.planning_materialization import (  # noqa: E402
    analyze_planning_materialization_files,
    analyze_planning_materialization_run_dir,
    write_planning_materialization_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo planning trajectory materialization evidence."
    )
    parser.add_argument("--run-dir", help="Run artifact directory.")
    parser.add_argument("--planning-topic-debug", help="artifacts/planning_topic_debug.jsonl.")
    parser.add_argument(
        "--planning-topic-debug-summary",
        help="artifacts/planning_topic_debug_summary.json.",
    )
    parser.add_argument(
        "--planning-route-segment-debug",
        help="artifacts/planning_route_segment_debug.jsonl.",
    )
    parser.add_argument(
        "--reference-line-contract",
        help="artifacts/apollo_reference_line_contract.jsonl.",
    )
    parser.add_argument("--topic-publish-stats", help="artifacts/topic_publish_stats.jsonl.")
    parser.add_argument("--hdmap-projection", help="artifacts/apollo_hdmap_projection.jsonl.")
    parser.add_argument("--summary", help="summary.json.")
    parser.add_argument("--bridge-stats", help="artifacts/cyber_bridge_stats.json.")
    parser.add_argument("--planning-log", action="append", default=[], help="Apollo planning log.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.run_dir:
        report = analyze_planning_materialization_run_dir(args.run_dir)
    else:
        report = analyze_planning_materialization_files(
            planning_topic_debug=args.planning_topic_debug,
            planning_topic_debug_summary=args.planning_topic_debug_summary,
            planning_route_segment_debug=args.planning_route_segment_debug,
            reference_line_contract=args.reference_line_contract,
            topic_publish_stats=args.topic_publish_stats,
            hdmap_projection=args.hdmap_projection,
            summary=args.summary,
            bridge_stats=args.bridge_stats,
            planning_logs=args.planning_log,
        )
    outputs = write_planning_materialization_report(report, args.out)
    print(
        json.dumps(
            {
                "verdict": report.get("verdict"),
                "nonempty_trajectory_ratio": report.get("nonempty_trajectory_ratio"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.get("verdict") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
