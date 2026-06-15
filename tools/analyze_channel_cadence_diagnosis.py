#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.channel_cadence_diagnosis import (  # noqa: E402
    analyze_channel_cadence_diagnosis_files,
    analyze_channel_cadence_diagnosis_run_dir,
    write_channel_cadence_diagnosis_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Diagnose Apollo channel cadence failures from run artifacts."
    )
    parser.add_argument("--run-dir", help="Run directory. If provided, known artifact paths are auto-resolved.")
    parser.add_argument("--channel-stats", help="Input channel_stats.json.")
    parser.add_argument("--channel-health", help="Input apollo_channel_health_report.json.")
    parser.add_argument("--carla-tick-health-summary", help="Input carla_tick_health_summary.json.")
    parser.add_argument("--carla-tick-health-log", help="Input carla_tick_health.jsonl.")
    parser.add_argument("--topic-publish-stats", help="Input topic_publish_stats.jsonl.")
    parser.add_argument("--publish-gap-trace", help="Input publish_gap_trace.jsonl.")
    parser.add_argument("--summary", help="Input summary.json.")
    parser.add_argument("--manifest", help="Input manifest.json.")
    parser.add_argument(
        "--config",
        default="configs/algorithms/apollo_natural_driving_channels.yaml",
        help="Channel health config YAML.",
    )
    parser.add_argument(
        "--out",
        help="Output directory. Defaults to <run-dir>/analysis/channel_cadence_diagnosis.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.run_dir and not args.channel_stats:
        raise SystemExit("--run-dir or --channel-stats is required")

    if args.run_dir:
        run_dir = Path(args.run_dir).expanduser()
        report = analyze_channel_cadence_diagnosis_run_dir(run_dir, config_path=args.config)
        out_dir = Path(args.out).expanduser() if args.out else run_dir / "analysis" / "channel_cadence_diagnosis"
    else:
        report = analyze_channel_cadence_diagnosis_files(
            channel_stats_path=args.channel_stats,
            channel_health_path=args.channel_health,
            carla_tick_health_summary_path=args.carla_tick_health_summary,
            carla_tick_health_log_path=args.carla_tick_health_log,
            topic_publish_stats_path=args.topic_publish_stats,
            publish_gap_trace_path=args.publish_gap_trace,
            summary_path=args.summary,
            manifest_path=args.manifest,
            config_path=args.config,
        )
        out_dir = Path(args.out).expanduser() if args.out else Path("analysis/channel_cadence_diagnosis")

    outputs = write_channel_cadence_diagnosis_report(report, out_dir)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "primary_cadence_issue": report.get("primary_cadence_issue"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "warnings": report.get("warnings") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
