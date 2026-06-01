#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.failure_timeline import (  # noqa: E402
    analyze_failure_timeline_run_dir,
    write_failure_timeline_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build an offline Town01 failure timeline from existing run artifacts."
    )
    parser.add_argument("--run-dir", required=True, type=Path, help="Run directory containing summary/timeseries artifacts.")
    parser.add_argument(
        "--out",
        type=Path,
        help="Output directory. Defaults to <run-dir>/analysis/failure_timeline.",
    )
    parser.add_argument(
        "--window-rows",
        type=int,
        default=60,
        help="Rows before/after the anchor event to summarize.",
    )
    parser.add_argument(
        "--fail-on-status",
        default="",
        help="Comma-separated statuses that should return non-zero, for example insufficient_data.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out_dir = args.out or args.run_dir / "analysis" / "failure_timeline"
    report = analyze_failure_timeline_run_dir(args.run_dir, window_rows=args.window_rows)
    outputs = write_failure_timeline_report(report, out_dir)
    payload = {
        "status": report.get("status"),
        "run_id": report.get("run_id"),
        "route_id": report.get("route_id"),
        "primary_failure": report.get("primary_failure"),
        "anchor_event": report.get("anchor_event"),
        "ordering_findings": report.get("ordering_findings") or [],
        "missing_inputs": report.get("missing_inputs") or [],
        "missing_fields": report.get("missing_fields") or [],
        "outputs": outputs,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    return 2 if payload["status"] in fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main())
