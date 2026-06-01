#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from carla_testbed.analysis.autoware_control_diagnostics import (
    analyze_autoware_control_run,
    write_autoware_control_diagnostics,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Autoware planning/control diagnostics from a run dir.")
    parser.add_argument("--run-dir", required=True, help="Run directory containing summary.json and artifacts/")
    parser.add_argument(
        "--out",
        default=None,
        help="Output directory. Defaults to <run-dir>/analysis/autoware_control",
    )
    parser.add_argument("--planning-speed-threshold-mps", type=float, default=3.0)
    parser.add_argument("--control-speed-threshold-mps", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    out = Path(args.out) if args.out else run_dir / "analysis" / "autoware_control"
    report = analyze_autoware_control_run(
        run_dir,
        planning_speed_threshold_mps=args.planning_speed_threshold_mps,
        control_speed_threshold_mps=args.control_speed_threshold_mps,
    )
    write_autoware_control_diagnostics(report, out)
    status = (report.get("verdict") or {}).get("status")
    reasons = ",".join((report.get("verdict") or {}).get("failure_reasons") or [])
    print(f"autoware_control_diagnostics status={status} reasons={reasons or 'none'} out={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
