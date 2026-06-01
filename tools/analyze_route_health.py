#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.route_health import analyze_route_health
from carla_testbed.analysis.route_health_report import (
    analyze_route_health_run_dir,
    load_timeseries_rows,
    write_route_health_report,
)
from carla_testbed.routes.io import load_route_json


def _planned_outputs(out_dir: Path) -> dict[str, str]:
    return {
        "route_health_json": str(out_dir / "route_health.json"),
        "route_health_csv": str(out_dir / "route_health.csv"),
        "curve_segments_csv": str(out_dir / "curve_segments.csv"),
        "route_health_summary_md": str(out_dir / "route_health_summary.md"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze static route health and optional run timeseries.")
    parser.add_argument("--route", help="Route JSON path.")
    parser.add_argument("--run-dir", help="Run directory containing manifest/summary/timeseries/route artifacts.")
    parser.add_argument("--timeseries", help="Optional timeseries CSV/JSONL/JSON path.")
    parser.add_argument("--out", help="Output directory. Defaults to <run-dir>/analysis/route_health in --run-dir mode.")
    parser.add_argument(
        "--curvature-abs-threshold",
        type=float,
        default=0.03,
        help="Absolute curvature threshold used to merge curve segments.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned inputs/outputs without writing reports.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if bool(args.route) == bool(args.run_dir):
        print("exactly one of --route or --run-dir is required", file=sys.stderr)
        return 2
    if args.route and not args.out:
        print("--out is required in --route mode", file=sys.stderr)
        return 2
    route_path = Path(args.route).expanduser() if args.route else None
    run_dir = Path(args.run_dir).expanduser() if args.run_dir else None
    timeseries_path = Path(args.timeseries).expanduser() if args.timeseries else None
    out_dir = Path(args.out).expanduser() if args.out else None

    planned_out = out_dir
    if planned_out is None and run_dir is not None:
        planned_out = run_dir / "analysis" / "route_health"

    if args.dry_run:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "route": str(route_path) if route_path else None,
                    "run_dir": str(run_dir) if run_dir else None,
                    "timeseries": str(timeseries_path) if timeseries_path else None,
                    "out": str(planned_out),
                    "outputs": _planned_outputs(planned_out),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if run_dir is not None:
        result = analyze_route_health_run_dir(
            run_dir,
            out_dir=out_dir,
            curvature_abs_threshold=args.curvature_abs_threshold,
        )
        print(
            json.dumps(
                {
                    "dry_run": False,
                    "inputs": result["inputs"],
                    "outputs": result["outputs"],
                    "verdict": result["report"].get("verdict"),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    route = load_route_json(route_path)
    rows = load_timeseries_rows(timeseries_path) if timeseries_path else None
    report = analyze_route_health(route, rows, curvature_abs_threshold=args.curvature_abs_threshold)
    outputs = write_route_health_report(out_dir, route, report)
    print(json.dumps({"dry_run": False, "outputs": outputs, "verdict": report.get("verdict")}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
