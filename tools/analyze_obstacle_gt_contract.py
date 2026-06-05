#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.obstacle_gt_contract import (
    analyze_obstacle_gt_contract_file,
    analyze_obstacle_gt_contract_run_dir,
    write_obstacle_gt_contract_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze CARLA GT obstacle -> Apollo perception contract")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", help="Path to obstacle_gt_contract.jsonl or JSON")
    source.add_argument("--run-dir", help="Run directory containing artifacts/obstacle_gt_contract.jsonl")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--scenario-class", default=None)
    parser.add_argument("--dynamic-obstacle-required", action="store_true", default=None)
    args = parser.parse_args()

    if args.run_dir:
        report = analyze_obstacle_gt_contract_run_dir(
            args.run_dir,
            scenario_class=args.scenario_class,
            dynamic_obstacle_required=args.dynamic_obstacle_required,
        )
    else:
        report = analyze_obstacle_gt_contract_file(
            args.input,
            scenario_class=args.scenario_class,
            dynamic_obstacle_required=args.dynamic_obstacle_required,
        )
    write_obstacle_gt_contract_report(report, Path(args.out))
    return 0 if report["status"] in {"pass", "pass_empty", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
