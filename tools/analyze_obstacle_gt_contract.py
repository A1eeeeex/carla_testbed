#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from carla_testbed.analysis.obstacle_gt_contract import (
    analyze_obstacle_gt_contract_file,
    write_obstacle_gt_contract_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze CARLA GT obstacle -> Apollo perception contract")
    parser.add_argument("--input", required=True, help="Path to obstacle_gt_contract.jsonl or JSON")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--scenario-class", default=None)
    parser.add_argument("--dynamic-obstacle-required", action="store_true", default=None)
    args = parser.parse_args()

    report = analyze_obstacle_gt_contract_file(
        args.input,
        scenario_class=args.scenario_class,
        dynamic_obstacle_required=args.dynamic_obstacle_required,
    )
    write_obstacle_gt_contract_report(report, Path(args.out))
    return 0 if report["status"] in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
