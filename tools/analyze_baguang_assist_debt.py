#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.baguang_assist_debt import (  # noqa: E402
    analyze_baguang_assist_debt,
    write_baguang_assist_debt_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate Baguang Apollo/Autoware assist-reduction evidence into an assist-debt report."
    )
    parser.add_argument("--config", default="configs/experiments/baguang_assist_reduction.yaml")
    parser.add_argument(
        "--comparison-report",
        default=(
            "runs/analysis/baguang_stack_comparison_20260531_reduced_assists/"
            "baguang_stack_comparison_report.json"
        ),
    )
    parser.add_argument(
        "--reduction-root",
        action="append",
        default=[],
        help="Assist-reduction root directory or manifest path. Repeatable. Defaults to runs/assist_reduction.",
    )
    parser.add_argument("--out", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_baguang_assist_debt(
        config_path=args.config,
        comparison_report_path=args.comparison_report,
        reduction_roots=args.reduction_root or ["runs/assist_reduction"],
    )
    outputs = write_baguang_assist_debt_report(report, args.out)
    payload = {
        "status": report.get("verdict", {}).get("status"),
        "next_priority": report.get("verdict", {}).get("next_priority"),
        "outputs": outputs,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
