#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.baguang_stack_comparison import (  # noqa: E402
    DEFAULT_SUITE_PATH,
    analyze_baguang_stack_comparison,
    write_baguang_stack_comparison_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare assisted Apollo and Autoware Baguang follow-stop run artifacts."
    )
    parser.add_argument("--suite", default=str(DEFAULT_SUITE_PATH), help="Baguang stack comparison suite YAML.")
    parser.add_argument("--out", required=True, help="Output directory for comparison report files.")
    parser.add_argument(
        "--fail-on-status",
        default="fail",
        help="Comma-separated statuses that should return non-zero, e.g. fail,insufficient_data.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_baguang_stack_comparison(args.suite)
    outputs = write_baguang_stack_comparison_report(report, args.out)
    verdict = report.get("verdict") if isinstance(report.get("verdict"), dict) else {}
    payload = {
        "status": verdict.get("status"),
        "reason": verdict.get("reason"),
        "stack_verdicts": verdict.get("stack_verdicts"),
        "outputs": outputs,
        "claim_boundary": report.get("claim_boundary"),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    return 1 if verdict.get("status") in fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main())
