#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.natural_driving import (  # noqa: E402
    analyze_natural_driving_suite,
    problem_run_details,
    write_natural_driving_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate Town01 truth-input natural-driving run artifacts."
    )
    parser.add_argument("--suite-root", required=True, help="Root containing per-scenario run artifact dirs.")
    parser.add_argument("--out", required=True, help="Output directory for natural_driving_report.* files.")
    parser.add_argument(
        "--fail-on-status",
        default="fail",
        help=(
            "Comma-separated natural-driving verdict statuses that should return non-zero. "
            "Use fail,warn,insufficient_data for strict evidence gates."
        ),
    )
    parser.add_argument(
        "--require-full-target-coverage",
        action="store_true",
        help=(
            "Require at least one passing run for every target Town01 truth-input capability "
            "class before returning success."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_natural_driving_suite(
        args.suite_root,
        require_full_target_coverage=True if args.require_full_target_coverage else None,
    )
    outputs = write_natural_driving_report(report, args.out)
    verdict = report.get("verdict") if isinstance(report.get("verdict"), dict) else {}
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    coverage = report.get("capability_coverage") if isinstance(report.get("capability_coverage"), dict) else {}
    status = verdict.get("status")
    problems = problem_run_details(report.get("run_results") or [])
    coverage_passed = bool(
        verdict.get("can_claim_full_natural_driving") is True
        and coverage.get("can_claim_full_natural_driving") is True
    )
    payload = {
        "status": status,
        "run_count": report["run_count"],
        "summary": summary,
        "verdict": verdict,
        "coverage_check": {
            "required": bool(args.require_full_target_coverage),
            "passed": coverage_passed,
            "suite_plan_missing": verdict.get("suite_plan_missing") or [],
            "missing_required_scenario_classes": coverage.get("missing_required_scenario_classes") or [],
            "unproven_required_scenario_classes": coverage.get("unproven_required_scenario_classes") or [],
            "missing_required_scenario_ids": coverage.get("missing_required_scenario_ids") or [],
            "unproven_required_scenario_ids": coverage.get("unproven_required_scenario_ids") or [],
            "scenario_identity_mismatches": coverage.get("scenario_identity_mismatches") or [],
        },
        "problem_run_count": len(problems),
        "problem_runs": problems,
        "outputs": outputs,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.require_full_target_coverage and not coverage_passed:
        return 2
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    return 1 if status in fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main())
