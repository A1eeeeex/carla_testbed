#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.calibration.report import analyze_calibration_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a minimal control-actuation calibration report.")
    parser.add_argument("--profile", type=Path, required=True, help="Calibration profile YAML.")
    parser.add_argument("--trials", type=Path, required=True, help="Calibration trials CSV.")
    parser.add_argument("--gate-results", type=Path, help="Optional 097/217/031 no-regression gate results JSON.")
    parser.add_argument("--out", type=Path, required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report, outputs = analyze_calibration_report(
        profile_path=args.profile,
        trials_path=args.trials,
        gate_results_path=args.gate_results,
        out_dir=args.out,
    )
    print(
        json.dumps(
            {
                "outputs": outputs,
                "profile_id": report.get("profile_id"),
                "promotion_allowed": (report.get("no_regression") or {}).get("promotion_allowed"),
                "recommendation": report.get("recommendation"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
