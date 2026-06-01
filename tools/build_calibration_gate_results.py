#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.calibration.gates import (
    build_gate_results_from_ab_report_file,
    write_gate_results,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build calibration no-regression gate results from a Town01 A/B report.")
    parser.add_argument("--ab-report", type=Path, required=True, help="Input ab_report.json.")
    parser.add_argument("--out", type=Path, required=True, help="Output gate_results.json.")
    parser.add_argument(
        "--require-pass",
        action="store_true",
        help="Return non-zero unless 097/217/031 all have status=pass.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    gate_results = build_gate_results_from_ab_report_file(args.ab_report)
    output = write_gate_results(args.out, gate_results)
    payload = {
        "output": output,
        "promotion_allowed": gate_results.get("promotion_allowed"),
        "gates": {
            route_id: gate.get("status")
            for route_id, gate in (gate_results.get("gates") or {}).items()
        },
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.require_pass and not gate_results.get("promotion_allowed"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
