#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_route_contract import (  # noqa: E402
    analyze_apollo_route_contract_run_dir,
    write_apollo_route_contract_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze Apollo routing response compatibility with the scenario route."
    )
    parser.add_argument("--run-dir", required=True, help="Run artifact directory.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_apollo_route_contract_run_dir(args.run_dir)
    outputs = write_apollo_route_contract_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "routing_length_ratio": report.get("routing_length_ratio"),
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
