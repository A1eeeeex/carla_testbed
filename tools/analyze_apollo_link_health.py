#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_link_health import (  # noqa: E402
    analyze_apollo_link_health_run_dir,
    write_apollo_link_health_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aggregate Apollo online-run link health evidence.")
    parser.add_argument("--run-dir", required=True, help="Run artifact directory.")
    parser.add_argument(
        "--out",
        help="Output directory. Defaults to <run-dir>/analysis/apollo_link_health.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir).expanduser()
    out_dir = Path(args.out).expanduser() if args.out else run_dir / "analysis" / "apollo_link_health"
    report = analyze_apollo_link_health_run_dir(run_dir)
    outputs = write_apollo_link_health_report(report, out_dir)
    print(
        json.dumps(
            {
                "primary_blocker": report.get("primary_blocker"),
                "secondary_blockers": report.get("secondary_blockers") or [],
                "can_claim_unassisted_natural_driving": report.get(
                    "can_claim_unassisted_natural_driving"
                ),
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("primary_blocker") else 0


if __name__ == "__main__":
    raise SystemExit(main())
