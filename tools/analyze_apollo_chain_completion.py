#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_chain_completion import (  # noqa: E402
    analyze_apollo_chain_completion_run_dir,
    write_apollo_chain_completion_report,
)
from carla_testbed.analysis.apollo_link_health import analyze_apollo_link_health_run_dir  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate Apollo chain completion evidence using link-health layers.",
    )
    parser.add_argument("--run-dir", required=True, help="Run artifact directory.")
    parser.add_argument(
        "--reference",
        default="configs/reference/apollo_reference_chain.yaml",
        help="Apollo reference chain YAML.",
    )
    parser.add_argument(
        "--replacement",
        default="configs/reference/apollo_gt_replacement_matrix.yaml",
        help="Apollo GT replacement matrix YAML.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for apollo_chain_completion_report.json.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir).expanduser()
    report = analyze_apollo_chain_completion_run_dir(
        run_dir,
        reference_path=args.reference,
        replacement_path=args.replacement,
    )
    link_health = analyze_apollo_link_health_run_dir(run_dir)
    outputs = write_apollo_chain_completion_report(
        report,
        args.out,
        link_health_report=link_health,
        write_compatible_link_health=True,
    )
    print(
        json.dumps(
            {
                "verdict": report.get("verdict"),
                "failure_stage": report.get("failure_stage"),
                "capability_status": report.get("capability_status") or {},
                "can_claim_unassisted_natural_driving": report.get(
                    "can_claim_unassisted_natural_driving"
                ),
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("verdict") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
