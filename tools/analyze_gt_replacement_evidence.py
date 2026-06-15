#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.gt_replacement_evidence import (  # noqa: E402
    analyze_gt_replacement_evidence_run_dir,
    write_gt_replacement_evidence_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Apollo GT replacement evidence report.")
    parser.add_argument("--run-dir", required=True, help="Run artifact directory.")
    parser.add_argument(
        "--reference-chain",
        default="configs/reference/apollo_reference_chain.yaml",
        help="Apollo reference chain YAML.",
    )
    parser.add_argument(
        "--replacement-matrix",
        default="configs/reference/apollo_gt_replacement_matrix.yaml",
        help="Apollo GT replacement matrix YAML.",
    )
    parser.add_argument(
        "--out",
        help="Output directory. Defaults to <run-dir>/analysis/gt_replacement_evidence.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir).expanduser()
    out_dir = Path(args.out).expanduser() if args.out else run_dir / "analysis" / "gt_replacement_evidence"
    report = analyze_gt_replacement_evidence_run_dir(
        run_dir,
        reference_path=args.reference_chain,
        replacement_path=args.replacement_matrix,
    )
    outputs = write_gt_replacement_evidence_report(report, out_dir)
    print(
        json.dumps(
            {
                "verdict": report.get("verdict"),
                "failure_stage": report.get("failure_stage"),
                "gt_replacements_claim_grade": report.get("gt_replacements_claim_grade"),
                "blocking_replacements": report.get("blocking_replacements") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("verdict") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
