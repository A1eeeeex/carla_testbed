#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.prediction_evidence import (  # noqa: E402
    analyze_prediction_evidence_files,
    analyze_prediction_evidence_run_dir,
    write_prediction_evidence_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Apollo prediction evidence report.")
    parser.add_argument("--run-dir", help="Run artifact directory.")
    parser.add_argument("--channel-stats", help="channel_stats.json.")
    parser.add_argument("--cyber-bridge-stats", help="cyber_bridge_stats.json.")
    parser.add_argument("--summary", help="summary.json.")
    parser.add_argument("--manifest", help="manifest.json.")
    parser.add_argument("--planning-topic-debug-summary", help="planning_topic_debug_summary.json.")
    parser.add_argument("--obstacle-gt-contract", help="obstacle_gt_contract_report.json.")
    parser.add_argument("--prediction-log", action="append", default=[], help="Prediction log file.")
    parser.add_argument(
        "--replacement-matrix",
        default="configs/reference/apollo_gt_replacement_matrix.yaml",
        help="GT replacement matrix used to resolve explicit scenario-scoped prediction bypass.",
    )
    parser.add_argument(
        "--no-replacement-matrix",
        action="store_true",
        help="Disable replacement-matrix bypass lookup; useful for negative tests.",
    )
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    replacement_matrix = None if args.no_replacement_matrix else args.replacement_matrix
    if args.run_dir:
        report = analyze_prediction_evidence_run_dir(
            args.run_dir,
            replacement_matrix_path=replacement_matrix,
        )
    else:
        report = analyze_prediction_evidence_files(
            channel_stats=args.channel_stats,
            bridge_runtime_stats=args.cyber_bridge_stats,
            summary=args.summary,
            manifest=args.manifest,
            planning_topic_debug_summary=args.planning_topic_debug_summary,
            obstacle_gt_contract=args.obstacle_gt_contract,
            prediction_logs=args.prediction_log,
            replacement_matrix_path=replacement_matrix,
        )
    outputs = write_prediction_evidence_report(report, args.out)
    print(
        json.dumps(
            {
                "prediction_mode": report.get("prediction_mode"),
                "verdict": report.get("verdict"),
                "hard_gate_eligible": report.get("hard_gate_eligible"),
                "blocking_capabilities": report.get("blocking_capabilities") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("verdict") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
