#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_reference_line_contract import (  # noqa: E402
    analyze_apollo_reference_line_contract_files,
    analyze_apollo_reference_line_contract_run_dir,
    write_apollo_reference_line_contract_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Apollo planning/control reference-line contract evidence.")
    parser.add_argument("--run-dir", help="Run directory containing Apollo debug artifacts.")
    parser.add_argument("--contract", help="artifacts/apollo_reference_line_contract.jsonl")
    parser.add_argument("--planning-topic-debug", help="artifacts/planning_topic_debug.jsonl")
    parser.add_argument("--planning-route-segment-debug", help="artifacts/planning_route_segment_debug.jsonl")
    parser.add_argument("--planning-info-log", help="artifacts/apollo_planning.INFO")
    parser.add_argument("--control-decode-debug", help="artifacts/control_decode_debug.jsonl or bridge_control_decode.jsonl")
    parser.add_argument("--debug-timeseries", help="artifacts/debug_timeseries.csv or timeseries.csv")
    parser.add_argument("--localization-contract", help="analysis/localization_contract/localization_contract_report.json")
    parser.add_argument("--hdmap-projection", help="Optional Apollo HDMap projection artifact.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.run_dir:
        report = analyze_apollo_reference_line_contract_run_dir(args.run_dir)
    else:
        report = analyze_apollo_reference_line_contract_files(
            contract_path=args.contract,
            planning_topic_debug_path=args.planning_topic_debug,
            planning_route_segment_debug_path=args.planning_route_segment_debug,
            planning_info_log_path=args.planning_info_log,
            control_decode_debug_path=args.control_decode_debug,
            debug_timeseries_path=args.debug_timeseries,
            localization_contract_path=args.localization_contract,
            hdmap_projection_path=args.hdmap_projection,
        )
    outputs = write_apollo_reference_line_contract_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "outputs": outputs}, indent=2, sort_keys=True))
    return 1 if report.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
