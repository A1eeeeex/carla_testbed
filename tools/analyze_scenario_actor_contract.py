#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.scenario_actor_contract import (
    analyze_scenario_actor_contract,
    analyze_scenario_actor_contract_run_dir,
    write_scenario_actor_contract_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze fixed-scene actor behavior contract")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run-dir", help="Run directory containing fixed-scene artifacts")
    source.add_argument("--storyboard", help="Path to fixed_scene_resolved.json")
    parser.add_argument("--trace", help="Path to scenario_actor_trace.jsonl")
    parser.add_argument("--phase-events", help="Path to scenario_phase_events.jsonl")
    parser.add_argument("--out", required=True, help="Output directory")
    args = parser.parse_args()

    if args.run_dir:
        report = analyze_scenario_actor_contract_run_dir(args.run_dir)
    else:
        report = analyze_scenario_actor_contract(
            storyboard_path=args.storyboard,
            trace_path=args.trace,
            events_path=args.phase_events,
        )
    write_scenario_actor_contract_report(report, args.out)
    return 0 if report["status"] in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
