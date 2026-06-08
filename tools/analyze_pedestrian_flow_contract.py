#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.pedestrian_flow_contract import (  # noqa: E402
    analyze_pedestrian_flow_contract_files,
    write_pedestrian_flow_contract_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze CARLA pedestrian flow contract artifacts.")
    parser.add_argument("--run-dir", help="Run directory to discover traffic_flow_manifest/events.")
    parser.add_argument("--manifest", help="artifacts/traffic_flow_manifest.json path.")
    parser.add_argument("--events", help="artifacts/traffic_flow_events.jsonl path.")
    parser.add_argument("--candidates", help="artifacts/walker_spawn_candidates.jsonl path.")
    parser.add_argument("--out", required=True, help="Output directory.")
    args = parser.parse_args()

    report = analyze_pedestrian_flow_contract_files(
        run_dir=args.run_dir,
        manifest_path=args.manifest,
        events_path=args.events,
        candidates_path=args.candidates,
    )
    outputs = write_pedestrian_flow_contract_report(report, args.out)
    print(outputs["pedestrian_flow_contract_report"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
