#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.traffic_flow_contract import (  # noqa: E402
    analyze_traffic_flow_contract_files,
    write_traffic_flow_contract_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze CARLA Traffic Manager traffic flow contract.")
    parser.add_argument("--run-dir", help="Run directory to discover traffic_flow_manifest/events.")
    parser.add_argument("--manifest", help="artifacts/traffic_flow_manifest.json path.")
    parser.add_argument("--events", help="artifacts/traffic_flow_events.jsonl path.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_traffic_flow_contract_files(
        run_dir=args.run_dir,
        manifest_path=args.manifest,
        events_path=args.events,
    )
    outputs = write_traffic_flow_contract_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "warnings": report.get("warnings") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if report.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
