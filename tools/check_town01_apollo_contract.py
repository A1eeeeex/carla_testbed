#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.apollo.town01_contract import (  # noqa: E402
    check_town01_apollo_contract_file,
    write_town01_apollo_contract_report,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the offline Town01 Apollo route/HDMap/signal contract."
    )
    parser.add_argument("--config", required=True, help="Path to the Town01 Apollo contract YAML.")
    parser.add_argument("--out", required=True, help="Output directory for town01_apollo_contract_report.json.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = check_town01_apollo_contract_file(args.config)
    outputs = write_town01_apollo_contract_report(report, args.out)
    payload = {
        "status": report["status"],
        "report": outputs["town01_apollo_contract_report"],
        "route_count": len(report.get("route_results", [])),
        "signal_count": len(report.get("signal_results", [])),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
