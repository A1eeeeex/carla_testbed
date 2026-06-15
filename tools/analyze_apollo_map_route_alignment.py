#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_map_identity import (  # noqa: E402
    analyze_apollo_map_identity_run_dir,
    write_apollo_map_identity_report,
)
from carla_testbed.analysis.apollo_map_route_alignment import (  # noqa: E402
    analyze_apollo_map_route_alignment_run_dir,
    write_apollo_map_route_alignment_report,
)
from carla_testbed.analysis.apollo_route_contract import build_lane_equivalence_town01  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Apollo map, route, lane, and reference-line alignment.")
    parser.add_argument("--run-dir", required=True, help="Run artifact directory.")
    parser.add_argument(
        "--expected-apollo-map-name",
        default="carla_town01",
        help="Expected Apollo HDMap directory/name, default carla_town01.",
    )
    parser.add_argument(
        "--out",
        help="Output directory. Defaults to <run-dir>/analysis/apollo_map_route_alignment.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir).expanduser()
    map_identity = analyze_apollo_map_identity_run_dir(
        run_dir,
        expected_apollo_map_name=args.expected_apollo_map_name,
    )
    write_apollo_map_identity_report(map_identity, run_dir / "artifacts")
    report = analyze_apollo_map_route_alignment_run_dir(
        run_dir,
        expected_apollo_map_name=args.expected_apollo_map_name,
    )
    route_contract_path = run_dir / "analysis" / "apollo_route_contract" / "apollo_route_contract_report.json"
    lane_equivalence = None
    if route_contract_path.exists():
        lane_equivalence = build_lane_equivalence_town01(json.loads(route_contract_path.read_text(encoding="utf-8")))
    outputs = write_apollo_map_route_alignment_report(
        report,
        Path(args.out).expanduser() if args.out else run_dir / "analysis" / "apollo_map_route_alignment",
        lane_equivalence=lane_equivalence,
        lane_equivalence_out_dir=run_dir / "artifacts" if lane_equivalence is not None else None,
    )
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "diagnosis": report.get("diagnosis"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "warnings": report.get("warnings") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
