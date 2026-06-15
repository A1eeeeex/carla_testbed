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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Apollo HDMap identity evidence for a run.")
    parser.add_argument("--run-dir", required=True, help="Run artifact directory.")
    parser.add_argument(
        "--expected-apollo-map-name",
        default="carla_town01",
        help="Expected Apollo HDMap directory/name, default carla_town01.",
    )
    parser.add_argument(
        "--out",
        help="Output directory. Defaults to <run-dir>/artifacts.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir).expanduser()
    report = analyze_apollo_map_identity_run_dir(
        run_dir,
        expected_apollo_map_name=args.expected_apollo_map_name,
    )
    outputs = write_apollo_map_identity_report(report, Path(args.out).expanduser() if args.out else run_dir / "artifacts")
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
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
