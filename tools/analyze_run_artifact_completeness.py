#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.artifact_completeness import (  # noqa: E402
    check_run_artifact_completeness,
    write_run_artifact_completeness_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write run artifact completeness evidence.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--out", help="Output directory. Defaults to <run-dir>/analysis/artifact_completeness.")
    parser.add_argument("--scenario-class")
    parser.add_argument(
        "--profile",
        choices=["natural_driving", "phase1"],
        default="natural_driving",
        help="Artifact surface to validate. Phase 1 checks scenario-run artifacts, not Apollo claim artifacts.",
    )
    args = parser.parse_args(argv)
    run_dir = Path(args.run_dir).expanduser()
    out_dir = Path(args.out).expanduser() if args.out else run_dir / "analysis" / "artifact_completeness"
    report = check_run_artifact_completeness(
        run_dir,
        scenario_class=args.scenario_class,
        profile=args.profile,
    )
    outputs = write_run_artifact_completeness_report(report, out_dir)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "artifact_complete": report.get("artifact_complete"),
                "missing_artifacts": report.get("missing_artifacts") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
