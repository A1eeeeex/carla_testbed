#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_control_runtime_diff import (  # noqa: E402
    analyze_apollo_control_runtime_diff,
    write_apollo_control_runtime_diff,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare Apollo control runtime artifacts across two Phase 1 runs."
    )
    parser.add_argument("--baseline-run", required=True, help="Run directory for the baseline/no-overlay run.")
    parser.add_argument("--candidate-run", required=True, help="Run directory for the candidate/overlay run.")
    parser.add_argument("--out", required=True, help="Output directory for the diff report.")
    args = parser.parse_args(argv)

    report = analyze_apollo_control_runtime_diff(
        baseline_run=args.baseline_run,
        candidate_run=args.candidate_run,
    )
    outputs = write_apollo_control_runtime_diff(report, args.out)
    print(json.dumps({"status": report["verdict"]["status"], "outputs": outputs}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
