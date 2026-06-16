#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_status import classify_phase1_run, write_phase1_status  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Classify a Phase 1 ScenarioRun.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    report = classify_phase1_run(args.run_dir)
    outputs = {}
    if args.out:
        outputs = write_phase1_status(report, args.out)
    print(json.dumps({"status": report.get("status"), "failure_reason": report.get("failure_reason"), "outputs": outputs}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
