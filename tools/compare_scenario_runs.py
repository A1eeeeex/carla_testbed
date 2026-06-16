#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs, write_scenario_comparison  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare Phase 1 ScenarioRun artifacts.")
    parser.add_argument("run_dirs", nargs="+")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    report = compare_scenario_runs(args.run_dirs)
    outputs = write_scenario_comparison(report, args.out)
    print(json.dumps({"comparison_status": report.get("comparison_status"), "reason": report.get("reason"), "outputs": outputs}, indent=2, sort_keys=True))
    return 1 if report.get("comparison_status") == "invalid" else 0


if __name__ == "__main__":
    raise SystemExit(main())
