#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_scenario_catalog import (  # noqa: E402
    analyze_phase1_scenario_catalog,
    write_phase1_scenario_catalog,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write Phase 1 scenario catalog readiness summary.")
    parser.add_argument("--repo", default=".")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    report = analyze_phase1_scenario_catalog(args.repo)
    outputs = write_phase1_scenario_catalog(report, args.out)
    print(json.dumps({"status": "ok", "outputs": outputs, "summary": report.get("summary")}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
