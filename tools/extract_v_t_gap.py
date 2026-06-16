#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.v_t_gap import extract_v_t_gap, write_v_t_gap_report  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract Phase 1 ego-v / target-v / gap curves.")
    parser.add_argument("--run-dir")
    parser.add_argument("--timeseries")
    parser.add_argument("--actor-trace")
    parser.add_argument("--fixed-scene-resolved")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    report = extract_v_t_gap(
        run_dir=args.run_dir,
        timeseries=args.timeseries,
        actor_trace=args.actor_trace,
        fixed_scene_resolved=args.fixed_scene_resolved,
    )
    outputs = write_v_t_gap_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "invalid_reason": report.get("invalid_reason"), "outputs": outputs}, indent=2, sort_keys=True))
    return 1 if report.get("status") == "invalid" else 0


if __name__ == "__main__":
    raise SystemExit(main())
