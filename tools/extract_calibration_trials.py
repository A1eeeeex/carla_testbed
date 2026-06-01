#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.calibration.trial_extraction import (
    discover_timeseries_paths,
    extract_control_actuation_trials,
    load_timeseries_rows,
    write_calibration_trials_csv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract minimal control-actuation calibration trials from run timeseries artifacts."
    )
    parser.add_argument("--timeseries", action="append", type=Path, help="Timeseries CSV/JSONL/JSON path.")
    parser.add_argument("--run-dir", action="append", type=Path, help="Run directory containing timeseries artifacts.")
    parser.add_argument("--route-id", help="Route id override for extracted trials.")
    parser.add_argument("--backend", help="Backend override for extracted trials.")
    parser.add_argument("--fixed-dt-s", type=float, help="Fallback fixed dt when timeseries lacks sim_time.")
    parser.add_argument("--min-duration-s", type=float, default=0.1, help="Minimum active command segment duration.")
    parser.add_argument("--out", type=Path, required=True, help="Output calibration_trials.csv path.")
    return parser


def _iter_timeseries_paths(args: argparse.Namespace) -> list[Path]:
    paths: list[Path] = []
    paths.extend(args.timeseries or [])
    for run_dir in args.run_dir or []:
        paths.extend(discover_timeseries_paths(run_dir))
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        resolved = path.expanduser()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    timeseries_paths = _iter_timeseries_paths(args)
    if not timeseries_paths:
        print(json.dumps({"status": "missing_timeseries", "output": str(args.out), "trial_count": 0}, indent=2))
        return 2

    trials = []
    inputs = []
    for path in timeseries_paths:
        rows = load_timeseries_rows(path)
        extracted = extract_control_actuation_trials(
            rows,
            route_id=args.route_id,
            backend=args.backend,
            min_duration_s=args.min_duration_s,
            fixed_dt_s=args.fixed_dt_s,
        )
        trials.extend(extracted)
        inputs.append({"path": str(path), "rows": len(rows), "trials": len(extracted)})

    write_calibration_trials_csv(args.out, trials)
    counts = Counter(str(row.get("command_type") or "") for row in trials)
    print(
        json.dumps(
            {
                "status": "ok",
                "output": str(args.out),
                "trial_count": len(trials),
                "command_counts": dict(sorted(counts.items())),
                "inputs": inputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
