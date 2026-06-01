#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.curve_pair_semantics import (
    compare_curve_pair_files,
    render_curve_pair_summary,
)


def _route_health_from_run_dir(run_dir: Path) -> Path:
    return run_dir / "analysis" / "route_health" / "route_health.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare curve217/curve213 route-health semantics.")
    parser.add_argument(
        "--route-health",
        action="append",
        type=Path,
        default=[],
        help="route_health.json path. Provide at least two.",
    )
    parser.add_argument(
        "--run-dir",
        action="append",
        type=Path,
        default=[],
        help="Run dir containing analysis/route_health/route_health.json.",
    )
    parser.add_argument("--out", type=Path, help="Directory to write curve_pair_semantics.json/md.")
    parser.add_argument("--onset-delta-threshold-seq", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = list(args.route_health)
    paths.extend(_route_health_from_run_dir(path) for path in args.run_dir)
    if len(paths) < 2:
        print("--route-health/--run-dir must provide at least two reports", file=sys.stderr)
        return 2
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        print(json.dumps({"status": "missing_inputs", "missing": missing}, indent=2, sort_keys=True))
        return 2
    report = compare_curve_pair_files(paths, onset_delta_threshold_seq=args.onset_delta_threshold_seq)
    outputs: dict[str, str] = {}
    if args.out is not None:
        args.out.mkdir(parents=True, exist_ok=True)
        json_path = args.out / "curve_pair_semantics.json"
        md_path = args.out / "curve_pair_semantics.md"
        json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        md_path.write_text(render_curve_pair_summary(report), encoding="utf-8")
        outputs = {
            "curve_pair_semantics_json": str(json_path),
            "curve_pair_semantics_md": str(md_path),
        }
    print(json.dumps({"report": report, "outputs": outputs}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
