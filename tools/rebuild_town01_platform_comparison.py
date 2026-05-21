#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import write_platform_comparison, write_platform_report


DEFAULT_BATCH_ROOTS = [
    "runs/town01_route_health_longwarm_probe_20260325",
    "runs/town01_route_health_lat_controller_fix_rerun_20260325",
    "runs/town01_route_health_lateral_subset_xrandr_windowfix_20260326",
    "runs/town01_route_health_lateral_units_fix_probe_20260326",
    "runs/town01_route_health_lateral_units_fix_pair_rerun_20260326",
    "runs/town01_route_health_lateral_subset_post_pool_fix_20260326",
    "runs/town01_route_health_lateral_longticks_064_900_20260326",
    "runs/town01_route_health_lateral_longticks_097_900_20260326",
    "runs/town01_route_health_lateral_subset_cleaned_regress_20260326",
    "runs/town01_route_health_guarded_lateral_first_wave_smoke_20260326",
]
DEFAULT_BATCH_ROOT_GLOBS = [
    "runs/town01_route_health_guarded_lateral_first_wave_repeat_*",
    "runs/town01_route_health_stage6_cache_clear_probe_*",
    "runs/town01_route_health_stage6_generation_guard_probe_*",
]


def _load_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _merge_rows(batch_roots: List[Path]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen_run_dirs = set()
    for batch_root in batch_roots:
        csv_path = batch_root / "artifacts" / "town01_route_health_platform_comparison.csv"
        if not csv_path.exists():
            continue
        for row in _load_rows(csv_path):
            run_dir = str(row.get("run_dir") or "").strip()
            if not run_dir or run_dir in seen_run_dirs:
                continue
            seen_run_dirs.add(run_dir)
            rows.append(dict(row))
    rows.sort(
        key=lambda item: (
            str(item.get("comparison_label") or ""),
            str(item.get("route_id") or ""),
            str(item.get("run_dir") or ""),
        )
    )
    return rows


def _resolved_default_batch_roots() -> List[Path]:
    ordered: List[Path] = []
    seen: set[Path] = set()
    for item in DEFAULT_BATCH_ROOTS:
        path = Path(item).expanduser().resolve()
        if path in seen:
            continue
        seen.add(path)
        ordered.append(path)
    for pattern in DEFAULT_BATCH_ROOT_GLOBS:
        for path in sorted(REPO_ROOT.glob(pattern)):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            ordered.append(resolved)
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild stable Town01 global comparison/report from batch-local comparison CSVs.")
    parser.add_argument(
        "--batch-root",
        action="append",
        dest="batch_roots",
        help="Batch root containing artifacts/town01_route_health_platform_comparison.csv. Can be repeated.",
    )
    parser.add_argument(
        "--comparison-out",
        default="artifacts/town01_route_health_platform_comparison.csv",
        help="Output path for rebuilt global comparison CSV.",
    )
    parser.add_argument(
        "--report-out",
        default="artifacts/town01_route_health_platform_mainline_report.md",
        help="Output path for rebuilt global platform report.",
    )
    args = parser.parse_args()

    batch_roots = [Path(item).expanduser().resolve() for item in (args.batch_roots or _resolved_default_batch_roots())]
    rows = _merge_rows(batch_roots)
    comparison_out = Path(args.comparison_out).expanduser().resolve()
    report_out = Path(args.report_out).expanduser().resolve()
    write_platform_comparison(rows, comparison_out)
    write_platform_report(rows, report_out)
    print(
        "[town01-platform-rebuild] "
        f"rows={len(rows)} "
        f"batch_root_count={len(batch_roots)} "
        f"comparison_out={comparison_out}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
