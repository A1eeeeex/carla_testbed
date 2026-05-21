#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import safe_bool, safe_float, safe_int


def _load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _classify_row(row: Dict[str, str]) -> str:
    completion = safe_float(row.get("route_completion_ratio")) or 0.0
    persistent = safe_bool(row.get("persistent_path_fallback_at_end")) is True
    fallback_count = safe_int(row.get("path_fallback_count")) or 0
    label = str(row.get("route_health_label") or "")
    if label == "route_health_pass":
        return "pass"
    if persistent:
        return "persistent_fallback"
    if completion >= 0.55:
        return "candidate"
    if fallback_count >= 100:
        return "recoverable_fallback_pressure"
    return "underperforming"


def render_report(rows: List[Dict[str, str]], title: str) -> str:
    lines = [
        f"# {title}",
        "",
        f"- run_count: `{len(rows)}`",
        "",
        "## Route Classification",
        "",
    ]
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            -(safe_float(row.get("route_completion_ratio")) or 0.0),
            -(safe_float(row.get("route_distance_achieved_m")) or 0.0),
            str(row.get("route_id") or ""),
        ),
    )
    for row in sorted_rows:
        route_id = str(row.get("route_id") or "")
        classification = _classify_row(row)
        lines.append(
            f"- `{route_id}`: `{classification}`, "
            f"completion=`{row.get('route_completion_ratio')}`, "
            f"distance_m=`{row.get('route_distance_achieved_m')}`, "
            f"path_fallback_count=`{row.get('path_fallback_count')}`, "
            f"persistent_path_fallback_at_end=`{row.get('persistent_path_fallback_at_end')}`"
        )
    lines.extend(
        [
            "",
            "## Key Conclusion",
            "",
        ]
    )
    passing = [row for row in rows if _classify_row(row) == "pass"]
    candidates = [row for row in rows if _classify_row(row) == "candidate"]
    persistent = [row for row in rows if _classify_row(row) == "persistent_fallback"]
    pressure = [row for row in rows if _classify_row(row) == "recoverable_fallback_pressure"]
    lines.append(
        "- current split: "
        f"`pass={len(passing)}`, `candidate={len(candidates)}`, "
        f"`recoverable_fallback_pressure={len(pressure)}`, `persistent_fallback={len(persistent)}`"
    )
    if passing:
        lines.append(f"- best route: `{passing[0].get('route_id')}`")
    if candidates:
        lines.append(
            "- next-best candidate routes: "
            + ", ".join(f"`{row.get('route_id')}`" for row in candidates[:3])
        )
    if pressure:
        lines.append(
            "- recoverable fallback pressure routes: "
            + ", ".join(f"`{row.get('route_id')}`" for row in pressure[:3])
        )
    if persistent:
        lines.append(
            "- persistent fallback routes: "
            + ", ".join(f"`{row.get('route_id')}`" for row in persistent[:3])
        )
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            "- keep `pass/candidate` routes in first-wave guarded lateral smoke",
            "- deprioritize `persistent_fallback` and `recoverable_fallback_pressure` routes from first-wave smoke",
            "- continue Apollo planning-side root-cause analysis on the worst fallback routes",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize guarded lateral completion gaps for a Town01 batch.")
    parser.add_argument(
        "--comparison-csv",
        default="runs/town01_route_health_lateral_subset_cleaned_regress_20260326/artifacts/town01_route_health_platform_comparison.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/town01_lateral_completion_gap_report_20260326.md",
    )
    parser.add_argument(
        "--title",
        default="Town01 Lateral Completion Gap Report 2026-03-26",
    )
    args = parser.parse_args()

    rows = _load_rows(Path(args.comparison_csv).expanduser().resolve())
    report_path = Path(args.report).expanduser().resolve()
    report_path.write_text(render_report(rows, args.title), encoding="utf-8")
    print(f"[town01-lateral-completion-gap] rows={len(rows)} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
