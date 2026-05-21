#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import (
    _capability_direction_class,
    _capability_geometry_class,
    default_corpus_path,
    load_route_corpus,
    safe_float,
)
from tools.analyze_town01_capability_progress import (
    _best_history_row,
    _best_row,
    _historical_row_status,
    _load_csv_rows,
    _load_historical_summary_rows,
    route_status,
)


PROFILE_TO_CANDIDATE_SUBSET = {
    "lane_keep": "lane_keep_candidate",
    "curve_lane_follow": "curve_lane_follow_candidate",
    "junction_traverse": "junction_traverse_candidate",
    "traffic_light_actual": "traffic_light_candidate",
}

REVIEWED_STATUSES = {"pass", "candidate", "reviewed"}


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_batch_summary_rows(batch_root: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not batch_root.exists():
        return rows
    for summary_path in sorted(batch_root.rglob("summary.json")):
        summary = _load_json(summary_path)
        route_id = str(summary.get("route_id") or "").strip()
        if not route_id:
            continue
        rows.append(
            {
                "route_id": route_id,
                "comparison_label": str(summary.get("comparison_label") or "").strip(),
                "route_health_label": str(summary.get("route_health_label") or "").strip(),
                "route_completion_ratio": safe_float(summary.get("route_completion_ratio")) or 0.0,
                "route_distance_achieved_m": safe_float(summary.get("route_distance_achieved_m")) or 0.0,
                "summary_path": str(summary_path),
            }
        )
    return rows


def _group_rows_by_route(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        route_id = str(row.get("route_id") or "").strip()
        if not route_id:
            continue
        grouped.setdefault(route_id, []).append(row)
    return grouped


def _pack_readiness(
    *,
    batch_reviewed_count: int,
    batch_pass_count: int,
    history_reviewed_count: int,
    history_pass_count: int,
    route_count: int,
) -> str:
    if route_count <= 0:
        return "empty"
    if batch_reviewed_count >= route_count:
        return "batch_complete_with_pass" if batch_pass_count > 0 else "batch_complete_reviewed"
    if batch_reviewed_count > 0:
        return "batch_partial_with_pass" if batch_pass_count > 0 else "batch_partial"
    if history_reviewed_count >= route_count:
        return "history_complete_with_pass" if history_pass_count > 0 else "history_complete_reviewed"
    if history_reviewed_count > 0:
        return "history_partial_with_pass" if history_pass_count > 0 else "history_partial"
    return "no_evidence"


def build_manifest_review_rows(
    manifest: Dict[str, Any],
    corpus: Dict[str, Any],
    comparison_rows: Sequence[Dict[str, str]],
    historical_summary_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    routes_by_id = {
        str(route.get("route_id") or "").strip(): route
        for route in list(corpus.get("routes") or [])
        if isinstance(route, dict) and str(route.get("route_id") or "").strip()
    }
    comparison_by_route = _group_rows_by_route(comparison_rows)
    results: List[Dict[str, Any]] = []
    for entry in list(manifest.get("entries") or []):
        if not isinstance(entry, dict):
            continue
        capability_profile = str(entry.get("capability_profile") or "").strip()
        candidate_subset = PROFILE_TO_CANDIDATE_SUBSET.get(capability_profile, "")
        semantic_key = f"semantic::{candidate_subset}" if candidate_subset else ""
        batch_root = Path(str(entry.get("batch_root") or "")).expanduser()
        batch_rows = _load_batch_summary_rows(batch_root)
        batch_rows_by_route = _group_rows_by_route(batch_rows)
        route_rows: List[Dict[str, Any]] = []
        batch_reviewed_count = 0
        batch_pass_count = 0
        history_reviewed_count = 0
        history_pass_count = 0
        for route_id in [str(item).strip() for item in list(entry.get("route_ids") or []) if str(item).strip()]:
            route = routes_by_id.get(route_id) or {}
            comparison_best = _best_row(comparison_by_route.get(route_id) or [])
            history_rows = [
                row
                for row in historical_summary_rows
                if str(row.get("route_id") or "").strip() == route_id
                and (not semantic_key or bool(row.get(semantic_key)))
            ]
            batch_best = _best_history_row(batch_rows_by_route.get(route_id) or [])
            history_best = _best_history_row(history_rows)
            batch_status = _historical_row_status(batch_best) if batch_best else "missing"
            history_status = _historical_row_status(history_best) if history_best else "missing"
            if batch_status in REVIEWED_STATUSES:
                batch_reviewed_count += 1
            if batch_status == "pass":
                batch_pass_count += 1
            if history_status in REVIEWED_STATUSES:
                history_reviewed_count += 1
            if history_status == "pass":
                history_pass_count += 1
            route_rows.append(
                {
                    "route_id": route_id,
                    "corpus_status": route_status(route),
                    "geometry_class": _capability_geometry_class(route, candidate_subset),
                    "direction_class": _capability_direction_class(route, candidate_subset),
                    "batch_status": batch_status,
                    "batch_label": str(batch_best.get("route_health_label") or "").strip() or "none",
                    "batch_completion": safe_float(batch_best.get("route_completion_ratio")) or 0.0,
                    "batch_comparison_label": str(batch_best.get("comparison_label") or "").strip() or "none",
                    "history_status": history_status,
                    "history_label": str(history_best.get("route_health_label") or "").strip() or "none",
                    "history_completion": safe_float(history_best.get("route_completion_ratio")) or 0.0,
                    "history_comparison_label": str(history_best.get("comparison_label") or "").strip() or "none",
                    "current_best_label": str(comparison_best.get("route_health_label") or "").strip() or "none",
                    "current_best_completion": safe_float(comparison_best.get("route_completion_ratio")) or 0.0,
                    "current_best_comparison_label": str(comparison_best.get("comparison_label") or "").strip() or "none",
                }
            )
        missing_batch_routes = [row["route_id"] for row in route_rows if row["batch_status"] == "missing"]
        missing_history_routes = [row["route_id"] for row in route_rows if row["history_status"] == "missing"]
        results.append(
            {
                "capability_profile": capability_profile,
                "subset_name": str(entry.get("subset_name") or "").strip(),
                "comparison_label": str(entry.get("comparison_label") or "").strip(),
                "ticks": int(entry.get("ticks") or 700),
                "sample_size": int(entry.get("sample_size") or len(route_rows)),
                "extra_args": list(entry.get("extra_args") or []),
                "batch_root": str(batch_root),
                "route_ids_file": str(entry.get("route_ids_file") or "").strip(),
                "preset_overrides_file": str(entry.get("preset_overrides_file") or "").strip(),
                "overrides_file": str(entry.get("overrides_file") or "").strip(),
                "route_count": len(route_rows),
                "effective_sample_size": int(entry.get("effective_sample_size") or len(route_rows)),
                "batch_observed_route_count": len(route_rows) - len(missing_batch_routes),
                "batch_reviewed_count": batch_reviewed_count,
                "batch_pass_count": batch_pass_count,
                "history_observed_route_count": len(route_rows) - len(missing_history_routes),
                "history_reviewed_count": history_reviewed_count,
                "history_pass_count": history_pass_count,
                "missing_batch_routes": missing_batch_routes,
                "missing_history_routes": missing_history_routes,
                "pack_readiness": _pack_readiness(
                    batch_reviewed_count=batch_reviewed_count,
                    batch_pass_count=batch_pass_count,
                    history_reviewed_count=history_reviewed_count,
                    history_pass_count=history_pass_count,
                    route_count=len(route_rows),
                ),
                "route_rows": route_rows,
            }
        )
    return results


def render_report(
    rows: Sequence[Dict[str, Any]],
    *,
    manifest_path: Path,
    comparison_path: Path,
    corpus_path: Path,
) -> str:
    lines = [
        "# Town01 Capability Review Manifest Report",
        "",
        f"- generated_at_local: `{datetime.now().isoformat(timespec='seconds')}`",
        f"- manifest_path: `{manifest_path}`",
        f"- comparison_path: `{comparison_path}`",
        f"- corpus_path: `{corpus_path}`",
        "",
        "## Summary",
        "",
    ]
    for row in rows:
        lines.append(
            f"- `{row['capability_profile']}` -> `{row['subset_name']}` | readiness=`{row['pack_readiness']}` | batch_reviewed=`{row['batch_reviewed_count']}/{row['route_count']}` | history_reviewed=`{row['history_reviewed_count']}/{row['route_count']}` | missing_batch=`{', '.join(row['missing_batch_routes']) or 'none'}` | missing_history=`{', '.join(row['missing_history_routes']) or 'none'}`"
        )
    lines.extend(["", "## Route Detail", ""])
    for row in rows:
        lines.append(f"### {row['capability_profile']}")
        lines.append("")
        lines.append(f"- subset: `{row['subset_name']}`")
        lines.append(f"- comparison_label: `{row['comparison_label']}`")
        lines.append(f"- route_ids_file: `{row.get('route_ids_file') or 'none'}`")
        lines.append(f"- preset_overrides_file: `{row.get('preset_overrides_file') or 'none'}`")
        lines.append(f"- overrides_file: `{row.get('overrides_file') or 'none'}`")
        lines.append(f"- batch_root: `{row['batch_root']}`")
        lines.append(f"- pack_readiness: `{row['pack_readiness']}`")
        lines.append("")
        for route_row in list(row.get("route_rows") or []):
            lines.append(
                "- `{route_id}` | class=`{geometry_class}` | dir=`{direction_class}` | corpus=`{corpus_status}` | batch=`{batch_status}` ({batch_label}, {batch_completion:.3f}, {batch_comparison_label}) | history=`{history_status}` ({history_label}, {history_completion:.3f}, {history_comparison_label}) | current_best=`{current_best_label}` ({current_best_completion:.3f}, {current_best_comparison_label})".format(
                    **route_row
                )
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_summary_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    fieldnames = [
        "capability_profile",
        "subset_name",
        "comparison_label",
        "route_count",
        "effective_sample_size",
        "batch_observed_route_count",
        "batch_reviewed_count",
        "batch_pass_count",
        "history_observed_route_count",
        "history_reviewed_count",
        "history_pass_count",
        "pack_readiness",
        "missing_batch_routes",
        "missing_history_routes",
        "route_ids_file",
        "preset_overrides_file",
        "overrides_file",
        "batch_root",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "capability_profile": row["capability_profile"],
                    "subset_name": row["subset_name"],
                    "comparison_label": row["comparison_label"],
                    "route_count": row["route_count"],
                    "effective_sample_size": row["effective_sample_size"],
                    "batch_observed_route_count": row["batch_observed_route_count"],
                    "batch_reviewed_count": row["batch_reviewed_count"],
                    "batch_pass_count": row["batch_pass_count"],
                    "history_observed_route_count": row["history_observed_route_count"],
                    "history_reviewed_count": row["history_reviewed_count"],
                    "history_pass_count": row["history_pass_count"],
                    "pack_readiness": row["pack_readiness"],
                    "missing_batch_routes": ",".join(row["missing_batch_routes"]),
                    "missing_history_routes": ",".join(row["missing_history_routes"]),
                    "route_ids_file": row.get("route_ids_file") or "",
                    "preset_overrides_file": row.get("preset_overrides_file") or "",
                    "overrides_file": row.get("overrides_file") or "",
                    "batch_root": row["batch_root"],
                }
            )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze frozen Town01 capability review manifest coverage.")
    parser.add_argument("--manifest", type=Path, default=REPO_ROOT / "artifacts" / f"town01_capability_review_manifest_{datetime.now().strftime('%Y%m%d')}.json")
    parser.add_argument("--corpus", type=Path, default=default_corpus_path(REPO_ROOT))
    parser.add_argument("--comparison", type=Path, default=REPO_ROOT / "artifacts" / "town01_route_health_platform_comparison.csv")
    parser.add_argument("--runs-root", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "artifacts" / f"town01_capability_review_manifest_report_{datetime.now().strftime('%Y%m%d')}.md")
    parser.add_argument("--summary-csv-output", type=Path, default=REPO_ROOT / "artifacts" / f"town01_capability_review_manifest_summary_{datetime.now().strftime('%Y%m%d')}.csv")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    manifest_path = Path(args.manifest).expanduser().resolve()
    corpus_path = Path(args.corpus).expanduser().resolve()
    comparison_path = Path(args.comparison).expanduser().resolve()
    runs_root = Path(args.runs_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    summary_csv_output_path = Path(args.summary_csv_output).expanduser().resolve()

    manifest = _load_json(manifest_path)
    corpus = load_route_corpus(corpus_path)
    comparison_rows = _load_csv_rows(comparison_path)
    historical_summary_rows = _load_historical_summary_rows(runs_root)
    rows = build_manifest_review_rows(
        manifest,
        corpus,
        comparison_rows,
        historical_summary_rows,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_report(rows, manifest_path=manifest_path, comparison_path=comparison_path, corpus_path=corpus_path),
        encoding="utf-8",
    )
    write_summary_csv(summary_csv_output_path, rows)
    print(f"[town01-capability-review-manifest] written: {output_path}")
    print(f"[town01-capability-review-manifest] summary: {summary_csv_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
