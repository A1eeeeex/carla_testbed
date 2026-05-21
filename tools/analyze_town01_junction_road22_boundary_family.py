#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json, load_jsonl, safe_float, safe_int


SURFACE_FILES: Sequence[Tuple[str, str]] = (
    ("planning", "planning_topic_debug.jsonl"),
    ("stage5", "stage5_apollo_reference_line_debug.jsonl"),
    ("apollo_route", "apollo_route_segment_debug.jsonl"),
    ("apollo_ref", "apollo_reference_line_debug.jsonl"),
    ("lane_follow", "stage5_apollo_lane_follow_map_debug.jsonl"),
)

NONPLANNING_SURFACES = ("stage5", "apollo_route", "apollo_ref", "lane_follow")


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _first_row(rows: Sequence[Dict[str, Any]], predicate) -> Optional[Dict[str, Any]]:
    for row in rows:
        if predicate(row):
            return row
    return None


def _nearest_row(rows: Sequence[Dict[str, Any]], target_ts: Optional[float]) -> Optional[Dict[str, Any]]:
    if target_ts is None:
        return None
    candidates = [row for row in rows if safe_float(row.get("timestamp")) is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda row: abs((safe_float(row.get("timestamp")) or 0.0) - target_ts))


def _row_has_lane_semantics(row: Optional[Dict[str, Any]]) -> bool:
    if not row:
        return False
    return any(
        bool(_safe_text(row.get(key)).strip())
        for key in ("lane_id_first", "target_lane_id_first", "current_lane_id", "target_lane_id")
    )


def _row_primary_lane_id(row: Optional[Dict[str, Any]]) -> str:
    if not row:
        return ""
    for key in ("lane_id_first", "target_lane_id_first", "current_lane_id", "target_lane_id"):
        value = _safe_text(row.get(key)).strip()
        if value:
            return value
    return ""


def _row_primary_lane_road(row: Optional[Dict[str, Any]]) -> str:
    if not row:
        return ""
    for key in ("lane_id_first_road_id", "target_lane_id_first_road_id", "current_lane_road_id", "target_lane_road_id"):
        value = _safe_text(row.get(key)).strip()
        if value:
            return value
    return ""


def _row_status_tuple(row: Optional[Dict[str, Any]]) -> Tuple[Optional[int], str, str, str, str]:
    if not row:
        return (None, "", "", "", "")
    return (
        safe_int(row.get("reference_line_count")),
        _safe_text(row.get("create_route_segments_status")),
        _safe_text(row.get("reference_line_provider_status")),
        _safe_text(row.get("lane_follow_map_status")),
        _safe_text(row.get("planning_empty_reason_guess")),
    )


def _row_status_family(row: Optional[Dict[str, Any]]) -> str:
    ref_count, create_status, provider_status, lane_follow_status, empty_reason = _row_status_tuple(row)
    return (
        f"ref{ref_count if ref_count is not None else 'none'}"
        f"_{create_status or 'none'}"
        f"_{provider_status or 'none'}"
        f"_{lane_follow_status or 'none'}"
        f"_{empty_reason or 'none'}"
    )


def _row_summary(row: Optional[Dict[str, Any]]) -> str:
    if not row:
        return "missing"
    road = _row_primary_lane_road(row) or "none"
    lane = _row_primary_lane_id(row) or "none"
    route_count = safe_int(row.get("route_segment_count"))
    routing_road_count = safe_int(row.get("routing_road_count"))
    return (
        f"roads={routing_road_count if routing_road_count is not None else 'none'}"
        f", route_segments={route_count if route_count is not None else 'none'}"
        f", road={road}"
        f", lane={lane}"
        f", status={_row_status_family(row)}"
    )


def _delta(row: Optional[Dict[str, Any]], base_ts: Optional[float]) -> Optional[float]:
    if not row or base_ts is None:
        return None
    row_ts = safe_float(row.get("timestamp"))
    if row_ts is None:
        return None
    return row_ts - base_ts


def _fmt_float(value: Optional[float], digits: int = 3) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def _bool_text(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return ""


def _build_snapshot(label: str, run_dir: Path) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    rows_by_surface = {surface_name: load_jsonl(run_dir / "artifacts" / relpath) for surface_name, relpath in SURFACE_FILES}
    for surface_name, rows in rows_by_surface.items():
        if not rows:
            raise SystemExit(f"missing {surface_name} rows: {run_dir}")
    routing_event_rows = load_jsonl(run_dir / "artifacts" / "routing_event_debug.jsonl")
    if not routing_event_rows:
        raise SystemExit(f"missing routing_event rows: {run_dir}")

    long_phase_row = _first_row(
        routing_event_rows, lambda row: _safe_text(row.get("reroute_reason")) == "long_phase_transition"
    )
    if not long_phase_row:
        raise SystemExit(f"missing long_phase_transition: {run_dir}")
    long_phase_ts = safe_float(long_phase_row.get("timestamp"))
    if long_phase_ts is None:
        raise SystemExit(f"invalid long_phase_transition timestamp: {run_dir}")

    planning_rows = rows_by_surface["planning"]
    boundary_idx = next(
        idx
        for idx, row in enumerate(planning_rows)
        if safe_float(row.get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= long_phase_ts
        and (safe_int(row.get("routing_road_count")) or 0) >= 22
    )
    boundary_row = planning_rows[boundary_idx]
    boundary_ts = safe_float(boundary_row.get("timestamp"))
    if boundary_ts is None:
        raise SystemExit(f"invalid boundary timestamp: {run_dir}")

    boundary_prev_row = _first_row(
        list(reversed(planning_rows[:boundary_idx])),
        lambda row: safe_float(row.get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= long_phase_ts,
    )
    planning_recovery_row = _first_row(
        planning_rows[boundary_idx:],
        lambda row: _row_has_lane_semantics(row),
    )

    phase_rows: Dict[str, Dict[str, Optional[Dict[str, Any]]]] = {}
    for surface_name, rows in rows_by_surface.items():
        boundary_row_for_surface = _nearest_row(rows, boundary_ts)
        prev_row = None
        if boundary_row_for_surface:
            boundary_pos = rows.index(boundary_row_for_surface)
            prev_row = _first_row(
                list(reversed(rows[:boundary_pos])),
                lambda row: safe_float(row.get("timestamp")) is not None
                and safe_float(row.get("timestamp")) >= long_phase_ts,
            )
        recovery_row = _first_row(
            rows,
            lambda row: safe_float(row.get("timestamp")) is not None
            and safe_float(row.get("timestamp")) >= boundary_ts
            and _row_has_lane_semantics(row),
        )
        phase_rows[surface_name] = {
            "prev": prev_row,
            "boundary": boundary_row_for_surface,
            "recovery": recovery_row,
        }

    nonplanning_prev_status_values = {
        _row_status_family(phase_rows[surface_name]["prev"]) for surface_name in NONPLANNING_SURFACES
    }
    nonplanning_boundary_status_values = {
        _row_status_family(phase_rows[surface_name]["boundary"]) for surface_name in NONPLANNING_SURFACES
    }
    nonplanning_recovery_status_values = {
        _row_status_family(phase_rows[surface_name]["recovery"]) for surface_name in NONPLANNING_SURFACES
    }
    nonplanning_prev_road_values = {
        _row_primary_lane_road(phase_rows[surface_name]["prev"]) for surface_name in NONPLANNING_SURFACES if _row_primary_lane_road(phase_rows[surface_name]["prev"])
    }
    nonplanning_boundary_road_values = {
        _row_primary_lane_road(phase_rows[surface_name]["boundary"]) for surface_name in NONPLANNING_SURFACES if _row_primary_lane_road(phase_rows[surface_name]["boundary"])
    }
    nonplanning_recovery_road_values = {
        _row_primary_lane_road(phase_rows[surface_name]["recovery"]) for surface_name in NONPLANNING_SURFACES if _row_primary_lane_road(phase_rows[surface_name]["recovery"])
    }

    family_transition = ""
    prev_status_example = next(iter(nonplanning_prev_status_values)) if nonplanning_prev_status_values else "missing"
    boundary_status_example = next(iter(nonplanning_boundary_status_values)) if nonplanning_boundary_status_values else "missing"
    recovery_status_example = next(iter(nonplanning_recovery_status_values)) if nonplanning_recovery_status_values else "missing"
    if prev_status_example != boundary_status_example and boundary_status_example != recovery_status_example:
        family_transition = f"{prev_status_example} -> {boundary_status_example} -> {recovery_status_example}"
    elif prev_status_example != boundary_status_example:
        family_transition = f"{prev_status_example} -> {boundary_status_example}"
    else:
        family_transition = prev_status_example

    boundary_family = ""
    planning_prev_has_semantics = _row_has_lane_semantics(boundary_prev_row)
    planning_boundary_has_semantics = _row_has_lane_semantics(boundary_row)
    planning_recovery_has_semantics = _row_has_lane_semantics(planning_recovery_row)
    if planning_prev_has_semantics and planning_boundary_has_semantics:
        boundary_family = "boundary_expands_preview_without_semantic_break"
    elif planning_prev_has_semantics and not planning_boundary_has_semantics and planning_recovery_has_semantics:
        boundary_family = "boundary_breaks_existing_semantics_then_restores"
    elif not planning_prev_has_semantics and not planning_boundary_has_semantics and planning_recovery_has_semantics:
        boundary_family = "boundary_precedes_first_semantics_then_restores"
    elif planning_prev_has_semantics and not planning_boundary_has_semantics:
        boundary_family = "boundary_breaks_existing_semantics_without_restore"
    else:
        boundary_family = "boundary_missing_semantics"

    return {
        "label": label,
        "run_dir": str(run_dir),
        "route_id": summary.get("route_id"),
        "route_health_label": summary.get("route_health_label"),
        "route_completion_ratio": safe_float(summary.get("route_completion_ratio")),
        "route_distance_achieved_m": safe_float(summary.get("route_distance_achieved_m")),
        "path_fallback_count": safe_int(summary.get("path_fallback_count")),
        "long_phase_to_boundary_sec": _delta(boundary_row, long_phase_ts),
        "planning_prev_dt_sec": _delta(boundary_prev_row, boundary_ts),
        "planning_boundary_dt_sec": _delta(boundary_row, boundary_ts),
        "planning_recovery_dt_sec": _delta(planning_recovery_row, boundary_ts),
        "planning_prev_summary": _row_summary(boundary_prev_row),
        "planning_boundary_summary": _row_summary(boundary_row),
        "planning_recovery_summary": _row_summary(planning_recovery_row),
        "nonplanning_prev_status_family": " / ".join(sorted(nonplanning_prev_status_values)),
        "nonplanning_boundary_status_family": " / ".join(sorted(nonplanning_boundary_status_values)),
        "nonplanning_recovery_status_family": " / ".join(sorted(nonplanning_recovery_status_values)),
        "nonplanning_prev_road_family": "/".join(sorted(nonplanning_prev_road_values)) if nonplanning_prev_road_values else "none",
        "nonplanning_boundary_road_family": "/".join(sorted(nonplanning_boundary_road_values)) if nonplanning_boundary_road_values else "none",
        "nonplanning_recovery_road_family": "/".join(sorted(nonplanning_recovery_road_values)) if nonplanning_recovery_road_values else "none",
        "nonplanning_prev_aligned": len(nonplanning_prev_status_values) == 1,
        "nonplanning_boundary_aligned": len(nonplanning_boundary_status_values) == 1,
        "nonplanning_recovery_aligned": len(nonplanning_recovery_status_values) == 1,
        "boundary_family": boundary_family,
        "family_transition": family_transition,
        "stage5_prev_summary": _row_summary(phase_rows["stage5"]["prev"]),
        "stage5_boundary_summary": _row_summary(phase_rows["stage5"]["boundary"]),
        "stage5_recovery_summary": _row_summary(phase_rows["stage5"]["recovery"]),
        "apollo_route_prev_summary": _row_summary(phase_rows["apollo_route"]["prev"]),
        "apollo_route_boundary_summary": _row_summary(phase_rows["apollo_route"]["boundary"]),
        "apollo_route_recovery_summary": _row_summary(phase_rows["apollo_route"]["recovery"]),
        "apollo_ref_prev_summary": _row_summary(phase_rows["apollo_ref"]["prev"]),
        "apollo_ref_boundary_summary": _row_summary(phase_rows["apollo_ref"]["boundary"]),
        "apollo_ref_recovery_summary": _row_summary(phase_rows["apollo_ref"]["recovery"]),
        "lane_follow_prev_summary": _row_summary(phase_rows["lane_follow"]["prev"]),
        "lane_follow_boundary_summary": _row_summary(phase_rows["lane_follow"]["boundary"]),
        "lane_follow_recovery_summary": _row_summary(phase_rows["lane_follow"]["recovery"]),
    }


def _csv_rows(snapshots: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(snapshot) for snapshot in snapshots]


def _render_report(snapshots: Sequence[Dict[str, Any]], generated_at_local: str) -> str:
    lines: List[str] = [
        "# Town01 Junction Road22 Boundary Family Compare",
        "",
        f"- generated_at_local: `{generated_at_local}`",
        "",
        "## Scope",
        "",
    ]
    for snapshot in snapshots:
        lines.append(f"- `{snapshot['label']}`: `{snapshot['route_id']}`")
        lines.append(f"  - run_dir: `{snapshot['run_dir']}`")

    lines.extend(
        [
            "",
            "## Boundary Table",
            "",
            "| sample | route | completion | distance_m | fallback | long_phase_to_boundary_sec | planning_prev_dt_sec | planning_recovery_dt_sec | boundary_family | family_transition | nonplanning_prev_status | nonplanning_boundary_status | nonplanning_recovery_status | nonplanning_prev_road | nonplanning_boundary_road | nonplanning_recovery_road | prev_aligned | boundary_aligned | recovery_aligned |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for snapshot in snapshots:
        lines.append(
            "| "
            + " | ".join(
                [
                    snapshot["label"],
                    str(snapshot["route_id"]),
                    _fmt_float(snapshot.get("route_completion_ratio"), 6),
                    _fmt_float(snapshot.get("route_distance_achieved_m"), 3),
                    str(snapshot.get("path_fallback_count") or ""),
                    _fmt_float(snapshot.get("long_phase_to_boundary_sec"), 3),
                    _fmt_float(snapshot.get("planning_prev_dt_sec"), 3),
                    _fmt_float(snapshot.get("planning_recovery_dt_sec"), 3),
                    str(snapshot.get("boundary_family") or ""),
                    str(snapshot.get("family_transition") or ""),
                    str(snapshot.get("nonplanning_prev_status_family") or ""),
                    str(snapshot.get("nonplanning_boundary_status_family") or ""),
                    str(snapshot.get("nonplanning_recovery_status_family") or ""),
                    str(snapshot.get("nonplanning_prev_road_family") or ""),
                    str(snapshot.get("nonplanning_boundary_road_family") or ""),
                    str(snapshot.get("nonplanning_recovery_road_family") or ""),
                    _bool_text(snapshot.get("nonplanning_prev_aligned")),
                    _bool_text(snapshot.get("nonplanning_boundary_aligned")),
                    _bool_text(snapshot.get("nonplanning_recovery_aligned")),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Key Findings",
            "",
            "- This compare focuses on the exact moment the first `22-road` boundary family forms.",
            "  It compares the planning row before the boundary, the boundary row itself, and the first recovered row after the boundary.",
            "- `181066` does not undergo a family flip at the boundary.",
            "  Its planning row is already road `11` before the boundary, the nonplanning surfaces are already aligned on `trajectory_nonzero_debug_missing`,",
            "  and the boundary itself only expands preview depth while preserving road-`11` semantics.",
            "- `219062` does undergo a real boundary-time family flip.",
            "  Before the boundary it still has road-`8` semantics and the nonplanning surfaces are aligned on `trajectory_nonzero_debug_missing`.",
            "  At the boundary those same nonplanning surfaces synchronously flip to `failed / route_segments_present_reference_line_missing / reference_line_missing`,",
            "  and all visible lane semantics disappear together.",
            "  The first recovered row then returns to the earlier `trajectory_nonzero_debug_missing` family, but only as road `8`.",
            "- `219063` is different from `219062` only in what exists before the flip, not in the flip family itself.",
            "  Its planning pre-boundary row is still road `8`, but there is no post-long-phase semantic nonplanning row immediately before the boundary.",
            "  The boundary row is therefore the first visible nonplanning family after long phase, and it is already the same `failed` blank-semantics family as `219062`.",
            "  The first recovered row again returns only to `trajectory_nonzero_debug_missing` on road `8`.",
            "- So the current Town01 blocker is now sharper than a generic `road22 blank boundary` description:",
            "  the key distinction is not only blank-vs-resolved at the boundary,",
            "  but whether the route preserves an already-formed `trajectory_nonzero_debug_missing` lane-semantic family into the deep-preview expansion (`181066`),",
            "  or flips that family into a synchronized `failed / reference_line_missing` blank family at the exact first `22-road` row (`219062/219063`).",
            "",
            "## Per-Run Read",
            "",
        ]
    )
    for snapshot in snapshots:
        lines.extend(
            [
                f"### `{snapshot['label']}` / `{snapshot['route_id']}`",
                "",
                f"- completion / distance / fallback: "
                f"`{snapshot.get('route_completion_ratio')}` / `{snapshot.get('route_distance_achieved_m')}` / `{snapshot.get('path_fallback_count')}`",
                f"- boundary family: `{snapshot.get('boundary_family')}`",
                f"- family transition: `{snapshot.get('family_transition')}`",
                f"- planning phase rows:",
                f"  - prev: `{snapshot.get('planning_prev_summary')}`",
                f"  - boundary: `{snapshot.get('planning_boundary_summary')}`",
                f"  - recovery: `{snapshot.get('planning_recovery_summary')}`",
                f"- stage5 phase rows:",
                f"  - prev: `{snapshot.get('stage5_prev_summary')}`",
                f"  - boundary: `{snapshot.get('stage5_boundary_summary')}`",
                f"  - recovery: `{snapshot.get('stage5_recovery_summary')}`",
                f"- apollo_route phase rows:",
                f"  - prev: `{snapshot.get('apollo_route_prev_summary')}`",
                f"  - boundary: `{snapshot.get('apollo_route_boundary_summary')}`",
                f"  - recovery: `{snapshot.get('apollo_route_recovery_summary')}`",
                f"- apollo_ref phase rows:",
                f"  - prev: `{snapshot.get('apollo_ref_prev_summary')}`",
                f"  - boundary: `{snapshot.get('apollo_ref_boundary_summary')}`",
                f"  - recovery: `{snapshot.get('apollo_ref_recovery_summary')}`",
                f"- lane_follow phase rows:",
                f"  - prev: `{snapshot.get('lane_follow_prev_summary')}`",
                f"  - boundary: `{snapshot.get('lane_follow_boundary_summary')}`",
                f"  - recovery: `{snapshot.get('lane_follow_recovery_summary')}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare road22 boundary family formation for Town01 junction runs.")
    parser.add_argument(
        "--case",
        action="append",
        required=True,
        help="Case in label=/abs/path/to/run_dir form. Can be passed multiple times.",
    )
    parser.add_argument("--report", required=True, help="Markdown report output path.")
    parser.add_argument("--summary-csv", required=True, help="CSV summary output path.")
    return parser.parse_args()


def _parse_case(case_arg: str) -> Tuple[str, Path]:
    if "=" not in case_arg:
        raise SystemExit(f"invalid --case {case_arg!r}; expected label=/abs/path")
    label, path_text = case_arg.split("=", 1)
    label = label.strip()
    run_dir = Path(path_text.strip()).resolve()
    if not label:
        raise SystemExit(f"invalid empty case label in {case_arg!r}")
    if not run_dir.exists():
        raise SystemExit(f"run dir does not exist: {run_dir}")
    return label, run_dir


def main() -> None:
    args = _parse_args()
    snapshots = [_build_snapshot(label, run_dir) for label, run_dir in (_parse_case(case_arg) for case_arg in args.case)]
    generated_at_local = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    report_path = Path(args.report).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(_render_report(snapshots, generated_at_local), encoding="utf-8")

    csv_path = Path(args.summary_csv).resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows = _csv_rows(snapshots)
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


if __name__ == "__main__":
    main()
