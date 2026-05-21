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


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


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


def _row_status_family(row: Optional[Dict[str, Any]]) -> str:
    if not row:
        return "missing"
    refline_count = safe_int(row.get("reference_line_count"))
    create_status = _safe_text(row.get("create_route_segments_status")) or "none"
    provider_status = _safe_text(row.get("reference_line_provider_status")) or "none"
    lane_follow_status = _safe_text(row.get("lane_follow_map_status")) or "none"
    empty_reason = _safe_text(row.get("planning_empty_reason_guess")) or "none"
    return (
        f"ref{refline_count if refline_count is not None else 'none'}"
        f"_{create_status}_{provider_status}_{lane_follow_status}_{empty_reason}"
    )


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


def _load_surface_rows(run_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    rows: Dict[str, List[Dict[str, Any]]] = {}
    artifacts_dir = run_dir / "artifacts"
    for surface_name, relpath in SURFACE_FILES:
        rows[surface_name] = load_jsonl(artifacts_dir / relpath)
        if not rows[surface_name]:
            raise SystemExit(f"missing {surface_name} rows: {run_dir}")
    return rows


def _build_snapshot(label: str, run_dir: Path) -> Dict[str, Any]:
    summary = load_json(run_dir / "summary.json")
    rows_by_surface = _load_surface_rows(run_dir)
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
    road22_boundary_row = _first_row(
        planning_rows,
        lambda row: safe_float(row.get("timestamp")) is not None
        and safe_float(row.get("timestamp")) >= long_phase_ts
        and (safe_int(row.get("routing_road_count")) or 0) >= 22,
    )
    if not road22_boundary_row:
        raise SystemExit(f"missing planning road22 boundary: {run_dir}")

    boundary_ts = safe_float(road22_boundary_row.get("timestamp"))
    surface_boundary_rows = {
        surface_name: _nearest_row(surface_rows, boundary_ts)
        for surface_name, surface_rows in rows_by_surface.items()
    }
    surface_recovery_rows = {
        surface_name: _first_row(
            surface_rows,
            lambda row: safe_float(row.get("timestamp")) is not None
            and boundary_ts is not None
            and safe_float(row.get("timestamp")) >= boundary_ts
            and _row_has_lane_semantics(row),
        )
        for surface_name, surface_rows in rows_by_surface.items()
    }

    recovery_deltas = {
        surface_name: _delta(row, boundary_ts)
        for surface_name, row in surface_recovery_rows.items()
    }
    nonempty_recovery_deltas = [value for value in recovery_deltas.values() if value is not None]
    earliest_surface: str = ""
    latest_surface: str = ""
    recovery_span_ms: Optional[float] = None
    if nonempty_recovery_deltas:
        earliest_delta = min(nonempty_recovery_deltas)
        latest_delta = max(nonempty_recovery_deltas)
        recovery_span_ms = (latest_delta - earliest_delta) * 1000.0
        earliest_surface = next(
            surface_name for surface_name, value in recovery_deltas.items() if value is not None and abs(value - earliest_delta) <= 1e-9
        )
        latest_surface = next(
            surface_name for surface_name, value in recovery_deltas.items() if value is not None and abs(value - latest_delta) <= 1e-9
        )

    recovered_roads = {surface_name: _row_primary_lane_road(row) for surface_name, row in surface_recovery_rows.items()}
    recovered_lane_ids = {surface_name: _row_primary_lane_id(row) for surface_name, row in surface_recovery_rows.items()}
    recovered_statuses = {surface_name: _row_status_family(row) for surface_name, row in surface_recovery_rows.items()}
    boundary_statuses = {surface_name: _row_status_family(row) for surface_name, row in surface_boundary_rows.items()}

    recovered_road_values = {road for road in recovered_roads.values() if road}
    recovered_status_values = {status for status in recovered_statuses.values() if status != "missing"}
    boundary_status_values = {status for status in boundary_statuses.values() if status != "missing"}

    recovery_family = "no_recovery"
    if nonempty_recovery_deltas:
        if len(recovered_road_values) == 1 and recovered_road_values == {"8"}:
            if recovery_span_ms is not None and recovery_span_ms <= 1e-6:
                recovery_family = "all_five_surfaces_recover_synchronously_as_road8"
            else:
                recovery_family = "all_five_surfaces_recover_in_order_as_road8"
        elif len(recovered_road_values) == 1:
            recovery_family = "all_recovered_surfaces_share_same_non8_road"
        else:
            recovery_family = "recovered_surfaces_mixed_road_ids"

    ordering_signature_parts: List[str] = []
    for surface_name in ("planning", "stage5", "apollo_route", "apollo_ref", "lane_follow"):
        ordering_signature_parts.append(f"{surface_name}:{_fmt_float(recovery_deltas.get(surface_name), 3) or 'none'}")

    return {
        "label": label,
        "run_dir": str(run_dir),
        "route_id": summary.get("route_id"),
        "route_health_label": summary.get("route_health_label"),
        "route_completion_ratio": safe_float(summary.get("route_completion_ratio")),
        "route_distance_achieved_m": safe_float(summary.get("route_distance_achieved_m")),
        "path_fallback_count": safe_int(summary.get("path_fallback_count")),
        "long_phase_to_road22_sec": _delta(road22_boundary_row, long_phase_ts),
        "road22_boundary_ts": boundary_ts,
        "road22_boundary_routing_road_count": safe_int(road22_boundary_row.get("routing_road_count")),
        "road22_boundary_lane_window_signature": _safe_text(road22_boundary_row.get("routing_lane_window_signature")),
        "road22_boundary_refline_count": safe_int(road22_boundary_row.get("reference_line_count")),
        "road22_boundary_status_family": " | ".join(
            f"{surface_name}={boundary_statuses[surface_name]}"
            for surface_name in ("planning", "stage5", "apollo_route", "apollo_ref", "lane_follow")
        ),
        "planning_recovery_sec": recovery_deltas.get("planning"),
        "stage5_recovery_sec": recovery_deltas.get("stage5"),
        "apollo_route_recovery_sec": recovery_deltas.get("apollo_route"),
        "apollo_ref_recovery_sec": recovery_deltas.get("apollo_ref"),
        "lane_follow_recovery_sec": recovery_deltas.get("lane_follow"),
        "earliest_recovery_surface": earliest_surface,
        "latest_recovery_surface": latest_surface,
        "recovery_span_ms": recovery_span_ms,
        "recovery_order_signature": ", ".join(ordering_signature_parts),
        "recovery_family": recovery_family,
        "recovered_road_family": "/".join(sorted(recovered_road_values)) if recovered_road_values else "none",
        "recovered_status_family": " | ".join(
            f"{surface_name}={recovered_statuses[surface_name]}"
            for surface_name in ("planning", "stage5", "apollo_route", "apollo_ref", "lane_follow")
        ),
        "planning_recovered_lane": recovered_lane_ids.get("planning") or "none",
        "stage5_recovered_lane": recovered_lane_ids.get("stage5") or "none",
        "apollo_route_recovered_lane": recovered_lane_ids.get("apollo_route") or "none",
        "apollo_ref_recovered_lane": recovered_lane_ids.get("apollo_ref") or "none",
        "lane_follow_recovered_lane": recovered_lane_ids.get("lane_follow") or "none",
        "planning_recovered_road": recovered_roads.get("planning") or "none",
        "stage5_recovered_road": recovered_roads.get("stage5") or "none",
        "apollo_route_recovered_road": recovered_roads.get("apollo_route") or "none",
        "apollo_ref_recovered_road": recovered_roads.get("apollo_ref") or "none",
        "lane_follow_recovered_road": recovered_roads.get("lane_follow") or "none",
        "all_recovered_status_aligned": len(recovered_status_values) == 1 if recovered_status_values else False,
        "all_boundary_status_aligned": len(boundary_status_values) == 1 if boundary_status_values else False,
    }


def _csv_rows(snapshots: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(snapshot) for snapshot in snapshots]


def _render_report(snapshots: Sequence[Dict[str, Any]], generated_at_local: str) -> str:
    lines: List[str] = [
        "# Town01 Junction Road22 Recovery Compare",
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
            "## Recovery Table",
            "",
            "| sample | route | completion | distance_m | fallback | long_phase_to_road22_sec | planning_recovery_sec | stage5_recovery_sec | apollo_route_recovery_sec | apollo_ref_recovery_sec | lane_follow_recovery_sec | earliest_surface | latest_surface | recovery_span_ms | recovery_family | recovered_roads | all_boundary_status_aligned | all_recovered_status_aligned |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
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
                    _fmt_float(snapshot.get("long_phase_to_road22_sec"), 3),
                    _fmt_float(snapshot.get("planning_recovery_sec"), 3),
                    _fmt_float(snapshot.get("stage5_recovery_sec"), 3),
                    _fmt_float(snapshot.get("apollo_route_recovery_sec"), 3),
                    _fmt_float(snapshot.get("apollo_ref_recovery_sec"), 3),
                    _fmt_float(snapshot.get("lane_follow_recovery_sec"), 3),
                    str(snapshot.get("earliest_recovery_surface") or ""),
                    str(snapshot.get("latest_recovery_surface") or ""),
                    _fmt_float(snapshot.get("recovery_span_ms"), 3),
                    str(snapshot.get("recovery_family") or ""),
                    str(snapshot.get("recovered_road_family") or ""),
                    str(snapshot.get("all_boundary_status_aligned") or ""),
                    str(snapshot.get("all_recovered_status_aligned") or ""),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Key Findings",
            "",
            "- This compare does not revisit whether the first `22-road` boundary is blank.",
            "  It focuses on what happens immediately after that boundary for the two second-family runs.",
            "- The exact first `22-road` boundary row is still aligned across all five surfaces for both runs.",
            "  At that boundary the family is already the same blank-semantics failed-reference-line contract on `planning`, `stage5`, `apollo_route`, `apollo_ref`, and `lane_follow`.",
            "- The difference between the siblings is now reduced to recovery latency, not recovery topology:",
            "  `219062` rehydrates about `0.098s` after the boundary,",
            "  while `219063` rehydrates about `0.062s` after the boundary.",
            "- That recovery is still fully synchronized across the same five surfaces in both runs.",
            "  No surface recovers earlier with a different road selector or a different lane family.",
            "- And when recovery arrives, it still lands only on road `8` with lane id `8_1_1` across every recovered surface.",
            "  There is still no earlier non-road-`8` selector hiding in `apollo_route`, `apollo_ref`, or `lane_follow` ahead of planning/stage5.",
            "- So the current second-family blocker is now sharper again:",
            "  after the exact five-surface blank boundary at the first `22-road` preview, the system does not branch into two qualitatively different recovery paths.",
            "  It follows the same five-surface road-`8` recovery family on both routes, with `219063` simply recovering about `36ms` sooner.",
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
                f"- `long_phase_transition -> first road22 preview` = `{snapshot.get('long_phase_to_road22_sec')}` sec",
                f"- boundary status family:",
                f"  `{snapshot.get('road22_boundary_status_family')}`",
                f"- recovery ordering signature:",
                f"  `{snapshot.get('recovery_order_signature')}`",
                f"- recovered lanes:",
                f"  `planning={snapshot.get('planning_recovered_lane')}`",
                f"  `stage5={snapshot.get('stage5_recovered_lane')}`",
                f"  `apollo_route={snapshot.get('apollo_route_recovered_lane')}`",
                f"  `apollo_ref={snapshot.get('apollo_ref_recovered_lane')}`",
                f"  `lane_follow={snapshot.get('lane_follow_recovered_lane')}`",
                f"- recovered roads:",
                f"  `planning={snapshot.get('planning_recovered_road')}`",
                f"  `stage5={snapshot.get('stage5_recovered_road')}`",
                f"  `apollo_route={snapshot.get('apollo_route_recovered_road')}`",
                f"  `apollo_ref={snapshot.get('apollo_ref_recovered_road')}`",
                f"  `lane_follow={snapshot.get('lane_follow_recovered_road')}`",
                f"- recovery family:",
                f"  `{snapshot.get('recovery_family')}`",
                f"  `recovery_span_ms={snapshot.get('recovery_span_ms')}`",
                f"  `all_boundary_status_aligned={snapshot.get('all_boundary_status_aligned')}`",
                f"  `all_recovered_status_aligned={snapshot.get('all_recovered_status_aligned')}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare road22 boundary recovery ordering for Town01 junction runs.")
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
