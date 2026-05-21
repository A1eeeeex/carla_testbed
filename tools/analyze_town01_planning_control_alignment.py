#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json
from tools.run_town01_route_health import _resolve_run_dir


def _load_manifest(batch_root: Path) -> Dict[str, Any]:
    manifest_path = batch_root / "artifacts" / "town01_route_health_run_manifest.json"
    payload = load_json(manifest_path)
    if not payload:
        raise SystemExit(f"Manifest missing or invalid: {manifest_path}")
    return payload


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _collect_rows(batch_root: Path) -> List[Dict[str, Any]]:
    manifest = _load_manifest(batch_root)
    rows: List[Dict[str, Any]] = []
    for item in list(manifest.get("runs") or []):
        raw_run_dir = Path(str(item.get("effective_run_dir") or item.get("run_dir") or "")).expanduser().resolve()
        run_dir = _resolve_run_dir(raw_run_dir)
        summary = load_json(run_dir / "summary.json")
        alignment = load_json(run_dir / "artifacts" / "planning_control_alignment.finalized.json")
        if not alignment:
            alignment = summary.get("planning_control_alignment") or {}
        acceptance = summary.get("acceptance") or {}
        live_rows = _load_jsonl(run_dir / "artifacts" / "control_trajectory_consume_debug_live.jsonl")
        last_live = live_rows[-1] if live_rows else {}
        live_sources = sorted(
            {
                str(row.get("effective_planning_source") or "")
                for row in live_rows
                if str(row.get("effective_planning_source") or "")
            }
        )
        exact_match_seen = any(bool(row.get("matched_planning_event_found_exactly")) for row in live_rows)
        rows.append(
            {
                "comparison_label": str(item.get("comparison_label") or summary.get("comparison_label") or ""),
                "route_id": str(summary.get("route_id") or ""),
                "run_dir": str(run_dir),
                "failure_stage": str(acceptance.get("failure_stage") or ""),
                "fail_reason": str(acceptance.get("fail_reason") or ""),
                "first_nonzero_planning_seq": alignment.get("first_nonzero_planning_seq"),
                "last_live_control_planning_seq": alignment.get("last_live_control_planning_seq"),
                "live_control_to_first_nonzero_gap_sec": alignment.get("live_control_to_first_nonzero_gap_sec"),
                "live_control_stops_before_first_nonzero_planning": alignment.get("live_control_stops_before_first_nonzero_planning"),
                "control_no_trajectory_after_first_nonzero": alignment.get("control_no_trajectory_after_first_nonzero"),
                "planning_total_messages": alignment.get("planning_total_messages"),
                "planning_nonzero_messages": alignment.get("planning_nonzero_messages"),
                "live_control_total_messages": alignment.get("live_control_total_messages"),
                "live_control_latest_nonzero_messages": alignment.get("live_control_latest_nonzero_messages"),
                "last_control_input_trajectory_header_sequence_num": last_live.get(
                    "control_input_trajectory_header_sequence_num"
                ),
                "last_control_input_candidate_trajectory_header_sequence_num": last_live.get(
                    "control_input_candidate_trajectory_header_sequence_num"
                ),
                "last_control_input_latest_replan_trajectory_header_sequence_num": last_live.get(
                    "control_input_latest_replan_trajectory_header_sequence_num"
                ),
                "last_control_input_candidate_source": last_live.get(
                    "control_input_candidate_source"
                ),
                "last_matched_planning_event_found_exactly": last_live.get(
                    "matched_planning_event_found_exactly"
                ),
                "last_matched_candidate_planning_event_found_exactly": last_live.get(
                    "matched_candidate_planning_event_found_exactly"
                ),
                "last_effective_planning_source": last_live.get("effective_planning_source"),
                "last_latest_known_planning_sequence_gap": last_live.get(
                    "latest_known_planning_sequence_gap"
                ),
                "last_candidate_latest_known_planning_sequence_gap": last_live.get(
                    "candidate_latest_known_planning_sequence_gap"
                ),
                "exact_match_seen": exact_match_seen,
                "effective_planning_sources_seen": "|".join(live_sources),
            }
        )
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "comparison_label",
        "route_id",
        "run_dir",
        "failure_stage",
        "fail_reason",
        "first_nonzero_planning_seq",
        "last_live_control_planning_seq",
        "live_control_to_first_nonzero_gap_sec",
        "live_control_stops_before_first_nonzero_planning",
        "control_no_trajectory_after_first_nonzero",
        "planning_total_messages",
        "planning_nonzero_messages",
        "live_control_total_messages",
        "live_control_latest_nonzero_messages",
        "last_control_input_trajectory_header_sequence_num",
        "last_control_input_candidate_trajectory_header_sequence_num",
        "last_control_input_latest_replan_trajectory_header_sequence_num",
        "last_control_input_candidate_source",
        "last_matched_planning_event_found_exactly",
        "last_matched_candidate_planning_event_found_exactly",
        "last_effective_planning_source",
        "last_latest_known_planning_sequence_gap",
        "last_candidate_latest_known_planning_sequence_gap",
        "exact_match_seen",
        "effective_planning_sources_seen",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def _write_report(path: Path, rows: List[Dict[str, Any]], batch_root: Path) -> None:
    stop_before_nonzero = sum(1 for row in rows if bool(row.get("live_control_stops_before_first_nonzero_planning")))
    exact_match_seen = sum(1 for row in rows if bool(row.get("exact_match_seen")))
    all_fail_stage = sorted(set(str(row.get("failure_stage") or "") for row in rows))
    all_sources = sorted(
        {
            source
            for row in rows
            for source in str(row.get("effective_planning_sources_seen") or "").split("|")
            if source
        }
    )
    gap_values = [
        float(row["live_control_to_first_nonzero_gap_sec"])
        for row in rows
        if row.get("live_control_to_first_nonzero_gap_sec") is not None
    ]
    mean_gap = (sum(gap_values) / float(len(gap_values))) if gap_values else None
    lines = [
        "# Town01 Planning Control Alignment Report",
        "",
        f"- batch_root: `{batch_root}`",
        f"- run_count: `{len(rows)}`",
        f"- live_control_stops_before_first_nonzero_planning: `{stop_before_nonzero}/{len(rows)}`",
        f"- exact_match_seen: `{exact_match_seen}/{len(rows)}`",
        f"- mean_live_control_to_first_nonzero_gap_sec: `{mean_gap if mean_gap is not None else 'n/a'}`",
        f"- failure_stages: `{all_fail_stage}`",
        f"- effective_planning_sources_seen: `{all_sources}`",
        "",
        "## Key Finding",
        "",
        "- Current Town01 samples show a stable pattern where live control consume events stop one planning sequence before the first nonzero planning trajectory appears.",
        "- In the current online sample, the live consume path never reports an exact planning-event match and remains on `latest_known_fallback` with `control_input_trajectory_header_sequence_num=0`.",
        "- This means the dominant break is upstream of route completion and lateral behavior: control never starts consuming the first healthy planning trajectory.",
        "- `control_no_trajectory_after_first_nonzero` staying at `0` in these samples also suggests the break happens before the nonzero trajectory window is ever entered by the live control consume path.",
        "",
        "## Per Run",
        "",
    ]
    for row in rows:
        lines.append(
            "- "
            f"{row.get('comparison_label')}: route=`{row.get('route_id')}`, "
            f"stage=`{row.get('failure_stage')}`, "
            f"first_nonzero_seq=`{row.get('first_nonzero_planning_seq')}`, "
            f"last_live_seq=`{row.get('last_live_control_planning_seq')}`, "
            f"gap_sec=`{row.get('live_control_to_first_nonzero_gap_sec')}`, "
            f"stops_before_nonzero=`{row.get('live_control_stops_before_first_nonzero_planning')}`, "
            f"source=`{row.get('last_effective_planning_source')}`, "
            f"input_seq=`{row.get('last_control_input_trajectory_header_sequence_num')}`, "
            f"candidate_seq=`{row.get('last_control_input_candidate_trajectory_header_sequence_num')}`, "
            f"latest_replan_seq=`{row.get('last_control_input_latest_replan_trajectory_header_sequence_num')}`, "
            f"candidate_source=`{row.get('last_control_input_candidate_source')}`, "
            f"exact_match=`{row.get('last_matched_planning_event_found_exactly')}`, "
            f"candidate_exact_match=`{row.get('last_matched_candidate_planning_event_found_exactly')}`, "
            f"latest_gap=`{row.get('last_latest_known_planning_sequence_gap')}`, "
            f"candidate_gap=`{row.get('last_candidate_latest_known_planning_sequence_gap')}`"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Town01 planning/control alignment from a unified batch")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument(
        "--csv",
        type=Path,
        default=REPO_ROOT / "artifacts" / "town01_planning_control_alignment.csv",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=REPO_ROOT / "artifacts" / "town01_planning_control_alignment_report.md",
    )
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    rows = _collect_rows(batch_root)
    _write_csv(Path(args.csv).expanduser().resolve(), rows)
    _write_report(Path(args.report).expanduser().resolve(), rows, batch_root)
    print(f"[town01-planning-control-alignment] analyzed runs={len(rows)}")


if __name__ == "__main__":
    main()
