#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json
from tools.run_town01_route_health import _resolve_run_dir


_SEQ_RE = re.compile(r"planning_seq_num:(\d+)")


def _load_manifest(batch_root: Path) -> Dict[str, Any]:
    path = batch_root / "artifacts" / "town01_route_health_run_manifest.json"
    payload = load_json(path)
    if not payload:
        raise SystemExit(f"Manifest missing or invalid: {path}")
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


def _control_log_seq_stats(path: Path) -> Dict[str, Any]:
    seqs: List[int] = []
    if not path.exists():
        return {"count": 0, "last": None, "max": None}
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = _SEQ_RE.search(line)
        if not match:
            continue
        try:
            seqs.append(int(match.group(1)))
        except Exception:
            continue
    return {
        "count": len(seqs),
        "last": seqs[-1] if seqs else None,
        "max": max(seqs) if seqs else None,
    }


def _collect_rows(batch_root: Path) -> List[Dict[str, Any]]:
    manifest = _load_manifest(batch_root)
    rows: List[Dict[str, Any]] = []
    for item in list(manifest.get("runs") or []):
        raw_run_dir = Path(str(item.get("effective_run_dir") or item.get("run_dir") or "")).expanduser().resolve()
        run_dir = _resolve_run_dir(raw_run_dir)
        artifacts = run_dir / "artifacts"
        planning_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")
        raw_rows = _load_jsonl(artifacts / "apollo_control_raw.jsonl")
        live_rows = _load_jsonl(artifacts / "control_trajectory_consume_debug_live.jsonl")
        if not planning_rows or not raw_rows:
            continue
        first_nonzero = next(
            (row for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) > 0),
            None,
        )
        if first_nonzero is None:
            continue
        first_nonzero_ts = float(first_nonzero["timestamp"])
        first_nonzero_seq = int(first_nonzero["planning_header_sequence_num"])
        raw_after = [row for row in raw_rows if float(row.get("ts_sec") or 0.0) >= first_nonzero_ts]
        live_after = [row for row in live_rows if float(row.get("timestamp") or 0.0) >= first_nonzero_ts]
        last_raw = raw_rows[-1]
        last_raw_fields = last_raw.get("apollo_control_raw") or {}
        last_live = live_rows[-1] if live_rows else {}
        log_stats = _control_log_seq_stats(artifacts / "apollo_control.INFO")
        rows.append(
            {
                "comparison_label": str(item.get("comparison_label") or ""),
                "route_id": str(load_json(run_dir / "summary.json").get("route_id") or ""),
                "run_dir": str(run_dir),
                "first_nonzero_planning_seq": first_nonzero_seq,
                "first_nonzero_planning_ts": first_nonzero_ts,
                "raw_control_message_count": len(raw_rows),
                "raw_control_after_first_nonzero_count": len(raw_after),
                "live_control_after_first_nonzero_count": len(live_after),
                "last_raw_ts": last_raw.get("ts_sec"),
                "gap_first_nonzero_minus_last_raw_sec": float(first_nonzero_ts - float(last_raw.get("ts_sec") or 0.0)),
                "last_control_header_seq": last_raw_fields.get("control_header_sequence_num"),
                "last_debug_input_trajectory_header_sequence_num": last_raw_fields.get("debug_input_trajectory_header_sequence_num"),
                "last_debug_input_latest_replan_trajectory_header_sequence_num": last_raw_fields.get(
                    "debug_input_latest_replan_trajectory_header_sequence_num"
                ),
                "last_live_effective_planning_source": last_live.get("effective_planning_source"),
                "last_live_control_input_trajectory_header_sequence_num": last_live.get(
                    "control_input_trajectory_header_sequence_num"
                ),
                "last_live_control_input_candidate_trajectory_header_sequence_num": last_live.get(
                    "control_input_candidate_trajectory_header_sequence_num"
                ),
                "last_live_control_input_latest_replan_trajectory_header_sequence_num": last_live.get(
                    "control_input_latest_replan_trajectory_header_sequence_num"
                ),
                "last_live_control_input_candidate_source": last_live.get(
                    "control_input_candidate_source"
                ),
                "last_live_matched_planning_event_found_exactly": last_live.get(
                    "matched_planning_event_found_exactly"
                ),
                "last_live_matched_candidate_planning_event_found_exactly": last_live.get(
                    "matched_candidate_planning_event_found_exactly"
                ),
                "last_live_latest_known_planning_sequence_gap": last_live.get(
                    "latest_known_planning_sequence_gap"
                ),
                "last_live_candidate_latest_known_planning_sequence_gap": last_live.get(
                    "candidate_latest_known_planning_sequence_gap"
                ),
                "control_log_planning_seq_count": log_stats["count"],
                "control_log_last_planning_seq": log_stats["last"],
                "control_log_max_planning_seq": log_stats["max"],
            }
        )
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "comparison_label",
        "route_id",
        "run_dir",
        "first_nonzero_planning_seq",
        "first_nonzero_planning_ts",
        "raw_control_message_count",
        "raw_control_after_first_nonzero_count",
        "live_control_after_first_nonzero_count",
        "last_raw_ts",
        "gap_first_nonzero_minus_last_raw_sec",
        "last_control_header_seq",
        "last_debug_input_trajectory_header_sequence_num",
        "last_debug_input_latest_replan_trajectory_header_sequence_num",
        "last_live_effective_planning_source",
        "last_live_control_input_trajectory_header_sequence_num",
        "last_live_control_input_candidate_trajectory_header_sequence_num",
        "last_live_control_input_latest_replan_trajectory_header_sequence_num",
        "last_live_control_input_candidate_source",
        "last_live_matched_planning_event_found_exactly",
        "last_live_matched_candidate_planning_event_found_exactly",
        "last_live_latest_known_planning_sequence_gap",
        "last_live_candidate_latest_known_planning_sequence_gap",
        "control_log_planning_seq_count",
        "control_log_last_planning_seq",
        "control_log_max_planning_seq",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def _write_report(path: Path, rows: List[Dict[str, Any]], batch_root: Path) -> None:
    zero_input_seq = sum(
        1
        for row in rows
        if int(row.get("last_debug_input_trajectory_header_sequence_num") or 0) == 0
        and int(row.get("last_debug_input_latest_replan_trajectory_header_sequence_num") or 0) == 0
    )
    raw_stops = sum(1 for row in rows if int(row.get("raw_control_after_first_nonzero_count") or 0) == 0)
    live_stops = sum(1 for row in rows if int(row.get("live_control_after_first_nonzero_count") or 0) == 0)
    gaps = [float(row["gap_first_nonzero_minus_last_raw_sec"]) for row in rows]
    mean_gap = (sum(gaps) / float(len(gaps))) if gaps else None
    lines = [
        "# Town01 Control Input Sequence Report",
        "",
        f"- batch_root: `{batch_root}`",
        f"- run_count: `{len(rows)}`",
        f"- raw_control_stops_before_first_nonzero_planning: `{raw_stops}/{len(rows)}`",
        f"- live_control_stops_before_first_nonzero_planning: `{live_stops}/{len(rows)}`",
        f"- last_input_debug_seq_zero: `{zero_input_seq}/{len(rows)}`",
        f"- mean_gap_first_nonzero_minus_last_raw_sec: `{mean_gap if mean_gap is not None else 'n/a'}`",
        "",
        "## Key Finding",
        "",
        "- In current offline Town01 samples, `apollo_control_raw.jsonl` stops before the first nonzero planning trajectory arrives.",
        "- In the current online Town01 sample, the live consume stream also stops before the first nonzero planning trajectory, with `effective_planning_source=latest_known_fallback`, `control_input_trajectory_header_sequence_num=0`, and no nonzero candidate sequence from either primary or latest-replan input headers.",
        "- At the same time, `apollo_control.INFO` continues reporting `planning_seq_num` progression while control raw `input_debug.trajectory_header.sequence_num` stays at `0`.",
        "- This indicates the current bridge field `debug_input_trajectory_header_sequence_num` is not equivalent to the internal `planning_seq_num` reported by Apollo control logs in these samples.",
        "",
        "## Per Run",
        "",
    ]
    for row in rows:
        lines.append(
            "- "
            f"{row.get('comparison_label')}: route=`{row.get('route_id')}`, "
            f"first_nonzero_seq=`{row.get('first_nonzero_planning_seq')}`, "
            f"last_raw_input_seq=`{row.get('last_debug_input_trajectory_header_sequence_num')}`, "
            f"last_raw_latest_replan_seq=`{row.get('last_debug_input_latest_replan_trajectory_header_sequence_num')}`, "
            f"last_live_source=`{row.get('last_live_effective_planning_source')}`, "
            f"last_live_input_seq=`{row.get('last_live_control_input_trajectory_header_sequence_num')}`, "
            f"last_live_candidate_seq=`{row.get('last_live_control_input_candidate_trajectory_header_sequence_num')}`, "
            f"last_live_latest_replan_seq=`{row.get('last_live_control_input_latest_replan_trajectory_header_sequence_num')}`, "
            f"last_live_candidate_source=`{row.get('last_live_control_input_candidate_source')}`, "
            f"last_live_exact_match=`{row.get('last_live_matched_planning_event_found_exactly')}`, "
            f"last_live_candidate_exact_match=`{row.get('last_live_matched_candidate_planning_event_found_exactly')}`, "
            f"last_live_gap=`{row.get('last_live_latest_known_planning_sequence_gap')}`, "
            f"last_live_candidate_gap=`{row.get('last_live_candidate_latest_known_planning_sequence_gap')}`, "
            f"control_log_last_seq=`{row.get('control_log_last_planning_seq')}`, "
            f"raw_after_first_nonzero=`{row.get('raw_control_after_first_nonzero_count')}`, "
            f"live_after_first_nonzero=`{row.get('live_control_after_first_nonzero_count')}`, "
            f"gap_sec=`{row.get('gap_first_nonzero_minus_last_raw_sec')}`"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Town01 control input trajectory sequence visibility")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument(
        "--csv",
        type=Path,
        default=REPO_ROOT / "artifacts" / "town01_control_input_sequence.csv",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=REPO_ROOT / "artifacts" / "town01_control_input_sequence_report.md",
    )
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    rows = _collect_rows(batch_root)
    _write_csv(Path(args.csv).expanduser().resolve(), rows)
    _write_report(Path(args.report).expanduser().resolve(), rows, batch_root)
    print(f"[town01-control-input-seq] analyzed runs={len(rows)}")


if __name__ == "__main__":
    main()
