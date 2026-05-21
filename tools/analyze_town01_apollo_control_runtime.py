#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import load_json
from tools.run_town01_route_health import _resolve_run_dir


EMPTY_TRAJECTORY_RE = re.compile(r"planning has no trajectory point\. planning_seq_num:?(\d+)")


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


def _load_optional_jsonl(artifacts: Path, names: List[str]) -> List[Dict[str, Any]]:
    for name in names:
        path = artifacts / name
        if path.exists():
            return _load_jsonl(path)
    return []


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_control_log(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "empty_trajectory_count": 0,
            "empty_trajectory_last_seq": None,
            "empty_trajectory_max_seq": None,
            "estop_trigger_count": 0,
        }
    empty_traj_seqs: List[int] = []
    estop_trigger_count = 0
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = EMPTY_TRAJECTORY_RE.search(line)
        if match:
            try:
                empty_traj_seqs.append(int(match.group(1)))
            except Exception:
                pass
        if "Estop triggered! No control core method executed!" in line:
            estop_trigger_count += 1
    return {
        "empty_trajectory_count": len(empty_traj_seqs),
        "empty_trajectory_last_seq": empty_traj_seqs[-1] if empty_traj_seqs else None,
        "empty_trajectory_max_seq": max(empty_traj_seqs) if empty_traj_seqs else None,
        "estop_trigger_count": estop_trigger_count,
    }


def _extract_control_log_transition_lines(
    path: Path, seq_start: Optional[int], seq_end: Optional[int], limit: int = 24
) -> List[str]:
    if not path.exists():
        return []
    rows: List[str] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        match = EMPTY_TRAJECTORY_RE.search(line)
        if match:
            try:
                seq = int(match.group(1))
            except Exception:
                seq = None
            if seq is not None and seq_start is not None and seq_end is not None and seq_start <= seq <= seq_end:
                rows.append(f"{line_no}:{line.strip()}")
                continue
        if "Estop triggered! No control core method executed!" in line:
            rows.append(f"{line_no}:{line.strip()}")
    if len(rows) > limit:
        rows = rows[-limit:]
    return rows


def _extract_planning_transition_window(
    planning_rows: List[Dict[str, Any]],
    seq_start: Optional[int],
    seq_end: Optional[int],
) -> List[Dict[str, Any]]:
    if seq_start is None or seq_end is None:
        return []
    window: List[Dict[str, Any]] = []
    for row in planning_rows:
        seq = _safe_int(row.get("planning_header_sequence_num"))
        if seq is None or seq < seq_start or seq > seq_end:
            continue
        window.append(
            {
                "planning_header_sequence_num": seq,
                "timestamp": _safe_float(row.get("timestamp")),
                "planning_header_timestamp_sec": _safe_float(row.get("planning_header_timestamp_sec")),
                "trajectory_point_count": int(row.get("trajectory_point_count", 0) or 0),
                "is_replan": bool(row.get("is_replan")),
                "trajectory_type": row.get("trajectory_type"),
                "estop": bool(row.get("estop")),
            }
        )
    return window


def _extract_live_transition_window(
    live_rows: List[Dict[str, Any]],
    seq_start: Optional[int],
    seq_end: Optional[int],
    limit: int = 12,
) -> List[Dict[str, Any]]:
    if seq_start is None or seq_end is None:
        return []
    window: List[Dict[str, Any]] = []
    for row in live_rows:
        latest_seq = _safe_int(row.get("latest_known_planning_sequence_num"))
        if latest_seq is None or latest_seq < seq_start or latest_seq > seq_end:
            continue
        window.append(
            {
                "timestamp": _safe_float(row.get("timestamp")),
                "effective_planning_source": row.get("effective_planning_source"),
                "control_input_trajectory_header_sequence_num": _safe_int(
                    row.get("control_input_trajectory_header_sequence_num")
                ),
                "control_input_latest_replan_trajectory_header_sequence_num": _safe_int(
                    row.get("control_input_latest_replan_trajectory_header_sequence_num")
                ),
                "control_input_candidate_trajectory_header_sequence_num": _safe_int(
                    row.get("control_input_candidate_trajectory_header_sequence_num")
                ),
                "matched_planning_event_found_exactly": bool(
                    row.get("matched_planning_event_found_exactly")
                ),
                "matched_candidate_planning_event_found_exactly": row.get(
                    "matched_candidate_planning_event_found_exactly"
                ),
                "latest_known_planning_sequence_num": latest_seq,
                "latest_known_planning_sequence_gap": _safe_int(
                    row.get("latest_known_planning_sequence_gap")
                ),
                "candidate_latest_known_planning_sequence_gap": _safe_int(
                    row.get("candidate_latest_known_planning_sequence_gap")
                ),
            }
        )
    if len(window) > limit:
        window = window[-limit:]
    return window


def _load_last_control_raw(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    last_row: Dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            last_row = item
    payload = last_row.get("apollo_control_raw") if isinstance(last_row, dict) else None
    if not isinstance(payload, dict):
        payload = {}
    return {
        "ts_sec": _safe_float(last_row.get("ts_sec")),
        "control_header_sequence_num": _safe_int(payload.get("control_header_sequence_num")),
        "control_header_timestamp_sec": _safe_float(payload.get("control_header_timestamp_sec")),
        "driving_mode": _safe_int(payload.get("driving_mode")),
        "throttle": _safe_float(payload.get("throttle")),
        "brake": _safe_float(payload.get("brake")),
        "steering_target": _safe_float(payload.get("steering_target")),
        "debug_input_trajectory_header_sequence_num": _safe_int(
            payload.get("debug_input_trajectory_header_sequence_num")
        ),
        "debug_input_latest_replan_trajectory_header_sequence_num": _safe_int(
            payload.get("debug_input_latest_replan_trajectory_header_sequence_num")
        ),
    }


def _index_rows_by_seq(rows: List[Dict[str, Any]], seq_key: str) -> Dict[int, Dict[str, Any]]:
    index: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        seq = _safe_int(row.get(seq_key))
        if seq is None:
            continue
        index[seq] = row
    return index


def _collect_rows(batch_root: Path) -> List[Dict[str, Any]]:
    manifest = _load_manifest(batch_root)
    rows: List[Dict[str, Any]] = []
    for item in list(manifest.get("runs") or []):
        raw_run_dir = Path(str(item.get("effective_run_dir") or item.get("run_dir") or "")).expanduser().resolve()
        run_dir = _resolve_run_dir(raw_run_dir)
        artifacts = run_dir / "artifacts"
        summary = load_json(run_dir / "summary.json")
        planning_rows = _load_jsonl(artifacts / "planning_topic_debug.jsonl")
        live_rows = _load_jsonl(artifacts / "control_trajectory_consume_debug_live.jsonl")
        refline_rows = _load_optional_jsonl(
            artifacts,
            [
                "apollo_reference_line_debug.jsonl",
                "stage5_apollo_reference_line_debug.jsonl",
            ],
        )
        if not planning_rows or not live_rows:
            continue

        nonzero_rows = [
            row for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) > 0
        ]
        if not nonzero_rows:
            continue
        first_nonzero = nonzero_rows[0]
        first_nonzero_seq = _safe_int(first_nonzero.get("planning_header_sequence_num"))
        first_nonzero_ts = _safe_float(first_nonzero.get("timestamp"))
        first_nonzero_is_replan = bool(first_nonzero.get("is_replan"))

        zero_replan_predecessor = None
        if first_nonzero_seq is not None:
            for row in planning_rows:
                seq = _safe_int(row.get("planning_header_sequence_num"))
                if seq is None or seq >= first_nonzero_seq:
                    continue
                if bool(row.get("is_replan")) and int(row.get("trajectory_point_count", 0) or 0) == 0:
                    zero_replan_predecessor = row

        last_planning = planning_rows[-1]
        last_live = live_rows[-1]
        last_planning_ts = _safe_float(last_planning.get("timestamp"))
        last_live_ts = _safe_float(last_live.get("timestamp"))
        planning_continues_after_control_stop_sec = (
            float(last_planning_ts - last_live_ts)
            if last_planning_ts is not None and last_live_ts is not None
            else None
        )

        control_log = _parse_control_log(artifacts / "apollo_control.INFO")
        zero_replan_seq = _safe_int((zero_replan_predecessor or {}).get("planning_header_sequence_num"))
        seq_window_start = None
        seq_window_end = None
        if zero_replan_seq is not None and first_nonzero_seq is not None:
            seq_window_start = max(0, zero_replan_seq - 2)
            seq_window_end = first_nonzero_seq + 2

        planning_transition_window = _extract_planning_transition_window(
            planning_rows, seq_window_start, seq_window_end
        )
        live_transition_window = _extract_live_transition_window(
            live_rows, max(0, (zero_replan_seq or 0) - 1) if zero_replan_seq is not None else None, first_nonzero_seq
        )
        control_log_transition_lines = _extract_control_log_transition_lines(
            artifacts / "apollo_control.INFO",
            zero_replan_seq - 1 if zero_replan_seq is not None else None,
            first_nonzero_seq if first_nonzero_seq is not None else None,
        )
        last_control_raw = _load_last_control_raw(artifacts / "apollo_control_raw.jsonl")
        refline_by_seq = _index_rows_by_seq(refline_rows, "planning_header_sequence_num")
        zero_replan_refline = refline_by_seq.get(zero_replan_seq or -1, {})
        first_nonzero_refline = refline_by_seq.get(first_nonzero_seq or -1, {})
        rows.append(
            {
                "comparison_label": str(item.get("comparison_label") or summary.get("comparison_label") or ""),
                "route_id": str(summary.get("route_id") or ""),
                "run_dir": str(run_dir),
                "failure_stage": str((summary.get("acceptance") or {}).get("failure_stage") or ""),
                "first_nonzero_planning_seq": first_nonzero_seq,
                "first_nonzero_planning_ts": first_nonzero_ts,
                "first_nonzero_planning_is_replan": first_nonzero_is_replan,
                "zero_replan_predecessor_seq": _safe_int((zero_replan_predecessor or {}).get("planning_header_sequence_num")),
                "zero_replan_predecessor_ts": _safe_float((zero_replan_predecessor or {}).get("timestamp")),
                "last_live_control_planning_seq": _safe_int(last_live.get("latest_planning_msg_sequence_num")),
                "last_live_control_ts": last_live_ts,
                "last_live_effective_planning_source": last_live.get("effective_planning_source"),
                "last_live_control_input_seq": _safe_int(last_live.get("control_input_trajectory_header_sequence_num")),
                "last_live_control_candidate_seq": _safe_int(last_live.get("control_input_candidate_trajectory_header_sequence_num")),
                "last_live_control_candidate_source": last_live.get("control_input_candidate_source"),
                "exact_match_seen": any(bool(r.get("matched_planning_event_found_exactly")) for r in live_rows),
                "exact_candidate_match_seen": any(bool(r.get("matched_candidate_planning_event_found_exactly")) for r in live_rows),
                "last_planning_ts": last_planning_ts,
                "planning_continues_after_control_stop_sec": planning_continues_after_control_stop_sec,
                "control_log_empty_trajectory_count": control_log["empty_trajectory_count"],
                "control_log_empty_trajectory_last_seq": control_log["empty_trajectory_last_seq"],
                "control_log_empty_trajectory_max_seq": control_log["empty_trajectory_max_seq"],
                "control_log_estop_trigger_count": control_log["estop_trigger_count"],
                "zero_replan_reference_line_provider_status": zero_replan_refline.get(
                    "reference_line_provider_status"
                ),
                "zero_replan_create_route_segments_status": zero_replan_refline.get(
                    "create_route_segments_status"
                ),
                "zero_replan_lane_follow_map_status": zero_replan_refline.get(
                    "lane_follow_map_status"
                ),
                "zero_replan_planning_empty_reason_guess": zero_replan_refline.get(
                    "planning_empty_reason_guess"
                ),
                "first_nonzero_reference_line_provider_status": first_nonzero_refline.get(
                    "reference_line_provider_status"
                ),
                "first_nonzero_lane_follow_map_status": first_nonzero_refline.get(
                    "lane_follow_map_status"
                ),
                "first_nonzero_route_segment_count": _safe_int(
                    first_nonzero_refline.get("route_segment_count")
                ),
                "last_control_raw_seq": last_control_raw.get("control_header_sequence_num"),
                "last_control_raw_ts": last_control_raw.get("ts_sec"),
                "last_control_raw_brake": last_control_raw.get("brake"),
                "last_control_raw_throttle": last_control_raw.get("throttle"),
                "last_control_raw_steering_target": last_control_raw.get("steering_target"),
                "last_control_raw_driving_mode": last_control_raw.get("driving_mode"),
                "last_control_raw_input_seq": last_control_raw.get("debug_input_trajectory_header_sequence_num"),
                "last_control_raw_latest_replan_seq": last_control_raw.get(
                    "debug_input_latest_replan_trajectory_header_sequence_num"
                ),
                "planning_transition_window": planning_transition_window,
                "live_transition_window": live_transition_window,
                "control_log_transition_lines": control_log_transition_lines,
            }
        )
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "comparison_label",
        "route_id",
        "run_dir",
        "failure_stage",
        "first_nonzero_planning_seq",
        "first_nonzero_planning_ts",
        "first_nonzero_planning_is_replan",
        "zero_replan_predecessor_seq",
        "zero_replan_predecessor_ts",
        "last_live_control_planning_seq",
        "last_live_control_ts",
        "last_live_effective_planning_source",
        "last_live_control_input_seq",
        "last_live_control_candidate_seq",
        "last_live_control_candidate_source",
        "exact_match_seen",
        "exact_candidate_match_seen",
        "last_planning_ts",
        "planning_continues_after_control_stop_sec",
        "control_log_empty_trajectory_count",
        "control_log_empty_trajectory_last_seq",
        "control_log_empty_trajectory_max_seq",
        "control_log_estop_trigger_count",
        "zero_replan_reference_line_provider_status",
        "zero_replan_create_route_segments_status",
        "zero_replan_lane_follow_map_status",
        "zero_replan_planning_empty_reason_guess",
        "first_nonzero_reference_line_provider_status",
        "first_nonzero_lane_follow_map_status",
        "first_nonzero_route_segment_count",
        "last_control_raw_seq",
        "last_control_raw_ts",
        "last_control_raw_brake",
        "last_control_raw_throttle",
        "last_control_raw_steering_target",
        "last_control_raw_driving_mode",
        "last_control_raw_input_seq",
        "last_control_raw_latest_replan_seq",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def _write_report(path: Path, rows: List[Dict[str, Any]], batch_root: Path) -> None:
    lines = [
        "# Town01 Apollo Control Runtime Report",
        "",
        f"- batch_root: `{batch_root}`",
        f"- run_count: `{len(rows)}`",
        "",
        "## Key Finding",
        "",
        "- This report focuses on Apollo control runtime behavior around the `zero-point replan -> first nonzero replan` boundary.",
        "- A zero `input_debug.trajectory_header.sequence_num` is not treated as a bridge-only problem; Apollo control source writes that header only on the normal `ProduceControlCommand()` path.",
        "- The critical question is whether control crosses from the last zero-point replan into the first nonzero replan, or stops emitting output before that boundary.",
        "",
        "## Per Run",
        "",
    ]
    for row in rows:
        lines.append(
            "- "
            f"{row.get('comparison_label')}: route=`{row.get('route_id')}`, "
            f"stage=`{row.get('failure_stage')}`, "
            f"zero_replan_seq=`{row.get('zero_replan_predecessor_seq')}`, "
            f"first_nonzero_seq=`{row.get('first_nonzero_planning_seq')}`, "
            f"last_live_seq=`{row.get('last_live_control_planning_seq')}`, "
            f"live_source=`{row.get('last_live_effective_planning_source')}`, "
            f"input_seq=`{row.get('last_live_control_input_seq')}`, "
            f"candidate_seq=`{row.get('last_live_control_candidate_seq')}`, "
            f"exact_match=`{row.get('exact_match_seen')}`, "
            f"candidate_exact_match=`{row.get('exact_candidate_match_seen')}`, "
            f"control_log_empty_last_seq=`{row.get('control_log_empty_trajectory_last_seq')}`, "
            f"estop_trigger_count=`{row.get('control_log_estop_trigger_count')}`, "
            f"planning_continues_after_control_stop_sec=`{row.get('planning_continues_after_control_stop_sec')}`"
        )
        lines.append(
            f"  zero_replan_planning_state: "
            f"reference_line_provider_status=`{row.get('zero_replan_reference_line_provider_status')}`, "
            f"create_route_segments_status=`{row.get('zero_replan_create_route_segments_status')}`, "
            f"lane_follow_map_status=`{row.get('zero_replan_lane_follow_map_status')}`, "
            f"empty_reason_guess=`{row.get('zero_replan_planning_empty_reason_guess')}`"
        )
        lines.append(
            f"  first_nonzero_planning_state: "
            f"reference_line_provider_status=`{row.get('first_nonzero_reference_line_provider_status')}`, "
            f"lane_follow_map_status=`{row.get('first_nonzero_lane_follow_map_status')}`, "
            f"route_segment_count=`{row.get('first_nonzero_route_segment_count')}`"
        )
        lines.append(
            f"  last_control_raw: seq=`{row.get('last_control_raw_seq')}`, "
            f"input_seq=`{row.get('last_control_raw_input_seq')}`, "
            f"latest_replan_seq=`{row.get('last_control_raw_latest_replan_seq')}`, "
            f"driving_mode=`{row.get('last_control_raw_driving_mode')}`, "
            f"throttle=`{row.get('last_control_raw_throttle')}`, "
            f"brake=`{row.get('last_control_raw_brake')}`, "
            f"steering_target=`{row.get('last_control_raw_steering_target')}`"
        )
        lines.append("  planning_transition_window:")
        for item in row.get("planning_transition_window") or []:
            lines.append(
                "  - "
                f"seq=`{item.get('planning_header_sequence_num')}`, "
                f"points=`{item.get('trajectory_point_count')}`, "
                f"is_replan=`{item.get('is_replan')}`, "
                f"timestamp=`{item.get('timestamp')}`, "
                f"header_timestamp_sec=`{item.get('planning_header_timestamp_sec')}`"
            )
        lines.append("  live_transition_window:")
        for item in row.get("live_transition_window") or []:
            lines.append(
                "  - "
                f"latest_known_seq=`{item.get('latest_known_planning_sequence_num')}`, "
                f"source=`{item.get('effective_planning_source')}`, "
                f"input_seq=`{item.get('control_input_trajectory_header_sequence_num')}`, "
                f"latest_replan_seq=`{item.get('control_input_latest_replan_trajectory_header_sequence_num')}`, "
                f"candidate_seq=`{item.get('control_input_candidate_trajectory_header_sequence_num')}`, "
                f"exact_match=`{item.get('matched_planning_event_found_exactly')}`, "
                f"timestamp=`{item.get('timestamp')}`"
            )
        lines.append("  control_log_transition_lines:")
        for item in row.get("control_log_transition_lines") or []:
            lines.append(f"  - `{item}`")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Apollo control runtime around Town01 planning/control transition")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument(
        "--csv",
        type=Path,
        default=REPO_ROOT / "artifacts" / "town01_apollo_control_runtime.csv",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=REPO_ROOT / "artifacts" / "town01_apollo_control_runtime_report.md",
    )
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    rows = _collect_rows(batch_root)
    _write_csv(Path(args.csv).expanduser().resolve(), rows)
    _write_report(Path(args.report).expanduser().resolve(), rows, batch_root)
    print(f"[town01-apollo-control-runtime] analyzed runs={len(rows)}")


if __name__ == "__main__":
    main()
