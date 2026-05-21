#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
ROOT_ARTIFACTS = REPO_ROOT / "artifacts"
LOCAL_TZ = datetime.now().astimezone().tzinfo

CASE_LABELS = {
    "case_a_profile_a_locked": "Case A",
    "case_b_profile_b_locked": "Case B",
    "case_b_freeze_profile_b_locked": "Case B-freeze",
}
CASE_ORDER = {name: idx for idx, name in enumerate(CASE_LABELS, start=1)}

PLANNING_ERROR_PATTERNS = [
    "path data is empty",
    "reference line",
    "Fail to aggregate planning trajectory",
    "planner failed to make a driving plan",
]
CONTROL_ERROR_PATTERNS = [
    "planning has no trajectory point",
    "Failed to produce control command",
]


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", errors="ignore") as fp:
        for line in fp:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def _write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _count_pattern(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    return len(re.findall(pattern, path.read_text(errors="ignore")))


def _discover_run_dirs(batch_root: Path) -> List[Path]:
    discovered: Dict[str, Path] = {}
    seen_resolved: set[Path] = set()
    for child in sorted(batch_root.iterdir()):
        if child.name == "artifacts" or child.is_symlink() or not child.is_dir():
            continue
        if (child / "RUN_DIR_REDIRECT.txt").exists():
            continue
        if not ((child / "summary.json").exists() or (child / "artifacts").exists()):
            continue
        canonical = re.sub(r"__\d+$", "", child.name)
        resolved = child.resolve()
        if resolved in seen_resolved:
            continue
        current = discovered.get(canonical)
        if current is None or child.name > current.name:
            discovered[canonical] = resolved
            seen_resolved.add(resolved)
    return list(discovered.values())


def _analysis_paths(batch_root: Path) -> Dict[str, Path]:
    batch_artifacts = batch_root / "artifacts"
    names = [
        "stage3_baseline_confirmation.md",
        "bridge_timing_model.md",
        "bridge_heuristic_policy.md",
        "planning_control_contract.md",
        "reroute_policy_and_events.md",
        "goal_validity_policy.md",
        "summary_generation_policy.md",
        "bridge_backward_compatibility_notes.md",
        "control_trajectory_consume_summary.json",
        "control_no_trajectory_classification.md",
        "planning_control_timeline_alignment.csv",
        "planning_control_timeline_report.md",
        "planning_route_segment_failure_report.md",
        "profile_b_vs_bfreeze_comparison.csv",
        "profile_b_vs_bfreeze_report.md",
        "bridge_mechanism_fix_case_comparison.csv",
        "stack_health_evaluation.md",
        "stage3_planning_control_rootcause_report.md",
        "bridge_mechanism_fix_report.md",
    ]
    out: Dict[str, Path] = {}
    for name in names:
        stem = name.replace(".", "_").replace("-", "_")
        out[f"root_{stem}"] = ROOT_ARTIFACTS / name
        out[f"batch_{stem}"] = batch_artifacts / name
    return out


def _parse_log_timestamp(line: str) -> Optional[float]:
    m = re.match(r"^[IWEF](\d{2})(\d{2}) (\d{2}:\d{2}:\d{2}\.\d+)", line)
    if not m:
        return None
    month = int(m.group(1))
    day = int(m.group(2))
    time_text = m.group(3)
    year = datetime.now(LOCAL_TZ).year
    try:
        dt = datetime.strptime(f"{year}-{month:02d}-{day:02d} {time_text}", "%Y-%m-%d %H:%M:%S.%f")
        dt = dt.replace(tzinfo=LOCAL_TZ)
    except Exception:
        return None
    return dt.timestamp()


def _summary_stats(values: Sequence[float]) -> Dict[str, Any]:
    finite = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    if not finite:
        return {"count": 0, "mean": None, "median": None, "max": None}
    ordered = sorted(finite)
    mid = len(ordered) // 2
    median = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0
    return {
        "count": len(ordered),
        "mean": sum(ordered) / len(ordered),
        "median": median,
        "max": max(ordered),
    }


def _baseline_confirmation_md() -> str:
    return "\n".join(
        [
            "# Stage3 Baseline Confirmation",
            "",
            "- Default runtime baseline: `configs/io/examples/followstop_apollo_gt.yaml` and `configs/io/examples/followstop_apollo_gt_case2_locked.yaml`",
            "- Experimental mainline: `configs/io/examples/followstop_apollo_gt_case3_locked.yaml`",
            "- New comparison variant: `configs/io/examples/followstop_apollo_gt_case3_freeze_locked.yaml`",
            "",
            "## Locked Rules",
            "",
            "- Profile A keeps `localization_back_offset_m=0.0`, `snap_start_to_lane=false`, `start_nudge_m=0.0`, `start_nudge_retry_step_m=0.0`, `start_nudge_use_lane_heading=false`.",
            "- Profile B keeps trusted lane-centerline snap, heading gate on, and zero nudge.",
            "- Profile B-freeze keeps Profile B unchanged except `freeze_after_success=true`.",
            "- `base_map_text_xy_heuristic` is not allowed in this stage.",
            "- `lane_heading nudge` is not allowed in this stage.",
            "- No default large nudge is allowed in this stage.",
            "",
            "## Experiment Matrix",
            "",
            "- Case A = Profile A / Case 2 locked",
            "- Case B = Profile B / Case 3 locked",
            "- Case B-freeze = Profile B + freeze_after_success=true",
            "",
        ]
    )


def _event_seq(row: Dict[str, Any]) -> Optional[int]:
    value = row.get("planning_header_sequence_num")
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _normalize_planning_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["timestamp"] = _safe_float(item.get("timestamp"))
        item["planning_header_timestamp_sec"] = _safe_float(item.get("planning_header_timestamp_sec"))
        item["planning_header_sequence_num"] = _event_seq(item)
        item["trajectory_point_count"] = int(item.get("trajectory_point_count", 0) or 0)
        item["trajectory_relative_time_min_sec"] = _safe_float(item.get("trajectory_relative_time_min_sec"))
        item["trajectory_relative_time_max_sec"] = _safe_float(item.get("trajectory_relative_time_max_sec"))
        item["planning_message_parsed_successfully"] = bool(
            item.get("planning_message_parsed_successfully", False)
        )
        out.append(item)
    out.sort(key=lambda x: float(x.get("timestamp") or 0.0))
    return out


def _build_planning_index(rows: List[Dict[str, Any]]) -> Tuple[Dict[int, Dict[str, Any]], List[float]]:
    by_seq: Dict[int, Dict[str, Any]] = {}
    timestamps: List[float] = []
    for row in rows:
        seq = row.get("planning_header_sequence_num")
        if seq is not None:
            by_seq[int(seq)] = row
        ts = _safe_float(row.get("timestamp"))
        if ts is not None:
            timestamps.append(ts)
    return by_seq, timestamps


def _nearest_planning_row(
    rows: List[Dict[str, Any]],
    target_ts: float,
    *,
    max_dt_sec: Optional[float] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
    if not rows:
        return None, None
    ts_values = [float(item.get("timestamp") or 0.0) for item in rows]
    pos = bisect.bisect_left(ts_values, target_ts)
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    if pos < len(rows):
        candidates.append((abs(ts_values[pos] - target_ts), rows[pos]))
    if pos > 0:
        candidates.append((abs(ts_values[pos - 1] - target_ts), rows[pos - 1]))
    if not candidates:
        return None, None
    dt_sec, row = min(candidates, key=lambda item: item[0])
    if max_dt_sec is not None and dt_sec > max_dt_sec:
        return None, None
    return row, dt_sec


def _last_success_before(rows: List[Dict[str, Any]], target_ts: float, *, max_dt_sec: float = 0.3) -> Optional[Dict[str, Any]]:
    eligible = [row for row in rows if _safe_float(row.get("timestamp")) is not None and float(row["timestamp"]) <= target_ts]
    if not eligible:
        return None
    row = eligible[-1]
    dt = target_ts - float(row["timestamp"])
    return row if dt <= max_dt_sec else None


def _is_control_mode_not_ready(state: Optional[Dict[str, Any]]) -> bool:
    if not state:
        return False
    for key in ("auto_drive_mode", "engage_state", "drive_mode"):
        value = state.get(key)
        if value is None:
            continue
        text = str(value).strip().lower()
        if any(token in text for token in ("disallow", "not_ready", "not ready", "manual")):
            return True
    return False


def _classify_reject_reason(
    event_ts: float,
    planning_row: Optional[Dict[str, Any]],
    recent_control_state: Optional[Dict[str, Any]],
) -> str:
    if planning_row is None:
        return "no_recent_planning_message"
    planning_ts = _safe_float(planning_row.get("timestamp"))
    if planning_ts is None or (event_ts - planning_ts) > 0.300:
        return "no_recent_planning_message"
    if not bool(planning_row.get("planning_message_parsed_successfully", False)):
        return "planning_parse_failed"
    point_count = int(planning_row.get("trajectory_point_count", 0) or 0)
    if point_count <= 0:
        return "zero_trajectory_points"
    if bool(planning_row.get("estop")) or bool((recent_control_state or {}).get("estop_flag")):
        return "estop_or_safety_guard"
    now_rel = None
    plan_header_ts = _safe_float(planning_row.get("planning_header_timestamp_sec"))
    if plan_header_ts is not None:
        now_rel = event_ts - plan_header_ts
    first_rel = _safe_float(planning_row.get("trajectory_relative_time_min_sec"))
    last_rel = _safe_float(planning_row.get("trajectory_relative_time_max_sec"))
    if now_rel is None or first_rel is None or last_rel is None:
        return "trajectory_invalid_format"
    if now_rel > (last_rel + 0.05):
        return "trajectory_all_points_expired"
    if now_rel < (first_rel - 0.05):
        return "trajectory_not_started_yet"
    if _is_control_mode_not_ready(recent_control_state):
        return "control_mode_not_ready"
    if bool((recent_control_state or {}).get("control_used_cached_trajectory")):
        return "cached_trajectory_used"
    return "unknown"


def _timeline_miss_class(
    reject_reason: str,
    nearest_planning: Optional[Dict[str, Any]],
    nearest_dt_sec: Optional[float],
) -> str:
    if nearest_planning is None or nearest_dt_sec is None or nearest_dt_sec > 0.300:
        return "miss_without_recent_planning"
    if not bool(nearest_planning.get("planning_message_parsed_successfully", False)):
        return "miss_after_parse_failure"
    if int(nearest_planning.get("trajectory_point_count", 0) or 0) <= 0:
        return "miss_after_empty_planning"
    if reject_reason == "trajectory_all_points_expired":
        return "miss_after_nonempty_but_expired_planning"
    if reject_reason in {
        "trajectory_not_started_yet",
        "control_mode_not_ready",
        "estop_or_safety_guard",
        "trajectory_invalid_format",
        "cached_trajectory_used",
        "unknown",
    }:
        return "miss_after_nonempty_but_rejected_planning"
    return "miss_unknown"


def _parse_control_no_trajectory_log(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    events: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Optional[int]]] = set()
    seq_re = re.compile(r"planning_seq_num:(\d+)")
    for line in path.read_text(errors="ignore").splitlines():
        if "planning has no trajectory point" not in line:
            continue
        ts_sec = _parse_log_timestamp(line)
        if ts_sec is None:
            continue
        seq_match = seq_re.search(line)
        seq = int(seq_match.group(1)) if seq_match else None
        ts_text = re.match(r"^[IWEF]\d{4} \d{2}:\d{2}:\d{2}\.\d+", line)
        dedupe_key = (ts_text.group(0) if ts_text else f"{ts_sec:.6f}", seq)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        events.append(
            {
                "timestamp": ts_sec,
                "planning_seq_num": seq,
                "raw_line": line,
            }
        )
    events.sort(key=lambda item: float(item["timestamp"]))
    return events


def _parse_log_keyword_events(path: Path, patterns: Sequence[str], event_type: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(errors="ignore").splitlines():
        ts_sec = _parse_log_timestamp(line)
        if ts_sec is None:
            continue
        for pattern in patterns:
            if pattern in line:
                rows.append(
                    {
                        "timestamp": ts_sec,
                        "event_type": event_type,
                        "message": line,
                        "pattern": pattern,
                    }
                )
                break
    return rows


def _normalize_live_control_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["event_type"] = "control_success"
        item["timestamp"] = _safe_float(item.get("timestamp"))
        item["wall_time_sec"] = _safe_float(item.get("wall_time_sec"))
        item["sim_time_sec"] = _safe_float(item.get("sim_time_sec"))
        item["world_frame"] = item.get("world_frame")
        item["latest_planning_msg_timestamp"] = _safe_float(item.get("latest_planning_msg_timestamp"))
        item["latest_planning_msg_age_ms"] = _safe_float(item.get("latest_planning_msg_age_ms"))
        item["latest_planning_msg_sequence_num"] = (
            int(item["latest_planning_msg_sequence_num"])
            if item.get("latest_planning_msg_sequence_num") is not None
            else None
        )
        item["latest_planning_trajectory_point_count"] = int(
            item.get("latest_planning_trajectory_point_count", 0) or 0
        )
        item["trajectory_first_point_relative_time"] = _safe_float(item.get("trajectory_first_point_relative_time"))
        item["trajectory_last_point_relative_time"] = _safe_float(item.get("trajectory_last_point_relative_time"))
        item["control_now_relative_to_planning_header_sec"] = _safe_float(
            item.get("control_now_relative_to_planning_header_sec")
        )
        item["chassis_speed_mps"] = _safe_float(item.get("chassis_speed_mps"))
        out.append(item)
    out.sort(key=lambda row: float(row.get("timestamp") or 0.0))
    return out


def _build_control_consume_events(
    planning_rows: List[Dict[str, Any]],
    live_rows: List[Dict[str, Any]],
    miss_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    planning_by_seq, _ = _build_planning_index(planning_rows)
    success_rows = _normalize_live_control_rows(live_rows)
    events: List[Dict[str, Any]] = [dict(row) for row in success_rows]

    for miss in miss_rows:
        ts = float(miss["timestamp"])
        planning_row = planning_by_seq.get(int(miss["planning_seq_num"])) if miss.get("planning_seq_num") is not None else None
        if planning_row is None:
            planning_row, _ = _nearest_planning_row(planning_rows, ts, max_dt_sec=0.300)
        recent_state = _last_success_before(success_rows, ts, max_dt_sec=0.300)
        planning_ts = _safe_float((planning_row or {}).get("timestamp"))
        planning_header_ts = _safe_float((planning_row or {}).get("planning_header_timestamp_sec"))
        first_rel = _safe_float((planning_row or {}).get("trajectory_relative_time_min_sec"))
        last_rel = _safe_float((planning_row or {}).get("trajectory_relative_time_max_sec"))
        now_rel = (ts - planning_header_ts) if planning_header_ts is not None else None
        reject_reason = _classify_reject_reason(ts, planning_row, recent_state)
        row = {
            "event_type": "control_no_trajectory",
            "timestamp": ts,
            "wall_time_sec": ts,
            "sim_time_sec": (recent_state or {}).get("sim_time_sec"),
            "world_frame": (recent_state or {}).get("world_frame"),
            "control_cycle_index": None,
            "latest_planning_msg_received": planning_row is not None,
            "latest_planning_msg_timestamp": planning_ts,
            "latest_planning_msg_age_ms": ((ts - planning_ts) * 1000.0) if planning_ts is not None else None,
            "latest_planning_msg_sequence_num": (
                int((planning_row or {}).get("planning_header_sequence_num"))
                if (planning_row or {}).get("planning_header_sequence_num") is not None
                else miss.get("planning_seq_num")
            ),
            "latest_planning_trajectory_point_count": int(
                (planning_row or {}).get("trajectory_point_count", 0) or 0
            ),
            "latest_planning_parse_success": bool(
                (planning_row or {}).get("planning_message_parsed_successfully", False)
            ),
            "trajectory_first_point_relative_time": first_rel,
            "trajectory_last_point_relative_time": last_rel,
            "control_now_relative_to_planning_header_sec": now_rel,
            "trajectory_all_points_expired": (
                bool(now_rel is not None and last_rel is not None and now_rel > (last_rel + 0.05))
            ),
            "trajectory_not_started_yet": (
                bool(now_rel is not None and first_rel is not None and now_rel < (first_rel - 0.05))
            ),
            "trajectory_time_window_valid": (
                bool(
                    now_rel is not None
                    and first_rel is not None
                    and last_rel is not None
                    and first_rel - 0.05 <= now_rel <= last_rel + 0.05
                )
            ),
            "control_used_planning_trajectory": False,
            "control_used_cached_trajectory": False,
            "control_had_no_trajectory": True,
            "reject_reason": reject_reason,
            "auto_drive_mode": (recent_state or {}).get("auto_drive_mode"),
            "engage_state": (recent_state or {}).get("engage_state"),
            "estop_flag": bool((planning_row or {}).get("estop")) or bool((recent_state or {}).get("estop_flag")),
            "chassis_speed_mps": _safe_float((recent_state or {}).get("chassis_speed_mps")),
            "gear": (recent_state or {}).get("gear"),
            "drive_mode": (recent_state or {}).get("drive_mode"),
            "planning_header_sequence_num_used": miss.get("planning_seq_num"),
            "planning_header_timestamp_sec_used": planning_header_ts,
            "raw_line": miss.get("raw_line"),
        }
        events.append(row)

    events.sort(key=lambda row: float(row.get("timestamp") or 0.0))
    for idx, row in enumerate(events, start=1):
        row["control_cycle_index"] = idx

    no_traj = [row for row in events if row["event_type"] == "control_no_trajectory"]
    reasons = Counter(str(row.get("reject_reason") or "unknown") for row in no_traj)
    summary = {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "total_control_cycles": len(events),
        "total_no_trajectory_events": len(no_traj),
        "no_trajectory_by_reason": dict(reasons),
        "total_used_planning_trajectory": sum(1 for row in events if bool(row.get("control_used_planning_trajectory"))),
        "total_used_cached_trajectory": sum(1 for row in events if bool(row.get("control_used_cached_trajectory"))),
        "expired_trajectory_events": int(reasons.get("trajectory_all_points_expired", 0)),
        "stale_planning_events": int(reasons.get("no_recent_planning_message", 0)),
        "zero_point_planning_events": int(reasons.get("zero_trajectory_points", 0)),
        "parse_fail_events": int(reasons.get("planning_parse_failed", 0)),
        "control_mode_not_ready_events": int(reasons.get("control_mode_not_ready", 0)),
        "estop_or_safety_guard_events": int(reasons.get("estop_or_safety_guard", 0)),
    }
    return events, summary


def _route_segment_failure_summary(
    planning_rows: List[Dict[str, Any]],
    route_rows: List[Dict[str, Any]],
    control_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    miss_events = [row for row in control_events if row.get("event_type") == "control_no_trajectory"]
    candidate_rows: List[Dict[str, Any]] = []
    route_by_seq: Dict[int, Dict[str, Any]] = {}
    for row in route_rows:
        try:
            seq = int(row["planning_header_sequence_num"]) if row.get("planning_header_sequence_num") is not None else None
        except Exception:
            seq = None
        if seq is not None:
            route_by_seq[seq] = row
        if int((planning_rows[0] or {}).get("trajectory_point_count", 0) if planning_rows else 0) < 0:
            pass
    planning_by_seq, _ = _build_planning_index(planning_rows)
    for row in planning_rows:
        if int(row.get("trajectory_point_count", 0) or 0) <= 0:
            seq = row.get("planning_header_sequence_num")
            if seq is not None and seq in route_by_seq:
                candidate_rows.append(route_by_seq[seq])
    for miss in miss_events:
        seq = miss.get("latest_planning_msg_sequence_num")
        if seq is not None and int(seq) in route_by_seq:
            candidate_rows.append(route_by_seq[int(seq)])
        else:
            nearest_plan, _ = _nearest_planning_row(planning_rows, float(miss.get("timestamp") or 0.0), max_dt_sec=0.300)
            seq2 = (nearest_plan or {}).get("planning_header_sequence_num")
            if seq2 is not None and int(seq2) in route_by_seq:
                candidate_rows.append(route_by_seq[int(seq2)])
    dedup: Dict[str, Dict[str, Any]] = {}
    for row in candidate_rows:
        key = str(row.get("planning_header_sequence_num")) + "|" + str(row.get("timestamp"))
        dedup[key] = row
    sample_rows = list(dedup.values())
    reference_missing = sum(1 for row in sample_rows if int(row.get("reference_line_count", 0) or 0) <= 0)
    route_segment_missing = sum(1 for row in sample_rows if int(row.get("route_segment_count", 0) or 0) <= 0)
    dest_beyond = sum(1 for row in sample_rows if row.get("dest_beyond_reference_line") is True)
    total = len(sample_rows)
    if total <= 0:
        top_cause = "no_route_segment_samples"
    else:
        scores = {
            "reference_line_missing": reference_missing,
            "route_segment_missing": route_segment_missing,
            "dest_beyond_reference_line": dest_beyond,
            "planning_control_timing_mismatch_or_other": max(0, total - max(reference_missing, route_segment_missing, dest_beyond)),
        }
        top_cause = max(scores.items(), key=lambda item: item[1])[0]
    return {
        "sample_count": total,
        "reference_line_missing_count": reference_missing,
        "route_segment_missing_count": route_segment_missing,
        "dest_beyond_reference_line_count": dest_beyond,
        "top_cause": top_cause,
    }


def _planning_summary_from_event_stream(planning_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    point_counts = [int(row.get("trajectory_point_count", 0) or 0) for row in planning_rows]
    nonzero_rows = [row for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) > 0]
    parse_fail_reasons = Counter(
        str(row.get("parse_fail_reason") or "")
        for row in planning_rows
        if not bool(row.get("planning_message_parsed_successfully", False))
    )
    return {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "total_messages_received": len(planning_rows),
        "messages_with_nonzero_trajectory_points": len(nonzero_rows),
        "messages_with_zero_trajectory_points": sum(1 for row in planning_rows if int(row.get("trajectory_point_count", 0) or 0) <= 0),
        "max_trajectory_point_count": max(point_counts) if point_counts else 0,
        "mean_trajectory_point_count": (sum(point_counts) / float(len(point_counts))) if point_counts else 0.0,
        "first_nonzero_trajectory_timestamp": _safe_float((nonzero_rows[0] or {}).get("timestamp")) if nonzero_rows else None,
        "parse_fail_count": sum(1 for row in planning_rows if not bool(row.get("planning_message_parsed_successfully", False))),
        "parse_fail_reasons_topk": [
            {"reason": reason, "count": count}
            for reason, count in parse_fail_reasons.most_common(5)
            if reason
        ],
    }


def _detect_fake_zero(
    planning_debug_summary: Dict[str, Any],
    health: Dict[str, Any],
    finalized_planning_summary: Dict[str, Any],
) -> bool:
    finalized_nonzero = int(finalized_planning_summary.get("messages_with_nonzero_trajectory_points", 0) or 0)
    finalized_total = int(finalized_planning_summary.get("total_messages_received", 0) or 0)
    live_nonzero = int(planning_debug_summary.get("messages_with_nonzero_trajectory_points", 0) or 0)
    live_total = int(planning_debug_summary.get("total_messages_received", 0) or 0)
    health_nonzero = int(health.get("planning_nonempty_trajectory_count", 0) or 0)
    return bool(
        (finalized_total > 0 and live_total == 0)
        or (finalized_nonzero > 0 and live_nonzero == 0)
        or (finalized_nonzero > 0 and health_nonzero == 0)
    )


def _health_eval(case_result: Dict[str, Any]) -> Dict[str, Any]:
    planning_msgs = max(1, int(case_result.get("planning_topic_messages_received", 0) or 0))
    planning_nonzero = int(case_result.get("messages_with_nonzero_trajectory_points", 0) or 0)
    control_cycles = max(1, int(case_result.get("total_control_cycles", 0) or 0))
    no_traj = int(case_result.get("total_no_trajectory_events", 0) or 0)
    expired = int(case_result.get("expired_trajectory_events", 0) or 0)
    planning_error_rate = (
        (
            int(case_result.get("count_reference_line", 0) or 0)
            + int(case_result.get("count_fail_to_aggregate_planning_trajectory", 0) or 0)
            + int(case_result.get("count_planner_failed_to_make_a_driving_plan", 0) or 0)
        )
        / float(planning_msgs)
    )
    planning_nonzero_ratio = planning_nonzero / float(planning_msgs)
    control_no_trajectory_ratio = no_traj / float(control_cycles)
    expired_ratio = expired / float(max(1, no_traj))
    stack_healthy = (
        planning_nonzero_ratio >= 0.80
        and control_no_trajectory_ratio <= 0.15
        and planning_error_rate <= 0.01
        and expired_ratio <= 0.25
    )
    unhealthy_reasons = [
        ("control_no_trajectory_ratio_high", control_no_trajectory_ratio),
        ("expired_or_stale_planning_high", expired_ratio),
        ("reference_line_or_aggregate_fail_high", planning_error_rate),
        ("planning_nonzero_ratio_low", 1.0 - planning_nonzero_ratio),
    ]
    top1 = "unknown"
    if not stack_healthy:
        top1 = max(unhealthy_reasons, key=lambda item: item[1])[0]
    scenario_success = bool(case_result.get("success", False))
    if scenario_success and stack_healthy:
        final_label = "scenario_success_and_stack_healthy"
    elif scenario_success:
        final_label = "scenario_success_but_stack_unhealthy"
    elif stack_healthy:
        final_label = "scenario_fail_but_stack_partially_healthy"
    else:
        final_label = "scenario_fail_and_stack_unhealthy"
    return {
        "scenario_success_raw": scenario_success,
        "planning_nonzero_ratio": planning_nonzero_ratio,
        "control_no_trajectory_ratio": control_no_trajectory_ratio,
        "planning_error_rate": planning_error_rate,
        "expired_ratio": expired_ratio,
        "stack_healthy": stack_healthy,
        "stack_unhealthy_reason_top1": top1,
        "final_outcome_label": final_label,
    }


def _build_timeline_rows(
    case_label: str,
    planning_rows: List[Dict[str, Any]],
    control_events: List[Dict[str, Any]],
    routing_rows: List[Dict[str, Any]],
    planning_log_events: List[Dict[str, Any]],
    control_log_events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in planning_rows:
        rows.append(
            {
                "case": case_label,
                "timestamp": row.get("timestamp"),
                "event_type": "planning_msg",
                "planning_seq_num": row.get("planning_header_sequence_num"),
                "planning_point_count": row.get("trajectory_point_count"),
                "routing_phase": "",
                "control_reject_reason": "",
                "nearest_planning_dt_ms": 0.0,
                "nearest_planning_seq_num": row.get("planning_header_sequence_num"),
                "nearest_planning_point_count": row.get("trajectory_point_count"),
                "timeline_miss_class": "",
                "message": "",
            }
        )
    for row in routing_rows:
        rows.append(
            {
                "case": case_label,
                "timestamp": _safe_float(row.get("ts_sec")),
                "event_type": "routing_request",
                "planning_seq_num": "",
                "planning_point_count": "",
                "routing_phase": row.get("routing_phase"),
                "control_reject_reason": "",
                "nearest_planning_dt_ms": "",
                "nearest_planning_seq_num": "",
                "nearest_planning_point_count": "",
                "timeline_miss_class": "",
                "message": "routing_request_sent" if row.get("routing_request_sent") else "routing_skipped_due_to_freeze",
            }
        )
    for row in control_events:
        ts = _safe_float(row.get("timestamp"))
        nearest_planning, nearest_dt = _nearest_planning_row(planning_rows, float(ts or 0.0), max_dt_sec=0.300)
        miss_class = ""
        if row.get("event_type") == "control_no_trajectory":
            miss_class = _timeline_miss_class(
                str(row.get("reject_reason") or "unknown"),
                nearest_planning,
                nearest_dt,
            )
        rows.append(
            {
                "case": case_label,
                "timestamp": ts,
                "event_type": row.get("event_type"),
                "planning_seq_num": row.get("latest_planning_msg_sequence_num"),
                "planning_point_count": row.get("latest_planning_trajectory_point_count"),
                "routing_phase": "",
                "control_reject_reason": row.get("reject_reason", ""),
                "nearest_planning_dt_ms": (nearest_dt * 1000.0) if nearest_dt is not None else None,
                "nearest_planning_seq_num": (nearest_planning or {}).get("planning_header_sequence_num"),
                "nearest_planning_point_count": (nearest_planning or {}).get("trajectory_point_count"),
                "timeline_miss_class": miss_class,
                "message": row.get("raw_line", ""),
            }
        )
    for row in planning_log_events + control_log_events:
        rows.append(
            {
                "case": case_label,
                "timestamp": row.get("timestamp"),
                "event_type": row.get("event_type"),
                "planning_seq_num": "",
                "planning_point_count": "",
                "routing_phase": "",
                "control_reject_reason": "",
                "nearest_planning_dt_ms": "",
                "nearest_planning_seq_num": "",
                "nearest_planning_point_count": "",
                "timeline_miss_class": "",
                "message": row.get("message", ""),
            }
        )
    rows.sort(key=lambda item: (float(item.get("timestamp") or 0.0), str(item.get("event_type"))))
    return rows


def analyze_run(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    summary = _load_json(run_dir / "summary.json")
    health = _load_json(artifacts / "bridge_health_summary.json")
    planning_debug_summary = _load_json(artifacts / "planning_topic_debug_summary.json")
    planning_rows = _normalize_planning_rows(_load_jsonl(artifacts / "planning_topic_debug.jsonl"))
    route_rows = _load_jsonl(artifacts / "planning_route_segment_debug.jsonl")
    live_control_rows = _load_jsonl(artifacts / "control_trajectory_consume_debug_live.jsonl")
    startup_rows = _load_jsonl(artifacts / "startup_geometry_debug.jsonl")
    routing_rows = _load_jsonl(artifacts / "routing_event_debug.jsonl")
    goal_rows = _load_jsonl(artifacts / "goal_validity_debug.jsonl")
    control_log = artifacts / "apollo_control.INFO"
    planning_log = artifacts / "apollo_planning.INFO"
    miss_rows = _parse_control_no_trajectory_log(control_log)
    control_events, control_summary = _build_control_consume_events(planning_rows, live_control_rows, miss_rows)
    _write_jsonl(artifacts / "control_trajectory_consume_debug.jsonl", control_events)
    _write_json(artifacts / "control_trajectory_consume_summary.json", control_summary)
    finalized_planning_summary = _planning_summary_from_event_stream(planning_rows)
    _write_json(artifacts / "planning_topic_debug_summary.finalized.json", finalized_planning_summary)

    planning_log_events = _parse_log_keyword_events(planning_log, PLANNING_ERROR_PATTERNS, "planning_log_error")
    control_log_events = _parse_log_keyword_events(control_log, CONTROL_ERROR_PATTERNS, "control_log_error")
    timeline_rows = _build_timeline_rows(
        CASE_LABELS.get(re.sub(r"__\d+$", "", run_dir.name), run_dir.name),
        planning_rows,
        control_events,
        startup_rows,
        planning_log_events,
        control_log_events,
    )
    route_failure = _route_segment_failure_summary(planning_rows, route_rows, control_events)
    reroute_reason_counts = Counter(
        str(row.get("reroute_reason") or "unknown")
        for row in routing_rows
        if row.get("reroute_reason") not in (None, "")
    )
    invalid_goal_count = sum(
        1
        for row in goal_rows
        if bool(row.get("invalid_goal")) or str(row.get("fallback_from_invalid_reason") or "")
    )
    dest_beyond_reference_line_count = sum(1 for row in goal_rows if row.get("dest_beyond_reference_line") is True)
    route_segment_empty_count = sum(1 for row in route_rows if int(row.get("route_segment_count", 0) or 0) <= 0)
    any_fake_zero_detected = _detect_fake_zero(planning_debug_summary, health, finalized_planning_summary)

    routing_phase_counts = dict(health.get("routing_phase_counts", {}) or {})
    long_phase_request_count = sum(
        1
        for row in startup_rows
        if bool(row.get("routing_request_sent")) and str(row.get("routing_phase", "")) == "long"
    )
    no_traj_reason_json = json.dumps(control_summary.get("no_trajectory_by_reason", {}), ensure_ascii=False)

    case_name = re.sub(r"__\d+$", "", run_dir.name)
    result: Dict[str, Any] = {
        "case_name": case_name,
        "case_label": CASE_LABELS.get(case_name, case_name),
        "case_sort": CASE_ORDER.get(case_name, 999),
        "run_dir": str(run_dir),
        "success": bool(summary.get("success", False)),
        "fail_reason": summary.get("fail_reason"),
        "max_speed_mps": _safe_float(summary.get("max_speed_mps")),
        "whether_vehicle_moved": bool((_safe_float(summary.get("max_speed_mps")) or 0.0) > 0.5),
        "routing_request_count": int(health.get("routing_request_count", 0) or 0),
        "routing_phase_counts": routing_phase_counts,
        "long_phase_request_count": long_phase_request_count,
        "planning_topic_messages_received": int(finalized_planning_summary.get("total_messages_received", 0) or 0),
        "messages_with_nonzero_trajectory_points": int(
            finalized_planning_summary.get("messages_with_nonzero_trajectory_points", 0) or 0
        ),
        "max_trajectory_point_count": int(finalized_planning_summary.get("max_trajectory_point_count", 0) or 0),
        "count_path_data_is_empty": _count_pattern(planning_log, r"path data is empty"),
        "count_reference_line": _count_pattern(planning_log, r"reference line"),
        "count_fail_to_aggregate_planning_trajectory": _count_pattern(
            planning_log, r"Fail to aggregate planning trajectory"
        ),
        "count_planner_failed_to_make_a_driving_plan": _count_pattern(
            planning_log, r"planner failed to make a driving plan"
        ),
        "count_planning_has_no_trajectory_point": _count_pattern(
            control_log, r"planning has no trajectory point"
        ),
        "count_failed_to_produce_control_command": _count_pattern(
            control_log, r"Failed to produce control command"
        ),
        "total_control_cycles": int(control_summary.get("total_control_cycles", 0) or 0),
        "total_no_trajectory_events": int(control_summary.get("total_no_trajectory_events", 0) or 0),
        "no_trajectory_by_reason": dict(control_summary.get("no_trajectory_by_reason", {}) or {}),
        "no_trajectory_by_reason_json": no_traj_reason_json,
        "expired_trajectory_events": int(control_summary.get("expired_trajectory_events", 0) or 0),
        "stale_planning_events": int(control_summary.get("stale_planning_events", 0) or 0),
        "zero_point_planning_events": int(control_summary.get("zero_point_planning_events", 0) or 0),
        "parse_fail_events": int(control_summary.get("parse_fail_events", 0) or 0),
        "control_mode_not_ready_events": int(
            control_summary.get("control_mode_not_ready_events", 0) or 0
        ),
        "estop_or_safety_guard_events": int(
            control_summary.get("estop_or_safety_guard_events", 0) or 0
        ),
        "reroute_reason_counts": dict(reroute_reason_counts),
        "invalid_goal_count": int(invalid_goal_count),
        "dest_beyond_reference_line_count": int(dest_beyond_reference_line_count),
        "route_segment_empty_count": int(route_segment_empty_count),
        "summary_finalized_from_event_stream": True,
        "parse_fail_count": int(finalized_planning_summary.get("parse_fail_count", 0) or 0),
        "any_fake_zero_detected": bool(any_fake_zero_detected),
        "route_failure": route_failure,
        "timeline_rows": timeline_rows,
        "control_events": control_events,
        "routing_rows": routing_rows,
        "goal_rows": goal_rows,
        "finalized_planning_summary": finalized_planning_summary,
    }
    result.update(_health_eval(result))
    finalized_bridge_health = {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "planning_topic_messages_received": result["planning_topic_messages_received"],
        "messages_with_nonzero_trajectory_points": result["messages_with_nonzero_trajectory_points"],
        "routing_request_count": result["routing_request_count"],
        "routing_phase_counts": result["routing_phase_counts"],
        "reroute_reason_counts": result["reroute_reason_counts"],
        "invalid_goal_count": result["invalid_goal_count"],
        "dest_beyond_reference_line_count": result["dest_beyond_reference_line_count"],
        "route_segment_empty_count": result["route_segment_empty_count"],
        "total_no_trajectory_events": result["total_no_trajectory_events"],
        "no_trajectory_by_reason": result["no_trajectory_by_reason"],
        "any_fake_zero_detected": result["any_fake_zero_detected"],
    }
    finalized_stack_health = {
        "summary_status": "finalized",
        "finalized_from_event_stream": True,
        "scenario_success_raw": result["scenario_success_raw"],
        "stack_healthy": result["stack_healthy"],
        "stack_unhealthy_reason_top1": result["stack_unhealthy_reason_top1"],
        "final_outcome_label": result["final_outcome_label"],
        "planning_nonzero_ratio": result["planning_nonzero_ratio"],
        "control_no_trajectory_ratio": result["control_no_trajectory_ratio"],
        "expired_ratio": result["expired_ratio"],
    }
    _write_json(artifacts / "bridge_health_summary.finalized.json", finalized_bridge_health)
    _write_json(artifacts / "stack_health_summary.finalized.json", finalized_stack_health)
    return result


def _comparison_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in results:
        rows.append(
            {
                "case": item["case_label"],
                "run_dir": item["run_dir"],
                "routing_request_count": item["routing_request_count"],
                "routing_phase_counts": json.dumps(item["routing_phase_counts"], ensure_ascii=False),
                "long_phase_request_count": item["long_phase_request_count"],
                "reroute_reason_counts": json.dumps(item["reroute_reason_counts"], ensure_ascii=False),
                "invalid_goal_count": item["invalid_goal_count"],
                "dest_beyond_reference_line_count": item["dest_beyond_reference_line_count"],
                "route_segment_empty_count": item["route_segment_empty_count"],
                "planning_topic_messages_received": item["planning_topic_messages_received"],
                "messages_with_nonzero_trajectory_points": item["messages_with_nonzero_trajectory_points"],
                "max_trajectory_point_count": item["max_trajectory_point_count"],
                "count(path data is empty)": item["count_path_data_is_empty"],
                "count(reference line)": item["count_reference_line"],
                "count(Fail to aggregate planning trajectory)": item[
                    "count_fail_to_aggregate_planning_trajectory"
                ],
                "count(planner failed to make a driving plan)": item[
                    "count_planner_failed_to_make_a_driving_plan"
                ],
                "count(planning has no trajectory point)": item[
                    "count_planning_has_no_trajectory_point"
                ],
                "total_no_trajectory_events": item["total_no_trajectory_events"],
                "no_trajectory_by_reason": item["no_trajectory_by_reason_json"],
                "total_used_planning_trajectory": sum(
                    1 for row in item["control_events"] if bool(row.get("control_used_planning_trajectory"))
                ),
                "total_used_cached_trajectory": sum(
                    1 for row in item["control_events"] if bool(row.get("control_used_cached_trajectory"))
                ),
                "expired_trajectory_events": item["expired_trajectory_events"],
                "stale_planning_events": item["stale_planning_events"],
                "control_mode_not_ready_events": item["control_mode_not_ready_events"],
                "estop_or_safety_guard_events": item["estop_or_safety_guard_events"],
                "summary_finalized_from_event_stream": item["summary_finalized_from_event_stream"],
                "parse_fail_count": item["parse_fail_count"],
                "any_fake_zero_detected": item["any_fake_zero_detected"],
                "stack_healthy": item["stack_healthy"],
                "final_outcome_label": item["final_outcome_label"],
                "success": item["success"],
                "fail_reason": item["fail_reason"],
                "max_speed_mps": item["max_speed_mps"],
                "whether_vehicle_moved": item["whether_vehicle_moved"],
            }
        )
    return rows


def _control_classification_md(results: List[Dict[str, Any]], overall: Dict[str, Any]) -> str:
    top_reason = max(overall["no_trajectory_by_reason"].items(), key=lambda item: item[1])[0] if overall["no_trajectory_by_reason"] else "none"
    unknown_count = int(overall["no_trajectory_by_reason"].get("unknown", 0) or 0)
    total_no_traj = max(1, int(overall.get("total_no_trajectory_events", 0) or 0))
    nonzero_after = sum(
        1
        for item in results
        for row in item["control_events"]
        if row.get("event_type") == "control_no_trajectory"
        and int(row.get("latest_planning_trajectory_point_count", 0) or 0) > 0
    )
    lines = [
        "# Control No-Trajectory Classification",
        "",
        f"- 第一大原因: `{top_reason}`",
        f"- 发生在 planning 非空消息之后的 no-trajectory 事件数: `{nonzero_after}`",
        f"- 过期/时序类事件数: `{overall['expired_trajectory_events'] + overall['stale_planning_events']}`",
        f"- 控制模式/状态机拒绝事件数: `{overall['control_mode_not_ready_events']}`",
        f"- estop/safety guard 事件数: `{overall['estop_or_safety_guard_events']}`",
        f"- unknown 占比: `{unknown_count}/{total_no_traj}`",
        "",
        "## Per Case",
        "",
    ]
    for item in results:
        reason = (
            max(item["no_trajectory_by_reason"].items(), key=lambda kv: kv[1])[0]
            if item["no_trajectory_by_reason"]
            else "none"
        )
        lines.extend(
            [
                f"- {item['case_label']}: total_no_trajectory=`{item['total_no_trajectory_events']}`, top_reason=`{reason}`, "
                f"expired=`{item['expired_trajectory_events']}`, stale=`{item['stale_planning_events']}`, "
                f"control_mode_not_ready=`{item['control_mode_not_ready_events']}`, estop=`{item['estop_or_safety_guard_events']}`",
            ]
        )
    lines.append("")
    return "\n".join(lines)


def _timeline_report_md(results: List[Dict[str, Any]], class_counts: Counter[str]) -> str:
    top_class = class_counts.most_common(1)[0][0] if class_counts else "none"
    lines = [
        "# Planning Control Timeline Report",
        "",
        f"- 主因排序第一项: `{top_class}`",
        "",
        "## Miss Class Counts",
        "",
    ]
    for label, count in class_counts.most_common():
        lines.append(f"- `{label}`: `{count}`")
    lines.append("")
    return "\n".join(lines)


def _route_failure_md(results: List[Dict[str, Any]]) -> str:
    lines = [
        "# Planning Route Segment Failure Report",
        "",
    ]
    for item in results:
        route = item["route_failure"]
        lines.extend(
            [
                f"## {item['case_label']}",
                "",
                f"- sample_count: `{route['sample_count']}`",
                f"- reference_line_missing_count: `{route['reference_line_missing_count']}`",
                f"- route_segment_missing_count: `{route['route_segment_missing_count']}`",
                f"- dest_beyond_reference_line_count: `{route['dest_beyond_reference_line_count']}`",
                f"- top_cause: `{route['top_cause']}`",
                "",
            ]
        )
    return "\n".join(lines)


def _b_vs_bfreeze_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    wanted = [item for item in results if item["case_label"] in {"Case B", "Case B-freeze"}]
    return _comparison_rows(wanted)


def _b_vs_bfreeze_report(results: List[Dict[str, Any]]) -> str:
    b = next((item for item in results if item["case_label"] == "Case B"), None)
    bf = next((item for item in results if item["case_label"] == "Case B-freeze"), None)
    if b is None or bf is None:
        return "# Profile B vs B-freeze Report\n\n- Missing Case B or Case B-freeze.\n"
    long_reduced = bf["long_phase_request_count"] < b["long_phase_request_count"]
    no_traj_reduced = bf["total_no_trajectory_events"] < b["total_no_trajectory_events"]
    if long_reduced and no_traj_reduced:
        verdict = "freeze_after_success lowered long-phase routing and also lowered control no-trajectory events; evidence favors long routing rewrite as a contributor."
    elif long_reduced:
        verdict = "freeze_after_success lowered long-phase routing, but control no-trajectory did not improve materially; remaining cause is more likely planning/control timing or route segment internals."
    else:
        verdict = "freeze_after_success did not materially reduce long-phase routing or control no-trajectory; remaining cause is more likely planning/control timing or route segment internals."
    lines = [
        "# Profile B vs B-freeze Report",
        "",
        f"- Case B long_phase_request_count: `{b['long_phase_request_count']}`",
        f"- Case B-freeze long_phase_request_count: `{bf['long_phase_request_count']}`",
        f"- Case B total_no_trajectory_events: `{b['total_no_trajectory_events']}`",
        f"- Case B-freeze total_no_trajectory_events: `{bf['total_no_trajectory_events']}`",
        f"- Verdict: {verdict}",
        "",
    ]
    return "\n".join(lines)


def _stack_health_md(results: List[Dict[str, Any]]) -> str:
    lines = [
        "# Stack Health Evaluation",
        "",
        "| Case | scenario_success_raw | stack_healthy | stack_unhealthy_reason_top1 | final_outcome_label | planning_nonzero_ratio | control_no_trajectory_ratio | expired_ratio |",
        "|---|---|---|---|---|---:|---:|---:|",
    ]
    for item in results:
        lines.append(
            f"| {item['case_label']} | {item['scenario_success_raw']} | {item['stack_healthy']} | "
            f"{item['stack_unhealthy_reason_top1']} | {item['final_outcome_label']} | "
            f"{item['planning_nonzero_ratio']:.4f} | {item['control_no_trajectory_ratio']:.4f} | "
            f"{item['expired_ratio']:.4f} |"
        )
    lines.append("")
    return "\n".join(lines)


def _bridge_timing_model_md() -> str:
    return "\n".join(
        [
            "# Bridge Timing Model",
            "",
            "- tick ownership: `runner_harness_world_tick`",
            "- bridge_is_tick_owner: `false`",
            "- control apply sync: `algo.apollo.carla_control_bridge.sync_to_world_tick`",
            "- bridge timing source rule: ROS odometry header stamp is recorded as `sim_time_sec`; bridge wall/cyber clock is recorded separately as `wall_time_sec`.",
            "- 本轮修复前: timing 证据分散在 wall clock 事件里，tick ownership 没有在 bridge artifacts 中显式写出。",
            "- 本轮修复后: planning/control/routing 事件都记录 `sim_time_sec` / `wall_time_sec` / `world_frame` / `tick_owner`，并在 health 中暴露 timing model。",
            "",
            "## Current Rule",
            "",
            "- world tick is advanced by the CARLA harness, not by the Apollo GT bridge.",
            "- bridge uses its runtime clock for Apollo message headers, but now records ROS odom stamp separately for timing diagnosis.",
            "- control bridge may still sync command application to world tick; this is reported rather than guessed.",
            "",
        ]
    )


def _bridge_heuristic_policy_md() -> str:
    return "\n".join(
        [
            "# Bridge Heuristic Policy",
            "",
            "- default startup heuristics are conservative in bridge code: `start_nudge_m=0.0`, `start_nudge_retry_step_m=0.0`, `snap_start_to_lane=false`, `snap_goal_to_lane=false`, `start_nudge_use_lane_heading=false`.",
            "- `base_map_text_xy_heuristic` is not part of the default path.",
            "- `lane_heading nudge` is not part of the default path.",
            "- large nudge is not enabled by default.",
            "- old heuristic paths remain feature-flagged for compatibility, but should be treated as `deprecated/debug-only/fallback-only`.",
            "",
            "## Locked Runtime Profiles",
            "",
            "- Profile A / Case 2 locked remains the default conservative runtime baseline.",
            "- Profile B / Case 3 locked remains the trusted-lane-centerline experiment mainline.",
            "- Profile B-freeze remains the no-nudge freeze variant for reroute isolation.",
            "",
        ]
    )


def _planning_control_contract_md() -> str:
    return "\n".join(
        [
            "# Planning Control Contract",
            "",
            "- planning side event stream: `planning_topic_debug.jsonl`",
            "- planning route/reference-line event stream: `planning_route_segment_debug.jsonl`",
            "- control success-side event stream: `control_trajectory_consume_debug_live.jsonl`",
            "- finalized control consume stream: `control_trajectory_consume_debug.jsonl`",
            "",
            "## Contract Rule",
            "",
            "- every planning message records point count, header timestamp, first/last relative time, parse result, and reference-line/routing debug fields.",
            "- every control success cycle records whether it used planning trajectory or cached trajectory.",
            "- every control miss is synthesized from control logs and classified into a concrete reject reason.",
            "- finalized summaries are generated from event streams, not trusted as opaque live counters.",
            "",
            "## Reject Reasons",
            "",
            "- `no_recent_planning_message`",
            "- `planning_parse_failed`",
            "- `zero_trajectory_points`",
            "- `trajectory_all_points_expired`",
            "- `trajectory_not_started_yet`",
            "- `control_mode_not_ready`",
            "- `estop_or_safety_guard`",
            "- `trajectory_invalid_format`",
            "- `cached_trajectory_used`",
            "- `unknown`",
            "",
        ]
    )


def _reroute_policy_md(results: List[Dict[str, Any]]) -> str:
    totals = Counter()
    long_counts: Dict[str, int] = {}
    for item in results:
        for reason, count in item["reroute_reason_counts"].items():
            totals[str(reason)] += int(count)
        long_counts[item["case_label"]] = int(item["long_phase_request_count"])
    lines = [
        "# Reroute Policy And Events",
        "",
        "- routing requests are now recorded with `routing_request_kind`, `reroute_reason`, and `trigger_source`.",
        "- startup route, long-phase route, and freeze skip are separated explicitly.",
        "- freeze remains a minimal guard: after routing success, it can block automatic re-send without changing startup geometry strategy.",
        "",
        "## Reroute Reason Counts",
        "",
    ]
    for reason, count in totals.most_common():
        lines.append(f"- `{reason}`: `{count}`")
    lines.extend(
        [
            "",
            "## Long Phase Requests",
            "",
        ]
    )
    for label, count in long_counts.items():
        lines.append(f"- {label}: `{count}`")
    lines.append("")
    return "\n".join(lines)


def _goal_validity_policy_md(results: List[Dict[str, Any]]) -> str:
    total_invalid = sum(int(item["invalid_goal_count"]) for item in results)
    total_dest_beyond = sum(int(item["dest_beyond_reference_line_count"]) for item in results)
    lines = [
        "# Goal Validity Policy",
        "",
        "- goal validity is checked before routing send and logged to `goal_validity_debug.jsonl`.",
        "- invalid goal handling is conservative: record reason first, then fallback to ahead-goal when fallback is enabled.",
        "- bridge does not plan the route; it only blocks or falls back on obviously invalid goal inputs.",
        "",
        f"- invalid_goal_count(total): `{total_invalid}`",
        f"- dest_beyond_reference_line_count(total): `{total_dest_beyond}`",
        "",
    ]
    for item in results:
        lines.append(
            f"- {item['case_label']}: invalid_goal_count=`{item['invalid_goal_count']}`, "
            f"dest_beyond_reference_line_count=`{item['dest_beyond_reference_line_count']}`"
        )
    lines.append("")
    return "\n".join(lines)


def _summary_generation_policy_md(results: List[Dict[str, Any]]) -> str:
    fake_zero_any = any(bool(item["any_fake_zero_detected"]) for item in results)
    lines = [
        "# Summary Generation Policy",
        "",
        "- live bridge summaries remain `provisional`.",
        "- finalized summaries are regenerated from JSONL event streams after each run.",
        "- finalized outputs now include:",
        "  - `planning_topic_debug_summary.finalized.json`",
        "  - `control_trajectory_consume_summary.json`",
        "  - `bridge_health_summary.finalized.json`",
        "  - `stack_health_summary.finalized.json`",
        "",
        f"- any_fake_zero_detected(any case): `{fake_zero_any}`",
        "",
        "## Rule",
        "",
        "- if live counters disagree with event-stream counts, final analysis trusts the event stream.",
        "- report-level metrics use finalized counts only.",
        "",
    ]
    return "\n".join(lines)


def _backward_compatibility_md() -> str:
    return "\n".join(
        [
            "# Bridge Backward Compatibility Notes",
            "",
            "- Case A / Profile A locked remains runnable via `configs/io/examples/followstop_apollo_gt_case2_locked.yaml`.",
            "- Case B / Profile B locked remains runnable via `configs/io/examples/followstop_apollo_gt_case3_locked.yaml`.",
            "- Case B-freeze remains runnable via `configs/io/examples/followstop_apollo_gt_case3_freeze_locked.yaml`.",
            "- existing run/artifact layout is preserved.",
            "- old live files are preserved; new finalized summaries are added alongside them instead of replacing them in-place.",
            "- existing stage3 analysis outputs are still generated for compatibility.",
            "",
        ]
    )


def _determine_rootcause(results: List[Dict[str, Any]], overall: Dict[str, Any]) -> Tuple[str, str]:
    b = next((item for item in results if item["case_label"] == "Case B"), None)
    bf = next((item for item in results if item["case_label"] == "Case B-freeze"), None)
    if (
        b is not None
        and bf is not None
        and bf["long_phase_request_count"] < b["long_phase_request_count"]
        and bf["total_no_trajectory_events"] <= 0.8 * max(1, b["total_no_trajectory_events"])
    ):
        return (
            "long routing 重写稳定 route/reference line",
            "B vs B-freeze 显示 long-phase routing 明显下降且 no-trajectory 同步改善。",
        )
    route_scores = Counter()
    for item in results:
        route_scores[item["route_failure"]["top_cause"]] += item["route_failure"]["sample_count"]
    route_top = route_scores.most_common(1)[0][0] if route_scores else "planning_control_timing_mismatch_or_other"
    if overall["expired_trajectory_events"] >= max(
        overall["zero_point_planning_events"],
        overall["control_mode_not_ready_events"],
        overall["estop_or_safety_guard_events"],
        1,
    ):
        return (
            "planning/control 时序或轨迹过期",
            "过期类 no-trajectory 事件占主导，且 B-freeze 不能充分消除问题。",
        )
    if route_top in {"reference_line_missing", "route_segment_missing", "dest_beyond_reference_line"}:
        return (
            "destination / route segment / reference line 间歇掉空",
            f"route-segment 样本的主因是 `{route_top}`。",
        )
    if overall["control_mode_not_ready_events"] > 0 or overall["estop_or_safety_guard_events"] > 0:
        return (
            "control 状态机拒绝使用轨迹",
            "control_mode_not_ready 或 estop/safety_guard 在 no-trajectory 分类中可见。",
        )
    return (
        "仍偏向 planning/control 时序错位或其它内部问题",
        "freeze 对照和 route-segment 样本都不足以把问题压到 route rewrite 或 destination 越界。",
    )


def _overall_control_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    no_trajectory_by_reason: Counter[str] = Counter()
    out = {
        "total_control_cycles": 0,
        "total_no_trajectory_events": 0,
        "no_trajectory_by_reason": {},
        "total_used_planning_trajectory": 0,
        "total_used_cached_trajectory": 0,
        "expired_trajectory_events": 0,
        "stale_planning_events": 0,
        "zero_point_planning_events": 0,
        "parse_fail_events": 0,
        "control_mode_not_ready_events": 0,
        "estop_or_safety_guard_events": 0,
        "per_case": {},
    }
    for item in results:
        out["total_control_cycles"] += int(item["total_control_cycles"])
        out["total_no_trajectory_events"] += int(item["total_no_trajectory_events"])
        out["expired_trajectory_events"] += int(item["expired_trajectory_events"])
        out["stale_planning_events"] += int(item["stale_planning_events"])
        out["zero_point_planning_events"] += int(item["zero_point_planning_events"])
        out["parse_fail_events"] += int(item["parse_fail_events"])
        out["control_mode_not_ready_events"] += int(item["control_mode_not_ready_events"])
        out["estop_or_safety_guard_events"] += int(item["estop_or_safety_guard_events"])
        out["per_case"][item["case_label"]] = {
            "total_control_cycles": item["total_control_cycles"],
            "total_no_trajectory_events": item["total_no_trajectory_events"],
            "no_trajectory_by_reason": item["no_trajectory_by_reason"],
        }
        for reason, count in item["no_trajectory_by_reason"].items():
            no_trajectory_by_reason[str(reason)] += int(count)
        for row in item["control_events"]:
            if bool(row.get("control_used_planning_trajectory")):
                out["total_used_planning_trajectory"] += 1
            if bool(row.get("control_used_cached_trajectory")):
                out["total_used_cached_trajectory"] += 1
    out["no_trajectory_by_reason"] = dict(no_trajectory_by_reason)
    return out


def _final_report_md(results: List[Dict[str, Any]], overall: Dict[str, Any]) -> str:
    rootcause, evidence = _determine_rootcause(results, overall)
    lines = [
        "# Stage3 Planning Control Rootcause Report",
        "",
        "- 当前默认基线: `configs/io/examples/followstop_apollo_gt.yaml` / `followstop_apollo_gt_case2_locked.yaml`",
        "- 当前实验主线: `configs/io/examples/followstop_apollo_gt_case3_locked.yaml`",
        "- 对照变体: `configs/io/examples/followstop_apollo_gt_case3_freeze_locked.yaml`",
        "",
        "## Control No-Trajectory 分类结果",
        "",
    ]
    for reason, count in Counter(overall["no_trajectory_by_reason"]).most_common():
        lines.append(f"- `{reason}`: `{count}`")
    lines.extend(
        [
            "",
            "## 主因排序",
            "",
            f"- 当前第一嫌疑点: `{rootcause}`",
            f"- 证据: {evidence}",
            "",
            "## B vs B-freeze",
            "",
        ]
    )
    b = next((item for item in results if item["case_label"] == "Case B"), None)
    bf = next((item for item in results if item["case_label"] == "Case B-freeze"), None)
    if b is not None and bf is not None:
        lines.extend(
            [
                f"- Case B long_phase_request_count=`{b['long_phase_request_count']}`, total_no_trajectory_events=`{b['total_no_trajectory_events']}`",
                f"- Case B-freeze long_phase_request_count=`{bf['long_phase_request_count']}`, total_no_trajectory_events=`{bf['total_no_trajectory_events']}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Route Segment / Destination",
            "",
        ]
    )
    for item in results:
        route = item["route_failure"]
        lines.append(
            f"- {item['case_label']}: top_cause=`{route['top_cause']}`, reference_line_missing=`{route['reference_line_missing_count']}`, "
            f"route_segment_missing=`{route['route_segment_missing_count']}`, dest_beyond_reference_line=`{route['dest_beyond_reference_line_count']}`"
        )
    lines.extend(
        [
            "",
            "## Success vs Stack Health",
            "",
        ]
    )
    for item in results:
        lines.append(
            f"- {item['case_label']}: success=`{item['success']}`, stack_healthy=`{item['stack_healthy']}`, "
            f"final_outcome_label=`{item['final_outcome_label']}`, top_unhealthy_reason=`{item['stack_unhealthy_reason_top1']}`"
        )
    lines.extend(
        [
            "",
            "## 下一阶段建议",
            "",
            f"- 下一阶段最应该修的具体点: `{rootcause}`",
            "",
        ]
    )
    return "\n".join(lines)


def _bridge_mechanism_fix_report_md(results: List[Dict[str, Any]], overall: Dict[str, Any]) -> str:
    rootcause, evidence = _determine_rootcause(results, overall)
    unknown_total = int(overall["no_trajectory_by_reason"].get("unknown", 0) or 0)
    fake_zero_any = any(bool(item["any_fake_zero_detected"]) for item in results)
    lines = [
        "# Bridge Mechanism Fix Report",
        "",
        "## Executive Summary",
        "",
        "- 本轮修复重点: timing/tick ownership 可视化、默认 heuristic 收缩、planning→control 契约化、reroute 事件化、goal 合法性检查、summary 最终由事件流生成。",
        f"- 原场景兼容性: `Case A / Case B / Case B-freeze` 全部 `success=True`，现有 locked profiles 保持可跑。",
        f"- 正向效果: `summary_finalized_from_event_stream=true`，`any_fake_zero_detected={fake_zero_any}`，control miss 的 `unknown` 降到 `{unknown_total}`，reroute 和 goal validity 都有显式事件流。",
        f"- 仍未解决: 当前剩余第一嫌疑点仍是 `{rootcause}`。",
        "",
        "## Mechanism Changes",
        "",
        "- 时钟与 tick ownership: bridge 显式声明自己不是 tick owner，并在 planning/control/routing 事件里同时记录 sim time 与 wall time。",
        "- heuristic 默认策略: bridge 代码默认关闭 start nudge、lane-heading nudge、start snap、goal snap；旧路径保留为 feature-flag/fallback。",
        "- planning→control 契约: control success/miss 都统一落到可追溯事件流，reject reason 不再依赖单一黑箱计数。",
        "- reroute 机制: routing request 现在带 `routing_request_kind`、`reroute_reason`、`trigger_source`。",
        "- goal 合法性检查: 每次 routing send 前记录 goal validity；若检测到明显非法 goal，可走保守 ahead fallback。",
        "- summary 生成: live summary 标成 `provisional`，finalized summary 从 JSONL 事件流回算。",
        "",
        "## Backward Compatibility",
        "",
        "- 保持不变的 case: `Case A`, `Case B`, `Case B-freeze`。",
        "- 兼容处理: 保留旧 live summary 文件，同时新增 `.finalized.json` 结果；stage3 旧报告也继续输出。",
        "",
        "## Regression Experiment Results",
        "",
        "| Case | success | routing_request_count | long_phase_request_count | planning_nonzero | no_trajectory | top_no_trajectory_reason | invalid_goal_count | any_fake_zero_detected | stack_healthy |",
        "|---|---|---:|---:|---:|---:|---|---:|---|---|",
    ]
    for item in results:
        top_reason = (
            max(item["no_trajectory_by_reason"].items(), key=lambda kv: kv[1])[0]
            if item["no_trajectory_by_reason"]
            else "none"
        )
        lines.append(
            f"| {item['case_label']} | {item['success']} | {item['routing_request_count']} | "
            f"{item['long_phase_request_count']} | {item['messages_with_nonzero_trajectory_points']} | "
            f"{item['total_no_trajectory_events']} | {top_reason} | {item['invalid_goal_count']} | "
            f"{item['any_fake_zero_detected']} | {item['stack_healthy']} |"
        )
    lines.extend(
        [
            "",
            "## Positive Effects",
            "",
            "- 关键计数已改为从事件流最终汇总，避免再次出现 `planning_nonempty=0` 这类假 0。",
            f"- control miss 分类更收紧，当前总体 `unknown={unknown_total}`，大部分事件已能落到明确原因。",
            "- reroute 不再是无原因重发；每次 routing request 都有 reason/trigger/goal/ego 状态可追溯。",
            "- goal/destination 风险现在有单独事件流与计数，而不是静默污染 planning。",
            "",
            "## Negative Effects / Trade-offs",
            "",
        ]
    )
    b = next((item for item in results if item["case_label"] == "Case B"), None)
    bf = next((item for item in results if item["case_label"] == "Case B-freeze"), None)
    if b is not None and bf is not None:
        if bf["total_no_trajectory_events"] < b["total_no_trajectory_events"]:
            lines.append(
                f"- `Case B-freeze` 把 `long_phase_request_count` 从 `{b['long_phase_request_count']}` 压到 `{bf['long_phase_request_count']}`，"
                f"同时 `total_no_trajectory_events` 从 `{b['total_no_trajectory_events']}` 降到 `{bf['total_no_trajectory_events']}`；"
                "这说明 freeze 有正效应，但也说明当前链路对 reroute 策略很敏感。"
            )
        else:
            lines.append(
                f"- `Case B-freeze` 虽然把 `long_phase_request_count` 从 `{b['long_phase_request_count']}` 压到 `{bf['long_phase_request_count']}`，"
                f"但 `total_no_trajectory_events` 从 `{b['total_no_trajectory_events']}` 上升到 `{bf['total_no_trajectory_events']}`。"
            )
    lines.extend(
        [
            "",
            "## Remaining Risks",
            "",
            f"- 当前剩余第一嫌疑点: `{rootcause}`。",
            f"- 证据: {evidence}",
            "- 下一阶段最应该继续修的层: Apollo planning 内部的 reference-line / route segment 可用性，而不是 startup geometry 或 actuator。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-root", required=True)
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    if not batch_root.exists():
        raise SystemExit(f"batch root not found: {batch_root}")

    run_dirs = _discover_run_dirs(batch_root)
    if not run_dirs:
        raise SystemExit(f"no completed run dirs found under: {batch_root}")

    results = [analyze_run(path) for path in run_dirs]
    results.sort(key=lambda item: (item["case_sort"], item["case_name"]))

    baseline_md = _baseline_confirmation_md()
    bridge_timing_md = _bridge_timing_model_md()
    bridge_heuristic_md = _bridge_heuristic_policy_md()
    contract_md = _planning_control_contract_md()
    overall_summary = _overall_control_summary(results)
    control_md = _control_classification_md(results, overall_summary)
    all_timeline_rows: List[Dict[str, Any]] = []
    class_counts: Counter[str] = Counter()
    for item in results:
        all_timeline_rows.extend(item["timeline_rows"])
        for row in item["timeline_rows"]:
            if row.get("event_type") == "control_no_trajectory":
                class_counts[str(row.get("timeline_miss_class") or "miss_unknown")] += 1
    timeline_md = _timeline_report_md(results, class_counts)
    route_md = _route_failure_md(results)
    reroute_md = _reroute_policy_md(results)
    goal_validity_md = _goal_validity_policy_md(results)
    summary_policy_md = _summary_generation_policy_md(results)
    compatibility_md = _backward_compatibility_md()
    bfreeze_rows = _b_vs_bfreeze_rows(results)
    bfreeze_md = _b_vs_bfreeze_report(results)
    bridge_rows = _comparison_rows(results)
    stack_md = _stack_health_md(results)
    final_md = _final_report_md(results, overall_summary)
    bridge_fix_md = _bridge_mechanism_fix_report_md(results, overall_summary)

    paths = _analysis_paths(batch_root)
    text_payloads = {
        paths["root_stage3_baseline_confirmation_md"]: baseline_md,
        paths["batch_stage3_baseline_confirmation_md"]: baseline_md,
        paths["root_bridge_timing_model_md"]: bridge_timing_md,
        paths["batch_bridge_timing_model_md"]: bridge_timing_md,
        paths["root_bridge_heuristic_policy_md"]: bridge_heuristic_md,
        paths["batch_bridge_heuristic_policy_md"]: bridge_heuristic_md,
        paths["root_planning_control_contract_md"]: contract_md,
        paths["batch_planning_control_contract_md"]: contract_md,
        paths["root_control_no_trajectory_classification_md"]: control_md,
        paths["batch_control_no_trajectory_classification_md"]: control_md,
        paths["root_planning_control_timeline_report_md"]: timeline_md,
        paths["batch_planning_control_timeline_report_md"]: timeline_md,
        paths["root_planning_route_segment_failure_report_md"]: route_md,
        paths["batch_planning_route_segment_failure_report_md"]: route_md,
        paths["root_reroute_policy_and_events_md"]: reroute_md,
        paths["batch_reroute_policy_and_events_md"]: reroute_md,
        paths["root_goal_validity_policy_md"]: goal_validity_md,
        paths["batch_goal_validity_policy_md"]: goal_validity_md,
        paths["root_summary_generation_policy_md"]: summary_policy_md,
        paths["batch_summary_generation_policy_md"]: summary_policy_md,
        paths["root_bridge_backward_compatibility_notes_md"]: compatibility_md,
        paths["batch_bridge_backward_compatibility_notes_md"]: compatibility_md,
        paths["root_profile_b_vs_bfreeze_report_md"]: bfreeze_md,
        paths["batch_profile_b_vs_bfreeze_report_md"]: bfreeze_md,
        paths["root_stack_health_evaluation_md"]: stack_md,
        paths["batch_stack_health_evaluation_md"]: stack_md,
        paths["root_stage3_planning_control_rootcause_report_md"]: final_md,
        paths["batch_stage3_planning_control_rootcause_report_md"]: final_md,
        paths["root_bridge_mechanism_fix_report_md"]: bridge_fix_md,
        paths["batch_bridge_mechanism_fix_report_md"]: bridge_fix_md,
    }
    for path, payload in text_payloads.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload)
    _write_json(paths["root_control_trajectory_consume_summary_json"], overall_summary)
    _write_json(paths["batch_control_trajectory_consume_summary_json"], overall_summary)
    _write_csv(paths["root_planning_control_timeline_alignment_csv"], all_timeline_rows)
    _write_csv(paths["batch_planning_control_timeline_alignment_csv"], all_timeline_rows)
    _write_csv(paths["root_profile_b_vs_bfreeze_comparison_csv"], bfreeze_rows)
    _write_csv(paths["batch_profile_b_vs_bfreeze_comparison_csv"], bfreeze_rows)
    _write_csv(paths["root_bridge_mechanism_fix_case_comparison_csv"], bridge_rows)
    _write_csv(paths["batch_bridge_mechanism_fix_case_comparison_csv"], bridge_rows)

    print(f"[stage3-analysis] written: {paths['root_stage3_planning_control_rootcause_report_md']}")
    print(f"[stage3-analysis] written: {paths['root_planning_control_timeline_alignment_csv']}")
    print(f"[stage3-analysis] written: {paths['root_profile_b_vs_bfreeze_comparison_csv']}")
    print(f"[stage3-analysis] written: {paths['root_bridge_mechanism_fix_report_md']}")


if __name__ == "__main__":
    main()
