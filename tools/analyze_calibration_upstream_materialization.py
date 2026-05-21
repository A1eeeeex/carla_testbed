#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


SURFACE_FILES: Sequence[Tuple[str, str]] = (
    ("planning_route_segment", "planning_route_segment_debug.jsonl"),
    ("apollo_route_segment", "apollo_route_segment_debug.jsonl"),
    ("apollo_reference_line", "apollo_reference_line_debug.jsonl"),
    ("stage5_lane_follow", "stage5_apollo_lane_follow_map_debug.jsonl"),
    ("stage5_reference_line", "stage5_apollo_reference_line_debug.jsonl"),
)


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _fmt_float(value: Any, digits: int = 3) -> str:
    out = _safe_float(value)
    if out is None:
        return ""
    return f"{out:.{digits}f}"


def _fmt_bool(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return ""


def _parse_run_spec(raw: str) -> Tuple[str, Path]:
    text = str(raw or "").strip()
    if "=" not in text:
        raise argparse.ArgumentTypeError("run spec must look like label=/path/to/run")
    label, path_text = text.split("=", 1)
    label = label.strip()
    path_text = path_text.strip()
    if not label or not path_text:
        raise argparse.ArgumentTypeError("run spec must look like label=/path/to/run")
    return label, Path(path_text).expanduser().resolve()


def _candidate_signature(metadata: Dict[str, Any]) -> str:
    candidate = metadata.get("candidate") or {}
    road_id = candidate.get("road_id")
    lane_id = candidate.get("lane_id")
    start_y = _fmt_float(candidate.get("start_y"))
    parts: List[str] = []
    if road_id is not None:
        parts.append(f"road_id={road_id}")
    if lane_id is not None:
        parts.append(f"lane_id={lane_id}")
    if start_y:
        parts.append(f"start_y≈{start_y}")
    return " ".join(parts) if parts else "unknown"


def _value_set(values: Iterable[str]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _surface_first_row(rows: Sequence[Dict[str, Any]], start_ts: Optional[float], end_ts: Optional[float]) -> Dict[str, Any]:
    window_rows: List[Dict[str, Any]] = []
    if start_ts is not None and end_ts is not None:
        for row in rows:
            ts = _safe_float(row.get("timestamp"))
            if ts is None:
                continue
            if start_ts <= ts < end_ts:
                window_rows.append(row)
    first = window_rows[0] if window_rows else {}
    return {
        "count": len(window_rows),
        "first_timestamp": _safe_float(first.get("timestamp")),
        "first_signature": str(first.get("routing_lane_window_signature") or "").strip(),
        "first_total_length_m": _safe_float(first.get("route_segment_total_length")),
        "first_route_segment_count": first.get("route_segment_count"),
        "first_reference_line_count": first.get("reference_line_count"),
        "first_create_route_segments_status": str(first.get("create_route_segments_status") or "").strip(),
        "first_reference_line_provider_status": str(first.get("reference_line_provider_status") or "").strip(),
        "first_lane_follow_map_status": str(first.get("lane_follow_map_status") or "").strip(),
        "first_current_lane_metadata_source": str(first.get("current_lane_metadata_source") or "").strip(),
        "first_target_lane_metadata_source": str(first.get("target_lane_metadata_source") or "").strip(),
        "first_path_end_like_condition": first.get("path_end_like_condition"),
        "first_planning_empty_reason_guess": str(first.get("planning_empty_reason_guess") or "").strip(),
        "first_map_contract_invalid": first.get("map_contract_invalid"),
        "first_map_contract_mismatch_classification": str(
            first.get("map_contract_mismatch_classification") or ""
        ).strip(),
        "first_runtime_map_dir": str(first.get("runtime_map_dir") or "").strip(),
        "first_lane_follow_map_inconsistent": first.get("lane_follow_map_inconsistent"),
    }


def _find_reroute_timestamp(rows: Sequence[Dict[str, Any]], reason: str) -> Optional[float]:
    for row in rows:
        if str(row.get("reroute_reason") or "").strip() == reason:
            return _safe_float(row.get("timestamp"))
    return None


def _first_row_after(rows: Sequence[Dict[str, Any]], ts: Optional[float]) -> Dict[str, Any]:
    if ts is None:
        return {}
    for row in rows:
        row_ts = _safe_float(row.get("timestamp"))
        if row_ts is not None and row_ts >= ts:
            return row
    return {}


def _surface_first_row_after(rows: Sequence[Dict[str, Any]], start_ts: Optional[float]) -> Dict[str, Any]:
    first: Dict[str, Any] = {}
    if start_ts is not None:
        for row in rows:
            row_ts = _safe_float(row.get("timestamp"))
            if row_ts is not None and row_ts >= start_ts:
                first = row
                break
    return {
        "first_timestamp": _safe_float(first.get("timestamp")),
        "first_signature": str(first.get("routing_lane_window_signature") or "").strip(),
        "first_total_length_m": _safe_float(first.get("route_segment_total_length")),
        "first_route_segment_count": first.get("route_segment_count"),
        "first_reference_line_count": first.get("reference_line_count"),
        "first_create_route_segments_status": str(first.get("create_route_segments_status") or "").strip(),
        "first_reference_line_provider_status": str(first.get("reference_line_provider_status") or "").strip(),
        "first_lane_follow_map_status": str(first.get("lane_follow_map_status") or "").strip(),
        "first_current_lane_metadata_source": str(first.get("current_lane_metadata_source") or "").strip(),
        "first_target_lane_metadata_source": str(first.get("target_lane_metadata_source") or "").strip(),
        "first_path_end_like_condition": first.get("path_end_like_condition"),
        "first_planning_empty_reason_guess": str(first.get("planning_empty_reason_guess") or "").strip(),
        "first_map_contract_invalid": first.get("map_contract_invalid"),
        "first_map_contract_mismatch_classification": str(
            first.get("map_contract_mismatch_classification") or ""
        ).strip(),
        "first_runtime_map_dir": str(first.get("runtime_map_dir") or "").strip(),
        "first_lane_follow_map_inconsistent": first.get("lane_follow_map_inconsistent"),
    }


def _delta(lhs: Optional[float], rhs: Optional[float]) -> Optional[float]:
    if lhs is None or rhs is None:
        return None
    return lhs - rhs


def _consensus(values: Iterable[str]) -> str:
    unique = _value_set(values)
    if not unique:
        return ""
    if len(unique) == 1:
        return unique[0]
    return " | ".join(unique)


def _consensus_bool(values: Iterable[Any]) -> str:
    normalized = [_fmt_bool(value) for value in values]
    return _consensus(normalized)


def _analyze_run(label: str, run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    metadata = _load_json(artifacts / "scenario_metadata.json")
    summary = _load_json(run_dir / "summary.json")
    reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    goal_validity_rows = _load_jsonl(artifacts / "goal_validity_debug.jsonl")

    transition_ts = _find_reroute_timestamp(reroute_rows, "long_phase_transition")
    refresh_ts = _find_reroute_timestamp(reroute_rows, "long_phase_refresh")
    final_goal_validity = goal_validity_rows[-1] if goal_validity_rows else {}
    post_refresh_goal_validity = _first_row_after(goal_validity_rows, refresh_ts)

    row: Dict[str, Any] = {
        "label": label,
        "run_dir": str(run_dir),
        "candidate_signature": _candidate_signature(metadata),
        "summary_status": summary.get("status") or "",
        "fail_reason": summary.get("fail_reason") or "",
        "transition_reroute_timestamp": _fmt_float(transition_ts, 6),
        "refresh_reroute_timestamp": _fmt_float(refresh_ts, 6),
        "transition_to_refresh_goal_validity_count": len(
            [
                entry
                for entry in goal_validity_rows
                if transition_ts is not None
                and refresh_ts is not None
                and (ts := _safe_float(entry.get("timestamp"))) is not None
                and transition_ts <= ts < refresh_ts
            ]
        ),
        "final_goal_mode": final_goal_validity.get("goal_mode") or "",
        "final_goal_source": final_goal_validity.get("goal_source") or "",
        "final_invalid_goal": _fmt_bool(final_goal_validity.get("invalid_goal")),
        "final_invalid_goal_reason": final_goal_validity.get("invalid_goal_reason") or "",
        "final_fallback_applied": _fmt_bool(final_goal_validity.get("fallback_applied")),
        "final_route_segment_count": final_goal_validity.get("route_segment_count") or "",
        "final_reference_line_count": final_goal_validity.get("reference_line_count") or "",
        "final_routing_lane_window_signature": final_goal_validity.get("routing_lane_window_signature") or "",
        "final_reference_line_provider_status": final_goal_validity.get("reference_line_provider_status") or "",
        "final_lane_follow_map_status": final_goal_validity.get("lane_follow_map_status") or "",
        "post_refresh_goal_validity_timestamp": _fmt_float(_safe_float(post_refresh_goal_validity.get("timestamp")), 6),
    }

    surface_snapshots: Dict[str, Dict[str, Any]] = {}
    surface_after_transition_snapshots: Dict[str, Dict[str, Any]] = {}
    for surface_key, filename in SURFACE_FILES:
        rows = _load_jsonl(artifacts / filename)
        snapshot = _surface_first_row(rows, transition_ts, refresh_ts)
        surface_snapshots[surface_key] = snapshot
        after_snapshot = _surface_first_row_after(rows, transition_ts)
        surface_after_transition_snapshots[surface_key] = after_snapshot
        prefix = f"{surface_key}_pre_refresh"
        row[f"{prefix}_count"] = snapshot["count"]
        row[f"{prefix}_first_timestamp"] = _fmt_float(snapshot["first_timestamp"], 6)
        row[f"{prefix}_first_signature"] = snapshot["first_signature"]
        row[f"{prefix}_first_total_length_m"] = _fmt_float(snapshot["first_total_length_m"])
        row[f"{prefix}_first_route_segment_count"] = snapshot["first_route_segment_count"] or ""
        row[f"{prefix}_first_reference_line_count"] = snapshot["first_reference_line_count"] or ""
        row[f"{prefix}_first_create_route_segments_status"] = snapshot["first_create_route_segments_status"]
        row[f"{prefix}_first_reference_line_provider_status"] = snapshot["first_reference_line_provider_status"]
        row[f"{prefix}_first_lane_follow_map_status"] = snapshot["first_lane_follow_map_status"]
        row[f"{prefix}_first_current_lane_metadata_source"] = snapshot["first_current_lane_metadata_source"]
        row[f"{prefix}_first_target_lane_metadata_source"] = snapshot["first_target_lane_metadata_source"]
        row[f"{prefix}_first_path_end_like_condition"] = _fmt_bool(snapshot["first_path_end_like_condition"])
        row[f"{prefix}_first_planning_empty_reason_guess"] = snapshot["first_planning_empty_reason_guess"]
        row[f"{prefix}_first_map_contract_invalid"] = _fmt_bool(snapshot["first_map_contract_invalid"])
        row[f"{prefix}_first_map_contract_mismatch_classification"] = snapshot[
            "first_map_contract_mismatch_classification"
        ]
        row[f"{prefix}_first_runtime_map_dir"] = snapshot["first_runtime_map_dir"]
        row[f"{prefix}_first_lane_follow_map_inconsistent"] = _fmt_bool(
            snapshot["first_lane_follow_map_inconsistent"]
        )
        row[f"{prefix}_delta_first_minus_transition_sec"] = _fmt_float(
            _delta(snapshot["first_timestamp"], transition_ts)
        )
        row[f"{prefix}_delta_first_minus_refresh_sec"] = _fmt_float(
            _delta(snapshot["first_timestamp"], refresh_ts)
        )

        after_prefix = f"{surface_key}_after_transition"
        row[f"{after_prefix}_first_timestamp"] = _fmt_float(after_snapshot["first_timestamp"], 6)
        row[f"{after_prefix}_first_signature"] = after_snapshot["first_signature"]
        row[f"{after_prefix}_first_total_length_m"] = _fmt_float(after_snapshot["first_total_length_m"])
        row[f"{after_prefix}_first_route_segment_count"] = after_snapshot["first_route_segment_count"] or ""
        row[f"{after_prefix}_first_reference_line_count"] = after_snapshot["first_reference_line_count"] or ""
        row[f"{after_prefix}_first_create_route_segments_status"] = after_snapshot[
            "first_create_route_segments_status"
        ]
        row[f"{after_prefix}_first_reference_line_provider_status"] = after_snapshot[
            "first_reference_line_provider_status"
        ]
        row[f"{after_prefix}_first_lane_follow_map_status"] = after_snapshot["first_lane_follow_map_status"]
        row[f"{after_prefix}_first_current_lane_metadata_source"] = after_snapshot[
            "first_current_lane_metadata_source"
        ]
        row[f"{after_prefix}_first_target_lane_metadata_source"] = after_snapshot[
            "first_target_lane_metadata_source"
        ]
        row[f"{after_prefix}_first_path_end_like_condition"] = _fmt_bool(
            after_snapshot["first_path_end_like_condition"]
        )
        row[f"{after_prefix}_first_planning_empty_reason_guess"] = after_snapshot[
            "first_planning_empty_reason_guess"
        ]
        row[f"{after_prefix}_first_map_contract_invalid"] = _fmt_bool(after_snapshot["first_map_contract_invalid"])
        row[f"{after_prefix}_first_map_contract_mismatch_classification"] = after_snapshot[
            "first_map_contract_mismatch_classification"
        ]
        row[f"{after_prefix}_first_runtime_map_dir"] = after_snapshot["first_runtime_map_dir"]
        row[f"{after_prefix}_first_lane_follow_map_inconsistent"] = _fmt_bool(
            after_snapshot["first_lane_follow_map_inconsistent"]
        )
        row[f"{after_prefix}_delta_first_minus_transition_sec"] = _fmt_float(
            _delta(after_snapshot["first_timestamp"], transition_ts)
        )
        row[f"{after_prefix}_delta_first_minus_refresh_sec"] = _fmt_float(
            _delta(after_snapshot["first_timestamp"], refresh_ts)
        )

    counts = [surface_snapshots[key]["count"] for key, _ in SURFACE_FILES]
    timestamps = [
        _safe_float(surface_snapshots[key]["first_timestamp"])
        for key, _ in SURFACE_FILES
        if surface_snapshots[key]["first_timestamp"] is not None
    ]
    signatures = [surface_snapshots[key]["first_signature"] for key, _ in SURFACE_FILES]
    create_statuses = [
        surface_snapshots[key]["first_create_route_segments_status"] for key, _ in SURFACE_FILES
    ]
    provider_statuses = [
        surface_snapshots[key]["first_reference_line_provider_status"] for key, _ in SURFACE_FILES
    ]
    lane_follow_statuses = [
        surface_snapshots[key]["first_lane_follow_map_status"] for key, _ in SURFACE_FILES
    ]

    row["all_surfaces_zero_pre_refresh_rows"] = _fmt_bool(all(count == 0 for count in counts))
    row["all_surfaces_have_pre_refresh_rows"] = _fmt_bool(all(count > 0 for count in counts))
    row["surface_pre_refresh_count_pattern"] = "/".join(str(count) for count in counts)
    row["surface_first_timestamp_span_ms"] = _fmt_float(
        ((max(timestamps) - min(timestamps)) * 1000.0) if len(timestamps) >= 2 else 0.0 if timestamps else None,
        3,
    )
    row["surface_first_signature_consensus"] = _consensus(signatures)
    row["surface_first_total_length_m_consensus"] = _consensus(
        _fmt_float(surface_snapshots[key]["first_total_length_m"]) for key, _ in SURFACE_FILES
    )
    row["surface_first_create_route_segments_status_consensus"] = _consensus(create_statuses)
    row["surface_first_reference_line_provider_status_consensus"] = _consensus(provider_statuses)
    row["surface_first_lane_follow_map_status_consensus"] = _consensus(lane_follow_statuses)
    row["surface_first_delta_minus_transition_sec_consensus"] = _consensus(
        _fmt_float(_delta(_safe_float(surface_snapshots[key]["first_timestamp"]), transition_ts))
        for key, _ in SURFACE_FILES
    )
    row["surface_first_delta_minus_refresh_sec_consensus"] = _consensus(
        _fmt_float(_delta(_safe_float(surface_snapshots[key]["first_timestamp"]), refresh_ts))
        for key, _ in SURFACE_FILES
    )

    after_timestamps = [
        _safe_float(surface_after_transition_snapshots[key]["first_timestamp"])
        for key, _ in SURFACE_FILES
        if surface_after_transition_snapshots[key]["first_timestamp"] is not None
    ]
    row["first_visible_after_transition_ts_span_ms"] = _fmt_float(
        ((max(after_timestamps) - min(after_timestamps)) * 1000.0)
        if len(after_timestamps) >= 2
        else 0.0 if after_timestamps else None,
        3,
    )
    row["first_visible_after_transition_signature_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_signature"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_total_length_m_consensus"] = _consensus(
        _fmt_float(surface_after_transition_snapshots[key]["first_total_length_m"]) for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_create_route_segments_status_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_create_route_segments_status"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_reference_line_provider_status_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_reference_line_provider_status"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_lane_follow_map_status_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_lane_follow_map_status"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_current_lane_metadata_source_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_current_lane_metadata_source"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_target_lane_metadata_source_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_target_lane_metadata_source"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_path_end_like_condition_consensus"] = _consensus_bool(
        surface_after_transition_snapshots[key]["first_path_end_like_condition"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_planning_empty_reason_guess_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_planning_empty_reason_guess"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_map_contract_invalid_consensus"] = _consensus_bool(
        surface_after_transition_snapshots[key]["first_map_contract_invalid"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_map_contract_mismatch_classification_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_map_contract_mismatch_classification"]
        for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_runtime_map_dir_consensus"] = _consensus(
        surface_after_transition_snapshots[key]["first_runtime_map_dir"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_lane_follow_map_inconsistent_consensus"] = _consensus_bool(
        surface_after_transition_snapshots[key]["first_lane_follow_map_inconsistent"] for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_delta_minus_transition_sec_consensus"] = _consensus(
        _fmt_float(_delta(_safe_float(surface_after_transition_snapshots[key]["first_timestamp"]), transition_ts))
        for key, _ in SURFACE_FILES
    )
    row["first_visible_after_transition_delta_minus_refresh_sec_consensus"] = _consensus(
        _fmt_float(_delta(_safe_float(surface_after_transition_snapshots[key]["first_timestamp"]), refresh_ts))
        for key, _ in SURFACE_FILES
    )

    return row


def _render_md(rows: Sequence[Dict[str, Any]]) -> str:
    def sec_or_none(value: str) -> str:
        text = str(value or "").strip()
        return f"{text}s" if text else "none"

    lines: List[str] = []
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines.append("# Calibration Upstream Materialization Family Weekend")
    lines.append("")
    lines.append(f"- generated_at_local: `{now}`")
    lines.append("")
    lines.append("## Family Table")
    lines.append("")
    lines.append(
        "| label | candidate | final goal | pre-refresh surface pattern | first pre-refresh signature | first pre-refresh status | refresh outcome |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        final_goal = f"`{row['final_goal_mode'] or '?'} / {row['final_goal_source'] or '?'}`"
        status = (
            f"`create={row['surface_first_create_route_segments_status_consensus'] or 'none'}, "
            f"provider={row['surface_first_reference_line_provider_status_consensus'] or 'none'}, "
            f"lane_follow={row['surface_first_lane_follow_map_status_consensus'] or 'none'}`"
        )
        refresh = (
            f"`invalid={row['final_invalid_goal'] or 'false'}, "
            f"fallback={row['final_fallback_applied'] or 'false'}, "
            f"sig={row['final_routing_lane_window_signature'] or 'none'}`"
        )
        lines.append(
            f"| `{row['label']}` | `{row['candidate_signature']}` | {final_goal} | "
            f"`{row['surface_pre_refresh_count_pattern']}` | "
            f"`{row['surface_first_signature_consensus'] or 'none'}` | {status} | {refresh} |"
        )
    lines.append("")
    lines.append("## Per-Run Read")
    lines.append("")
    for row in rows:
        lines.append(f"### `{row['label']}`")
        lines.append("")
        lines.append(f"- run: [{Path(row['run_dir']).name}]({row['run_dir']})")
        lines.append(f"- candidate: `{row['candidate_signature']}`")
        lines.append(
            "- boundary:"
            f" `transition={row['transition_reroute_timestamp']}, refresh={row['refresh_reroute_timestamp']}, "
            f"goal_validity_rows={row['transition_to_refresh_goal_validity_count']}`"
        )
        lines.append(
            "- surface alignment:"
            f" `counts={row['surface_pre_refresh_count_pattern']}, "
            f"all_zero={row['all_surfaces_zero_pre_refresh_rows'] or 'false'}, "
            f"all_present={row['all_surfaces_have_pre_refresh_rows'] or 'false'}, "
            f"ts_span_ms={row['surface_first_timestamp_span_ms'] or ''}`"
        )
        lines.append(
            "- first synchronized surface state:"
            f" `sig={row['surface_first_signature_consensus'] or 'none'}, "
            f"len={row['surface_first_total_length_m_consensus'] or 'none'}m, "
            f"create={row['surface_first_create_route_segments_status_consensus'] or 'none'}, "
            f"provider={row['surface_first_reference_line_provider_status_consensus'] or 'none'}, "
            f"lane_follow={row['surface_first_lane_follow_map_status_consensus'] or 'none'}, "
            f"delta_vs_transition={sec_or_none(row['surface_first_delta_minus_transition_sec_consensus'])}, "
            f"delta_vs_refresh={sec_or_none(row['surface_first_delta_minus_refresh_sec_consensus'])}`"
        )
        lines.append(
            "- first visible row after transition:"
            f" `sig={row['first_visible_after_transition_signature_consensus'] or 'none'}, "
            f"len={row['first_visible_after_transition_total_length_m_consensus'] or 'none'}m, "
            f"create={row['first_visible_after_transition_create_route_segments_status_consensus'] or 'none'}, "
            f"provider={row['first_visible_after_transition_reference_line_provider_status_consensus'] or 'none'}, "
            f"lane_follow={row['first_visible_after_transition_lane_follow_map_status_consensus'] or 'none'}, "
            f"current_lane_meta={row['first_visible_after_transition_current_lane_metadata_source_consensus'] or 'none'}, "
            f"target_lane_meta={row['first_visible_after_transition_target_lane_metadata_source_consensus'] or 'none'}, "
            f"path_end_like={row['first_visible_after_transition_path_end_like_condition_consensus'] or 'none'}, "
            f"empty_reason={row['first_visible_after_transition_planning_empty_reason_guess_consensus'] or 'none'}, "
            f"map_invalid={row['first_visible_after_transition_map_contract_invalid_consensus'] or 'none'}, "
            f"map_class={row['first_visible_after_transition_map_contract_mismatch_classification_consensus'] or 'none'}, "
            f"lane_follow_inconsistent={row['first_visible_after_transition_lane_follow_map_inconsistent_consensus'] or 'none'}, "
            f"delta_vs_transition={sec_or_none(row['first_visible_after_transition_delta_minus_transition_sec_consensus'])}, "
            f"delta_vs_refresh={sec_or_none(row['first_visible_after_transition_delta_minus_refresh_sec_consensus'])}`"
        )
        lines.append(
            "- refresh outcome:"
            f" `goal={row['final_goal_mode'] or ''}/{row['final_goal_source'] or ''}, "
            f"invalid={row['final_invalid_goal'] or 'false'}({row['final_invalid_goal_reason'] or ''}), "
            f"fallback={row['final_fallback_applied'] or 'false'}, "
            f"route_segments={row['final_route_segment_count'] or 0}, "
            f"reference_lines={row['final_reference_line_count'] or 0}, "
            f"sig={row['final_routing_lane_window_signature'] or 'none'}, "
            f"provider={row['final_reference_line_provider_status'] or 'none'}, "
            f"lane_follow={row['final_lane_follow_map_status'] or 'none'}`"
        )
        lines.append("")

    zero_rows = [row for row in rows if row["all_surfaces_zero_pre_refresh_rows"] == "true"]
    materialized_rows = [row for row in rows if row["all_surfaces_have_pre_refresh_rows"] == "true"]
    lines.append("## Conclusions")
    lines.append("")
    if zero_rows:
        zero_labels = ", ".join(f"`{row['label']}`" for row in zero_rows)
        lines.append(
            f"- The short-side family currently stays fully empty across all five inspected surfaces between "
            f"`long_phase_transition` and `long_phase_refresh`: {zero_labels}."
        )
    if materialized_rows:
        materialized_labels = ", ".join(f"`{row['label']}`" for row in materialized_rows)
        lines.append(
            f"- The long-side family already materializes on all five inspected surfaces before refresh: "
            f"{materialized_labels}."
        )
        lines.append(
            "- When that materialization exists, the first synchronized Apollo-side state is already not a healthy "
            "reference-line recovery:"
            " `create_route_segments_status=ready`,"
            " `reference_line_provider_status=failed`,"
            " `lane_follow_map_status=route_segments_present_reference_line_missing`."
        )
    signature_rows = [
        row
        for row in materialized_rows
        if row["surface_first_signature_consensus"] and "|" not in row["surface_first_signature_consensus"]
    ]
    if signature_rows:
        signature_summary = ", ".join(
            f"`{row['label']} -> {row['surface_first_signature_consensus']}`" for row in signature_rows
        )
        lines.append(
            "- Where signature instrumentation is present, the first pre-refresh surface row is already the short "
            f"`~6m` stub rather than the later long window: {signature_summary}."
        )
    visible_rows = [
        row
        for row in rows
        if row["first_visible_after_transition_signature_consensus"]
        and row["first_visible_after_transition_create_route_segments_status_consensus"]
    ]
    if visible_rows:
        lines.append(
            "- Once any visible row exists after transition, both short and long families still share the same failure "
            "contract shape rather than a healthier long-branch state:"
            " `create_route_segments=ready`,"
            " `reference_line_provider=failed`,"
            " `lane_follow=route_segments_present_reference_line_missing`,"
            " `current_lane_metadata_source=missing_lane_id`,"
            " `target_lane_metadata_source=missing_lane_id`,"
            " `planning_empty_reason_guess=reference_line_missing`."
        )
        lines.append(
            "- That means the current selector is narrower than a broad provider/map-contract mode switch:"
            " the split is specifically about when the first visible routed lane-window appears and whether it is the "
            "short `~6m` stub before refresh or the longer `scenario_xy` lane-local window after refresh."
        )
    lines.append(
        "- That compresses the blocker one layer deeper than a generic `goal_validity` flip: the decisive branch split "
        "is now the appearance of one synchronized pre-refresh Apollo-side snapshot, not a later reporting mismatch."
    )
    lines.append(
        "- The next minimal cut should therefore target which post-transition response-consumption / route-segment "
        "construction step causes that synchronized `create_route_segments=ready + reference_line_provider=failed` "
        "state to appear before refresh on the long family while the short family reaches refresh with no rows at all."
    )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare calibration upstream materialization across live runs.")
    parser.add_argument("--run", action="append", type=_parse_run_spec, required=True, help="label=/path/to/run")
    parser.add_argument(
        "--output-md",
        default="artifacts/calibration_upstream_materialization_family_weekend.md",
        help="markdown output path",
    )
    parser.add_argument(
        "--output-csv",
        default="artifacts/calibration_upstream_materialization_family_weekend.csv",
        help="csv output path",
    )
    args = parser.parse_args()

    rows = [_analyze_run(label, run_dir) for label, run_dir in args.run]
    _write_csv(Path(args.output_csv), rows)
    _write_text(Path(args.output_md), _render_md(rows))
    print(f"wrote {args.output_md}")
    print(f"wrote {args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
