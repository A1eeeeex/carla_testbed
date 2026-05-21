#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


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


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
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


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _delta(lhs: Optional[float], rhs: Optional[float]) -> Optional[float]:
    if lhs is None or rhs is None:
        return None
    return lhs - rhs


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


def _visible_route_debug(row: Dict[str, Any]) -> bool:
    if (_safe_int(row.get("route_segment_count")) or 0) > 0:
        return True
    if _safe_float(row.get("route_segment_total_length")) is not None:
        return True
    signature = _text(row.get("routing_lane_window_signature")).strip().lower()
    return bool(signature and signature != "none")


def _snapshot_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": _safe_float(row.get("timestamp")),
        "planning_header_sequence_num": _safe_int(row.get("planning_header_sequence_num")),
        "signature": _text(row.get("routing_lane_window_signature")).strip(),
        "total_length_m": _safe_float(row.get("route_segment_total_length")),
        "route_segment_count": _safe_int(row.get("route_segment_count")),
        "reference_line_count": _safe_int(row.get("reference_line_count")),
        "provider_status": _text(row.get("reference_line_provider_status")).strip(),
        "create_status": _text(row.get("create_route_segments_status")).strip(),
        "lane_follow_status": _text(row.get("lane_follow_map_status")).strip(),
        "source": _text(row.get("route_debug_source")).strip(),
    }


def _snapshot_dict(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": _safe_float(snapshot.get("timestamp")),
        "planning_header_sequence_num": _safe_int(snapshot.get("planning_header_sequence_num")),
        "signature": _text(snapshot.get("routing_lane_window_signature")).strip(),
        "total_length_m": _safe_float(snapshot.get("route_segment_total_length")),
        "route_segment_count": _safe_int(snapshot.get("route_segment_count")),
        "reference_line_count": _safe_int(snapshot.get("reference_line_count")),
        "provider_status": _text(snapshot.get("reference_line_provider_status")).strip(),
        "create_status": _text(snapshot.get("create_route_segments_status")).strip(),
        "lane_follow_status": _text(snapshot.get("lane_follow_map_status")).strip(),
        "source": _text(snapshot.get("route_debug_source")).strip(),
    }


def _snapshot_present(snapshot: Dict[str, Any]) -> bool:
    if not snapshot:
        return False
    if _safe_float(snapshot.get("timestamp")) is not None:
        return True
    if (_safe_int(snapshot.get("route_segment_count")) or 0) > 0:
        return True
    if (_safe_int(snapshot.get("reference_line_count")) or 0) > 0:
        return True
    signature = _text(snapshot.get("routing_lane_window_signature")).strip().lower()
    return bool(signature and signature != "none")


def _signature_match(snapshot: Dict[str, Any], row: Dict[str, Any]) -> bool:
    sig_a = _text(snapshot.get("signature")).strip()
    sig_b = _text(row.get("signature")).strip()
    return bool(sig_a and sig_b and sig_a == sig_b)


def _seq_match(snapshot: Dict[str, Any], row: Dict[str, Any]) -> bool:
    seq_a = _safe_int(snapshot.get("planning_header_sequence_num"))
    seq_b = _safe_int(row.get("planning_header_sequence_num"))
    return seq_a is not None and seq_b is not None and seq_a == seq_b


def _branch_family(goal_distance_m: Any) -> str:
    distance = _safe_float(goal_distance_m)
    if distance is None:
        return "unknown"
    return "short" if distance < 150.0 else "long"


def _selector_family(row: Dict[str, Any]) -> str:
    if row["recent_route_debug_present"] == "true" and row["recent_route_debug_fresh"] == "true":
        if row["recent_signature_matches_last_pre_refresh"] == "true" and row["recent_signature_matches_first_post_refresh"] != "true":
            return "fresh_pre_refresh_stub_snapshot"
        if row["recent_signature_matches_first_post_refresh"] == "true":
            return "fresh_post_refresh_snapshot"
        return "fresh_snapshot_other_family"
    if int(row["pre_refresh_visible_route_debug_count"] or 0) <= 0:
        return "no_snapshot_pre_refresh_empty"
    return "no_fresh_snapshot_despite_pre_refresh_visibility"


def _derive_ready_reason(
    *,
    explicit_reason: Any,
    recent_present: bool,
    recent_fresh: bool,
    recent_route_segment_count: Any,
    recent_reference_line_count: Any,
) -> str:
    explicit = _text(explicit_reason).strip()
    if explicit:
        return explicit
    if not recent_present or not recent_fresh:
        return "missing_or_stale"
    if (_safe_int(recent_reference_line_count) or 0) > 0:
        return "reference_line_count"
    if (_safe_int(recent_route_segment_count) or 0) > 0:
        return "route_segment_count"
    return "fresh_but_empty"


def _analyze_run(label: str, run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    metadata = _load_json(artifacts / "scenario_metadata.json")
    summary = _load_json(run_dir / "summary.json")
    planning_summary = _load_json(artifacts / "planning_topic_debug_summary.json")
    reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    stage5_rows = _load_jsonl(artifacts / "stage5_apollo_reference_line_debug.jsonl")

    final_reroute = reroute_rows[-1] if reroute_rows else {}
    current_goal = final_reroute.get("current_goal") or {}
    goal_resolution = final_reroute.get("goal_resolution") or {}
    trigger = final_reroute.get("trigger_condition_snapshot") or {}
    recent_snapshot_raw = final_reroute.get("recent_route_debug_snapshot") or {}
    recent_snapshot = _snapshot_dict(recent_snapshot_raw)
    recent_present = _snapshot_present(recent_snapshot_raw)
    recent_fresh = bool(trigger.get("recent_route_debug_fresh")) if recent_present else False
    ready_reason = _derive_ready_reason(
        explicit_reason=trigger.get("route_debug_ready_reason"),
        recent_present=recent_present,
        recent_fresh=recent_fresh,
        recent_route_segment_count=recent_snapshot.get("route_segment_count"),
        recent_reference_line_count=recent_snapshot.get("reference_line_count"),
    )
    refresh_ts = _safe_float(final_reroute.get("timestamp"))
    first_response_ts = _safe_float(planning_summary.get("routing_first_response_after_last_routing_send_ts_sec"))
    transition_row = next(
        (row for row in reroute_rows if _text(row.get("reroute_reason")).strip() == "long_phase_transition"),
        {},
    )
    transition_ts = _safe_float(transition_row.get("timestamp"))

    visible_rows = [row for row in stage5_rows if _visible_route_debug(row)]
    pre_refresh_visible_rows = [
        _snapshot_row(row)
        for row in visible_rows
        if refresh_ts is not None and _safe_float(row.get("timestamp")) is not None and _safe_float(row.get("timestamp")) < refresh_ts
    ]
    post_refresh_visible_rows = [
        _snapshot_row(row)
        for row in visible_rows
        if refresh_ts is not None and _safe_float(row.get("timestamp")) is not None and _safe_float(row.get("timestamp")) >= refresh_ts
    ]
    first_pre = pre_refresh_visible_rows[0] if pre_refresh_visible_rows else {}
    last_pre = pre_refresh_visible_rows[-1] if pre_refresh_visible_rows else {}
    first_post = post_refresh_visible_rows[0] if post_refresh_visible_rows else {}

    row: Dict[str, Any] = {
        "label": label,
        "run_dir": str(run_dir),
        "candidate_signature": _candidate_signature(metadata),
        "summary_status": _text(summary.get("summary_status") or summary.get("status")),
        "fail_reason": _text(summary.get("fail_reason")),
        "transition_reroute_timestamp": _fmt_float(transition_ts, 6),
        "refresh_reroute_timestamp": _fmt_float(refresh_ts, 6),
        "first_response_after_last_send_timestamp": _fmt_float(first_response_ts, 6),
        "final_goal_mode": _text(current_goal.get("goal_mode") or goal_resolution.get("final_goal_mode")),
        "final_goal_source": _text(current_goal.get("goal_source") or goal_resolution.get("final_goal_source")),
        "final_goal_distance_m": _fmt_float(
            current_goal.get("goal_distance_m") or goal_resolution.get("final_goal_distance_m")
        ),
        "final_branch_family": _branch_family(
            current_goal.get("goal_distance_m") or goal_resolution.get("final_goal_distance_m")
        ),
        "route_debug_ready_for_long_phase": _fmt_bool(trigger.get("route_debug_ready_for_long_phase")),
        "route_debug_ready_reason": ready_reason,
        "recent_route_debug_present": _fmt_bool(recent_present),
        "recent_route_debug_fresh": _fmt_bool(recent_fresh),
        "recent_route_debug_age_sec": _fmt_float(trigger.get("recent_route_debug_age_sec"), 6) if recent_present else "",
        "recent_route_debug_timestamp": _fmt_float(recent_snapshot.get("timestamp"), 6) if recent_present else "",
        "recent_route_debug_planning_seq": _text(recent_snapshot.get("planning_header_sequence_num")),
        "recent_route_debug_signature": _text(recent_snapshot.get("signature")),
        "recent_route_debug_total_length_m": _fmt_float(recent_snapshot.get("total_length_m")),
        "recent_route_debug_route_segment_count": _text(recent_snapshot.get("route_segment_count")),
        "recent_route_debug_reference_line_count": _text(recent_snapshot.get("reference_line_count")),
        "recent_route_debug_source": _text(recent_snapshot.get("source")),
        "recent_route_debug_create_status": _text(recent_snapshot.get("create_status")),
        "recent_route_debug_provider_status": _text(recent_snapshot.get("provider_status")),
        "recent_route_debug_lane_follow_status": _text(recent_snapshot.get("lane_follow_status")),
        "pre_refresh_visible_route_debug_count": _text(len(pre_refresh_visible_rows)),
        "first_pre_refresh_visible_timestamp": _fmt_float(first_pre.get("timestamp"), 6),
        "first_pre_refresh_visible_seq": _text(first_pre.get("planning_header_sequence_num")),
        "first_pre_refresh_visible_signature": _text(first_pre.get("signature")),
        "first_pre_refresh_visible_total_length_m": _fmt_float(first_pre.get("total_length_m")),
        "last_pre_refresh_visible_timestamp": _fmt_float(last_pre.get("timestamp"), 6),
        "last_pre_refresh_visible_seq": _text(last_pre.get("planning_header_sequence_num")),
        "last_pre_refresh_visible_signature": _text(last_pre.get("signature")),
        "last_pre_refresh_visible_total_length_m": _fmt_float(last_pre.get("total_length_m")),
        "first_post_refresh_visible_timestamp": _fmt_float(first_post.get("timestamp"), 6),
        "first_post_refresh_visible_seq": _text(first_post.get("planning_header_sequence_num")),
        "first_post_refresh_visible_signature": _text(first_post.get("signature")),
        "first_post_refresh_visible_total_length_m": _fmt_float(first_post.get("total_length_m")),
        "recent_signature_matches_first_pre_refresh": _fmt_bool(_signature_match(recent_snapshot, first_pre)),
        "recent_signature_matches_last_pre_refresh": _fmt_bool(_signature_match(recent_snapshot, last_pre)),
        "recent_signature_matches_first_post_refresh": _fmt_bool(_signature_match(recent_snapshot, first_post)),
        "recent_seq_matches_first_pre_refresh": _fmt_bool(_seq_match(recent_snapshot, first_pre)),
        "recent_seq_matches_last_pre_refresh": _fmt_bool(_seq_match(recent_snapshot, last_pre)),
        "recent_seq_matches_first_post_refresh": _fmt_bool(_seq_match(recent_snapshot, first_post)),
        "delta_recent_minus_transition_sec": _fmt_float(_delta(recent_snapshot.get("timestamp"), transition_ts), 6),
        "delta_recent_minus_refresh_sec": _fmt_float(_delta(recent_snapshot.get("timestamp"), refresh_ts), 6),
        "delta_recent_minus_first_response_sec": _fmt_float(_delta(recent_snapshot.get("timestamp"), first_response_ts), 6),
        "delta_recent_minus_first_pre_refresh_sec": _fmt_float(_delta(recent_snapshot.get("timestamp"), first_pre.get("timestamp")), 6),
        "delta_recent_minus_last_pre_refresh_sec": _fmt_float(_delta(recent_snapshot.get("timestamp"), last_pre.get("timestamp")), 6),
        "delta_first_post_refresh_minus_refresh_sec": _fmt_float(_delta(first_post.get("timestamp"), refresh_ts), 6),
    }
    row["selector_family"] = _selector_family(row)
    return row


def _render_md(rows: Sequence[Dict[str, Any]]) -> str:
    def sec_text(value: Any) -> str:
        text = str(value or "").strip()
        return f"{text}s" if text else "none"

    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines: List[str] = [
        "# Calibration Route-Debug Selector Lineage Weekend",
        "",
        f"- generated_at_local: `{now}`",
        "",
        "## Selector Table",
        "",
        "| label | final branch | ready reason | recent snapshot | pre-refresh visible family | first post-refresh family | selector family |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        recent_render = (
            f"{row['recent_route_debug_signature'] or 'none'}"
            f" / fresh={row['recent_route_debug_fresh'] or 'false'}"
            f" / age={row['recent_route_debug_age_sec'] or 'none'}"
        )
        pre_render = (
            f"{row['pre_refresh_visible_route_debug_count'] or '0'} rows"
            f" / last={row['last_pre_refresh_visible_signature'] or 'none'}"
        )
        post_render = (
            f"{row['first_post_refresh_visible_signature'] or 'none'}"
            f" @ {row['delta_first_post_refresh_minus_refresh_sec'] or 'none'}"
        )
        lines.append(
            f"| `{row['label']}` | `{row['final_branch_family']} / {row['final_goal_mode'] or '?'} / {row['final_goal_source'] or '?'}` | "
            f"`{row['route_debug_ready_reason'] or 'none'}` | `{recent_render}` | `{pre_render}` | `{post_render}` | `{row['selector_family']}` |"
        )

    lines.extend(
        [
            "",
            "## Per-Run Read",
            "",
        ]
    )
    for row in rows:
        lines.append(f"### `{row['label']}`")
        lines.append("")
        lines.append(f"- run: [{Path(row['run_dir']).name}]({row['run_dir']})")
        lines.append(
            f"- final refresh branch: `{row['final_branch_family']} / {row['final_goal_mode'] or 'none'} / "
            f"{row['final_goal_source'] or 'none'} / len≈{row['final_goal_distance_m'] or 'none'}m`"
        )
        lines.append(
            f"- refresh selector state: `route_debug_ready={row['route_debug_ready_for_long_phase'] or 'false'}, "
            f"reason={row['route_debug_ready_reason'] or 'none'}, "
            f"recent_present={row['recent_route_debug_present'] or 'false'}, "
            f"recent_fresh={row['recent_route_debug_fresh'] or 'false'}, "
            f"recent_age={sec_text(row['recent_route_debug_age_sec'])}`"
        )
        lines.append(
            f"- recent snapshot lineage: `ts={row['recent_route_debug_timestamp'] or 'none'}, "
            f"seq={row['recent_route_debug_planning_seq'] or 'none'}, "
            f"sig={row['recent_route_debug_signature'] or 'none'}, "
            f"len={row['recent_route_debug_total_length_m'] or 'none'}m, "
            f"create={row['recent_route_debug_create_status'] or 'none'}, "
            f"provider={row['recent_route_debug_provider_status'] or 'none'}, "
            f"lane_follow={row['recent_route_debug_lane_follow_status'] or 'none'}`"
        )
        lines.append(
            f"- pre-refresh visible lineage: `count={row['pre_refresh_visible_route_debug_count'] or '0'}, "
            f"first={row['first_pre_refresh_visible_signature'] or 'none'}"
            f"@{sec_text(row['first_pre_refresh_visible_timestamp']) if row['first_pre_refresh_visible_timestamp'] else 'none'}, "
            f"last={row['last_pre_refresh_visible_signature'] or 'none'}"
            f"@{sec_text(row['last_pre_refresh_visible_timestamp']) if row['last_pre_refresh_visible_timestamp'] else 'none'}`"
        )
        lines.append(
            f"- family matching: `recent_vs_first_pre_sig={row['recent_signature_matches_first_pre_refresh'] or 'false'}, "
            f"recent_vs_last_pre_sig={row['recent_signature_matches_last_pre_refresh'] or 'false'}, "
            f"recent_vs_post_sig={row['recent_signature_matches_first_post_refresh'] or 'false'}, "
            f"recent_vs_first_pre_seq={row['recent_seq_matches_first_pre_refresh'] or 'false'}, "
            f"recent_vs_last_pre_seq={row['recent_seq_matches_last_pre_refresh'] or 'false'}, "
            f"recent_vs_post_seq={row['recent_seq_matches_first_post_refresh'] or 'false'}`"
        )
        lines.append(
            f"- timing deltas: `recent_vs_transition={sec_text(row['delta_recent_minus_transition_sec'])}, "
            f"recent_vs_response={sec_text(row['delta_recent_minus_first_response_sec'])}, "
            f"recent_vs_refresh={sec_text(row['delta_recent_minus_refresh_sec'])}, "
            f"recent_vs_first_pre={sec_text(row['delta_recent_minus_first_pre_refresh_sec'])}, "
            f"recent_vs_last_pre={sec_text(row['delta_recent_minus_last_pre_refresh_sec'])}, "
            f"first_post_vs_refresh={sec_text(row['delta_first_post_refresh_minus_refresh_sec'])}`"
        )
        lines.append(f"- selector family: `{row['selector_family']}`")
        lines.append("")

    long_rows = [row for row in rows if row["selector_family"] == "fresh_pre_refresh_stub_snapshot"]
    short_rows = [row for row in rows if row["selector_family"] == "no_snapshot_pre_refresh_empty"]
    lines.extend(
        [
            "## Conclusions",
            "",
        ]
    )
    if long_rows:
        rendered = ", ".join(f"`{row['label']}`" for row in long_rows)
        lines.append(
            "- The fallback-bearing selector family is now narrower than generic `pre-refresh rows exist`:"
            f" {rendered} reaches `long_phase_refresh` with a fresh recent route-debug snapshot that still points at the"
            " short pre-refresh `~6m` stub family rather than the later post-refresh long window."
        )
    if short_rows:
        rendered = ", ".join(f"`{row['label']}`" for row in short_rows)
        lines.append(
            "- The short truthful family is equally consistent on the other side:"
            f" {rendered} reaches refresh with no recent snapshot and no pre-refresh visible route-debug rows;"
            " its first visible family only appears after refresh as the `scenario_xy` `~116m` lane-local window."
        )
    specific_rows = [
        row
        for row in rows
        if row["recent_route_debug_present"] == "true"
        and row["recent_signature_matches_last_pre_refresh"] == "true"
        and row["recent_signature_matches_first_post_refresh"] != "true"
    ]
    for row in specific_rows:
        lines.append(
            f"- `{row['label']}` shows the refresh selector is not already consuming the post-refresh fallback-long window:"
            f" its recent snapshot is still `{row['recent_route_debug_signature'] or 'none'}`, "
            f"which matches the pre-refresh family and not the first post-refresh family `{row['first_post_refresh_visible_signature'] or 'none'}`."
        )
        lines.append(
            f"- The same run also shows the selector does not need the newest visible row:"
            f" the recent snapshot trails the last pre-refresh visible row by `{sec_text(row['delta_recent_minus_last_pre_refresh_sec'])}`, "
            f"yet it is still fresh enough at refresh (`age={sec_text(row['recent_route_debug_age_sec'])}`)."
        )
    lines.append(
        "- That means the next live cut should watch one boundary more precisely than `did any upstream row appear`:"
        " did `long_phase_refresh` capture a fresh recent route-debug snapshot from the `~6m` stub family inside the 1s staleness window."
    )
    lines.append(
        "- If a future run materializes pre-refresh rows but still reaches refresh without that fresh snapshot, "
        "the blocker will have moved from upstream materialization to snapshot selection / freshness propagation."
    )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare route-debug selector lineage across calibration live runs.")
    parser.add_argument("--run", action="append", type=_parse_run_spec, required=True, help="label=/path/to/run")
    parser.add_argument(
        "--output-md",
        default="artifacts/calibration_route_debug_selector_lineage_weekend.md",
        help="markdown output path",
    )
    parser.add_argument(
        "--output-csv",
        default="artifacts/calibration_route_debug_selector_lineage_weekend.csv",
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
