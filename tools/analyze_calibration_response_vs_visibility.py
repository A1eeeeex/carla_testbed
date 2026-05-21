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
    parts: List[str] = []
    road_id = candidate.get("road_id")
    lane_id = candidate.get("lane_id")
    start_y = _fmt_float(candidate.get("start_y"))
    if road_id is not None:
        parts.append(f"road_id={road_id}")
    if lane_id is not None:
        parts.append(f"lane_id={lane_id}")
    if start_y:
        parts.append(f"start_y≈{start_y}")
    return " ".join(parts) if parts else "unknown"


def _first_row_after(rows: Sequence[Dict[str, Any]], ts: Optional[float]) -> Dict[str, Any]:
    if ts is None:
        return {}
    for row in rows:
        row_ts = _safe_float(row.get("timestamp"))
        if row_ts is not None and row_ts >= ts:
            return row
    return {}


def _reroute_ts(rows: Sequence[Dict[str, Any]], reason: str) -> Optional[float]:
    for row in rows:
        if str(row.get("reroute_reason") or "").strip() == reason:
            return _safe_float(row.get("timestamp"))
    return None


def _ordering(lhs: Optional[float], rhs: Optional[float]) -> str:
    if lhs is None or rhs is None:
        return ""
    delta = lhs - rhs
    if abs(delta) <= 0.010:
        return "same_window"
    return "before" if delta < 0 else "after"


def _analyze_run(label: str, run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    metadata = _load_json(artifacts / "scenario_metadata.json")
    summary = _load_json(run_dir / "summary.json")
    reroute_rows = _load_jsonl(artifacts / "reroute_decision_debug.jsonl")
    goal_validity_rows = _load_jsonl(artifacts / "goal_validity_debug.jsonl")
    route_rows = _load_jsonl(artifacts / "planning_route_segment_debug.jsonl")
    planning_summary = _load_json(artifacts / "planning_topic_debug_summary.json")
    bridge_summary = _load_json(artifacts / "bridge_health_summary.json")
    planning_timing = bridge_summary.get("planning_timing_summary") or {}
    if not isinstance(planning_timing, dict):
        planning_timing = {}

    transition_ts = _reroute_ts(reroute_rows, "long_phase_transition")
    refresh_ts = _reroute_ts(reroute_rows, "long_phase_refresh")
    response_ts = _safe_float(
        planning_timing.get("routing_first_response_after_last_routing_send_ts_sec")
        or bridge_summary.get("routing_first_response_after_last_routing_send_ts_sec")
    )
    success_response_ts = _safe_float(
        planning_timing.get("routing_first_success_response_after_last_routing_send_ts_sec")
        or bridge_summary.get("routing_first_success_response_after_last_routing_send_ts_sec")
    )
    first_visible_row = _first_row_after(route_rows, transition_ts)
    final_goal_validity_row = goal_validity_rows[-1] if goal_validity_rows else {}
    final_goal_validity_ts = _safe_float(final_goal_validity_row.get("timestamp"))

    row: Dict[str, Any] = {
        "label": label,
        "run_dir": str(run_dir),
        "candidate_signature": _candidate_signature(metadata),
        "summary_status": summary.get("status") or "",
        "fail_reason": summary.get("fail_reason") or "",
        "transition_timestamp": _fmt_float(transition_ts, 6),
        "refresh_timestamp": _fmt_float(refresh_ts, 6),
        "first_routing_response_timestamp": _fmt_float(response_ts, 6),
        "first_success_routing_response_timestamp": _fmt_float(success_response_ts, 6),
        "first_visible_timestamp": _fmt_float(_safe_float(first_visible_row.get("timestamp")), 6),
        "first_visible_signature": first_visible_row.get("routing_lane_window_signature") or "",
        "first_visible_total_length_m": _fmt_float(first_visible_row.get("route_segment_total_length")),
        "first_visible_create_route_segments_status": first_visible_row.get("create_route_segments_status") or "",
        "first_visible_reference_line_provider_status": first_visible_row.get("reference_line_provider_status") or "",
        "first_visible_lane_follow_map_status": first_visible_row.get("lane_follow_map_status") or "",
        "final_goal_validity_timestamp": _fmt_float(final_goal_validity_ts, 6),
        "final_goal_mode": final_goal_validity_row.get("goal_mode") or "",
        "final_goal_source": final_goal_validity_row.get("goal_source") or "",
        "final_invalid_goal": _fmt_bool(final_goal_validity_row.get("invalid_goal")),
        "final_fallback_applied": _fmt_bool(final_goal_validity_row.get("fallback_applied")),
        "delta_response_minus_transition_sec": _fmt_float(_delta(response_ts, transition_ts)),
        "delta_first_visible_minus_transition_sec": _fmt_float(
            _delta(_safe_float(first_visible_row.get("timestamp")), transition_ts)
        ),
        "delta_refresh_minus_transition_sec": _fmt_float(_delta(refresh_ts, transition_ts)),
        "delta_final_goal_validity_minus_transition_sec": _fmt_float(_delta(final_goal_validity_ts, transition_ts)),
        "delta_first_visible_minus_response_sec": _fmt_float(
            _delta(_safe_float(first_visible_row.get("timestamp")), response_ts)
        ),
        "delta_refresh_minus_response_sec": _fmt_float(_delta(refresh_ts, response_ts)),
        "delta_final_goal_validity_minus_response_sec": _fmt_float(_delta(final_goal_validity_ts, response_ts)),
        "first_visible_vs_response_order": _ordering(
            _safe_float(first_visible_row.get("timestamp")), response_ts
        ),
        "refresh_vs_response_order": _ordering(refresh_ts, response_ts),
        "final_goal_validity_vs_response_order": _ordering(final_goal_validity_ts, response_ts),
        "planning_first_msg_after_response_sec": _fmt_float(
            planning_summary.get("first_msg_after_first_routing_response_after_last_routing_send_sec")
        ),
    }
    return row


def _render_md(rows: Sequence[Dict[str, Any]]) -> str:
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines: List[str] = []
    lines.append("# Calibration Response Vs Visibility Weekend")
    lines.append("")
    lines.append(f"- generated_at_local: `{now}`")
    lines.append("")
    lines.append("## Table")
    lines.append("")
    lines.append(
        "| label | candidate | response | first visible row | refresh | final goal-validity | ordering |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        response = f"`{row['first_routing_response_timestamp'] or 'none'}`"
        visible = (
            f"`{row['first_visible_signature'] or 'none'} @ {row['delta_first_visible_minus_transition_sec'] or 'none'}s`"
        )
        refresh = f"`{row['delta_refresh_minus_transition_sec'] or 'none'}s`"
        final_goal = f"`{row['final_goal_mode'] or '?'}/{row['final_goal_source'] or '?'}`"
        ordering = (
            f"`visible_vs_response={row['first_visible_vs_response_order'] or 'none'}, "
            f"refresh_vs_response={row['refresh_vs_response_order'] or 'none'}, "
            f"goal_vs_response={row['final_goal_validity_vs_response_order'] or 'none'}`"
        )
        lines.append(
            f"| `{row['label']}` | `{row['candidate_signature']}` | {response} | {visible} | {refresh} | {final_goal} | {ordering} |"
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
            "- boundary sequence:"
            f" `transition={row['transition_timestamp']}, "
            f"response={row['first_routing_response_timestamp'] or 'none'}, "
            f"first_visible={row['first_visible_timestamp'] or 'none'}, "
            f"refresh={row['refresh_timestamp'] or 'none'}, "
            f"final_goal_validity={row['final_goal_validity_timestamp'] or 'none'}`"
        )
        lines.append(
            "- first visible row:"
            f" `sig={row['first_visible_signature'] or 'none'}, "
            f"len={row['first_visible_total_length_m'] or 'none'}m, "
            f"create={row['first_visible_create_route_segments_status'] or 'none'}, "
            f"provider={row['first_visible_reference_line_provider_status'] or 'none'}, "
            f"lane_follow={row['first_visible_lane_follow_map_status'] or 'none'}`"
        )
        lines.append(
            "- ordering:"
            f" `visible_vs_response={row['first_visible_vs_response_order'] or 'none'} "
            f"({row['delta_first_visible_minus_response_sec'] or ''}s), "
            f"refresh_vs_response={row['refresh_vs_response_order'] or 'none'} "
            f"({row['delta_refresh_minus_response_sec'] or ''}s), "
            f"goal_vs_response={row['final_goal_validity_vs_response_order'] or 'none'} "
            f"({row['delta_final_goal_validity_minus_response_sec'] or ''}s)`"
        )
        lines.append(
            "- outcome:"
            f" `goal={row['final_goal_mode'] or ''}/{row['final_goal_source'] or ''}, "
            f"invalid={row['final_invalid_goal'] or 'false'}, "
            f"fallback={row['final_fallback_applied'] or 'false'}, "
            f"planning_first_msg_after_response={row['planning_first_msg_after_response_sec'] or 'none'}s`"
        )
        lines.append("")
    if rows:
        lines.append("## Conclusions")
        lines.append("")
        lines.append(
            "- The response-aware truthful low-y pair does not support a simple 'recorded routing response arrives earlier on the long branch' story."
        )
        lines.append(
            "- In the long fallback-bearing rerun, the first visible `~6m` stub and even the final goal-validity flip both occur before the recorded first routing-response timestamp."
        )
        lines.append(
            "- In the short truthful rerun, the recorded response lands in the same window as refresh, but the first visible row still appears much later and only after refresh."
        )
        lines.append(
            "- That compresses the remaining selector one step deeper: it is now about what is materialized/consumed before the recorded response boundary on the long branch and why the short branch records a response without materializing a visible row in the same window."
        )
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare recorded routing response ordering against first visible calibration rows.")
    parser.add_argument("--run", action="append", type=_parse_run_spec, required=True, help="label=/path/to/run")
    parser.add_argument(
        "--output-md",
        default="artifacts/calibration_response_vs_visibility_weekend.md",
        help="markdown output path",
    )
    parser.add_argument(
        "--output-csv",
        default="artifacts/calibration_response_vs_visibility_weekend.csv",
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
