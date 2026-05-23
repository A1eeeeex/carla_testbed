#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _runtime_status(summary: Dict[str, Any]) -> str:
    contract = summary.get("runtime_contract")
    if isinstance(contract, dict):
        return str(contract.get("status") or "unknown")
    return str(summary.get("runtime_contract_status") or "unknown")


def _float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _summary_key(summary: Dict[str, Any], path: Path) -> str:
    route_id = str(summary.get("route_id") or "").strip()
    if route_id:
        return route_id
    metadata = summary.get("scenario_metadata")
    if isinstance(metadata, dict):
        route_id = str(metadata.get("route_id") or "").strip()
        if route_id:
            return route_id
    return path.parent.name


def _collect_summaries(roots: Sequence[Path]) -> Dict[str, Tuple[Path, Dict[str, Any]]]:
    discovered: Dict[str, Tuple[Path, Dict[str, Any]]] = {}
    for root in roots:
        resolved = root.expanduser().resolve()
        candidates = [resolved] if resolved.name == "summary.json" else list(resolved.rglob("summary.json"))
        for path in candidates:
            summary = _load_json(path)
            if not summary:
                continue
            key = _summary_key(summary, path)
            current = discovered.get(key)
            if current is None or path.stat().st_mtime >= current[0].stat().st_mtime:
                discovered[key] = (path, summary)
    return discovered


def _control_consuming(summary: Dict[str, Any]) -> bool:
    return str(summary.get("control_handoff_status") or "") == "control_consuming_with_nonzero_planning"


def _route_established(summary: Dict[str, Any]) -> bool:
    label = str(summary.get("route_health_label") or "")
    if label == "route_not_established":
        return False
    if int(summary.get("routing_success_count") or 0) > 0:
        return True
    return bool(summary.get("routing_materialized"))


def _candidate_gt_ingress_blocked(summary: Dict[str, Any]) -> bool:
    layer = str(summary.get("command_materialization_layer") or "").strip().lower()
    stage = str(summary.get("command_materialization_stage") or "").strip().lower()
    reason = str(summary.get("command_materialization_reason") or "").strip().lower()
    materialization = str(summary.get("materialization_status") or "").strip().lower()
    if layer == "gt_ingress":
        return True
    if stage == "waiting_for_first_odometry":
        return True
    if materialization == "bridge_alive_no_routing" and ("connect_failed" in reason or "time-out" in reason):
        return True
    return False


def _direct_apply_metric_mismatch(summary: Dict[str, Any]) -> bool:
    transport = str(summary.get("transport_mode") or "").strip().lower()
    if transport != "carla_direct":
        return False
    consistency_status = str(summary.get("direct_metric_consistency_status") or "").strip().lower()
    if consistency_status in {"short_apply_metric_mismatch", "metric_mismatch"}:
        return True
    window = str(summary.get("direct_control_apply_window_status") or "").strip().lower()
    if window != "short_apply_window":
        return False
    summary_speed = _float_or_none(summary.get("max_speed_mps")) or 0.0
    direct_speed = _float_or_none(summary.get("direct_control_apply_max_speed_mps")) or 0.0
    route_distance = _float_or_none(summary.get("route_distance_achieved_m")) or 0.0
    apply_span = _float_or_none(summary.get("direct_control_apply_frame_span")) or 0.0
    if summary_speed - direct_speed >= 2.0:
        return True
    if apply_span < 20.0 and abs(route_distance) >= 20.0:
        return True
    return False


def _distance_quality(baseline: Optional[Dict[str, Any]], candidate: Optional[Dict[str, Any]]) -> str:
    baseline_distance = _float_or_none((baseline or {}).get("route_distance_achieved_m"))
    candidate_distance = _float_or_none((candidate or {}).get("route_distance_achieved_m"))
    if baseline_distance is None and candidate_distance is None:
        return "missing"
    flags: List[str] = []
    if baseline_distance is not None and baseline_distance < 0.0:
        flags.append("baseline_negative")
    if candidate_distance is not None and candidate_distance < 0.0:
        flags.append("candidate_negative")
    return "+".join(flags) if flags else "ok"


def _runtime_interruption_reason(summary_path: Optional[Path]) -> str:
    if summary_path is None:
        return ""
    # The route-health wrapper redirects non-empty run dirs to ``__02`` etc.
    # Child stdout/stderr logs can live in the sibling pre-redirect run dir,
    # so search the route seed directory instead of only the summary dir.
    search_root = summary_path.parent
    if len(summary_path.parents) >= 2:
        search_root = summary_path.parents[1]
    try:
        log_paths = sorted(
            search_root.rglob("followstop_child.stderr.log"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        log_paths = []
    for log_path in log_paths[:4]:
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace").lower()
        except Exception:
            continue
        if "runtimeerror" in text and "world.tick" in text:
            if "time-out" in text or "timeout" in text:
                return "simulator_world_tick_timeout"
            return "simulator_world_tick_runtime_error"
        if "time-out of 30000ms while waiting for the simulator" in text:
            return "simulator_rpc_timeout"
    return ""


def _classify_row(
    baseline: Optional[Dict[str, Any]],
    candidate: Optional[Dict[str, Any]],
    *,
    candidate_summary_path: Optional[Path] = None,
) -> str:
    if baseline is None:
        return "baseline_missing"
    if candidate is None:
        return "candidate_missing"

    if _runtime_interruption_reason(candidate_summary_path):
        return "candidate_inconclusive_runtime_interrupted"

    candidate_window = str(candidate.get("direct_control_apply_window_status") or "")
    if _direct_apply_metric_mismatch(candidate):
        return "candidate_inconclusive_short_apply_metric_mismatch"
    if candidate_window == "short_apply_window":
        return "candidate_inconclusive_short_apply_window"
    if _candidate_gt_ingress_blocked(candidate):
        return "candidate_inconclusive_gt_ingress"

    baseline_aligned = _runtime_status(baseline) == "aligned"
    candidate_aligned = _runtime_status(candidate) == "aligned"
    baseline_control = _control_consuming(baseline)
    candidate_control = _control_consuming(candidate)
    baseline_established = _route_established(baseline)
    candidate_established = _route_established(candidate)

    if baseline_aligned and not candidate_aligned:
        return "candidate_negative_runtime"
    if baseline_established and not candidate_established:
        return "candidate_negative_materialization"
    if baseline_control and not candidate_control:
        return "candidate_negative_control"
    if candidate_control and not baseline_control:
        return "candidate_positive_control"

    baseline_distance = _float_or_none(baseline.get("route_distance_achieved_m")) or 0.0
    candidate_distance = _float_or_none(candidate.get("route_distance_achieved_m")) or 0.0
    baseline_speed = _float_or_none(baseline.get("max_speed_mps")) or 0.0
    candidate_speed = _float_or_none(candidate.get("max_speed_mps")) or 0.0
    distance_delta = candidate_distance - baseline_distance
    speed_delta = candidate_speed - baseline_speed

    if candidate_aligned and candidate_control and distance_delta >= 5.0:
        return "candidate_positive_distance"
    if baseline_control and candidate_control and distance_delta <= -5.0:
        return "candidate_negative_distance"
    if candidate_aligned and candidate_control and speed_delta >= 0.5 and distance_delta >= -2.0:
        return "candidate_positive_speed"
    return "candidate_inconclusive"


def _row_for(route_id: str, baseline_item: Optional[Tuple[Path, Dict[str, Any]]], candidate_item: Optional[Tuple[Path, Dict[str, Any]]]) -> Dict[str, Any]:
    baseline = baseline_item[1] if baseline_item else None
    candidate = candidate_item[1] if candidate_item else None
    candidate_summary_path = candidate_item[0] if candidate_item else None
    baseline_distance = _float_or_none((baseline or {}).get("route_distance_achieved_m"))
    candidate_distance = _float_or_none((candidate or {}).get("route_distance_achieved_m"))
    baseline_speed = _float_or_none((baseline or {}).get("max_speed_mps"))
    candidate_speed = _float_or_none((candidate or {}).get("max_speed_mps"))
    candidate_direct_apply_max_speed = _float_or_none((candidate or {}).get("direct_control_apply_max_speed_mps"))
    candidate_invalid_reason = _runtime_interruption_reason(candidate_summary_path)
    return {
        "route_id": route_id,
        "baseline_summary": str(baseline_item[0]) if baseline_item else "",
        "candidate_summary": str(candidate_item[0]) if candidate_item else "",
        "baseline_transport": (baseline or {}).get("transport_mode", ""),
        "candidate_transport": (candidate or {}).get("transport_mode", ""),
        "baseline_runtime": _runtime_status(baseline or {}),
        "candidate_runtime": _runtime_status(candidate or {}),
        "baseline_handoff": (baseline or {}).get("control_handoff_status", ""),
        "candidate_handoff": (candidate or {}).get("control_handoff_status", ""),
        "baseline_health": (baseline or {}).get("route_health_label", ""),
        "candidate_health": (candidate or {}).get("route_health_label", ""),
        "baseline_distance_m": baseline_distance,
        "candidate_distance_m": candidate_distance,
        "distance_delta_m": (
            candidate_distance - baseline_distance
            if candidate_distance is not None and baseline_distance is not None
            else None
        ),
        "baseline_max_speed_mps": baseline_speed,
        "candidate_max_speed_mps": candidate_speed,
        "speed_delta_mps": (
            candidate_speed - baseline_speed if candidate_speed is not None and baseline_speed is not None else None
        ),
        "candidate_direct_apply_count": (candidate or {}).get("direct_control_apply_count", ""),
        "candidate_direct_apply_window_status": (candidate or {}).get("direct_control_apply_window_status", ""),
        "candidate_direct_apply_frame_span": (candidate or {}).get("direct_control_apply_frame_span", ""),
        "candidate_direct_apply_max_speed_mps": candidate_direct_apply_max_speed,
        "candidate_direct_apply_max_throttle": (candidate or {}).get("direct_control_apply_max_throttle", ""),
        "candidate_direct_metric_consistency_status": (candidate or {}).get("direct_metric_consistency_status", ""),
        "candidate_direct_metric_consistency_reasons": ";".join(
            str(item) for item in ((candidate or {}).get("direct_metric_consistency_reasons") or [])
        ),
        "candidate_materialization_status": (candidate or {}).get("materialization_status", ""),
        "candidate_command_materialization_layer": (candidate or {}).get("command_materialization_layer", ""),
        "candidate_command_materialization_stage": (candidate or {}).get("command_materialization_stage", ""),
        "candidate_command_materialization_reason": (candidate or {}).get("command_materialization_reason", ""),
        "candidate_invalid_reason": candidate_invalid_reason,
        "distance_quality": _distance_quality(baseline, candidate),
        "verdict": _classify_row(
            baseline,
            candidate,
            candidate_summary_path=candidate_summary_path,
        ),
    }


def build_rows(
    baseline_roots: Sequence[Path],
    candidate_roots: Sequence[Path],
    route_ids: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    baseline = _collect_summaries(baseline_roots)
    candidate = _collect_summaries(candidate_roots)
    requested_route_ids = [str(item).strip() for item in list(route_ids or []) if str(item).strip()]
    resolved_route_ids = requested_route_ids or sorted(set(baseline) | set(candidate))
    return [_row_for(route_id, baseline.get(route_id), candidate.get(route_id)) for route_id in resolved_route_ids]


def summarize_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    verdict_counts: Dict[str, int] = {}
    for row in rows:
        verdict = str(row.get("verdict") or "unknown")
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
    candidate_missing_routes = [
        str(row.get("route_id") or "")
        for row in rows
        if str(row.get("verdict") or "") == "candidate_missing"
    ]
    candidate_runtime_interrupted_routes = [
        str(row.get("route_id") or "")
        for row in rows
        if str(row.get("verdict") or "") == "candidate_inconclusive_runtime_interrupted"
    ]
    candidate_negative_routes = [
        str(row.get("route_id") or "")
        for row in rows
        if str(row.get("verdict") or "").startswith("candidate_negative")
    ]
    candidate_positive_routes = [
        str(row.get("route_id") or "")
        for row in rows
        if str(row.get("verdict") or "").startswith("candidate_positive")
    ]
    route_count = len(rows)
    candidate_positive_count = sum(
        count for verdict, count in verdict_counts.items() if verdict.startswith("candidate_positive")
    )
    candidate_negative_count = sum(
        count for verdict, count in verdict_counts.items() if verdict.startswith("candidate_negative")
    )
    candidate_inconclusive_count = sum(
        count for verdict, count in verdict_counts.items() if verdict.startswith("candidate_inconclusive")
    )
    pending_rerun_routes = list(
        dict.fromkeys([*candidate_runtime_interrupted_routes, *candidate_missing_routes])
    )
    if route_count == 0:
        decision_status = "missing"
        decision_reason = "no A/B rows were available"
    elif candidate_negative_routes:
        decision_status = "candidate_negative"
        decision_reason = "one or more candidate routes are materially worse than baseline"
    elif pending_rerun_routes:
        decision_status = "partial_pending_rerun"
        decision_reason = "candidate has missing or runtime-interrupted routes that must be rerun before transport promotion"
    elif candidate_positive_count == route_count:
        decision_status = "candidate_positive"
        decision_reason = "all compared routes are positive for carla_direct"
    elif candidate_inconclusive_count:
        decision_status = "candidate_inconclusive"
        decision_reason = "all required summaries exist, but at least one candidate route remains non-decisive"
    else:
        decision_status = "candidate_valid_mixed"
        decision_reason = "A/B evidence is valid but not uniformly positive"
    return {
        "route_count": route_count,
        "baseline_aligned_count": sum(1 for row in rows if row.get("baseline_runtime") == "aligned"),
        "candidate_aligned_count": sum(1 for row in rows if row.get("candidate_runtime") == "aligned"),
        "baseline_control_consuming_count": sum(
            1 for row in rows if row.get("baseline_handoff") == "control_consuming_with_nonzero_planning"
        ),
        "candidate_control_consuming_count": sum(
            1 for row in rows if row.get("candidate_handoff") == "control_consuming_with_nonzero_planning"
        ),
        "candidate_positive_count": candidate_positive_count,
        "candidate_negative_count": candidate_negative_count,
        "candidate_inconclusive_count": candidate_inconclusive_count,
        "verdict_counts": verdict_counts,
        "decision_status": decision_status,
        "decision_reason": decision_reason,
        "candidate_positive_routes": [item for item in candidate_positive_routes if item],
        "candidate_negative_routes": [item for item in candidate_negative_routes if item],
        "candidate_missing_routes": [item for item in candidate_missing_routes if item],
        "candidate_runtime_interrupted_routes": [
            item for item in candidate_runtime_interrupted_routes if item
        ],
        "pending_rerun_routes": [item for item in pending_rerun_routes if item],
    }


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "route_id",
        "baseline_transport",
        "candidate_transport",
        "baseline_runtime",
        "candidate_runtime",
        "baseline_handoff",
        "candidate_handoff",
        "baseline_health",
        "candidate_health",
        "baseline_distance_m",
        "candidate_distance_m",
        "distance_delta_m",
        "baseline_max_speed_mps",
        "candidate_max_speed_mps",
        "speed_delta_mps",
        "candidate_direct_apply_count",
        "candidate_direct_apply_window_status",
        "candidate_direct_apply_frame_span",
        "candidate_direct_apply_max_speed_mps",
        "candidate_direct_apply_max_throttle",
        "candidate_direct_metric_consistency_status",
        "candidate_direct_metric_consistency_reasons",
        "candidate_materialization_status",
        "candidate_command_materialization_layer",
        "candidate_command_materialization_stage",
        "candidate_command_materialization_reason",
        "candidate_invalid_reason",
        "distance_quality",
        "verdict",
        "baseline_summary",
        "candidate_summary",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def write_markdown(path: Path, rows: Sequence[Dict[str, Any]], summary: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Transport A/B Summary",
        "",
        "## Aggregate",
        "",
        f"- route_count: `{summary['route_count']}`",
        f"- baseline_aligned_count: `{summary['baseline_aligned_count']}`",
        f"- candidate_aligned_count: `{summary['candidate_aligned_count']}`",
        f"- baseline_control_consuming_count: `{summary['baseline_control_consuming_count']}`",
        f"- candidate_control_consuming_count: `{summary['candidate_control_consuming_count']}`",
        f"- candidate_positive_count: `{summary['candidate_positive_count']}`",
        f"- candidate_negative_count: `{summary['candidate_negative_count']}`",
        f"- candidate_inconclusive_count: `{summary['candidate_inconclusive_count']}`",
        f"- verdict_counts: `{json.dumps(summary['verdict_counts'], sort_keys=True)}`",
        "",
        "## Decision",
        "",
        f"- decision_status: `{summary.get('decision_status', '')}`",
        f"- decision_reason: `{summary.get('decision_reason', '')}`",
        f"- pending_rerun_routes: `{', '.join(summary.get('pending_rerun_routes') or []) or 'none'}`",
        f"- candidate_positive_routes: `{', '.join(summary.get('candidate_positive_routes') or []) or 'none'}`",
        f"- candidate_negative_routes: `{', '.join(summary.get('candidate_negative_routes') or []) or 'none'}`",
        "",
        "## Routes",
        "",
        "| route_id | baseline | candidate | delta_m | distance_quality | direct_apply | materialization | verdict |",
        "|---|---:|---:|---:|---|---|---|---|",
    ]
    for row in rows:
        baseline_cell = f"{_format_value(row.get('baseline_distance_m'))}m / {_format_value(row.get('baseline_max_speed_mps'))}mps"
        candidate_cell = f"{_format_value(row.get('candidate_distance_m'))}m / {_format_value(row.get('candidate_max_speed_mps'))}mps"
        materialization_cell = row.get("candidate_materialization_status") or row.get("candidate_command_materialization_stage") or ""
        if row.get("candidate_invalid_reason"):
            materialization_cell = f"{materialization_cell}; invalid={row.get('candidate_invalid_reason')}"
        direct_apply_cell = "{} / span={} / vmax={}".format(
            row.get("candidate_direct_apply_window_status") or "",
            _format_value(row.get("candidate_direct_apply_frame_span")),
            _format_value(row.get("candidate_direct_apply_max_speed_mps")),
        )
        if row.get("candidate_direct_metric_consistency_status"):
            direct_apply_cell = f"{direct_apply_cell} / metric={row.get('candidate_direct_metric_consistency_status')}"
        lines.append(
            "| `{}` | {} | {} | {} | `{}` | `{}` | `{}` | `{}` |".format(
                row.get("route_id"),
                baseline_cell,
                candidate_cell,
                _format_value(row.get("distance_delta_m")),
                row.get("distance_quality") or "",
                direct_apply_cell,
                materialization_cell,
                row.get("verdict"),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze completed Town01 ros2_gt vs carla_direct A/B summary files.")
    parser.add_argument("--baseline-root", action="append", type=Path, required=True)
    parser.add_argument("--candidate-root", action="append", type=Path, required=True)
    parser.add_argument(
        "--route-id",
        action="append",
        default=[],
        help=(
            "Optional route id filter. May be repeated. Useful for analyzing a recovery rerun "
            "against a larger baseline pool without reporting unrelated missing candidate routes."
        ),
    )
    parser.add_argument("--csv-out", type=Path, required=True)
    parser.add_argument("--md-out", type=Path, required=True)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    rows = build_rows(args.baseline_root, args.candidate_root, route_ids=list(args.route_id or []))
    summary = summarize_rows(rows)
    write_csv(args.csv_out, rows)
    write_markdown(args.md_out, rows, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
