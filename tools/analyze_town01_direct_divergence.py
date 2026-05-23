#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _float_or_none(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _int_or_none(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _runtime_status(summary: Dict[str, Any]) -> str:
    contract = summary.get("runtime_contract")
    if isinstance(contract, dict):
        return str(contract.get("status") or "unknown")
    return str(summary.get("runtime_contract_status") or "unknown")


def _find_summary_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if resolved.is_file() and resolved.name == "summary.json":
        return resolved
    direct = resolved / "summary.json"
    if direct.exists():
        return direct
    candidates = list(resolved.rglob("summary.json"))
    if not candidates:
        raise FileNotFoundError(f"summary.json not found under {resolved}")
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _runtime_interruption_reason(summary_path: Path) -> str:
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


def _jsonl_stats(path: Path) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "count": 0,
        "first_frame": None,
        "last_frame": None,
        "frame_span": None,
        "max_speed_mps": None,
        "max_throttle": None,
        "max_brake": None,
        "max_abs_steer": None,
        "first_record": None,
        "last_record": None,
    }
    if not path.exists():
        return stats
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        frame = _int_or_none(record.get("frame_id") or record.get("frame"))
        speed = _float_or_none(record.get("speed_mps"))
        throttle = _float_or_none(record.get("throttle"))
        brake = _float_or_none(record.get("brake"))
        steer = _float_or_none(record.get("steer"))
        stats["count"] += 1
        if stats["first_record"] is None:
            stats["first_record"] = record
        stats["last_record"] = record
        if frame is not None:
            if stats["first_frame"] is None:
                stats["first_frame"] = frame
            stats["last_frame"] = frame
        if speed is not None:
            stats["max_speed_mps"] = max(stats["max_speed_mps"] or speed, speed)
        if throttle is not None:
            stats["max_throttle"] = max(stats["max_throttle"] or throttle, throttle)
        if brake is not None:
            stats["max_brake"] = max(stats["max_brake"] or brake, brake)
        if steer is not None:
            stats["max_abs_steer"] = max(stats["max_abs_steer"] or abs(steer), abs(steer))
    first_frame = stats.get("first_frame")
    last_frame = stats.get("last_frame")
    if first_frame is not None and last_frame is not None:
        stats["frame_span"] = int(last_frame) - int(first_frame) + 1
    return stats


def _csv_timeseries_stats(path: Path) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "row_count": 0,
        "first_frame": None,
        "last_frame": None,
        "frame_span": None,
        "max_v_mps": None,
        "max_cmd_throttle": None,
        "max_applied_throttle": None,
        "max_applied_brake": None,
        "max_abs_applied_steer": None,
    }
    if not path.exists():
        return stats
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            frame = _int_or_none(row.get("frame"))
            speed = _float_or_none(row.get("v_mps"))
            cmd_throttle = _float_or_none(row.get("cmd_throttle"))
            applied_throttle = _float_or_none(row.get("applied_throttle"))
            applied_brake = _float_or_none(row.get("applied_brake"))
            applied_steer = _float_or_none(row.get("applied_steer"))
            stats["row_count"] += 1
            if frame is not None:
                if stats["first_frame"] is None:
                    stats["first_frame"] = frame
                stats["last_frame"] = frame
            if speed is not None:
                stats["max_v_mps"] = max(stats["max_v_mps"] or speed, speed)
            if cmd_throttle is not None:
                stats["max_cmd_throttle"] = max(stats["max_cmd_throttle"] or cmd_throttle, cmd_throttle)
            if applied_throttle is not None:
                stats["max_applied_throttle"] = max(stats["max_applied_throttle"] or applied_throttle, applied_throttle)
            if applied_brake is not None:
                stats["max_applied_brake"] = max(stats["max_applied_brake"] or applied_brake, applied_brake)
            if applied_steer is not None:
                stats["max_abs_applied_steer"] = max(stats["max_abs_applied_steer"] or abs(applied_steer), abs(applied_steer))
    first_frame = stats.get("first_frame")
    last_frame = stats.get("last_frame")
    if first_frame is not None and last_frame is not None:
        stats["frame_span"] = int(last_frame) - int(first_frame) + 1
    return stats


def _choose_number(*values: Any) -> Optional[float]:
    for value in values:
        number = _float_or_none(value)
        if number is not None:
            return number
    return None


def build_snapshot(path: Path, label: str) -> Dict[str, Any]:
    summary_path = _find_summary_path(path)
    run_dir = summary_path.parent
    artifacts_dir = run_dir / "artifacts"
    summary = _load_json(summary_path)
    direct_stats = _load_json(artifacts_dir / "direct_bridge_stats.json")
    command = _load_json(artifacts_dir / "command_materialization_summary.json")
    handoff = _load_json(artifacts_dir / "control_handoff_summary.json")
    transport = _load_json(artifacts_dir / "bridge_transport_summary.json")
    direct_apply = _jsonl_stats(artifacts_dir / "direct_bridge_control_apply.jsonl")
    timeseries = _csv_timeseries_stats(run_dir / "timeseries.csv")

    route_id = str(summary.get("route_id") or summary.get("scenario_metadata", {}).get("route_id") or run_dir.name)
    direct_apply_count = _choose_number(
        summary.get("direct_control_apply_count"),
        direct_stats.get("control_apply_count"),
        direct_apply.get("count"),
    )
    direct_apply_span = _choose_number(
        summary.get("direct_control_apply_frame_span"),
        direct_stats.get("control_apply_frame_span"),
        direct_apply.get("frame_span"),
    )
    direct_apply_max_speed = _choose_number(
        summary.get("direct_control_apply_max_speed_mps"),
        direct_stats.get("control_apply_max_speed_mps"),
        direct_apply.get("max_speed_mps"),
    )
    direct_apply_max_throttle = _choose_number(
        summary.get("direct_control_apply_max_throttle"),
        direct_stats.get("control_apply_max_throttle"),
        direct_apply.get("max_throttle"),
    )

    return {
        "label": label,
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "route_id": route_id,
        "transport_mode": summary.get("transport_mode") or transport.get("transport_mode") or "",
        "runtime_status": _runtime_status(summary),
        "runtime_interruption_reason": _runtime_interruption_reason(summary_path),
        "control_handoff_status": summary.get("control_handoff_status") or handoff.get("control_handoff_status") or "",
        "route_health_label": summary.get("route_health_label") or "",
        "materialization_status": summary.get("materialization_status") or "",
        "command_layer": summary.get("command_materialization_layer") or command.get("command_path_layer") or "",
        "command_stage": summary.get("command_materialization_stage") or command.get("command_path_stage") or "",
        "command_reason": summary.get("command_materialization_reason") or command.get("last_error") or "",
        "route_distance_m": _float_or_none(summary.get("route_distance_achieved_m")),
        "route_completion_ratio": _float_or_none(summary.get("route_completion_ratio")),
        "max_speed_mps": _float_or_none(summary.get("max_speed_mps")),
        "planning_nonzero_ratio": _float_or_none(summary.get("planning_nonzero_ratio")),
        "control_used_planning_ratio": _float_or_none(summary.get("control_used_planning_ratio")),
        "routing_request_count": _int_or_none(summary.get("routing_request_count")),
        "routing_success_count": _int_or_none(summary.get("routing_success_count")),
        "direct_apply_count": direct_apply_count,
        "direct_apply_window_status": summary.get("direct_control_apply_window_status") or "",
        "direct_apply_frame_span": direct_apply_span,
        "direct_apply_first_frame": _choose_number(
            summary.get("direct_control_first_apply_frame"),
            direct_stats.get("control_apply_first_frame"),
            direct_apply.get("first_frame"),
        ),
        "direct_apply_last_frame": _choose_number(
            summary.get("direct_control_last_apply_frame"),
            direct_stats.get("control_apply_last_frame"),
            direct_apply.get("last_frame"),
        ),
        "direct_apply_max_speed_mps": direct_apply_max_speed,
        "direct_apply_max_throttle": direct_apply_max_throttle,
        "direct_apply_max_brake": _choose_number(direct_apply.get("max_brake")),
        "direct_apply_max_abs_steer": _choose_number(direct_apply.get("max_abs_steer")),
        "direct_snapshot_count": _int_or_none(direct_stats.get("snapshot_count")),
        "direct_ego_discovery_fail_count": _int_or_none(direct_stats.get("ego_discovery_fail_count")),
        "timeseries_row_count": timeseries.get("row_count"),
        "timeseries_frame_span": timeseries.get("frame_span"),
        "timeseries_max_v_mps": timeseries.get("max_v_mps"),
        "timeseries_max_cmd_throttle": timeseries.get("max_cmd_throttle"),
        "timeseries_max_applied_throttle": timeseries.get("max_applied_throttle"),
        "timeseries_max_applied_brake": timeseries.get("max_applied_brake"),
        "timeseries_max_abs_applied_steer": timeseries.get("max_abs_applied_steer"),
    }


def _delta(right: Optional[float], left: Optional[float]) -> Optional[float]:
    if right is None or left is None:
        return None
    return right - left


def _is_direct_apply_metric_mismatch(snapshot: Dict[str, Any]) -> bool:
    if str(snapshot.get("transport_mode") or "").lower() != "carla_direct":
        return False
    span = _float_or_none(snapshot.get("direct_apply_frame_span")) or 0.0
    if span >= 20.0:
        return False
    summary_speed = _float_or_none(snapshot.get("max_speed_mps")) or 0.0
    direct_speed = _float_or_none(snapshot.get("direct_apply_max_speed_mps")) or 0.0
    timeseries_speed = _float_or_none(snapshot.get("timeseries_max_v_mps")) or 0.0
    distance = abs(_float_or_none(snapshot.get("route_distance_m")) or 0.0)
    if summary_speed - direct_speed >= 2.0:
        return True
    if timeseries_speed - direct_speed >= 2.0:
        return True
    if span > 0.0 and distance >= 20.0:
        return True
    return False


def compare_snapshots(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    distance_delta = _delta(_float_or_none(right.get("route_distance_m")), _float_or_none(left.get("route_distance_m")))
    speed_delta = _delta(_float_or_none(right.get("max_speed_mps")), _float_or_none(left.get("max_speed_mps")))
    planning_delta = _delta(
        _float_or_none(right.get("planning_nonzero_ratio")),
        _float_or_none(left.get("planning_nonzero_ratio")),
    )
    direct_apply_span_delta = _delta(
        _float_or_none(right.get("direct_apply_frame_span")),
        _float_or_none(left.get("direct_apply_frame_span")),
    )

    if right.get("runtime_interruption_reason"):
        verdict = "right_runtime_interrupted"
        explanation = f"right run was interrupted by {right.get('runtime_interruption_reason')}; distance deltas are not capability evidence"
    elif _is_direct_apply_metric_mismatch(right):
        verdict = "right_direct_short_apply_metric_mismatch"
        explanation = "right run reports route progress/speed that is not supported by its short direct apply window"
    elif right.get("runtime_status") != "aligned" and left.get("runtime_status") == "aligned":
        verdict = "right_runtime_regression"
        explanation = "right run lost aligned runtime while left remained aligned"
    elif (
        left.get("control_handoff_status") == "control_consuming_with_nonzero_planning"
        and right.get("control_handoff_status") != "control_consuming_with_nonzero_planning"
    ):
        verdict = "right_control_materialization_regression"
        explanation = "right run did not preserve planning-to-control handoff"
    elif (
        str(right.get("transport_mode") or "").lower() == "carla_direct"
        and (_float_or_none(right.get("direct_apply_frame_span")) or 0.0) >= 20.0
        and distance_delta is not None
        and distance_delta <= -5.0
    ):
        verdict = "right_direct_long_apply_lower_distance"
        explanation = "right direct run applied control over an observable window but covered less route distance"
    elif distance_delta is not None and distance_delta >= 5.0:
        verdict = "right_positive_distance"
        explanation = "right run covered materially more route distance"
    elif speed_delta is not None and speed_delta >= 0.5 and (distance_delta is None or distance_delta >= -2.0):
        verdict = "right_positive_speed"
        explanation = "right run is faster without a material distance regression"
    else:
        verdict = "inconclusive"
        explanation = "no single dominant divergence is proven by the available summaries"

    return {
        "left_label": left["label"],
        "right_label": right["label"],
        "route_id_left": left.get("route_id"),
        "route_id_right": right.get("route_id"),
        "distance_delta_m": distance_delta,
        "speed_delta_mps": speed_delta,
        "planning_nonzero_ratio_delta": planning_delta,
        "direct_apply_frame_span_delta": direct_apply_span_delta,
        "verdict": verdict,
        "explanation": explanation,
    }


def build_report(left_run: Path, left_label: str, right_run: Path, right_label: str) -> Dict[str, Any]:
    left = build_snapshot(left_run, left_label)
    right = build_snapshot(right_run, right_label)
    return {
        "left": left,
        "right": right,
        "comparison": compare_snapshots(left, right),
    }


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _field_rows(fields: Iterable[str], left: Dict[str, Any], right: Dict[str, Any]) -> List[str]:
    rows: List[str] = []
    for field in fields:
        rows.append(
            "| `{}` | `{}` | `{}` |".format(
                field,
                _format_value(left.get(field)),
                _format_value(right.get(field)),
            )
        )
    return rows


def write_markdown(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    left = report["left"]
    right = report["right"]
    comparison = report["comparison"]
    fields = [
        "transport_mode",
        "runtime_status",
        "runtime_interruption_reason",
        "control_handoff_status",
        "route_health_label",
        "materialization_status",
        "command_layer",
        "command_stage",
        "route_distance_m",
        "route_completion_ratio",
        "max_speed_mps",
        "planning_nonzero_ratio",
        "control_used_planning_ratio",
        "routing_request_count",
        "routing_success_count",
        "direct_apply_count",
        "direct_apply_window_status",
        "direct_apply_frame_span",
        "direct_apply_max_speed_mps",
        "direct_apply_max_throttle",
        "direct_snapshot_count",
        "direct_ego_discovery_fail_count",
        "timeseries_row_count",
        "timeseries_frame_span",
        "timeseries_max_v_mps",
    ]
    lines = [
        "# Town01 Direct Divergence Report",
        "",
        "## Verdict",
        "",
        f"- verdict: `{comparison['verdict']}`",
        f"- explanation: {comparison['explanation']}",
        f"- distance_delta_m: `{_format_value(comparison.get('distance_delta_m'))}`",
        f"- speed_delta_mps: `{_format_value(comparison.get('speed_delta_mps'))}`",
        f"- planning_nonzero_ratio_delta: `{_format_value(comparison.get('planning_nonzero_ratio_delta'))}`",
        f"- direct_apply_frame_span_delta: `{_format_value(comparison.get('direct_apply_frame_span_delta'))}`",
        "",
        "## Runs",
        "",
        f"- left `{left['label']}`: `{left['summary_path']}`",
        f"- right `{right['label']}`: `{right['summary_path']}`",
        "",
        "## Field Comparison",
        "",
        f"| field | {left['label']} | {right['label']} |",
        "|---|---:|---:|",
    ]
    lines.extend(_field_rows(fields, left, right))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_json(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare two Town01 runs and classify first direct-chain divergence.")
    parser.add_argument("--left-run", type=Path, required=True)
    parser.add_argument("--left-label", required=True)
    parser.add_argument("--right-run", type=Path, required=True)
    parser.add_argument("--right-label", required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--md-out", type=Path, required=True)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    report = build_report(args.left_run, args.left_label, args.right_run, args.right_label)
    write_json(args.json_out, report)
    write_markdown(args.md_out, report)
    print(json.dumps(report["comparison"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
