from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

BAGUANG_APOLLO_LATERAL_BLOCKER_SCHEMA_VERSION = "baguang_apollo_lateral_blocker.v1"
DEFAULT_NO_LATERAL_RUN = Path(
    "runs/assist_reduction/baguang_online_20260531_apollo_no_lateral/apollo/"
    "apollo_no_lateral_stabilizer/"
    "baguang_assist_reduction_20260531_114748__apollo_no_lateral_stabilizer__02"
)


def analyze_baguang_apollo_lateral_blocker(run_dir: str | Path = DEFAULT_NO_LATERAL_RUN) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    summary = _read_json(root / "summary.json")
    direct_stats = _read_json(root / "artifacts" / "direct_bridge_stats.json")
    cyber_stats = _read_json(root / "artifacts" / "cyber_bridge_stats.json")
    planning_summary = _read_json(root / "artifacts" / "planning_topic_debug_summary.json")
    map_contract = _read_json(root / "artifacts" / "map_contract_guard.json")
    timeseries_rows = _read_csv_rows(root / "timeseries.csv")
    bridge_decode_rows = _read_jsonl(root / "artifacts" / "bridge_control_decode.jsonl")
    direct_apply_rows = _read_jsonl(root / "artifacts" / "direct_bridge_control_apply.jsonl")
    planning_debug_rows = _read_jsonl(root / "artifacts" / "planning_topic_debug.jsonl")

    runtime_chain = _runtime_chain(summary, direct_stats, cyber_stats, planning_summary, map_contract)
    lateral = _lateral_divergence(timeseries_rows)
    control = _control_semantics(timeseries_rows, bridge_decode_rows, direct_apply_rows, direct_stats)
    planning = _planning_semantics(planning_summary, planning_debug_rows)
    status, reason = _verdict(runtime_chain, lateral, control)
    findings = _findings(runtime_chain, lateral, control, planning)
    return {
        "schema_version": BAGUANG_APOLLO_LATERAL_BLOCKER_SCHEMA_VERSION,
        "run_dir": str(root),
        "status": status,
        "reason": reason,
        "runtime_chain": runtime_chain,
        "lateral_divergence": lateral,
        "control_semantics": control,
        "planning_semantics": planning,
        "findings": findings,
        "claim_boundary": {
            "proves_apollo_algorithm_lateral_limitation": False,
            "reason": (
                "This report localizes the no-lateral-stabilizer failure. It does not by itself "
                "separate Apollo algorithm capability from map/reference-line/input-contract issues."
            ),
        },
        "missing_inputs": _missing_inputs(root),
    }


def write_baguang_apollo_lateral_blocker_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "baguang_apollo_lateral_blocker_report.json"
    md_path = out / "baguang_apollo_lateral_blocker_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _runtime_chain(
    summary: Mapping[str, Any],
    direct_stats: Mapping[str, Any],
    cyber_stats: Mapping[str, Any],
    planning_summary: Mapping[str, Any],
    map_contract: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "summary_success": summary.get("success"),
        "fail_reason": summary.get("fail_reason"),
        "exit_reason": summary.get("exit_reason"),
        "collision_count": _number(summary.get("collision_count")),
        "routing_request_count": _number(cyber_stats.get("routing_request_count")),
        "routing_success_count": _number(cyber_stats.get("routing_success_count")),
        "planning_total_messages": _number(planning_summary.get("total_messages_received")),
        "planning_nonzero_messages": _number(planning_summary.get("messages_with_nonzero_trajectory_points")),
        "control_rx_count": _number(cyber_stats.get("control_rx_count")),
        "control_tx_count": _number(cyber_stats.get("control_tx_count")),
        "direct_control_apply_count": _number(direct_stats.get("control_apply_count")),
        "direct_control_apply_fail_count": _number(direct_stats.get("control_apply_fail_count")),
        "straight_lane_lateral_stabilizer_enabled": direct_stats.get("straight_lane_lateral_stabilizer_enabled"),
        "straight_lane_lateral_stabilizer_apply_count": _number(
            direct_stats.get("straight_lane_lateral_stabilizer_apply_count")
        ),
        "apollo_map_complete": map_contract.get("effective_bridge_map_complete"),
        "container_runtime_map_complete": map_contract.get("container_runtime_map_complete"),
    }


def _lateral_divergence(rows: list[dict[str, str]]) -> dict[str, Any]:
    cte = _numeric_column(rows, "cross_track_error")
    heading = _numeric_column(rows, "heading_error")
    speed = _numeric_column(rows, "ego_speed", "v_mps")
    return {
        "timeseries_rows": len(rows),
        "cross_track_error": _series_stats(cte),
        "heading_error": _series_stats(heading),
        "ego_speed": _series_stats(speed),
        "first_cross_track_thresholds": {
            str(threshold): _first_threshold(rows, "cross_track_error", threshold)
            for threshold in (0.5, 1.0, 5.0, 10.0, 20.0, 40.0)
        },
        "collision_row": _first_count_row(rows, "collision_count"),
    }


def _control_semantics(
    timeseries_rows: list[dict[str, str]],
    bridge_decode_rows: list[dict[str, Any]],
    direct_apply_rows: list[dict[str, Any]],
    direct_stats: Mapping[str, Any],
) -> dict[str, Any]:
    p0_apollo_steer = _numeric_column(timeseries_rows, "apollo_steer_raw")
    p0_applied_steer = _numeric_column(timeseries_rows, "carla_steer_applied", "applied_steer")
    raw_steer = _numeric_json_field(bridge_decode_rows, "raw_steer")
    commanded_steer = _numeric_json_field(bridge_decode_rows, "commanded_steer")
    direct_source_steer = _numeric_json_field(direct_apply_rows, "source_steer")
    direct_applied_steer = _numeric_json_field(direct_apply_rows, "steer")
    guard_counts = {
        "straight_lane_zero_steer": sum(1 for row in bridge_decode_rows if row.get("straight_lane_zero_steer_applied")),
        "low_speed_steer_guard": sum(1 for row in bridge_decode_rows if row.get("low_speed_steer_guard_applied")),
        "sustained_lateral_guard": sum(1 for row in bridge_decode_rows if row.get("sustained_lateral_guard_applied")),
        "trajectory_contract_guard": sum(
            1 for row in bridge_decode_rows if row.get("trajectory_contract_lateral_guard_applied")
        ),
        "force_zero_steer": sum(1 for row in bridge_decode_rows if row.get("force_zero_steer_applied")),
    }
    p0_raw_is_zero = bool(p0_apollo_steer) and max(abs(value) for value in p0_apollo_steer) == 0.0
    bridge_raw_nonzero = bool(raw_steer) and max(abs(value) for value in raw_steer) > 1e-6
    return {
        "timeseries_apollo_steer_raw": _series_stats(p0_apollo_steer),
        "timeseries_carla_steer_applied": _series_stats(p0_applied_steer),
        "bridge_decode_raw_steer": _series_stats(raw_steer),
        "bridge_decode_commanded_steer": _series_stats(commanded_steer),
        "direct_apply_source_steer": _series_stats(direct_source_steer),
        "direct_apply_steer": _series_stats(direct_applied_steer),
        "guard_counts": guard_counts,
        "straight_lane_lateral_stabilizer": {
            "enabled": direct_stats.get("straight_lane_lateral_stabilizer_enabled"),
            "apply_count": _number(direct_stats.get("straight_lane_lateral_stabilizer_apply_count")),
        },
        "p0_timeseries_raw_steer_mismatch": p0_raw_is_zero and bridge_raw_nonzero,
        "p0_timeseries_raw_steer_mismatch_reason": (
            "timeseries apollo_steer_raw is zero while bridge_control_decode.raw_steer is nonzero"
            if p0_raw_is_zero and bridge_raw_nonzero
            else ""
        ),
    }


def _planning_semantics(
    planning_summary: Mapping[str, Any],
    planning_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    status_counts = Counter(str(row.get("trajectory_header_status") or "<empty>") for row in planning_rows)
    point_counts = _numeric_json_field(planning_rows, "trajectory_point_count")
    path_lengths = _numeric_json_field(planning_rows, "trajectory_total_path_length")
    return {
        "summary_status": planning_summary.get("summary_status"),
        "total_messages_received": planning_summary.get("total_messages_received"),
        "messages_with_nonzero_trajectory_points": planning_summary.get("messages_with_nonzero_trajectory_points"),
        "last_routing_unique_lane_signature": planning_summary.get("last_routing_unique_lane_signature"),
        "last_routing_total_length": planning_summary.get("last_routing_total_length"),
        "trajectory_header_status_top": status_counts.most_common(8),
        "trajectory_point_count": _series_stats(point_counts),
        "trajectory_total_path_length": _series_stats(path_lengths),
    }


def _verdict(
    runtime_chain: Mapping[str, Any],
    lateral: Mapping[str, Any],
    control: Mapping[str, Any],
) -> tuple[str, str]:
    if runtime_chain.get("summary_success") is not False:
        return "insufficient_data", "run_is_not_a_failed_no_lateral_sample"
    if not runtime_chain.get("direct_control_apply_count"):
        return "insufficient_data", "direct_control_apply_missing"
    cte_stats = _as_mapping(lateral.get("cross_track_error"))
    if _number(cte_stats.get("max_abs")) < 5.0:
        return "warn", "no_large_lateral_divergence_detected"
    if control.get("straight_lane_lateral_stabilizer", {}).get("enabled") is False:
        return "fail", "lateral_divergence_after_stabilizer_removed"
    return "warn", "lateral_divergence_detected_with_unclear_stabilizer_state"


def _findings(
    runtime_chain: Mapping[str, Any],
    lateral: Mapping[str, Any],
    control: Mapping[str, Any],
    planning: Mapping[str, Any],
) -> list[str]:
    findings: list[str] = []
    if runtime_chain.get("routing_success_count"):
        findings.append("routing_materialized")
    if runtime_chain.get("planning_nonzero_messages"):
        findings.append("planning_nonempty_available")
    if runtime_chain.get("control_tx_count") and runtime_chain.get("direct_control_apply_count"):
        findings.append("control_to_carla_actuation_materialized")
    if control.get("straight_lane_lateral_stabilizer", {}).get("enabled") is False:
        findings.append("straight_lane_lateral_stabilizer_disabled")
    if control.get("p0_timeseries_raw_steer_mismatch"):
        findings.append("timeseries_p0_apollo_steer_raw_mismatch")
    if _number(_as_mapping(lateral.get("cross_track_error")).get("max_abs")) >= 5.0:
        findings.append("large_lateral_divergence")
    statuses = [item[0] for item in planning.get("trajectory_header_status_top") or []]
    if "planner failed to make a driving plan" in statuses:
        findings.append("planning_reported_driving_plan_failures")
    if "Fail to shrink routing segments." in statuses:
        findings.append("planning_reported_route_segment_shrink_failures")
    return findings


def _series_stats(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "max": None, "max_abs": None, "p95_abs": None}
    abs_values = sorted(abs(value) for value in values)
    p95_index = int(0.95 * (len(abs_values) - 1))
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "max_abs": max(abs_values),
        "p95_abs": abs_values[p95_index],
    }


def _first_threshold(rows: list[dict[str, str]], field: str, threshold: float) -> dict[str, Any] | None:
    for row in rows:
        value = _number(row.get(field))
        if abs(value) < threshold:
            continue
        return _row_snapshot(row)
    return None


def _first_count_row(rows: list[dict[str, str]], field: str) -> dict[str, Any] | None:
    for row in rows:
        if _number(row.get(field)) > 0:
            return _row_snapshot(row)
    return None


def _row_snapshot(row: Mapping[str, str]) -> dict[str, Any]:
    keys = (
        "frame_id",
        "frame",
        "sim_time",
        "ego_x",
        "ego_y",
        "ego_speed",
        "cross_track_error",
        "heading_error",
        "apollo_steer_raw",
        "carla_steer_applied",
        "collision_count",
    )
    return {key: _number(row.get(key)) for key in keys if row.get(key) not in (None, "")}


def _numeric_column(rows: Sequence[Mapping[str, str]], *names: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        for name in names:
            value = _maybe_number(row.get(name))
            if value is not None:
                values.append(value)
                break
    return values


def _numeric_json_field(rows: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _maybe_number(row.get(field))
        if value is not None:
            values.append(value)
    return values


def _missing_inputs(root: Path) -> list[str]:
    rels = (
        "summary.json",
        "timeseries.csv",
        "artifacts/bridge_control_decode.jsonl",
        "artifacts/direct_bridge_control_apply.jsonl",
        "artifacts/direct_bridge_stats.json",
        "artifacts/cyber_bridge_stats.json",
        "artifacts/planning_topic_debug_summary.json",
        "artifacts/planning_topic_debug.jsonl",
    )
    return [rel for rel in rels if not (root / rel).exists()]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _maybe_number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _number(value: Any, default: float = 0.0) -> float:
    parsed = _maybe_number(value)
    return default if parsed is None else parsed


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lateral = _as_mapping(report.get("lateral_divergence"))
    control = _as_mapping(report.get("control_semantics"))
    runtime = _as_mapping(report.get("runtime_chain"))
    planning = _as_mapping(report.get("planning_semantics"))
    lines = [
        "# Baguang Apollo Lateral Blocker",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        f"- run_dir: `{report.get('run_dir')}`",
        f"- routing_success_count: `{runtime.get('routing_success_count')}`",
        f"- planning_nonzero_messages: `{runtime.get('planning_nonzero_messages')}`",
        f"- control_tx_count: `{runtime.get('control_tx_count')}`",
        f"- direct_control_apply_count: `{runtime.get('direct_control_apply_count')}`",
        f"- stabilizer_enabled: `{_as_mapping(control.get('straight_lane_lateral_stabilizer')).get('enabled')}`",
        f"- max_abs_cross_track_error_m: `{_as_mapping(lateral.get('cross_track_error')).get('max_abs')}`",
        f"- max_abs_heading_error_rad: `{_as_mapping(lateral.get('heading_error')).get('max_abs')}`",
        f"- timeseries_raw_steer_mismatch: `{control.get('p0_timeseries_raw_steer_mismatch')}`",
        f"- planning_status_top: `{planning.get('trajectory_header_status_top')}`",
        "",
        "## Findings",
    ]
    lines.extend(f"- `{item}`" for item in report.get("findings") or [])
    lines.extend(
        [
            "",
            "## Claim Boundary",
            "",
            "This report localizes the no-lateral-stabilizer failure; it does not by itself prove an Apollo algorithm limitation independent of map/reference-line/input-contract issues.",
            "",
        ]
    )
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Baguang Apollo no-lateral-stabilizer blocker artifacts.")
    parser.add_argument("--run-dir", default=str(DEFAULT_NO_LATERAL_RUN))
    parser.add_argument("--out", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = analyze_baguang_apollo_lateral_blocker(args.run_dir)
    outputs = write_baguang_apollo_lateral_blocker_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "reason": report.get("reason"), "outputs": outputs}, indent=2))
    return 1 if report.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
