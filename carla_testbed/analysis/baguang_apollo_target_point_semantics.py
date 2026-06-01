from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from .baguang_apollo_lateral_blocker import DEFAULT_NO_LATERAL_RUN
from .baguang_apollo_lateral_stabilizer_ab import DEFAULT_STABILIZER_RUN

BAGUANG_APOLLO_TARGET_POINT_SEMANTICS_SCHEMA_VERSION = "baguang_apollo_target_point_semantics.v1"


def analyze_baguang_apollo_target_point_semantics(
    *,
    no_lateral_run: str | Path = DEFAULT_NO_LATERAL_RUN,
    stabilizer_run: str | Path = DEFAULT_STABILIZER_RUN,
    high_steer_threshold: float = 20.0,
    kappa_spike_threshold: float = 0.05,
) -> dict[str, Any]:
    runs = {
        "no_lateral": analyze_baguang_apollo_target_point_semantics_run(
            no_lateral_run,
            high_steer_threshold=high_steer_threshold,
            kappa_spike_threshold=kappa_spike_threshold,
        ),
        "stabilizer_enabled": analyze_baguang_apollo_target_point_semantics_run(
            stabilizer_run,
            high_steer_threshold=high_steer_threshold,
            kappa_spike_threshold=kappa_spike_threshold,
        ),
    }
    findings = _pair_findings(runs)
    return {
        "schema_version": BAGUANG_APOLLO_TARGET_POINT_SEMANTICS_SCHEMA_VERSION,
        "status": "warn" if findings else "pass",
        "reason": "target_point_semantics_require_audit" if findings else "no_target_point_semantics_anomaly",
        "thresholds": {
            "high_steer": float(high_steer_threshold),
            "kappa_spike": float(kappa_spike_threshold),
        },
        "runs": runs,
        "findings": findings,
        "claim_boundary": {
            "proves_apollo_algorithm_limitation": False,
            "reason": (
                "This report connects Apollo control debug target points to planning trajectory debug. "
                "It does not by itself separate Apollo algorithm behavior from map, reference-line, "
                "localization-frame, or adapter contract issues."
            ),
        },
    }


def analyze_baguang_apollo_target_point_semantics_run(
    run_dir: str | Path,
    *,
    high_steer_threshold: float = 20.0,
    kappa_spike_threshold: float = 0.05,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    summary = _read_json(root / "summary.json")
    control_rows = _control_rows(root / "artifacts" / "apollo_control_raw.jsonl")
    planning_rows = _read_jsonl(root / "artifacts" / "planning_topic_debug.jsonl")
    consume_rows = _read_jsonl(root / "artifacts" / "control_trajectory_consume_debug_live.jsonl")
    lateral_geometry_rows = _read_csv_rows(root / "artifacts" / "lateral_geometry_debug.csv")

    planning_by_sequence = {
        row.get("planning_header_sequence_num"): row
        for row in planning_rows
        if row.get("planning_header_sequence_num") is not None
    }
    consume_by_cycle = {
        int(_num(row.get("control_cycle_index"))): row
        for row in consume_rows
        if row.get("control_cycle_index") is not None
    }
    high_control_rows = [
        row for row in control_rows if abs(_num(row.get("steering_target"))) >= float(high_steer_threshold)
    ]
    alignments = [_alignment(row, planning_by_sequence, consume_by_cycle) for row in high_control_rows]
    reference_curvature = _numeric_csv(lateral_geometry_rows, "reference_lane_curvature")
    planning_first_kappa = _numeric(planning_rows, "first_trajectory_point_kappa")
    findings = _run_findings(
        alignments,
        planning_rows,
        planning_first_kappa,
        reference_curvature,
        high_control_rows,
        kappa_spike_threshold=kappa_spike_threshold,
    )
    return {
        "run_dir": str(root),
        "summary": {
            "success": summary.get("success"),
            "exit_reason": summary.get("exit_reason"),
            "fail_reason": summary.get("fail_reason"),
            "collision_count": summary.get("collision_count"),
        },
        "planning": _planning_summary(planning_rows),
        "control": {
            "control_raw_count": len(control_rows),
            "high_steer_count": len(high_control_rows),
            "high_steer_ratio": (len(high_control_rows) / len(control_rows)) if control_rows else None,
            "steering_target": _series_stats(_numeric(control_rows, "steering_target")),
            "target_point_kappa": _series_stats(_numeric(control_rows, "debug_simple_lat_current_target_point_kappa")),
            "target_point_s": _series_stats(_numeric(control_rows, "debug_simple_lat_current_target_point_s")),
            "lateral_error": _series_stats(_numeric(control_rows, "debug_simple_lat_lateral_error")),
            "heading_error": _series_stats(_numeric(control_rows, "debug_simple_lat_heading_error")),
        },
        "high_steer_alignment": _alignment_summary(alignments),
        "lateral_geometry": {
            "reference_lane_curvature": _series_stats(reference_curvature),
        },
        "samples": alignments[:5],
        "findings": findings,
        "missing_inputs": _missing_inputs(root),
    }


def write_baguang_apollo_target_point_semantics_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "baguang_apollo_target_point_semantics_report.json"
    md_path = out / "baguang_apollo_target_point_semantics_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _alignment(
    control: Mapping[str, Any],
    planning_by_sequence: Mapping[Any, Mapping[str, Any]],
    consume_by_cycle: Mapping[int, Mapping[str, Any]],
) -> dict[str, Any]:
    control_seq = int(_num(control.get("control_header_sequence_num")))
    planning_seq = control.get("debug_input_trajectory_header_sequence_num")
    planning = planning_by_sequence.get(planning_seq, {})
    consume = consume_by_cycle.get(control_seq, {})
    control_kappa = _maybe_num(control.get("debug_simple_lat_current_target_point_kappa"))
    planning_kappa = _maybe_num(planning.get("first_trajectory_point_kappa"))
    control_theta = _maybe_num(control.get("debug_simple_lat_current_target_point_theta"))
    planning_theta = _maybe_num(planning.get("first_trajectory_point_theta"))
    return {
        "control_sequence": control_seq,
        "steering_target": _maybe_num(control.get("steering_target")),
        "input_planning_sequence": planning_seq,
        "planning_found": bool(planning),
        "planning_trajectory_point_count": planning.get("trajectory_point_count"),
        "planning_reference_line_count": planning.get("reference_line_count"),
        "planning_status": planning.get("trajectory_header_status"),
        "control_target_kappa": control_kappa,
        "planning_first_point_kappa": planning_kappa,
        "target_kappa_matches_planning_first_point": _close(control_kappa, planning_kappa),
        "control_target_theta": control_theta,
        "planning_first_point_theta": planning_theta,
        "target_theta_matches_planning_first_point": _close(control_theta, planning_theta),
        "control_target_s": control.get("debug_simple_lat_current_target_point_s"),
        "control_target_relative_time": control.get("debug_simple_lat_current_target_point_relative_time"),
        "lateral_error": control.get("debug_simple_lat_lateral_error"),
        "heading_error": control.get("debug_simple_lat_heading_error"),
        "consume_found": bool(consume),
        "consume_matched_planning_exactly": consume.get("matched_planning_event_found_exactly"),
        "consume_used_planning_trajectory": consume.get("control_used_planning_trajectory"),
        "consume_used_cached_trajectory": consume.get("control_used_cached_trajectory"),
        "consume_trajectory_time_window_valid": consume.get("trajectory_time_window_valid"),
        "consume_trajectory_not_started_yet": consume.get("trajectory_not_started_yet"),
        "consume_latest_planning_age_ms": consume.get("latest_planning_msg_age_ms"),
        "consume_control_message_age_ms": consume.get("control_message_age_ms"),
    }


def _planning_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    ref_counts = _numeric(rows, "reference_line_count")
    point_counts = _numeric(rows, "trajectory_point_count")
    for row in rows:
        status = str(row.get("trajectory_header_status") or "<empty>")
        statuses[status] = statuses.get(status, 0) + 1
    return {
        "rows": len(rows),
        "nonempty_trajectory_rows": sum(1 for value in point_counts if value > 0),
        "reference_line_count": _series_stats(ref_counts),
        "reference_line_nonzero_rows": sum(1 for value in ref_counts if value > 0),
        "trajectory_point_count": _series_stats(point_counts),
        "first_trajectory_point_kappa": _series_stats(_numeric(rows, "first_trajectory_point_kappa")),
        "first_trajectory_point_theta": _series_stats(_numeric(rows, "first_trajectory_point_theta")),
        "status_counts": dict(sorted(statuses.items(), key=lambda item: item[1], reverse=True)[:8]),
    }


def _alignment_summary(alignments: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(alignments)
    return {
        "count": count,
        "planning_found_count": sum(1 for row in alignments if row.get("planning_found")),
        "planning_nonempty_count": sum(1 for row in alignments if _num(row.get("planning_trajectory_point_count")) > 0),
        "reference_line_zero_count": sum(1 for row in alignments if _num(row.get("planning_reference_line_count")) == 0),
        "target_kappa_matches_planning_first_point_count": sum(
            1 for row in alignments if row.get("target_kappa_matches_planning_first_point")
        ),
        "target_theta_matches_planning_first_point_count": sum(
            1 for row in alignments if row.get("target_theta_matches_planning_first_point")
        ),
        "consume_found_count": sum(1 for row in alignments if row.get("consume_found")),
        "consume_matched_planning_exactly_count": sum(
            1 for row in alignments if row.get("consume_matched_planning_exactly")
        ),
        "consume_used_planning_trajectory_count": sum(
            1 for row in alignments if row.get("consume_used_planning_trajectory")
        ),
        "consume_trajectory_time_window_valid_count": sum(
            1 for row in alignments if row.get("consume_trajectory_time_window_valid")
        ),
        "consume_latest_planning_age_ms": _series_stats(_numeric(alignments, "consume_latest_planning_age_ms")),
        "consume_control_message_age_ms": _series_stats(_numeric(alignments, "consume_control_message_age_ms")),
        "control_target_kappa": _series_stats(_numeric(alignments, "control_target_kappa")),
        "planning_first_point_kappa": _series_stats(_numeric(alignments, "planning_first_point_kappa")),
    }


def _run_findings(
    alignments: Sequence[Mapping[str, Any]],
    planning_rows: Sequence[Mapping[str, Any]],
    planning_first_kappa: Sequence[float],
    reference_curvature: Sequence[float],
    high_control_rows: Sequence[Mapping[str, Any]],
    *,
    kappa_spike_threshold: float,
) -> list[str]:
    findings: list[str] = []
    if high_control_rows:
        findings.append("high_steer_present")
    if planning_rows and not any(_num(row.get("reference_line_count")) > 0 for row in planning_rows):
        findings.append("planning_reference_line_debug_missing_or_zero")
    if max((abs(value) for value in planning_first_kappa), default=0.0) >= kappa_spike_threshold and max(
        (abs(value) for value in reference_curvature),
        default=0.0,
    ) < 0.001:
        findings.append("planning_first_point_kappa_spike_on_straight_reference")
    if alignments and sum(1 for row in alignments if row.get("target_kappa_matches_planning_first_point")) == len(
        alignments
    ):
        findings.append("control_target_kappa_tracks_planning_first_point_kappa")
    if alignments and sum(1 for row in alignments if row.get("consume_used_planning_trajectory")) > 0:
        findings.append("high_steer_controls_used_planning_trajectory")
    return findings


def _pair_findings(runs: Mapping[str, Mapping[str, Any]]) -> list[str]:
    findings: list[str] = []
    all_run_findings = [item for run in runs.values() for item in run.get("findings", [])]
    if "planning_first_point_kappa_spike_on_straight_reference" in all_run_findings:
        findings.append("target_point_kappa_semantics_suspect")
    if "control_target_kappa_tracks_planning_first_point_kappa" in all_run_findings:
        findings.append("high_steer_tracks_planning_first_point_kappa")
    if "planning_reference_line_debug_missing_or_zero" in all_run_findings:
        findings.append("reference_line_debug_gap_blocks_full_attribution")
    if "high_steer_controls_used_planning_trajectory" in all_run_findings:
        findings.append("high_steer_not_explained_by_missing_control_handoff")
    return findings


def _control_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _read_jsonl(path):
        payload = row.get("apollo_control_raw")
        rows.append(dict(payload) if isinstance(payload, Mapping) else row)
    return rows


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


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _missing_inputs(root: Path) -> list[str]:
    rels = (
        "summary.json",
        "artifacts/apollo_control_raw.jsonl",
        "artifacts/planning_topic_debug.jsonl",
        "artifacts/control_trajectory_consume_debug_live.jsonl",
        "artifacts/lateral_geometry_debug.csv",
    )
    return [rel for rel in rels if not (root / rel).exists()]


def _numeric(rows: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _maybe_num(row.get(field))
        if value is not None:
            values.append(value)
    return values


def _numeric_csv(rows: Sequence[Mapping[str, str]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _maybe_num(row.get(field))
        if value is not None:
            values.append(value)
    return values


def _series_stats(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "max": None, "max_abs": None, "p95_abs": None}
    abs_values = sorted(abs(value) for value in values)
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "max_abs": max(abs_values),
        "p95_abs": abs_values[int(0.95 * (len(abs_values) - 1))],
    }


def _maybe_num(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _num(value: Any) -> float:
    parsed = _maybe_num(value)
    return 0.0 if parsed is None else parsed


def _close(left: float | None, right: float | None, *, tol: float = 1e-9) -> bool:
    return left is not None and right is not None and abs(left - right) <= tol


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Baguang Apollo Target Point Semantics",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        "",
        "## Run Results",
    ]
    for name, run in (report.get("runs") or {}).items():
        if not isinstance(run, Mapping):
            continue
        planning = run.get("planning") if isinstance(run.get("planning"), Mapping) else {}
        align = run.get("high_steer_alignment") if isinstance(run.get("high_steer_alignment"), Mapping) else {}
        geom = run.get("lateral_geometry") if isinstance(run.get("lateral_geometry"), Mapping) else {}
        lines.extend(
            [
                "",
                f"### {name}",
                f"- run_dir: `{run.get('run_dir')}`",
                f"- high_steer_count: `{(run.get('control') or {}).get('high_steer_count') if isinstance(run.get('control'), Mapping) else None}`",
                f"- planning.first_kappa.max_abs: `{_stat(planning.get('first_trajectory_point_kappa'), 'max_abs')}`",
                f"- planning.reference_line_nonzero_rows: `{planning.get('reference_line_nonzero_rows')}`",
                f"- reference_lane_curvature.max_abs: `{_stat((geom.get('reference_lane_curvature') if isinstance(geom, Mapping) else {}), 'max_abs')}`",
                f"- high_steer target-kappa planning-match count: `{align.get('target_kappa_matches_planning_first_point_count')}` / `{align.get('count')}`",
                f"- high_steer used-planning count: `{align.get('consume_used_planning_trajectory_count')}`",
                f"- findings: `{', '.join(run.get('findings') or [])}`",
            ]
        )
    lines.extend(["", "## Findings"])
    lines.extend(f"- `{item}`" for item in report.get("findings") or [])
    lines.extend(
        [
            "",
            "## Claim Boundary",
            "",
            "This report links high source steer to planning/control target-point evidence. It is not a standalone Apollo capability verdict.",
            "",
        ]
    )
    return "\n".join(lines)


def _stat(value: Any, field: str) -> Any:
    return value.get(field) if isinstance(value, Mapping) else None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Apollo target-point semantics on Baguang paired runs.")
    parser.add_argument("--no-lateral-run", default=str(DEFAULT_NO_LATERAL_RUN))
    parser.add_argument("--stabilizer-run", default=str(DEFAULT_STABILIZER_RUN))
    parser.add_argument("--high-steer-threshold", type=float, default=20.0)
    parser.add_argument("--kappa-spike-threshold", type=float, default=0.05)
    parser.add_argument("--out", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = analyze_baguang_apollo_target_point_semantics(
        no_lateral_run=args.no_lateral_run,
        stabilizer_run=args.stabilizer_run,
        high_steer_threshold=args.high_steer_threshold,
        kappa_spike_threshold=args.kappa_spike_threshold,
    )
    outputs = write_baguang_apollo_target_point_semantics_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "reason": report.get("reason"), "outputs": outputs}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
