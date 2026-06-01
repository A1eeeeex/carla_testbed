from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from .baguang_apollo_lateral_blocker import DEFAULT_NO_LATERAL_RUN
from .baguang_apollo_lateral_stabilizer_ab import DEFAULT_STABILIZER_RUN

BAGUANG_APOLLO_SOURCE_STEER_SCHEMA_VERSION = "baguang_apollo_source_steer.v1"


def analyze_baguang_apollo_source_steer(
    *,
    no_lateral_run: str | Path = DEFAULT_NO_LATERAL_RUN,
    stabilizer_run: str | Path = DEFAULT_STABILIZER_RUN,
    high_steer_threshold: float = 20.0,
) -> dict[str, Any]:
    runs = {
        "no_lateral": analyze_baguang_apollo_source_steer_run(
            no_lateral_run,
            high_steer_threshold=high_steer_threshold,
        ),
        "stabilizer_enabled": analyze_baguang_apollo_source_steer_run(
            stabilizer_run,
            high_steer_threshold=high_steer_threshold,
        ),
    }
    findings = _pair_findings(runs)
    return {
        "schema_version": BAGUANG_APOLLO_SOURCE_STEER_SCHEMA_VERSION,
        "status": "warn" if findings else "pass",
        "reason": "source_steer_semantics_require_audit" if findings else "no_source_steer_anomaly_detected",
        "high_steer_threshold": float(high_steer_threshold),
        "runs": runs,
        "findings": findings,
        "claim_boundary": {
            "proves_apollo_algorithm_limitation": False,
            "reason": (
                "This report flags source-steer and target-point semantics for audit. It does not by itself "
                "prove an Apollo algorithm limitation independent of map/reference-line/input-contract issues."
            ),
        },
    }


def analyze_baguang_apollo_source_steer_run(
    run_dir: str | Path,
    *,
    high_steer_threshold: float = 20.0,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    control_rows = _control_rows(root / "artifacts" / "apollo_control_raw.jsonl")
    bridge_decode_rows = _read_jsonl(root / "artifacts" / "bridge_control_decode.jsonl")
    direct_apply_rows = _read_jsonl(root / "artifacts" / "direct_bridge_control_apply.jsonl")
    lateral_geometry_rows = _read_csv_rows(root / "artifacts" / "lateral_geometry_debug.csv")
    summary = _read_json(root / "summary.json")
    direct_stats = _read_json(root / "artifacts" / "direct_bridge_stats.json")

    high_steer_rows = [
        row for row in control_rows if abs(_num(row.get("steering_target"))) >= float(high_steer_threshold)
    ]
    target_kappa = _numeric(control_rows, "debug_simple_lat_current_target_point_kappa")
    reference_curvature = _numeric_csv(lateral_geometry_rows, "reference_lane_curvature")
    run_findings = _run_findings(high_steer_rows, target_kappa, reference_curvature, direct_apply_rows, direct_stats)
    return {
        "run_dir": str(root),
        "summary": {
            "success": summary.get("success"),
            "exit_reason": summary.get("exit_reason"),
            "fail_reason": summary.get("fail_reason"),
            "collision_count": summary.get("collision_count"),
            "max_speed_mps": summary.get("max_speed_mps"),
        },
        "direct_stabilizer": {
            "enabled": direct_stats.get("straight_lane_lateral_stabilizer_enabled"),
            "apply_count": direct_stats.get("straight_lane_lateral_stabilizer_apply_count"),
        },
        "control_raw_count": len(control_rows),
        "high_steer_count": len(high_steer_rows),
        "high_steer_ratio": (len(high_steer_rows) / len(control_rows)) if control_rows else None,
        "steering_target": _series_stats(_numeric(control_rows, "steering_target")),
        "simple_lat": {
            "target_point_kappa": _series_stats(target_kappa),
            "target_point_s": _series_stats(_numeric(control_rows, "debug_simple_lat_current_target_point_s")),
            "target_point_relative_time": _series_stats(
                _numeric(control_rows, "debug_simple_lat_current_target_point_relative_time")
            ),
            "lateral_error": _series_stats(_numeric(control_rows, "debug_simple_lat_lateral_error")),
            "heading_error": _series_stats(_numeric(control_rows, "debug_simple_lat_heading_error")),
            "steering_position": _series_stats(_numeric(control_rows, "debug_simple_lat_steering_position")),
            "current_steer_interval": _series_stats(_numeric(control_rows, "debug_simple_lon_current_steer_interval")),
            "current_speed_mps": _series_stats(_numeric(control_rows, "debug_simple_lon_current_speed")),
            "path_remain_m": _series_stats(_numeric(control_rows, "debug_simple_lon_path_remain")),
            "ref_speed_mps": _series_stats(_numeric(control_rows, "debug_simple_lat_ref_speed")),
        },
        "high_steer_context": _high_steer_context_summary(high_steer_rows),
        "bridge_decode": {
            "raw_steer": _series_stats(_numeric(bridge_decode_rows, "raw_steer")),
            "commanded_steer": _series_stats(_numeric(bridge_decode_rows, "commanded_steer")),
        },
        "direct_apply": {
            "source_steer": _series_stats(_numeric(direct_apply_rows, "source_steer")),
            "applied_steer": _series_stats(_numeric(direct_apply_rows, "steer")),
        },
        "lateral_geometry": {
            "reference_lane_curvature": _series_stats(reference_curvature),
            "e_y_m": _series_stats(_numeric_csv(lateral_geometry_rows, "e_y_m")),
            "e_psi_deg": _series_stats(_numeric_csv(lateral_geometry_rows, "e_psi_deg")),
            "lane_dist_m": _series_stats(_numeric_csv(lateral_geometry_rows, "lane_dist_m")),
        },
        "high_steer_samples": [_sample_control_row(row) for row in high_steer_rows[:5]],
        "findings": run_findings,
        "missing_inputs": _missing_inputs(root),
    }


def write_baguang_apollo_source_steer_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "baguang_apollo_source_steer_report.json"
    md_path = out / "baguang_apollo_source_steer_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _run_findings(
    high_steer_rows: Sequence[Mapping[str, Any]],
    target_kappa: Sequence[float],
    reference_curvature: Sequence[float],
    direct_apply_rows: Sequence[Mapping[str, Any]],
    direct_stats: Mapping[str, Any],
) -> list[str]:
    findings: list[str] = []
    if high_steer_rows:
        findings.append("high_source_steer_present")
    if max((abs(value) for value in target_kappa), default=0.0) > 0.05 and max(
        (abs(value) for value in reference_curvature),
        default=0.0,
    ) < 0.001:
        findings.append("target_point_curvature_spike_on_straight_reference")
    if _high_steer_with_small_lateral_error(high_steer_rows):
        findings.append("high_source_steer_with_small_simple_lat_error")
    if _high_steer_low_speed_context(high_steer_rows):
        findings.append("high_source_steer_concentrates_in_low_speed_terminal_context")
    if direct_stats.get("straight_lane_lateral_stabilizer_enabled") is True:
        source = _series_stats(_numeric(direct_apply_rows, "source_steer"))
        applied = _series_stats(_numeric(direct_apply_rows, "steer"))
        if _num(source.get("max_abs")) > 0.05 and _num(applied.get("max_abs")) < 0.01:
            findings.append("stabilizer_suppressed_source_steer")
    return findings


def _pair_findings(runs: Mapping[str, Mapping[str, Any]]) -> list[str]:
    findings: list[str] = []
    if any("target_point_curvature_spike_on_straight_reference" in run.get("findings", []) for run in runs.values()):
        findings.append("target_point_curvature_semantics_suspect")
    if "stabilizer_suppressed_source_steer" in runs.get("stabilizer_enabled", {}).get("findings", []):
        findings.append("stabilizer_masks_source_steer_anomaly")
    if "high_source_steer_present" in runs.get("no_lateral", {}).get("findings", []):
        findings.append("no_lateral_run_exposes_source_steer_to_carla")
    if any(
        "high_source_steer_concentrates_in_low_speed_terminal_context" in run.get("findings", [])
        for run in runs.values()
    ):
        findings.append("high_source_steer_is_low_speed_terminal_semantics")
    return findings


def _high_steer_with_small_lateral_error(rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        lat = abs(_num(row.get("debug_simple_lat_lateral_error")))
        heading = abs(_num(row.get("debug_simple_lat_heading_error")))
        if lat < 0.1 and heading < 0.02:
            return True
    return False


def _high_steer_low_speed_context(rows: Sequence[Mapping[str, Any]]) -> bool:
    speeds = _numeric(rows, "debug_simple_lon_current_speed")
    path_remain = _numeric(rows, "debug_simple_lon_path_remain")
    if not speeds:
        return False
    speed_stats = _series_stats(speeds)
    path_stats = _series_stats(path_remain)
    return _num(speed_stats.get("p95_abs")) <= 5.0 and (
        not path_remain or _num(path_stats.get("p95_abs")) <= 60.0
    )


def _high_steer_context_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "count": 0,
            "current_speed_mps": _series_stats([]),
            "path_remain_m": _series_stats([]),
            "target_point_kappa": _series_stats([]),
            "lateral_error": _series_stats([]),
            "heading_error": _series_stats([]),
            "steer_angle_limited": _series_stats([]),
            "low_speed_count": 0,
            "near_terminal_count": 0,
            "small_error_count": 0,
        }
    return {
        "count": len(rows),
        "current_speed_mps": _series_stats(_numeric(rows, "debug_simple_lon_current_speed")),
        "path_remain_m": _series_stats(_numeric(rows, "debug_simple_lon_path_remain")),
        "target_point_kappa": _series_stats(_numeric(rows, "debug_simple_lat_current_target_point_kappa")),
        "lateral_error": _series_stats(_numeric(rows, "debug_simple_lat_lateral_error")),
        "heading_error": _series_stats(_numeric(rows, "debug_simple_lat_heading_error")),
        "steer_angle_limited": _series_stats(_numeric(rows, "debug_simple_lat_steer_angle_limited")),
        "low_speed_count": sum(1 for row in rows if _num(row.get("debug_simple_lon_current_speed")) <= 5.0),
        "near_terminal_count": sum(1 for row in rows if _num(row.get("debug_simple_lon_path_remain")) <= 60.0),
        "small_error_count": sum(
            1
            for row in rows
            if abs(_num(row.get("debug_simple_lat_lateral_error"))) < 0.1
            and abs(_num(row.get("debug_simple_lat_heading_error"))) < 0.02
        ),
    }


def _sample_control_row(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "control_header_sequence_num",
        "steering_target",
        "debug_simple_lat_lateral_error",
        "debug_simple_lat_heading_error",
        "debug_simple_lat_current_target_point_kappa",
        "debug_simple_lat_current_target_point_s",
        "debug_simple_lat_current_target_point_relative_time",
        "debug_simple_lat_steering_position",
        "debug_simple_lon_current_speed",
        "debug_simple_lon_path_remain",
        "debug_simple_lat_steer_angle_limited",
    )
    return {key: row.get(key) for key in keys}


def _control_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _read_jsonl(path):
        payload = row.get("apollo_control_raw")
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
        else:
            rows.append(row)
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
        "artifacts/bridge_control_decode.jsonl",
        "artifacts/direct_bridge_control_apply.jsonl",
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


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Baguang Apollo Source Steer",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        f"- high_steer_threshold: `{report.get('high_steer_threshold')}`",
        "",
        "## Run Results",
    ]
    for name, run in (report.get("runs") or {}).items():
        if not isinstance(run, Mapping):
            continue
        simple_lat = run.get("simple_lat") if isinstance(run.get("simple_lat"), Mapping) else {}
        high_context = run.get("high_steer_context") if isinstance(run.get("high_steer_context"), Mapping) else {}
        geom = run.get("lateral_geometry") if isinstance(run.get("lateral_geometry"), Mapping) else {}
        direct = run.get("direct_apply") if isinstance(run.get("direct_apply"), Mapping) else {}
        lines.extend(
            [
                "",
                f"### {name}",
                f"- run_dir: `{run.get('run_dir')}`",
                f"- high_steer_count: `{run.get('high_steer_count')}`",
                f"- high_steer_ratio: `{run.get('high_steer_ratio')}`",
                f"- steering_target.max_abs: `{_stat(run.get('steering_target'), 'max_abs')}`",
                f"- target_point_kappa.max_abs: `{_stat(simple_lat.get('target_point_kappa'), 'max_abs')}`",
                f"- high_steer.current_speed.p95_abs: `{_stat(high_context.get('current_speed_mps'), 'p95_abs')}`",
                f"- high_steer.path_remain.p95_abs: `{_stat(high_context.get('path_remain_m'), 'p95_abs')}`",
                f"- high_steer.small_error_count: `{high_context.get('small_error_count')}`",
                f"- reference_lane_curvature.max_abs: `{_stat(geom.get('reference_lane_curvature'), 'max_abs')}`",
                f"- direct_source_steer.max_abs: `{_stat(direct.get('source_steer'), 'max_abs')}`",
                f"- direct_applied_steer.max_abs: `{_stat(direct.get('applied_steer'), 'max_abs')}`",
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
            "This report identifies source-steer and target-point semantics that need audit. It is not a standalone Apollo capability verdict.",
            "",
        ]
    )
    return "\n".join(lines)


def _stat(value: Any, field: str) -> Any:
    return value.get(field) if isinstance(value, Mapping) else None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Apollo source steer semantics on Baguang paired runs.")
    parser.add_argument("--no-lateral-run", default=str(DEFAULT_NO_LATERAL_RUN))
    parser.add_argument("--stabilizer-run", default=str(DEFAULT_STABILIZER_RUN))
    parser.add_argument("--high-steer-threshold", type=float, default=20.0)
    parser.add_argument("--out", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = analyze_baguang_apollo_source_steer(
        no_lateral_run=args.no_lateral_run,
        stabilizer_run=args.stabilizer_run,
        high_steer_threshold=args.high_steer_threshold,
    )
    outputs = write_baguang_apollo_source_steer_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "reason": report.get("reason"), "outputs": outputs}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
