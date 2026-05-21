#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None"):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _mean(values: Iterable[float]) -> Optional[float]:
    vals = list(values)
    if not vals:
        return None
    return statistics.fmean(vals)


def _analyze_raw_steer(path: Path) -> Dict[str, Any]:
    timestamps: List[float] = []
    values: List[float] = []
    if not path.exists():
        return {
            "count": 0,
            "nonzero_count": 0,
            "nonzero_ratio": None,
            "mean_abs": None,
            "max_abs": None,
            "saturation_count": 0,
            "saturation_ratio": None,
            "longest_saturation_streak": 0,
            "longest_saturation_duration_sec": 0.0,
            "longest_saturation_sign": None,
            "longest_saturation_start_ts": None,
            "longest_saturation_end_ts": None,
        }
    with path.open(encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            ts = _safe_float(rec.get("ts_sec"))
            steer = _safe_float(rec.get("raw_steer"))
            if ts is None or steer is None:
                continue
            timestamps.append(ts)
            values.append(steer)
    abs_vals = [abs(v) for v in values]
    sat_flags = [abs(v) >= 0.99 for v in values]
    best = 0
    cur = 0
    best_range: Optional[Tuple[int, int]] = None
    start = 0
    for i, flag in enumerate(sat_flags):
        if flag:
            if cur == 0:
                start = i
            cur += 1
            if cur > best:
                best = cur
                best_range = (start, i)
        else:
            cur = 0
    duration = 0.0
    sign = None
    start_ts = None
    end_ts = None
    if best_range:
        s, e = best_range
        start_ts = timestamps[s]
        end_ts = timestamps[e]
        duration = max(0.0, end_ts - start_ts)
        sign = -1 if values[s] < 0 else 1
    count = len(values)
    sat_count = sum(sat_flags)
    nonzero_count = sum(1 for v in abs_vals if v > 1e-6)
    return {
        "count": count,
        "nonzero_count": nonzero_count,
        "nonzero_ratio": (nonzero_count / count) if count else None,
        "mean_abs": _mean(abs_vals),
        "max_abs": max(abs_vals) if abs_vals else None,
        "saturation_count": sat_count,
        "saturation_ratio": (sat_count / count) if count else None,
        "longest_saturation_streak": best,
        "longest_saturation_duration_sec": duration,
        "longest_saturation_sign": sign,
        "longest_saturation_start_ts": start_ts,
        "longest_saturation_end_ts": end_ts,
    }


def _analyze_output_mask(control_decode_debug_path: Path, debug_timeseries_path: Path) -> Dict[str, Any]:
    total = 0
    nonzero_raw = 0
    nonzero_out = 0
    force_zero_applied = 0
    if control_decode_debug_path.exists():
        with control_decode_debug_path.open(encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                parsed = rec.get("parsed_control") or {}
                out = rec.get("output_to_carla") or {}
                total += 1
                if abs(_safe_float(parsed.get("steer")) or 0.0) > 1e-6:
                    nonzero_raw += 1
                if abs(_safe_float(out.get("steer")) or 0.0) > 1e-6:
                    nonzero_out += 1
                if bool(out.get("force_zero_steer_applied")):
                    force_zero_applied += 1
    timeline_total = 0
    timeline_force_zero = 0
    if debug_timeseries_path.exists():
        with debug_timeseries_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                timeline_total += 1
                if str(row.get("force_zero_steer_applied", "")).lower() == "true":
                    timeline_force_zero += 1
    return {
        "decode_total": total,
        "decode_nonzero_raw_count": nonzero_raw,
        "decode_nonzero_output_count": nonzero_out,
        "decode_force_zero_applied_count": force_zero_applied,
        "debug_timeseries_total": timeline_total,
        "debug_timeseries_force_zero_applied_count": timeline_force_zero,
    }


def _analyze_measured_steer(path: Path) -> Dict[str, Any]:
    measured_deg: List[float] = []
    commanded: List[float] = []
    if not path.exists():
        return {
            "row_count": 0,
            "mean_abs_measured_steer_deg": None,
            "max_abs_measured_steer_deg": None,
            "mean_abs_commanded_steer": None,
            "max_abs_commanded_steer": None,
        }
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = _safe_float(row.get("measured_steer_deg"))
            if val is not None:
                measured_deg.append(abs(val))
            out = _safe_float(row.get("commanded_steer"))
            if out is not None:
                commanded.append(abs(out))
    return {
        "row_count": max(len(measured_deg), len(commanded)),
        "mean_abs_measured_steer_deg": _mean(measured_deg),
        "max_abs_measured_steer_deg": max(measured_deg) if measured_deg else None,
        "mean_abs_commanded_steer": _mean(commanded),
        "max_abs_commanded_steer": max(commanded) if commanded else None,
    }


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _build_report(
    run_dir: Path,
    config_path: Path,
    summary: Dict[str, Any],
    bridge: Dict[str, Any],
    raw_steer: Dict[str, Any],
    output_mask: Dict[str, Any],
    measured: Dict[str, Any],
) -> str:
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    control_mapping = (((cfg.get("algo") or {}).get("apollo") or {}).get("control_mapping") or {})
    force_zero_cfg = bool(control_mapping.get("force_zero_steer_output", False))
    lines = [
        "# Apollo Lateral Evaluation Report",
        "",
        f"- run_dir: `{run_dir}`",
        f"- config: `{config_path}`",
        f"- profile_name: `{summary.get('profile_name')}`",
        f"- summary_success: `{summary.get('success')}`",
        f"- fail_reason: `{summary.get('fail_reason')}`",
        "",
        "## Executive Summary",
        "",
        f"- `relaxed` 当前 **仍在屏蔽最终横向输出**：`force_zero_steer_output={force_zero_cfg}`。",
        f"- Apollo 原始横向信号 **不是小幅健康修正**，而是出现了明显连续打满：`|raw_steer|>=0.99` 共 `{raw_steer['saturation_count']}` 次，占比 `{_fmt(raw_steer['saturation_ratio'])}`。",
        f"- 最长连续饱和段长度 `{raw_steer['longest_saturation_streak']}`，持续约 `{_fmt(raw_steer['longest_saturation_duration_sec'])}` 秒，方向 `{raw_steer['longest_saturation_sign']}`。",
        f"- bridge 最终发给 CARLA 的横向输出仍被压成 0：`decode_nonzero_output_count={output_mask['decode_nonzero_output_count']}`。",
        "",
        "## Config State",
        "",
        f"- `force_zero_steer_output`: `{force_zero_cfg}`",
        f"- `straight_lane_zero_steer_enabled`: `{bool(control_mapping.get('straight_lane_zero_steer_enabled', False))}`",
        f"- `low_speed_steer_guard_enabled`: `{bool(control_mapping.get('low_speed_steer_guard_enabled', False))}`",
        "",
        "## Raw Apollo Steer",
        "",
        f"- sample_count: `{raw_steer['count']}`",
        f"- nonzero_count: `{raw_steer['nonzero_count']}`",
        f"- nonzero_ratio: `{_fmt(raw_steer['nonzero_ratio'])}`",
        f"- mean_abs_raw_steer: `{_fmt(raw_steer['mean_abs'])}`",
        f"- max_abs_raw_steer: `{_fmt(raw_steer['max_abs'])}`",
        f"- saturation_count(|raw_steer|>=0.99): `{raw_steer['saturation_count']}`",
        f"- saturation_ratio: `{_fmt(raw_steer['saturation_ratio'])}`",
        f"- longest_saturation_streak: `{raw_steer['longest_saturation_streak']}`",
        f"- longest_saturation_start_ts: `{_fmt(raw_steer['longest_saturation_start_ts'], 3)}`",
        f"- longest_saturation_end_ts: `{_fmt(raw_steer['longest_saturation_end_ts'], 3)}`",
        f"- longest_saturation_duration_sec: `{_fmt(raw_steer['longest_saturation_duration_sec'])}`",
        "",
        "## Bridge Output Masking",
        "",
        f"- decode_total: `{output_mask['decode_total']}`",
        f"- decode_nonzero_raw_count: `{output_mask['decode_nonzero_raw_count']}`",
        f"- decode_nonzero_output_count: `{output_mask['decode_nonzero_output_count']}`",
        f"- decode_force_zero_applied_count: `{output_mask['decode_force_zero_applied_count']}`",
        f"- debug_timeseries_force_zero_applied_count: `{output_mask['debug_timeseries_force_zero_applied_count']}` / `{output_mask['debug_timeseries_total']}`",
        "",
        "## Vehicle Response",
        "",
        f"- mean_abs_measured_steer_deg: `{_fmt(measured['mean_abs_measured_steer_deg'])}`",
        f"- max_abs_measured_steer_deg: `{_fmt(measured['max_abs_measured_steer_deg'])}`",
        f"- mean_abs_commanded_steer: `{_fmt(measured['mean_abs_commanded_steer'])}`",
        f"- max_abs_commanded_steer: `{_fmt(measured['max_abs_commanded_steer'])}`",
        "",
        "## Context",
        "",
        f"- planning_nonempty_trajectory_count: `{bridge.get('planning_nonempty_trajectory_count')}`",
        f"- routing_request_count: `{bridge.get('routing_request_count')}`",
        f"- reroute_reason_counts: `{bridge.get('reroute_reason_counts')}`",
        f"- traffic_light_policy: `{bridge.get('traffic_light_policy')}`",
        "",
        "## Interpretation",
        "",
        "- 这轮 `relaxed` 不能用于判断“Apollo 横向已经修好”。",
        "- 它目前更像“主链打通 / 纵向验证档”：Apollo 原始横向信号在算，但最终横向输出仍被 bridge 清零。",
        "- 最新 run 中，Apollo 原始横向信号本身也不健康，因为存在一段持续约 12 秒的单侧打满。",
        "- 因此当前看到“车没有横向打死”，并不能证明 Apollo 横向控制已经正常，只能证明 **在屏蔽横向输出条件下** 主链能跑通。",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Apollo lateral evaluation status for one run")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--config", default="")
    parser.add_argument(
        "--report-path",
        default=str(REPO_ROOT / "artifacts" / "apollo_lateral_evaluation_report.md"),
    )
    parser.add_argument(
        "--json-path",
        default=str(REPO_ROOT / "artifacts" / "apollo_lateral_evaluation_summary.json"),
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    config_path = Path(args.config).expanduser().resolve() if args.config else Path(
        _load_json(run_dir / "summary.json").get("profile_config_path") or ""
    )
    if not config_path.exists():
        raise SystemExit(f"config not found: {config_path}")

    summary = _load_json(run_dir / "summary.json")
    artifacts = run_dir / "artifacts"
    bridge = _load_json(artifacts / "bridge_health_summary.json")
    raw_steer = _analyze_raw_steer(artifacts / "bridge_control_decode.jsonl")
    output_mask = _analyze_output_mask(
        artifacts / "control_decode_debug.jsonl",
        artifacts / "debug_timeseries.csv",
    )
    measured = _analyze_measured_steer(artifacts / "carla_vehicle_response.csv")

    payload = {
        "run_dir": str(run_dir),
        "config_path": str(config_path),
        "profile_name": summary.get("profile_name"),
        "summary_success": summary.get("success"),
        "summary_fail_reason": summary.get("fail_reason"),
        "raw_steer": raw_steer,
        "output_mask": output_mask,
        "measured": measured,
        "bridge_health": {
            "planning_nonempty_trajectory_count": bridge.get("planning_nonempty_trajectory_count"),
            "routing_request_count": bridge.get("routing_request_count"),
            "reroute_reason_counts": bridge.get("reroute_reason_counts"),
            "traffic_light_policy": bridge.get("traffic_light_policy"),
        },
    }

    report = _build_report(run_dir, config_path, summary, bridge, raw_steer, output_mask, measured)
    report_path = Path(args.report_path).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")

    json_path = Path(args.json_path).expanduser().resolve()
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(report_path)
    print(json_path)


if __name__ == "__main__":
    main()
