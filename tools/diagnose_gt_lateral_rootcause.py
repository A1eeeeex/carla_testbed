#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        out = float(value)
        return out if math.isfinite(out) else None
    text = str(value).strip()
    if not text:
        return None
    try:
        out = float(text)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _wrap_deg(deg: float) -> float:
    return (deg + 180.0) % 360.0 - 180.0


def _angle_diff_deg(a: float, b: float) -> float:
    return abs(_wrap_deg(a - b))


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_debug_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    try:
        with path.open("r", newline="") as fp:
            for row in csv.DictReader(fp):
                item = dict(row)
                item["ts_sec"] = _safe_float(row.get("ts_sec"))
                item["map_yaw_deg"] = _safe_float(row.get("map_yaw_deg"))
                item["preview_heading_deg"] = _safe_float(row.get("preview_heading_deg"))
                item["e_psi_deg"] = _safe_float(row.get("e_psi_deg"))
                item["e_y_m"] = _safe_float(row.get("e_y_m"))
                item["apollo_desired_steer"] = _safe_float(row.get("apollo_desired_steer"))
                item["commanded_steer"] = _safe_float(row.get("commanded_steer"))
                item["force_zero_steer_applied"] = _safe_bool(row.get("force_zero_steer_applied"))
                item["straight_lane_zero_steer_applied"] = _safe_bool(row.get("straight_lane_zero_steer_applied"))
                item["low_speed_steer_guard_applied"] = _safe_bool(row.get("low_speed_steer_guard_applied"))
                item["lane_inside"] = _safe_bool(row.get("lane_inside"))
                out.append(item)
    except Exception:
        return []

    ts_values = [r["ts_sec"] for r in out if r.get("ts_sec") is not None]
    t0 = min(ts_values) if ts_values else None
    for r in out:
        r["rel_t"] = (r["ts_sec"] - t0) if (t0 is not None and r.get("ts_sec") is not None) else None
    return out


def _summary(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {"count": 0, "mean": None, "min": None, "max": None, "median": None}
    arr = sorted(values)
    return {
        "count": len(arr),
        "mean": float(statistics.fmean(arr)),
        "min": float(arr[0]),
        "max": float(arr[-1]),
        "median": float(statistics.median(arr)),
        "p95": float(arr[int((len(arr) - 1) * 0.95)]),
    }


def _ratio_true(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
    vals = [r.get(key) for r in rows if r.get(key) is not None]
    if not vals:
        return None
    return float(sum(1 for v in vals if bool(v)) / len(vals))


def diagnose(run_dir: Path, startup_window_sec: float = 15.0) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    debug_rows = _load_debug_rows(artifacts / "debug_timeseries.csv")
    stats = _load_json(artifacts / "cyber_bridge_stats.json")
    effective_cfg = _load_yaml(run_dir / "effective.yaml")

    startup_rows = [
        r for r in debug_rows if r.get("rel_t") is not None and float(r["rel_t"]) <= float(startup_window_sec)
    ]

    map_yaw_start = [r["map_yaw_deg"] for r in startup_rows if r.get("map_yaw_deg") is not None]
    preview_start = [r["preview_heading_deg"] for r in startup_rows if r.get("preview_heading_deg") is not None]
    epsi_start = [r["e_psi_deg"] for r in startup_rows if r.get("e_psi_deg") is not None]
    epsi_abs_start = [abs(v) for v in epsi_start]
    angle_delta_start = [
        _angle_diff_deg(a, b)
        for a, b in zip(map_yaw_start, preview_start)
        if a is not None and b is not None
    ]
    lane_inside_ratio = _ratio_true(startup_rows, "lane_inside")

    apollo_steer = [r["apollo_desired_steer"] for r in debug_rows if r.get("apollo_desired_steer") is not None]
    cmd_steer = [r["commanded_steer"] for r in debug_rows if r.get("commanded_steer") is not None]
    apollo_steer_abs = [abs(v) for v in apollo_steer]
    cmd_steer_abs = [abs(v) for v in cmd_steer]

    force_zero_ratio = _ratio_true(debug_rows, "force_zero_steer_applied")
    straight_zero_ratio = _ratio_true(debug_rows, "straight_lane_zero_steer_applied")
    low_speed_guard_ratio = _ratio_true(debug_rows, "low_speed_steer_guard_applied")

    apollo_cfg = (((effective_cfg.get("algo") or {}).get("apollo") or {}) if effective_cfg else {}) or {}
    tf_cfg = (apollo_cfg.get("carla_to_apollo") or {}) if isinstance(apollo_cfg, dict) else {}
    bridge_cfg = (apollo_cfg.get("bridge") or {}) if isinstance(apollo_cfg, dict) else {}
    routing_cfg = (apollo_cfg.get("routing") or {}) if isinstance(apollo_cfg, dict) else {}
    ctrl_cfg = (apollo_cfg.get("control_mapping") or {}) if isinstance(apollo_cfg, dict) else {}

    start_proj = (stats.get("last_routing_projection") or {}).get("start", {}) if stats else {}
    last_pose = (stats.get("last_pose_debug") or {}) if stats else {}
    start_lane_yaw = _safe_float(start_proj.get("lane_yaw_deg"))
    start_map_yaw = _safe_float(last_pose.get("map_yaw_deg"))
    start_lane_heading_delta = (
        _angle_diff_deg(start_map_yaw, start_lane_yaw)
        if (start_lane_yaw is not None and start_map_yaw is not None)
        else None
    )

    startup_epsi_large = _summary(epsi_abs_start).get("median")
    startup_heading_delta_large = _summary(angle_delta_start).get("median")
    apollo_abs_mean = _summary(apollo_steer_abs).get("mean")
    cmd_abs_mean = _summary(cmd_steer_abs).get("mean")

    frame_or_lane_mismatch = bool(
        startup_epsi_large is not None
        and startup_heading_delta_large is not None
        and startup_epsi_large > 45.0
        and startup_heading_delta_large > 45.0
    )
    routing_projection_mismatch = bool(
        start_lane_heading_delta is not None
        and start_lane_heading_delta > 45.0
        and bool(routing_cfg.get("snap_start_to_lane", True))
    )
    apollo_output_abnormal = bool(apollo_abs_mean is not None and apollo_abs_mean > 0.4)
    bridge_output_suppressed = bool(
        cmd_abs_mean is not None
        and cmd_abs_mean < 0.02
        and force_zero_ratio is not None
        and force_zero_ratio > 0.6
    )
    steer_sign_secondary = bool((stats.get("steer_sign_auto_check") or {}).get("suggested") is not None)

    ranked: List[Dict[str, Any]] = []
    if frame_or_lane_mismatch:
        ranked.append(
            {
                "rank": 1,
                "level": "high",
                "cause": "input_frame_or_lane_heading_mismatch",
                "evidence": {
                    "startup_e_psi_abs_median_deg": startup_epsi_large,
                    "startup_map_preview_heading_delta_median_deg": startup_heading_delta_large,
                },
            }
        )
    if routing_projection_mismatch:
        ranked.append(
            {
                "rank": len(ranked) + 1,
                "level": "high",
                "cause": "routing_start_projection_heading_mismatch",
                "evidence": {
                    "start_projection_lane_yaw_deg": start_lane_yaw,
                    "last_pose_map_yaw_deg": start_map_yaw,
                    "heading_delta_deg": start_lane_heading_delta,
                    "snap_start_to_lane": bool(routing_cfg.get("snap_start_to_lane", True)),
                    "start_nudge_use_lane_heading": bool(routing_cfg.get("start_nudge_use_lane_heading", True)),
                },
            }
        )
    if apollo_output_abnormal and bridge_output_suppressed:
        ranked.append(
            {
                "rank": len(ranked) + 1,
                "level": "medium",
                "cause": "apollo_output_abnormal_but_bridge_guard_masks_it",
                "evidence": {
                    "apollo_desired_steer_abs_mean": apollo_abs_mean,
                    "commanded_steer_abs_mean": cmd_abs_mean,
                    "force_zero_steer_applied_ratio": force_zero_ratio,
                    "straight_lane_zero_steer_applied_ratio": straight_zero_ratio,
                    "low_speed_steer_guard_applied_ratio": low_speed_guard_ratio,
                },
            }
        )
    if steer_sign_secondary:
        ranked.append(
            {
                "rank": len(ranked) + 1,
                "level": "low",
                "cause": "steer_sign_mismatch_possible_secondary",
                "evidence": stats.get("steer_sign_auto_check"),
            }
        )
    if not ranked:
        ranked.append(
            {
                "rank": 1,
                "level": "unknown",
                "cause": "insufficient_evidence",
                "evidence": {},
            }
        )

    conclusion = (
        "Apollo 原始横向输出异常首先发生在输入侧/路线几何侧，bridge 末端 guard 仅做压制。"
        if apollo_output_abnormal and bridge_output_suppressed
        else "未观察到典型“原始输出异常但被末端完全压制”的模式。"
    )

    payload = {
        "run_dir": str(run_dir.resolve()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "startup_window_sec": startup_window_sec,
        "metrics": {
            "startup_map_yaw_deg": _summary(map_yaw_start),
            "startup_preview_heading_deg": _summary(preview_start),
            "startup_e_psi_deg": _summary(epsi_start),
            "startup_e_psi_abs_deg": _summary(epsi_abs_start),
            "startup_map_preview_heading_delta_deg": _summary(angle_delta_start),
            "startup_lane_inside_ratio": lane_inside_ratio,
            "apollo_desired_steer": _summary(apollo_steer),
            "commanded_steer": _summary(cmd_steer),
            "force_zero_steer_applied_ratio": force_zero_ratio,
            "straight_lane_zero_steer_applied_ratio": straight_zero_ratio,
            "low_speed_steer_guard_applied_ratio": low_speed_guard_ratio,
        },
        "config_snapshot": {
            "carla_to_apollo": {
                "tx": tf_cfg.get("tx"),
                "ty": tf_cfg.get("ty"),
                "tz": tf_cfg.get("tz"),
                "yaw_deg": tf_cfg.get("yaw_deg"),
                "auto_calib": tf_cfg.get("auto_calib"),
                "auto_calib_snap_right_angle": tf_cfg.get("auto_calib_snap_right_angle"),
                "auto_calib_samples": tf_cfg.get("auto_calib_samples"),
            },
            "bridge": {
                "localization_back_offset_m": bridge_cfg.get("localization_back_offset_m"),
            },
            "routing": {
                "start_nudge_m": routing_cfg.get("start_nudge_m"),
                "snap_start_to_lane": routing_cfg.get("snap_start_to_lane", True),
                "snap_goal_to_lane": routing_cfg.get("snap_goal_to_lane", True),
                "start_nudge_use_lane_heading": routing_cfg.get("start_nudge_use_lane_heading", True),
                "use_seed_heading": routing_cfg.get("use_seed_heading"),
            },
            "control_mapping": {
                "steer_sign": ctrl_cfg.get("steer_sign"),
                "force_zero_steer_output": ctrl_cfg.get("force_zero_steer_output"),
                "straight_lane_zero_steer_enabled": ctrl_cfg.get("straight_lane_zero_steer_enabled"),
                "low_speed_steer_guard_enabled": ctrl_cfg.get("low_speed_steer_guard_enabled"),
            },
        },
        "runtime_projection": {
            "last_pose_debug": last_pose,
            "last_routing_projection_start": start_proj,
            "start_lane_heading_delta_deg": start_lane_heading_delta,
            "steer_sign_auto_check": stats.get("steer_sign_auto_check"),
        },
        "root_cause_ranking": ranked,
        "conclusion": conclusion,
        "notes": [
            "lane_inside=true 仅表示离某条最近几何段的横向偏差在阈值内，不代表方向匹配正确。",
            "若启动窗口 map_yaw 与 preview_heading 存在约 90° 级差异，应优先检查 frame/routing 起点投影链。",
        ],
    }
    return payload


def _render_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# GT Lateral Rootcause Diagnosis")
    lines.append("")
    lines.append(f"- run_dir: `{payload.get('run_dir', '')}`")
    lines.append(f"- generated_at_utc: `{payload.get('generated_at_utc', '')}`")
    lines.append(f"- startup_window_sec: `{payload.get('startup_window_sec', '')}`")
    lines.append("")
    lines.append("## Key Metrics")
    lines.append("")
    m = payload.get("metrics", {}) or {}
    lines.append(f"- startup_e_psi_abs_deg: {m.get('startup_e_psi_abs_deg')}")
    lines.append(f"- startup_map_preview_heading_delta_deg: {m.get('startup_map_preview_heading_delta_deg')}")
    lines.append(f"- apollo_desired_steer: {m.get('apollo_desired_steer')}")
    lines.append(f"- commanded_steer: {m.get('commanded_steer')}")
    lines.append(f"- force_zero_steer_applied_ratio: {m.get('force_zero_steer_applied_ratio')}")
    lines.append("")
    lines.append("## Root Cause Ranking")
    lines.append("")
    for item in payload.get("root_cause_ranking", []) or []:
        lines.append(
            f"- [{item.get('level')}] #{item.get('rank')} `{item.get('cause')}` evidence={item.get('evidence')}"
        )
    lines.append("")
    lines.append("## Conclusion")
    lines.append("")
    lines.append(payload.get("conclusion", ""))
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose GT Apollo lateral root cause from run artifacts")
    parser.add_argument("--run-dir", required=True, help="run directory path, e.g. runs/<run_name>")
    parser.add_argument("--startup-window-sec", type=float, default=15.0)
    parser.add_argument("--no-md", action="store_true", help="skip markdown output")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.is_dir():
        raise SystemExit(f"run dir not found: {run_dir}")
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)

    payload = diagnose(run_dir, startup_window_sec=float(args.startup_window_sec))
    out_json = artifacts / "lateral_rootcause_summary.json"
    out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"[diag][lateral] written: {out_json}")
    if not args.no_md:
        out_md = artifacts / "lateral_rootcause_summary.md"
        out_md.write_text(_render_markdown(payload))
        print(f"[diag][lateral] written: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
