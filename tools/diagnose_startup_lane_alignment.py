#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def _summary(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {"count": 0, "mean": None, "median": None, "min": None, "max": None}
    arr = sorted(values)
    return {
        "count": len(arr),
        "mean": float(statistics.fmean(arr)),
        "median": float(statistics.median(arr)),
        "min": float(arr[0]),
        "max": float(arr[-1]),
        "p95": float(arr[int((len(arr) - 1) * 0.95)]),
    }


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
    rows: List[Dict[str, Any]] = []
    with path.open("r", newline="") as fp:
        for row in csv.DictReader(fp):
            item = dict(row)
            item["ts_sec"] = _safe_float(row.get("ts_sec"))
            item["map_x"] = _safe_float(row.get("map_x"))
            item["map_y"] = _safe_float(row.get("map_y"))
            item["map_yaw_deg"] = _safe_float(row.get("map_yaw_deg"))
            item["preview_heading_deg"] = _safe_float(row.get("preview_heading_deg"))
            item["e_psi_deg"] = _safe_float(row.get("e_psi_deg"))
            item["e_y_m"] = _safe_float(row.get("e_y_m"))
            item["lane_inside"] = _safe_bool(row.get("lane_inside"))
            rows.append(item)
    ts_vals = [r["ts_sec"] for r in rows if r.get("ts_sec") is not None]
    t0 = min(ts_vals) if ts_vals else None
    for r in rows:
        if t0 is not None and r.get("ts_sec") is not None:
            r["rel_t"] = float(r["ts_sec"] - t0)
        else:
            r["rel_t"] = None
    return rows


def _parse_routing_requests(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    text = path.read_text(errors="ignore")
    marker_pat = re.compile(r"Get new routing request:")
    starts = [m.start() for m in marker_pat.finditer(text)]
    if not starts:
        return []
    parts: List[str] = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if (i + 1) < len(starts) else len(text)
        parts.append(text[start:end])
    requests: List[Dict[str, Any]] = []
    wp_pat = re.compile(
        r"waypoint\s*\{\s*id:\s*\"([^\"]+)\"\s*s:\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*"
        r"pose\s*\{\s*x:\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*y:\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)",
        flags=re.DOTALL,
    )
    for part in parts:
        wps = []
        for lane_id, s_text, x_text, y_text in wp_pat.findall(part):
            s_val = _safe_float(s_text)
            x_val = _safe_float(x_text)
            y_val = _safe_float(y_text)
            if s_val is None or x_val is None or y_val is None:
                continue
            wps.append({"lane_id": lane_id, "s": s_val, "x": x_val, "y": y_val})
        if not wps:
            continue
        heading_deg = None
        if len(wps) >= 2:
            dx = wps[1]["x"] - wps[0]["x"]
            dy = wps[1]["y"] - wps[0]["y"]
            if math.hypot(dx, dy) > 1e-6:
                heading_deg = math.degrees(math.atan2(dy, dx))
        requests.append(
            {
                "waypoint_count": len(wps),
                "start": wps[0],
                "goal": wps[1] if len(wps) > 1 else None,
                "route_heading_deg": heading_deg,
            }
        )
    return requests


def _parse_negative_s(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"count": 0, "min_s": None, "samples": []}
    text = path.read_text(errors="ignore")
    pat = re.compile(r"Cannot find waypoint:\s*id\s*=\s*([^\s]+)\s+s\s*=\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)")
    samples: List[Dict[str, Any]] = []
    min_s: Optional[float] = None
    for lane_id, s_text in pat.findall(text):
        s_val = _safe_float(s_text)
        if s_val is None:
            continue
        if s_val < 0.0:
            samples.append({"lane_id": lane_id, "s": s_val})
            if min_s is None or s_val < min_s:
                min_s = s_val
    return {
        "count": len(samples),
        "min_s": min_s,
        "samples": samples[:20],
    }


def diagnose(run_dir: Path, startup_window_sec: float = 15.0) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    debug_rows = _load_debug_rows(artifacts / "debug_timeseries.csv")
    routing_requests = _parse_routing_requests(artifacts / "apollo_routing.INFO")
    negative_s = _parse_negative_s(artifacts / "apollo_planning.INFO")
    scenario_goal = _load_json(artifacts / "scenario_goal.json")
    effective_cfg = _load_yaml(run_dir / "effective.yaml")

    startup_rows = [r for r in debug_rows if r.get("rel_t") is not None and float(r["rel_t"]) <= float(startup_window_sec)]
    map_yaw = [r["map_yaw_deg"] for r in startup_rows if r.get("map_yaw_deg") is not None]
    preview_heading = [r["preview_heading_deg"] for r in startup_rows if r.get("preview_heading_deg") is not None]
    e_psi = [r["e_psi_deg"] for r in startup_rows if r.get("e_psi_deg") is not None]
    lane_inside_ratio = None
    lane_inside_vals = [r.get("lane_inside") for r in startup_rows if r.get("lane_inside") is not None]
    if lane_inside_vals:
        lane_inside_ratio = float(sum(1 for v in lane_inside_vals if bool(v)) / len(lane_inside_vals))

    apollo_cfg = (((effective_cfg.get("algo") or {}).get("apollo") or {}) if effective_cfg else {}) or {}
    routing_cfg = (apollo_cfg.get("routing") or {}) if isinstance(apollo_cfg, dict) else {}
    bridge_cfg = (apollo_cfg.get("bridge") or {}) if isinstance(apollo_cfg, dict) else {}
    tf_cfg = (apollo_cfg.get("carla_to_apollo") or {}) if isinstance(apollo_cfg, dict) else {}

    start_route = routing_requests[0] if routing_requests else {}
    start_route_heading = _safe_float((start_route.get("route_heading_deg") if start_route else None))
    startup_map_yaw_median = _summary(map_yaw).get("median")
    route_vs_map_delta = None
    yaw_suggestion = None
    if start_route_heading is not None and startup_map_yaw_median is not None:
        route_vs_map_delta = _wrap_deg(start_route_heading - startup_map_yaw_median)
        if abs(route_vs_map_delta) >= 20.0:
            yaw_suggestion = _wrap_deg(float(tf_cfg.get("yaw_deg", 0.0) or 0.0) + route_vs_map_delta)

    fallback_spawn = (scenario_goal.get("fallback_spawn_used") or {}) if isinstance(scenario_goal, dict) else {}
    fallback_ego = bool(fallback_spawn.get("ego", False))

    start_s = _safe_float(((start_route.get("start") or {}).get("s") if start_route else None))
    back_offset = _safe_float(bridge_cfg.get("localization_back_offset_m"))
    start_nudge = _safe_float(routing_cfg.get("start_nudge_m"))

    suggestions: List[Dict[str, Any]] = []
    if fallback_ego:
        suggestions.append(
            {
                "code": "spawn_fallback_detected",
                "level": "high",
                "message": "ego 出生点发生 fallback，优先修复 spawn fallback 策略，避免跳到无关 lane 起点。",
            }
        )
    if negative_s.get("count", 0) > 0:
        suggestions.append(
            {
                "code": "negative_s_detected",
                "level": "high",
                "message": "planning 日志存在 s<0，优先检查起点投影和 localization 参考点偏置。",
                "evidence": {
                    "negative_s_count": negative_s.get("count"),
                    "min_s": negative_s.get("min_s"),
                },
            }
        )
    if (negative_s.get("count", 0) > 0) and (back_offset is not None and back_offset > 0.0):
        suggestions.append(
            {
                "code": "back_offset_risk",
                "level": "medium",
                "message": "可先试将 localization_back_offset_m 降到 0.0 验证 reference-point 偏置是否主因。",
                "evidence": {"localization_back_offset_m": back_offset},
            }
        )
    if (negative_s.get("count", 0) > 0) and (start_nudge is not None and start_nudge > 0.0):
        suggestions.append(
            {
                "code": "startup_nudge_risk",
                "level": "medium",
                "message": "startup 起点 nudge 可能把 route start 前推到 ego 前方，先降低到 0.0 做对照。",
                "evidence": {"start_nudge_m": start_nudge, "first_route_start_s": start_s},
            }
        )
    if yaw_suggestion is not None:
        suggestions.append(
            {
                "code": "yaw_adjust_candidate",
                "level": "medium",
                "message": "route heading 与 startup map yaw 存在固定偏差，可尝试调整 carla_to_apollo.yaw_deg。",
                "evidence": {
                    "route_vs_map_delta_deg": route_vs_map_delta,
                    "suggested_carla_to_apollo_yaw_deg": yaw_suggestion,
                },
            }
        )

    payload = {
        "run_dir": str(run_dir.resolve()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "startup_window_sec": startup_window_sec,
        "startup_pose_stats": {
            "map_yaw_deg": _summary(map_yaw),
            "preview_heading_deg": _summary(preview_heading),
            "e_psi_deg": _summary(e_psi),
            "lane_inside_ratio": lane_inside_ratio,
        },
        "routing_requests": {
            "count": len(routing_requests),
            "first_request": start_route,
        },
        "planning_negative_s": negative_s,
        "scenario_spawn": {
            "fallback_spawn_used": fallback_spawn,
            "start_at_write_time": scenario_goal.get("start_at_write_time"),
        },
        "config_snapshot": {
            "routing": {
                "snap_start_to_lane": routing_cfg.get("snap_start_to_lane"),
                "snap_goal_to_lane": routing_cfg.get("snap_goal_to_lane"),
                "start_nudge_m": routing_cfg.get("start_nudge_m"),
                "start_nudge_use_lane_heading": routing_cfg.get("start_nudge_use_lane_heading"),
            },
            "bridge": {
                "localization_back_offset_m": bridge_cfg.get("localization_back_offset_m"),
            },
            "carla_to_apollo": {
                "yaw_deg": tf_cfg.get("yaw_deg"),
                "auto_calib": tf_cfg.get("auto_calib"),
                "auto_calib_snap_right_angle": tf_cfg.get("auto_calib_snap_right_angle"),
            },
        },
        "alignment_checks": {
            "route_heading_vs_startup_map_yaw_delta_deg": route_vs_map_delta,
            "yaw_suggestion_deg": yaw_suggestion,
            "suspect_negative_s": bool(negative_s.get("count", 0) > 0),
            "suspect_spawn_fallback": fallback_ego,
            "suspect_reference_point_mismatch": bool(
                negative_s.get("count", 0) > 0 and back_offset is not None and back_offset > 0.0
            ),
            "suspect_heading_frame_mismatch": bool(
                route_vs_map_delta is not None and abs(route_vs_map_delta) >= 45.0
            ),
        },
        "suggestions": suggestions,
        "notes": [
            "startup debug 的 preview_heading 来自 bridge 内部 map polyline 近邻，不等同于 Apollo HDMap lane heading。",
            "若 routing 已成功但 planning 报 s<0，应优先检查 spawn fallback 与 reference-point/back_offset。",
        ],
    }
    return payload


def _render_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Startup Lane Alignment Summary")
    lines.append("")
    lines.append(f"- run_dir: `{payload.get('run_dir', '')}`")
    lines.append(f"- generated_at_utc: `{payload.get('generated_at_utc', '')}`")
    lines.append("")
    lines.append("## Key Checks")
    lines.append("")
    checks = payload.get("alignment_checks", {}) or {}
    lines.append(f"- suspect_negative_s: `{checks.get('suspect_negative_s')}`")
    lines.append(f"- suspect_spawn_fallback: `{checks.get('suspect_spawn_fallback')}`")
    lines.append(f"- suspect_reference_point_mismatch: `{checks.get('suspect_reference_point_mismatch')}`")
    lines.append(f"- route_heading_vs_startup_map_yaw_delta_deg: `{checks.get('route_heading_vs_startup_map_yaw_delta_deg')}`")
    lines.append(f"- yaw_suggestion_deg: `{checks.get('yaw_suggestion_deg')}`")
    lines.append("")
    lines.append("## Suggestions")
    lines.append("")
    for item in payload.get("suggestions", []) or []:
        lines.append(f"- [{item.get('level')}] `{item.get('code')}`: {item.get('message')}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose startup lane alignment from run artifacts")
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
    out_json = artifacts / "startup_lane_alignment_summary.json"
    out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"[diag][startup-align] written: {out_json}")
    if not args.no_md:
        out_md = artifacts / "startup_lane_alignment_summary.md"
        out_md.write_text(_render_markdown(payload))
        print(f"[diag][startup-align] written: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
