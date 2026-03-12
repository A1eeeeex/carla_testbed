#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out


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


def _count_pattern(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    text = path.read_text(errors="ignore")
    return len(re.findall(pattern, text))


def _extract_negative_s(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"count": 0, "min_s": None}
    text = path.read_text(errors="ignore")
    pat = re.compile(r"Cannot find waypoint:\s*id\s*=\s*([^\s]+)\s+s\s*=\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)")
    min_s: Optional[float] = None
    count = 0
    lane_ids: Dict[str, int] = {}
    for lane_id, s_text in pat.findall(text):
        s_val = _safe_float(s_text)
        if s_val is None or s_val >= 0.0:
            continue
        count += 1
        lane_ids[lane_id] = lane_ids.get(lane_id, 0) + 1
        if min_s is None or s_val < min_s:
            min_s = s_val
    top_lane_id = None
    if lane_ids:
        top_lane_id = sorted(lane_ids.items(), key=lambda kv: kv[1], reverse=True)[0][0]
    return {"count": count, "min_s": min_s, "top_lane_id": top_lane_id}


def diagnose(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    summary = _load_json(run_dir / "summary.json")
    bridge_stats = _load_json(artifacts / "cyber_bridge_stats.json")
    bridge_health = _load_json(artifacts / "bridge_health_summary.json")
    startup_align = _load_json(artifacts / "startup_lane_alignment_summary.json")
    scenario_goal = _load_json(artifacts / "scenario_goal.json")
    effective_cfg = _load_yaml(run_dir / "effective.yaml")

    planning_log = artifacts / "apollo_planning.INFO"
    control_log = artifacts / "apollo_control.INFO"

    planning_ref_lane_empty = _count_pattern(planning_log, r"Reference lane is empty")
    planning_input_check_failed = _count_pattern(planning_log, r"Input check failed")
    planning_cannot_find_waypoint = _count_pattern(planning_log, r"Cannot find waypoint")
    planning_failed_extract_segments = _count_pattern(planning_log, r"Failed to extract segments from routing")
    planning_fail_get_reference_line = _count_pattern(planning_log, r"Fail to get reference line")

    control_no_trajectory = _count_pattern(control_log, r"planning has no trajectory point")
    control_failed_cmd = _count_pattern(control_log, r"Failed to produce control command:planning has no trajectory point")

    negative_s = _extract_negative_s(planning_log)

    routing_success_count = int(bridge_stats.get("routing_success_count", 0) or 0)
    routing_request_count = int(bridge_stats.get("routing_request_count", 0) or 0)
    routing_response_count = int(bridge_stats.get("routing_response_count", 0) or 0)

    apollo_cfg = (((effective_cfg.get("algo") or {}).get("apollo") or {}) if effective_cfg else {}) or {}
    routing_cfg = (apollo_cfg.get("routing") or {}) if isinstance(apollo_cfg, dict) else {}
    bridge_cfg = (apollo_cfg.get("bridge") or {}) if isinstance(apollo_cfg, dict) else {}

    fallback_spawn = (scenario_goal.get("fallback_spawn_used") or {}) if isinstance(scenario_goal, dict) else {}
    fallback_ego = bool(fallback_spawn.get("ego", False))
    back_offset = _safe_float(bridge_cfg.get("localization_back_offset_m"))

    startup_checks = (startup_align.get("alignment_checks") or {}) if isinstance(startup_align, dict) else {}
    heading_delta = _safe_float(startup_checks.get("route_heading_vs_startup_map_yaw_delta_deg"))

    ranking: List[Dict[str, Any]] = []

    if routing_success_count >= 1 and planning_ref_lane_empty > 0 and control_no_trajectory > 0:
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "high",
                "cause": "reference_line_build_failed_after_routing_success",
                "evidence": {
                    "routing_success_count": routing_success_count,
                    "planning_ref_lane_empty": planning_ref_lane_empty,
                    "control_no_trajectory": control_no_trajectory,
                },
            }
        )

    if negative_s.get("count", 0) > 0:
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "high",
                "cause": "lane_projection_negative_s",
                "evidence": negative_s,
            }
        )

    if fallback_ego:
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "high",
                "cause": "ego_spawn_fallback_to_unintended_index",
                "evidence": {"fallback_spawn_used": fallback_spawn},
            }
        )

    if negative_s.get("count", 0) > 0 and back_offset is not None and back_offset > 0.0:
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "medium",
                "cause": "localization_reference_point_offset_risk",
                "evidence": {
                    "localization_back_offset_m": back_offset,
                    "min_negative_s": negative_s.get("min_s"),
                },
            }
        )

    if not bool(routing_cfg.get("snap_start_to_lane", True)) or not bool(
        routing_cfg.get("start_nudge_use_lane_heading", True)
    ):
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "medium",
                "cause": "startup_lane_projection_guards_disabled",
                "evidence": {
                    "snap_start_to_lane": routing_cfg.get("snap_start_to_lane"),
                    "start_nudge_use_lane_heading": routing_cfg.get("start_nudge_use_lane_heading"),
                },
            }
        )

    if heading_delta is not None and abs(heading_delta) >= 45.0:
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "medium",
                "cause": "heading_frame_mismatch_possible",
                "evidence": {"route_heading_vs_startup_map_yaw_delta_deg": heading_delta},
            }
        )

    if not ranking:
        ranking.append(
            {
                "rank": 1,
                "level": "unknown",
                "cause": "insufficient_evidence",
                "evidence": {},
            }
        )

    conclusion = (
        "routing 已成功，但 reference line/trajectory 构建失败；主阻塞在起点投影与定位参考点链路，不在交通灯或末端 steer guard。"
        if routing_success_count >= 1 and planning_ref_lane_empty > 0
        else "未观测到典型 reference line 失败模式。"
    )

    payload = {
        "run_dir": str(run_dir.resolve()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "routing": {
            "routing_request_count": routing_request_count,
            "routing_response_count": routing_response_count,
            "routing_success_count": routing_success_count,
            "routing_goal_dist_m": bridge_health.get("routing_goal_dist_m"),
        },
        "planning_failure_signals": {
            "reference_lane_empty_count": planning_ref_lane_empty,
            "input_check_failed_count": planning_input_check_failed,
            "cannot_find_waypoint_count": planning_cannot_find_waypoint,
            "failed_extract_segments_count": planning_failed_extract_segments,
            "fail_get_reference_line_count": planning_fail_get_reference_line,
            "negative_s": negative_s,
        },
        "control_failure_signals": {
            "planning_no_trajectory_count": control_no_trajectory,
            "failed_produce_control_count": control_failed_cmd,
        },
        "scenario_and_config": {
            "summary_success": summary.get("success"),
            "summary_fail_reason": summary.get("fail_reason"),
            "fallback_spawn_used": fallback_spawn,
            "routing": {
                "snap_start_to_lane": routing_cfg.get("snap_start_to_lane"),
                "start_nudge_use_lane_heading": routing_cfg.get("start_nudge_use_lane_heading"),
                "start_nudge_m": routing_cfg.get("start_nudge_m"),
            },
            "bridge": {
                "localization_back_offset_m": bridge_cfg.get("localization_back_offset_m"),
            },
        },
        "root_cause_ranking": ranking,
        "conclusion": conclusion,
        "notes": [
            "routing_success 只代表 route graph 有解，不保证 ADC 当前定位点能被 reference_line_provider 正确接纳。",
            "出现 `Cannot find waypoint ... s<0` 时，应优先检查 spawn fallback、起点投影和 localization reference point。",
        ],
    }
    return payload


def _render_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Reference Line Failure Diagnosis")
    lines.append("")
    lines.append(f"- run_dir: `{payload.get('run_dir', '')}`")
    lines.append(f"- generated_at_utc: `{payload.get('generated_at_utc', '')}`")
    lines.append("")
    pf = payload.get("planning_failure_signals", {}) or {}
    cf = payload.get("control_failure_signals", {}) or {}
    lines.append("## Key Signals")
    lines.append("")
    lines.append(f"- reference_lane_empty_count: `{pf.get('reference_lane_empty_count')}`")
    lines.append(f"- cannot_find_waypoint_count: `{pf.get('cannot_find_waypoint_count')}`")
    lines.append(f"- negative_s: `{pf.get('negative_s')}`")
    lines.append(f"- planning_no_trajectory_count: `{cf.get('planning_no_trajectory_count')}`")
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
    parser = argparse.ArgumentParser(description="Diagnose reference-line failure from run artifacts")
    parser.add_argument("--run-dir", required=True, help="run directory path, e.g. runs/<run_name>")
    parser.add_argument("--no-md", action="store_true", help="skip markdown output")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.is_dir():
        raise SystemExit(f"run dir not found: {run_dir}")
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)

    payload = diagnose(run_dir)
    out_json = artifacts / "reference_line_rootcause_summary.json"
    out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"[diag][reference-line] written: {out_json}")
    if not args.no_md:
        out_md = artifacts / "reference_line_rootcause_summary.md"
        out_md.write_text(_render_markdown(payload))
        print(f"[diag][reference-line] written: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
