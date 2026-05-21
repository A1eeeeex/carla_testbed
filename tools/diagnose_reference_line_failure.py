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


def _extract_waypoint_lookup_summary(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "count": 0,
            "negative_s_count": 0,
            "nonnegative_s_count": 0,
            "min_s": None,
            "max_s": None,
            "top_lane_id": None,
            "top_waypoint_signature": None,
            "unique_waypoint_signature_count": 0,
        }
    text = path.read_text(errors="ignore")
    pat = re.compile(
        r"Cannot find waypoint:\s*id\s*=\s*([^\s]+)\s+s\s*=\s*([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)"
    )
    values: List[float] = []
    lane_counts: Dict[str, int] = {}
    signature_counts: Dict[str, int] = {}
    negative_s_count = 0
    nonnegative_s_count = 0
    for lane_id, s_text in pat.findall(text):
        s_val = _safe_float(s_text)
        if s_val is None:
            continue
        values.append(s_val)
        lane_counts[lane_id] = lane_counts.get(lane_id, 0) + 1
        signature = f"{lane_id}@{s_val:.4f}"
        signature_counts[signature] = signature_counts.get(signature, 0) + 1
        if s_val < 0.0:
            negative_s_count += 1
        else:
            nonnegative_s_count += 1
    top_lane_id = None
    top_waypoint_signature = None
    if lane_counts:
        top_lane_id = sorted(lane_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]
    if signature_counts:
        top_waypoint_signature = sorted(signature_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]
    return {
        "count": len(values),
        "negative_s_count": negative_s_count,
        "nonnegative_s_count": nonnegative_s_count,
        "min_s": min(values) if values else None,
        "max_s": max(values) if values else None,
        "top_lane_id": top_lane_id,
        "top_waypoint_signature": top_waypoint_signature,
        "unique_waypoint_signature_count": len(signature_counts),
    }


def _parse_routing_lane_window(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "request_count": 0,
            "latest_lane_id": None,
            "latest_start_s": None,
            "latest_end_s": None,
            "rows": [],
        }
    rows: List[Dict[str, Any]] = []
    pat = re.compile(
        r"navigator\.cc:89\]\s+([^\s]+)\s+\d+\s+([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)"
    )
    for line in path.read_text(errors="ignore").splitlines():
        m = pat.search(line)
        if not m:
            continue
        rows.append(
            {
                "lane_id": m.group(1),
                "start_s": _safe_float(m.group(2)),
                "end_s": _safe_float(m.group(3)),
            }
        )
    latest = rows[-1] if rows else {}
    return {
        "request_count": len(rows),
        "latest_lane_id": latest.get("lane_id"),
        "latest_start_s": latest.get("start_s"),
        "latest_end_s": latest.get("end_s"),
        "rows": rows,
    }


def _route_debug_summary(path: Path) -> Dict[str, Any]:
    rows = _load_jsonl(path)
    if not rows:
        return {
            "route_segment_present_reference_line_missing_count": 0,
            "current_lane_id_missing_count": 0,
            "target_lane_id_missing_count": 0,
            "last_route_segment_count": None,
            "last_reference_line_count": None,
            "last_lane_follow_map_status": None,
        }
    ref_missing_count = 0
    current_lane_missing_count = 0
    target_lane_missing_count = 0
    for row in rows:
        lane_follow_status = str(row.get("lane_follow_map_status", "") or "")
        if lane_follow_status == "route_segments_present_reference_line_missing":
            ref_missing_count += 1
        if not str(row.get("current_lane_id", "") or "").strip():
            current_lane_missing_count += 1
        if not str(row.get("target_lane_id_first", "") or "").strip():
            target_lane_missing_count += 1
    last = rows[-1]
    return {
        "route_segment_present_reference_line_missing_count": ref_missing_count,
        "current_lane_id_missing_count": current_lane_missing_count,
        "target_lane_id_missing_count": target_lane_missing_count,
        "last_route_segment_count": int(last.get("route_segment_count", 0) or 0),
        "last_reference_line_count": int(last.get("reference_line_count", 0) or 0),
        "last_lane_follow_map_status": str(last.get("lane_follow_map_status", "") or ""),
    }


def _container_hash_diff_summary(map_guard: Dict[str, Any]) -> Dict[str, Any]:
    hash_summary = map_guard.get("hash_summary") or {}
    out: Dict[str, Any] = {
        "runtime_resolution_mode": map_guard.get("runtime_resolution_mode"),
        "runtime_map_dir_container_actual": map_guard.get("runtime_map_dir_container_actual"),
        "runtime_component_source": map_guard.get("runtime_component_source"),
        "base_map_hash_equal": None,
        "routing_map_hash_equal": None,
        "sim_map_hash_equal": None,
    }
    if not isinstance(hash_summary, dict):
        return out
    for component in ("base_map", "routing_map", "sim_map"):
        detail = hash_summary.get(component) or {}
        if isinstance(detail, dict):
            out[f"{component}_hash_equal"] = detail.get("hash_equal")
            out[f"{component}_runtime_sha256"] = detail.get("runtime_sha256")
            out[f"{component}_effective_bridge_sha256"] = detail.get("effective_bridge_sha256")
            out[f"{component}_runtime_source"] = detail.get("runtime_source")
    return out


def diagnose(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    summary = _load_json(run_dir / "summary.json")
    bridge_stats = _load_json(artifacts / "cyber_bridge_stats.json")
    bridge_health = _load_json(artifacts / "bridge_health_summary.json")
    startup_align = _load_json(artifacts / "startup_lane_alignment_summary.json")
    scenario_goal = _load_json(artifacts / "scenario_goal.json")
    effective_cfg = _load_yaml(run_dir / "effective.yaml")
    map_guard = _load_json(artifacts / "map_contract_guard.json")

    planning_log = artifacts / "apollo_planning.INFO"
    routing_log = artifacts / "apollo_routing.INFO"
    control_log = artifacts / "apollo_control.INFO"
    route_debug_path = artifacts / "apollo_reference_line_debug.jsonl"

    planning_ref_lane_empty = _count_pattern(planning_log, r"Reference lane is empty")
    planning_input_check_failed = _count_pattern(planning_log, r"Input check failed")
    planning_cannot_find_waypoint = _count_pattern(planning_log, r"Cannot find waypoint")
    planning_failed_extract_segments = _count_pattern(planning_log, r"Failed to extract segments from routing")
    planning_fail_get_reference_line = _count_pattern(planning_log, r"Fail to get reference line")

    control_no_trajectory = _count_pattern(control_log, r"planning has no trajectory point")
    control_failed_cmd = _count_pattern(control_log, r"Failed to produce control command:planning has no trajectory point")

    negative_s = _extract_negative_s(planning_log)
    waypoint_lookup = _extract_waypoint_lookup_summary(planning_log)
    routing_lane_window = _parse_routing_lane_window(routing_log)
    route_debug = _route_debug_summary(route_debug_path)
    map_hash_summary = _container_hash_diff_summary(map_guard)

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

    top_lookup_lane = str(waypoint_lookup.get("top_lane_id") or "")
    latest_routing_lane = str(routing_lane_window.get("latest_lane_id") or "")
    latest_start_s = _safe_float(routing_lane_window.get("latest_start_s"))
    lookup_min_s = _safe_float(waypoint_lookup.get("min_s"))
    if (
        routing_success_count >= 1
        and int(waypoint_lookup.get("nonnegative_s_count", 0) or 0) > 0
        and int(route_debug.get("route_segment_present_reference_line_missing_count", 0) or 0) > 0
        and top_lookup_lane
        and latest_routing_lane
        and top_lookup_lane == latest_routing_lane
    ):
        evidence: Dict[str, Any] = {
            "top_lane_id": top_lookup_lane,
            "top_waypoint_signature": waypoint_lookup.get("top_waypoint_signature"),
            "latest_routing_lane_id": latest_routing_lane,
            "latest_routing_start_s": latest_start_s,
            "latest_routing_end_s": routing_lane_window.get("latest_end_s"),
            "route_segment_present_reference_line_missing_count": route_debug.get(
                "route_segment_present_reference_line_missing_count"
            ),
            "current_lane_id_missing_count": route_debug.get("current_lane_id_missing_count"),
            "target_lane_id_missing_count": route_debug.get("target_lane_id_missing_count"),
        }
        if lookup_min_s is not None and latest_start_s is not None:
            evidence["lookup_minus_latest_route_start_s"] = lookup_min_s - latest_start_s
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "high",
                "cause": "lane_follow_waypoint_lookup_failed_on_routed_lane",
                "evidence": evidence,
            }
        )

    if (
        map_hash_summary.get("base_map_hash_equal") is False
        and map_hash_summary.get("routing_map_hash_equal") is True
        and map_hash_summary.get("sim_map_hash_equal") is True
    ):
        ranking.append(
            {
                "rank": len(ranking) + 1,
                "level": "high",
                "cause": "container_base_map_hash_differs_from_effective_bridge",
                "evidence": {
                    "runtime_resolution_mode": map_hash_summary.get("runtime_resolution_mode"),
                    "runtime_map_dir_container_actual": map_hash_summary.get(
                        "runtime_map_dir_container_actual"
                    ),
                    "base_map_runtime_sha256": map_hash_summary.get("base_map_runtime_sha256"),
                    "base_map_effective_bridge_sha256": map_hash_summary.get(
                        "base_map_effective_bridge_sha256"
                    ),
                    "routing_map_hash_equal": map_hash_summary.get("routing_map_hash_equal"),
                    "sim_map_hash_equal": map_hash_summary.get("sim_map_hash_equal"),
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

    if (
        routing_success_count >= 1
        and top_lookup_lane
        and latest_routing_lane
        and top_lookup_lane == latest_routing_lane
        and int(waypoint_lookup.get("nonnegative_s_count", 0) or 0) > 0
    ):
        delta_text = ""
        if lookup_min_s is not None and latest_start_s is not None:
            delta_text = f"；lookup s 比最新 long-route start_s 早约 {latest_start_s - lookup_min_s:.2f}m"
        conclusion = (
            "routing 已成功落在同一条 lane 上，但 lane_follow/reference_line provider 仍在同 lane id 的正向 s 域反复找不到 waypoint"
            f"{delta_text}；当前主阻塞已经更具体地压缩到 runtime base_map 漂移或 vehicle-state/waypoint s 域不一致。"
        )
    elif routing_success_count >= 1 and planning_ref_lane_empty > 0:
        conclusion = "routing 已成功，但 reference line/trajectory 构建失败；主阻塞在 reference-line provider 链。"
    else:
        conclusion = "未观测到典型 reference line 失败模式。"

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
            "waypoint_lookup_summary": waypoint_lookup,
        },
        "control_failure_signals": {
            "planning_no_trajectory_count": control_no_trajectory,
            "failed_produce_control_count": control_failed_cmd,
        },
        "routing_lane_window": routing_lane_window,
        "route_debug_summary": route_debug,
        "map_contract_signals": {
            "runtime_resolution_mode": map_guard.get("runtime_resolution_mode"),
            "runtime_map_dir": map_guard.get("runtime_map_dir"),
            "runtime_map_dir_container_actual": map_guard.get("runtime_map_dir_container_actual"),
            "runtime_component_source": map_guard.get("runtime_component_source"),
            "same_derivation_chain": map_guard.get("same_derivation_chain"),
            "mismatch_reasons": map_guard.get("mismatch_reasons") or [],
            "hash_summary": map_guard.get("hash_summary") or {},
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
            "出现 `Cannot find waypoint ... s>0` 且 lane id 与 routing 最新请求一致时，应优先检查 runtime base_map 身份与 vehicle-state/waypoint s 域一致性。",
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
    lines.append(f"- waypoint_lookup_summary: `{pf.get('waypoint_lookup_summary')}`")
    lines.append(f"- planning_no_trajectory_count: `{cf.get('planning_no_trajectory_count')}`")
    lines.append(f"- routing_lane_window: `{payload.get('routing_lane_window')}`")
    lines.append(f"- map_contract_signals: `{payload.get('map_contract_signals')}`")
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
