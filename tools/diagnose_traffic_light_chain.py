#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


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


def _load_debug_policy_values(path: Path) -> List[str]:
    if not path.exists():
        return []
    values: List[str] = []
    try:
        with path.open("r", newline="") as fp:
            for row in csv.DictReader(fp):
                val = str(row.get("traffic_light_policy", "") or "").strip()
                if val:
                    values.append(val)
    except Exception:
        return []
    return sorted(set(values))


def diagnose(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    effective_cfg = _load_yaml(run_dir / "effective.yaml")
    bridge_stats = _load_json(artifacts / "cyber_bridge_stats.json")
    bridge_health = _load_json(artifacts / "bridge_health_summary.json")
    debug_policy_values = _load_debug_policy_values(artifacts / "debug_timeseries.csv")

    apollo_cfg = (((effective_cfg.get("algo") or {}).get("apollo") or {}) if effective_cfg else {}) or {}
    cfg_tl = (apollo_cfg.get("traffic_light") or {}) if isinstance(apollo_cfg, dict) else {}
    cfg_plan = (apollo_cfg.get("planning") or {}) if isinstance(apollo_cfg, dict) else {}

    cfg_policy = str(cfg_tl.get("policy", "") or "").strip().lower()
    cfg_force_ids = [str(x) for x in (cfg_tl.get("force_ids") or [])]
    cfg_disable_tl_rule = bool(cfg_plan.get("disable_traffic_light_rule", False))

    stats_tl = (bridge_stats.get("traffic_light") or {}) if isinstance(bridge_stats, dict) else {}
    runtime_policy = str(stats_tl.get("policy", bridge_health.get("traffic_light_policy", "")) or "").strip().lower()
    writer_enabled = bool(stats_tl.get("writer_enabled", False))
    publish_count = int(stats_tl.get("publish_count", bridge_health.get("traffic_light_force_green_publish_count", 0)) or 0)
    proto_error = str(stats_tl.get("proto_error", "") or "")
    runtime_force_ids = [str(x) for x in (stats_tl.get("force_ids") or cfg_force_ids)]

    causes: List[Dict[str, Any]] = []
    if cfg_policy != "force_green":
        causes.append(
            {
                "rank": 1,
                "severity": "high",
                "code": "config_not_force_green",
                "message": "配置层 traffic_light.policy 不是 force_green，桥接层不会走绿灯发布分支。",
                "evidence": {"configured_policy": cfg_policy or "<empty>"},
            }
        )
    if cfg_disable_tl_rule:
        causes.append(
            {
                "rank": 2,
                "severity": "high",
                "code": "planning_rule_disabled",
                "message": "planning.disable_traffic_light_rule=true，即便发布绿灯也可能被规划层规则裁掉。",
                "evidence": {"disable_traffic_light_rule": True},
            }
        )
    if cfg_policy == "force_green" and runtime_policy == "ignore":
        msg = "运行时 traffic_light.policy 从 force_green 退化到 ignore。"
        if proto_error:
            msg += "存在 proto_error，疑似 traffic_light protobuf 不可用。"
        causes.append(
            {
                "rank": 3,
                "severity": "high",
                "code": "runtime_fallback_to_ignore",
                "message": msg,
                "evidence": {"runtime_policy": runtime_policy, "proto_error": proto_error},
            }
        )
    if runtime_policy == "force_green" and publish_count <= 0:
        causes.append(
            {
                "rank": 4,
                "severity": "medium",
                "code": "no_force_green_publish",
                "message": "运行时虽然是 force_green，但 publish_count=0，未形成有效绿灯流。",
                "evidence": {"writer_enabled": writer_enabled, "publish_count": publish_count},
            }
        )
    if runtime_policy in {"force_green", "ignore"} and len(runtime_force_ids) <= 1:
        causes.append(
            {
                "rank": 5,
                "severity": "medium",
                "code": "force_ids_limited",
                "message": "force_green 仅覆盖固定少量 signal id，不是全局绿灯机制。",
                "evidence": {"force_ids": runtime_force_ids},
            }
        )
    if debug_policy_values:
        causes.append(
            {
                "rank": 6,
                "severity": "info",
                "code": "debug_policy_observed",
                "message": "debug_timeseries 中观测到的 traffic_light_policy。",
                "evidence": {"debug_policies": debug_policy_values},
            }
        )

    if cfg_policy != "force_green":
        conclusion = "当前 run 未启用 force_green（配置即 ignore），所以“始终发绿灯”根本没有进入执行分支。"
    elif runtime_policy == "ignore":
        conclusion = "配置要求 force_green，但运行时退化为 ignore（通常与 proto 不可用或分支降级有关）。"
    elif publish_count <= 0:
        conclusion = "配置和运行时都在 force_green 分支，但没有实际发布流，绿灯链路未生效。"
    else:
        conclusion = "force_green 在运行，但它只对 force_ids 生效，不能代表全局交通灯真值同步。"

    summary = {
        "run_dir": str(run_dir.resolve()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "traffic_light_chain": {
            "configured_policy": cfg_policy or None,
            "configured_force_ids": cfg_force_ids,
            "configured_disable_traffic_light_rule": cfg_disable_tl_rule,
            "runtime_policy": runtime_policy or None,
            "runtime_writer_enabled": writer_enabled,
            "runtime_publish_count": publish_count,
            "runtime_proto_error": proto_error or None,
            "runtime_force_ids": runtime_force_ids,
            "debug_timeseries_policy_values": debug_policy_values,
            "force_green_branch_entered": runtime_policy == "force_green",
            "force_green_stream_active": runtime_policy == "force_green" and writer_enabled and publish_count > 0,
        },
        "root_causes": causes,
        "conclusion": conclusion,
        "notes": [
            "force_green 为固定 signal id 注入，不等同于 CARLA 世界交通灯状态同步。",
            "若 planning 侧 traffic light rule 被关闭，交通灯输入对规划行为影响会弱化或无效。",
        ],
    }
    return summary


def _render_markdown(payload: Dict[str, Any]) -> str:
    tl = payload.get("traffic_light_chain", {}) or {}
    lines: List[str] = []
    lines.append("# Traffic Light Chain Diagnosis")
    lines.append("")
    lines.append(f"- run_dir: `{payload.get('run_dir', '')}`")
    lines.append(f"- generated_at_utc: `{payload.get('generated_at_utc', '')}`")
    lines.append(f"- configured_policy: `{tl.get('configured_policy')}`")
    lines.append(f"- runtime_policy: `{tl.get('runtime_policy')}`")
    lines.append(f"- runtime_writer_enabled: `{tl.get('runtime_writer_enabled')}`")
    lines.append(f"- runtime_publish_count: `{tl.get('runtime_publish_count')}`")
    lines.append(f"- configured_disable_traffic_light_rule: `{tl.get('configured_disable_traffic_light_rule')}`")
    lines.append(f"- debug_policy_values: `{tl.get('debug_timeseries_policy_values')}`")
    lines.append("")
    lines.append("## Conclusion")
    lines.append("")
    lines.append(payload.get("conclusion", ""))
    lines.append("")
    lines.append("## Root Causes")
    lines.append("")
    for item in payload.get("root_causes", []) or []:
        lines.append(
            f"- [{item.get('severity')}] `{item.get('code')}`: {item.get('message')} | evidence={item.get('evidence')}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose GT traffic light chain from run artifacts")
    parser.add_argument("--run-dir", required=True, help="run directory path, e.g. runs/<run_name>")
    parser.add_argument("--no-md", action="store_true", help="skip markdown output")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.is_dir():
        raise SystemExit(f"run dir not found: {run_dir}")
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)

    payload = diagnose(run_dir)
    out_json = artifacts / "traffic_light_debug_summary.json"
    out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"[diag][traffic-light] written: {out_json}")

    if not args.no_md:
        out_md = artifacts / "traffic_light_debug_summary.md"
        out_md.write_text(_render_markdown(payload))
        print(f"[diag][traffic-light] written: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
