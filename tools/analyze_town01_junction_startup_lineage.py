#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import safe_float
from tools.run_town01_route_health import _startup_probe_attempt_rows


DEFAULT_ANCHOR_SUMMARY = (
    "runs/town01_capability_promotion_junction_leftturn_lateral_20260327_161900/"
    "junction_traverse__focus_left_turn/"
    "junction_traverse__focus_left_turn__manual_online__town01_rh_spawn181_goal066__02/summary.json"
)
DEFAULT_FRESH_PROBE = (
    "runs/town01_capability_weekend_postcal_junction_probe_20260328_140803/"
    "carla_boot/carla_startup_probe.json"
)
DEFAULT_OUTPUT_MD = "artifacts/town01_junction_startup_lineage_contrast_20260328.md"
DEFAULT_OUTPUT_JSON = "artifacts/town01_junction_startup_lineage_contrast_20260328.json"


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_anchor_probe_path(summary_path: Path) -> Path | None:
    for candidate_root in (
        summary_path.parent,
        summary_path.parent.parent,
        summary_path.parent.parent.parent,
    ):
        candidate = candidate_root / "carla_boot" / "carla_startup_probe.json"
        if candidate.exists():
            return candidate
    return None


def _startup_lineage_payload(probe_path: Path) -> Dict[str, Any]:
    payload = _load_json(probe_path)
    rows = _startup_probe_attempt_rows(payload)
    if not rows:
        return {
            "probe_path": str(probe_path),
            "probe_present": False,
            "attempt_count": 0,
            "lineage_class": "probe_missing",
            "first_world_ready_attempt": 0,
            "pre_world_ready_failure_families": [],
            "final_status": "none",
            "final_failure_family": "none",
            "open_ports": [],
            "rpc_handshake_ready": False,
            "process_alive": False,
        }
    first_world_ready_index = next(
        (index for index, row in enumerate(rows) if bool(row.get("world_ready"))),
        -1,
    )
    first_world_ready_attempt = (
        int(safe_float(rows[first_world_ready_index].get("attempt")) or 0)
        if first_world_ready_index >= 0
        else 0
    )
    pre_world_ready_rows = rows[:first_world_ready_index] if first_world_ready_index >= 0 else rows
    pre_world_ready_failure_families: List[str] = []
    for row in pre_world_ready_rows:
        family = str(row.get("failure_family") or "").strip()
        if not family or family == "world_ready":
            continue
        if family not in pre_world_ready_failure_families:
            pre_world_ready_failure_families.append(family)
    final_row = rows[-1]
    diagnostics = final_row.get("launcher_diagnostics") or {}
    target_ports = diagnostics.get("target_port_snapshot") or []
    open_ports = [
        str(item.get("port"))
        for item in target_ports
        if isinstance(item, dict) and bool(item.get("open"))
    ]
    lineage_class = "startup_failed"
    if first_world_ready_attempt == 1:
        lineage_class = "world_ready_direct"
    elif first_world_ready_attempt > 1:
        lineage_class = "world_ready_after_retry"
    return {
        "probe_path": str(probe_path),
        "probe_present": True,
        "attempt_count": len(rows),
        "lineage_class": lineage_class,
        "first_world_ready_attempt": first_world_ready_attempt,
        "pre_world_ready_failure_families": pre_world_ready_failure_families,
        "final_status": str(final_row.get("status") or "").strip() or "none",
        "final_failure_family": str(final_row.get("failure_family") or "").strip() or "none",
        "open_ports": open_ports,
        "rpc_handshake_ready": bool(diagnostics.get("rpc_handshake_ready")),
        "process_alive": bool(diagnostics.get("process_alive")),
    }


def _contrast_payload(
    *,
    anchor_summary_path: Path,
    fresh_probe_path: Path,
    fresh_route_id: str,
    capability: str,
    runtime_profile: str,
) -> Dict[str, Any]:
    anchor_summary = _load_json(anchor_summary_path)
    anchor_probe_path = _resolve_anchor_probe_path(anchor_summary_path)
    anchor_route_id = str(anchor_summary.get("route_id") or "").strip() or "unknown_anchor_route"
    anchor_lineage = (
        _startup_lineage_payload(anchor_probe_path) if anchor_probe_path is not None else _startup_lineage_payload(Path(""))
    )
    fresh_lineage = _startup_lineage_payload(fresh_probe_path)
    return {
        "generated_at_local": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "capability": capability,
        "runtime_profile": runtime_profile,
        "anchor": {
            "route_id": anchor_route_id,
            "summary_path": str(anchor_summary_path),
            "probe_path": str(anchor_probe_path) if anchor_probe_path is not None else "",
            "startup": anchor_lineage,
        },
        "fresh_probe": {
            "route_id": fresh_route_id,
            "probe_path": str(fresh_probe_path),
            "startup": fresh_lineage,
        },
        "recommended_command": (
            "python3 tools/run_town01_route_health.py run "
            "--config configs/io/examples/town01_apollo_route_health_lateral_enabled_guarded.yaml "
            f"--capability-profile {capability} "
            f"--route-id {fresh_route_id} "
            "--batch-root runs/town01_weekend_junction_followup_isolation_20260328 "
            "--ticks 700 "
            "--enable-lateral "
            "--enable-guard "
            "--carla-launch-attempts 4 "
            "--carla-retry-delay-sec 5 "
            "--carla-no-retry-failure-families rpc_ready_followup_missing_eof_alive"
        ),
    }


def _markdown(payload: Dict[str, Any]) -> str:
    anchor = payload.get("anchor", {}) or {}
    fresh = payload.get("fresh_probe", {}) or {}
    anchor_startup = anchor.get("startup", {}) or {}
    fresh_startup = fresh.get("startup", {}) or {}
    anchor_pre = ", ".join(anchor_startup.get("pre_world_ready_failure_families", []) or []) or "none"
    fresh_pre = ", ".join(fresh_startup.get("pre_world_ready_failure_families", []) or []) or "none"
    fresh_ports = " / ".join(fresh_startup.get("open_ports", []) or []) or "none"
    command = str(payload.get("recommended_command") or "")
    return "\n".join(
        [
            "# Town01 Junction Startup Lineage Contrast",
            "",
            f"- generated_at_local: `{payload.get('generated_at_local', '')}`",
            f"- capability: `{payload.get('capability', '')}`",
            f"- runtime_profile: `{payload.get('runtime_profile', '')}`",
            "",
            "## Compared Evidence",
            "",
            f"- anchor runtime summary: [{Path(str(anchor.get('summary_path', ''))).name}]({anchor.get('summary_path', '')})",
            f"- anchor startup probe: [{Path(str(anchor.get('probe_path', ''))).name}]({anchor.get('probe_path', '')})",
            f"- fresh blocker probe: [{Path(str(fresh.get('probe_path', ''))).name}]({fresh.get('probe_path', '')})",
            "",
            "## Contrast Table",
            "",
            "| case | route / intent | startup lineage | first blocking family before route execution | execution result |",
            "| --- | --- | --- | --- | --- |",
            "| anchor | `{}` | `{}` | `{}` | canonical direct-runtime anchor survives startup and still gives the strongest `junction_traverse` semantic evidence |".format(
                anchor.get("route_id", ""),
                anchor_startup.get("lineage_class", "none"),
                (
                    f"{anchor_pre} on attempt 1, then world_ready on attempt {anchor_startup.get('first_world_ready_attempt', 0)}"
                    if anchor_startup.get("first_world_ready_attempt")
                    else anchor_startup.get("final_failure_family", "none")
                ),
            ),
            "| fresh blocker | intended `{}` | `{}` | `{}` | never enters route execution, so it blocks fresh online verification before capability evidence can update |".format(
                fresh.get("route_id", ""),
                fresh_startup.get("lineage_class", "none"),
                (
                    f"{fresh_startup.get('final_failure_family', 'none')} on attempt 1 with ports open and process {'alive' if fresh_startup.get('process_alive') else 'not_alive'}"
                ),
            ),
            "",
            "## Read",
            "",
            "- The current split is no longer just:",
            "  - `good semantic anchor` vs `bad fresh rerun`",
            "- It is now more exactly:",
            f"  - anchor `{anchor.get('route_id', '')}`",
            "    - already proves the retryable same-probe recovery path is real",
            f"    - because its own startup lineage is `{anchor_startup.get('lineage_class', 'none')}`",
            f"    - and its pre-world-ready failure family is `{anchor_pre}`",
            f"  - fresh `{fresh.get('route_id', '')}` probe",
            "    - fails in a different family",
            f"    - `{fresh_startup.get('final_failure_family', 'none')}`",
            f"    - with `rpc_handshake_ready={fresh_startup.get('rpc_handshake_ready', False)}`, open ports `{fresh_ports}`, and process `{'alive' if fresh_startup.get('process_alive') else 'not_alive'}`",
            "",
            "## Operational Consequence",
            "",
            "- The next minimal high-yield Town01 live step should not treat these two startup families as interchangeable noise.",
            "- Current live tactic should stay split:",
            "  - keep bounded same-probe retry available for:",
            f"    - `{anchor_pre}`",
            "  - isolate first and avoid same-probe retry for:",
            f"    - `{fresh_startup.get('final_failure_family', 'none')}`",
            "",
            "## Recommended Next Live Command",
            "",
            "```bash",
            command,
            "```",
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a focused Town01 junction startup lineage contrast artifact.")
    parser.add_argument("--anchor-summary", default=DEFAULT_ANCHOR_SUMMARY)
    parser.add_argument("--fresh-probe", default=DEFAULT_FRESH_PROBE)
    parser.add_argument("--fresh-route-id", default="town01_rh_spawn219_goal062")
    parser.add_argument("--capability", default="junction_traverse")
    parser.add_argument("--runtime-profile", default="lateral-enabled + guard")
    parser.add_argument("--output-md", default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--output-json", default=DEFAULT_OUTPUT_JSON)
    args = parser.parse_args()

    anchor_summary_path = Path(args.anchor_summary).expanduser()
    if not anchor_summary_path.is_absolute():
        anchor_summary_path = (REPO_ROOT / anchor_summary_path).resolve()
    fresh_probe_path = Path(args.fresh_probe).expanduser()
    if not fresh_probe_path.is_absolute():
        fresh_probe_path = (REPO_ROOT / fresh_probe_path).resolve()
    output_md_path = Path(args.output_md).expanduser()
    if not output_md_path.is_absolute():
        output_md_path = (REPO_ROOT / output_md_path).resolve()
    output_json_path = Path(args.output_json).expanduser()
    if not output_json_path.is_absolute():
        output_json_path = (REPO_ROOT / output_json_path).resolve()

    payload = _contrast_payload(
        anchor_summary_path=anchor_summary_path,
        fresh_probe_path=fresh_probe_path,
        fresh_route_id=str(args.fresh_route_id),
        capability=str(args.capability),
        runtime_profile=str(args.runtime_profile),
    )
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    output_md_path.write_text(_markdown(payload), encoding="utf-8")
    print(f"[town01-junction-startup-lineage] written_md={output_md_path}")
    print(f"[town01-junction-startup-lineage] written_json={output_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
