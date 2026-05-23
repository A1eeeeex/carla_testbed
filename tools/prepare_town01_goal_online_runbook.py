#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_20260522.md"


def _cmd(*parts: str) -> List[str]:
    return [str(part) for part in parts if str(part) != ""]


def _shell(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in cmd)


def _path_status(path: Path) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "exists": resolved.exists(),
    }


def build_runbook_payload(*, python_exec: str = CONDA_CARLA16_PYTHON) -> Dict[str, Any]:
    smoke_config = REPO_ROOT / "configs" / "examples" / "smoke.yaml"
    followstop_config = (
        REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_lateral_enabled_stitcher_v1.yaml"
    )
    town01_baseline_config = (
        REPO_ROOT / "configs" / "io" / "examples" / "town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    )
    town01_direct_config = (
        REPO_ROOT
        / "configs"
        / "io"
        / "examples"
        / "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
    )
    online_chain = REPO_ROOT / "tools" / "run_town01_capability_online_chain.py"
    transport_ab = REPO_ROOT / "tools" / "run_town01_transport_ab.py"
    goal_sequence = REPO_ROOT / "tools" / "run_town01_goal_sequence.py"
    demo_showcase = REPO_ROOT / "tools" / "run_town01_demo_showcase.py"
    post_triage = REPO_ROOT / "tools" / "inspect_town01_post_online_triage.py"
    execution_inspector = REPO_ROOT / "tools" / "inspect_town01_goal_online_runbook_execution.py"

    commands = [
        {
            "key": "offline_smoke_config",
            "phase": "offline",
            "purpose": "Verify the typed config/CLI/artifact layer without CARLA or Apollo.",
            "required_for_goal": True,
            "starts_carla": False,
            "starts_apollo": False,
            "command": _cmd(
                python_exec,
                "-m",
                "carla_testbed",
                "smoke",
                "--config",
                "configs/examples/smoke.yaml",
                "--run-dir",
                "runs/goal_smoke_config",
            ),
            "expected_outputs": [
                "runs/goal_smoke_config/manifest.json",
                "runs/goal_smoke_config/summary.json",
            ],
        },
        {
            "key": "followstop_lateral_enabled_canary",
            "phase": "online_followstop",
            "purpose": "Check that the legacy followstop Apollo GT/lateral-enabled path still dispatches after the structure changes.",
            "required_for_goal": True,
            "starts_carla": True,
            "starts_apollo": True,
            "command": _cmd(
                python_exec,
                "-m",
                "carla_testbed",
                "run",
                "--config",
                "configs/io/examples/followstop_apollo_gt_lateral_enabled_stitcher_v1.yaml",
                "--run-dir",
                "runs/followstop_goal_lateral_enabled_canary",
            ),
            "expected_outputs": [
                "runs/followstop_goal_lateral_enabled_canary/summary.json",
                "runs/followstop_goal_lateral_enabled_canary/artifacts/control_handoff_summary.json",
            ],
        },
        {
            "key": "town01_ros2_gt_canary",
            "phase": "online_town01_ros2_gt",
            "purpose": "Protect the current Town01 ros2_gt mainline on the strongest lane_keep 097 canary.",
            "required_for_goal": True,
            "starts_carla": True,
            "starts_apollo": True,
            "command": _cmd(
                python_exec,
                "tools/run_town01_capability_online_chain.py",
                "--enable-lateral",
                "--enable-guard",
                "--config",
                "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
                "--startup-profile",
                "render_offscreen",
                "--carla-world-ready-timeout-sec",
                "180",
                "--carla-launch-attempts",
                "1",
                "--ticks",
                "420",
                "--post-fail-steps",
                "120",
                "--progress-update-sec",
                "10",
                "--comparison-label-suffix",
                "goal_ros2_gt_canary_420",
                "--continue-on-failure",
                "--step",
                "lane_keep:town01_rh_spawn097_goal046",
            ),
            "expected_outputs": [
                "runs/town01_capability_online_chain_*/**/summary.json",
                "artifacts/town01_goal_status_20260522.md",
            ],
        },
        {
            "key": "town01_direct_goal_sequence",
            "phase": "online_town01_direct",
            "purpose": "Run the direct curve gate and automatically continue to demo recording only if the evidence gate allows it.",
            "required_for_goal": True,
            "starts_carla": True,
            "starts_apollo": True,
            "command": _cmd(
                python_exec,
                "tools/run_town01_goal_sequence.py",
                "--python-exec",
                python_exec,
            ),
            "expected_outputs": [
                "artifacts/town01_goal_sequence_20260522.json",
                "artifacts/town01_post_online_triage_20260522.md",
            ],
        },
        {
            "key": "town01_transport_ab_canonical",
            "phase": "online_transport_ab",
            "purpose": "Fair same-route A/B for ros2_gt vs carla_direct on the canonical Town01 set.",
            "required_for_goal": True,
            "starts_carla": True,
            "starts_apollo": True,
            "command": _cmd(
                python_exec,
                "tools/run_town01_transport_ab.py",
                "--route-set",
                "canonical",
                "--continue-on-failure",
                "--carla-ignore-memory-preflight",
                "--python-exec",
                python_exec,
            ),
            "expected_outputs": [
                "runs/town01_transport_ab_*/transport_ab_summary.md",
                "artifacts/town01_transport_ab_manifest_*.json",
            ],
        },
        {
            "key": "town01_demo_recording",
            "phase": "online_demo",
            "purpose": "Record the CARLA + Apollo Dreamview demo after the transport/curve evidence gate is ready.",
            "required_for_goal": True,
            "starts_carla": True,
            "starts_apollo": True,
            "command": _cmd(
                python_exec,
                "tools/run_town01_demo_showcase.py",
                "--mode",
                "short",
                "--record-dreamview",
                "--dreamview-auto-open",
                "--dreamview-open-wait-page",
                "--dreamview-browser-cmd",
                "auto",
                "--dreamview-capture-mode",
                "tick_snapshot",
                "--dreamview-capture-region",
                "1280x720+0,0",
                "--dreamview-use-fixed-region",
                "--require-recording-ready",
            ),
            "expected_outputs": [
                "runs/town01_capability_online_chain_*/artifacts/town01_demo_showcase_manifest.json",
                "runs/town01_capability_online_chain_*/artifacts/town01_demo_recording_inspection.json",
                "runs/town01_capability_online_chain_*/**/artifacts/dreamview_capture_manifest.json",
            ],
        },
        {
            "key": "post_online_triage",
            "phase": "post_online",
            "purpose": "After any online command, collapse current evidence into the next command and failure detail.",
            "required_for_goal": True,
            "starts_carla": False,
            "starts_apollo": False,
            "command": _cmd(python_exec, "tools/inspect_town01_post_online_triage.py"),
            "expected_outputs": [
                "artifacts/town01_post_online_triage_20260522.json",
                "artifacts/town01_post_online_triage_20260522.md",
            ],
        },
        {
            "key": "inspect_online_runbook_execution",
            "phase": "post_online",
            "purpose": "After running the online runbook executor, classify whether the execution advanced, stalled, or failed.",
            "required_for_goal": False,
            "starts_carla": False,
            "starts_apollo": False,
            "command": _cmd(python_exec, "tools/inspect_town01_goal_online_runbook_execution.py"),
            "expected_outputs": [
                "artifacts/town01_goal_online_runbook_execution_inspection_20260522.json",
                "artifacts/town01_goal_online_runbook_execution_inspection_20260522.md",
            ],
        },
        {
            "key": "final_strict_offline_audit",
            "phase": "final_gate",
            "purpose": "Only use after online evidence and demo recording are ready.",
            "required_for_goal": True,
            "starts_carla": False,
            "starts_apollo": False,
            "command": _cmd(
                python_exec,
                "tools/run_town01_goal_offline_audit.py",
                "--python-exec",
                python_exec,
                "--require-goal-ready",
            ),
            "expected_outputs": [
                "artifacts/town01_goal_offline_audit_20260522.json",
                "artifacts/town01_goal_offline_audit_20260522.md",
            ],
        },
    ]
    checks = {
        "smoke_config": _path_status(smoke_config),
        "followstop_config": _path_status(followstop_config),
        "town01_baseline_config": _path_status(town01_baseline_config),
        "town01_direct_config": _path_status(town01_direct_config),
        "online_chain": _path_status(online_chain),
        "transport_ab": _path_status(transport_ab),
        "goal_sequence": _path_status(goal_sequence),
        "demo_showcase": _path_status(demo_showcase),
        "post_triage": _path_status(post_triage),
        "execution_inspector": _path_status(execution_inspector),
    }
    missing = [name for name, item in checks.items() if not item.get("exists")]
    return {
        "status": "ready" if not missing else "missing_inputs",
        "python_exec": python_exec,
        "missing_inputs": missing,
        "checks": checks,
        "commands": commands,
        "recommended_order": [
            "offline_smoke_config",
            "followstop_lateral_enabled_canary",
            "town01_ros2_gt_canary",
            "town01_direct_goal_sequence",
            "post_online_triage",
            "town01_transport_ab_canonical",
            "town01_demo_recording",
            "final_strict_offline_audit",
        ],
        "notes": [
            "Online commands should be run by the operator in the conda carla16 environment.",
            "Do not treat dry-run artifacts as online capability evidence.",
            "The direct goal sequence only records demo after the curve gate is decisive.",
        ],
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Goal Online Runbook",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- python_exec: `{payload.get('python_exec', '')}`",
        f"- missing_inputs: `{', '.join(payload.get('missing_inputs') or []) or 'none'}`",
        "",
        "## Recommended Order",
        "",
    ]
    command_by_key = {item["key"]: item for item in payload.get("commands", [])}
    for index, key in enumerate(payload.get("recommended_order") or [], start=1):
        item = command_by_key.get(key, {})
        lines.append(
            f"{index}. `{key}` - {item.get('purpose', '')}"
        )
    lines.extend(["", "## Commands", ""])
    ordered_items: List[Dict[str, Any]] = []
    seen = set()
    for key in payload.get("recommended_order") or []:
        if key in command_by_key:
            ordered_items.append(command_by_key[key])
            seen.add(key)
    for item in payload.get("commands", []):
        key = item.get("key")
        if key not in seen:
            ordered_items.append(item)
    for item in ordered_items:
        lines.extend(
            [
                f"### {item.get('key', '')}",
                "",
                f"- phase: `{item.get('phase', '')}`",
                f"- starts_carla: `{item.get('starts_carla', False)}`",
                f"- starts_apollo: `{item.get('starts_apollo', False)}`",
                f"- required_for_goal: `{item.get('required_for_goal', False)}`",
                f"- purpose: {item.get('purpose', '')}",
                "",
                "```bash",
                _shell(item.get("command") or []),
                "```",
                "",
                "Expected outputs:",
            ]
        )
        for output in item.get("expected_outputs") or []:
            lines.append(f"- `{output}`")
        lines.append("")
    lines.extend(["## Notes", ""])
    for note in payload.get("notes") or []:
        lines.append(f"- {note}")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare the online validation runbook for the current Town01 goal.")
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_runbook_payload(python_exec=str(args.python_exec))
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"status": payload["status"], "missing_inputs": payload["missing_inputs"]}, indent=2))


if __name__ == "__main__":
    main()
