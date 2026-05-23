#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_after_online import build_after_online_payload
from tools.inspect_town01_goal_online_runbook_results import build_results_payload
from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON, build_goal_status
from tools.inspect_town01_online_preflight import DEFAULT_OUTPUT_JSON as DEFAULT_PREFLIGHT_JSON
from tools.run_town01_goal_offline_audit import DEFAULT_OUTPUT_JSON as DEFAULT_OFFLINE_AUDIT_JSON

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_completion_audit_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_completion_audit_20260522.md"

PayloadBuilder = Callable[[], Dict[str, Any]]


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _dict(payload: Any) -> Dict[str, Any]:
    return payload if isinstance(payload, dict) else {}


def _list(payload: Any) -> List[Any]:
    return payload if isinstance(payload, list) else []


def _result_counts(runbook: Dict[str, Any]) -> Dict[str, int]:
    results = _dict(runbook.get("results"))
    counts: Dict[str, int] = {}
    for item in results.values():
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _failed_check_names(preflight: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for item in _list(preflight.get("checks")):
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "") == "failed":
            name = str(item.get("name") or "").strip()
            if name:
                names.append(name)
    return names


def _passing_tool_tests(audit: Dict[str, Any]) -> bool:
    for result in _list(audit.get("test_results")):
        if not isinstance(result, dict):
            continue
        if str(result.get("status") or "") != "passed":
            return False
    return True


def _requirement(
    *,
    key: str,
    title: str,
    status: str,
    evidence: List[str],
    missing: List[str],
    next_action: str,
) -> Dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "status": status,
        "evidence": evidence,
        "missing": missing,
        "next_action": next_action,
    }


def _structural_validation_requirement(audit: Dict[str, Any]) -> Dict[str, Any]:
    audit_status = str(audit.get("audit_status") or "")
    tests_ok = _passing_tool_tests(audit)
    if audit_status == "offline_pass_goal_in_progress" and tests_ok:
        status = "proven"
        missing: List[str] = []
    else:
        status = "incomplete"
        missing = ["offline audit must pass with all non-runtime tests"]
    return _requirement(
        key="structural_validation",
        title="Large structural changes remain importable/testable",
        status=status,
        evidence=[f"offline_audit={audit_status}", f"tool_tests_ok={tests_ok}"],
        missing=missing,
        next_action=(
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_offline_audit.py "
            f"--python-exec {CONDA_CARLA16_PYTHON}"
        ),
    )


def _critical_scene_online_requirement(runbook: Dict[str, Any], after_online: Dict[str, Any], preflight: Dict[str, Any]) -> Dict[str, Any]:
    runbook_status = str(runbook.get("status") or "")
    next_key = str(runbook.get("next_key") or "")
    counts = _result_counts(runbook)
    after_status = str(after_online.get("status") or "")
    preflight_status = str(preflight.get("status") or "")
    failed_checks = _failed_check_names(preflight)
    if preflight_status == "failed":
        status = "incomplete"
        missing = [f"online preflight failed: {', '.join(failed_checks) or 'unknown check'}"]
    elif runbook_status == "ready_for_goal_completion_review":
        status = "proven"
        missing: List[str] = []
    elif counts.get("passed", 0) or counts.get("ready", 0):
        status = "partial"
        missing = [f"next runbook gate still pending: {next_key or 'unknown'}"]
    else:
        status = "incomplete"
        missing = ["no real online critical-scene gate has been proven yet"]
    return _requirement(
        key="critical_scene_online_validation",
        title="Key scenes still run after the large structural changes",
        status=status,
        evidence=[
            f"preflight_status={preflight_status}",
            f"runbook_status={runbook_status}",
            f"runbook_next_key={next_key}",
            f"after_online={after_status}",
            f"result_counts={counts}",
        ],
        missing=missing,
        next_action=str(after_online.get("next_command") or ""),
    )


def _transport_requirement(goal: Dict[str, Any], after_online: Dict[str, Any]) -> Dict[str, Any]:
    transport_gate = _dict(after_online.get("transport_gate"))
    decision = _dict(goal.get("transport_decision"))
    pending = [str(item) for item in _list(transport_gate.get("pending_rerun_routes"))]
    positive = int(transport_gate.get("candidate_positive_count") or 0)
    negative = int(transport_gate.get("candidate_negative_count") or 0)
    promotable = bool(transport_gate.get("transport_promotable") or decision.get("transport_promotable"))
    if promotable and not pending and negative == 0:
        status = "proven"
        missing: List[str] = []
    elif positive > 0 and negative == 0:
        status = "partial"
        missing = [f"pending rerun routes: {', '.join(pending) or 'unknown'}"]
    else:
        status = "incomplete"
        missing = ["transport A/B has not produced a promotable or positive candidate state"]
    return _requirement(
        key="transport_ab_decision",
        title="Compare carla-ros2-apollo against carla-apollo direct transport",
        status=status,
        evidence=[
            f"decision={decision.get('status', '')}",
            f"action={transport_gate.get('direct_candidate_action', '')}",
            f"positive={positive}",
            f"negative={negative}",
            f"promotable={promotable}",
        ],
        missing=missing,
        next_action=str((goal.get("next_command") or {}).get("command") or after_online.get("next_command") or ""),
    )


def _demo_requirement(goal: Dict[str, Any], after_online: Dict[str, Any]) -> Dict[str, Any]:
    demo = _dict(goal.get("demo_recording"))
    readiness = _dict(after_online.get("demo_readiness"))
    demo_status = str(demo.get("status") or "")
    readiness_status = str(readiness.get("status") or "")
    if demo_status == "ready":
        status = "proven"
        missing: List[str] = []
    elif readiness_status in {"ready", "ready_with_warnings"}:
        status = "partial"
        missing = ["demo recording has not been produced/inspected yet"]
    else:
        status = "incomplete"
        missing = ["demo recording readiness is not ready"]
    return _requirement(
        key="demo_recording",
        title="Record CARLA and Apollo Dreamview demo after the chain is smooth",
        status=status,
        evidence=[f"demo_status={demo_status}", f"demo_readiness={readiness_status}"],
        missing=missing,
        next_action=str((goal.get("recommended_commands") or {}).get("demo_recording_online") or ""),
    )


def build_completion_payload(
    *,
    python_exec: str = CONDA_CARLA16_PYTHON,
    offline_audit_path: Path = DEFAULT_OFFLINE_AUDIT_JSON,
    preflight_path: Path = DEFAULT_PREFLIGHT_JSON,
    offline_audit_builder: PayloadBuilder | None = None,
    preflight_builder: PayloadBuilder | None = None,
    runbook_builder: PayloadBuilder | None = None,
    goal_builder: PayloadBuilder = build_goal_status,
    after_online_builder: PayloadBuilder | None = None,
) -> Dict[str, Any]:
    audit = offline_audit_builder() if offline_audit_builder else _load_json(offline_audit_path)
    preflight = preflight_builder() if preflight_builder else _load_json(preflight_path)
    runbook = runbook_builder() if runbook_builder else build_results_payload(python_exec=python_exec)
    goal = goal_builder()
    after_online = after_online_builder() if after_online_builder else build_after_online_payload(
        python_exec=python_exec,
        allow_dry_run_fallback=True,
    )
    requirements = [
        _structural_validation_requirement(audit),
        _critical_scene_online_requirement(runbook, after_online, preflight),
        _transport_requirement(goal, after_online),
        _demo_requirement(goal, after_online),
    ]
    counts: Dict[str, int] = {}
    for item in requirements:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    if counts.get("incomplete", 0):
        status = "incomplete"
    elif counts.get("partial", 0):
        status = "partial"
    else:
        status = "complete"
    return {
        "status": status,
        "requirement_counts": counts,
        "requirements": requirements,
        "next_command": str(after_online.get("next_command") or (goal.get("next_command") or {}).get("command") or ""),
        "goal_status": str(goal.get("overall_status") or ""),
        "goal_blockers": goal.get("blockers") or [],
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Goal Completion Audit",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- goal_status: `{payload.get('goal_status', '')}`",
        f"- goal_blockers: `{', '.join(payload.get('goal_blockers') or []) or 'none'}`",
        f"- requirement_counts: `{json.dumps(payload.get('requirement_counts') or {}, sort_keys=True)}`",
        "",
        "## Next Command",
        "",
        "```bash",
        str(payload.get("next_command") or ""),
        "```",
        "",
        "## Requirements",
        "",
        "| key | status | evidence | missing |",
        "|---|---|---|---|",
    ]
    for item in payload.get("requirements") or []:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` |".format(
                item.get("key", ""),
                item.get("status", ""),
                "; ".join(str(value) for value in _list(item.get("evidence"))).replace("|", "\\|"),
                "; ".join(str(value) for value in _list(item.get("missing"))).replace("|", "\\|"),
            )
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit whether the active Town01 goal is actually complete.")
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--offline-audit-json", type=Path, default=DEFAULT_OFFLINE_AUDIT_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--require-complete", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_completion_payload(
        python_exec=str(args.python_exec),
        offline_audit_path=args.offline_audit_json,
    )
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"status": payload["status"], "next_command": payload.get("next_command", "")}, indent=2))
    if args.require_complete and payload["status"] != "complete":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
