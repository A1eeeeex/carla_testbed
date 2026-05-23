#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON
from tools.inspect_town01_goal_status import find_latest_demo_root
from tools.inspect_town01_goal_status import inspect_demo_status
from tools.inspect_town01_goal_status import inspect_transport_ab as classify_transport_ab_root
from tools.prepare_town01_goal_online_runbook import build_runbook_payload

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_results_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_results_20260522.md"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _glob(pattern: str) -> List[Path]:
    return sorted(REPO_ROOT.glob(pattern))


def _summary_status(path: Path, *, success_field: bool = True) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    payload = _load_json(resolved)
    if not payload:
        return {"status": "missing", "path": str(resolved), "reason": "summary is missing or unreadable"}
    if success_field and payload.get("success") is not True:
        return {
            "status": "present_not_success",
            "path": str(resolved),
            "reason": f"success={payload.get('success')}",
            "summary": payload,
        }
    return {"status": "passed", "path": str(resolved), "reason": "", "summary": payload}


def _int_value(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _latest_summary_with_label(label: str, *, route_id: str = "") -> Dict[str, Any]:
    summaries = sorted((REPO_ROOT / "runs").glob("town01_capability_online_chain_*/**/summary.json"))
    matches: List[Path] = []
    for path in summaries:
        text_path = str(path)
        if label not in text_path:
            continue
        if route_id:
            payload = _load_json(path)
            observed_route = str(payload.get("route_id") or (payload.get("scenario_metadata") or {}).get("route_id") or "")
            if observed_route != route_id:
                continue
        matches.append(path)
    if not matches:
        return {
            "status": "missing",
            "path": "",
            "reason": f"no summary found with label {label!r}" + (f" and route_id {route_id}" if route_id else ""),
        }
    latest = max(matches, key=lambda item: item.stat().st_mtime)
    payload = _load_json(latest)
    runtime = payload.get("runtime_contract") if isinstance(payload.get("runtime_contract"), dict) else {}
    control = str(payload.get("control_handoff_status") or "")
    if runtime.get("status") == "aligned" and control == "control_consuming_with_nonzero_planning":
        status = "passed"
        reason = ""
    else:
        status = "present_not_passing"
        reason = f"runtime={runtime.get('status')} control={control}"
    return {"status": status, "path": str(latest.resolve()), "reason": reason, "summary": payload}


def _inspect_offline_smoke() -> Dict[str, Any]:
    official = _summary_status(REPO_ROOT / "runs" / "goal_smoke_config" / "summary.json")
    if official["status"] == "passed":
        return official
    alternate = _summary_status(REPO_ROOT / "runs" / "goal_smoke_config_check" / "summary.json")
    if alternate["status"] == "passed":
        alternate["status"] = "alternate_passed"
        alternate["reason"] = "official run dir missing, but goal_smoke_config_check passed"
        alternate["official_path"] = str((REPO_ROOT / "runs" / "goal_smoke_config" / "summary.json").resolve())
        return alternate
    return official


def _read_text_tail(path: Path, *, limit: int = 2000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    return text[-limit:]


def _resolve_redirected_run_dir(run_dir: Path) -> Path:
    redirect = run_dir / "RUN_DIR_REDIRECT.txt"
    if not redirect.exists():
        return run_dir
    try:
        raw = redirect.read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        return run_dir
    if not raw:
        return run_dir
    target = Path(raw).expanduser()
    if not target.is_absolute():
        target = REPO_ROOT / target
    return target if target.exists() else run_dir


def _parse_doctor_carla_server(text: str) -> Dict[str, Any]:
    section: Dict[str, str] = {}
    in_carla_server = False
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            in_carla_server = line == "[carla_server]"
            continue
        if not in_carla_server or ":" not in line:
            continue
        key, value = line.split(":", 1)
        section[key.strip()] = value.strip()
    if not section:
        return {}
    return {
        "host": section.get("host", ""),
        "port": section.get("port", ""),
        "reachable": section.get("reachable", ""),
        "error": section.get("error", ""),
    }


def _followstop_attempt_context(*, limit: int = 5) -> List[Dict[str, Any]]:
    runs_root = REPO_ROOT / "runs"
    candidates = sorted(
        [path for path in runs_root.glob("followstop_goal_lateral_enabled_canary*") if path.is_dir()],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    contexts: List[Dict[str, Any]] = []
    for run_dir in candidates[: max(int(limit), 1)]:
        artifacts = run_dir / "artifacts"
        run_meta = _load_json(artifacts / "run_meta.json")
        launch_policy = run_meta.get("carla_launch_policy") if isinstance(run_meta.get("carla_launch_policy"), dict) else {}
        doctor_path = artifacts / "doctor.txt"
        doctor_tail = _read_text_tail(doctor_path, limit=1200)
        contexts.append(
            {
                "name": run_dir.name,
                "path": str(run_dir.resolve()),
                "mtime": run_dir.stat().st_mtime,
                "has_summary": (run_dir / "summary.json").exists(),
                "has_effective_config": (run_dir / "effective.yaml").exists(),
                "has_run_meta": (artifacts / "run_meta.json").exists(),
                "has_doctor_output": doctor_path.exists(),
                "doctor_carla_server": _parse_doctor_carla_server(doctor_tail),
                "carla_launch_policy": {
                    field: launch_policy.get(field)
                    for field in ("start", "host", "port", "town", "extra_args", "need_ros2_native")
                    if field in launch_policy
                },
            }
        )
    return contexts


def _inspect_followstop_attempt_without_summary(run_dir: Path) -> Dict[str, Any]:
    artifacts = run_dir / "artifacts"
    stdout_log = artifacts / "followstop_child.stdout.log"
    stderr_log = artifacts / "followstop_child.stderr.log"
    effective = run_dir / "effective.yaml"
    run_meta = artifacts / "run_meta.json"
    doctor = artifacts / "doctor.txt"
    return {
        "status": "online_attempt_no_summary",
        "path": str(run_dir.resolve()),
        "reason": "online run directory exists, but summary.json is missing",
        "summary_path": str((run_dir / "summary.json").resolve()),
        "has_effective_config": effective.exists(),
        "has_run_meta": run_meta.exists(),
        "has_doctor_output": doctor.exists(),
        "stdout_log": str(stdout_log.resolve()) if stdout_log.exists() else "",
        "stderr_log": str(stderr_log.resolve()) if stderr_log.exists() else "",
        "stdout_tail": _read_text_tail(stdout_log),
        "stderr_tail": _read_text_tail(stderr_log),
        "recent_attempts": _followstop_attempt_context(),
    }


def _inspect_followstop() -> Dict[str, Any]:
    target_run_dir = REPO_ROOT / "runs" / "followstop_goal_lateral_enabled_canary"
    run_dir = _resolve_redirected_run_dir(target_run_dir)
    summary = run_dir / "summary.json"
    result = _summary_status(summary, success_field=False)
    if result["status"] == "missing":
        if run_dir.exists():
            missing = _inspect_followstop_attempt_without_summary(run_dir)
            if run_dir != target_run_dir:
                missing["target_run_dir"] = str(target_run_dir.resolve())
                missing["redirected_run_dir"] = str(run_dir.resolve())
            return missing
        dry_run_effective = REPO_ROOT / "runs" / "followstop_goal_lateral_enabled_canary_check" / "effective.yaml"
        if dry_run_effective.exists():
            return {
                "status": "dry_run_only",
                "path": str(dry_run_effective.resolve()),
                "reason": "only dry-run compatibility output exists; target online run directory/summary is missing",
                "target_run_dir": str(run_dir.resolve()),
                "target_summary_path": str(summary.resolve()),
                "recent_attempts": _followstop_attempt_context(),
            }
        return result

    payload = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    artifacts = run_dir / "artifacts"
    cyber_stats = _load_json(artifacts / "cyber_bridge_stats.json")
    handoff = _load_json(artifacts / "control_handoff_summary.json")
    ros2_gt = payload.get("ros2_gt") if isinstance(payload.get("ros2_gt"), dict) else {}
    ros2_counts = ros2_gt.get("counts") if isinstance(ros2_gt.get("counts"), dict) else {}
    handoff_status = str(handoff.get("control_handoff_status") or payload.get("control_handoff_status") or "")
    checks = {
        "summary_success": payload.get("success") is True,
        "adapter_started": bool(payload.get("adapter_started")),
        "max_speed_mps": _float_value(payload.get("max_speed_mps")),
        "ros2_odom_count": _int_value(ros2_counts.get("odom")),
        "cyber_loc_count": _int_value(cyber_stats.get("loc_count")),
        "cyber_chassis_count": _int_value(cyber_stats.get("chassis_count")),
        "cyber_control_tx_count": _int_value(cyber_stats.get("control_tx_count")),
        "cyber_routing_success_count": _int_value(cyber_stats.get("routing_success_count")),
        "handoff_status": handoff_status,
    }
    if checks["summary_success"]:
        status = "passed"
        reason = ""
    elif (
        checks["adapter_started"]
        and checks["max_speed_mps"] >= 0.5
        and (checks["ros2_odom_count"] > 0 or checks["cyber_loc_count"] > 0)
        and (
            checks["cyber_control_tx_count"] > 0
            or handoff_status == "control_consuming_with_nonzero_planning"
        )
    ):
        status = "canary_evidence_present"
        reason = "summary success is false, but followstop online stack materialized enough for the structural canary"
    else:
        status = "present_not_passing"
        reason = (
            f"success={payload.get('success')} adapter_started={checks['adapter_started']} "
            f"max_speed_mps={checks['max_speed_mps']} ros2_odom={checks['ros2_odom_count']} "
            f"cyber_control_tx={checks['cyber_control_tx_count']} handoff={handoff_status}"
        )
    result.update(
        {
            "status": status,
            "reason": reason,
            "checks": checks,
            "target_run_dir": str(target_run_dir.resolve()),
            "redirected_run_dir": str(run_dir.resolve()) if run_dir != target_run_dir else "",
            "cyber_bridge_stats_path": str((artifacts / "cyber_bridge_stats.json").resolve()),
            "control_handoff_summary_path": str((artifacts / "control_handoff_summary.json").resolve()),
        }
    )
    return result


def _inspect_town01_ros2_canary() -> Dict[str, Any]:
    return _latest_summary_with_label("goal_ros2_gt_canary_420", route_id="town01_rh_spawn097_goal046")


def _inspect_goal_sequence() -> Dict[str, Any]:
    path = REPO_ROOT / "artifacts" / "town01_goal_sequence_20260522.json"
    payload = _load_json(path)
    if not payload:
        dry_run = REPO_ROOT / "artifacts" / "town01_goal_sequence_dry_run_20260522.json"
        if dry_run.exists():
            return {
                "status": "dry_run_only",
                "path": str(dry_run.resolve()),
                "reason": "online goal sequence artifact is missing; dry-run artifact exists",
            }
        return {"status": "missing", "path": str(path.resolve()), "reason": "goal sequence artifact is missing"}
    raw_status = str(payload.get("status") or "")
    if raw_status in {"completed_goal_ready", "completed_goal_in_progress"}:
        status = "passed" if raw_status == "completed_goal_ready" else "present_goal_in_progress"
    elif raw_status == "dry_run":
        status = "dry_run_only"
    else:
        status = raw_status or "present_unknown"
    return {"status": status, "path": str(path.resolve()), "reason": str(payload.get("reason") or ""), "payload": payload}


def _inspect_post_online_triage() -> Dict[str, Any]:
    path = REPO_ROOT / "artifacts" / "town01_post_online_triage_20260522.json"
    payload = _load_json(path)
    if not payload:
        return {"status": "missing", "path": str(path.resolve()), "reason": "triage artifact is missing"}
    triage = payload.get("triage") if isinstance(payload.get("triage"), dict) else {}
    triage_status = str(triage.get("status") or "present_unknown")
    recognized_statuses = {
        "awaiting_direct_curve_gate",
        "ready_for_demo_recording",
        "fix_demo_readiness_first",
        "ready_for_final_strict_audit",
        "continue_goal_audit",
    }
    status = "passed" if triage_status in recognized_statuses else triage_status
    return {
        "status": status,
        "triage_status": triage_status,
        "path": str(path.resolve()),
        "reason": str(triage.get("reason") or ""),
        "next_command": str(triage.get("next_command") or ""),
    }


def _inspect_transport_ab() -> Dict[str, Any]:
    root, source_manifest, latest_dry_run_manifest = _latest_transport_ab_root()
    if root is not None:
        classified = classify_transport_ab_root(root)
        status = str(classified.get("status") or "present_unknown")
        report = root / "transport_ab_summary.md"
        path = report if report.exists() else root
        reason_by_status = {
            "candidate_positive": "direct candidate is positive on the inspected transport A/B routes",
            "candidate_has_negative_routes": "direct candidate has negative transport A/B routes; this is decisive but not a promotion",
            "partial_pending_rerun": "transport A/B is partial; at least one candidate route is missing or runtime-interrupted",
            "candidate_valid_mixed": "transport A/B is present but mixed; more interpretation or rerun is needed",
            "missing": "transport A/B root is present but does not contain usable baseline/candidate summaries",
        }
        result = {
            "status": status,
            "path": str(path.resolve()),
            "reason": reason_by_status.get(status, ""),
            "root": str(root.resolve()),
            "summary": classified.get("summary", {}),
            "verdict_counts": (classified.get("summary") or {}).get("verdict_counts", {}),
        }
        if source_manifest is not None:
            result["manifest_path"] = str(source_manifest.resolve())
        if latest_dry_run_manifest is not None:
            result["latest_dry_run_manifest_path"] = str(latest_dry_run_manifest.resolve())
        return result

    reports = _glob("runs/town01_transport_ab_*/transport_ab_summary.md")
    if reports:
        latest = max(reports, key=lambda item: item.stat().st_mtime)
        return {
            "status": "present_unknown",
            "path": str(latest.resolve()),
            "reason": "transport A/B report exists but baseline/candidate root could not be inferred",
        }
    manifests = _glob("artifacts/town01_transport_ab_manifest_*.json")
    latest_manifest = latest_dry_run_manifest or (max(manifests, key=lambda item: item.stat().st_mtime) if manifests else None)
    if latest_manifest and _transport_manifest_is_dry_run(_load_json(latest_manifest)):
        return {
            "status": "dry_run_only",
            "path": str(latest_manifest.resolve()),
            "reason": "only dry-run transport A/B manifest is available; no usable online A/B root yet",
        }
    return {"status": "missing", "path": "", "reason": "no transport A/B report found"}


def _inspect_demo_recording() -> Dict[str, Any]:
    root = find_latest_demo_root(REPO_ROOT / "runs")
    if root is None:
        readiness = REPO_ROOT / "artifacts" / "town01_demo_readiness_20260522.json"
        if readiness.exists():
            payload = _load_json(readiness)
            return {
                "status": "readiness_only",
                "path": str(readiness.resolve()),
                "reason": f"demo readiness exists with status={payload.get('status')}; no demo recording batch found yet",
                "readiness_status": str(payload.get("status") or ""),
            }
        return {
            "status": "missing",
            "path": "",
            "reason": "no Town01 demo recording batch found",
        }
    inspected = inspect_demo_status(
        root,
        require_demo=True,
        require_dreamview=True,
        min_carla_frames=10,
        min_dreamview_frames=10,
    )
    raw_status = str(inspected.get("status") or "present_unknown")
    status = "passed" if raw_status == "ready" else raw_status
    return {
        "status": status,
        "path": str(root.resolve()),
        "reason": str(inspected.get("reason") or ""),
        "inspection": inspected,
    }


def _transport_manifest_is_dry_run(payload: Dict[str, Any]) -> bool:
    baseline_cmd = " ".join(str(item) for item in payload.get("baseline_command") or [])
    candidate_cmd = " ".join(str(item) for item in payload.get("candidate_command") or [])
    return "--dry-run" in baseline_cmd or "--dry-run" in candidate_cmd


def _resolve_transport_root_from_manifest(payload: Dict[str, Any]) -> Path | None:
    raw = str(payload.get("ab_batch_root") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else (REPO_ROOT / path)
    report = str(payload.get("report_md") or payload.get("report_csv") or "").strip()
    if report:
        path = Path(report).expanduser()
        if not path.is_absolute():
            path = REPO_ROOT / path
        return path.parent
    return None


def _transport_root_is_usable(root: Path) -> bool:
    return (root / "baseline").exists() or (root / "candidate").exists()


def _latest_transport_ab_root() -> tuple[Path | None, Path | None, Path | None]:
    latest_dry_run_manifest: Path | None = None
    manifests = sorted(_glob("artifacts/town01_transport_ab_manifest_*.json"), key=lambda item: item.stat().st_mtime)
    for manifest in reversed(manifests):
        payload = _load_json(manifest)
        if _transport_manifest_is_dry_run(payload):
            if latest_dry_run_manifest is None:
                latest_dry_run_manifest = manifest
            continue
        root = _resolve_transport_root_from_manifest(payload)
        if root is not None and _transport_root_is_usable(root):
            return root, manifest, latest_dry_run_manifest

    roots = sorted(_glob("runs/town01_transport_ab_*"), key=lambda item: item.stat().st_mtime)
    for root in reversed(roots):
        if _transport_root_is_usable(root):
            return root, None, latest_dry_run_manifest
    return None, None, latest_dry_run_manifest


def _inspect_final_audit() -> Dict[str, Any]:
    path = REPO_ROOT / "artifacts" / "town01_goal_offline_audit_20260522.json"
    payload = _load_json(path)
    if not payload:
        return {"status": "missing", "path": str(path.resolve()), "reason": "offline audit is missing"}
    return {
        "status": str(payload.get("audit_status") or "present_unknown"),
        "path": str(path.resolve()),
        "reason": f"goal_status={((payload.get('goal_status') or {}).get('overall_status') or '')}",
    }


def inspect_results() -> Dict[str, Dict[str, Any]]:
    return {
        "offline_smoke_config": _inspect_offline_smoke(),
        "followstop_lateral_enabled_canary": _inspect_followstop(),
        "town01_ros2_gt_canary": _inspect_town01_ros2_canary(),
        "town01_direct_goal_sequence": _inspect_goal_sequence(),
        "post_online_triage": _inspect_post_online_triage(),
        "town01_transport_ab_canonical": _inspect_transport_ab(),
        "town01_demo_recording": _inspect_demo_recording(),
        "final_strict_offline_audit": _inspect_final_audit(),
    }


PASSING_STATUSES = {"passed", "alternate_passed", "canary_evidence_present", "candidate_positive"}


def build_results_payload(*, python_exec: str = CONDA_CARLA16_PYTHON) -> Dict[str, Any]:
    runbook = build_runbook_payload(python_exec=python_exec)
    results = inspect_results()
    order = list(runbook.get("recommended_order") or [])
    missing_or_incomplete = [
        key for key in order if str((results.get(key) or {}).get("status") or "") not in PASSING_STATUSES
    ]
    if not missing_or_incomplete and str((results.get("final_strict_offline_audit") or {}).get("status")) == "offline_pass_goal_ready":
        status = "ready_for_goal_completion_review"
    elif missing_or_incomplete:
        status = "in_progress"
    else:
        status = "needs_final_audit"
    next_key = missing_or_incomplete[0] if missing_or_incomplete else "final_strict_offline_audit"
    command_by_key = {item.get("key"): item for item in runbook.get("commands") or []}
    next_command = " ".join(
        shlex_quote(str(item)) for item in (command_by_key.get(next_key, {}).get("command") or [])
    )
    executor_command = " ".join(
        shlex_quote(part)
        for part in (
            str(python_exec),
            "tools/run_town01_goal_online_runbook.py",
            "--python-exec",
            str(python_exec),
        )
    )
    return {
        "status": status,
        "next_key": next_key,
        "next_command": next_command,
        "executor_command": executor_command,
        "runbook_status": runbook.get("status", ""),
        "results": results,
        "recommended_order": order,
    }


def shlex_quote(text: str) -> str:
    import shlex

    return shlex.quote(text)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Goal Online Runbook Results",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- next_key: `{payload.get('next_key', '')}`",
        f"- runbook_status: `{payload.get('runbook_status', '')}`",
        "",
        "## Result Matrix",
        "",
        "| key | status | reason | path |",
        "|---|---|---|---|",
    ]
    results = payload.get("results") if isinstance(payload.get("results"), dict) else {}
    for key in payload.get("recommended_order") or []:
        item = results.get(key) if isinstance(results.get(key), dict) else {}
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` |".format(
                key,
                item.get("status", ""),
                str(item.get("reason", "")).replace("|", "\\|"),
                item.get("path", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Recommended Executor Command",
            "",
            "Run this wrapper so execution, results, next-action, handoff, and offline-audit artifacts all refresh together.",
            "",
            "```bash",
            str(payload.get("executor_command", "")),
            "```",
            "",
            "## Current Gate Underlying Command",
            "",
            "```bash",
            str(payload.get("next_command", "")),
            "```",
        ]
    )
    next_key = str(payload.get("next_key") or "")
    next_item = results.get(next_key) if isinstance(results.get(next_key), dict) else {}
    attempts = next_item.get("recent_attempts") if isinstance(next_item.get("recent_attempts"), list) else []
    if attempts:
        lines.extend(
            [
                "",
                "## Next Gate Recent Attempts",
                "",
                "| name | summary | effective | doctor reachable | doctor error | launch extra_args |",
                "|---|---|---|---|---|---|",
            ]
        )
        for attempt in attempts:
            doctor = attempt.get("doctor_carla_server") if isinstance(attempt.get("doctor_carla_server"), dict) else {}
            launch = attempt.get("carla_launch_policy") if isinstance(attempt.get("carla_launch_policy"), dict) else {}
            lines.append(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                    attempt.get("name", ""),
                    bool(attempt.get("has_summary")),
                    bool(attempt.get("has_effective_config")),
                    str(doctor.get("reachable", "")).replace("|", "\\|"),
                    str(doctor.get("error", "")).replace("|", "\\|"),
                    str(launch.get("extra_args", "")).replace("|", "\\|"),
                )
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect which critical online runbook gates have current evidence.")
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--print-next-command", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_results_payload(python_exec=str(args.python_exec))
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    if args.print_next_command:
        print(str(payload.get("next_command") or ""))
    else:
        print(json.dumps({"status": payload["status"], "next_key": payload["next_key"]}, indent=2))


if __name__ == "__main__":
    main()
