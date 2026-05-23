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

from tools.analyze_town01_direct_curve_recovery import (
    DEFAULT_LABEL_HINT,
    DEFAULT_ROUTE_IDS,
    build_recovery_payload,
    find_latest_candidate_roots,
)
from tools.analyze_town01_transport_ab import build_rows, summarize_rows
from tools.inspect_town01_demo_recording import build_inspection

DEFAULT_TRANSPORT_AB_ROOT = REPO_ROOT / "runs" / "town01_transport_ab_20260522_160253"
DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_status_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_status_20260522.md"
CONDA_CARLA16_PYTHON = "/home/ubuntu/miniconda3/envs/carla16/bin/python3"

DECISIVE_CURVE_VERDICTS = {
    "recovery_positive",
    "recovery_negative",
    "recovery_valid_mixed",
}

CURVE_ROUTE_COMMAND_KEYS = {
    "town01_rh_spawn176_goal061": "direct_curve176_recovery_online",
    "town01_rh_spawn177_goal052": "direct_curve177_recovery_online",
}

RECOVERED_ROW_VERDICT_PREFIXES = (
    "candidate_positive",
    "candidate_negative",
)


def _route_output_token(route_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(route_id).strip())


def retry_artifact_path(route_id: str, *, dry_run: bool = False, artifacts_root: Path = REPO_ROOT / "artifacts") -> Path:
    token = _route_output_token(route_id) or "unknown_route"
    dry_run_suffix = "_dry_run" if dry_run else ""
    return artifacts_root / f"town01_direct_curve_recovery_retry_{token}{dry_run_suffix}_20260522.json"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _collect_summary_paths(root: Path) -> List[Path]:
    resolved = root.expanduser().resolve()
    if not resolved.exists():
        return []
    if resolved.name == "summary.json":
        return [resolved]
    return sorted(resolved.rglob("summary.json"))


def inspect_retry_artifact(path: Path, *, route_id: str, dry_run: bool) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    payload = _load_json(resolved)
    if not payload:
        return {
            "route_id": route_id,
            "status": "missing",
            "dry_run": bool(dry_run),
            "path": str(resolved),
            "final_verdict": "",
            "attempt_count": 0,
            "last_attempt": {},
        }
    attempts = [item for item in (payload.get("attempts") or []) if isinstance(item, dict)]
    last_attempt = attempts[-1] if attempts else {}
    raw_status = str(payload.get("status") or "unknown")
    if raw_status == "dry_run":
        status = "dry_run_only" if dry_run else "legacy_dry_run_ignored"
    elif raw_status == "completed":
        status = "completed"
    elif raw_status == "retryable_verdict_exhausted":
        status = "retry_exhausted"
    elif raw_status == "preflight_failed":
        status = "preflight_failed"
    else:
        status = raw_status
    return {
        "route_id": route_id,
        "status": status,
        "raw_status": raw_status,
        "dry_run": bool(dry_run),
        "path": str(resolved),
        "final_verdict": "" if (raw_status == "dry_run" and not dry_run) else str(payload.get("final_verdict") or ""),
        "attempt_count": 0 if (raw_status == "dry_run" and not dry_run) else len(attempts),
        "last_attempt": {
            "attempt": "" if (raw_status == "dry_run" and not dry_run) else last_attempt.get("attempt", ""),
            "returncode": None if (raw_status == "dry_run" and not dry_run) else last_attempt.get("returncode"),
            "verdict": "" if (raw_status == "dry_run" and not dry_run) else str(last_attempt.get("verdict") or ""),
            "candidate_invalid_reason": (
                "legacy dry-run artifact in online slot ignored"
                if (raw_status == "dry_run" and not dry_run)
                else str(last_attempt.get("candidate_invalid_reason") or "")
            ),
            "candidate_summary": "" if (raw_status == "dry_run" and not dry_run) else str(last_attempt.get("candidate_summary") or ""),
            "candidate_summary_new_for_attempt": None if (raw_status == "dry_run" and not dry_run) else last_attempt.get("candidate_summary_new_for_attempt"),
            "stale_previous_verdict": "" if (raw_status == "dry_run" and not dry_run) else str(last_attempt.get("stale_previous_verdict") or ""),
            "retrying": False if (raw_status == "dry_run" and not dry_run) else bool(last_attempt.get("retrying", False)),
        },
    }


def inspect_retry_artifacts(
    *,
    route_ids: Sequence[str] = DEFAULT_ROUTE_IDS,
    artifacts_root: Path = REPO_ROOT / "artifacts",
) -> Dict[str, Any]:
    routes: List[Dict[str, Any]] = []
    for route_id in route_ids:
        online = inspect_retry_artifact(retry_artifact_path(route_id, dry_run=False, artifacts_root=artifacts_root), route_id=route_id, dry_run=False)
        dry_run = inspect_retry_artifact(retry_artifact_path(route_id, dry_run=True, artifacts_root=artifacts_root), route_id=route_id, dry_run=True)
        routes.append(
            {
                "route_id": route_id,
                "online": online,
                "dry_run": dry_run,
            }
        )
    online_statuses = [str(item["online"].get("status") or "missing") for item in routes]
    if any(status == "completed" for status in online_statuses):
        status = "has_completed_attempt"
    elif any(status in {"retry_exhausted", "preflight_failed"} for status in online_statuses):
        status = "has_failed_attempt"
    elif any(item["dry_run"].get("status") != "missing" for item in routes):
        status = "dry_run_only"
    else:
        status = "missing"
    return {
        "status": status,
        "routes": routes,
    }


def _has_startup_probe(root: Path) -> bool:
    resolved = root.expanduser().resolve()
    if not resolved.exists():
        return False
    return any(resolved.rglob("carla_startup_probe.json"))


def inspect_transport_ab(root: Path, *, route_ids: Sequence[str] | None = None) -> Dict[str, Any]:
    resolved = root.expanduser().resolve()
    baseline_root = resolved / "baseline"
    candidate_root = resolved / "candidate"
    if not baseline_root.exists() and not candidate_root.exists():
        return {
            "status": "missing",
            "root": str(resolved),
            "summary": {},
            "rows": [],
            "reason": "transport A/B root does not contain baseline/candidate directories",
        }
    rows = build_rows([baseline_root], [candidate_root], route_ids=route_ids)
    summary = summarize_rows(rows)
    verdict_counts = dict(summary.get("verdict_counts") or {})
    missing_count = int(verdict_counts.get("candidate_missing", 0) or 0)
    interrupted_count = int(verdict_counts.get("candidate_inconclusive_runtime_interrupted", 0) or 0)
    negative_count = int(summary.get("candidate_negative_count") or 0)
    positive_count = int(summary.get("candidate_positive_count") or 0)
    route_count = int(summary.get("route_count") or 0)
    if route_count == 0:
        status = "missing"
    elif negative_count:
        status = "candidate_has_negative_routes"
    elif missing_count or interrupted_count:
        status = "partial_pending_rerun"
    elif positive_count == route_count:
        status = "candidate_positive"
    else:
        status = "candidate_valid_mixed"
    return {
        "status": status,
        "root": str(resolved),
        "baseline_root": str(baseline_root),
        "candidate_root": str(candidate_root),
        "summary": summary,
        "rows": rows,
    }


def inspect_curve_recovery_payload(payload: Dict[str, Any], *, source: str, path: Path | None = None) -> Dict[str, Any]:
    if not payload:
        return {
            "status": "missing",
            "source": source,
            "path": str(path.expanduser().resolve()) if path is not None else "",
            "recovery_verdict": "",
            "reason": "curve recovery assessment is missing or unreadable",
        }
    verdict = str(payload.get("recovery_verdict") or "")
    if verdict in DECISIVE_CURVE_VERDICTS:
        status = "decisive"
    elif verdict == "recovery_incomplete_candidate_missing":
        status = "pending_missing_candidate_curve"
    elif verdict == "recovery_inconclusive":
        status = "pending_inconclusive_curve"
    else:
        status = "pending"
    return {
        "status": status,
        "source": source,
        "path": str(path.expanduser().resolve()) if path is not None else "",
        "recovery_verdict": verdict,
        "baseline_root": payload.get("baseline_root", ""),
        "candidate_root": payload.get("candidate_root", ""),
        "summary": payload.get("summary") or {},
        "rows": payload.get("rows") or [],
    }


def inspect_curve_recovery(path: Path) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    payload = _load_json(resolved)
    return inspect_curve_recovery_payload(payload, source="existing_json", path=resolved)


def inspect_auto_curve_recovery(
    *,
    baseline_root: Path,
    runs_root: Path,
    route_ids: Sequence[str],
    label_hint: str,
) -> Dict[str, Any]:
    try:
        candidate_roots = find_latest_candidate_roots(runs_root, route_ids=route_ids, label_hint=label_hint)
    except FileNotFoundError as exc:
        return {
            "status": "missing",
            "source": "auto_latest_candidates",
            "path": "",
            "recovery_verdict": "",
            "reason": str(exc),
        }
    payload = build_recovery_payload(
        baseline_root=baseline_root.expanduser().resolve(),
        candidate_root=candidate_roots,
        route_ids=route_ids,
    )
    return inspect_curve_recovery_payload(payload, source="auto_latest_candidates")


def inspect_startup_only_runs(runs_root: Path) -> Dict[str, Any]:
    resolved = runs_root.expanduser().resolve()
    runs: List[Dict[str, Any]] = []
    for run_root in sorted(resolved.glob("town01_capability_online_chain_*")):
        summary_paths = _collect_summary_paths(run_root)
        if summary_paths:
            continue
        if not _has_startup_probe(run_root):
            continue
        runs.append(
            {
                "run_root": str(run_root.resolve()),
                "status": "startup_only_no_route_summaries",
            }
        )
    return {
        "status": "found" if runs else "none",
        "runs_root": str(resolved),
        "count": len(runs),
        "runs": runs,
    }


def find_latest_demo_root(runs_root: Path) -> Path | None:
    resolved = runs_root.expanduser().resolve()
    if not resolved.exists():
        return None
    manifests = sorted(
        resolved.glob("town01_capability_online_chain_*/artifacts/town01_demo_showcase_manifest.json"),
        key=lambda path: path.stat().st_mtime,
    )
    if not manifests:
        return None
    return manifests[-1].parents[1].resolve()


def inspect_demo_status(
    demo_root: Path | None,
    *,
    require_demo: bool,
    require_dreamview: bool,
    min_carla_frames: int,
    min_dreamview_frames: int,
) -> Dict[str, Any]:
    if demo_root is None:
        return {
            "status": "not_checked",
            "required": bool(require_demo),
            "source": "none",
            "reason": "no demo batch root was provided",
        }
    resolved = demo_root.expanduser().resolve()
    if not resolved.exists():
        return {
            "status": "missing",
            "required": bool(require_demo),
            "source": "explicit",
            "batch_root": str(resolved),
            "reason": "demo batch root does not exist",
        }
    inspection = build_inspection(
        resolved,
        require_carla=True,
        require_dreamview=require_dreamview,
        min_carla_frames=min_carla_frames,
        min_dreamview_frames=min_dreamview_frames,
    )
    return {
        "status": str(inspection.get("status") or "unknown"),
        "required": bool(require_demo),
        "source": "explicit",
        "inspection": inspection,
    }


def classify_transport_decision(transport_ab: Dict[str, Any], curve_recovery: Dict[str, Any]) -> Dict[str, Any]:
    transport_status = str(transport_ab.get("status") or "")
    curve_status = str(curve_recovery.get("status") or "")
    recovery_verdict = str(curve_recovery.get("recovery_verdict") or "")
    transport_summary = transport_ab.get("summary") if isinstance(transport_ab.get("summary"), dict) else {}
    curve_summary = curve_recovery.get("summary") if isinstance(curve_recovery.get("summary"), dict) else {}
    transport_negative = int(transport_summary.get("candidate_negative_count") or 0)
    transport_positive = int(transport_summary.get("candidate_positive_count") or 0)
    transport_routes = int(transport_summary.get("route_count") or 0)
    curve_negative = int(curve_summary.get("candidate_negative_count") or 0)
    curve_positive = int(curve_summary.get("candidate_positive_count") or 0)
    curve_routes = int(curve_summary.get("route_count") or 0)

    if transport_status == "missing":
        return {
            "status": "insufficient_evidence",
            "transport_promotable": False,
            "direct_candidate_action": "collect_transport_ab",
            "reason": "transport A/B evidence is missing",
        }
    if transport_negative or transport_status == "candidate_has_negative_routes":
        return {
            "status": "candidate_negative",
            "transport_promotable": False,
            "direct_candidate_action": "do_not_promote",
            "reason": "one or more non-curve A/B routes are negative for carla_direct",
        }
    if curve_status != "decisive":
        return {
            "status": "pending_curve_recovery",
            "transport_promotable": False,
            "direct_candidate_action": "rerun_curve_pair",
            "reason": "curve recovery is not decisive yet",
        }
    if recovery_verdict == "recovery_negative" or curve_negative:
        return {
            "status": "candidate_negative",
            "transport_promotable": False,
            "direct_candidate_action": "keep_as_experimental_or_lane_junction_only",
            "reason": "direct curve recovery produced a negative verdict",
        }
    if recovery_verdict == "recovery_positive" and curve_routes > 0 and curve_positive == curve_routes:
        return {
            "status": "candidate_positive",
            "transport_promotable": True,
            "direct_candidate_action": "promote_to_transport_candidate_after_demo_gate",
            "reason": "non-curve A/B has no negative routes and curve recovery is positive",
        }
    if recovery_verdict == "recovery_valid_mixed":
        return {
            "status": "candidate_mixed",
            "transport_promotable": False,
            "direct_candidate_action": "continue_targeted_analysis",
            "reason": "curve recovery is valid but not uniformly positive",
        }
    if transport_status == "candidate_positive" and transport_positive == transport_routes:
        return {
            "status": "candidate_positive",
            "transport_promotable": True,
            "direct_candidate_action": "promote_to_transport_candidate_after_demo_gate",
            "reason": "all transport A/B routes are positive",
        }
    return {
        "status": "candidate_inconclusive",
        "transport_promotable": False,
        "direct_candidate_action": "continue_ab_analysis",
        "reason": "transport evidence is valid but not sufficient for a positive or negative decision",
    }


def _transport_pending_route_ids(transport_ab: Dict[str, Any]) -> List[str]:
    pending_verdicts = {
        "candidate_missing",
        "candidate_inconclusive_runtime_interrupted",
    }
    route_ids: List[str] = []
    for row in transport_ab.get("rows") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("verdict") or "") not in pending_verdicts:
            continue
        route_id = str(row.get("route_id") or "").strip()
        if route_id:
            route_ids.append(route_id)
    return route_ids


def _transport_partial_resolved_by_curve_recovery(
    transport_ab: Dict[str, Any],
    curve_recovery: Dict[str, Any],
    *,
    curve_route_ids: Sequence[str],
) -> bool:
    if transport_ab.get("status") != "partial_pending_rerun":
        return False
    if curve_recovery.get("status") != "decisive":
        return False
    pending = set(_transport_pending_route_ids(transport_ab))
    if not pending:
        return False
    curve_ids = {str(route_id).strip() for route_id in curve_route_ids if str(route_id).strip()}
    return pending <= curve_ids


def _curve_route_needs_rerun(row: Dict[str, Any]) -> bool:
    verdict = str(row.get("verdict") or "")
    if verdict.startswith(RECOVERED_ROW_VERDICT_PREFIXES):
        return False
    return True


def _select_curve_rerun_command_key(curve_recovery: Dict[str, Any]) -> tuple[str, str]:
    rows = [row for row in (curve_recovery.get("rows") or []) if isinstance(row, dict)]
    rows_by_route = {str(row.get("route_id") or ""): row for row in rows}
    for route_id in DEFAULT_ROUTE_IDS:
        row = rows_by_route.get(route_id)
        if row is None:
            return CURVE_ROUTE_COMMAND_KEYS.get(route_id, "direct_curve_recovery_online"), (
                f"{route_id} has no curve recovery row yet"
            )
        if _curve_route_needs_rerun(row):
            verdict = str(row.get("verdict") or "unknown")
            invalid = str(row.get("candidate_invalid_reason") or "").strip()
            reason = f"{route_id} is still {verdict}"
            if invalid:
                reason = f"{reason} ({invalid})"
            return CURVE_ROUTE_COMMAND_KEYS.get(route_id, "direct_curve_recovery_online"), reason
    return "direct_curve_recovery_online", "curve recovery is non-decisive but no single route gap was isolated"


def select_next_command(payload: Dict[str, Any]) -> Dict[str, str]:
    commands = payload.get("recommended_commands") or {}
    curve = payload.get("curve_recovery") if isinstance(payload.get("curve_recovery"), dict) else {}
    demo = payload.get("demo_recording") if isinstance(payload.get("demo_recording"), dict) else {}
    transport = payload.get("transport_ab") if isinstance(payload.get("transport_ab"), dict) else {}

    if payload.get("overall_status") == "ready_for_final_review":
        key = "final_strict_offline_audit"
        reason = "all current blockers are cleared; run the strict offline audit before final sign-off"
    elif curve.get("status") != "decisive":
        key, reason = _select_curve_rerun_command_key(curve)
    elif demo.get("required") and demo.get("status") != "ready":
        key = "demo_recording_online"
        reason = "transport and curve evidence are decisive enough; demo recording is still missing or not ready"
    elif transport.get("status") == "partial_pending_rerun":
        key = "post_curve_recovery_goal_check"
        reason = "old A/B root is partial; refresh the goal status with the latest split curve evidence"
    else:
        key = "final_strict_offline_audit"
        reason = "no higher-priority online action remains in the current evidence state"

    return {
        "key": key,
        "command": str(commands.get(key, "")),
        "reason": reason,
    }


def build_goal_status(
    *,
    transport_ab_root: Path = DEFAULT_TRANSPORT_AB_ROOT,
    curve_recovery_json: Path | None = None,
    runs_root: Path = REPO_ROOT / "runs",
    demo_root: Path | None = None,
    auto_demo_root: bool = True,
    auto_curve_recovery: bool = True,
    curve_label_hint: str = DEFAULT_LABEL_HINT,
    curve_route_ids: Sequence[str] = DEFAULT_ROUTE_IDS,
    require_demo: bool = True,
    require_dreamview: bool = True,
    min_carla_frames: int = 10,
    min_dreamview_frames: int = 10,
) -> Dict[str, Any]:
    transport_ab = inspect_transport_ab(transport_ab_root)
    curve_recovery_path = curve_recovery_json or (transport_ab_root / "direct_curve_recovery_assessment.json")
    curve_recovery = inspect_curve_recovery(curve_recovery_path)
    if auto_curve_recovery:
        auto_curve_recovery_result = inspect_auto_curve_recovery(
            baseline_root=transport_ab_root / "baseline",
            runs_root=runs_root,
            route_ids=curve_route_ids,
            label_hint=curve_label_hint,
        )
        if auto_curve_recovery_result.get("status") != "missing":
            curve_recovery = auto_curve_recovery_result
    startup_only = inspect_startup_only_runs(runs_root)
    resolved_demo_root = demo_root
    demo_source = "explicit" if demo_root is not None else "none"
    if resolved_demo_root is None and auto_demo_root:
        resolved_demo_root = find_latest_demo_root(runs_root)
        if resolved_demo_root is not None:
            demo_source = "auto_latest_demo"
    demo = inspect_demo_status(
        resolved_demo_root,
        require_demo=require_demo,
        require_dreamview=require_dreamview,
        min_carla_frames=min_carla_frames,
        min_dreamview_frames=min_dreamview_frames,
    )
    demo["source"] = demo_source if resolved_demo_root is not None else demo.get("source", "none")
    retry_artifacts = inspect_retry_artifacts(route_ids=curve_route_ids)

    blockers: List[str] = []
    transport_partial_resolved = _transport_partial_resolved_by_curve_recovery(
        transport_ab,
        curve_recovery,
        curve_route_ids=curve_route_ids,
    )
    if transport_ab["status"] in {"missing", "candidate_has_negative_routes"}:
        blockers.append(f"transport_ab:{transport_ab['status']}")
    elif transport_ab["status"] == "partial_pending_rerun" and not transport_partial_resolved:
        blockers.append(f"transport_ab:{transport_ab['status']}")
    if curve_recovery["status"] != "decisive":
        blockers.append(f"curve_recovery:{curve_recovery['status']}")
    if require_demo and demo["status"] != "ready":
        blockers.append(f"demo_recording:{demo['status']}")

    overall_status = "ready_for_final_review" if not blockers else "in_progress"
    next_actions = _next_actions(transport_ab, curve_recovery, demo, require_demo=require_demo)
    recommended_commands = build_recommended_commands()
    transport_decision = classify_transport_decision(transport_ab, curve_recovery)
    payload = {
        "overall_status": overall_status,
        "blockers": blockers,
        "transport_decision": transport_decision,
        "transport_ab": transport_ab,
        "curve_recovery": curve_recovery,
        "direct_curve_retry": retry_artifacts,
        "startup_only_runs": startup_only,
        "demo_recording": demo,
        "next_actions": next_actions,
        "recommended_commands": recommended_commands,
    }
    payload["next_command"] = select_next_command(payload)
    return payload


def _next_actions(
    transport_ab: Dict[str, Any],
    curve_recovery: Dict[str, Any],
    demo: Dict[str, Any],
    *,
    require_demo: bool,
) -> List[str]:
    actions: List[str] = []
    if curve_recovery.get("status") in {"missing", "pending_missing_candidate_curve", "pending_inconclusive_curve", "pending"}:
        actions.append(
            "rerun direct curve routes, preferably as split one-route batches: curve176 first, then curve177"
        )
    if transport_ab.get("status") == "partial_pending_rerun":
        actions.append("refresh transport A/B after curve-pair recovery to remove missing/interrupted samples")
    if require_demo and demo.get("status") != "ready":
        actions.append("record a Town01 showcase batch with CARLA dual-cam and Dreamview capture enabled")
    if not actions:
        actions.append("review direct-vs-ros2 verdict and decide whether direct remains a candidate or gets promoted")
    return actions


def build_recommended_commands() -> Dict[str, str]:
    direct_curve_recovery = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_capability_online_chain.py \\",
            "  --enable-lateral \\",
            "  --enable-guard \\",
            "  --config configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml \\",
            "  --startup-profile render_offscreen_no_ros2 \\",
            "  --carla-world-ready-timeout-sec 180 \\",
            "  --carla-launch-attempts 1 \\",
            "  --ticks 420 \\",
            "  --post-fail-steps 120 \\",
            "  --progress-update-sec 10 \\",
            f"  --comparison-label-suffix {DEFAULT_LABEL_HINT} \\",
            "  --continue-on-failure \\",
            "  --carla-ignore-memory-preflight \\",
            "  --step curve_lane_follow:town01_rh_spawn176_goal061 \\",
            "  --step curve_lane_follow:town01_rh_spawn177_goal052",
        ]
    )
    direct_curve176_recovery = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_direct_curve_recovery_retry.py \\",
            "  --route-id town01_rh_spawn176_goal061 \\",
            "  --retries 1 \\",
            f"  --python-exec {CONDA_CARLA16_PYTHON}",
        ]
    )
    direct_curve177_recovery = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_direct_curve_recovery_retry.py \\",
            "  --route-id town01_rh_spawn177_goal052 \\",
            "  --retries 1 \\",
            f"  --python-exec {CONDA_CARLA16_PYTHON}",
        ]
    )
    direct_curve_pair_recovery = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_direct_curve_pair_recovery_retry.py \\",
            f"  --python-exec {CONDA_CARLA16_PYTHON}",
        ]
    )
    inspect_goal = f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_goal_status.py"
    online_preflight = f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_online_preflight.py"
    goal_resume = f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_goal_resume.py"
    strict_goal_audit = (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_offline_audit.py "
        f"--python-exec {CONDA_CARLA16_PYTHON} --require-goal-ready"
    )
    analyze_curve_recovery = f"{CONDA_CARLA16_PYTHON} tools/analyze_town01_direct_curve_recovery.py"
    current_curve176_divergence = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/analyze_town01_direct_divergence.py \\",
            "  --left-run runs/town01_transport_ab_20260522_160253/baseline/curve_lane_follow__adhoc__town01_rh_spawn176_goal061__seed/curve_lane_follow__adhoc__town01_rh_spawn176_goal061__seed__transport_ab_baseline__town01_rh_spawn176_goal061__02 \\",
            "  --left-label ros2_gt_curve176_baseline \\",
            "  --right-run runs/town01_transport_ab_20260522_160253/candidate/curve_lane_follow__adhoc__town01_rh_spawn176_goal061__seed/curve_lane_follow__adhoc__town01_rh_spawn176_goal061__seed__transport_ab_direct_candidate__town01_rh_spawn176_goal061__02 \\",
            "  --right-label carla_direct_curve176_interrupted \\",
            "  --json-out artifacts/town01_direct_curve176_divergence_20260522.json \\",
            "  --md-out artifacts/town01_direct_curve176_divergence_20260522.md",
        ]
    )
    demo_dry_run = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_demo_showcase.py \\",
            "  --mode short \\",
            "  --record-dreamview \\",
            "  --dreamview-auto-open \\",
            "  --dreamview-open-wait-page \\",
            "  --dreamview-browser-cmd auto \\",
            "  --dreamview-capture-mode tick_snapshot \\",
            "  --dreamview-capture-region 1280x720+0,0 \\",
            "  --dreamview-use-fixed-region \\",
            "  --dry-run",
        ]
    )
    demo_readiness = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_demo_readiness.py \\",
            "  --browser-cmd auto \\",
            "  --capture-mode tick_snapshot \\",
            "  --capture-region 1280x720+0,0 \\",
            "  --use-fixed-region",
        ]
    )
    demo_record = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_demo_showcase.py \\",
            "  --mode short \\",
            "  --record-dreamview \\",
            "  --dreamview-auto-open \\",
            "  --dreamview-open-wait-page \\",
            "  --dreamview-browser-cmd auto \\",
            "  --dreamview-capture-mode tick_snapshot \\",
            "  --dreamview-capture-region 1280x720+0,0 \\",
            "  --dreamview-use-fixed-region \\",
            "  --require-recording-ready",
        ]
    )
    goal_sequence = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_sequence.py \\",
            f"  --python-exec {CONDA_CARLA16_PYTHON}",
        ]
    )
    inspect_demo = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_demo_recording.py \\",
            "  <NEW_TOWN01_DEMO_BATCH_ROOT> \\",
            "  --require-dreamview",
        ]
    )
    inspect_goal_with_demo = "\n".join(
        [
            f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_goal_status.py \\",
            "  --demo-root <NEW_TOWN01_DEMO_BATCH_ROOT>",
        ]
    )
    return {
        "direct_curve_recovery_online": direct_curve_recovery,
        "direct_curve_pair_recovery_online": direct_curve_pair_recovery,
        "direct_curve176_recovery_online": direct_curve176_recovery,
        "direct_curve177_recovery_online": direct_curve177_recovery,
        "goal_resume": goal_resume,
        "online_preflight": online_preflight,
        "post_curve_recovery_goal_check": inspect_goal,
        "final_strict_offline_audit": strict_goal_audit,
        "post_curve_recovery_detail": analyze_curve_recovery,
        "current_curve176_divergence_report": current_curve176_divergence,
        "demo_recording_readiness": demo_readiness,
        "demo_recording_dry_run": demo_dry_run,
        "demo_recording_online": demo_record,
        "goal_sequence_online": goal_sequence,
        "demo_recording_inspect": inspect_demo,
        "goal_check_with_demo": inspect_goal_with_demo,
    }


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    transport = payload["transport_ab"]
    curve = payload["curve_recovery"]
    retry = payload.get("direct_curve_retry") if isinstance(payload.get("direct_curve_retry"), dict) else {}
    demo = payload["demo_recording"]
    lines = [
        "# Town01 Goal Status",
        "",
        f"- overall_status: `{payload['overall_status']}`",
        f"- blockers: `{', '.join(payload['blockers']) if payload['blockers'] else 'none'}`",
        "",
        "## Transport A/B",
        "",
        f"- status: `{transport['status']}`",
        f"- root: `{transport.get('root', '')}`",
        f"- route_count: `{(transport.get('summary') or {}).get('route_count', 0)}`",
        f"- baseline_aligned_count: `{(transport.get('summary') or {}).get('baseline_aligned_count', 0)}`",
        f"- candidate_aligned_count: `{(transport.get('summary') or {}).get('candidate_aligned_count', 0)}`",
        f"- candidate_control_consuming_count: `{(transport.get('summary') or {}).get('candidate_control_consuming_count', 0)}`",
        f"- verdict_counts: `{json.dumps((transport.get('summary') or {}).get('verdict_counts', {}), sort_keys=True)}`",
        "",
        "## Direct-vs-ROS2 Decision",
        "",
        f"- status: `{payload.get('transport_decision', {}).get('status', '')}`",
        f"- transport_promotable: `{payload.get('transport_decision', {}).get('transport_promotable', False)}`",
        f"- action: `{payload.get('transport_decision', {}).get('direct_candidate_action', '')}`",
        f"- reason: `{payload.get('transport_decision', {}).get('reason', '')}`",
        "",
        "## Direct Curve Recovery",
        "",
        f"- status: `{curve['status']}`",
        f"- source: `{curve.get('source', '')}`",
        f"- recovery_verdict: `{curve.get('recovery_verdict', '')}`",
        f"- path: `{curve.get('path', '')}`",
        f"- baseline_root: `{curve.get('baseline_root', '')}`",
        f"- candidate_root: `{curve.get('candidate_root', '')}`",
        f"- verdict_counts: `{json.dumps((curve.get('summary') or {}).get('verdict_counts', {}), sort_keys=True)}`",
        "",
        "## Direct Curve Retry Attempts",
        "",
        f"- status: `{retry.get('status', '')}`",
        "",
        "| route_id | online_status | final_verdict | attempts | last_verdict | summary_new | invalid_reason | dry_run_status |",
        "|---|---|---|---:|---|---|---|---|",
    ]
    for item in retry.get("routes") or []:
        online = item.get("online") if isinstance(item.get("online"), dict) else {}
        dry_run = item.get("dry_run") if isinstance(item.get("dry_run"), dict) else {}
        last = online.get("last_attempt") if isinstance(online.get("last_attempt"), dict) else {}
        lines.append(
            "| `{}` | `{}` | `{}` | {} | `{}` | `{}` | `{}` | `{}` |".format(
                item.get("route_id", ""),
                online.get("status", ""),
                online.get("final_verdict", ""),
                online.get("attempt_count", 0),
                last.get("verdict", ""),
                last.get("candidate_summary_new_for_attempt", ""),
                last.get("candidate_invalid_reason", ""),
                dry_run.get("status", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Demo Recording",
            "",
            f"- status: `{demo['status']}`",
            f"- source: `{demo.get('source', '')}`",
            f"- required: `{demo.get('required', False)}`",
        ]
    )
    inspection = demo.get("inspection") or {}
    if inspection:
        lines.extend(
            [
                f"- batch_root: `{inspection.get('batch_root', '')}`",
                f"- route_count: `{inspection.get('route_count', 0)}`",
                f"- carla_ok_count: `{inspection.get('carla_ok_count', 0)}`",
                f"- dreamview_ok_count: `{inspection.get('dreamview_ok_count', 0)}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Startup-Only Runs",
            "",
            f"- status: `{payload['startup_only_runs']['status']}`",
            f"- count: `{payload['startup_only_runs']['count']}`",
            "",
            "## Next Actions",
            "",
        ]
    )
    for action in payload.get("next_actions", []):
        lines.append(f"- {action}")
    next_command = payload.get("next_command") if isinstance(payload.get("next_command"), dict) else {}
    lines.extend(
        [
            "",
            "## Next Command",
            "",
            f"- key: `{next_command.get('key', '')}`",
            f"- reason: `{next_command.get('reason', '')}`",
            "",
            "```bash",
            str(next_command.get("command", "")),
            "```",
        ]
    )
    lines.extend(["", "## Recommended Commands", ""])
    command_titles = {
        "direct_curve_recovery_online": "Direct curve-pair online recovery",
        "direct_curve_pair_recovery_online": "Direct curve-pair retry wrapper",
        "direct_curve176_recovery_online": "Direct curve176 split online recovery",
        "direct_curve177_recovery_online": "Direct curve177 split online recovery",
        "goal_resume": "Goal resume summary",
        "online_preflight": "Online preflight without launching CARLA/Apollo",
        "post_curve_recovery_goal_check": "Post-curve goal check",
        "final_strict_offline_audit": "Final strict offline audit",
        "post_curve_recovery_detail": "Curve recovery detail report",
        "current_curve176_divergence_report": "Current curve176 divergence report",
        "demo_recording_readiness": "Demo recording readiness check",
        "demo_recording_dry_run": "Demo recording dry-run",
        "demo_recording_online": "Demo recording online run",
        "goal_sequence_online": "Goal sequence: curve gate then demo when ready",
        "demo_recording_inspect": "Inspect demo recording",
        "goal_check_with_demo": "Goal check with demo root",
    }
    for key, command in (payload.get("recommended_commands") or {}).items():
        lines.extend([f"### {command_titles.get(key, key)}", "", "```bash", str(command), "```", ""])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def exit_code_for_goal_status(payload: Dict[str, Any], *, require_ready: bool) -> int:
    if require_ready and payload.get("overall_status") != "ready_for_final_review":
        return 2
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect current Town01 goal evidence without launching CARLA/Apollo.")
    parser.add_argument("--transport-ab-root", type=Path, default=DEFAULT_TRANSPORT_AB_ROOT)
    parser.add_argument("--curve-recovery-json", type=Path, default=None)
    parser.add_argument("--auto-curve-recovery", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--curve-label-hint", default=DEFAULT_LABEL_HINT)
    parser.add_argument("--curve-route-id", action="append", default=None)
    parser.add_argument("--runs-root", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--demo-root", type=Path, default=None)
    parser.add_argument("--auto-demo-root", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--require-demo", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--require-dreamview", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--min-carla-frames", type=int, default=10)
    parser.add_argument("--min-dreamview-frames", type=int, default=10)
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Exit with code 2 unless the full goal status is ready_for_final_review.",
    )
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument(
        "--print-next-command",
        action="store_true",
        help="Print only the selected next command after writing status artifacts.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_goal_status(
        transport_ab_root=args.transport_ab_root,
        curve_recovery_json=args.curve_recovery_json,
        runs_root=args.runs_root,
        demo_root=args.demo_root,
        auto_demo_root=bool(args.auto_demo_root),
        auto_curve_recovery=bool(args.auto_curve_recovery),
        curve_label_hint=str(args.curve_label_hint or ""),
        curve_route_ids=[
            str(route_id).strip()
            for route_id in list(args.curve_route_id if args.curve_route_id is not None else DEFAULT_ROUTE_IDS)
            if str(route_id).strip()
        ],
        require_demo=bool(args.require_demo),
        require_dreamview=bool(args.require_dreamview),
        min_carla_frames=int(args.min_carla_frames),
        min_dreamview_frames=int(args.min_dreamview_frames),
    )
    _write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    if args.print_next_command:
        print(str((payload.get("next_command") or {}).get("command") or ""))
    else:
        print(
            json.dumps(
                {
                    "overall_status": payload["overall_status"],
                    "blockers": payload["blockers"],
                    "next_command": payload.get("next_command", {}),
                },
                indent=2,
            )
        )
    raise SystemExit(exit_code_for_goal_status(payload, require_ready=bool(args.require_ready)))


if __name__ == "__main__":
    main()
