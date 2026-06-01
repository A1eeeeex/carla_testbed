#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.transport_ab import check_ab_report_requirements


STAGE_OUTPUTS = {
    "01_lane097_canary": ("analysis/ab_report.json",),
    "02_hard_gates": ("analysis/ab_report.json",),
    "03_curve_diagnostics": ("analysis/ab_report.json",),
    "04_random_regression": ("analysis/ab_report.json",),
    "06_audit": ("town01_goal_audit.json",),
    "08_postprocess": ("town01_postprocess.json",),
    "09_natural_driving_suite": ("suite_manifest.json",),
    "10_natural_driving_postprocess": (
        "analysis/natural_driving/natural_driving_report.json",
        "analysis/natural_driving/natural_driving_postprocess.json",
        "analysis/goal_audit/town01_goal_audit.json",
    ),
    "11_final_goal_audit": ("town01_goal_audit.json",),
}
AB_STAGE_REQUIREMENTS = {
    "01_lane097_canary": {"require_hard_gate_pass": False, "required_positive_routes": ["lane097"]},
    "02_hard_gates": {"require_hard_gate_pass": True},
    "03_curve_diagnostics": {"require_hard_gate_pass": False, "require_route_curve_p1_complete": True},
    "04_random_regression": {"require_hard_gate_pass": False},
}
STRICT_DIRECT_REQUIREMENTS = {
    "required_steering_mode": "legacy_double_percent",
    "required_direct_control_apply_mode": "frame_flush_only",
    "required_direct_stale_world_frame_policy": "always_republish",
    "require_direct_transport_contract_aligned": True,
    "required_direct_bridge_cadence_ratio_min": 0.8,
}
DISK_WARN_FREE_BYTES = 80 * 1024**3
DISK_CRITICAL_FREE_BYTES = 40 * 1024**3
DISK_WARN_USED_RATIO = 0.90
REQUIRED_REPO_FILES = (
    "tools/run_town01_direct_ab.py",
    "tools/analyze_ab_report.py",
    "tools/audit_town01_goal.py",
    "tools/postprocess_town01_goal.py",
    "tools/build_calibration_gate_results.py",
    "tools/run_town01_demo_showcase.py",
    "tools/run_town01_natural_driving_suite.py",
    "tools/postprocess_town01_natural_driving.py",
    "configs/routes/town01/canonical_five.yaml",
    "configs/routes/town01/random_regression_pool_20260416.yaml",
    "configs/scenarios/town01_natural_driving_suite.yaml",
    "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
    "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml",
)
TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS = {
    "traffic_light_red_stop": "red_stop",
    "traffic_light_green_go": "green_go",
    "traffic_light_red_to_green_release": "red_to_green_release",
}
BAD_RUN_STATUSES = {"failed", "timeout", "skipped", "cancelled", "canceled"}
BAD_FAILURE_REASONS = {
    "no_control",
    "planning_missing",
    "control_missing",
    "bridge_drop",
    "timeout",
    "artifact_missing",
    "unknown",
}
LOG_TAIL_LINES = 12
LOG_TAIL_CHARS = 4000


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _resolve(path_text: str | None, *, base_dir: Path) -> Path | None:
    if not path_text:
        return None
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _exists(path: Path | None) -> bool:
    return bool(path is not None and path.exists())


def _disk_status(path: Path) -> dict[str, Any]:
    probe = path if path.exists() else path.parent
    usage = shutil.disk_usage(probe)
    used_ratio = 1.0 - (usage.free / usage.total if usage.total else 0.0)
    if usage.free < DISK_CRITICAL_FREE_BYTES:
        status = "critical"
        reason = "free space below 40GiB"
    elif usage.free < DISK_WARN_FREE_BYTES or used_ratio >= DISK_WARN_USED_RATIO:
        status = "warn"
        reason = "free space below 80GiB or filesystem usage at/above 90%"
    else:
        status = "ok"
        reason = "sufficient free space for short validation runs"
    return {
        "path": str(probe),
        "status": status,
        "reason": reason,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_gib": round(usage.free / 1024**3, 2),
        "used_percent": round(used_ratio * 100.0, 1),
    }


def _preflight_status(repo_root: Path, python_exec: str | None) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    if python_exec:
        python_path = Path(python_exec).expanduser()
        checks.append(
            {
                "name": "python",
                "path": str(python_path),
                "exists": python_path.exists(),
                "required": True,
            }
        )
    else:
        checks.append(
            {
                "name": "python",
                "path": "",
                "exists": False,
                "required": True,
            }
        )
    for relative in REQUIRED_REPO_FILES:
        path = repo_root / relative
        checks.append(
            {
                "name": relative,
                "path": str(path),
                "exists": path.exists(),
                "required": True,
            }
        )
    missing = [item for item in checks if item.get("required") and not item.get("exists")]
    return {
        "status": "ok" if not missing else "missing_required",
        "missing_count": len(missing),
        "missing": missing,
        "checks": checks,
    }


def _read_exit_code(path: Path | None) -> int | None:
    if path is None or not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _demo_inspection_from_marker(marker: Path | None) -> Path | None:
    if marker is None or not marker.exists():
        return None
    text = marker.read_text(encoding="utf-8").strip()
    return Path(text).expanduser() if text else None


def _stage_dependencies(item: Mapping[str, Any]) -> list[dict[str, Any]]:
    dependencies: list[dict[str, Any]] = []
    for raw in item.get("dependencies") or []:
        if not isinstance(raw, Mapping):
            continue
        stage = raw.get("stage")
        if not stage:
            continue
        dependencies.append(
            {
                "stage": str(stage),
                "require_summary_pass": bool(raw.get("require_summary_pass")),
                "allowed_statuses": [str(item) for item in raw.get("allowed_statuses") or ["completed"]],
                "require_summary_present": bool(
                    raw.get("require_summary_present", bool(raw.get("require_summary_pass")))
                ),
            }
        )
    return dependencies


def _tail_text(path_text: Any, *, max_lines: int = LOG_TAIL_LINES, max_chars: int = LOG_TAIL_CHARS) -> dict[str, Any]:
    if not path_text:
        return {"exists": False, "tail": None, "error": None}
    path = Path(str(path_text)).expanduser()
    if not path.exists():
        return {"exists": False, "tail": None, "error": None}
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return {"exists": True, "tail": None, "error": f"{type(exc).__name__}: {exc}"}
    lines = text.splitlines()
    tail = "\n".join(lines[-max_lines:])
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return {"exists": True, "tail": tail, "error": None}


def _run_log_entry(row: Mapping[str, Any]) -> dict[str, Any]:
    stdout = _tail_text(row.get("command_stdout_path"))
    stderr = _tail_text(row.get("command_stderr_path"))
    return {
        "run_id": row.get("run_id"),
        "route_id": row.get("route_id"),
        "backend": row.get("backend"),
        "run_status": row.get("run_status"),
        "return_code": row.get("return_code"),
        "failure_reason": row.get("failure_reason"),
        "direct_transport_contract_status": row.get("direct_transport_contract_status"),
        "command_stdout_path": row.get("command_stdout_path"),
        "command_stderr_path": row.get("command_stderr_path"),
        "command_stdout_exists": stdout["exists"],
        "command_stderr_exists": stderr["exists"],
        "command_stdout_tail": stdout["tail"],
        "command_stderr_tail": stderr["tail"],
        "command_stdout_tail_error": stdout["error"],
        "command_stderr_tail_error": stderr["error"],
        "artifact_dir": row.get("artifact_dir"),
        "actual_run_dir": row.get("actual_run_dir"),
    }


def _interesting_run_logs(report: Mapping[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    rows = [item for item in report.get("run_results") or [] if isinstance(item, Mapping)]
    selected: list[Mapping[str, Any]] = []
    for row in rows:
        status = str(row.get("run_status") or "").lower()
        return_code = row.get("return_code")
        failure_reason = str(row.get("failure_reason") or "")
        direct_status = str(row.get("direct_transport_contract_status") or "")
        if return_code not in (None, "", 0, "0"):
            selected.append(row)
        elif status in BAD_RUN_STATUSES:
            selected.append(row)
        elif failure_reason in BAD_FAILURE_REASONS:
            selected.append(row)
        elif direct_status == "mismatch":
            selected.append(row)
    if not selected and rows:
        selected = rows[:limit]
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in selected:
        run_id = str(row.get("run_id") or "")
        if run_id in seen:
            continue
        seen.add(run_id)
        deduped.append(_run_log_entry(row))
        if len(deduped) >= limit:
            break
    return deduped


def _ab_report_summary(stage_name: str, report_path: Path | None) -> dict[str, Any] | None:
    if report_path is None or not report_path.exists():
        return None
    try:
        report = _load_json(report_path)
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "summary_type": "ab_report",
            "status": "unreadable",
            "error": f"{type(exc).__name__}: {exc}",
            "report_path": str(report_path),
        }
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    hard_gate = verdict.get("hard_gate_summary") if isinstance(verdict.get("hard_gate_summary"), Mapping) else {}
    comparisons = [item for item in report.get("comparisons") or [] if isinstance(item, Mapping)]
    direct_rows = [
        item for item in report.get("run_results") or []
        if isinstance(item, Mapping) and item.get("backend") == "carla_direct"
    ]
    requirement_kwargs = {
        **STRICT_DIRECT_REQUIREMENTS,
        **AB_STAGE_REQUIREMENTS.get(stage_name, {}),
    }
    requirement_check = check_ab_report_requirements(report, **requirement_kwargs)
    direct_policies = sorted(
        {
            str(row.get("direct_stale_world_frame_policy"))
            for row in direct_rows
            if row.get("direct_stale_world_frame_policy")
        }
    )
    direct_contract_statuses = sorted(
        {
            str(row.get("direct_transport_contract_status"))
            for row in direct_rows
            if row.get("direct_transport_contract_status")
        }
    )
    low_cadence_routes: list[str] = []
    for comparison in comparisons:
        cadence = comparison.get("cadence_comparison") if isinstance(comparison.get("cadence_comparison"), Mapping) else {}
        loc_ratio = cadence.get("bridge_loc_hz_ratio")
        chassis_ratio = cadence.get("bridge_chassis_hz_ratio")
        try:
            loc_value = None if loc_ratio is None else float(loc_ratio)
            chassis_value = None if chassis_ratio is None else float(chassis_ratio)
        except (TypeError, ValueError):
            loc_value = None
            chassis_value = None
        if loc_value is None or chassis_value is None or loc_value < 0.8 or chassis_value < 0.8:
            if comparison.get("route_id"):
                low_cadence_routes.append(str(comparison.get("route_id")))
    return {
        "summary_type": "ab_report",
        "status": "passed" if requirement_check.get("passed") else "failed",
        "report_path": str(report_path),
        "verdict_status": verdict.get("status"),
        "hard_gate_status": hard_gate.get("status"),
        "hard_gate_pass": hard_gate.get("pass"),
        "comparison_statuses": {
            str(item.get("route_id")): item.get("status")
            for item in comparisons
            if item.get("route_id")
        },
        "direct_policies": direct_policies,
        "direct_contract_statuses": direct_contract_statuses,
        "low_cadence_routes": sorted(set(low_cadence_routes)),
        "requirement_passed": bool(requirement_check.get("passed")),
        "requirement_failures": requirement_check.get("failures") or [],
        "run_logs": _interesting_run_logs(report),
    }


def _natural_driving_report_summary(report_path: Path | None) -> dict[str, Any] | None:
    if report_path is None or not report_path.exists():
        return None
    try:
        report = _load_json(report_path)
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "summary_type": "natural_driving_report",
            "status": "unreadable",
            "error": f"{type(exc).__name__}: {exc}",
            "report_path": str(report_path),
        }
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    verdict_status = str(verdict.get("status") or "")
    coverage = (
        report.get("capability_coverage")
        if isinstance(report.get("capability_coverage"), Mapping)
        else {}
    )
    verdict_claim = verdict.get("can_claim_full_natural_driving")
    coverage_claim = coverage.get("can_claim_full_natural_driving")
    can_claim_full = bool(verdict_claim is True and coverage_claim is True)
    missing_classes = list(
        verdict.get("missing_required_scenario_classes")
        or coverage.get("missing_required_scenario_classes")
        or []
    )
    unproven_classes = list(
        verdict.get("unproven_required_scenario_classes")
        or coverage.get("unproven_required_scenario_classes")
        or []
    )
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    run_results = [item for item in report.get("run_results") or [] if isinstance(item, Mapping)]
    traffic_light_expectation_blockers = _traffic_light_expectation_blockers(run_results)
    claim_blockers: list[str] = []
    if missing_classes:
        claim_blockers.append("missing_required_scenario_classes")
    if unproven_classes:
        claim_blockers.append("unproven_required_scenario_classes")
    if any(blocker.get("reason") == "traffic_light_expectation_mismatch" for blocker in traffic_light_expectation_blockers):
        claim_blockers.append("traffic_light_expectation_mismatch")
    elif any(blocker.get("reason") == "traffic_light_stimulus_not_claim_grade" for blocker in traffic_light_expectation_blockers):
        claim_blockers.append("traffic_light_stimulus_not_claim_grade")
    elif traffic_light_expectation_blockers:
        claim_blockers.append("traffic_light_expectation_not_manifest_backed")
    if verdict_status == "pass" and can_claim_full and not claim_blockers:
        status = "passed"
    elif verdict_status in {"pass", "warn", "fail", "insufficient_data"}:
        status = "failed"
        if verdict_status == "pass" and not can_claim_full:
            claim_blockers.append("can_claim_full_natural_driving_not_true")
        elif verdict_status != "pass":
            claim_blockers.append(f"verdict_{verdict_status}")
    else:
        status = "unknown"
        claim_blockers.append("verdict_unknown")
    problem_run_details = _natural_driving_problem_details(run_results)
    return {
        "summary_type": "natural_driving_report",
        "status": status,
        "report_path": str(report_path),
        "verdict_status": verdict_status,
        "can_claim_full_natural_driving": can_claim_full,
        "verdict_can_claim_full_natural_driving": verdict_claim,
        "coverage_can_claim_full_natural_driving": coverage_claim,
        "missing_required_scenario_classes": missing_classes,
        "unproven_required_scenario_classes": unproven_classes,
        "claim_blockers": claim_blockers,
        "pass_count": summary.get("pass_count"),
        "warn_count": summary.get("warn_count"),
        "fail_count": summary.get("fail_count"),
        "insufficient_data_count": summary.get("insufficient_data_count"),
        "failed_runs": verdict.get("failed_runs") or [],
        "insufficient_data_runs": verdict.get("insufficient_data_runs") or [],
        "warning_runs": verdict.get("warning_runs") or [],
        "run_statuses": {
            str(item.get("run_id")): item.get("verdict")
            for item in run_results
            if item.get("run_id")
        },
        "problem_run_details": problem_run_details,
        "traffic_light_expectation_blockers": traffic_light_expectation_blockers,
    }


def _goal_audit_summary(report_path: Path | None) -> dict[str, Any] | None:
    if report_path is None or not report_path.exists():
        return None
    try:
        report = _load_json(report_path)
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "summary_type": "goal_audit",
            "status": "unreadable",
            "error": f"{type(exc).__name__}: {exc}",
            "report_path": str(report_path),
        }
    audit_status = str(report.get("status") or "unknown")
    sections = report.get("sections") if isinstance(report.get("sections"), Mapping) else {}
    ab = sections.get("ab") if isinstance(sections.get("ab"), Mapping) else {}
    direct = ab.get("direct_cadence") if isinstance(ab.get("direct_cadence"), Mapping) else {}
    natural = sections.get("natural_driving") if isinstance(sections.get("natural_driving"), Mapping) else {}
    calibration = sections.get("calibration") if isinstance(sections.get("calibration"), Mapping) else {}
    random_regression = sections.get("random_regression") if isinstance(sections.get("random_regression"), Mapping) else {}
    demo = sections.get("demo_recording") if isinstance(sections.get("demo_recording"), Mapping) else {}
    missing_evidence = [str(item) for item in report.get("missing_evidence") or []]
    next_actions = [str(item) for item in report.get("next_actions") or []]
    next_action_commands = (
        dict(report.get("next_action_commands"))
        if isinstance(report.get("next_action_commands"), Mapping)
        else {}
    )
    return {
        "summary_type": "goal_audit",
        "status": "passed" if audit_status == "complete" else "incomplete",
        "report_path": str(report_path),
        "verdict_status": audit_status,
        "audit_status": audit_status,
        "missing_evidence_count": len(missing_evidence),
        "missing_evidence": missing_evidence[:12],
        "next_actions": next_actions[:12],
        "next_action_commands": next_action_commands,
        "requirement_failures": missing_evidence[:12],
        "section_statuses": {
            "ab": ab.get("status"),
            "hard_gate": (ab.get("hard_gate") or {}).get("status")
            if isinstance(ab.get("hard_gate"), Mapping)
            else None,
            "direct_cadence": direct.get("status"),
            "natural_driving": natural.get("status"),
            "calibration": calibration.get("status"),
            "random_regression": random_regression.get("status"),
            "demo_recording": demo.get("status"),
        },
        "guardrails": list(report.get("guardrails") or [])[:8],
    }


def _traffic_light_expectation_blockers(run_results: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for run in run_results:
        scenario_class = str(run.get("scenario_class") or "")
        expected = TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS.get(scenario_class)
        if expected is None:
            continue
        observed = run.get("traffic_light_expected_behavior")
        source = run.get("traffic_light_expectation_source")
        if observed in {None, ""}:
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "reason": "traffic_light_expectation_missing",
                }
            )
        elif observed != expected:
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "reason": "traffic_light_expectation_mismatch",
                    "expected_behavior": expected,
                    "observed_behavior": observed,
                }
            )
        elif source != "manifest":
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "reason": "traffic_light_expectation_not_manifest_backed",
                    "expected_behavior": expected,
                    "source": source,
                }
            )
        elif run.get("traffic_light_claim_grade") is not True:
            blockers.append(
                {
                    "run_id": run.get("run_id"),
                    "scenario_class": scenario_class,
                    "reason": "traffic_light_stimulus_not_claim_grade",
                    "expected_behavior": expected,
                    "stimulus_mode": run.get("traffic_light_stimulus_mode"),
                }
            )
    return blockers


def _natural_driving_problem_details(run_results: list[Mapping[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for item in run_results:
        verdict = str(item.get("verdict") or "")
        if verdict not in {"fail", "insufficient_data", "warn"}:
            continue
        details.append(
            {
                "run_id": item.get("run_id"),
                "scenario_class": item.get("scenario_class"),
                "route_id": item.get("route_id"),
                "verdict": item.get("verdict"),
                "failure_reason": item.get("failure_reason"),
                "missing_artifacts": list(item.get("missing_artifacts") or [])[:8],
                "missing_fields": list(item.get("missing_fields") or [])[:8],
                "runtime_contract_status": item.get("runtime_contract_status"),
                "control_handoff_status": item.get("control_handoff_status"),
                "control_health_status": item.get("control_health_status"),
            }
        )
        if len(details) >= limit:
            break
    return details


def _status_for_stage(
    name: str,
    item: Mapping[str, Any],
    *,
    manifest_dir: Path,
    repo_root: Path,
) -> dict[str, Any]:
    script_path = _resolve(item.get("path"), base_dir=manifest_dir)
    run_root = _resolve(item.get("run_root"), base_dir=repo_root)
    marker = _resolve(item.get("marker"), base_dir=manifest_dir)
    exit_code_path = _resolve(item.get("exit_code_path"), base_dir=manifest_dir)
    last_exit_code = _read_exit_code(exit_code_path)
    evidence_paths: list[str] = []
    existing_evidence_paths: list[str] = []
    missing_evidence_paths: list[str] = []

    if name == "00_status":
        return {
            "name": name,
            "status": "utility",
            "last_exit_code": last_exit_code,
            "exit_code_path": None if exit_code_path is None else str(exit_code_path),
            "exit_code_exists": _exists(exit_code_path),
            "script_path": None if script_path is None else str(script_path),
            "script_exists": _exists(script_path),
            "run_root": None if run_root is None else str(run_root),
            "run_root_exists": _exists(run_root),
            "marker": None if marker is None else str(marker),
            "marker_exists": _exists(marker),
            "evidence_paths": [str(run_root)] if run_root is not None else [],
        }
    elif name in STAGE_OUTPUTS:
        for relative in STAGE_OUTPUTS[name]:
            candidate = run_root / relative if run_root is not None else None
            if candidate is not None:
                evidence_paths.append(str(candidate))
        existing_evidence_paths = [path for path in evidence_paths if Path(path).exists()]
        missing_evidence_paths = [path for path in evidence_paths if not Path(path).exists()]
        completed = bool(evidence_paths) and not missing_evidence_paths
        partial = _exists(run_root) or bool(existing_evidence_paths)
    elif name == "05_demo_recording":
        inspection = _demo_inspection_from_marker(marker)
        if inspection is not None:
            evidence_paths.append(str(inspection))
        completed = _exists(inspection)
        partial = _exists(run_root)
    elif name == "07_calibration_gates":
        completed = _exists(run_root)
        partial = False
        if run_root is not None:
            evidence_paths.append(str(run_root))
    else:
        completed = False
        partial = False

    summary = None
    if name in AB_STAGE_REQUIREMENTS and evidence_paths:
        summary = _ab_report_summary(name, Path(evidence_paths[0]))
    elif name == "10_natural_driving_postprocess" and evidence_paths:
        summary = _natural_driving_report_summary(Path(evidence_paths[0]))
        local_audit = _goal_audit_summary(Path(evidence_paths[2])) if len(evidence_paths) > 2 else None
        if isinstance(summary, dict) and local_audit is not None:
            summary["local_goal_audit"] = local_audit
    elif name in {"06_audit", "11_final_goal_audit"} and evidence_paths:
        summary = _goal_audit_summary(Path(evidence_paths[0]))
    if last_exit_code is not None and last_exit_code != 0:
        status = "failed"
    elif isinstance(summary, Mapping) and summary.get("status") == "failed":
        status = "failed"
    elif completed:
        status = "completed"
    elif partial:
        status = "partial"
    else:
        status = "planned"
    return {
        "name": name,
        "status": status,
        "last_exit_code": last_exit_code,
        "exit_code_path": None if exit_code_path is None else str(exit_code_path),
        "exit_code_exists": _exists(exit_code_path),
        "script_path": None if script_path is None else str(script_path),
        "script_exists": _exists(script_path),
        "run_root": None if run_root is None else str(run_root),
        "run_root_exists": _exists(run_root),
        "marker": None if marker is None else str(marker),
        "marker_exists": _exists(marker),
        "evidence_paths": evidence_paths,
        "existing_evidence_paths": existing_evidence_paths,
        "missing_evidence_paths": missing_evidence_paths,
        "summary": summary,
        "dependencies": _stage_dependencies(item),
    }


def _dependency_status(stage: Mapping[str, Any], by_name: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    dependencies = [item for item in stage.get("dependencies") or [] if isinstance(item, Mapping)]
    if not dependencies:
        return {"status": "none", "blocked": False, "reasons": []}
    reasons: list[str] = []
    checks: list[dict[str, Any]] = []
    for dependency in dependencies:
        dep_name = str(dependency.get("stage") or "")
        dep_stage = by_name.get(dep_name)
        require_summary_pass = bool(dependency.get("require_summary_pass"))
        require_summary_present = bool(dependency.get("require_summary_present"))
        allowed_statuses = [str(item) for item in dependency.get("allowed_statuses") or ["completed"]]
        if not dep_stage:
            reason = f"missing dependency stage: {dep_name}"
            reasons.append(reason)
            checks.append(
                {
                    "stage": dep_name,
                    "require_summary_pass": require_summary_pass,
                    "require_summary_present": require_summary_present,
                    "allowed_statuses": allowed_statuses,
                    "status": "missing",
                    "summary_status": None,
                    "passed": False,
                    "reason": reason,
                }
            )
            continue
        dep_status = dep_stage.get("status")
        summary = dep_stage.get("summary") if isinstance(dep_stage.get("summary"), Mapping) else {}
        summary_status = summary.get("status")
        passed = (
            dep_status in allowed_statuses
            and (not require_summary_present or bool(summary))
            and (not require_summary_pass or summary_status == "passed")
        )
        reason = None
        if dep_status not in allowed_statuses:
            reason = f"dependency {dep_name} status={dep_status}, expected one of {allowed_statuses}"
        elif require_summary_present and not summary:
            reason = f"dependency {dep_name} summary missing"
        elif require_summary_pass and summary_status != "passed":
            reason = f"dependency {dep_name} summary={summary_status}, expected passed"
        if reason:
            reasons.append(reason)
        checks.append(
            {
                "stage": dep_name,
                "require_summary_pass": require_summary_pass,
                "require_summary_present": require_summary_present,
                "allowed_statuses": allowed_statuses,
                "status": dep_status,
                "summary_status": summary_status,
                "passed": passed,
                "reason": reason,
            }
        )
    return {
        "status": "blocked" if reasons else "satisfied",
        "blocked": bool(reasons),
        "reasons": reasons,
        "checks": checks,
    }


def inspect_packet(manifest_path: str | Path) -> dict[str, Any]:
    path = Path(manifest_path).expanduser().resolve()
    manifest = _load_json(path)
    scripts = manifest.get("scripts") if isinstance(manifest.get("scripts"), dict) else {}
    repo_root = _resolve(manifest.get("repo_root"), base_dir=path.parent) or path.parent
    python_exec = str(manifest.get("python") or sys.executable)
    stages = [
        _status_for_stage(name, item, manifest_dir=path.parent, repo_root=repo_root)
        for name, item in sorted(scripts.items())
        if isinstance(item, Mapping)
    ]
    by_name = {str(stage.get("name")): stage for stage in stages if stage.get("name")}
    for stage in stages:
        stage["dependency_status"] = _dependency_status(stage, by_name)
    execution_stages = [stage for stage in stages if stage.get("status") != "utility"]
    runnable = [stage for stage in execution_stages if stage.get("script_exists")]
    next_stage = next((stage for stage in runnable if stage.get("status") != "completed"), None)
    completed_count = sum(1 for stage in execution_stages if stage.get("status") == "completed")
    partial_count = sum(1 for stage in execution_stages if stage.get("status") == "partial")
    failed_count = sum(1 for stage in execution_stages if stage.get("status") == "failed")
    blocked_stages = [
        {
            "stage": stage.get("name"),
            "reasons": (stage.get("dependency_status") or {}).get("reasons") or [],
        }
        for stage in execution_stages
        if (stage.get("dependency_status") or {}).get("blocked")
    ]
    if not execution_stages:
        status = "invalid_manifest"
    elif failed_count:
        status = "attention_required"
    elif completed_count == len(execution_stages):
        status = "complete"
    elif partial_count:
        status = "in_progress"
    else:
        status = "ready_to_start"
    return {
        "schema_version": "town01_goal_validation_packet_status.v1",
        "status": status,
        "manifest_path": str(path),
        "packet_root": str(path.parent),
        "repo_root": str(repo_root),
        "python": python_exec,
        "disk": _disk_status(repo_root),
        "preflight": _preflight_status(repo_root, python_exec),
        "stage_count": len(execution_stages),
        "utility_count": len(stages) - len(execution_stages),
        "completed_count": completed_count,
        "partial_count": partial_count,
        "failed_count": failed_count,
        "blocked_stage_count": len(blocked_stages),
        "blocked_stages": blocked_stages,
        "stages": stages,
        "next_stage": next_stage,
        "next_command": "" if next_stage is None else f"bash {next_stage['script_path']}",
        "guardrails": [
            "status inspection is offline and does not start CARLA/Apollo",
            "completed packet status is evidence organization only, not proof that Town01 goal is solved",
        ],
    }


def _active_stage_summary(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    next_stage = payload.get("next_stage") if isinstance(payload.get("next_stage"), Mapping) else None
    stages = [stage for stage in payload.get("stages") or [] if isinstance(stage, Mapping)]
    stage: Mapping[str, Any] | None = None
    if next_stage and isinstance(next_stage.get("summary"), Mapping):
        stage = next_stage
    else:
        failed = [
            item
            for item in stages
            if item.get("status") == "failed" and isinstance(item.get("summary"), Mapping)
        ]
        if failed:
            stage = failed[0]
        else:
            completed = [
                item
                for item in stages
                if item.get("status") == "completed" and isinstance(item.get("summary"), Mapping)
            ]
            if completed:
                stage = completed[-1]
    if not stage:
        return None
    summary = stage.get("summary") if isinstance(stage.get("summary"), Mapping) else {}
    failures = summary.get("requirement_failures") or []
    result = {
        "stage": stage.get("name"),
        "stage_status": stage.get("status"),
        "summary_type": summary.get("summary_type"),
        "summary_status": summary.get("status"),
        "verdict_status": summary.get("verdict_status"),
        "hard_gate_status": summary.get("hard_gate_status"),
        "direct_policies": summary.get("direct_policies") or [],
        "low_cadence_routes": summary.get("low_cadence_routes") or [],
        "run_logs": summary.get("run_logs") or [],
        "missing_evidence_paths": list(stage.get("missing_evidence_paths") or []),
        "existing_evidence_paths": list(stage.get("existing_evidence_paths") or []),
        "failure_count": len(failures),
        "failures": failures[:5],
    }
    for key in (
        "pass_count",
        "warn_count",
        "fail_count",
        "insufficient_data_count",
        "failed_runs",
        "insufficient_data_runs",
        "warning_runs",
        "run_statuses",
        "problem_run_details",
        "can_claim_full_natural_driving",
        "missing_required_scenario_classes",
        "unproven_required_scenario_classes",
        "claim_blockers",
        "traffic_light_expectation_blockers",
        "audit_status",
        "missing_evidence_count",
        "missing_evidence",
        "next_actions",
        "next_action_commands",
        "section_statuses",
        "local_goal_audit",
    ):
        if key in summary:
            result[key] = summary.get(key)
    if summary.get("summary_type") == "natural_driving_report":
        issue_runs = list(summary.get("failed_runs") or []) + list(summary.get("insufficient_data_runs") or [])
        result["natural_driving_issue_count"] = len(issue_runs)
        result["natural_driving_issue_runs"] = issue_runs[:8]
        result["natural_driving_problem_details"] = list(summary.get("problem_run_details") or [])[:8]
    return result


def write_markdown(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    next_stage = payload.get("next_stage") if isinstance(payload.get("next_stage"), Mapping) else {}
    lines = [
        "# Town01 Goal Validation Packet Status",
        "",
        f"- status: `{payload.get('status')}`",
        f"- completed_count: `{payload.get('completed_count')}` / `{payload.get('stage_count')}`",
        f"- partial_count: `{payload.get('partial_count')}`",
        f"- failed_count: `{payload.get('failed_count')}`",
        f"- next_stage: `{next_stage.get('name', '')}`",
        "",
        "## Disk",
        "",
    ]
    disk = payload.get("disk") if isinstance(payload.get("disk"), Mapping) else {}
    if disk:
        lines.extend(
            [
                f"- status: `{disk.get('status')}`",
                f"- free_gib: `{disk.get('free_gib')}`",
                f"- used_percent: `{disk.get('used_percent')}`",
                f"- reason: {disk.get('reason')}",
                "",
            ]
        )
    preflight = payload.get("preflight") if isinstance(payload.get("preflight"), Mapping) else {}
    if preflight:
        lines.extend(
            [
                "## Preflight",
                "",
                f"- status: `{preflight.get('status')}`",
                f"- missing_count: `{preflight.get('missing_count')}`",
                "",
            ]
        )
        missing = preflight.get("missing") or []
        if missing:
            lines.extend(["Missing required files:", ""])
            lines.extend(f"- `{item.get('path')}`" for item in missing if isinstance(item, Mapping))
            lines.append("")
    lines.extend(
        [
        "## Next Command",
        "",
        "```bash",
        str(payload.get("next_command") or ""),
        "```",
        "",
        "## Stages",
        "",
        "| stage | status | exit_code | summary | script | evidence |",
        "|---|---|---:|---|---|---|",
        ]
    )
    for stage in payload.get("stages") or []:
        if not isinstance(stage, Mapping):
            continue
        evidence = "<br>".join(stage.get("evidence_paths") or [])
        summary = stage.get("summary") if isinstance(stage.get("summary"), Mapping) else {}
        dependency_status = stage.get("dependency_status") if isinstance(stage.get("dependency_status"), Mapping) else {}
        summary_text = ""
        if summary:
            failures = summary.get("requirement_failures") or []
            summary_text = (
                f"{summary.get('status', '')}; verdict={summary.get('verdict_status', '')}; "
                f"hard_gate={summary.get('hard_gate_status', '')}; "
                f"failures={len(failures)}"
            )
        if dependency_status.get("status") == "blocked":
            blocked = "; ".join(dependency_status.get("reasons") or [])
            summary_text = f"{summary_text}; deps_blocked={blocked}" if summary_text else f"deps_blocked={blocked}"
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | `{}` | {} |".format(
                stage.get("name", ""),
                stage.get("status", ""),
                "" if stage.get("last_exit_code") is None else stage.get("last_exit_code"),
                summary_text,
                stage.get("script_path", ""),
                evidence or "",
            )
        )
    summary_stages = [
        stage for stage in payload.get("stages") or []
        if isinstance(stage, Mapping) and isinstance(stage.get("summary"), Mapping)
    ]
    if summary_stages:
        lines.extend(["", "## Evidence Stage Summaries", ""])
        for stage in summary_stages:
            summary = stage["summary"]
            summary_type = summary.get("summary_type")
            missing_paths = [str(item) for item in stage.get("missing_evidence_paths") or []]
            lines.extend(
                [
                    f"### {stage.get('name')}",
                    "",
                    f"- summary_type: `{summary_type}`",
                    f"- status: `{summary.get('status')}`",
                    f"- verdict: `{summary.get('verdict_status')}`",
                    f"- missing_evidence_paths: `{len(missing_paths)}`",
                    "",
                ]
            )
            if missing_paths:
                lines.extend(f"- missing: `{path}`" for path in missing_paths[:8])
                lines.append("")
            if summary_type == "natural_driving_report":
                lines.extend(
                    [
                        f"- pass_count: `{summary.get('pass_count')}`",
                        f"- warn_count: `{summary.get('warn_count')}`",
                        f"- fail_count: `{summary.get('fail_count')}`",
                        f"- insufficient_data_count: `{summary.get('insufficient_data_count')}`",
                        f"- can_claim_full_natural_driving: `{summary.get('can_claim_full_natural_driving')}`",
                        f"- missing_required_scenario_classes: `{', '.join(summary.get('missing_required_scenario_classes') or []) or 'none'}`",
                        f"- unproven_required_scenario_classes: `{', '.join(summary.get('unproven_required_scenario_classes') or []) or 'none'}`",
                        f"- claim_blockers: `{', '.join(summary.get('claim_blockers') or []) or 'none'}`",
                        f"- failed_runs: `{', '.join(summary.get('failed_runs') or []) or 'none'}`",
                        f"- insufficient_data_runs: `{', '.join(summary.get('insufficient_data_runs') or []) or 'none'}`",
                        f"- warning_runs: `{', '.join(summary.get('warning_runs') or []) or 'none'}`",
                        "",
                    ]
                )
                local_audit = (
                    summary.get("local_goal_audit")
                    if isinstance(summary.get("local_goal_audit"), Mapping)
                    else {}
                )
                if local_audit:
                    lines.extend(
                        [
                            f"- local_goal_audit_status: `{local_audit.get('audit_status')}`",
                            f"- local_goal_audit_missing_evidence_count: `{local_audit.get('missing_evidence_count')}`",
                            "",
                        ]
                    )
                details = [item for item in summary.get("problem_run_details") or [] if isinstance(item, Mapping)]
                if details:
                    lines.extend(
                        [
                            "| run_id | scenario_class | verdict | failure_reason | missing_artifacts | missing_fields |",
                            "| --- | --- | --- | --- | --- | --- |",
                        ]
                    )
                    for detail in details[:8]:
                        lines.append(
                            "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                                detail.get("run_id"),
                                detail.get("scenario_class"),
                                detail.get("verdict"),
                                detail.get("failure_reason"),
                                ", ".join(detail.get("missing_artifacts") or []),
                                ", ".join(detail.get("missing_fields") or []),
                            )
                        )
                    lines.append("")
                blockers = [
                    item
                    for item in summary.get("traffic_light_expectation_blockers") or []
                    if isinstance(item, Mapping)
                ]
                if blockers:
                    lines.extend(
                        [
                            "| run_id | scenario_class | reason | expected_behavior | observed_behavior | source |",
                            "| --- | --- | --- | --- | --- | --- |",
                        ]
                    )
                    for blocker in blockers[:8]:
                        lines.append(
                            "| `{}` | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                                blocker.get("run_id"),
                                blocker.get("scenario_class"),
                                blocker.get("reason"),
                                blocker.get("expected_behavior"),
                                blocker.get("observed_behavior"),
                                blocker.get("source"),
                            )
                        )
                    lines.append("")
            elif summary_type == "goal_audit":
                section_statuses = (
                    summary.get("section_statuses")
                    if isinstance(summary.get("section_statuses"), Mapping)
                    else {}
                )
                lines.extend(
                    [
                        f"- audit_status: `{summary.get('audit_status')}`",
                        f"- missing_evidence_count: `{summary.get('missing_evidence_count')}`",
                        f"- ab: `{section_statuses.get('ab')}`",
                        f"- hard_gate: `{section_statuses.get('hard_gate')}`",
                        f"- direct_cadence: `{section_statuses.get('direct_cadence')}`",
                        f"- natural_driving: `{section_statuses.get('natural_driving')}`",
                        f"- calibration: `{section_statuses.get('calibration')}`",
                        f"- random_regression: `{section_statuses.get('random_regression')}`",
                        f"- demo_recording: `{section_statuses.get('demo_recording')}`",
                        "",
                    ]
                )
                missing_evidence = [str(item) for item in summary.get("missing_evidence") or []]
                if missing_evidence:
                    lines.extend(["Missing evidence:", ""])
                    lines.extend(f"- {item}" for item in missing_evidence)
                    lines.append("")
                next_actions = [str(item) for item in summary.get("next_actions") or []]
                if next_actions:
                    lines.extend(["Next actions:", ""])
                    lines.extend(f"- {item}" for item in next_actions)
                    lines.append("")
                commands = (
                    summary.get("next_action_commands")
                    if isinstance(summary.get("next_action_commands"), Mapping)
                    else {}
                )
                if commands:
                    lines.extend(["Recommended commands:", ""])
                    for label, command in commands.items():
                        lines.extend([f"- `{label}`:", "", "```bash", str(command), "```", ""])
            else:
                lines.extend(
                    [
                        f"- hard_gate: `{summary.get('hard_gate_status')}`",
                        f"- direct_policies: `{', '.join(summary.get('direct_policies') or [])}`",
                        f"- direct_contract_statuses: `{', '.join(summary.get('direct_contract_statuses') or [])}`",
                        f"- low_cadence_routes: `{', '.join(summary.get('low_cadence_routes') or [])}`",
                        "",
                    ]
                )
            run_logs = [item for item in summary.get("run_logs") or [] if isinstance(item, Mapping)]
            if run_logs:
                lines.extend(["Run logs:", ""])
                for item in run_logs:
                    lines.append(
                        "- `{}` `{}` `{}` stdout=`{}` stderr=`{}`".format(
                            item.get("run_id"),
                            item.get("backend"),
                            item.get("run_status"),
                            item.get("command_stdout_path"),
                            item.get("command_stderr_path"),
                        )
                    )
                    stderr_tail = item.get("command_stderr_tail")
                    if stderr_tail:
                        lines.extend(["", "stderr tail:", "", "```text", str(stderr_tail), "```", ""])
                lines.append("")
            failures = summary.get("requirement_failures") or []
            if failures:
                lines.extend(["Requirement failures:", ""])
                lines.extend(f"- {item}" for item in failures)
                lines.append("")
    blocked = payload.get("blocked_stages") or []
    if blocked:
        lines.extend(["", "## Dependency Blocks", ""])
        for item in blocked:
            if not isinstance(item, Mapping):
                continue
            lines.append(f"### {item.get('stage')}")
            lines.append("")
            for reason in item.get("reasons") or []:
                lines.append(f"- {reason}")
            lines.append("")
    lines.extend(["", "## Guardrails", ""])
    lines.extend(f"- {item}" for item in payload.get("guardrails") or [])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect a generated Town01 validation packet and report next stage.")
    parser.add_argument("manifest", type=Path, help="town01_goal_validation_packet.json")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--md-out", type=Path)
    parser.add_argument("--print-next-command", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = inspect_packet(args.manifest)
    json_out = args.json_out or (Path(args.manifest).expanduser().parent / "town01_goal_validation_packet_status.json")
    md_out = args.md_out or (Path(args.manifest).expanduser().parent / "town01_goal_validation_packet_status.md")
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(md_out, payload)
    if args.print_next_command:
        print(payload.get("next_command") or "")
    else:
        disk = payload.get("disk") if isinstance(payload.get("disk"), Mapping) else {}
        preflight = payload.get("preflight") if isinstance(payload.get("preflight"), Mapping) else {}
        active_summary = _active_stage_summary(payload)
        next_stage = payload.get("next_stage") if isinstance(payload.get("next_stage"), Mapping) else {}
        print(
            json.dumps(
                {
                    "status": payload.get("status"),
                    "failed_count": payload.get("failed_count"),
                    "blocked_stage_count": payload.get("blocked_stage_count"),
                    "blocked_stages": payload.get("blocked_stages") or [],
                    "disk_status": disk.get("status"),
                    "disk_free_gib": disk.get("free_gib"),
                    "disk_used_percent": disk.get("used_percent"),
                    "preflight_status": preflight.get("status"),
                    "preflight_missing_count": preflight.get("missing_count"),
                    "active_stage_summary": active_summary,
                    "active_stage_failures": []
                    if active_summary is None
                    else active_summary.get("failures", []),
                    "active_stage_missing_evidence_paths": []
                    if active_summary is None
                    else active_summary.get("missing_evidence_paths", []),
                    "active_stage_run_logs": []
                    if active_summary is None
                    else active_summary.get("run_logs", []),
                    "active_stage_problem_runs": []
                    if active_summary is None
                    else active_summary.get("natural_driving_issue_runs", []),
                    "active_stage_problem_details": []
                    if active_summary is None
                    else active_summary.get("natural_driving_problem_details", []),
                    "active_stage_missing_evidence": []
                    if active_summary is None
                    else active_summary.get("missing_evidence", []),
                    "active_stage_next_actions": []
                    if active_summary is None
                    else active_summary.get("next_actions", []),
                    "active_stage_next_action_commands": {}
                    if active_summary is None
                    else active_summary.get("next_action_commands", {}),
                    "next_stage": next_stage.get("name"),
                    "next_stage_run_root": next_stage.get("run_root"),
                    "next_stage_script_path": next_stage.get("script_path"),
                    "next_stage_evidence_paths": next_stage.get("evidence_paths") or [],
                    "next_command": payload.get("next_command"),
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if payload.get("status") != "invalid_manifest" else 2


if __name__ == "__main__":
    raise SystemExit(main())
