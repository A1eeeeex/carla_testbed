from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

SCHEMA_VERSION = "apollo_control_runtime_diff.v1"


def analyze_apollo_control_runtime_diff(
    *,
    baseline_run: str | Path,
    candidate_run: str | Path,
) -> dict[str, Any]:
    baseline = Path(baseline_run).expanduser()
    candidate = Path(candidate_run).expanduser()
    baseline_summary = _run_summary(baseline)
    candidate_summary = _run_summary(candidate)
    overlay_comparison = _overlay_comparison(baseline, candidate)
    docker_libs = _docker_libs_comparison(baseline, candidate)
    survival = {
        "baseline": _control_survival(baseline),
        "candidate": _control_survival(candidate),
        "candidate_improved": _survival_improved(
            _control_survival(baseline),
            _control_survival(candidate),
        ),
    }
    handoff = {
        "baseline": _handoff_summary(baseline),
        "candidate": _handoff_summary(candidate),
    }
    crash_signature_changed = bool(
        handoff["baseline"].get("crash_detected")
        and not handoff["candidate"].get("crash_detected")
    )
    blocking_reasons = _blocking_reasons(
        baseline=baseline_summary,
        candidate=candidate_summary,
        overlay_comparison=overlay_comparison,
        survival=survival,
        handoff=handoff,
    )
    warnings = _warnings(
        baseline=baseline_summary,
        candidate=candidate_summary,
        docker_libs=docker_libs,
    )
    verdict_status = "pass" if not blocking_reasons else "insufficient_data"
    return {
        "schema_version": SCHEMA_VERSION,
        "baseline_run": baseline_summary,
        "candidate_run": candidate_summary,
        "overlay_comparison": overlay_comparison,
        "docker_libs_comparison": docker_libs,
        "control_survival": survival,
        "control_handoff": handoff,
        "crash_signature_changed": crash_signature_changed,
        "primary_difference": _primary_difference(overlay_comparison, survival, handoff),
        "diagnostic_conclusion": _diagnostic_conclusion(overlay_comparison, survival, handoff),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "verdict": {
            "status": verdict_status,
            "claim_boundary": (
                "This report compares runtime conditions only. It can support a diagnostic "
                "hypothesis about Apollo control process survival, but it is not Apollo "
                "natural-driving evidence and does not make a control-runtime overlay default."
            ),
        },
    }


def write_apollo_control_runtime_diff(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    report_path = out / "apollo_control_runtime_diff_report.json"
    summary_path = out / "apollo_control_runtime_diff_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_apollo_control_runtime_diff_summary(report), encoding="utf-8")
    return {"report": str(report_path), "summary": str(summary_path)}


def render_apollo_control_runtime_diff_summary(report: Mapping[str, Any]) -> str:
    baseline = report.get("baseline_run") if isinstance(report.get("baseline_run"), Mapping) else {}
    candidate = report.get("candidate_run") if isinstance(report.get("candidate_run"), Mapping) else {}
    overlay = report.get("overlay_comparison") if isinstance(report.get("overlay_comparison"), Mapping) else {}
    survival = report.get("control_survival") if isinstance(report.get("control_survival"), Mapping) else {}
    handoff = report.get("control_handoff") if isinstance(report.get("control_handoff"), Mapping) else {}
    lines = [
        "# Apollo Control Runtime Diff",
        "",
        f"- baseline_run: `{baseline.get('run_id')}`",
        f"- candidate_run: `{candidate.get('run_id')}`",
        f"- same_scenario_case: `{report.get('baseline_run', {}).get('scenario_case') == report.get('candidate_run', {}).get('scenario_case')}`",
        f"- overlay_difference: `{overlay.get('difference')}`",
        f"- baseline_overlay_active: `{overlay.get('baseline_overlay_active')}`",
        f"- candidate_overlay_active: `{overlay.get('candidate_overlay_active')}`",
        f"- candidate_selected_count: `{overlay.get('candidate_selected_count')}`",
        f"- single_added_target: `{overlay.get('single_added_target')}`",
        f"- control_cmd_proto_runtime_target_added: `{overlay.get('control_cmd_proto_runtime_target_added')}`",
        f"- candidate_survival_improved: `{survival.get('candidate_improved')}`",
        f"- baseline_handoff: `{_nested_get(handoff, 'baseline', 'verdict')}` / `{_nested_get(handoff, 'baseline', 'failure_stage')}`",
        f"- candidate_handoff: `{_nested_get(handoff, 'candidate', 'verdict')}` / `{_nested_get(handoff, 'candidate', 'failure_stage')}`",
        f"- primary_difference: `{report.get('primary_difference')}`",
        f"- diagnostic_conclusion: `{report.get('diagnostic_conclusion')}`",
        "",
        "## Boundary",
        "",
        str((report.get("verdict") or {}).get("claim_boundary") or ""),
    ]
    blocking = list(report.get("blocking_reasons") or [])
    if blocking:
        lines.extend(["", "## Blocking Reasons", ""])
        lines.extend(f"- `{item}`" for item in blocking)
    warnings = list(report.get("warnings") or [])
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in warnings)
    targets = overlay.get("candidate_overlay_targets")
    if isinstance(targets, list) and targets:
        lines.extend(["", "## Candidate Overlay Targets", ""])
        lines.extend(f"- `{item}`" for item in targets[:20])
    added_records = overlay.get("added_records")
    if isinstance(added_records, list) and added_records:
        lines.extend(["", "## Added Overlay Records", ""])
        for item in added_records[:20]:
            if isinstance(item, Mapping):
                lines.append(
                    f"- `{item.get('name')}`: `{item.get('source')}` -> `{item.get('target')}`"
                )
    return "\n".join(lines) + "\n"


def _run_summary(run_dir: Path) -> dict[str, Any]:
    manifest = _read_json(run_dir / "manifest.json")
    summary = _read_json(run_dir / "summary.json")
    return {
        "run_dir": str(run_dir),
        "run_id": manifest.get("run_id") or summary.get("run_id") or run_dir.name,
        "scenario_id": manifest.get("scenario_id") or summary.get("scenario_id"),
        "scenario_case": manifest.get("scenario_case")
        or manifest.get("fixed_scene_case")
        or summary.get("scenario_case")
        or summary.get("scenario_id"),
        "backend": manifest.get("backend") or manifest.get("backend_name") or summary.get("backend"),
        "success": summary.get("success"),
        "fail_reason": summary.get("fail_reason") or summary.get("failure_reason"),
    }


def _overlay_comparison(baseline: Path, candidate: Path) -> dict[str, Any]:
    baseline_manifest = _read_json(baseline / "artifacts" / "apollo_control_runtime_overlay_manifest.json")
    candidate_manifest = _read_json(candidate / "artifacts" / "apollo_control_runtime_overlay_manifest.json")
    baseline_records = _overlay_records(baseline_manifest)
    candidate_records = _overlay_records(candidate_manifest)
    baseline_targets = _overlay_targets(baseline_records)
    candidate_targets = _overlay_targets(candidate_records)
    added_targets = sorted(set(candidate_targets) - set(baseline_targets))
    removed_targets = sorted(set(baseline_targets) - set(candidate_targets))
    added_records = _records_for_targets(candidate_records, added_targets)
    removed_records = _records_for_targets(baseline_records, removed_targets)
    single_added = added_records[0] if len(added_records) == 1 else {}
    baseline_active = bool(baseline_manifest.get("overlay_active") or baseline_records)
    candidate_active = bool(candidate_manifest.get("overlay_active") or candidate_records)
    if baseline_active == candidate_active and set(baseline_targets) == set(candidate_targets):
        difference = "none"
    elif not baseline_active and candidate_active:
        difference = "candidate_overlay_enabled"
    elif baseline_active and not candidate_active:
        difference = "baseline_overlay_enabled"
    else:
        difference = "overlay_target_set_changed"
    return {
        "baseline_overlay_active": baseline_active,
        "candidate_overlay_active": candidate_active,
        "baseline_selected_count": int(baseline_manifest.get("selected_count") or len(baseline_records)),
        "candidate_selected_count": int(candidate_manifest.get("selected_count") or len(candidate_records)),
        "baseline_overlay_targets": baseline_targets,
        "candidate_overlay_targets": candidate_targets,
        "added_targets": added_targets,
        "removed_targets": removed_targets,
        "added_records": added_records,
        "removed_records": removed_records,
        "single_added_target": single_added.get("target") if single_added else None,
        "single_added_name": single_added.get("name") if single_added else None,
        "single_added_source": single_added.get("source") if single_added else None,
        "single_added_source_root": single_added.get("source_root") if single_added else None,
        "control_cmd_proto_runtime_target_added": _is_control_cmd_proto_record(single_added),
        "candidate_source_roots": sorted({_source_root(record) for record in candidate_records if _source_root(record)}),
        "difference": difference,
    }


def _docker_libs_comparison(baseline: Path, candidate: Path) -> dict[str, Any]:
    baseline_manifest = _read_json_list(baseline / "artifacts" / "apollo_docker_libs_manifest.json")
    candidate_manifest = _read_json_list(candidate / "artifacts" / "apollo_docker_libs_manifest.json")
    baseline_map = _docker_lib_map(baseline_manifest)
    candidate_map = _docker_lib_map(candidate_manifest)
    changed = []
    for alias in sorted(set(baseline_map) | set(candidate_map)):
        if baseline_map.get(alias) != candidate_map.get(alias):
            changed.append(
                {
                    "alias_name": alias,
                    "baseline": baseline_map.get(alias),
                    "candidate": candidate_map.get(alias),
                }
            )
    return {
        "baseline_entry_count": len(baseline_manifest),
        "candidate_entry_count": len(candidate_manifest),
        "same_manifest": not changed and bool(baseline_manifest or candidate_manifest),
        "changed_entries": changed,
    }


def _control_survival(run_dir: Path) -> dict[str, Any]:
    payload = _read_json(run_dir / "artifacts" / "apollo_control_deferred_survival.json")
    return {
        "available": bool(payload),
        "control_started_pid_seen": payload.get("control_started_pid_seen"),
        "control_survived_5s": payload.get("control_survived_5s"),
        "control_survived_10s": payload.get("control_survived_10s"),
        "control_present_at_end": payload.get("control_present_at_end"),
    }


def _handoff_summary(run_dir: Path) -> dict[str, Any]:
    report = _read_json(run_dir / "analysis" / "apollo_control_handoff" / "apollo_control_handoff_report.json")
    process = report.get("process_health") if isinstance(report.get("process_health"), Mapping) else {}
    return {
        "available": bool(report),
        "verdict": report.get("verdict") or report.get("status"),
        "failure_stage": report.get("failure_stage"),
        "blocking_reasons": list(report.get("blocking_reasons") or []),
        "crash_detected": process.get("crash_detected"),
        "crash_reason": process.get("crash_reason"),
        "crash_signature": process.get("crash_signature"),
    }


def _survival_improved(baseline: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    return bool(
        baseline.get("available")
        and candidate.get("available")
        and not baseline.get("control_survived_10s")
        and candidate.get("control_survived_10s")
        and candidate.get("control_present_at_end")
    )


def _primary_difference(
    overlay: Mapping[str, Any],
    survival: Mapping[str, Any],
    handoff: Mapping[str, Any],
) -> str:
    if (
        overlay.get("difference") == "overlay_target_set_changed"
        and overlay.get("control_cmd_proto_runtime_target_added")
        and survival.get("candidate_improved")
        and _nested_get(handoff, "candidate", "failure_stage") == "none"
    ):
        return "control_cmd_proto_runtime_target_correlates_with_process_survival"
    if (
        overlay.get("difference") == "overlay_target_set_changed"
        and overlay.get("single_added_target")
        and survival.get("candidate_improved")
        and _nested_get(handoff, "candidate", "failure_stage") == "none"
    ):
        return "single_overlay_target_correlates_with_process_survival"
    if (
        overlay.get("difference") == "candidate_overlay_enabled"
        and survival.get("candidate_improved")
        and _nested_get(handoff, "baseline", "crash_reason") == "tcmalloc_invalid_free"
        and not _nested_get(handoff, "candidate", "crash_detected")
    ):
        return "candidate_control_runtime_overlay_correlates_with_process_survival"
    if overlay.get("difference") != "none":
        return str(overlay.get("difference"))
    return "insufficient_runtime_difference"


def _diagnostic_conclusion(
    overlay: Mapping[str, Any],
    survival: Mapping[str, Any],
    handoff: Mapping[str, Any],
) -> str:
    if _primary_difference(overlay, survival, handoff) == "control_cmd_proto_runtime_target_correlates_with_process_survival":
        return (
            "The candidate adds exactly one control command protobuf runtime target and keeps "
            "Apollo control alive where the baseline does not. This supports a "
            "control-message/protobuf runtime differential hypothesis, not a driving-capability "
            "claim and not proof that the target can be promoted by itself."
        )
    if _primary_difference(overlay, survival, handoff) == "single_overlay_target_correlates_with_process_survival":
        return (
            "The candidate adds exactly one runtime overlay target and improves Apollo control "
            "process survival. This narrows the runtime differential, but still requires ABI or "
            "provenance validation before changing any default runtime condition."
        )
    if _primary_difference(overlay, survival, handoff) == "candidate_control_runtime_overlay_correlates_with_process_survival":
        return (
            "The no-overlay run crashes in Apollo control with tcmalloc_invalid_free, while the "
            "candidate run enables a 17-file control-runtime overlay and keeps control alive. "
            "This supports a runtime-binary/configuration differential hypothesis, not a "
            "driving-capability claim."
        )
    return "insufficient_data_to_attribute_control_runtime_change"


def _blocking_reasons(
    *,
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    overlay_comparison: Mapping[str, Any],
    survival: Mapping[str, Any],
    handoff: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not baseline.get("run_id") or not candidate.get("run_id"):
        reasons.append("run_identity_missing")
    if baseline.get("scenario_case") != candidate.get("scenario_case"):
        reasons.append("scenario_case_mismatch")
    if baseline.get("backend") != candidate.get("backend"):
        reasons.append("backend_mismatch")
    if overlay_comparison.get("difference") == "none":
        reasons.append("no_runtime_overlay_difference")
    if not survival.get("baseline", {}).get("available") or not survival.get("candidate", {}).get("available"):
        reasons.append("control_survival_artifact_missing")
    if not handoff.get("baseline", {}).get("available") or not handoff.get("candidate", {}).get("available"):
        reasons.append("control_handoff_artifact_missing")
    return reasons


def _warnings(
    *,
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    docker_libs: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = ["diagnostic_only_not_capability_evidence"]
    if baseline.get("success") or candidate.get("success"):
        warnings.append("runtime_diff_does_not_evaluate_driving_success")
    if not docker_libs.get("same_manifest"):
        warnings.append("apollo_docker_libs_manifest_differs_or_missing")
    return warnings


def _overlay_records(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    records = payload.get("records")
    if isinstance(records, list):
        return [record for record in records if isinstance(record, Mapping)]
    return []


def _overlay_targets(records: list[Mapping[str, Any]]) -> list[str]:
    return sorted(str(record.get("target")) for record in records if record.get("target"))


def _records_for_targets(records: list[Mapping[str, Any]], targets: list[str]) -> list[dict[str, Any]]:
    target_set = set(targets)
    result: list[dict[str, Any]] = []
    for record in records:
        target = str(record.get("target") or "")
        if target not in target_set:
            continue
        result.append(
            {
                "name": str(record.get("name") or Path(target).name),
                "source": str(record.get("source") or ""),
                "target": target,
                "source_root": _source_root(record),
            }
        )
    return sorted(result, key=lambda item: str(item.get("target") or ""))


def _is_control_cmd_proto_record(record: Mapping[str, Any]) -> bool:
    if not record:
        return False
    haystack = " ".join(
        str(record.get(key) or "")
        for key in ("name", "source", "target", "source_root")
    ).lower()
    return "control_cmd_proto" in haystack or "common_msgs/control_msgs" in haystack


def _source_root(record: Mapping[str, Any]) -> str | None:
    source = str(record.get("source") or "")
    if not source:
        return None
    marker = "/control_runtime"
    if marker in source:
        return source.split(marker, 1)[0] + marker
    return str(Path(source).parent)


def _docker_lib_map(entries: list[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for entry in entries:
        alias = entry.get("alias_name") or entry.get("path")
        if not alias:
            continue
        result[str(alias)] = {
            "resolved_name": entry.get("resolved_name"),
            "resolved_path": entry.get("resolved_path"),
            "sync_mode": entry.get("sync_mode"),
            "symlink_target": entry.get("symlink_target"),
        }
    return result


def _nested_get(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_json_list(path: Path) -> list[Mapping[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, Mapping)]
