from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

PHASE1_STATUS_SCHEMA_VERSION = "phase1_status.v1"

INVALID_REASONS = {
    "setup_failed",
    "missing_target_actor",
    "missing_required_artifact",
    "backend_not_ready",
    "no_timeseries",
    "fixed_scene_failed",
    "config_invalid",
}
FAILED_REASONS = {
    "collision",
    "no_control",
    "stuck",
    "overshoot_target",
    "unsafe_gap",
    "timeout",
    "off_route",
    "lane_invasion",
}
DEGRADED_REASONS = {
    "late_response",
    "large_final_gap",
    "small_final_gap",
    "unstable_control",
}


def classify_phase1_run(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    v_t_gap = _read_json(root / "analysis" / "v_t_gap" / "v_t_gap_report.json")
    required_artifacts = _required_artifacts(root)
    missing = [name for name, state in required_artifacts.items() if state == "missing"]
    if required_artifacts["timeseries"] == "missing":
        return _status(root, manifest, summary, "invalid", "no_timeseries", missing, v_t_gap, required_artifacts)
    base_missing = [name for name in ("manifest", "summary") if required_artifacts[name] == "missing"]
    if base_missing:
        return _status(root, manifest, summary, "invalid", "missing_required_artifact", missing, v_t_gap, required_artifacts)

    target_contract = _target_contract(root, v_t_gap)
    if target_contract.get("status") == "missing":
        return _status(root, manifest, summary, "invalid", "missing_target_actor", [], v_t_gap, required_artifacts)
    if not v_t_gap and target_contract.get("status") != "not_required":
        return _status(
            root,
            manifest,
            summary,
            "invalid",
            "missing_required_artifact",
            ["analysis/v_t_gap/v_t_gap_report.json"],
            v_t_gap,
            required_artifacts,
        )

    if v_t_gap and v_t_gap.get("status") == "invalid":
        reason = str(v_t_gap.get("invalid_reason") or "missing_required_artifact")
        if reason not in INVALID_REASONS:
            reason = "missing_required_artifact"
        return _status(root, manifest, summary, "invalid", reason, [], v_t_gap, required_artifacts)

    if _summary_failed_by_setup(summary):
        return _status(root, manifest, summary, "invalid", "setup_failed", [], v_t_gap, required_artifacts)
    if _backend_not_ready(summary, manifest):
        return _status(root, manifest, summary, "invalid", "backend_not_ready", [], v_t_gap, required_artifacts)
    if _fixed_scene_failed(summary):
        return _status(root, manifest, summary, "invalid", "fixed_scene_failed", [], v_t_gap, required_artifacts)

    failed = _failed_reason(summary, v_t_gap)
    if failed:
        return _status(root, manifest, summary, "failed", failed, [], v_t_gap, required_artifacts)

    degraded = _degraded_reason(summary, v_t_gap)
    if degraded:
        return _status(root, manifest, summary, "degraded", degraded, [], v_t_gap, required_artifacts)

    return _status(root, manifest, summary, "success", None, [], v_t_gap, required_artifacts)


def write_phase1_status(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "phase1_status.json"
    md_path = output / "phase1_status_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(
        "# Phase 1 Run Status\n\n"
        f"Status: `{report.get('status')}`\n\n"
        f"Failure reason: `{report.get('failure_reason')}`\n\n"
        f"Evaluable: `{report.get('evaluable')}`\n",
        encoding="utf-8",
    )
    return {"report": str(json_path), "summary": str(md_path)}


def _status(
    root: Path,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    status: str,
    reason: str | None,
    missing_artifacts: list[str],
    v_t_gap: Mapping[str, Any],
    required_artifacts: Mapping[str, str],
) -> dict[str, Any]:
    original_reason = summary.get("fail_reason") or summary.get("failure_reason")
    scenario_case = (
        manifest.get("scenario_case")
        or manifest.get("fixed_scene_case")
        or manifest.get("scenario_id")
        or summary.get("scenario_case")
        or summary.get("scenario_id")
    )
    invalid_reasons = [reason] if status == "invalid" and reason else []
    failed_reasons = [reason] if status == "failed" and reason else []
    degraded_reasons = [reason] if status == "degraded" and reason else []
    return {
        "schema_version": PHASE1_STATUS_SCHEMA_VERSION,
        "run_dir": str(root),
        "run_id": manifest.get("run_id") or summary.get("run_id") or root.name,
        "scenario_id": manifest.get("scenario_id") or summary.get("scenario_id"),
        "scenario_case": scenario_case,
        "backend": manifest.get("backend") or manifest.get("backend_name") or summary.get("backend"),
        "backend_name": manifest.get("backend_name") or manifest.get("backend") or summary.get("backend_name"),
        "backend_type": manifest.get("backend_type") or summary.get("backend_type"),
        "status": status,
        "failure_reason": reason,
        "invalid_reasons": invalid_reasons,
        "degraded_reasons": degraded_reasons,
        "failed_reasons": failed_reasons,
        "original_failure_reason": original_reason,
        "evaluable": status != "invalid",
        "evidence_files": _evidence_files(root),
        "missing_artifacts": missing_artifacts,
        "required_artifacts": dict(required_artifacts),
        "v_t_gap_status": v_t_gap.get("status") if v_t_gap else None,
        "summary_status": summary.get("status"),
        "summary_success": summary.get("success"),
        "notes": _status_notes(status, reason),
        "claim_boundary": "phase1_status_is_scenario_evaluation_not_natural_driving_claim",
    }


def _required_artifacts(root: Path) -> dict[str, str]:
    return {
        "manifest": "present" if (root / "manifest.json").exists() else "missing",
        "summary": "present" if (root / "summary.json").exists() else "missing",
        "timeseries": "present" if (root / "timeseries.csv").exists() or (root / "timeseries.jsonl").exists() else "missing",
        "v_t_gap_report": "present"
        if (root / "analysis" / "v_t_gap" / "v_t_gap_report.json").exists()
        else "missing",
    }


def _evidence_files(root: Path) -> list[str]:
    candidates = [
        "manifest.json",
        "summary.json",
        "timeseries.csv",
        "timeseries.jsonl",
        "artifacts/fixed_scene_resolved.json",
        "analysis/v_t_gap/v_t_gap_report.json",
        "analysis/v_t_gap/v_t_gap.csv",
    ]
    return [str(root / rel) for rel in candidates if (root / rel).exists()]


def _status_notes(status: str, reason: str | None) -> list[str]:
    notes = ["phase1_status_is_scenario_evaluation_not_natural_driving_claim"]
    if status == "invalid":
        notes.append("invalid_run_is_setup_or_evidence_failure_not_backend_loss")
    if reason:
        notes.append(f"classified_by_{reason}")
    return notes


def _summary_failed_by_setup(summary: Mapping[str, Any]) -> bool:
    reason = str(summary.get("fail_reason") or summary.get("failure_reason") or "")
    return reason in {"setup_failed", "bridge_runtime_import_failed", "config_invalid"}


def _backend_not_ready(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> bool:
    reason = str(summary.get("fail_reason") or summary.get("failure_reason") or "")
    if reason == "backend_not_ready":
        return True
    return manifest.get("backend_ready") is False


def _fixed_scene_failed(summary: Mapping[str, Any]) -> bool:
    return str(summary.get("fixed_scene_contract_status") or "") == "fail"


def _failed_reason(summary: Mapping[str, Any], v_t_gap: Mapping[str, Any]) -> str | None:
    for key in ("collision_count", "lane_invasion_count"):
        try:
            if float(summary.get(key) or 0.0) > 0.0:
                return "collision" if key == "collision_count" else "lane_invasion"
        except (TypeError, ValueError):
            pass
    reason = str(summary.get("fail_reason") or summary.get("failure_reason") or "")
    if reason in FAILED_REASONS:
        return reason
    rows = v_t_gap.get("rows") if isinstance(v_t_gap.get("rows"), list) else []
    gaps = [_to_float(row.get("gap_m")) for row in rows if isinstance(row, Mapping)]
    gaps = [gap for gap in gaps if gap is not None]
    if gaps and min(gaps) < 0.0:
        return "unsafe_gap"
    return None


def _degraded_reason(summary: Mapping[str, Any], v_t_gap: Mapping[str, Any]) -> str | None:
    reason = str(summary.get("degraded_reason") or "")
    if reason in DEGRADED_REASONS:
        return reason
    rows = v_t_gap.get("rows") if isinstance(v_t_gap.get("rows"), list) else []
    final_gap = _final_gap(rows)
    if final_gap is not None and final_gap > 60.0:
        return "large_final_gap"
    if v_t_gap.get("status") == "warn":
        return "large_final_gap"
    return None


def _final_gap(rows: Any) -> float | None:
    if not rows:
        return None
    for row in reversed(rows):
        if isinstance(row, Mapping):
            gap = _to_float(row.get("gap_m"))
            if gap is not None:
                return gap
    return None


def _target_contract(root: Path, v_t_gap: Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(v_t_gap.get("target_actor_contract"), Mapping):
        return v_t_gap["target_actor_contract"]
    resolved = _read_json(root / "artifacts" / "fixed_scene_resolved.json")
    contract = resolved.get("target_actor_contract")
    return contract if isinstance(contract, Mapping) else {}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
