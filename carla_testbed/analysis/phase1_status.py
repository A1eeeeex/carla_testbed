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
    required = ["manifest.json", "summary.json"]
    missing = [name for name in required if not (root / name).exists()]
    if not (root / "timeseries.csv").exists() and not (root / "timeseries.jsonl").exists():
        missing.append("timeseries.*")
    if missing:
        return _status(root, manifest, summary, "invalid", "missing_required_artifact", missing, v_t_gap)

    target_contract = _target_contract(root, v_t_gap)
    if target_contract.get("status") == "missing":
        return _status(root, manifest, summary, "invalid", "missing_target_actor", [], v_t_gap)

    if v_t_gap and v_t_gap.get("status") == "invalid":
        reason = str(v_t_gap.get("invalid_reason") or "missing_required_artifact")
        if reason not in INVALID_REASONS:
            reason = "missing_required_artifact"
        return _status(root, manifest, summary, "invalid", reason, [], v_t_gap)

    if _summary_failed_by_setup(summary):
        return _status(root, manifest, summary, "invalid", "setup_failed", [], v_t_gap)
    if _backend_not_ready(summary, manifest):
        return _status(root, manifest, summary, "invalid", "backend_not_ready", [], v_t_gap)
    if _fixed_scene_failed(summary):
        return _status(root, manifest, summary, "invalid", "fixed_scene_failed", [], v_t_gap)

    failed = _failed_reason(summary, v_t_gap)
    if failed:
        return _status(root, manifest, summary, "failed", failed, [], v_t_gap)

    degraded = _degraded_reason(summary, v_t_gap)
    if degraded:
        return _status(root, manifest, summary, "degraded", degraded, [], v_t_gap)

    return _status(root, manifest, summary, "success", None, [], v_t_gap)


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
) -> dict[str, Any]:
    return {
        "schema_version": PHASE1_STATUS_SCHEMA_VERSION,
        "run_dir": str(root),
        "run_id": manifest.get("run_id") or summary.get("run_id") or root.name,
        "scenario_id": manifest.get("scenario_id") or summary.get("scenario_id"),
        "backend": manifest.get("backend"),
        "backend_type": manifest.get("backend_type"),
        "status": status,
        "failure_reason": reason,
        "evaluable": status != "invalid",
        "missing_artifacts": missing_artifacts,
        "v_t_gap_status": v_t_gap.get("status") if v_t_gap else None,
        "summary_status": summary.get("status"),
        "summary_success": summary.get("success"),
        "claim_boundary": "phase1_status_is_scenario_evaluation_not_natural_driving_claim",
    }


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
    if v_t_gap.get("status") == "warn":
        return "large_final_gap"
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
