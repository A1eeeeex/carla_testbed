from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.phase1_status import classify_phase1_run, write_phase1_status
from carla_testbed.analysis.v_t_gap import extract_v_t_gap, write_v_t_gap_report

PHASE1_POSTPROCESS_SCHEMA_VERSION = "phase1_postprocess.v1"


def run_phase1_postprocess(run_dir: str | Path) -> dict[str, Any]:
    """Write Phase 1 run-local analysis artifacts.

    This is intentionally backend-neutral. It consumes already-written run
    artifacts and does not change runtime behavior.
    """

    root = Path(run_dir).expanduser()
    analysis = root / "analysis"
    v_t_gap_report = extract_v_t_gap(run_dir=root)
    v_t_gap_paths = write_v_t_gap_report(v_t_gap_report, analysis / "v_t_gap")
    phase1_report = classify_phase1_run(root)
    phase1_paths = write_phase1_status(phase1_report, analysis / "phase1_status")
    completeness = build_phase1_artifact_completeness(root)
    completeness_path = analysis / "phase1_status" / "artifact_completeness.json"
    completeness_path.parent.mkdir(parents=True, exist_ok=True)
    completeness_path.write_text(json.dumps(completeness, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "schema_version": PHASE1_POSTPROCESS_SCHEMA_VERSION,
        "run_dir": str(root),
        "v_t_gap_status": v_t_gap_report.get("status"),
        "phase1_status": phase1_report.get("status"),
        "phase1_failure_reason": phase1_report.get("failure_reason"),
        "artifact_completeness_status": completeness.get("status"),
        "outputs": {
            "v_t_gap": v_t_gap_paths,
            "phase1_status": phase1_paths,
            "artifact_completeness": str(completeness_path),
        },
    }


def build_phase1_artifact_completeness(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    required = {
        "manifest": root / "manifest.json",
        "summary": root / "summary.json",
        "events": root / "events.jsonl",
        "timeseries": _first_existing(root / "timeseries.csv", root / "timeseries.jsonl"),
        "fixed_scene_resolved": root / "artifacts" / "fixed_scene_resolved.json",
        "fixed_scene_runtime_state": root / "artifacts" / "fixed_scene_runtime_state.json",
        "scenario_actor_trace": root / "artifacts" / "scenario_actor_trace.jsonl",
        "scenario_phase_events": root / "artifacts" / "scenario_phase_events.jsonl",
        "ego_control_trace": root / "artifacts" / "ego_control_trace.jsonl",
        "v_t_gap_report": root / "analysis" / "v_t_gap" / "v_t_gap_report.json",
        "phase1_status": root / "analysis" / "phase1_status" / "phase1_status.json",
    }
    artifacts: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for name, path in required.items():
        exists = bool(path and Path(path).exists())
        artifacts[name] = {"status": "present" if exists else "missing", "path": str(path) if path else None}
        if not exists:
            missing.append(name)
    return {
        "schema_version": "phase1_artifact_completeness.v1",
        "run_dir": str(root),
        "status": "pass" if not missing else "invalid",
        "missing_artifacts": missing,
        "artifacts": artifacts,
        "claim_boundary": "artifact completeness supports Phase 1 run evaluability only, not natural-driving capability.",
    }


def _first_existing(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None
