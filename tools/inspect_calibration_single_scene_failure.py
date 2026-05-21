#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def load_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""


def safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _delta_sec(start: Any, end: Any) -> Optional[float]:
    start_f = safe_float(start)
    end_f = safe_float(end)
    if start_f is None or end_f is None:
        return None
    return end_f - start_f


def _resolve_run_dir(run_dir: Path) -> Path:
    redirect = run_dir / "RUN_DIR_REDIRECT.txt"
    if redirect.exists():
        target = Path(redirect.read_text(encoding="utf-8").strip())
        if target.exists():
            return target
    latest = run_dir.parent / "LATEST.txt"
    if latest.exists():
        target = Path(latest.read_text(encoding="utf-8").strip())
        if target.exists() and target.parent == run_dir.parent:
            if target == run_dir or target.name.startswith(f"{run_dir.name}__"):
                return target
    candidates = sorted(
        (path for path in run_dir.parent.glob(f"{run_dir.name}__*") if path.is_dir()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        if (candidate / "effective.yaml").exists() or (candidate / "artifacts").exists():
            return candidate
    return run_dir


def _find_capture_detail(summary: Dict[str, Any], scene_name: str) -> Dict[str, Any]:
    captures = summary.get("capture_summary", {}).get("captures", [])
    if isinstance(captures, list):
        for item in captures:
            if isinstance(item, dict) and str(item.get("capture_id") or "").strip() == scene_name:
                return item
        for item in captures:
            if isinstance(item, dict):
                return item
    return {}


def _timeline_row(rows: List[Dict[str, Any]], stage: str) -> Dict[str, Any]:
    for row in rows:
        if str(row.get("stage") or "").strip() == stage:
            return row
    return {}


def _last_nonempty_line(text: str) -> str:
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line:
            return line
    return ""


def _extract_bool_line(text: str, prefix: str) -> Optional[bool]:
    for line in text.splitlines():
        if line.startswith(prefix):
            value = line.split(":", 1)[1].strip().lower()
            if value == "true":
                return True
            if value == "false":
                return False
    return None


def _extract_timeout_sec(text: str) -> Optional[float]:
    match = re.search(r"timed out after ([0-9]+(?:\.[0-9]+)?)s", text)
    return float(match.group(1)) if match else None


def analyze_pipeline_run(run_dir: Path, scene_name: str) -> Dict[str, Any]:
    manifest = load_json(run_dir / "artifacts" / "unified_calibration_manifest.json")
    capture_detail = _find_capture_detail(manifest, scene_name)
    suite_dir = run_dir / "suite"
    base_scene_dir = suite_dir / "scenes" / scene_name
    resolved_scene_dir = Path(str(capture_detail.get("run_dir") or "")).expanduser()
    if not resolved_scene_dir.exists():
        resolved_scene_dir = _resolve_run_dir(base_scene_dir)
    startup_rows = load_jsonl_rows(resolved_scene_dir / "artifacts" / "startup_stage_timeline.jsonl")
    world_ready_summary = load_json(resolved_scene_dir / "artifacts" / "carla_world_ready_summary.json")
    doctor_text = load_text(base_scene_dir / "artifacts" / "doctor.txt")
    scene_log_path = Path(str(capture_detail.get("log_path") or suite_dir / "logs" / f"{scene_name}.log"))
    scene_log_text = load_text(scene_log_path)
    followstop_stdout = load_text(base_scene_dir / "artifacts" / "followstop_child.stdout.log")
    followstop_stderr = load_text(base_scene_dir / "artifacts" / "followstop_child.stderr.log")
    failure_summary = str(capture_detail.get("failure_summary") or "")

    run_initialized_row = _timeline_row(startup_rows, "run_initialized")
    carla_launch_start_row = _timeline_row(startup_rows, "carla_launch_start")
    carla_wait_ready_ok_row = _timeline_row(startup_rows, "carla_wait_ready_ok")
    carla_get_world_start_row = _timeline_row(startup_rows, "carla_get_world_start")
    carla_get_world_ok_row = _timeline_row(startup_rows, "carla_get_world_ok")

    initial_carla_reachable = _extract_bool_line(doctor_text, "reachable:")
    scene_timeout_sec = _extract_timeout_sec(failure_summary)
    wait_ready_elapsed_sec = None
    wait_ready_attempts = world_ready_summary.get("wait_ready_attempts")
    if isinstance(wait_ready_attempts, list) and wait_ready_attempts:
        wait_ready_elapsed_sec = safe_float(wait_ready_attempts[-1].get("elapsed_sec"))

    carla_wait_ready_ok = bool(carla_wait_ready_ok_row)
    carla_get_world_started = bool(carla_get_world_start_row)
    carla_get_world_ok = bool(carla_get_world_ok_row)
    world_ready_reached = str(world_ready_summary.get("status") or "").strip() == "world_ready"
    startup_summary_status = str(world_ready_summary.get("status") or "").strip()
    summary_exists = (resolved_scene_dir / "summary.json").exists()
    metadata_exists = (resolved_scene_dir / "artifacts" / "scenario_metadata.json").exists()
    raw_control_exists = (resolved_scene_dir / "artifacts" / "apollo_control_raw.jsonl").exists()
    redirect_present = (base_scene_dir / "RUN_DIR_REDIRECT.txt").exists()
    keyboard_interrupt_seen = "KeyboardInterrupt" in scene_log_text
    followstop_child_started = "[carla] launching:" in followstop_stdout

    highest_confirmed_stage = "SCENE_START"
    if carla_wait_ready_ok:
        highest_confirmed_stage = "CARLA_WAIT_READY_OK"
    if carla_get_world_started:
        highest_confirmed_stage = "CARLA_GET_WORLD_STARTED"
    if carla_get_world_ok:
        highest_confirmed_stage = "CARLA_GET_WORLD_OK"
    if world_ready_reached:
        highest_confirmed_stage = "WORLD_READY"
    if summary_exists or metadata_exists or raw_control_exists:
        highest_confirmed_stage = "POST_WORLD_READY_CAPTURE"

    root_blocker_family = "scene_timeout_before_carla_ready"
    if carla_wait_ready_ok and not carla_get_world_started:
        root_blocker_family = "scene_timeout_between_carla_wait_ready_and_get_world"
    if carla_get_world_started and not carla_get_world_ok and not world_ready_reached:
        root_blocker_family = "scene_timeout_during_carla_get_world"
    elif world_ready_reached and not raw_control_exists:
        root_blocker_family = "world_ready_without_capture_artifacts"
    elif raw_control_exists:
        root_blocker_family = "capture_artifacts_present"

    blocker_chain: List[str] = []
    if initial_carla_reachable is False:
        blocker_chain.append("carla_port_initially_unreachable")
    if followstop_child_started:
        blocker_chain.append("followstop_child_started")
    if carla_wait_ready_ok:
        blocker_chain.append("carla_wait_ready_ok")
    if carla_get_world_started:
        blocker_chain.append("carla_get_world_start")
    if not carla_get_world_ok:
        blocker_chain.append("carla_get_world_ok_missing")
    if not world_ready_reached:
        blocker_chain.append("world_ready_missing")
    if keyboard_interrupt_seen:
        blocker_chain.append("scene_timeout_sigint_propagated")
    if not summary_exists:
        blocker_chain.append("summary_missing")
    if not metadata_exists:
        blocker_chain.append("scenario_metadata_missing")
    if not raw_control_exists:
        blocker_chain.append("raw_control_missing")
    if int(capture_detail.get("exit_code") or 0) != 0:
        blocker_chain.append(f"scene_exit_code_{capture_detail.get('exit_code')}")
    if int(manifest.get("capture_summary", {}).get("valid_capture_count") or 0) == 0:
        blocker_chain.append("valid_capture_count_zero")

    start_ts = run_initialized_row.get("ts_sec") or carla_launch_start_row.get("ts_sec")
    get_world_budget_remaining_sec = None
    if scene_timeout_sec is not None:
        elapsed_to_get_world = _delta_sec(start_ts, carla_get_world_start_row.get("ts_sec"))
        if elapsed_to_get_world is not None:
            get_world_budget_remaining_sec = scene_timeout_sec - elapsed_to_get_world

    blocker_analysis = {
        "highest_confirmed_stage": highest_confirmed_stage,
        "root_blocker_family": root_blocker_family,
        "terminal_blocker_family": blocker_chain[-1] if blocker_chain else root_blocker_family,
        "blocker_chain": blocker_chain,
        "blocker_summary": " -> ".join(blocker_chain) if blocker_chain else root_blocker_family,
        "initial_carla_reachable": initial_carla_reachable,
        "followstop_child_started": followstop_child_started,
        "carla_wait_ready_ok": carla_wait_ready_ok,
        "carla_get_world_started": carla_get_world_started,
        "carla_get_world_ok": carla_get_world_ok,
        "world_ready_reached": world_ready_reached,
        "summary_exists": summary_exists,
        "scenario_metadata_exists": metadata_exists,
        "raw_control_exists": raw_control_exists,
        "redirect_present": redirect_present,
        "keyboard_interrupt_seen": keyboard_interrupt_seen,
        "scene_timeout_sec": scene_timeout_sec,
        "wait_ready_elapsed_sec": wait_ready_elapsed_sec,
        "timing_summary": {
            "launch_to_wait_ready_ok_sec": _delta_sec(start_ts, carla_wait_ready_ok_row.get("ts_sec")),
            "wait_ready_ok_to_get_world_start_sec": _delta_sec(
                carla_wait_ready_ok_row.get("ts_sec"),
                carla_get_world_start_row.get("ts_sec"),
            ),
            "remaining_scene_budget_after_get_world_start_sec": get_world_budget_remaining_sec,
        },
        "failure_summary_excerpt": failure_summary.splitlines()[0].strip() if failure_summary else "",
        "scene_log_last_line": _last_nonempty_line(scene_log_text),
        "followstop_stdout_last_line": _last_nonempty_line(followstop_stdout),
        "followstop_stderr_last_line": _last_nonempty_line(followstop_stderr),
        "invalid_reason": str(capture_detail.get("invalid_reason") or ""),
        "recommended_next_focus": (
            "reuse a ready Town01 session or instrument the fresh scene path around carla.Client.get_world before routing/capture debugging"
            if root_blocker_family == "scene_timeout_during_carla_get_world"
            else "inspect why the calibration scene still fails before valid capture artifacts appear"
        ),
        "evidence_paths": {
            "unified_manifest": str(run_dir / "artifacts" / "unified_calibration_manifest.json"),
            "scene_log": str(scene_log_path),
            "resolved_scene_dir": str(resolved_scene_dir),
            "base_scene_dir": str(base_scene_dir),
            "startup_stage_timeline": str(resolved_scene_dir / "artifacts" / "startup_stage_timeline.jsonl"),
            "carla_world_ready_summary": str(resolved_scene_dir / "artifacts" / "carla_world_ready_summary.json"),
            "doctor": str(base_scene_dir / "artifacts" / "doctor.txt"),
            "followstop_stdout": str(base_scene_dir / "artifacts" / "followstop_child.stdout.log"),
            "followstop_stderr": str(base_scene_dir / "artifacts" / "followstop_child.stderr.log"),
        },
    }

    return {
        "run_dir": str(run_dir),
        "scene_name": scene_name,
        "scene_run_dir": str(resolved_scene_dir),
        "startup_snapshot": {
            "startup_stage_last": str(startup_rows[-1].get("stage") or "") if startup_rows else "",
            "startup_stage_count": len(startup_rows),
            "carla_world_ready_status": startup_summary_status,
            "carla_world_ready_final_town": world_ready_summary.get("final_town"),
            "scene_timeout_sec": scene_timeout_sec,
            "wait_ready_elapsed_sec": wait_ready_elapsed_sec,
            "scene_exit_code": capture_detail.get("exit_code"),
            "valid_capture_count": manifest.get("capture_summary", {}).get("valid_capture_count"),
            "invalid_capture_count": manifest.get("capture_summary", {}).get("invalid_capture_count"),
        },
        "blocker_analysis": blocker_analysis,
    }


def render_report(payload: Dict[str, Any]) -> str:
    startup_snapshot = payload.get("startup_snapshot", {})
    blocker = payload.get("blocker_analysis", {})
    return "\n".join(
        [
            "# Calibration Single-Scene Failure Inspection",
            "",
            f"- run_dir: `{payload.get('run_dir')}`",
            f"- scene_name: `{payload.get('scene_name')}`",
            f"- scene_run_dir: `{payload.get('scene_run_dir')}`",
            f"- startup_stage_last: `{startup_snapshot.get('startup_stage_last')}`",
            f"- startup_stage_count: `{startup_snapshot.get('startup_stage_count')}`",
            f"- carla_world_ready_status: `{startup_snapshot.get('carla_world_ready_status')}`",
            f"- carla_world_ready_final_town: `{startup_snapshot.get('carla_world_ready_final_town')}`",
            f"- scene_timeout_sec: `{startup_snapshot.get('scene_timeout_sec')}`",
            f"- wait_ready_elapsed_sec: `{startup_snapshot.get('wait_ready_elapsed_sec')}`",
            f"- scene_exit_code: `{startup_snapshot.get('scene_exit_code')}`",
            f"- valid_capture_count: `{startup_snapshot.get('valid_capture_count')}`",
            f"- invalid_capture_count: `{startup_snapshot.get('invalid_capture_count')}`",
            "",
            "## Blocker Chain",
            "",
            f"- highest_confirmed_stage: `{blocker.get('highest_confirmed_stage')}`",
            f"- root_blocker_family: `{blocker.get('root_blocker_family')}`",
            f"- terminal_blocker_family: `{blocker.get('terminal_blocker_family')}`",
            f"- blocker_summary: `{blocker.get('blocker_summary')}`",
            f"- initial_carla_reachable: `{blocker.get('initial_carla_reachable')}`",
            f"- redirect_present: `{blocker.get('redirect_present')}`",
            f"- followstop_child_started: `{blocker.get('followstop_child_started')}`",
            f"- carla_wait_ready_ok: `{blocker.get('carla_wait_ready_ok')}`",
            f"- carla_get_world_started: `{blocker.get('carla_get_world_started')}`",
            f"- carla_get_world_ok: `{blocker.get('carla_get_world_ok')}`",
            f"- world_ready_reached: `{blocker.get('world_ready_reached')}`",
            f"- summary_exists: `{blocker.get('summary_exists')}`",
            f"- scenario_metadata_exists: `{blocker.get('scenario_metadata_exists')}`",
            f"- raw_control_exists: `{blocker.get('raw_control_exists')}`",
            f"- keyboard_interrupt_seen: `{blocker.get('keyboard_interrupt_seen')}`",
            f"- invalid_reason: `{blocker.get('invalid_reason')}`",
            f"- failure_summary_excerpt: `{blocker.get('failure_summary_excerpt')}`",
            f"- launch_to_wait_ready_ok_sec: `{((blocker.get('timing_summary') or {}).get('launch_to_wait_ready_ok_sec'))}`",
            f"- wait_ready_ok_to_get_world_start_sec: `{((blocker.get('timing_summary') or {}).get('wait_ready_ok_to_get_world_start_sec'))}`",
            f"- remaining_scene_budget_after_get_world_start_sec: `{((blocker.get('timing_summary') or {}).get('remaining_scene_budget_after_get_world_start_sec'))}`",
            f"- followstop_stdout_last_line: `{blocker.get('followstop_stdout_last_line')}`",
            f"- scene_log_last_line: `{blocker.get('scene_log_last_line')}`",
            f"- recommended_next_focus: `{blocker.get('recommended_next_focus')}`",
            "",
            "## Evidence Paths",
            "",
            f"- unified_manifest: `{((blocker.get('evidence_paths') or {}).get('unified_manifest'))}`",
            f"- scene_log: `{((blocker.get('evidence_paths') or {}).get('scene_log'))}`",
            f"- resolved_scene_dir: `{((blocker.get('evidence_paths') or {}).get('resolved_scene_dir'))}`",
            f"- base_scene_dir: `{((blocker.get('evidence_paths') or {}).get('base_scene_dir'))}`",
            f"- startup_stage_timeline: `{((blocker.get('evidence_paths') or {}).get('startup_stage_timeline'))}`",
            f"- carla_world_ready_summary: `{((blocker.get('evidence_paths') or {}).get('carla_world_ready_summary'))}`",
            f"- doctor: `{((blocker.get('evidence_paths') or {}).get('doctor'))}`",
            f"- followstop_stdout: `{((blocker.get('evidence_paths') or {}).get('followstop_stdout'))}`",
            f"- followstop_stderr: `{((blocker.get('evidence_paths') or {}).get('followstop_stderr'))}`",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect calibration single-scene bounded failure runs")
    parser.add_argument("--run-dir", required=True, help="Unified calibration pipeline run directory")
    parser.add_argument("--scene-name", default="lat_straight_track", help="Scene name to inspect")
    parser.add_argument("--report", help="Optional markdown report output path")
    parser.add_argument("--json-output", help="Optional JSON output path")
    args = parser.parse_args()

    payload = analyze_pipeline_run(Path(args.run_dir).expanduser().resolve(), args.scene_name)
    if args.report:
        Path(args.report).expanduser().resolve().write_text(render_report(payload), encoding="utf-8")
    if args.json_output:
        Path(args.json_output).expanduser().resolve().write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
