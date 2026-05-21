#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import _compute_route_metrics, load_json, safe_float


def _load_debug_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _last_row_snapshot(debug_rows: List[Dict[str, str]]) -> Dict[str, Any]:
    if not debug_rows:
        return {}
    last = debug_rows[-1]
    return {
        "ts_sec": safe_float(last.get("ts_sec")),
        "speed_mps": safe_float(last.get("speed_mps")),
        "apollo_desired_steer": safe_float(last.get("apollo_desired_steer")),
        "commanded_steer": safe_float(last.get("commanded_steer")),
        "commanded_throttle": safe_float(last.get("commanded_throttle")),
        "commanded_brake": safe_float(last.get("commanded_brake")),
        "planning_lateral_latest_sequence_num": last.get("planning_lateral_latest_sequence_num"),
        "planning_lateral_latest_point_count": last.get("planning_lateral_latest_point_count"),
        "trajectory_contract_lateral_guard_applied": last.get("trajectory_contract_lateral_guard_applied"),
    }


def _load_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _load_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _extract_prefixed_value(text: str, prefix: str) -> str:
    for line in text.splitlines():
        if line.startswith(prefix):
            return line.split(":", 1)[1].strip()
    return ""


def _extract_section_text(text: str, header: str, stop_headers: List[str]) -> str:
    lines = text.splitlines()
    capture = False
    captured: List[str] = []
    for line in lines:
        stripped = line.strip()
        if capture and stripped in stop_headers:
            break
        if capture:
            captured.append(line)
            continue
        if stripped == header:
            capture = True
    return "\n".join(captured).strip()


def _safe_int(value: Any) -> int | None:
    parsed = safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _resolve_route_id(run_dir: Path, summary_data: Dict[str, Any], scenario_meta: Dict[str, Any]) -> str:
    route_id = str(scenario_meta.get("route_id") or summary_data.get("route_id") or "").strip()
    if route_id:
        return route_id
    for token in run_dir.name.split("__"):
        token = token.strip()
        if token.startswith("town01_"):
            return token
    return ""


def _candidate_related_run_dirs(run_dir: Path) -> List[Path]:
    candidates: List[Path] = [run_dir]
    match = re.match(r"^(?P<base>.+)__\d+$", run_dir.name)
    if match:
        base_dir = run_dir.parent / match.group("base")
        if base_dir.exists() and base_dir not in candidates:
            candidates.append(base_dir)
    return candidates


def _find_related_artifact(run_dir: Path, artifact_name: str) -> Path | None:
    for candidate_dir in _candidate_related_run_dirs(run_dir):
        candidate = candidate_dir / "artifacts" / artifact_name
        if candidate.exists():
            return candidate
    return None


def _startup_snapshot(
    run_dir: Path,
    summary_data: Dict[str, Any],
    debug_path: Path,
    startup_timeline: List[Dict[str, Any]],
    carla_world_ready_summary: Dict[str, Any],
) -> Dict[str, Any]:
    startup_stage = load_json(run_dir / "artifacts" / "startup_stage.json")
    acceptance = summary_data.get("acceptance") or {}
    dreamview = summary_data.get("dreamview_recording") or {}
    return {
        "debug_timeseries_available": debug_path.exists(),
        "startup_stage": startup_stage.get("stage"),
        "startup_stage_ts_sec": safe_float(startup_stage.get("ts_sec")),
        "startup_timeline_last_stage": (startup_timeline[-1].get("stage") if startup_timeline else None),
        "startup_timeline_last_error": (startup_timeline[-1].get("error") if startup_timeline else None),
        "carla_world_ready_status": carla_world_ready_summary.get("status"),
        "carla_world_ready_current_town": carla_world_ready_summary.get("final_town")
        or carla_world_ready_summary.get("current_town_before_load"),
        "summary_status": summary_data.get("summary_status"),
        "summary_fail_reason": acceptance.get("fail_reason"),
        "summary_failure_stage": acceptance.get("failure_stage"),
        "dreamview_recording_status": dreamview.get("recording_status"),
    }


def _followstop_stderr_signatures(stderr_text: str) -> List[str]:
    signatures: List[str] = []
    checks = (
        ("Town01RouteHealthScenario.build", "town01_route_health_build_frame"),
        ("ego.set_simulate_physics(True)", "ego_set_simulate_physics"),
        (
            "RuntimeError: time-out of 30000ms while waiting for the simulator",
            "simulator_rpc_timeout_30000ms",
        ),
    )
    for needle, label in checks:
        if needle in stderr_text:
            signatures.append(label)
    return signatures


def _acceptance_check_actual(summary_data: Dict[str, Any], key: str) -> Any:
    acceptance = summary_data.get("acceptance") or {}
    checks = acceptance.get("checks") or {}
    payload = checks.get(key) or {}
    if isinstance(payload, dict):
        return payload.get("actual")
    return None


def _acceptance_check_payload(summary_data: Dict[str, Any], key: str) -> Dict[str, Any]:
    acceptance = summary_data.get("acceptance") or {}
    checks = acceptance.get("checks") or {}
    payload = checks.get(key) or {}
    if isinstance(payload, dict):
        return payload
    return {}


def _trace_row_by_step(rows: List[Dict[str, Any]], step: str) -> Dict[str, Any]:
    return next((row for row in rows if str(row.get("step") or "").strip() == step), {})


def _timeline_row_by_stage(rows: List[Dict[str, Any]], stage: str) -> Dict[str, Any]:
    return next((row for row in rows if str(row.get("stage") or "").strip() == stage), {})


def _delta_sec(start_ts: Any, end_ts: Any) -> float | None:
    start = safe_float(start_ts)
    end = safe_float(end_ts)
    if start is None or end is None:
        return None
    return max(0.0, end - start)


def _blocker_analysis(
    *,
    run_dir: Path,
    summary_data: Dict[str, Any],
    startup_timeline: List[Dict[str, Any]],
    apollo_backend_trace: List[Dict[str, Any]],
    carla_world_ready_summary: Dict[str, Any],
    debug_path: Path,
) -> Dict[str, Any]:
    startup_stages = [str(row.get("stage") or "").strip() for row in startup_timeline if str(row.get("stage") or "").strip()]
    apollo_steps = [str(row.get("step") or "").strip() for row in apollo_backend_trace if str(row.get("step") or "").strip()]
    apollo_timeout_row = _trace_row_by_step(apollo_backend_trace, "docker_start_modules_timeout")
    carla_get_world_ok_row = _timeline_row_by_stage(startup_timeline, "carla_get_world_ok")
    adapter_prepare_done_row = _timeline_row_by_stage(startup_timeline, "adapter_prepare_done")
    scenario_build_start_row = _timeline_row_by_stage(startup_timeline, "scenario_build_start")
    followstop_stderr_path = _find_related_artifact(run_dir, "followstop_child.stderr.log")
    followstop_stderr_text = _load_text(followstop_stderr_path) if followstop_stderr_path is not None else ""
    followstop_signatures = _followstop_stderr_signatures(followstop_stderr_text)
    apollo_modules_start_log_path = run_dir / "artifacts" / "apollo_modules_start.log"
    apollo_modules_start_log_text = _load_text(apollo_modules_start_log_path)
    apollo_mainboard_runtime_check_path = run_dir / "artifacts" / "apollo_mainboard_runtime_check.log"
    apollo_mainboard_runtime_check_text = _load_text(apollo_mainboard_runtime_check_path)
    route_health = summary_data.get("route_health") or {}
    route_health_label = str(summary_data.get("route_health_label") or route_health.get("label") or "").strip()
    acceptance = summary_data.get("acceptance") or {}
    acceptance_failure_codes = [
        str(item).strip()
        for item in list(acceptance.get("failure_codes") or [])
        if str(item).strip()
    ]
    routing_request_count = _safe_int(_acceptance_check_actual(summary_data, "routing_request_count"))
    planning_nonzero_ratio = safe_float(_acceptance_check_actual(summary_data, "planning_nonzero_ratio"))
    control_used_planning_ratio = safe_float(_acceptance_check_actual(summary_data, "control_used_planning_ratio"))
    route_establishment_latency_sec = safe_float(_acceptance_check_actual(summary_data, "route_establishment_latency_sec"))
    route_distance_check = _acceptance_check_payload(summary_data, "route_distance_achieved_m")
    route_completion_check = _acceptance_check_payload(summary_data, "route_completion_ratio")
    max_speed_check = _acceptance_check_payload(summary_data, "max_speed_mps")
    low_speed_creep_check = _acceptance_check_payload(summary_data, "low_speed_creep_duration_sec")
    lateral_metrics = _acceptance_check_payload(summary_data, "lateral_metrics")
    planning_trajectory_type_summary = _acceptance_check_payload(summary_data, "planning_trajectory_type_summary")
    apollo_modules_start_log_returncode = _extract_prefixed_value(apollo_modules_start_log_text, "returncode:")
    apollo_modules_start_stdout = _extract_section_text(
        apollo_modules_start_log_text,
        "--- stdout ---",
        ["--- stderr ---"],
    )
    apollo_modules_start_stderr = _extract_section_text(
        apollo_modules_start_log_text,
        "--- stderr ---",
        [],
    )
    apollo_mainboard_runtime_check_returncode = _extract_prefixed_value(
        apollo_mainboard_runtime_check_text,
        "returncode:",
    )
    apollo_mainboard_fallback_ldd_returncode = _extract_prefixed_value(
        apollo_mainboard_runtime_check_text,
        "fallback_returncode:",
    )
    apollo_modules_start_stdout_nonempty = bool(apollo_modules_start_stdout.strip())
    apollo_modules_start_stderr_nonempty = bool(apollo_modules_start_stderr.strip())
    apollo_mainboard_runtime_check_timeout = apollo_mainboard_runtime_check_returncode == "timeout"
    apollo_mainboard_fallback_ldd_timeout = apollo_mainboard_fallback_ldd_returncode == "timeout"

    world_ready_reached = bool(
        str(carla_world_ready_summary.get("status") or "").strip() == "world_ready"
        or "carla_get_world_ok" in startup_stages
    )
    scenario_build_started = "scenario_build_start" in startup_stages
    scenario_build_completed = "scenario_build_done" in startup_stages
    apollo_modules_start_timeout = bool(apollo_timeout_row) or any(
        stage == "adapter_start_exception" and "timed out" in str(row.get("error") or "").lower()
        for stage, row in ((str(item.get("stage") or "").strip(), item) for item in startup_timeline)
    )
    scenario_build_simulator_timeout = "simulator_rpc_timeout_30000ms" in followstop_signatures
    route_established = bool(routing_request_count and routing_request_count > 0)
    route_distance_below_threshold = route_distance_check.get("ok") is False
    route_completion_below_threshold = route_completion_check.get("ok") is False
    max_speed_below_threshold = max_speed_check.get("ok") is False
    low_speed_creep_exceeded_threshold = low_speed_creep_check.get("ok") is False
    path_fallback_count = _safe_int(planning_trajectory_type_summary.get("path_fallback_count")) or 0
    persistent_path_fallback_at_end = bool(planning_trajectory_type_summary.get("persistent_path_fallback_at_end"))
    last_trajectory_type = str(planning_trajectory_type_summary.get("last_trajectory_type") or "").strip()
    raw_steer_nonzero_ratio = safe_float(lateral_metrics.get("raw_steer_nonzero_ratio"))
    commanded_steer_nonzero_ratio = safe_float(lateral_metrics.get("commanded_steer_nonzero_ratio"))
    force_zero_steer_applied_count = _safe_int(lateral_metrics.get("force_zero_steer_applied_count")) or 0
    commanded_steer_zero_while_raw_nonzero = bool(
        raw_steer_nonzero_ratio is not None
        and raw_steer_nonzero_ratio > 0.5
        and commanded_steer_nonzero_ratio == 0.0
        and force_zero_steer_applied_count > 0
    )

    highest_confirmed_stage = "PRE_WORLD_READY"
    if world_ready_reached:
        highest_confirmed_stage = "WORLD_READY"
    if "adapter_prepare_done" in startup_stages:
        highest_confirmed_stage = "APOLLO_ADAPTER_PREPARED"
    if scenario_build_started:
        highest_confirmed_stage = "SCENARIO_BUILD_STARTED"
    if scenario_build_completed:
        highest_confirmed_stage = "SCENARIO_BUILD_DONE"
    if route_established:
        highest_confirmed_stage = "ROUTING_READY"

    apollo_start_subfamily = ""
    if apollo_modules_start_timeout and apollo_mainboard_runtime_check_timeout and not (
        apollo_modules_start_stdout_nonempty or apollo_modules_start_stderr_nonempty
    ):
        apollo_start_subfamily = "mainboard_runtime_check_timeout_before_module_output"
    elif apollo_modules_start_timeout and not (
        apollo_modules_start_stdout_nonempty or apollo_modules_start_stderr_nonempty
    ):
        apollo_start_subfamily = "module_start_timeout_without_output"
    elif apollo_modules_start_timeout:
        apollo_start_subfamily = "module_start_timeout_with_partial_output"

    root_blocker_family = "pre_world_ready_startup_blocked"
    if world_ready_reached:
        root_blocker_family = "post_world_ready_route_establishment_absent"
    if world_ready_reached and apollo_mainboard_runtime_check_timeout:
        root_blocker_family = "post_world_ready_apollo_mainboard_runtime_timeout"
    elif world_ready_reached and apollo_modules_start_timeout:
        root_blocker_family = "post_world_ready_apollo_modules_start_timeout"
    elif world_ready_reached and scenario_build_simulator_timeout:
        root_blocker_family = "post_world_ready_scenario_build_simulator_timeout"
    elif route_established and route_health_label:
        root_blocker_family = route_health_label
    elif route_established:
        root_blocker_family = "route_established"

    blocker_chain: List[str] = []
    if world_ready_reached:
        blocker_chain.append("world_ready_reached")
    if apollo_mainboard_runtime_check_timeout:
        blocker_chain.append("apollo_mainboard_runtime_check_timeout")
    if apollo_modules_start_timeout:
        blocker_chain.append("apollo_modules_start_timeout")
    if scenario_build_simulator_timeout:
        blocker_chain.append("scenario_build_simulator_timeout")
    if routing_request_count == 0:
        blocker_chain.append("routing_request_count_zero")
    if route_established:
        blocker_chain.append("route_established")
    if planning_nonzero_ratio == 0.0:
        blocker_chain.append("planning_nonzero_ratio_zero")
    if route_distance_below_threshold:
        blocker_chain.append("route_distance_below_threshold")
    if route_completion_below_threshold:
        blocker_chain.append("route_completion_below_threshold")
    if max_speed_below_threshold:
        blocker_chain.append("max_speed_below_threshold")
    if low_speed_creep_exceeded_threshold:
        blocker_chain.append("low_speed_creep_exceeded_threshold")
    if persistent_path_fallback_at_end:
        blocker_chain.append("persistent_path_fallback_at_end")
    if last_trajectory_type == "PATH_FALLBACK":
        blocker_chain.append("ended_in_path_fallback")
    if commanded_steer_zero_while_raw_nonzero:
        blocker_chain.append("commanded_steer_zero_while_raw_nonzero")
    if not debug_path.exists():
        blocker_chain.append("debug_timeseries_missing")

    terminal_blocker_family = blocker_chain[-1] if blocker_chain else root_blocker_family
    followstop_last_line = ""
    for line in reversed(followstop_stderr_text.splitlines()):
        line = line.strip()
        if line:
            followstop_last_line = line
            break

    recommended_next_focus = (
        "inspect Apollo mainboard/runtime stall, then Town01RouteHealthScenario.build simulator RPC readiness before deeper routing/planning debugging"
        if apollo_mainboard_runtime_check_timeout
        else (
            "inspect Apollo modules start latency and Town01RouteHealthScenario.build simulator RPC readiness before deeper routing/planning debugging"
            if apollo_modules_start_timeout or scenario_build_simulator_timeout
            else (
                "inspect post-routing persistent path-fallback and zero-steer masking after route establishment"
                if route_established and persistent_path_fallback_at_end and commanded_steer_zero_while_raw_nonzero
                else (
                    "inspect why established routing still collapses into persistent path-fallback during cruise"
                    if route_established and persistent_path_fallback_at_end
                    else (
                        "inspect why established routing fails behavioral acceptance during cruise"
                        if route_established and route_health_label == "route_established_but_behavior_unhealthy"
                        else "inspect why routing requests stay absent after world_ready"
                    )
                )
            )
        )
    )

    timing_summary = {
        "world_ready_to_adapter_prepare_done_sec": _delta_sec(
            carla_get_world_ok_row.get("ts_sec"),
            adapter_prepare_done_row.get("ts_sec"),
        ),
        "adapter_prepare_done_to_apollo_modules_timeout_sec": _delta_sec(
            adapter_prepare_done_row.get("ts_sec"),
            apollo_timeout_row.get("ts_sec"),
        ),
        "apollo_modules_timeout_to_scenario_build_start_sec": _delta_sec(
            apollo_timeout_row.get("ts_sec"),
            scenario_build_start_row.get("ts_sec"),
        ),
    }

    return {
        "highest_confirmed_stage": highest_confirmed_stage,
        "root_blocker_family": root_blocker_family,
        "terminal_blocker_family": terminal_blocker_family,
        "blocker_chain": blocker_chain,
        "blocker_summary": " -> ".join(blocker_chain) if blocker_chain else root_blocker_family,
        "world_ready_reached": world_ready_reached,
        "apollo_start_subfamily": apollo_start_subfamily,
        "apollo_mainboard_runtime_check_timeout": apollo_mainboard_runtime_check_timeout,
        "apollo_mainboard_runtime_check_returncode": apollo_mainboard_runtime_check_returncode or "",
        "apollo_mainboard_fallback_ldd_timeout": apollo_mainboard_fallback_ldd_timeout,
        "apollo_mainboard_fallback_ldd_returncode": apollo_mainboard_fallback_ldd_returncode or "",
        "apollo_modules_start_timeout": apollo_modules_start_timeout,
        "apollo_modules_start_timeout_sec": safe_float(apollo_timeout_row.get("timeout_sec")),
        "apollo_modules_start_log_returncode": apollo_modules_start_log_returncode or "",
        "apollo_modules_start_stdout_nonempty": apollo_modules_start_stdout_nonempty,
        "apollo_modules_start_stderr_nonempty": apollo_modules_start_stderr_nonempty,
        "apollo_backend_last_step": apollo_steps[-1] if apollo_steps else None,
        "timing_summary": timing_summary,
        "scenario_build_started": scenario_build_started,
        "scenario_build_completed": scenario_build_completed,
        "scenario_build_simulator_timeout": scenario_build_simulator_timeout,
        "routing_request_count": routing_request_count,
        "route_established": route_established,
        "route_health_label": route_health_label,
        "acceptance_failure_codes": acceptance_failure_codes,
        "route_establishment_latency_sec": route_establishment_latency_sec,
        "planning_nonzero_ratio": planning_nonzero_ratio,
        "control_used_planning_ratio": control_used_planning_ratio,
        "route_distance_below_threshold": route_distance_below_threshold,
        "route_completion_below_threshold": route_completion_below_threshold,
        "max_speed_below_threshold": max_speed_below_threshold,
        "low_speed_creep_exceeded_threshold": low_speed_creep_exceeded_threshold,
        "path_fallback_count": path_fallback_count,
        "persistent_path_fallback_at_end": persistent_path_fallback_at_end,
        "last_trajectory_type": last_trajectory_type,
        "raw_steer_nonzero_ratio": raw_steer_nonzero_ratio,
        "commanded_steer_nonzero_ratio": commanded_steer_nonzero_ratio,
        "force_zero_steer_applied_count": force_zero_steer_applied_count,
        "commanded_steer_zero_while_raw_nonzero": commanded_steer_zero_while_raw_nonzero,
        "followstop_stderr_signatures": followstop_signatures,
        "followstop_stderr_last_line": followstop_last_line,
        "evidence_paths": {
            "startup_timeline": str(run_dir / "artifacts" / "startup_stage_timeline.jsonl"),
            "apollo_backend_trace": str(run_dir / "artifacts" / "apollo_backend_startup_trace.jsonl"),
            "apollo_modules_start_log": str(run_dir / "artifacts" / "apollo_modules_start.log"),
            "apollo_mainboard_runtime_check_log": str(run_dir / "artifacts" / "apollo_mainboard_runtime_check.log"),
            "followstop_stderr": str(followstop_stderr_path) if followstop_stderr_path is not None else "",
            "carla_world_ready_summary": str(run_dir / "artifacts" / "carla_world_ready_summary.json"),
        },
        "recommended_next_focus": recommended_next_focus,
    }


def _render_report(
    run_dir: Path,
    route_metrics: Dict[str, Any],
    last_row: Dict[str, Any],
    startup_snapshot: Dict[str, Any],
    blocker_analysis: Dict[str, Any],
) -> str:
    scenario_meta = load_json(run_dir / "artifacts" / "scenario_metadata.json")
    summary_data = load_json(run_dir / "summary.json")
    route_id = _resolve_route_id(run_dir, summary_data, scenario_meta)
    return "\n".join(
        [
            "# Town01 Live Route Progress Probe",
            "",
            f"- run_dir: `{run_dir}`",
            f"- route_id: `{route_id}`",
            f"- comparison_label: `{summary_data.get('comparison_label') or scenario_meta.get('comparison_label') or ''}`",
            f"- debug_timeseries_available: `{startup_snapshot.get('debug_timeseries_available')}`",
            f"- summary_status: `{startup_snapshot.get('summary_status')}`",
            f"- summary_fail_reason: `{startup_snapshot.get('summary_fail_reason')}`",
            f"- summary_failure_stage: `{startup_snapshot.get('summary_failure_stage')}`",
            f"- startup_stage: `{startup_snapshot.get('startup_stage')}`",
            f"- startup_timeline_last_stage: `{startup_snapshot.get('startup_timeline_last_stage')}`",
            f"- startup_timeline_last_error: `{startup_snapshot.get('startup_timeline_last_error')}`",
            f"- carla_world_ready_status: `{startup_snapshot.get('carla_world_ready_status')}`",
            f"- carla_world_ready_current_town: `{startup_snapshot.get('carla_world_ready_current_town')}`",
            f"- dreamview_recording_status: `{startup_snapshot.get('dreamview_recording_status')}`",
            "",
            "## Blocker Chain",
            "",
            f"- highest_confirmed_stage: `{blocker_analysis.get('highest_confirmed_stage')}`",
            f"- root_blocker_family: `{blocker_analysis.get('root_blocker_family')}`",
            f"- terminal_blocker_family: `{blocker_analysis.get('terminal_blocker_family')}`",
            f"- blocker_summary: `{blocker_analysis.get('blocker_summary')}`",
            f"- apollo_start_subfamily: `{blocker_analysis.get('apollo_start_subfamily')}`",
            f"- apollo_mainboard_runtime_check_timeout: `{blocker_analysis.get('apollo_mainboard_runtime_check_timeout')}`",
            f"- apollo_mainboard_fallback_ldd_timeout: `{blocker_analysis.get('apollo_mainboard_fallback_ldd_timeout')}`",
            f"- apollo_modules_start_log_returncode: `{blocker_analysis.get('apollo_modules_start_log_returncode')}`",
            f"- apollo_modules_start_stdout_nonempty: `{blocker_analysis.get('apollo_modules_start_stdout_nonempty')}`",
            f"- apollo_modules_start_stderr_nonempty: `{blocker_analysis.get('apollo_modules_start_stderr_nonempty')}`",
            f"- routing_request_count: `{blocker_analysis.get('routing_request_count')}`",
            f"- route_establishment_latency_sec: `{blocker_analysis.get('route_establishment_latency_sec')}`",
            f"- planning_nonzero_ratio: `{blocker_analysis.get('planning_nonzero_ratio')}`",
            f"- control_used_planning_ratio: `{blocker_analysis.get('control_used_planning_ratio')}`",
            f"- route_health_label: `{blocker_analysis.get('route_health_label')}`",
            f"- acceptance_failure_codes: `{blocker_analysis.get('acceptance_failure_codes')}`",
            f"- path_fallback_count: `{blocker_analysis.get('path_fallback_count')}`",
            f"- persistent_path_fallback_at_end: `{blocker_analysis.get('persistent_path_fallback_at_end')}`",
            f"- last_trajectory_type: `{blocker_analysis.get('last_trajectory_type')}`",
            f"- commanded_steer_zero_while_raw_nonzero: `{blocker_analysis.get('commanded_steer_zero_while_raw_nonzero')}`",
            f"- world_ready_to_adapter_prepare_done_sec: `{((blocker_analysis.get('timing_summary') or {}).get('world_ready_to_adapter_prepare_done_sec'))}`",
            f"- adapter_prepare_done_to_apollo_modules_timeout_sec: `{((blocker_analysis.get('timing_summary') or {}).get('adapter_prepare_done_to_apollo_modules_timeout_sec'))}`",
            f"- apollo_modules_timeout_to_scenario_build_start_sec: `{((blocker_analysis.get('timing_summary') or {}).get('apollo_modules_timeout_to_scenario_build_start_sec'))}`",
            f"- followstop_stderr_signatures: `{', '.join(blocker_analysis.get('followstop_stderr_signatures') or []) or 'none'}`",
            f"- followstop_stderr_last_line: `{blocker_analysis.get('followstop_stderr_last_line')}`",
            f"- recommended_next_focus: `{blocker_analysis.get('recommended_next_focus')}`",
            "",
            "## Current Route Metrics",
            "",
            f"- route_distance_achieved_m: `{route_metrics.get('route_distance_achieved_m')}`",
            f"- route_completion_ratio: `{route_metrics.get('route_completion_ratio')}`",
            f"- final_goal_distance_m: `{route_metrics.get('final_goal_distance_m')}`",
            f"- route_length_target_m: `{route_metrics.get('route_length_target_m')}`",
            "",
            "## Latest Debug Row",
            "",
            f"- ts_sec: `{last_row.get('ts_sec')}`",
            f"- speed_mps: `{last_row.get('speed_mps')}`",
            f"- apollo_desired_steer: `{last_row.get('apollo_desired_steer')}`",
            f"- commanded_steer: `{last_row.get('commanded_steer')}`",
            f"- commanded_throttle: `{last_row.get('commanded_throttle')}`",
            f"- commanded_brake: `{last_row.get('commanded_brake')}`",
            f"- planning_lateral_latest_sequence_num: `{last_row.get('planning_lateral_latest_sequence_num')}`",
            f"- planning_lateral_latest_point_count: `{last_row.get('planning_lateral_latest_point_count')}`",
            f"- trajectory_contract_lateral_guard_applied: `{last_row.get('trajectory_contract_lateral_guard_applied')}`",
            "",
            "## Notes",
            "",
            (
                "- `debug_timeseries.csv` missing; this probe is reporting the finalized summary/startup state only."
                if not startup_snapshot.get("debug_timeseries_available")
                else "- `debug_timeseries.csv` available; latest row and route metrics include live route-state fields."
            ),
            "",
        ]
    ) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect live Town01 route progress from a running run directory.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--report", default="", help="Optional markdown report path.")
    parser.add_argument("--json-output", default="", help="Optional JSON output path.")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    debug_path = run_dir / "artifacts" / "debug_timeseries.csv"
    summary_data = load_json(run_dir / "summary.json")
    scenario_meta = load_json(run_dir / "artifacts" / "scenario_metadata.json")
    route_id = _resolve_route_id(run_dir, summary_data, scenario_meta)
    startup_timeline = _load_jsonl_rows(run_dir / "artifacts" / "startup_stage_timeline.jsonl")
    apollo_backend_trace = _load_jsonl_rows(run_dir / "artifacts" / "apollo_backend_startup_trace.jsonl")
    carla_world_ready_summary = load_json(run_dir / "artifacts" / "carla_world_ready_summary.json")
    debug_rows = _load_debug_rows(debug_path) if debug_path.exists() else []
    route_metrics = _compute_route_metrics(run_dir, summary_data, scenario_meta, debug_rows)
    last_row = _last_row_snapshot(debug_rows)
    startup_snapshot = _startup_snapshot(
        run_dir,
        summary_data,
        debug_path,
        startup_timeline,
        carla_world_ready_summary,
    )
    blocker_analysis = _blocker_analysis(
        run_dir=run_dir,
        summary_data=summary_data,
        startup_timeline=startup_timeline,
        apollo_backend_trace=apollo_backend_trace,
        carla_world_ready_summary=carla_world_ready_summary,
        debug_path=debug_path,
    )

    payload = {
        "run_dir": str(run_dir),
        "route_id": route_id,
        "comparison_label": summary_data.get("comparison_label") or scenario_meta.get("comparison_label"),
        "startup_snapshot": startup_snapshot,
        "blocker_analysis": blocker_analysis,
        "route_metrics": route_metrics,
        "last_debug_row": last_row,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    if args.report:
        report_path = Path(args.report).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            _render_report(run_dir, route_metrics, last_row, startup_snapshot, blocker_analysis),
            encoding="utf-8",
        )
    if args.json_output:
        json_output_path = Path(args.json_output).expanduser().resolve()
        json_output_path.parent.mkdir(parents=True, exist_ok=True)
        json_output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
