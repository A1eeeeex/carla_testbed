from __future__ import annotations

import csv
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from carla_testbed.analysis.natural_driving_postprocess import postprocess_natural_driving_runs
from carla_testbed.analysis.town01_goal_audit import (
    build_goal_audit,
    find_goal_ab_report_paths,
    find_latest_file,
    render_goal_audit_markdown,
)

from .natural_driving_schema import list_scenarios, load_natural_driving_suite

SUITE_MANIFEST_SCHEMA_VERSION = "natural_driving_suite_manifest.v1"
POSTPROCESS_EVIDENCE_SCHEMA_VERSION = "natural_driving_postprocess_evidence.v1"

RUN_MATRIX_FIELDS = [
    "batch_id",
    "run_id",
    "scenario_id",
    "scenario_class",
    "route_id",
    "stable_id",
    "route_ref",
    "gate_role",
    "capability_profile",
    "route_step",
    "traffic_light_expectation_json",
    "traffic_light_stimulus_mode",
    "traffic_light_claim_grade",
    "traffic_light_control_overrides_json",
    "runner_overrides_json",
    "map",
    "algorithm_variant_id",
    "algorithm_variant_manifest_path",
    "online_config_path",
    "online_config_profile_name",
    "transport_mode",
    "transport_mode_source",
    "backend",
    "truth_input",
    "duration_s",
    "fixed_delta_seconds",
    "ticks",
    "runnable",
    "run_dir",
    "actual_run_dir",
    "artifact_index_status",
    "manifest_path",
    "config_resolved_path",
    "summary_path",
    "events_path",
    "timeseries_path",
    "route_health_path",
    "route_health_csv_path",
    "curve_segments_path",
    "route_health_summary_path",
    "channel_stats_path",
    "apollo_channel_health_path",
    "control_health_path",
    "failure_timeline_path",
    "route_start_alignment_path",
    "traffic_light_contract_path",
    "traffic_light_behavior_path",
    "artifact_completeness_path",
    "status",
    "return_code",
    "failure_reason",
    "command_stdout_path",
    "command_stderr_path",
    "command",
]

SCENARIO_CLASS_TO_CAPABILITY = {
    "lane_keep": "lane_keep",
    "curve_diagnostic": "curve_lane_follow",
    "junction_turn": "junction_traverse",
    "traffic_light_red_stop": "traffic_light_actual",
    "traffic_light_green_go": "traffic_light_actual",
    "traffic_light_red_to_green_release": "traffic_light_actual",
}

DEFAULT_FIXED_DELTA_SECONDS = 0.05
DEFAULT_RUNNER_SCRIPT = "tools/run_town01_capability_online_chain.py"
DEFAULT_ROUTE_HEALTH_CONFIG = "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
DEFAULT_STARTUP_PROFILE = "render_offscreen_no_ros2"
STRICT_POSTPROCESS_FAIL_STATUSES = "fail,warn,insufficient_data"

CommandRunner = Callable[[str, Path, Path], int]


@dataclass(frozen=True)
class NaturalDrivingRunnerConfig:
    suite_path: Path
    out_dir: Path
    dry_run: bool = False
    continue_on_failure: bool = False
    scenario_classes: tuple[str, ...] = ()
    scenario_ids: tuple[str, ...] = ()
    batch_id: str | None = None
    python_exec: str = sys.executable
    runner_script: str = DEFAULT_RUNNER_SCRIPT
    route_health_config: str = DEFAULT_ROUTE_HEALTH_CONFIG
    fixed_delta_seconds: float = DEFAULT_FIXED_DELTA_SECONDS
    startup_profile: str = DEFAULT_STARTUP_PROFILE
    post_fail_steps: int = 120
    carla_world_ready_timeout_sec: float = 180.0
    carla_launch_attempts: int = 1
    progress_update_sec: float = 5.0
    carla_ignore_memory_preflight: bool = False
    runner_overrides: tuple[str, ...] = ()


def parse_class_filter(text: str | Sequence[str] | None) -> tuple[str, ...]:
    if text is None:
        return ()
    if isinstance(text, str):
        items = [item.strip() for item in text.split(",")]
    else:
        items = [str(item).strip() for item in text]
    return tuple(item for item in items if item)


def parse_scenario_filter(text: str | Sequence[str] | None) -> tuple[str, ...]:
    return parse_class_filter(text)


def build_run_matrix(config: NaturalDrivingRunnerConfig) -> list[dict[str, Any]]:
    suite = load_natural_driving_suite(config.suite_path)
    mode = suite.get("mode") if isinstance(suite.get("mode"), Mapping) else {}
    runtime_config = _runtime_config_metadata(config.route_health_config)
    transport_mode = runtime_config.get("transport_mode") or mode.get("transport_mode") or mode.get("default_backend")
    transport_mode_source = runtime_config.get("transport_mode_source") or "suite_mode"
    selected_classes = set(config.scenario_classes)
    selected_scenario_ids = set(config.scenario_ids)
    batch_id = config.batch_id or config.out_dir.name or _batch_id()
    scenarios = [
        scenario
        for scenario in list_scenarios(suite)
        if not selected_classes or str(scenario.get("scenario_class") or "") in selected_classes
        if not selected_scenario_ids or str(scenario.get("scenario_id") or "") in selected_scenario_ids
    ]
    rows: list[dict[str, Any]] = []
    for scenario in scenarios:
        scenario_id = str(scenario["scenario_id"])
        scenario_class = str(scenario["scenario_class"])
        route_id = str(scenario["route_id"])
        traffic_light_expectation = scenario.get("traffic_light_expectation")
        if not isinstance(traffic_light_expectation, Mapping):
            traffic_light_expectation = None
        traffic_light_stimulus_mode = (
            str(traffic_light_expectation.get("stimulus_mode") or "").strip()
            if traffic_light_expectation
            else ""
        )
        traffic_light_claim_grade = (
            bool(traffic_light_expectation.get("claim_grade"))
            if traffic_light_expectation
            else False
        )
        traffic_light_control_overrides = _traffic_light_control_overrides_for_scenario(scenario)
        run_id = f"{batch_id}__{scenario_id}"
        run_dir = config.out_dir / scenario_class / scenario_id / run_id
        capability_profile = _capability_profile_for_scenario(scenario)
        online_route_id = _online_route_id_for_scenario(scenario)
        route_step = f"{capability_profile}:{online_route_id}" if capability_profile and online_route_id else None
        ticks = duration_to_ticks(float(scenario["duration_s"]), config.fixed_delta_seconds)
        runnable = route_step is not None
        command = _planned_command(
            config,
            scenario=scenario,
            run_dir=run_dir,
            route_step=route_step,
            ticks=ticks,
        )
        rows.append(
            {
                "batch_id": batch_id,
                "run_id": run_id,
                "scenario_id": scenario_id,
                "scenario_class": scenario_class,
                "route_id": route_id,
                "stable_id": scenario.get("stable_id"),
                "route_ref": scenario.get("route_ref"),
                "gate_role": scenario.get("gate_role"),
                "capability_profile": capability_profile,
                "route_step": route_step,
                "traffic_light_expectation": (
                    dict(traffic_light_expectation) if traffic_light_expectation else None
                ),
                "traffic_light_expectation_json": (
                    json.dumps(traffic_light_expectation, sort_keys=True)
                    if traffic_light_expectation
                    else ""
                ),
                "traffic_light_stimulus_mode": traffic_light_stimulus_mode,
                "traffic_light_claim_grade": traffic_light_claim_grade,
                "traffic_light_control_overrides_json": json.dumps(
                    traffic_light_control_overrides,
                    sort_keys=True,
                ),
                "runner_overrides_json": json.dumps(list(config.runner_overrides), sort_keys=True),
                "map": scenario.get("map"),
                "algorithm_variant_id": mode.get("algorithm_variant_id"),
                "algorithm_variant_manifest_path": mode.get("algorithm_variant_manifest_path"),
                "online_config_path": str(config.route_health_config),
                "online_config_profile_name": runtime_config.get("profile_name"),
                "transport_mode": transport_mode,
                "transport_mode_source": transport_mode_source,
                "backend": mode.get("backend"),
                "truth_input": bool(mode.get("truth_input")),
                "duration_s": float(scenario["duration_s"]),
                "fixed_delta_seconds": float(config.fixed_delta_seconds),
                "ticks": ticks,
                "runnable": runnable,
                "run_dir": str(run_dir),
                "actual_run_dir": None,
                "artifact_index_status": "not_checked",
                "manifest_path": str(run_dir / "manifest.json"),
                "config_resolved_path": str(run_dir / "config.resolved.yaml"),
                "summary_path": str(run_dir / "summary.json"),
                "events_path": str(run_dir / "events.jsonl"),
                "timeseries_path": str(run_dir / "timeseries.csv"),
                "route_health_path": str(run_dir / "analysis" / "route_health" / "route_health.json"),
                "route_health_csv_path": str(run_dir / "analysis" / "route_health" / "route_health.csv"),
                "curve_segments_path": str(run_dir / "analysis" / "route_health" / "curve_segments.csv"),
                "route_health_summary_path": str(
                    run_dir / "analysis" / "route_health" / "route_health_summary.md"
                ),
                "channel_stats_path": str(run_dir / "channel_stats.json"),
                "apollo_channel_health_path": str(
                    run_dir / "analysis" / "apollo_channel_health" / "apollo_channel_health_report.json"
                ),
                "control_health_path": str(
                    run_dir / "analysis" / "control_health" / "control_health_report.json"
                ),
                "failure_timeline_path": str(
                    run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json"
                ),
                "route_start_alignment_path": str(
                    run_dir
                    / "analysis"
                    / "route_start_alignment"
                    / "route_start_alignment_report.json"
                ),
                "traffic_light_contract_path": str(
                    run_dir / "analysis" / "traffic_light" / "traffic_light_contract_report.json"
                ),
                "traffic_light_behavior_path": str(
                    run_dir / "analysis" / "traffic_light" / "traffic_light_behavior_report.json"
                ),
                "artifact_completeness_path": str(
                    run_dir / "analysis" / "artifact_completeness" / "artifact_completeness_report.json"
                ),
                "status": "dry_run" if config.dry_run else "planned",
                "return_code": None,
                "failure_reason": None if runnable else "placeholder_route_ref_not_runnable",
                "command_stdout_path": str(run_dir / "command_stdout.log"),
                "command_stderr_path": str(run_dir / "command_stderr.log"),
                "command": command,
            }
        )
    return rows


def build_suite_manifest(
    config: NaturalDrivingRunnerConfig,
    matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    suite = load_natural_driving_suite(config.suite_path)
    batch_id = config.batch_id or config.out_dir.name or _batch_id()
    return {
        "schema_version": SUITE_MANIFEST_SCHEMA_VERSION,
        "batch_id": batch_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "suite_path": str(config.suite_path),
        "suite_name": suite.get("name"),
        "map": suite.get("map"),
        "mode": dict(suite.get("mode") or {}),
        "online_config": _runtime_config_metadata(config.route_health_config),
        "dry_run": config.dry_run,
        "continue_on_failure": config.continue_on_failure,
        "runner_overrides": list(config.runner_overrides),
        "scenario_class_filter": list(config.scenario_classes),
        "scenario_id_filter": list(config.scenario_ids),
        "run_count": len(matrix),
        "coverage": summarize_suite_coverage(matrix),
        "claim_boundary": {
            "can_claim_natural_driving_from_manifest": False,
            "postprocess_required_for_claim": True,
            "postprocess_evidence_status": "not_run",
            "required_report": "natural_driving_report.json",
            "required_evidence": [
                "manifest.json",
                "summary.json",
                "config.resolved.yaml or effective_config.yaml",
                "timeseries.csv or timeseries.jsonl",
                "events.jsonl",
                "analysis/route_health/route_health.json",
                "analysis/apollo_channel_health/apollo_channel_health_report.json",
                "analysis/control_health/control_health_report.json",
                "analysis/failure_timeline/failure_timeline_report.json",
                "analysis/route_start_alignment/route_start_alignment_report.json",
                "analysis/traffic_light/traffic_light_contract_report.json for traffic-light scenarios",
                "analysis/traffic_light/traffic_light_behavior_report.json for traffic-light scenarios",
                "analysis/artifact_completeness/artifact_completeness_report.json",
            ],
            "notes": (
                "suite_manifest.json and run_matrix.csv prove planned coverage only. "
                "A natural-driving capability claim requires offline postprocess outputs "
                "and locally verified run artifacts."
            ),
        },
        "runs": [dict(row) for row in matrix],
        "analysis_commands": {
            "natural_driving": (
                f"{config.python_exec} tools/analyze_town01_natural_driving.py "
                f"--suite-root {config.out_dir} --out {config.out_dir / 'analysis'}"
            ),
            "postprocess": (
                f"{config.python_exec} tools/postprocess_town01_natural_driving.py "
                f"--suite-root {config.out_dir} --out {config.out_dir / 'analysis' / 'natural_driving'}"
            ),
            "postprocess_strict": (
                f"{config.python_exec} tools/postprocess_town01_natural_driving.py "
                f"--suite-root {config.out_dir} --out {config.out_dir / 'analysis' / 'natural_driving'} "
                f"--require-full-target-coverage --fail-on-status {STRICT_POSTPROCESS_FAIL_STATUSES}"
            ),
            "goal_audit_strict": goal_audit_command(config),
            "postprocess_and_audit_strict": postprocess_and_audit_command(config),
        },
        "notes": (
            "Dry-run manifest for Town01 truth-input natural-driving suite. "
            "This runner does not start CARLA/Apollo in dry-run mode and does not set carla_direct "
            "as default. Any placeholder route refs are marked runnable=false instead of being "
            "executed as invalid online commands."
        ),
    }


def goal_audit_command(config: NaturalDrivingRunnerConfig) -> str:
    natural_report = (
        config.out_dir
        / "analysis"
        / "natural_driving"
        / "natural_driving_report.json"
    )
    audit_out = config.out_dir / "analysis" / "goal_audit"
    return (
        f"{config.python_exec} tools/audit_town01_goal.py "
        f"--ab-root runs/ab "
        f"--calibration-root runs "
        f"--demo-root runs "
        f"--natural-driving-report {natural_report} "
        f"--out {audit_out} "
        "--fail-on-status incomplete"
    )


def postprocess_and_audit_command(config: NaturalDrivingRunnerConfig) -> str:
    return (
        f"{config.python_exec} tools/run_town01_natural_driving_suite.py "
        f"--suite {config.suite_path} "
        f"--out {config.out_dir} "
        "--postprocess-existing "
        "--audit-after-postprocess "
        "--require-full-target-coverage "
        f"--fail-on-postprocess-status {STRICT_POSTPROCESS_FAIL_STATUSES} "
        "--fail-on-audit-status incomplete"
    )


def summarize_suite_coverage(matrix: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_class: dict[str, dict[str, Any]] = {}
    by_gate_role: dict[str, dict[str, Any]] = {}
    unrunnable: list[dict[str, Any]] = []
    traffic_light_classes = {
        "traffic_light_red_stop",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    }

    for row in matrix:
        scenario_class = str(row.get("scenario_class") or "unknown")
        gate_role = str(row.get("gate_role") or "unknown")
        runnable = bool(row.get("runnable"))

        class_bucket = by_class.setdefault(
            scenario_class,
            {
                "total": 0,
                "runnable": 0,
                "unrunnable": 0,
                "scenario_ids": [],
                "unrunnable_scenario_ids": [],
            },
        )
        class_bucket["total"] += 1
        class_bucket["scenario_ids"].append(row.get("scenario_id"))
        if runnable:
            class_bucket["runnable"] += 1
        else:
            class_bucket["unrunnable"] += 1
            class_bucket["unrunnable_scenario_ids"].append(row.get("scenario_id"))

        gate_bucket = by_gate_role.setdefault(
            gate_role,
            {
                "total": 0,
                "runnable": 0,
                "unrunnable": 0,
                "scenario_ids": [],
                "unrunnable_scenario_ids": [],
            },
        )
        gate_bucket["total"] += 1
        gate_bucket["scenario_ids"].append(row.get("scenario_id"))
        if runnable:
            gate_bucket["runnable"] += 1
        else:
            gate_bucket["unrunnable"] += 1
            gate_bucket["unrunnable_scenario_ids"].append(row.get("scenario_id"))
            unrunnable.append(
                {
                    "run_id": row.get("run_id"),
                    "scenario_id": row.get("scenario_id"),
                    "scenario_class": scenario_class,
                    "route_ref": row.get("route_ref"),
                    "failure_reason": row.get("failure_reason") or "unrunnable_scenario",
                }
            )

    traffic_light_rows = [
        row
        for row in matrix
        if str(row.get("scenario_class") or "") in traffic_light_classes
    ]
    traffic_light_claim_grade_rows = [
        row for row in traffic_light_rows if _coerce_bool(row.get("traffic_light_claim_grade")) is True
    ]
    deterministic_traffic_light_rows = [
        row
        for row in traffic_light_rows
        if str(row.get("traffic_light_stimulus_mode") or "") == "deterministic_gt_control"
    ]
    return {
        "total": len(matrix),
        "runnable": sum(1 for row in matrix if row.get("runnable")),
        "unrunnable": sum(1 for row in matrix if not row.get("runnable")),
        "by_scenario_class": by_class,
        "by_gate_role": by_gate_role,
        "traffic_light": {
            "total": len(traffic_light_rows),
            "runnable": sum(1 for row in traffic_light_rows if row.get("runnable")),
            "unrunnable": sum(1 for row in traffic_light_rows if not row.get("runnable")),
            "claim_grade": len(traffic_light_claim_grade_rows),
            "deterministic_gt_control": len(deterministic_traffic_light_rows),
            "non_claim_grade": len(traffic_light_rows) - len(traffic_light_claim_grade_rows),
            "scenario_ids": [row.get("scenario_id") for row in traffic_light_rows],
            "gate_roles": sorted({str(row.get("gate_role") or "unknown") for row in traffic_light_rows}),
            "evidence_note": (
                "Traffic-light rows entering the matrix only prove executable probes. "
                "They remain behavior evidence only after traffic-light contract and behavior reports exist."
            ),
        },
        "unrunnable_scenarios": unrunnable,
    }


def write_run_matrix_csv(path: str | Path, rows: Sequence[Mapping[str, Any]]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RUN_MATRIX_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in RUN_MATRIX_FIELDS})


def read_run_matrix_csv(path: str | Path) -> list[dict[str, Any]]:
    matrix_path = Path(path)
    if not matrix_path.exists():
        return []
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def write_suite_manifest(path: str | Path, manifest: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dict(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def refresh_suite_artifact_index(config: NaturalDrivingRunnerConfig) -> dict[str, Any]:
    return refresh_artifact_index_for_root(config.out_dir)


def refresh_artifact_index_for_root(root: str | Path) -> dict[str, Any]:
    suite_root = Path(root).expanduser()
    matrix_path = suite_root / "run_matrix.csv"
    manifest_path = suite_root / "suite_manifest.json"
    rows = read_run_matrix_csv(matrix_path)
    if not rows:
        return {
            "status": "no_run_matrix",
            "run_matrix": str(matrix_path),
            "suite_manifest": str(manifest_path),
            "refreshed_rows": 0,
        }

    refreshed_rows = 0
    for row in rows:
        if row.get("status") == "skipped" and row.get("artifact_index_status") == "not_applicable":
            continue
        if index_run_artifacts_for_row(row):
            refreshed_rows += 1
    write_run_matrix_csv(matrix_path, rows)

    manifest = _read_json(manifest_path)
    if manifest:
        manifest["runs"] = [dict(row) for row in rows]
        manifest["coverage"] = summarize_suite_coverage(rows)
        manifest["artifact_index_refreshed_at"] = datetime.now(timezone.utc).isoformat()
        manifest["artifact_index_refresh_status"] = "refreshed"
        write_suite_manifest(manifest_path, manifest)
    return {
        "status": "refreshed",
        "run_matrix": str(matrix_path),
        "suite_manifest": str(manifest_path),
        "refreshed_rows": refreshed_rows,
    }


def write_dry_run_outputs(config: NaturalDrivingRunnerConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    matrix = build_run_matrix(config)
    manifest = build_suite_manifest(config, matrix)
    config.out_dir.mkdir(parents=True, exist_ok=True)
    _create_run_dirs(matrix)
    _write_wrapper_run_manifests(matrix)
    write_run_matrix_csv(config.out_dir / "run_matrix.csv", matrix)
    write_suite_manifest(config.out_dir / "suite_manifest.json", manifest)
    return matrix, manifest


def execute_suite(
    config: NaturalDrivingRunnerConfig,
    *,
    command_runner: CommandRunner | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if config.dry_run:
        return write_dry_run_outputs(config)
    runner = command_runner or _subprocess_command_runner
    matrix = build_run_matrix(config)
    _create_run_dirs(matrix)
    _write_wrapper_run_manifests(matrix)
    _persist_suite_progress(config, matrix)
    stop_index: int | None = None
    for index, row in enumerate(matrix):
        if not row.get("runnable", True):
            row["return_code"] = None
            row["status"] = "skipped"
            row["failure_reason"] = row.get("failure_reason") or "unrunnable_scenario"
            row["artifact_index_status"] = "not_applicable"
            _persist_suite_progress(config, matrix)
            continue
        stdout_path = Path(str(row["command_stdout_path"]))
        stderr_path = Path(str(row["command_stderr_path"]))
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return_code = int(runner(str(row["command"]), stdout_path, stderr_path))
        except Exception as exc:
            return_code = 1
            stderr_path.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
        row["return_code"] = return_code
        row["status"] = "success" if return_code == 0 else "failed"
        row["failure_reason"] = None if return_code == 0 else "unknown"
        artifacts_found = index_run_artifacts_for_row(row)
        if return_code == 0 and not artifacts_found:
            row["status"] = "failed"
            row["failure_reason"] = "artifact_missing"
        _persist_suite_progress(config, matrix)
        if row["status"] == "failed" and not config.continue_on_failure:
            stop_index = index
            break
    if stop_index is not None:
        for row in matrix[stop_index + 1 :]:
            if row.get("status") == "planned":
                row["status"] = "skipped"
                row["failure_reason"] = "stopped_after_previous_failure"
                row["artifact_index_status"] = "not_applicable"
        _persist_suite_progress(config, matrix)
    manifest = build_suite_manifest(config, matrix)
    config.out_dir.mkdir(parents=True, exist_ok=True)
    write_run_matrix_csv(config.out_dir / "run_matrix.csv", matrix)
    write_suite_manifest(config.out_dir / "suite_manifest.json", manifest)
    return matrix, manifest


def _persist_suite_progress(
    config: NaturalDrivingRunnerConfig,
    matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Write recoverable suite state after each online run attempt."""
    manifest = build_suite_manifest(config, matrix)
    config.out_dir.mkdir(parents=True, exist_ok=True)
    write_run_matrix_csv(config.out_dir / "run_matrix.csv", matrix)
    write_suite_manifest(config.out_dir / "suite_manifest.json", manifest)
    return manifest


def index_run_artifacts_for_row(row: dict[str, Any]) -> bool:
    summary_path = _find_actual_summary_path(row.get("run_dir") or "")
    if summary_path is None:
        row["actual_run_dir"] = None
        row["artifact_index_status"] = "summary_missing"
        if row.get("failure_reason") is None:
            row["failure_reason"] = "artifact_missing"
        return False

    actual_run_dir = summary_path.parent
    _merge_run_manifest_metadata(actual_run_dir / "manifest.json", row)
    row["actual_run_dir"] = str(actual_run_dir)
    row["artifact_index_status"] = "found"
    row["summary_path"] = str(summary_path)
    row["manifest_path"] = _artifact_path(actual_run_dir, ["manifest.json"])
    row["config_resolved_path"] = _artifact_path(
        actual_run_dir,
        ["config.resolved.yaml", "effective_config.yaml", "effective.yaml"],
    )
    row["events_path"] = _artifact_path(actual_run_dir, ["events.jsonl"])
    row["timeseries_path"] = _artifact_path(actual_run_dir, ["timeseries.csv", "timeseries.jsonl"])
    row["route_health_path"] = _artifact_path(
        actual_run_dir,
        ["analysis/route_health/route_health.json", "route_health.json"],
    )
    row["route_health_csv_path"] = _artifact_path(
        actual_run_dir,
        ["analysis/route_health/route_health.csv", "route_health.csv"],
    )
    row["curve_segments_path"] = _artifact_path(
        actual_run_dir,
        ["analysis/route_health/curve_segments.csv", "curve_segments.csv"],
    )
    row["route_health_summary_path"] = _artifact_path(
        actual_run_dir,
        ["analysis/route_health/route_health_summary.md", "route_health_summary.md"],
    )
    row["channel_stats_path"] = _artifact_path(
        actual_run_dir,
        ["channel_stats.json", "artifacts/channel_stats.json"],
    )
    row["apollo_channel_health_path"] = _artifact_path(
        actual_run_dir,
        [
            "analysis/apollo_channel_health/apollo_channel_health_report.json",
            "apollo_channel_health_report.json",
        ],
    )
    row["control_health_path"] = _artifact_path(
        actual_run_dir,
        [
            "analysis/control_health/control_health_report.json",
            "control_health_report.json",
        ],
    )
    row["failure_timeline_path"] = _artifact_path(
        actual_run_dir,
        [
            "analysis/failure_timeline/failure_timeline_report.json",
            "failure_timeline_report.json",
        ],
    )
    row["route_start_alignment_path"] = _artifact_path(
        actual_run_dir,
        [
            "analysis/route_start_alignment/route_start_alignment_report.json",
            "route_start_alignment_report.json",
        ],
    )
    row["traffic_light_contract_path"] = _artifact_path(
        actual_run_dir,
        [
            "analysis/traffic_light/traffic_light_contract_report.json",
            "traffic_light_contract_report.json",
        ],
    )
    row["traffic_light_behavior_path"] = _artifact_path(
        actual_run_dir,
        [
            "analysis/traffic_light/traffic_light_behavior_report.json",
            "traffic_light_behavior_report.json",
        ],
    )
    row["artifact_completeness_path"] = _artifact_path(
        actual_run_dir,
        [
            "analysis/artifact_completeness/artifact_completeness_report.json",
            "artifact_completeness_report.json",
        ],
    )
    return True


def postprocess_suite_outputs(
    config: NaturalDrivingRunnerConfig,
    *,
    out_dir: str | Path | None = None,
    require_full_target_coverage: bool | None = None,
    refresh: bool = False,
) -> dict[str, Any]:
    target = Path(out_dir).expanduser() if out_dir is not None else config.out_dir / "analysis" / "natural_driving"
    pre_refresh = refresh_suite_artifact_index(config)
    report = postprocess_natural_driving_runs(
        config.out_dir,
        out_dir=target,
        require_full_target_coverage=require_full_target_coverage,
        refresh=refresh,
    )
    report["artifact_index_pre_refresh"] = pre_refresh
    report["artifact_index_refresh"] = refresh_suite_artifact_index(config)
    report["suite_manifest_postprocess_update"] = persist_postprocess_evidence_status(
        config.out_dir,
        report,
        require_full_target_coverage=require_full_target_coverage,
    )
    return report


def persist_postprocess_evidence_status(
    suite_root: str | Path,
    report: Mapping[str, Any],
    *,
    require_full_target_coverage: bool | None,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser()
    manifest_path = root / "suite_manifest.json"
    if not manifest_path.exists():
        return {
            "status": "suite_manifest_missing",
            "suite_manifest": str(manifest_path),
        }
    manifest = _read_json(manifest_path)
    if not manifest:
        return {
            "status": "suite_manifest_unreadable",
            "suite_manifest": str(manifest_path),
        }

    natural = report.get("natural_driving") if isinstance(report.get("natural_driving"), Mapping) else {}
    verdict = natural.get("verdict") if isinstance(natural.get("verdict"), Mapping) else {}
    coverage = (
        natural.get("capability_coverage")
        if isinstance(natural.get("capability_coverage"), Mapping)
        else {}
    )
    outputs = report.get("outputs") if isinstance(report.get("outputs"), Mapping) else {}
    status = natural.get("status")
    can_claim_full = bool(
        status == "pass"
        and verdict.get("can_claim_full_natural_driving") is True
        and coverage.get("can_claim_full_natural_driving") is True
    )
    evidence = {
        "schema_version": POSTPROCESS_EVIDENCE_SCHEMA_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "can_claim_full_natural_driving": can_claim_full,
        "require_full_target_coverage": bool(require_full_target_coverage),
        "outputs": dict(outputs),
        "summary": natural.get("summary") or {},
        "problem_run_count": int(natural.get("problem_run_count") or 0),
        "problem_runs": natural.get("problem_runs") or [],
        "capability_coverage": dict(coverage),
        "verdict": dict(verdict),
        "interpretation_boundary": (
            "This block records offline postprocess evidence. It still does not prove full "
            "Apollo perception/localization reproduction, and it must not be replaced by "
            "runner return_code or Dreamview-only observation."
        ),
    }
    manifest["postprocess_evidence"] = evidence
    manifest["postprocess_status"] = status
    manifest["postprocess_outputs"] = dict(outputs)
    manifest["postprocess_updated_at"] = evidence["updated_at"]
    claim_boundary = manifest.get("claim_boundary")
    if not isinstance(claim_boundary, dict):
        claim_boundary = {}
        manifest["claim_boundary"] = claim_boundary
    claim_boundary["postprocess_required_for_claim"] = True
    claim_boundary["postprocess_evidence_status"] = status or "unknown"
    claim_boundary["can_claim_natural_driving_from_manifest"] = False
    claim_boundary["can_claim_full_natural_driving_from_postprocess"] = can_claim_full
    claim_boundary["required_report"] = "natural_driving_report.json"
    write_suite_manifest(manifest_path, manifest)
    return {
        "status": "updated",
        "suite_manifest": str(manifest_path),
        "postprocess_status": status,
        "can_claim_full_natural_driving": can_claim_full,
    }


def audit_suite_goal_outputs(
    config: NaturalDrivingRunnerConfig,
    *,
    out_dir: str | Path | None = None,
    ab_root: str | Path = "runs/ab",
    calibration_root: str | Path = "runs",
    demo_root: str | Path = "runs",
    cadence_ratio_min: float = 0.8,
    refresh_ab_from_manifest: bool = True,
) -> dict[str, Any]:
    target = Path(out_dir).expanduser() if out_dir is not None else config.out_dir / "analysis" / "goal_audit"
    natural_report = config.out_dir / "analysis" / "natural_driving" / "natural_driving_report.json"
    ab_report_paths = find_goal_ab_report_paths(ab_root)
    ab_report = None if ab_report_paths else find_latest_file(ab_root, "ab_report.json")
    calibration_report = find_latest_file(calibration_root, "calibration_report.json")
    demo_recording = find_latest_file(demo_root, "town01_demo_recording_inspection.json")
    audit = build_goal_audit(
        ab_report_path=ab_report,
        ab_report_paths=ab_report_paths or None,
        calibration_report_path=calibration_report,
        natural_driving_report_path=natural_report,
        demo_recording_path=demo_recording,
        cadence_ratio_min=cadence_ratio_min,
        refresh_ab_from_manifest=refresh_ab_from_manifest,
    )
    target.mkdir(parents=True, exist_ok=True)
    json_path = target / "town01_goal_audit.json"
    md_path = target / "town01_goal_audit.md"
    json_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_goal_audit_markdown(audit), encoding="utf-8")
    return {
        "status": audit.get("status"),
        "audit": audit,
        "outputs": {
            "town01_goal_audit_json": str(json_path),
            "town01_goal_audit_md": str(md_path),
        },
    }


def _create_run_dirs(matrix: Sequence[Mapping[str, Any]]) -> None:
    for row in matrix:
        Path(str(row["run_dir"])).mkdir(parents=True, exist_ok=True)


def _write_wrapper_run_manifests(matrix: Sequence[Mapping[str, Any]]) -> None:
    for row in matrix:
        _merge_run_manifest_metadata(Path(str(row["run_dir"])) / "manifest.json", row)


def _merge_run_manifest_metadata(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = _read_json(path)
    defaults = {
        "schema_version": manifest.get("schema_version") or "natural_driving_run_manifest.v1",
        "run_id": row.get("run_id"),
        "scenario_id": row.get("scenario_id"),
        "scenario_class": row.get("scenario_class"),
        "route_id": row.get("route_id"),
        "stable_id": row.get("stable_id"),
        "route_ref": row.get("route_ref"),
        "map": row.get("map"),
        "traffic_light_expectation": row.get("traffic_light_expectation")
        or _decode_json_object(row.get("traffic_light_expectation_json")),
        "traffic_light_stimulus_mode": row.get("traffic_light_stimulus_mode"),
        "traffic_light_claim_grade": _coerce_bool(row.get("traffic_light_claim_grade")),
        "traffic_light_control_overrides": _decode_json_array(row.get("traffic_light_control_overrides_json")),
        "runner_overrides": _decode_json_array(row.get("runner_overrides_json")),
        "algorithm_variant_id": row.get("algorithm_variant_id"),
        "algorithm_variant_manifest_path": row.get("algorithm_variant_manifest_path"),
        "online_config_path": row.get("online_config_path"),
        "online_config_profile_name": row.get("online_config_profile_name"),
        "transport_mode": row.get("transport_mode"),
        "transport_mode_source": row.get("transport_mode_source"),
        "backend": row.get("backend"),
        "truth_input": _coerce_bool(row.get("truth_input")),
        "duration_s": row.get("duration_s"),
        "fixed_delta_seconds": row.get("fixed_delta_seconds"),
        "ticks": row.get("ticks"),
    }
    changed = False
    for key, value in defaults.items():
        if value is None or value == "":
            continue
        existing = manifest.get(key)
        if existing is None or existing == "":
            manifest[key] = value
            changed = True
    if changed or not path.exists():
        path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _subprocess_command_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_handle:
        completed = subprocess.run(
            command,
            shell=True,
            check=False,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
        )
    return int(completed.returncode)


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, Mapping):
        return dict(value)
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError:
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _find_actual_summary_path(run_root: str | Path) -> Path | None:
    root = Path(run_root).expanduser()
    if not root.exists():
        return None
    summaries = [
        path
        for path in root.rglob("summary.json")
        if path.is_file() and "analysis" not in path.parts
    ]
    if not summaries:
        return None
    return max(summaries, key=lambda path: path.stat().st_mtime)


def _artifact_path(actual_run_dir: Path, candidates: Sequence[str]) -> str | None:
    for relative in candidates:
        path = actual_run_dir / relative
        if path.exists():
            return str(path)
    basenames = {Path(candidate).name for candidate in candidates}
    for path in sorted(actual_run_dir.rglob("*")):
        if path.is_file() and path.name in basenames:
            return str(path)
    return None


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _read_json(path: str | Path) -> dict[str, Any]:
    input_path = Path(path)
    if not input_path.exists():
        return {}
    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _planned_command(
    config: NaturalDrivingRunnerConfig,
    *,
    scenario: Mapping[str, Any],
    run_dir: Path,
    route_step: str | None,
    ticks: int,
) -> str:
    if route_step is None:
        return (
            "UNRUNNABLE_PLACEHOLDER_ROUTE "
            f"scenario_id={scenario.get('scenario_id')} "
            f"route_ref={scenario.get('route_ref')}"
        )
    command_parts = [
        str(config.python_exec),
        str(config.runner_script),
        "--enable-lateral",
        "--enable-guard",
        "--config",
        str(config.route_health_config),
        "--startup-profile",
        str(config.startup_profile),
        "--ticks",
        str(ticks),
        "--post-fail-steps",
        str(config.post_fail_steps),
        "--carla-world-ready-timeout-sec",
        str(config.carla_world_ready_timeout_sec),
        "--carla-launch-attempts",
        str(config.carla_launch_attempts),
        "--progress-update-sec",
        str(config.progress_update_sec),
        "--comparison-label-suffix",
        f"natural_{scenario['scenario_id']}",
        "--batch-root-parent",
        str(run_dir),
        "--step",
        route_step,
    ]
    for override in _traffic_light_control_overrides_for_scenario(scenario):
        command_parts.extend(["--override", override])
    for override in config.runner_overrides:
        command_parts.extend(["--override", str(override)])
    if config.continue_on_failure:
        command_parts.append("--continue-on-failure")
    if config.carla_ignore_memory_preflight:
        command_parts.append("--carla-ignore-memory-preflight")
    return " ".join(shlex.quote(item) for item in command_parts)


def _runtime_config_metadata(path: str | Path) -> dict[str, Any]:
    config_path = Path(path).expanduser()
    metadata: dict[str, Any] = {
        "path": str(path),
        "profile_name": None,
        "transport_mode": None,
        "transport_mode_source": None,
        "warnings": [],
    }
    if not config_path.exists():
        metadata["warnings"].append("online_config_missing")
        return metadata
    try:
        import yaml
    except Exception:
        metadata["warnings"].append("yaml_unavailable")
        return metadata
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        metadata["warnings"].append(f"online_config_unreadable:{type(exc).__name__}")
        return metadata
    if not isinstance(payload, Mapping):
        metadata["warnings"].append("online_config_not_mapping")
        return metadata

    run = payload.get("run") if isinstance(payload.get("run"), Mapping) else {}
    scenario = payload.get("scenario") if isinstance(payload.get("scenario"), Mapping) else {}
    algo = payload.get("algo") if isinstance(payload.get("algo"), Mapping) else {}
    apollo = algo.get("apollo") if isinstance(algo.get("apollo"), Mapping) else {}

    metadata["profile_name"] = run.get("profile_name")
    explicit_transport = apollo.get("transport_mode")
    if explicit_transport not in {None, ""}:
        metadata["transport_mode"] = str(explicit_transport)
        metadata["transport_mode_source"] = "online_config.algo.apollo.transport_mode"
    elif scenario.get("publish_ros2_gt") is True:
        metadata["transport_mode"] = "ros2_gt"
        metadata["transport_mode_source"] = "online_config.scenario.publish_ros2_gt"
    elif scenario.get("publish_ros2_gt") is False:
        metadata["transport_mode"] = "non_ros2_gt"
        metadata["transport_mode_source"] = "online_config.scenario.publish_ros2_gt"
    return metadata


def duration_to_ticks(duration_s: float, fixed_delta_seconds: float) -> int:
    if fixed_delta_seconds <= 0:
        raise ValueError("fixed_delta_seconds must be > 0")
    return int(round(float(duration_s) / float(fixed_delta_seconds)))


def _capability_profile_for_scenario(scenario: Mapping[str, Any]) -> str | None:
    return SCENARIO_CLASS_TO_CAPABILITY.get(str(scenario.get("scenario_class") or ""))


def _online_route_id_for_scenario(scenario: Mapping[str, Any]) -> str | None:
    for key in ("route_ref", "stable_id"):
        value = str(scenario.get(key) or "").strip()
        if value and not value.startswith("placeholder:") and not value.startswith("inline:"):
            return value
    return None


def _traffic_light_control_overrides_for_scenario(scenario: Mapping[str, Any]) -> list[str]:
    scenario_class = str(scenario.get("scenario_class") or "")
    if not scenario_class.startswith("traffic_light"):
        return []
    expectation = scenario.get("traffic_light_expectation")
    if not isinstance(expectation, Mapping):
        return []
    stimulus_mode = str(expectation.get("stimulus_mode") or "").strip()
    if stimulus_mode != "deterministic_gt_control":
        return []
    initial_state = str(expectation.get("expected_initial_state") or "").strip().upper()
    if not initial_state:
        return []
    release_state = str(expectation.get("expected_release_state") or "").strip().upper()
    release_after_s = expectation.get("release_after_s", expectation.get("expected_release_after_s", None))
    if release_state and release_after_s is None:
        release_after_s = 10.0
    overrides = [
        "scenario.traffic_lights.control_mode=deterministic_gt_control",
        "scenario.traffic_lights.force_green=false",
        "scenario.traffic_lights.freeze=true",
        f"scenario.traffic_lights.initial_state={initial_state}",
        "algo.apollo.traffic_light.policy=carla_actual",
        "algo.apollo.planning.disable_traffic_light_rule=false",
        "algo.apollo.planning.lane_follow_only_scenario=false",
    ]
    if release_state:
        overrides.append(f"scenario.traffic_lights.release_state={release_state}")
        overrides.append(f"scenario.traffic_lights.release_after_s={float(release_after_s or 10.0)}")
    return overrides


def _batch_id() -> str:
    return f"town01_natural_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def _decode_json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if value is None:
        return []
    try:
        payload = json.loads(str(value))
    except Exception:
        return []
    return payload if isinstance(payload, list) else []
