from __future__ import annotations

import csv
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import yaml

from carla_testbed.analysis.apollo_channel_health import (
    CHANNEL_HEALTH_REPORT_SCHEMA_VERSION,
    analyze_apollo_channel_health_files,
    write_apollo_channel_health_report,
)
from carla_testbed.analysis.channel_stats_normalizer import normalize_channel_stats_for_run
from carla_testbed.analysis.control_health import analyze_control_health_run_dir, write_control_health_report
from carla_testbed.analysis.route_health_report import analyze_route_health_run_dir

from .ab_consistency import MAY_DIFFER, check_ab_manifest
from .ab_manifest import ABManifest, ABRunRecord, FIXED_VARIABLE_KEYS
from .canonical_routes import (
    load_canonical_routes,
    list_diagnostic_gates,
    list_hard_gates,
    list_informational_routes,
)

FAILURE_REASONS = [
    "no_control",
    "stuck",
    "off_route",
    "high_lateral_error",
    "heading_divergence",
    "collision",
    "lane_invasion",
    "timeout",
    "planning_missing",
    "control_missing",
    "bridge_drop",
    "artifact_missing",
    "unknown",
]

BACKEND_CONFIG_PATHS = {
    "ros2_gt": "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
    "carla_direct": "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml",
}

BACKEND_STARTUP_PROFILES = {
    "ros2_gt": "render_offscreen",
    "carla_direct": "render_offscreen_no_ros2",
}

ROUTE_CLASS_TO_CAPABILITY = {
    "lane_keep": "lane_keep",
    "lane_keep_or_mild_curve": "lane_keep",
    "junction_turn": "junction_traverse",
    "curve": "curve_lane_follow",
}

MATRIX_FIELDS = [
    "batch_id",
    "run_id",
    "route_id",
    "stable_id",
    "gate_role",
    "backend",
    "transport_mode",
    "bridge_mode",
    "backend_config_path",
    "direct_control_apply_mode",
    "direct_stale_world_frame_policy",
    "steering_percent_normalization",
    "duration_s",
    "ticks",
    "run_dir",
    "config_path",
    "summary_path",
    "route_health_path",
    "control_health_path",
    "apollo_channel_health_path",
    "status",
    "return_code",
    "actual_run_dir",
    "command_stdout_path",
    "command_stderr_path",
    "failure_reason",
    "route_health_error",
    "control_health_error",
    "apollo_channel_health_error",
    "command",
]

STEERING_NORMALIZATION_OVERRIDE_KEY = "algo.apollo.control_mapping.steering_percent_normalization"
DIRECT_CONTROL_APPLY_MODE_OVERRIDE_KEY = "algo.apollo.direct_bridge.control_apply_mode"
DIRECT_STALE_WORLD_FRAME_POLICY_OVERRIDE_KEY = "algo.apollo.direct_bridge.stale_world_frame_policy"
DEFAULT_CHANNEL_HEALTH_CONFIG = Path("configs/algorithms/apollo_natural_driving_channels.yaml")


@dataclass(frozen=True)
class ABRunnerConfig:
    route_config: Path
    durations_s: tuple[float, ...]
    baseline_backend: str
    candidate_backend: str
    out_dir: Path
    fixed_delta_seconds: float = 0.05
    include_diagnostic_curves: bool = False
    include_informational_routes: bool = False
    selected_routes: tuple[str, ...] = ()
    continue_on_failure: bool = False
    dry_run: bool = False
    batch_id: str | None = None
    python_exec: str = sys.executable
    runner_script: str = "tools/run_town01_capability_online_chain.py"
    post_fail_steps: int = 120
    carla_world_ready_timeout_sec: float = 180.0
    carla_launch_attempts: int = 1
    progress_update_sec: float = 3.0
    enable_guard: bool = True
    carla_ignore_memory_preflight: bool = False
    required_steering_normalization_mode: str = "legacy_double_percent"
    required_direct_control_apply_mode: str = "frame_flush_only"
    required_direct_stale_world_frame_policy: str = "always_republish"
    required_direct_bridge_cadence_ratio_min: float = 0.8


def parse_durations(text: str | Sequence[float | int | str]) -> tuple[float, ...]:
    if isinstance(text, str):
        raw_items = [item.strip() for item in text.split(",") if item.strip()]
    else:
        raw_items = [str(item).strip() for item in text if str(item).strip()]
    durations = tuple(float(item) for item in raw_items)
    if not durations or any(value <= 0 for value in durations):
        raise ValueError("durations must contain positive seconds")
    return durations


def parse_route_filter(text: str | None) -> tuple[str, ...]:
    if not text:
        return ()
    return tuple(item.strip() for item in text.split(",") if item.strip())


def duration_to_ticks(duration_s: float, fixed_delta_seconds: float) -> int:
    if fixed_delta_seconds <= 0:
        raise ValueError("fixed_delta_seconds must be > 0")
    return int(round(float(duration_s) / float(fixed_delta_seconds)))


def _numeric_alias(route_id: str) -> str | None:
    for prefix in ("random_junction_", "random_curve_", "random_lane_", "junction", "curve", "lane"):
        if route_id.startswith(prefix):
            suffix = route_id.removeprefix(prefix)
            return suffix if suffix else None
    return None


def _route_matches(route: dict[str, Any], selected: set[str], numeric_alias_counts: dict[str, int]) -> bool:
    if not selected:
        return True
    route_id = str(route.get("route_id") or "")
    aliases = {
        route_id,
        str(route.get("stable_id") or ""),
    }
    numeric = _numeric_alias(route_id)
    if numeric and (numeric_alias_counts.get(numeric, 0) <= 1 or route.get("gate_role") == "hard_gate"):
        aliases.add(numeric)
    for value in (route.get("spawn_ref"), route.get("goal_ref"), route.get("route_ref")):
        if value:
            aliases.add(str(value))
    tags = {str(tag) for tag in route.get("no_regression_tags") or []}
    aliases.update(tags)
    return bool(aliases.intersection(selected))


def select_routes(
    route_config: str | Path,
    *,
    include_diagnostic_curves: bool,
    selected_routes: Sequence[str],
    include_informational_routes: bool = False,
) -> list[dict[str, Any]]:
    cfg = load_canonical_routes(route_config)
    if selected_routes:
        routes = list(cfg.get("routes") or [])
    else:
        routes = list_hard_gates(cfg)
    if include_diagnostic_curves:
        routes.extend(list_diagnostic_gates(cfg))
    if include_informational_routes:
        routes.extend(list_informational_routes(cfg))
    selected = set(selected_routes)
    numeric_alias_counts: dict[str, int] = {}
    for route in cfg.get("routes") or []:
        numeric = _numeric_alias(str(route.get("route_id") or ""))
        if numeric:
            numeric_alias_counts[numeric] = numeric_alias_counts.get(numeric, 0) + 1
    deduped: dict[str, dict[str, Any]] = {}
    for route in routes:
        deduped[str(route.get("route_id"))] = route
    return [route for route in deduped.values() if _route_matches(route, selected, numeric_alias_counts)]


def _batch_id() -> str:
    return f"town01_direct_ab_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def _safe_duration_label(duration_s: float) -> str:
    return str(int(duration_s)) if float(duration_s).is_integer() else str(duration_s).replace(".", "p")


def _config_path_for_backend(backend: str) -> str:
    return BACKEND_CONFIG_PATHS.get(backend, f"configs/io/examples/{backend}.yaml")


def _startup_profile_for_backend(backend: str) -> str:
    return BACKEND_STARTUP_PROFILES.get(backend, "render_offscreen_no_ros2")


def _backend_transport_metadata(config_path: str) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        return {
            "transport_mode": None,
            "bridge_mode": None,
            "backend_config_path": config_path,
            "direct_control_apply_mode": None,
            "direct_stale_world_frame_policy": None,
            "steering_percent_normalization": None,
        }
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    apollo = ((payload.get("algo") or {}).get("apollo") or {}) if isinstance(payload, dict) else {}
    direct = apollo.get("direct_bridge") or {}
    control_mapping = apollo.get("control_mapping") or {}
    transport_mode = str(apollo.get("transport_mode") or "ros2_gt")
    return {
        "transport_mode": transport_mode,
        "bridge_mode": "carla_direct" if transport_mode == "carla_direct" else "ros2_gt",
        "backend_config_path": config_path,
        "direct_control_apply_mode": direct.get("control_apply_mode") if transport_mode == "carla_direct" else None,
        "direct_stale_world_frame_policy": (
            direct.get("stale_world_frame_policy") if transport_mode == "carla_direct" else None
        ),
        "steering_percent_normalization": control_mapping.get("steering_percent_normalization"),
    }


def _step_for_route(route: dict[str, Any]) -> str:
    capability = ROUTE_CLASS_TO_CAPABILITY.get(str(route.get("route_class") or ""))
    if not capability:
        raise ValueError(f"unsupported route_class for A/B runner: {route.get('route_class')!r}")
    return f"{capability}:{route['stable_id']}"


def _runtime_overrides_for_backend(config: ABRunnerConfig, transport_mode: str | None) -> list[str]:
    overrides = [
        f"{STEERING_NORMALIZATION_OVERRIDE_KEY}={config.required_steering_normalization_mode}",
    ]
    if transport_mode == "carla_direct":
        overrides.extend(
            [
                f"{DIRECT_CONTROL_APPLY_MODE_OVERRIDE_KEY}={config.required_direct_control_apply_mode}",
                f"{DIRECT_STALE_WORLD_FRAME_POLICY_OVERRIDE_KEY}={config.required_direct_stale_world_frame_policy}",
            ]
        )
    return overrides


def build_run_matrix(config: ABRunnerConfig) -> list[dict[str, Any]]:
    batch_id = config.batch_id or config.out_dir.name or _batch_id()
    routes = select_routes(
        config.route_config,
        include_diagnostic_curves=config.include_diagnostic_curves,
        selected_routes=config.selected_routes,
        include_informational_routes=config.include_informational_routes,
    )
    backends = [config.baseline_backend, config.candidate_backend]
    rows: list[dict[str, Any]] = []
    for route in routes:
        route_id = str(route["route_id"])
        stable_id = str(route["stable_id"])
        for backend in backends:
            config_path = _config_path_for_backend(backend)
            transport_metadata = _backend_transport_metadata(config_path)
            startup_profile = _startup_profile_for_backend(backend)
            route_step = _step_for_route(route)
            for duration_s in config.durations_s:
                duration_label = _safe_duration_label(duration_s)
                run_id = f"{batch_id}__{backend}__{route_id}__{duration_label}s"
                run_dir = config.out_dir / backend / route_id / f"{duration_label}s" / run_id
                ticks = duration_to_ticks(duration_s, config.fixed_delta_seconds)
                command_parts = [
                    str(config.python_exec),
                    str(config.runner_script),
                    "--enable-lateral",
                    "--config",
                    config_path,
                    "--startup-profile",
                    startup_profile,
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
                    f"direct_ab_{backend}_{route_id}_{duration_label}s",
                    "--batch-root-parent",
                    str(run_dir),
                    "--step",
                    route_step,
                ]
                if config.enable_guard:
                    command_parts.append("--enable-guard")
                if config.carla_ignore_memory_preflight:
                    command_parts.append("--carla-ignore-memory-preflight")
                for override in _runtime_overrides_for_backend(config, transport_metadata.get("transport_mode")):
                    command_parts.extend(["--override", override])
                    key, _, value = override.partition("=")
                    if key == STEERING_NORMALIZATION_OVERRIDE_KEY:
                        transport_metadata["steering_percent_normalization"] = value
                    elif key == DIRECT_CONTROL_APPLY_MODE_OVERRIDE_KEY:
                        transport_metadata["direct_control_apply_mode"] = value
                    elif key == DIRECT_STALE_WORLD_FRAME_POLICY_OVERRIDE_KEY:
                        transport_metadata["direct_stale_world_frame_policy"] = value
                if config.continue_on_failure:
                    command_parts.append("--continue-on-failure")
                rows.append(
                    {
                        "batch_id": batch_id,
                        "run_id": run_id,
                        "route_id": route_id,
                        "stable_id": stable_id,
                        "gate_role": route.get("gate_role"),
                        "backend": backend,
                        **transport_metadata,
                        "duration_s": float(duration_s),
                        "ticks": ticks,
                        "run_dir": str(run_dir),
                        "config_path": config_path,
                        "startup_profile": startup_profile,
                        "route_step": route_step,
                        "resolved_config_path": str(run_dir / "effective.yaml"),
                        "summary_path": str(run_dir / "summary.json"),
                        "route_health_path": str(run_dir / "analysis" / "route_health" / "route_health.json"),
                        "control_health_path": str(
                            run_dir / "analysis" / "control_health" / "control_health_report.json"
                        ),
                        "apollo_channel_health_path": str(
                            run_dir
                            / "analysis"
                            / "apollo_channel_health"
                            / "apollo_channel_health_report.json"
                        ),
                        "status": "dry_run" if config.dry_run else "planned",
                        "return_code": None,
                        "actual_run_dir": None,
                        "command_stdout_path": str(run_dir / "command_stdout.log"),
                        "command_stderr_path": str(run_dir / "command_stderr.log"),
                        "failure_reason": None,
                        "route_health_error": None,
                        "control_health_error": None,
                        "apollo_channel_health_error": None,
                        "command": " ".join(command_parts),
                    }
                )
    return rows


def _manifest_fixed_variables(config: ABRunnerConfig, route_ids: Sequence[str]) -> dict[str, Any]:
    route_id_value: Any = route_ids[0] if len(set(route_ids)) == 1 else list(dict.fromkeys(route_ids))
    baseline_assists: list[str] = []
    candidate_assists: list[str] = []
    if config.baseline_backend == "carla_direct":
        baseline_assists.append("carla_direct_transport")
    if config.candidate_backend == "carla_direct":
        candidate_assists.append("carla_direct_transport")
    fixed = {key: None for key in FIXED_VARIABLE_KEYS}
    fixed.update(
        {
            "town_map": "Town01",
            "route_id": route_id_value,
            "route_definition_hash": None,
            "spawn_pose": None,
            "spawn_ref": "from_canonical_route_asset",
            "goal_pose": None,
            "goal_ref": "from_canonical_route_asset",
            "ego_blueprint": "vehicle.lincoln.mkz_2020",
            "fixed_delta_seconds": float(config.fixed_delta_seconds),
            "sim_duration_s": list(config.durations_s),
            "random_seed": 20260416,
            "traffic_actors": [],
            "sensor_rig": "gt_only",
            "localization_source": "ground_truth",
            "chassis_source": "ground_truth",
            "routing_source": "canonical_route_asset",
            "control_mode": "apollo_control",
            "actuator_mapping_mode": "legacy",
            "steer_scale": 0.25,
            "guard_config_hash": "from_config",
            "calibration_profile_id": "none",
            "active_assists": {
                "baseline": baseline_assists,
                "candidate": candidate_assists,
            },
            "timeout_policy": "continue_on_failure" if config.continue_on_failure else "stop_on_first_failure",
        }
    )
    return fixed


def build_analysis_commands(config: ABRunnerConfig) -> dict[str, str]:
    base = (
        f"{config.python_exec} tools/analyze_ab_report.py "
        f"--batch-root {config.out_dir} --out {config.out_dir / 'analysis'}"
    )
    strict = (
        f"{base} --require-hard-gate-pass "
        f"--require-steering-normalization-mode {config.required_steering_normalization_mode} "
        f"--require-direct-control-apply-mode {config.required_direct_control_apply_mode} "
        f"--require-direct-stale-world-frame-policy {config.required_direct_stale_world_frame_policy} "
        "--require-direct-transport-contract-aligned "
        f"--require-direct-bridge-cadence-ratio-min {config.required_direct_bridge_cadence_ratio_min}"
    )
    return {
        "standard": base,
        "strict_hard_gate_steering_norm": strict,
    }


def build_ab_manifest(config: ABRunnerConfig, matrix: Sequence[dict[str, Any]]) -> ABManifest:
    batch_id = config.batch_id or config.out_dir.name or _batch_id()
    runs = [
        ABRunRecord(
            run_id=str(row["run_id"]),
            route_id=str(row["route_id"]),
            backend=str(row["backend"]),
            duration_s=float(row["duration_s"]),
            run_dir=str(row["run_dir"]),
            command=str(row["command"]),
            config_path=str(row["config_path"]),
            resolved_config_path=str(row["resolved_config_path"]),
            summary_path=str(row["summary_path"]),
            route_health_path=str(row["route_health_path"]),
            control_health_path=(
                None if row.get("control_health_path") is None else str(row.get("control_health_path"))
            ),
            apollo_channel_health_path=(
                None
                if row.get("apollo_channel_health_path") is None
                else str(row.get("apollo_channel_health_path"))
            ),
            status=str(row["status"]),
            failure_reason=row.get("failure_reason"),
            return_code=row.get("return_code"),
            actual_run_dir=row.get("actual_run_dir"),
            route_health_error=row.get("route_health_error"),
            transport_mode=row.get("transport_mode"),
            bridge_mode=row.get("bridge_mode"),
            backend_config_path=row.get("backend_config_path"),
            direct_control_apply_mode=row.get("direct_control_apply_mode"),
            direct_stale_world_frame_policy=row.get("direct_stale_world_frame_policy"),
            steering_percent_normalization=row.get("steering_percent_normalization"),
            command_stdout_path=row.get("command_stdout_path"),
            command_stderr_path=row.get("command_stderr_path"),
        )
        for row in matrix
    ]
    fixed = _manifest_fixed_variables(config, [str(row["route_id"]) for row in matrix])
    manifest = ABManifest(
        batch_id=batch_id,
        created_at=datetime.now(timezone.utc).isoformat(),
        route_set=str(config.route_config),
        baseline_backend=config.baseline_backend,
        candidate_backend=config.candidate_backend,
        durations_s=list(config.durations_s),
        runs=runs,
        fixed_variables=fixed,
        allowed_differences=list(MAY_DIFFER),
        missing_fixed_variables=[],
        consistency_status="unchecked",
        notes="Long-window Town01 baseline/direct A/B matrix. carla_direct remains experimental.",
        candidate_positive_inputs={
            "route_completion": None,
            "control_available": None,
            "planning_available": None,
            "localization_available": None,
            "chassis_available": None,
            "lateral_error": None,
            "heading_error": None,
            "failure_reason": None,
            "artifact_complete": None,
            "route_health_hard_gate_eligible": None,
        },
        analysis_commands=build_analysis_commands(config),
    )
    consistency = check_ab_manifest(manifest)
    manifest.missing_fixed_variables = list(consistency.missing_fixed_variables)
    manifest.consistency_status = consistency.status
    return manifest


def write_ab_matrix_csv(path: str | Path, rows: Sequence[dict[str, Any]]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MATRIX_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in MATRIX_FIELDS})


def write_ab_manifest(path: str | Path, manifest: ABManifest) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def write_dry_run_outputs(config: ABRunnerConfig) -> tuple[list[dict[str, Any]], ABManifest]:
    matrix = build_run_matrix(config)
    manifest = build_ab_manifest(config, matrix)
    config.out_dir.mkdir(parents=True, exist_ok=True)
    write_ab_matrix_csv(config.out_dir / "ab_matrix.csv", matrix)
    write_ab_manifest(config.out_dir / "ab_manifest.json", manifest)
    return matrix, manifest


def _find_actual_summary_path(run_root: str | Path) -> Path | None:
    root = Path(run_root).expanduser()
    if not root.exists():
        return None
    summaries = [path for path in root.rglob("summary.json") if "analysis" not in path.parts]
    if not summaries:
        return None
    return max(summaries, key=lambda path: path.stat().st_mtime)


def _scenario_class_for_row(row: dict[str, Any]) -> str | None:
    route_id = str(row.get("route_id") or "")
    if route_id.startswith("curve"):
        return "curve_diagnostic"
    if route_id.startswith("junction"):
        return "junction_turn"
    if route_id.startswith("lane"):
        return "lane_keep"
    return None


def _write_insufficient_channel_health_report(
    *,
    actual_run_dir: Path,
    row: dict[str, Any],
    config_path: Path,
) -> str:
    report_path = (
        actual_run_dir
        / "analysis"
        / "apollo_channel_health"
        / "apollo_channel_health_report.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": CHANNEL_HEALTH_REPORT_SCHEMA_VERSION,
        "status": "insufficient_data",
        "scenario_class": _scenario_class_for_row(row),
        "channel_results": {},
        "missing_inputs": ["channel_stats"],
        "missing_required_channels": [],
        "missing_optional_channels": [],
        "low_rate_channels": [],
        "gap_failures": [],
        "timestamp_failures": [],
        "sequence_failures": [],
        "stale_channels": [],
        "warnings": [],
        "source": {
            "config_path": str(config_path),
            "stats_path": None,
        },
        "interpretation_boundary": (
            "Channel health could not be evaluated because channel_stats.json "
            "or cyber_bridge_stats.json was missing."
        ),
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(report_path)


def _finalize_channel_health_artifact(actual_run_dir: Path, row: dict[str, Any]) -> None:
    config_path = DEFAULT_CHANNEL_HEALTH_CONFIG
    report_dir = actual_run_dir / "analysis" / "apollo_channel_health"
    report_path = report_dir / "apollo_channel_health_report.json"
    row["apollo_channel_health_path"] = str(report_path)
    try:
        stats = normalize_channel_stats_for_run(actual_run_dir)
        if stats is None:
            row["apollo_channel_health_path"] = _write_insufficient_channel_health_report(
                actual_run_dir=actual_run_dir,
                row=row,
                config_path=config_path,
            )
            row["apollo_channel_health_error"] = "channel_stats missing"
            if row.get("failure_reason") is None:
                row["failure_reason"] = "artifact_missing"
            return
        stats_path = Path(str(stats.get("_output_path") or actual_run_dir / "channel_stats.json"))
        report = analyze_apollo_channel_health_files(
            config_path,
            stats_path,
            scenario_class=_scenario_class_for_row(row),
        )
        outputs = write_apollo_channel_health_report(report, report_dir)
        row["apollo_channel_health_path"] = outputs["apollo_channel_health_report"]
        row["apollo_channel_health_error"] = None
    except Exception as exc:  # pragma: no cover - kept defensive for online runs.
        row["apollo_channel_health_error"] = f"{type(exc).__name__}: {exc}"
        if row.get("failure_reason") is None:
            row["failure_reason"] = "artifact_missing"


def finalize_route_health_artifacts_for_row(row: dict[str, Any]) -> bool:
    summary_path = _find_actual_summary_path(str(row.get("run_dir") or ""))
    if summary_path is None:
        row["route_health_error"] = "summary.json not found"
        if row.get("failure_reason") is None:
            row["failure_reason"] = "artifact_missing"
        return False
    actual_run_dir = summary_path.parent
    row["actual_run_dir"] = str(actual_run_dir)
    row["summary_path"] = str(summary_path)
    _finalize_channel_health_artifact(actual_run_dir, row)
    control_health_path = actual_run_dir / "analysis" / "control_health" / "control_health_report.json"
    row["control_health_path"] = str(control_health_path)
    try:
        control_report = analyze_control_health_run_dir(actual_run_dir)
        control_outputs = write_control_health_report(
            control_report,
            actual_run_dir / "analysis" / "control_health",
        )
        row["control_health_path"] = control_outputs["control_health_report"]
        row["control_health_error"] = None
    except Exception as exc:  # pragma: no cover - kept defensive for online runs.
        row["control_health_error"] = f"{type(exc).__name__}: {exc}"
        if row.get("failure_reason") is None:
            row["failure_reason"] = "artifact_missing"
    try:
        result = analyze_route_health_run_dir(actual_run_dir)
    except Exception as exc:  # pragma: no cover - kept defensive for online runs.
        row["route_health_error"] = f"{type(exc).__name__}: {exc}"
        if row.get("failure_reason") is None:
            row["failure_reason"] = "artifact_missing"
        return False
    row["route_health_path"] = result["outputs"]["route_health_json"]
    row["route_health_error"] = None
    return True


def _print_matrix_row_result(row: dict[str, Any], *, index: int, total: int) -> None:
    print(
        "[town01_direct_ab] completed "
        f"{row.get('run_id')} ({index + 1}/{total}) "
        f"status={row.get('status')} return_code={row.get('return_code')} "
        f"failure_reason={row.get('failure_reason')} "
        f"summary={row.get('summary_path')} route_health={row.get('route_health_path')} "
        f"stdout={row.get('command_stdout_path')} stderr={row.get('command_stderr_path')}",
        file=sys.stderr,
        flush=True,
    )


def execute_matrix(config: ABRunnerConfig) -> tuple[list[dict[str, Any]], ABManifest]:
    matrix = build_run_matrix(config)
    stop_index: int | None = None
    for index, row in enumerate(matrix):
        stdout_path = Path(str(row.get("command_stdout_path") or Path(row["run_dir"]) / "command_stdout.log"))
        stderr_path = Path(str(row.get("command_stderr_path") or Path(row["run_dir"]) / "command_stderr.log"))
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        print(
            "[town01_direct_ab] running "
            f"{row.get('run_id')} ({index + 1}/{len(matrix)}) "
            f"stdout={stdout_path} stderr={stderr_path}",
            file=sys.stderr,
            flush=True,
        )
        try:
            with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
                "w", encoding="utf-8"
            ) as stderr_handle:
                result = subprocess.run(
                    row["command"],
                    shell=True,
                    check=False,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    text=True,
                )
            row["return_code"] = int(result.returncode)
            row["status"] = "success" if result.returncode == 0 else "failed"
            row["failure_reason"] = None if result.returncode == 0 else "unknown"
            finalize_route_health_artifacts_for_row(row)
            _print_matrix_row_result(row, index=index, total=len(matrix))
            if result.returncode != 0 and not config.continue_on_failure:
                stop_index = index
                break
        except subprocess.TimeoutExpired:
            row["status"] = "timeout"
            row["return_code"] = None
            row["failure_reason"] = "timeout"
            stderr_path.write_text("subprocess timeout\n", encoding="utf-8")
            finalize_route_health_artifacts_for_row(row)
            _print_matrix_row_result(row, index=index, total=len(matrix))
            if not config.continue_on_failure:
                stop_index = index
                break
        except Exception as exc:
            row["status"] = "failed"
            row["return_code"] = None
            row["failure_reason"] = "unknown"
            stderr_path.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
            finalize_route_health_artifacts_for_row(row)
            _print_matrix_row_result(row, index=index, total=len(matrix))
            if not config.continue_on_failure:
                stop_index = index
                break
    if stop_index is not None:
        for skipped_index, row in enumerate(matrix[stop_index + 1 :], start=stop_index + 1):
            if row.get("status") == "planned":
                row["status"] = "skipped"
                row["failure_reason"] = "stopped_after_previous_failure"
                _print_matrix_row_result(row, index=skipped_index, total=len(matrix))
    manifest = build_ab_manifest(config, matrix)
    config.out_dir.mkdir(parents=True, exist_ok=True)
    write_ab_matrix_csv(config.out_dir / "ab_matrix.csv", matrix)
    write_ab_manifest(config.out_dir / "ab_manifest.json", manifest)
    return matrix, manifest


def load_experiment_config(path: str | Path) -> dict[str, Any]:
    cfg_path = Path(path).expanduser()
    payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"experiment config must be a mapping: {cfg_path}")
    return payload
