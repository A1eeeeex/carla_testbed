from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from carla_testbed.analysis.apollo_hdmap_projection import (
    analyze_apollo_hdmap_projection_file,
    write_apollo_hdmap_projection_report,
)
from carla_testbed.analysis.artifact_completeness import (
    check_run_artifact_completeness,
    write_run_artifact_completeness_report,
)
from carla_testbed.analysis.channel_stats_normalizer import normalize_channel_stats_for_run
from carla_testbed.analysis.routing_response_decoded import (
    read_routing_response_decoded,
    write_routing_response_decoded_report,
)
from carla_testbed.config import TestbedConfig
from carla_testbed.evidence import build_and_write_evidence_bundle
from carla_testbed.record import RunArtifactStore, build_manifest, build_summary
from carla_testbed.record.artifact_store import build_carla_world_identity

COMPAT_APOLLO_CYBER_GT_RUNTIME = "compat_apollo_cyber_gt_runtime"
TYPED_APOLLO_CLAIM_RUNTIME = "typed_apollo_claim_runtime"
APOLLO_CYBERRT_GT_TRANSPORT = "apollo_cyberrt_gt_over_ros2_transition"
APOLLO_VARIANT_ID = "apollo_10_0_carla_gt_town01_reference"
APOLLO_VARIANT_MANIFEST = "configs/algorithms/apollo_variant.carla_gt.example.yaml"

CONTROL_TRACE_FIELDS = [
    "sim_time",
    "frame_id",
    "route_id",
    "route_s",
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "throttle_raw",
    "throttle_mapped",
    "throttle_applied",
    "brake_raw",
    "brake_mapped",
    "brake_applied",
    "lateral_guard_applied",
    "trajectory_contract_guard_applied",
]


@dataclass(frozen=True)
class ApolloCompatRuntimeResult:
    run_dir: Path
    exit_code: int
    message: str
    outputs: dict[str, str]


def run_compat_apollo_cyber_gt_runtime(
    cfg: TestbedConfig,
    *,
    config_path: str | Path,
    run_dir: str | Path,
    resolved_config: Mapping[str, Any],
    legacy_dispatch_requested: bool = False,
) -> ApolloCompatRuntimeResult:
    """Dispatch a typed Apollo claim runtime without legacy fallback.

    Real Town01 legacy-driver configs are executed through the transition
    backend with a typed-resolved effective config. Minimal CI configs that do
    not describe a runnable legacy driver still materialize a non-claim
    artifact boundary only.
    """

    root = Path(run_dir).expanduser()
    metadata = _metadata_from_config(cfg, config_path=config_path, run_dir=root)
    if _transition_driver_name(cfg):
        return _run_typed_transition_backend(
            cfg,
            config_path=config_path,
            run_dir=root,
            resolved_config=resolved_config,
            metadata=metadata,
            legacy_dispatch_requested=legacy_dispatch_requested,
        )
    return _run_artifact_only_compat_runtime(
        cfg,
        config_path=config_path,
        run_dir=root,
        resolved_config=resolved_config,
        metadata=metadata,
        legacy_dispatch_requested=legacy_dispatch_requested,
    )


def _run_artifact_only_compat_runtime(
    cfg: TestbedConfig,
    *,
    config_path: str | Path,
    run_dir: Path,
    resolved_config: Mapping[str, Any],
    metadata: Mapping[str, Any],
    legacy_dispatch_requested: bool,
) -> ApolloCompatRuntimeResult:
    root = Path(run_dir).expanduser()
    store = RunArtifactStore(root).ensure()
    start_wall = time.time()
    store.write_resolved_config(dict(resolved_config))
    route_path = _write_route_artifact(root, route_id=metadata["route_id"], metadata=metadata)
    stats_path = _write_cyber_bridge_stats(root, cfg, metadata=metadata)
    _write_topic_publish_stats(root)
    _write_control_apply_trace(root)
    _write_timeseries_csv(root, route_id=metadata["route_id"])

    manifest = _build_manifest_payload(
        cfg,
        config_path=config_path,
        start_wall_time_s=start_wall,
        metadata=metadata,
        route_path=route_path,
        legacy_dispatch_requested=legacy_dispatch_requested,
    )
    store.write_manifest(manifest)

    events = store.open_events()
    events.append(
        {
            "event_type": "typed_apollo_claim_runtime_start",
            "runtime_dispatch_kind": COMPAT_APOLLO_CYBER_GT_RUNTIME,
            "runtime_execution_mode": "artifact_only",
            "wall_time_s": start_wall,
            "claim_boundary": "typed config loaded; legacy fallback forbidden",
        }
    )
    events.append(
        {
            "event_type": "apollo_runtime_evidence_missing",
            "severity": "insufficient_data",
            "missing_evidence": [
                "/apollo/routing_response",
                "apollo_hdmap_api_projection",
                "/apollo/localization/pose runtime samples",
                "/apollo/control runtime apply trace",
            ],
        }
    )
    end_wall = time.time()
    events.append(
        {
            "event_type": "typed_apollo_claim_runtime_end",
            "success": False,
            "exit_reason": "apollo_online_runtime_not_executed_in_compat_path",
            "wall_time_s": end_wall,
        }
    )
    events.close()

    summary = _build_summary_payload(
        cfg,
        wall_duration_s=end_wall - start_wall,
        metadata=metadata,
    )
    store.update_manifest({"end_time_wall_s": end_wall})
    store.write_summary(summary)

    _write_core_insufficient_reports(root, metadata=metadata, route_path=route_path, stats_path=stats_path)
    outputs = _write_secondary_reports(root, metadata=metadata)
    outputs.update(_write_boundary_and_completeness(root, metadata=metadata))
    outputs["manifest"] = str(root / "manifest.json")
    outputs["summary"] = str(root / "summary.json")
    outputs["timeseries"] = str(root / "timeseries.csv")
    outputs["config_resolved"] = str(root / "config.resolved.yaml")

    return ApolloCompatRuntimeResult(
        run_dir=root,
        exit_code=2,
        message=(
            f"{COMPAT_APOLLO_CYBER_GT_RUNTIME} wrote non-claim runtime boundary "
            f"artifacts to {root}; real Apollo/CARLA evidence is still insufficient_data"
        ),
        outputs=outputs,
    )


def _run_typed_transition_backend(
    cfg: TestbedConfig,
    *,
    config_path: str | Path,
    run_dir: Path,
    resolved_config: Mapping[str, Any],
    metadata: Mapping[str, Any],
    legacy_dispatch_requested: bool,
) -> ApolloCompatRuntimeResult:
    root = Path(run_dir).expanduser()
    start_wall = time.time()
    unexpected_preexisting = _unexpected_transition_preseeded_paths(root)
    if unexpected_preexisting:
        return _write_transition_preflight_failure(
            root,
            cfg=cfg,
            config_path=config_path,
            resolved_config=resolved_config,
            metadata=metadata,
            start_wall=start_wall,
            reason="run_dir_not_empty_before_transition_backend",
            legacy_dispatch_requested=legacy_dispatch_requested,
        )
    staging_dir = root.parent / f".{root.name}.typed_runtime"
    staging_dir.mkdir(parents=True, exist_ok=True)
    legacy_effective = _legacy_effective_config_from_typed(cfg, resolved_config, run_dir=root, metadata=metadata)
    legacy_effective_path = staging_dir / "typed_runtime.effective_legacy.yaml"
    legacy_effective_path.write_text(
        yaml.safe_dump(legacy_effective, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    resolved_config_path = staging_dir / "config.resolved.yaml"
    resolved_config_path.write_text(
        yaml.safe_dump(_json_safe(resolved_config), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    exit_code = 0
    error_text = ""
    try:
        exit_code = _invoke_tbio_transition_backend(legacy_effective_path, root)
    except subprocess.CalledProcessError as exc:
        exit_code = int(exc.returncode or 2)
        error_text = str(exc)
    except SystemExit as exc:
        exit_code = _system_exit_code(exc)
        error_text = str(exc)
    except BaseException as exc:
        exit_code = 2
        error_text = f"{exc.__class__.__name__}: {exc}"
    end_wall = time.time()
    store = RunArtifactStore(root).ensure()
    store.write_resolved_config(dict(resolved_config))
    shutil.copy2(legacy_effective_path, root / "typed_runtime.effective_legacy.yaml")
    _append_transition_events(
        root,
        start_wall=start_wall,
        end_wall=end_wall,
        exit_code=exit_code,
        error_text=error_text,
        staged_effective_config_path=legacy_effective_path,
    )

    root_legacy_effective_path = root / "typed_runtime.effective_legacy.yaml"
    transition_runtime_staged = staging_dir / "typed_transition_runtime.json"
    if transition_runtime_staged.exists() and not (root / "artifacts" / "typed_transition_runtime.json").exists():
        (root / "artifacts").mkdir(parents=True, exist_ok=True)
        shutil.copy2(transition_runtime_staged, root / "artifacts" / "typed_transition_runtime.json")

    runtime_metadata = _metadata_with_transition_outputs(root, metadata)
    _merge_manifest_after_transition(
        root,
        cfg=cfg,
        metadata=runtime_metadata,
        config_path=config_path,
        legacy_effective_path=root_legacy_effective_path,
        start_wall=start_wall,
        end_wall=end_wall,
        exit_code=exit_code,
        error_text=error_text,
        legacy_dispatch_requested=legacy_dispatch_requested,
    )
    _merge_summary_after_transition(
        root,
        cfg=cfg,
        metadata=runtime_metadata,
        wall_duration_s=end_wall - start_wall,
        exit_code=exit_code,
        error_text=error_text,
    )

    route_path = _write_route_artifact(root, route_id=str(runtime_metadata["route_id"]), metadata=runtime_metadata)
    stats_path = root / "artifacts" / "cyber_bridge_stats.json"
    if not stats_path.exists():
        stats_path = _write_cyber_bridge_stats(root, cfg, metadata=runtime_metadata)
    if not (root / "timeseries.csv").exists():
        _write_timeseries_csv(root, route_id=str(runtime_metadata["route_id"]))
    if not (root / "artifacts/topic_publish_stats.jsonl").exists():
        _write_topic_publish_stats(root)
    if not (root / "artifacts/control_apply_trace.jsonl").exists():
        _write_control_apply_trace(root)
    _write_core_insufficient_reports(
        root,
        metadata=runtime_metadata,
        route_path=route_path,
        stats_path=stats_path,
        overwrite=False,
    )
    outputs = _write_secondary_reports(root, metadata=runtime_metadata, overwrite=False)
    outputs.update(_maybe_run_phase1_postprocess(root))
    outputs.update(_write_boundary_and_completeness(root, metadata=runtime_metadata))
    outputs["manifest"] = str(root / "manifest.json")
    outputs["summary"] = str(root / "summary.json")
    outputs["config_resolved"] = str(root / "config.resolved.yaml")
    outputs["typed_runtime_effective_config"] = str(root_legacy_effective_path)
    return ApolloCompatRuntimeResult(
        run_dir=root,
        exit_code=exit_code,
        message=(
            f"{TYPED_APOLLO_CLAIM_RUNTIME} dispatched transition backend to {root}; "
            f"returncode={exit_code}"
        ),
        outputs=outputs,
    )


def _append_transition_events(
    root: Path,
    *,
    start_wall: float,
    end_wall: float,
    exit_code: int,
    error_text: str,
    staged_effective_config_path: Path,
) -> None:
    events = RunArtifactStore(root).open_events(append=True)
    events.append(
        {
            "event_type": "typed_apollo_claim_runtime_start",
            "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME,
            "runtime_execution_mode": "transition_backend",
            "typed_runtime_effective_config_path": str(staged_effective_config_path),
            "wall_time_s": start_wall,
        }
    )
    events.append(
        {
            "event_type": "typed_apollo_claim_runtime_end",
            "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME,
            "success": exit_code == 0,
            "returncode": exit_code,
            "error": error_text,
            "wall_time_s": end_wall,
        }
    )
    events.close()


def _write_transition_preflight_failure(
    root: Path,
    *,
    cfg: TestbedConfig,
    config_path: str | Path,
    resolved_config: Mapping[str, Any],
    metadata: Mapping[str, Any],
    start_wall: float,
    reason: str,
    legacy_dispatch_requested: bool,
) -> ApolloCompatRuntimeResult:
    store = RunArtifactStore(root).ensure()
    store.write_resolved_config(dict(resolved_config))
    route_path = _write_route_artifact(root, route_id=str(metadata["route_id"]), metadata=metadata)
    stats_path = _write_cyber_bridge_stats(root, cfg, metadata=metadata)
    manifest = _build_manifest_payload(
        cfg,
        config_path=config_path,
        start_wall_time_s=start_wall,
        metadata={**metadata, "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME},
        route_path=route_path,
        legacy_dispatch_requested=legacy_dispatch_requested,
    )
    manifest.update(
        {
            "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME,
            "runtime_execution_mode": "transition_backend_preflight",
            "legacy_fallback_used": False,
        }
    )
    manifest.setdefault("metadata", {})["transition_preflight_error"] = reason
    store.write_manifest(manifest)
    store.write_summary(
        {
            "run_id": metadata["run_id"],
            "scenario_id": metadata["scenario_id"],
            "scenario_class": metadata["scenario_class"],
            "route_id": metadata["route_id"],
            "backend": "apollo_cyberrt",
            "success": False,
            "exit_reason": reason,
            "typed_config_loaded": True,
            "legacy_fallback_used": False,
            "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME,
            "can_claim_unassisted_natural_driving": False,
            "why_not_claimable": [reason],
        }
    )
    _write_core_insufficient_reports(root, metadata=metadata, route_path=route_path, stats_path=stats_path)
    outputs = _write_secondary_reports(root, metadata=metadata)
    outputs.update(_maybe_run_phase1_postprocess(root))
    outputs.update(_write_boundary_and_completeness(root, metadata=metadata))
    outputs["manifest"] = str(root / "manifest.json")
    outputs["summary"] = str(root / "summary.json")
    outputs["config_resolved"] = str(root / "config.resolved.yaml")
    return ApolloCompatRuntimeResult(
        root,
        2,
        f"{TYPED_APOLLO_CLAIM_RUNTIME} preflight failed: {reason}",
        outputs=outputs,
    )


def _metadata_from_config(
    cfg: TestbedConfig,
    *,
    config_path: str | Path,
    run_dir: Path,
) -> dict[str, Any]:
    params = cfg.backend.params if isinstance(cfg.backend.params, Mapping) else {}
    legacy_run = _mapping(params.get("legacy_run"))
    legacy_algo = _mapping(params.get("legacy_algo"))
    legacy_scenario = _mapping(params.get("legacy_scenario"))
    legacy_carla = _mapping(params.get("legacy_carla"))
    profile_name = str(legacy_run.get("profile_name") or run_dir.name or cfg.run.id).strip()
    route_id = _first_text(
        legacy_run.get("route_id"),
        legacy_scenario.get("route_id"),
        _find_recursive(params, ("route_id", "route_ref")),
        _route_from_run_dir(run_dir),
    )
    scenario_id = _first_text(
        legacy_run.get("scenario_id"),
        legacy_scenario.get("scenario_id"),
        cfg.scenario.name,
        route_id,
        run_dir.parent.name,
    )
    capability_profile = _first_text(
        legacy_run.get("capability_profile"),
        _find_recursive(params, ("capability_profile",)),
        "",
    )
    scenario_class = _first_text(
        legacy_run.get("scenario_class"),
        legacy_scenario.get("scenario_class"),
        _find_recursive(params, ("scenario_class",)),
        _scenario_class(
            capability_profile=capability_profile,
            scenario_id=scenario_id,
            route_id=route_id,
            profile_name=profile_name,
        ),
    )
    map_name = _first_text(
        cfg.sim.town,
        legacy_carla.get("town"),
        legacy_carla.get("map"),
        "Town01",
    )
    return {
        "run_id": run_dir.name if run_dir.name else cfg.run.id,
        "scenario_id": scenario_id or route_id or cfg.scenario.name or run_dir.name,
        "scenario_class": scenario_class,
        "route_id": route_id or "unknown_route",
        "profile_name": profile_name or "unknown_profile",
        "capability_profile": capability_profile,
        "map_name": map_name,
        "config_path": str(Path(config_path)),
        "config_aliases_used": list(cfg.config_aliases_used or []),
    }


def _metadata_with_transition_outputs(root: Path, metadata: Mapping[str, Any]) -> dict[str, Any]:
    """Merge runtime-selected route identity back into typed compat metadata.

    Random-route transition runs often cannot know the final route_id until the
    legacy runner writes summary.json. Keeping the pre-dispatch placeholder in
    manifest/report metadata makes otherwise useful evidence look stale.
    """

    summary = _read_json(root / "summary.json")
    manifest = _read_json(root / "manifest.json")
    route = _read_json(root / "route.json")
    route_id = _first_non_placeholder_text(
        summary.get("route_id"),
        summary.get("selected_route_id"),
        manifest.get("route_id"),
        manifest.get("selected_route_id"),
        route.get("route_id"),
        metadata.get("route_id"),
    )
    updated = dict(metadata)
    if route_id:
        updated["route_id"] = route_id
    return updated


def _transition_driver_name(cfg: TestbedConfig) -> str:
    params = cfg.scenario.params if isinstance(cfg.scenario.params, Mapping) else {}
    driver = _first_text(params.get("driver"), cfg.scenario.name)
    if driver in {
        "carla_followstop",
        "carla_apollo_semantic_suite",
        "carla_actuator_calibration",
        "carla_town01_route_health",
    }:
        return driver
    return ""


def _legacy_effective_config_from_typed(
    cfg: TestbedConfig,
    resolved_config: Mapping[str, Any],
    *,
    run_dir: Path,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    params = cfg.backend.params if isinstance(cfg.backend.params, Mapping) else {}
    payload: dict[str, Any] = {}
    for legacy_key in ("io", "algo", "paths", "carla", "runtime", "logging", "reports", "checks", "profiles"):
        value = params.get(f"legacy_{legacy_key}")
        if isinstance(value, Mapping):
            payload[legacy_key] = _json_safe(value)
    run_payload = {
        "seed": cfg.run.seed,
        "ticks": cfg.run.max_ticks,
        "fixed_delta_seconds": cfg.run.fixed_dt_s,
        "profile_name": metadata["profile_name"],
        "claim_profile": bool(cfg.run.claim_profile),
        "materialization_probe": bool(getattr(cfg.run, "materialization_probe", False)),
    }
    legacy_run = params.get("legacy_run")
    if isinstance(legacy_run, Mapping):
        run_payload.update(_json_safe(legacy_run))
    payload["run"] = run_payload
    scenario_payload = dict(_json_safe(cfg.scenario.params))
    scenario_payload.setdefault("driver", cfg.scenario.name)
    payload["scenario"] = scenario_payload
    payload.setdefault("algo", {}).setdefault("stack", "apollo")
    payload.setdefault("recording", {})
    if cfg.recording.artifacts:
        payload["recording"] = {"artifacts": _json_safe(cfg.recording.artifacts)}
    payload["assist_ledger"] = _json_safe(cfg.assist_ledger)
    payload.setdefault("artifacts", {})["dir"] = str(run_dir / "artifacts")
    payload.setdefault("typed_runtime", {})
    payload["typed_runtime"].update(
        {
            "schema_version": "typed_apollo_claim_runtime.v1",
            "source_config_path": str(cfg.source_path or ""),
            "resolved_config_snapshot": _json_safe(resolved_config),
            "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME,
            "legacy_fallback_used": False,
        }
    )
    return payload


def _invoke_tbio_transition_backend(legacy_effective_path: Path, run_dir: Path) -> int:
    from tbio.carla.launch_policy import append_followstop_launch_args, resolve_carla_launch_policy

    python_exec = _resolve_transition_python(legacy_effective_path)
    repo_root = Path(__file__).resolve().parents[2]
    cfg = _read_yaml(legacy_effective_path)
    command = [
        python_exec,
        "-u",
        "-m",
        "examples.run_followstop",
        "--config",
        str(legacy_effective_path),
        "--run-dir",
        str(run_dir),
    ]
    command = append_followstop_launch_args(command, resolve_carla_launch_policy(cfg))
    runtime_json_path = legacy_effective_path.parent / "typed_transition_runtime.json"
    runtime_json_path.write_text(
        json.dumps(
            {
                "schema_version": "typed_transition_runtime.v1",
                "python_executable": python_exec,
                "current_python_executable": sys.executable,
                "transition_entrypoint": "examples.run_followstop",
                "command": command,
                "allow_preseeded_run_dir_env": "CARLA_TESTBED_TYPED_TRANSITION_ALLOW_PRESEEDED_RUN_DIR",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env["CARLA_TESTBED_TYPED_TRANSITION_ALLOW_PRESEEDED_RUN_DIR"] = "1"
    proc = subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        check=False,
    )
    return int(proc.returncode)


def _resolve_transition_python(legacy_effective_path: Path) -> str:
    cfg = _read_yaml(legacy_effective_path)
    explicit_candidates = [
        os.environ.get("CARLA_TESTBED_ONLINE_PYTHON"),
        os.environ.get("CARLA16_PYTHON"),
        _find_recursive(cfg, ("host_python_exec", "python_exec")),
    ]
    fallback_candidates = [
        "/home/ubuntu/miniconda3/envs/carla16/bin/python",
        "/home/ubuntu/miniconda3/envs/carla/bin/python",
        sys.executable,
    ]
    for raw in [*explicit_candidates, *fallback_candidates]:
        candidate = _expand_runtime_python_candidate(raw)
        if candidate and _is_executable_file(candidate):
            return candidate
    return sys.executable


def _maybe_run_phase1_postprocess(root: Path) -> dict[str, str]:
    if not _run_declares_phase1_scenario(root):
        return {}
    outputs: dict[str, str] = {}
    outputs.update(_maybe_export_apollo_hdmap_projection(root))
    out_dir = root / "analysis" / "phase1_postprocess"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "phase1_postprocess_report.json"
    try:
        from carla_testbed.analysis.phase1_postprocess import run_phase1_postprocess

        report = run_phase1_postprocess(root)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        outputs["phase1_postprocess"] = str(report_path)
        return outputs
    except Exception as exc:  # pragma: no cover - defensive online artifact path
        failure_path = out_dir / "phase1_postprocess_failure.json"
        failure = {
            "schema_version": "phase1_postprocess_failure.v1",
            "status": "fail",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "claim_boundary": (
                "Phase 1 postprocess failed after runtime completion; do not treat "
                "missing Phase 1 artifacts as backend behavior evidence."
            ),
        }
        failure_path.write_text(json.dumps(failure, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        outputs["phase1_postprocess_failure"] = str(failure_path)
        return outputs


def _maybe_export_apollo_hdmap_projection(root: Path) -> dict[str, str]:
    if not _phase1_auto_export_hdmap_projection_requested(root):
        return {}
    out_dir = root / "analysis" / "apollo_hdmap_projection_export"
    out_dir.mkdir(parents=True, exist_ok=True)
    status_path = out_dir / "apollo_hdmap_projection_export_status.json"
    projection_path = root / "artifacts" / "apollo_hdmap_projection.jsonl"
    if projection_path.exists() and projection_path.stat().st_size > 0:
        status = {
            "schema_version": "apollo_hdmap_projection_auto_export.v1",
            "status": "skipped",
            "reason": "existing_non_empty_projection_artifact",
            "out_path": str(projection_path),
            "claim_boundary": "Existing projection evidence is analyzed by Phase 1 postprocess; export does not change driving behavior.",
        }
        status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {"apollo_hdmap_projection_auto_export": str(status_path)}
    try:
        from carla_testbed.analysis.apollo_hdmap_projection import (
            analyze_apollo_hdmap_projection_file,
            write_apollo_hdmap_projection_report,
        )
        from carla_testbed.analysis.apollo_hdmap_projection_export import (
            export_apollo_hdmap_projection_jsonl,
            infer_map_xysl_config_from_run_dir,
        )

        frame_transform_path = _repo_root() / "configs" / "town01" / "apollo_frame_transform.example.yaml"
        export_status = export_apollo_hdmap_projection_jsonl(
            run_dir=root,
            out_path=projection_path,
            config=infer_map_xysl_config_from_run_dir(root),
            max_samples=80,
            include_route_samples=True,
            include_start_goal=True,
            frame_transform_path=frame_transform_path if frame_transform_path.exists() else None,
            route_sample_step_m=5.0,
            min_route_s_coverage=20.0,
        )
        analysis_report = analyze_apollo_hdmap_projection_file(projection_path)
        analysis_outputs = write_apollo_hdmap_projection_report(
            analysis_report,
            root / "analysis" / "apollo_hdmap_projection",
        )
        status = {
            "schema_version": "apollo_hdmap_projection_auto_export.v1",
            "status": export_status.get("status"),
            "export": export_status,
            "analysis": {
                "status": analysis_report.get("status"),
                "claim_grade": analysis_report.get("claim_grade"),
                "blocking_reasons": analysis_report.get("blocking_reasons") or [],
                "insufficient_reasons": analysis_report.get("insufficient_reasons") or [],
                "warnings": analysis_report.get("warnings") or [],
                "outputs": analysis_outputs,
            },
            "claim_boundary": "Projection export adds Apollo HDMap API evidence only; it does not change runtime behavior.",
        }
    except Exception as exc:  # pragma: no cover - depends on local Apollo/Docker runtime
        status = {
            "schema_version": "apollo_hdmap_projection_auto_export.v1",
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "out_path": str(projection_path),
            "claim_boundary": (
                "HDMap projection export failed during postprocess. Treat projection evidence as "
                "insufficient_data, not as a driving-behavior failure."
            ),
        }
    status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"apollo_hdmap_projection_auto_export": str(status_path)}


def _phase1_auto_export_hdmap_projection_requested(root: Path) -> bool:
    for rel in ("config.resolved.yaml", "typed_runtime.effective_legacy.yaml", "effective.yaml"):
        payload = _read_yaml(root / rel)
        if _find_recursive(payload, ("auto_export_apollo_hdmap_projection",)) is not None:
            return bool(_find_recursive(payload, ("auto_export_apollo_hdmap_projection",)))
    manifest = _read_json(root / "manifest.json")
    value = _find_recursive(manifest, ("auto_export_apollo_hdmap_projection",))
    return bool(value)


def _run_declares_phase1_scenario(root: Path) -> bool:
    for rel in ("config.resolved.yaml", "typed_runtime.effective_legacy.yaml", "effective.yaml"):
        payload = _read_yaml(root / rel)
        if _contains_key(payload, "phase1_scenario_path"):
            return True
    manifest = _read_json(root / "manifest.json")
    return _contains_key(manifest, "phase1_scenario_path")


def _contains_key(payload: Any, key: str) -> bool:
    if isinstance(payload, Mapping):
        for name, value in payload.items():
            if name == key and value:
                return True
            if _contains_key(value, key):
                return True
    elif isinstance(payload, list):
        return any(_contains_key(item, key) for item in payload)
    return False


def _expand_runtime_python_candidate(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    expanded = os.path.expanduser(os.path.expandvars(text))
    if "${" in expanded:
        # Host configs commonly preserve ${CARLA16_PYTHON} when the variable is
        # not exported in the caller shell. Do not pass the placeholder through
        # to subprocess.Popen; fall back to known local interpreters instead.
        return ""
    return expanded


def _is_executable_file(path_text: str) -> bool:
    path = Path(path_text)
    return path.is_file() and os.access(path, os.X_OK)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


_ALLOWED_TRANSITION_PRESEEDED_ROOT_FILES = {
    "launch_plan.json",
    "manifest.json",
    "plan.resolved.yaml",
    "preflight.json",
    "summary.json",
}

_ALLOWED_TRANSITION_PRESEEDED_EXECUTION_FILES = {
    "runtime.pid",
    "runtime_adapter_state.json",
    "runtime_stderr.log",
    "runtime_stdout.log",
}


def _unexpected_transition_preseeded_paths(path: Path) -> list[str]:
    """Return pre-existing run-dir entries that would risk stale evidence.

    Phase 1 platform execution writes neutral lifecycle artifacts before
    handing off to this typed Apollo transition backend. Those files are safe
    to preserve and later merge. Anything else may be a stale run artifact and
    must keep blocking transition execution.
    """

    if not path.exists():
        return []
    unexpected: list[str] = []
    try:
        children = list(path.iterdir())
    except OSError:
        return [str(path)]
    for child in children:
        rel = child.relative_to(path).as_posix()
        if child.is_file() and child.name in _ALLOWED_TRANSITION_PRESEEDED_ROOT_FILES:
            continue
        if child.is_dir() and child.name == "execution":
            unexpected.extend(_unexpected_execution_preseeded_paths(child, root=path))
            continue
        unexpected.append(rel)
    return unexpected


def _unexpected_execution_preseeded_paths(path: Path, *, root: Path) -> list[str]:
    try:
        children = list(path.iterdir())
    except OSError:
        return [path.relative_to(root).as_posix()]
    unexpected: list[str] = []
    for child in children:
        rel = child.relative_to(root).as_posix()
        if child.is_file() and child.name in _ALLOWED_TRANSITION_PRESEEDED_EXECUTION_FILES:
            continue
        unexpected.append(rel)
    return unexpected


def _system_exit_code(exc: SystemExit) -> int:
    code = exc.code
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    return 2


def _merge_manifest_after_transition(
    root: Path,
    *,
    cfg: TestbedConfig,
    metadata: Mapping[str, Any],
    config_path: str | Path,
    legacy_effective_path: Path,
    start_wall: float,
    end_wall: float,
    exit_code: int,
    error_text: str,
    legacy_dispatch_requested: bool,
) -> None:
    manifest_path = root / "manifest.json"
    existing = _read_json(manifest_path)
    if not existing:
        route_path = _write_route_artifact(root, route_id=str(metadata["route_id"]), metadata=metadata)
        existing = _build_manifest_payload(
            cfg,
            config_path=config_path,
            start_wall_time_s=start_wall,
            metadata={**metadata, "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME},
            route_path=route_path,
            legacy_dispatch_requested=legacy_dispatch_requested,
        )
    metadata_block = existing.get("metadata") if isinstance(existing.get("metadata"), Mapping) else {}
    metadata_block = {
        **dict(metadata_block),
        "mode": TYPED_APOLLO_CLAIM_RUNTIME,
        "starts_carla": True,
        "starts_apollo": True,
        "legacy_dispatch_requested": bool(legacy_dispatch_requested),
        "transition_backend_returncode": int(exit_code),
        "transition_backend_error": error_text,
    }
    existing.update(
        {
            "run_id": metadata["run_id"],
            "scenario_id": metadata["scenario_id"],
            "scenario_class": metadata["scenario_class"],
            "route_id": metadata["route_id"],
            "backend": "apollo_cyberrt",
            "backend_name": "apollo_cyberrt",
            "typed_config_loaded": True,
            "legacy_fallback_used": False,
            "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME,
            "runtime_execution_mode": "transition_backend",
            "compat_layers": ["ros2_gt_transition", "legacy_route_health_transition"],
            "typed_runtime_effective_config_path": str(legacy_effective_path.relative_to(root)),
            "algorithm_variant_id": existing.get("algorithm_variant_id") or APOLLO_VARIANT_ID,
            "algorithm_variant_manifest_path": existing.get("algorithm_variant_manifest_path")
            or APOLLO_VARIANT_MANIFEST,
            "online_config_path": str(Path(config_path)),
            "online_config_profile_name": metadata["profile_name"],
            "map": existing.get("map") or metadata["map_name"],
            "transport_mode": APOLLO_CYBERRT_GT_TRANSPORT,
            "transport_mode_source": "typed_transition_backend",
            "truth_input": True,
            "duration_s": existing.get("duration_s")
            if existing.get("duration_s") not in {None, ""}
            else round(float(cfg.run.max_ticks) * float(cfg.run.fixed_dt_s), 6),
            "fixed_delta_seconds": existing.get("fixed_delta_seconds") or float(cfg.run.fixed_dt_s),
            "ticks": existing.get("ticks") or int(cfg.run.max_ticks),
            "start_time_wall_s": existing.get("start_time_wall_s") or start_wall,
            "end_time_wall_s": end_wall,
            "metadata": metadata_block,
        }
    )
    carla_world = existing.get("carla_world")
    if not isinstance(carla_world, Mapping):
        existing["carla_world"] = build_carla_world_identity(
            configured_town=str(metadata["map_name"]),
            loaded_map_name=None,
            spawn_point_count=None,
            source="transition_backend_not_observed_in_manifest",
            warnings=["runtime did not persist CARLA world identity"],
        )
    RunArtifactStore(root).write_manifest(existing)


def _merge_summary_after_transition(
    root: Path,
    *,
    cfg: TestbedConfig,
    metadata: Mapping[str, Any],
    wall_duration_s: float,
    exit_code: int,
    error_text: str,
) -> None:
    summary_path = root / "summary.json"
    summary = _read_json(summary_path)
    if not summary:
        summary = build_summary(
            success=exit_code == 0,
            exit_reason="success" if exit_code == 0 else "typed_transition_backend_failed",
            frames=0,
            sim_duration_s=0.0,
            wall_duration_s=wall_duration_s,
            cleanup_errors_count=0,
            metadata={},
        )
    why_not = list(summary.get("why_not_claimable") or [])
    if exit_code != 0 and "typed_transition_backend_failed" not in why_not:
        why_not.append("typed_transition_backend_failed")
    for reason in (
        "routing_response_decoded_insufficient_data",
        "apollo_hdmap_projection_insufficient_data",
        "localization_contract_insufficient_data",
        "control_handoff_insufficient_data",
    ):
        if reason not in why_not:
            why_not.append(reason)
    metadata_block = summary.get("metadata") if isinstance(summary.get("metadata"), Mapping) else {}
    metadata_block = {
        **dict(metadata_block),
        "mode": TYPED_APOLLO_CLAIM_RUNTIME,
        "transition_backend_returncode": int(exit_code),
        "transition_backend_error": error_text,
    }
    summary.update(
        {
            "run_id": metadata["run_id"],
            "scenario_id": metadata["scenario_id"],
            "scenario_class": metadata["scenario_class"],
            "route_id": metadata["route_id"],
            "backend": "apollo_cyberrt",
            "typed_config_loaded": True,
            "legacy_fallback_used": False,
            "runtime_dispatch_kind": TYPED_APOLLO_CLAIM_RUNTIME,
            "runtime_execution_mode": "transition_backend",
            "transport_mode": APOLLO_CYBERRT_GT_TRANSPORT,
            "truth_input": True,
            "success": bool(summary.get("success")) and exit_code == 0,
            "exit_reason": summary.get("exit_reason")
            if exit_code == 0
            else (summary.get("exit_reason") or "typed_transition_backend_failed"),
            "can_claim_unassisted_natural_driving": False,
            "why_not_claimable": why_not,
            "config_ticks": int(cfg.run.max_ticks),
            "config_fixed_delta_seconds": float(cfg.run.fixed_dt_s),
            "metadata": metadata_block,
        }
    )
    RunArtifactStore(root).write_summary(summary)


def _build_manifest_payload(
    cfg: TestbedConfig,
    *,
    config_path: str | Path,
    start_wall_time_s: float,
    metadata: Mapping[str, Any],
    route_path: Path,
    legacy_dispatch_requested: bool,
) -> dict[str, Any]:
    manifest = build_manifest(
        run_id=str(metadata["run_id"]),
        start_time_wall_s=start_wall_time_s,
        config_path=Path(config_path),
        carla_host=cfg.sim.host,
        carla_port=cfg.sim.port,
        carla_town=str(metadata["map_name"]),
        scenario_name=str(metadata["scenario_id"]),
        backend_name="apollo_cyberrt",
        metadata={
            "mode": COMPAT_APOLLO_CYBER_GT_RUNTIME,
            "starts_carla": False,
            "starts_apollo": False,
            "legacy_dispatch_requested": bool(legacy_dispatch_requested),
            "claim_boundary": "No Apollo capability claim is possible without real runtime evidence.",
        },
    )
    manifest.update(
        {
            "run_id": metadata["run_id"],
            "scenario_id": metadata["scenario_id"],
            "scenario_class": metadata["scenario_class"],
            "route_id": metadata["route_id"],
            "backend": "apollo_cyberrt",
            "backend_name": "apollo_cyberrt",
            "typed_config_loaded": True,
            "legacy_fallback_used": False,
            "runtime_dispatch_kind": metadata.get("runtime_dispatch_kind") or COMPAT_APOLLO_CYBER_GT_RUNTIME,
            "compat_layers": ["legacy_route_health_transition"] if legacy_dispatch_requested else [],
            "algorithm_variant_id": APOLLO_VARIANT_ID,
            "algorithm_variant_manifest_path": APOLLO_VARIANT_MANIFEST,
            "online_config_path": str(Path(config_path)),
            "online_config_profile_name": metadata["profile_name"],
            "map": metadata["map_name"],
            "transport_mode": APOLLO_CYBERRT_GT_TRANSPORT,
            "transport_mode_source": "typed_compat_runtime",
            "truth_input": True,
            "duration_s": round(float(cfg.run.max_ticks) * float(cfg.run.fixed_dt_s), 6),
            "fixed_delta_seconds": float(cfg.run.fixed_dt_s),
            "ticks": int(cfg.run.max_ticks),
            "route_path": "route.json",
            "carla_world": build_carla_world_identity(
                configured_town=str(metadata["map_name"]),
                loaded_map_name=None,
                spawn_point_count=None,
                source="configured_only_no_carla_world_probe",
                warnings=[
                    "compat_runtime_did_not_connect_to_carla",
                    "configured_map_identity_is_not_runtime_world_evidence",
                ],
            ),
            "map_version_contract": {
                "status": "insufficient_data",
                "configured_map": metadata["map_name"],
                "map_hash": None,
                "apollo_hdmap_hash": None,
                "warning": "map/version digest was not observed by compat runtime",
            },
            "routing_response": {
                "status": "insufficient_data",
                "source_channel": "/apollo/routing_response",
                "message_count": 0,
            },
            "guard_config_snapshot": _guard_config_snapshot(cfg),
            "assist_ledger": {
                "schema_version": "assist_ledger.v1",
                "active_assists": [],
                "blocking_assists": [],
                "non_blocking_assists": [],
                "assist_sources": {},
                "assist_confidence": "explicit",
                "source_artifact": "config",
                "can_claim_unassisted_natural_driving": True,
                "notes": ["no configured runtime assist was declared; real apply evidence is still missing"],
            },
            "config_aliases_used": _alias_texts(metadata["config_aliases_used"]),
        }
    )
    return manifest


def _build_summary_payload(
    cfg: TestbedConfig,
    *,
    wall_duration_s: float,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    summary = build_summary(
        success=False,
        exit_reason="apollo_online_runtime_not_executed_in_compat_path",
        frames=0,
        sim_duration_s=0.0,
        wall_duration_s=wall_duration_s,
        cleanup_errors_count=0,
        metadata={
            "mode": COMPAT_APOLLO_CYBER_GT_RUNTIME,
            "starts_carla": False,
            "starts_apollo": False,
            "claim_boundary": "No natural-driving capability claim; runtime evidence is insufficient.",
        },
    )
    summary.update(
        {
            "run_id": metadata["run_id"],
            "scenario_id": metadata["scenario_id"],
            "scenario_class": metadata["scenario_class"],
            "route_id": metadata["route_id"],
            "backend": "apollo_cyberrt",
            "typed_config_loaded": True,
            "legacy_fallback_used": False,
            "runtime_dispatch_kind": metadata.get("runtime_dispatch_kind") or COMPAT_APOLLO_CYBER_GT_RUNTIME,
            "transport_mode": APOLLO_CYBERRT_GT_TRANSPORT,
            "truth_input": True,
            "routing_success_count": 0,
            "planning_nonempty_trajectory_ratio": None,
            "control_rx_count": 0,
            "control_tx_count": 0,
            "control_apply_count": 0,
            "lateral_guard_apply_count": None,
            "trajectory_contract_guard_apply_count": None,
            "can_claim_unassisted_natural_driving": False,
            "why_not_claimable": [
                "apollo_online_runtime_not_executed",
                "routing_response_decoded_insufficient_data",
                "apollo_hdmap_projection_insufficient_data",
                "localization_contract_insufficient_data",
                "control_handoff_insufficient_data",
            ],
            "config_ticks": int(cfg.run.max_ticks),
            "config_fixed_delta_seconds": float(cfg.run.fixed_dt_s),
        }
    )
    return summary


def _write_core_insufficient_reports(
    root: Path,
    *,
    metadata: Mapping[str, Any],
    route_path: Path,
    stats_path: Path,
    overwrite: bool = True,
) -> None:
    route_id = str(metadata["route_id"])
    scenario_class = str(metadata["scenario_class"])
    common = {
        "run_id": metadata["run_id"],
        "scenario_id": metadata["scenario_id"],
        "scenario_class": scenario_class,
        "route_id": route_id,
    }
    _write_json_maybe(
        root / "analysis/route_health/route_health.json",
        {
            "schema_version": "route_health.v1",
            "status": "insufficient_data",
            **common,
            "route_source": "missing",
            "evidence_level": "insufficient",
            "hard_gate_eligible": False,
            "route_evidence_reason": "compat_runtime_did_not_observe_route_geometry",
            "missing_inputs": ["route_definition", "runtime_timeseries_route_projection"],
            "missing_fields": ["route_points", "length_m"],
            "warnings": ["typed_compat_runtime_route_stub_not_claim_grade"],
            "source": {
                "manifest_path": "manifest.json",
                "summary_path": "summary.json",
                "timeseries_path": "timeseries.csv",
                "route_path": str(route_path.relative_to(root)),
            },
        },
        overwrite=overwrite,
    )
    _write_text_maybe(root / "analysis/route_health/route_health_summary.md", _summary_md("Route Health", "insufficient_data"), overwrite=overwrite)
    _write_csv_maybe(root / "analysis/route_health/route_health.csv", ["route_id", "status"], [{"route_id": route_id, "status": "insufficient_data"}], overwrite=overwrite)
    _write_csv_maybe(root / "analysis/route_health/curve_segments.csv", ["segment_id", "status"], [], overwrite=overwrite)
    _write_json_maybe(
        root / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "insufficient_data",
            **common,
            "required_channels": [
                "/apollo/localization/pose",
                "/apollo/canbus/chassis",
                "/apollo/perception/obstacles",
                "/apollo/planning",
                "/apollo/control",
            ],
            "missing_channels": [
                "/apollo/localization/pose",
                "/apollo/canbus/chassis",
                "/apollo/planning",
                "/apollo/control",
            ],
            "source": {
                "config_path": "config.resolved.yaml",
                "stats_path": str(stats_path.relative_to(root)),
            },
            "warnings": ["compat_runtime_did_not_sample_cyber_channels"],
        },
        overwrite=overwrite,
    )
    _write_json_maybe(
        root / "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "apollo_localization_contract.v1",
            "status": {"measurement_time_available": False},
            "claim_grade": False,
            "verdict": {
                "status": "insufficient_data",
                "blocking_reasons": ["localization_runtime_samples_missing"],
            },
            **common,
            "channel": {"name": "/apollo/localization/pose", "status": "insufficient_data", "message_count": 0},
            "time": {"measurement_header_delta_ms_p95": None},
            "reference_point": {"vehicle_reference_hard_gate_eligible": False},
            "missing_fields": ["localization_timestamp", "measurement_time", "pose"],
            "warnings": ["compat_runtime_did_not_publish_or_observe_localization"],
            "source": {
                "timeseries_path": "timeseries.csv",
                "channel_stats_path": str(stats_path.relative_to(root)),
                "route_health_path": "analysis/route_health/route_health.json",
            },
        },
        overwrite=overwrite,
    )
    _write_json_maybe(
        root / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {
            "schema_version": "apollo_reference_line_contract_report.v1",
            "status": "insufficient_data",
            "claim_grade": False,
            **common,
            "blocking_reasons": ["apollo_reference_line_runtime_evidence_missing"],
            "warnings": ["compat_runtime_did_not_observe_planning_reference_line"],
            "source": {"run_dir": "."},
        },
        overwrite=overwrite,
    )
    _write_json_maybe(
        root / "analysis/control_health/control_health_report.json",
        {
            "schema_version": "control_health_report.v1",
            "status": "insufficient_data",
            **common,
            "blocking_reasons": ["control_runtime_trace_missing"],
            "warnings": ["compat_runtime_did_not_apply_control"],
            "source": {
                "summary_path": "summary.json",
                "manifest_path": "manifest.json",
                "timeseries_path": "timeseries.csv",
            },
        },
        overwrite=overwrite,
    )
    _write_json_maybe(
        root / "analysis/failure_timeline/failure_timeline_report.json",
        {
            "schema_version": "failure_timeline_report.v1",
            "status": "insufficient_data",
            **common,
            "events": [
                {
                    "stage": "runtime_dispatch",
                    "status": "insufficient_data",
                    "reason": "apollo_online_runtime_not_executed_in_compat_path",
                }
            ],
            "source": {
                "manifest_path": "manifest.json",
                "summary_path": "summary.json",
                "events_path": "events.jsonl",
                "timeseries_path": "timeseries.csv",
                "route_health_path": "analysis/route_health/route_health.json",
                "control_health_path": "analysis/control_health/control_health_report.json",
            },
        },
        overwrite=overwrite,
    )
    _write_json_maybe(
        root / "analysis/route_start_alignment/route_start_alignment_report.json",
        {
            "schema_version": "route_start_alignment_report.v1",
            "status": "insufficient_data",
            **common,
            "blocking_reasons": ["route_start_runtime_pose_missing"],
            "source": {
                "manifest_path": "manifest.json",
                "summary_path": "summary.json",
                "timeseries_path": "timeseries.csv",
                "failure_timeline_path": "analysis/failure_timeline/failure_timeline_report.json",
            },
        },
        overwrite=overwrite,
    )


def _write_secondary_reports(
    root: Path,
    *,
    metadata: Mapping[str, Any],
    overwrite: bool = True,
) -> dict[str, str]:
    outputs: dict[str, str] = {}
    common = {
        "run_id": metadata["run_id"],
        "scenario_id": metadata["scenario_id"],
        "scenario_class": metadata["scenario_class"],
        "route_id": metadata["route_id"],
    }
    routing_raw = root / "artifacts" / "routing_response_decoded.json"
    routing_jsonl = root / "artifacts" / "routing_response_decoded.jsonl"
    if overwrite or (not routing_raw.exists() and not routing_jsonl.exists()):
        _write_json(
            routing_raw,
            {
                "schema_version": "routing_response_decoded_source.v1",
                "source": "/apollo/routing_response",
                "message_count": 0,
                "status": "insufficient_data",
                "lane_segments": [],
                "total_length_m": None,
            },
        )
    routing_source = routing_raw if routing_raw.exists() else routing_jsonl
    routing_report = read_routing_response_decoded(routing_source)
    routing_report.update(common)
    routing_report_path = root / "analysis/routing_response_decoded/routing_response_decoded_report.json"
    existing_routing_report = _read_json(routing_report_path)
    should_write_routing_report = (
        overwrite
        or not routing_report_path.exists()
        or (
            routing_report.get("status") == "pass"
            and (existing_routing_report or {}).get("status") != "pass"
        )
    )
    if should_write_routing_report:
        outputs.update(write_routing_response_decoded_report(routing_report, root / "analysis/routing_response_decoded"))
    existing_jsonl_report = read_routing_response_decoded(routing_jsonl) if routing_jsonl.exists() else {}
    should_write_routing_jsonl = (
        overwrite
        or not routing_jsonl.exists()
        or (
            routing_report.get("status") == "pass"
            and existing_jsonl_report.get("status") != "pass"
        )
    )
    if should_write_routing_jsonl:
        if routing_report.get("status") == "pass":
            _write_jsonl(routing_jsonl, [routing_report])
        else:
            _write_jsonl(
                routing_jsonl,
                [{"status": "insufficient_data", "message_count": 0, "source": "/apollo/routing_response"}],
            )

    hdmap_jsonl = root / "artifacts" / "apollo_hdmap_projection.jsonl"
    hdmap_jsonl.parent.mkdir(parents=True, exist_ok=True)
    if overwrite or not hdmap_jsonl.exists():
        hdmap_jsonl.write_text("", encoding="utf-8")
    hdmap_report = analyze_apollo_hdmap_projection_file(hdmap_jsonl)
    hdmap_report.update(common)
    if overwrite or not (root / "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json").exists():
        outputs.update(write_apollo_hdmap_projection_report(hdmap_report, root / "analysis/apollo_hdmap_projection"))

    normalized_stats = normalize_channel_stats_for_run(root)
    if normalized_stats is not None:
        output_path = normalized_stats.get("_output_path")
        normalized_output_path = normalized_stats.get("_normalized_output_path")
        if output_path:
            outputs["channel_stats"] = str(output_path)
        if normalized_output_path:
            outputs["channel_stats_normalized"] = str(normalized_output_path)

    report_specs = {
        "analysis/planning_materialization/planning_materialization_report.json": {
            "schema_version": "planning_materialization_report.v1",
            "status": "insufficient_data",
            "route_establishment": {"route_established": False},
            "metrics": {"nonempty_trajectory_ratio": None},
            "blocking_reasons": ["planning_runtime_messages_missing"],
        },
        "analysis/apollo_control_handoff/apollo_control_handoff_report.json": {
            "schema_version": "apollo_control_handoff.v1",
            "status": "insufficient_data",
            "verdict": "insufficient_data",
            "blocking_reasons": ["control_runtime_messages_missing"],
        },
        "analysis/control_attribution/control_attribution_report.json": {
            "schema_version": "control_attribution.v1",
            "status": "insufficient_data",
            "verdict": "insufficient_data",
            "dominant_breakpoint": "insufficient_data",
            "missing_fields": ["apollo_steer_raw_runtime_samples"],
        },
        "analysis/chassis_gt_contract/chassis_gt_contract_report.json": {
            "schema_version": "chassis_gt_contract_report.v1",
            "status": "insufficient_data",
            "claim_grade": False,
            "blocking_reasons": ["chassis_runtime_samples_missing"],
        },
        "analysis/apollo_route_contract/apollo_route_contract_report.json": {
            "schema_version": "apollo_route_contract_report.v1",
            "status": "insufficient_data",
            "blocking_reasons": ["routing_response_runtime_evidence_missing"],
        },
        "analysis/prediction_evidence/prediction_evidence_report.json": {
            "schema_version": "prediction_evidence.v1",
            "status": "insufficient_data",
            "verdict": "insufficient_data",
            "prediction_mode": "unknown",
            "hard_gate_eligible": False,
            "blocking_capabilities": ["prediction_status_unknown"],
        },
        "analysis/apollo_module_consumption/apollo_module_consumption_report.json": {
            "schema_version": "apollo_module_consumption_report.v1",
            "status": "insufficient_data",
            "blocking_reasons": ["apollo_module_runtime_logs_missing"],
        },
        "analysis/apollo_link_health/apollo_link_health_report.json": {
            "schema_version": "apollo_link_health.v1",
            "status": "insufficient_data",
            "primary_blocker": "runtime_evidence_missing",
            "can_claim_unassisted_natural_driving": False,
        },
        "analysis/assist_ledger/assist_ledger.json": {
            "schema_version": "assist_ledger.v1",
            "status": "pass",
            "active_assists": [],
            "blocking_assists": [],
            "non_blocking_assists": [],
            "assist_confidence": "explicit",
            "source_artifact": "config",
            "can_claim_unassisted_natural_driving": True,
            "notes": ["runtime evidence still missing; this ledger only records configured assists"],
        },
    }
    for relative, payload in report_specs.items():
        enriched = dict(payload)
        enriched.update(common)
        _write_json_maybe(root / relative, enriched, overwrite=overwrite)
    return outputs


def _write_boundary_and_completeness(root: Path, *, metadata: Mapping[str, Any]) -> dict[str, str]:
    completeness = check_run_artifact_completeness(root, scenario_class=metadata["scenario_class"])
    outputs = write_run_artifact_completeness_report(
        completeness,
        root / "analysis" / "artifact_completeness",
    )
    outputs.update(build_and_write_evidence_bundle(root))
    return outputs


def _write_cyber_bridge_stats(root: Path, cfg: TestbedConfig, *, metadata: Mapping[str, Any]) -> Path:
    stats_path = root / "artifacts" / "cyber_bridge_stats.json"
    _write_json(
        stats_path,
        {
            "schema_version": "cyber_bridge_stats.v1",
            "status": "insufficient_data",
            "runtime_dispatch_kind": COMPAT_APOLLO_CYBER_GT_RUNTIME,
            "starts_apollo": False,
            "starts_carla": False,
            "route_id": metadata["route_id"],
            "localization": {"message_count": 0, "frame_id": None},
            "chassis": {"message_count": 0},
            "routing_response": {"message_count": 0},
            "planning": {"message_count": 0, "nonempty_trajectory_count": 0},
            "control": {"rx_count": 0, "tx_count": 0, "apply_count": 0},
            "configured_fixed_delta_seconds": float(cfg.run.fixed_dt_s),
        },
    )
    _write_json(root / "artifacts/bridge_health_summary.json", {"status": "insufficient_data", "reason": "compat_runtime_no_bridge_process"})
    _write_json(root / "artifacts/bridge_transport_summary.json", {"status": "insufficient_data", "transport_mode": APOLLO_CYBERRT_GT_TRANSPORT})
    _write_json(root / "artifacts/planning_topic_debug_summary.json", {"status": "insufficient_data", "message_count": 0})
    return stats_path


def _write_topic_publish_stats(root: Path) -> None:
    _write_jsonl(
        root / "artifacts/topic_publish_stats.jsonl",
        [
            {
                "status": "insufficient_data",
                "source": COMPAT_APOLLO_CYBER_GT_RUNTIME,
                "message": "no CyberRT channel samples were collected",
            }
        ],
    )


def _write_control_apply_trace(root: Path) -> None:
    _write_jsonl(
        root / "artifacts/control_apply_trace.jsonl",
        [
            {
                "status": "insufficient_data",
                "source": COMPAT_APOLLO_CYBER_GT_RUNTIME,
                "message": "no control apply trace was collected",
            }
        ],
    )


def _write_route_artifact(
    root: Path,
    *,
    route_id: str,
    metadata: Mapping[str, Any] | None = None,
) -> Path:
    path = root / "route.json"
    existing = _read_json(path)
    if existing and not _route_json_is_stub(existing):
        return path
    scenario_metadata = _scenario_metadata_for_route(root, metadata=metadata)
    route_trace = scenario_metadata.get("route_trace")
    if isinstance(route_trace, list) and route_trace:
        points = _route_points_from_trace(route_trace)
        if points:
            _write_json(
                path,
                _route_trace_payload(route_id=route_id, scenario_metadata=scenario_metadata, points=points),
            )
            return path
    return _write_route_stub(root, route_id=route_id)


def _write_route_stub(root: Path, *, route_id: str) -> Path:
    path = root / "route.json"
    _write_json(
        path,
        {
            "schema_version": "route_stub.v1",
            "route_id": route_id,
            "status": "insufficient_data",
            "points": [],
            "claim_boundary": "This stub preserves source traceability only; it is not route geometry evidence.",
        },
    )
    return path


def _route_trace_payload(
    *,
    route_id: str,
    scenario_metadata: Mapping[str, Any],
    points: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    map_name = _first_text(scenario_metadata.get("map"), scenario_metadata.get("map_name"), "Town01")
    return {
        "schema_version": "runtime_route_trace.v1",
        "route_id": _first_text(scenario_metadata.get("route_id"), route_id),
        "map": map_name,
        "map_name": map_name,
        "source": "artifacts/scenario_metadata.json:route_trace",
        "coordinate_frame": "carla_world",
        "target_projection_frame": "apollo_map",
        "status": "pass",
        "points": [dict(point) for point in points],
        "spawn_pose": scenario_metadata.get("spawn"),
        "goal_pose": scenario_metadata.get("goal"),
        "metadata": {
            "route_trace_source": scenario_metadata.get("route_trace_source"),
            "route_trace_point_count": len(points),
            "route_trace_length_m": scenario_metadata.get("route_trace_length_m"),
            "route_trace_length_source": scenario_metadata.get("route_trace_length_source"),
            "claim_route_length_m": scenario_metadata.get("claim_route_length_m"),
            "claim_route_length_source": scenario_metadata.get("claim_route_length_source"),
            "declared_route_length_m": scenario_metadata.get("route_length_m"),
            "route_length_m_role": scenario_metadata.get("route_length_m_role"),
            "route_length_m_claim_grade": scenario_metadata.get("route_length_m_claim_grade"),
            "route_selected_from_corpus": scenario_metadata.get("route_selected_from_corpus"),
            "route_corpus_path": scenario_metadata.get("route_corpus_path"),
        },
        "claim_boundary": (
            "This route geometry is the configured scenario route trace. "
            "Apollo natural-driving claims still require apollo_route_contract, "
            "HDMap projection, reference-line, localization, control, perception, "
            "and assist evidence."
        ),
    }


def _scenario_metadata_for_route(
    root: Path,
    *,
    metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    candidates: list[Any] = []
    artifact_metadata = _read_json(root / "artifacts" / "scenario_metadata.json")
    if artifact_metadata:
        candidates.append(artifact_metadata)
    manifest = _read_json(root / "manifest.json")
    manifest_metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), Mapping) else {}
    if isinstance(manifest_metadata, Mapping):
        candidates.append(manifest_metadata.get("scenario_metadata"))
    if isinstance(metadata, Mapping):
        candidates.append(metadata.get("scenario_metadata"))
        candidates.append(metadata)
    for candidate in candidates:
        if isinstance(candidate, Mapping):
            route_trace = candidate.get("route_trace")
            if isinstance(route_trace, list) and route_trace:
                return dict(candidate)
    return {}


def _route_points_from_trace(route_trace: Sequence[Any]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for index, item in enumerate(route_trace):
        if not isinstance(item, Mapping):
            continue
        if item.get("x") is None or item.get("y") is None:
            continue
        point: dict[str, Any] = {
            "index": _int_or_default(item.get("index"), index),
            "x": item.get("x"),
            "y": item.get("y"),
            "z": item.get("z", 0.0),
            "s": item.get("s"),
            "heading": item.get("heading"),
            "curvature": item.get("curvature"),
            "lane_id": item.get("lane_id"),
            "tags": item.get("tags") or [],
        }
        points.append({key: value for key, value in point.items() if value is not None})
    return points


def _route_json_is_stub(payload: Mapping[str, Any]) -> bool:
    points = payload.get("points")
    schema_version = str(payload.get("schema_version") or "")
    status = str(payload.get("status") or "")
    return schema_version == "route_stub.v1" or status == "insufficient_data" or not points


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _write_timeseries_csv(root: Path, *, route_id: str) -> None:
    row = {
        "sim_time": "0.0",
        "frame_id": "0",
        "route_id": route_id,
        "route_s": "",
        "apollo_steer_raw": "",
        "bridge_steer_mapped": "",
        "carla_steer_applied": "",
        "throttle_raw": "",
        "throttle_mapped": "",
        "throttle_applied": "",
        "brake_raw": "",
        "brake_mapped": "",
        "brake_applied": "",
        "lateral_guard_applied": "",
        "trajectory_contract_guard_applied": "",
    }
    _write_csv(root / "timeseries.csv", CONTROL_TRACE_FIELDS, [row])


def _guard_config_snapshot(cfg: TestbedConfig) -> dict[str, Any]:
    params = cfg.backend.params if isinstance(cfg.backend.params, Mapping) else {}
    keys = [
        "low_speed_steer_guard_enabled",
        "low_speed_sustained_saturation_guard_enabled",
        "sustained_lateral_guard_enabled",
        "trajectory_contract_lateral_guard_enabled",
    ]
    found = {key: _find_recursive(params, (key,)) for key in keys}
    return {
        "status": "recorded_config_only",
        "guard_evidence_status": "insufficient_data_no_runtime_apply_trace",
        "enabled_flags": {key: value for key, value in found.items() if value is not None},
        "apply_counts": {
            "lateral_guard_apply_count": None,
            "trajectory_contract_guard_apply_count": None,
        },
    }


def _scenario_class(
    *,
    capability_profile: str,
    scenario_id: str,
    route_id: str,
    profile_name: str,
) -> str:
    text = " ".join([capability_profile, scenario_id, route_id, profile_name]).lower()
    if "traffic_light_red_to_green" in text:
        return "traffic_light_red_to_green_release"
    if "traffic_light_red" in text or "red_stop" in text:
        return "traffic_light_red_stop"
    if "traffic_light_green" in text or "green_go" in text:
        return "traffic_light_green_go"
    if "traffic_light" in text:
        return "traffic_light_red_stop"
    if "junction" in text:
        return "junction_turn"
    if "curve" in text:
        return "curve_diagnostic"
    return "lane_keep"


def _route_from_run_dir(run_dir: Path) -> str:
    text = str(run_dir)
    for token in ("lane_keep_097", "lane097", "spawn097_goal046", "097"):
        if token in text:
            return "lane_keep_097" if token == "lane_keep_097" else "town01_rh_spawn097_goal046"
    return ""


def _find_recursive(payload: Any, keys: Sequence[str]) -> Any:
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            if str(key) in keys:
                return value
            found = _find_recursive(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_recursive(item, keys)
            if found is not None:
                return found
    return None


def _first_text(*values: Any) -> str:
    for value in values:
        if value in {None, ""}:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _first_non_placeholder_text(*values: Any) -> str:
    placeholders = {"unknown", "unknown_route", "none", "null"}
    for value in values:
        text = _first_text(value)
        if text and text.lower() not in placeholders:
            return text
    return ""


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _summary_md(title: str, status: str) -> str:
    return f"# {title}\n\n- Status: `{status}`\n- Claim boundary: `insufficient runtime evidence`\n"


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_json_maybe(path: Path, payload: Mapping[str, Any], *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        return
    _write_json(path, payload)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_json_safe(row), sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_text_maybe(path: Path, text: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        return
    _write_text(path, text)


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _write_csv_maybe(
    path: Path,
    fieldnames: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    *,
    overwrite: bool,
) -> None:
    if path.exists() and not overwrite:
        return
    _write_csv(path, fieldnames, rows)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _alias_texts(items: Any) -> list[str]:
    out: list[str] = []
    for item in items or []:
        if isinstance(item, Mapping):
            source = str(item.get("from") or "").strip()
            target = str(item.get("to") or "").strip()
            text = f"{source}->{target}" if source or target else json.dumps(dict(item), sort_keys=True)
        else:
            text = str(item)
        if text:
            out.append(text)
    return out
