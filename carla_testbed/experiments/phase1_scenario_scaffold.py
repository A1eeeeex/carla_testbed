from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from carla_testbed.analysis.phase1_status import write_phase1_status
from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.registry import PlatformRegistry

from .phase1_scaffold import (
    APOLLO_FIXED_SCENE_PREFLIGHT_SCHEMA_VERSION,
    apollo_fixed_scene_dispatch_summary,
    build_phase1_scaffold_manifest,
    build_phase1_scaffold_status,
    missing_expected_artifacts,
    phase1_preflight_status,
    write_json,
    write_offline_fixed_scene_artifacts,
    write_phase1_scaffold_events,
)


def write_phase1_scenario_scaffold(
    *,
    scenario: str,
    backend: str,
    run_dir: str | Path,
    repo_root: str | Path = ".",
    algorithm: str | None = None,
    recording: str = "none",
    gate: str = "scenario_validation",
    traffic: str = "none",
    bridge_config: str | None = None,
) -> dict[str, Any]:
    """Create a CI-safe Phase 1 ScenarioRun scaffold.

    The scaffold records plan/preflight/expected-artifact evidence only. It
    never starts CARLA, Apollo, Autoware, or ROS2 and must therefore keep
    runtime-unavailable cases invalid/backend_not_ready.
    """

    root = Path(repo_root).expanduser()
    output = Path(run_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    plan = compile_run_plan(
        {
            "run": {"id": output.name},
            "include": {
                "platform": backend,
                "algorithm": algorithm or default_algorithm_for_backend(backend),
                "scenario": scenario,
                "recording": recording,
                "traffic": traffic,
                "gate": gate,
            },
        },
        registry=PlatformRegistry(repo_root=root),
    )
    backend_facade = default_backend_registry().for_plan(plan)
    contract = backend_facade.contract(plan).to_dict()
    backend_preflight = backend_facade.preflight(plan).to_dict()
    launch_plan = backend_facade.build_launch_plan(plan).to_dict()
    write_run_plan(plan, output / "run_plan.resolved.yaml")
    write_run_plan(plan, output / "config.resolved.yaml")

    fixed_scene_enabled = bool(plan.scenario.fixed_scene)
    offline_fixed_scene_artifacts = (
        write_offline_fixed_scene_artifacts(
            plan,
            output,
            repo_root=root,
            launch_plan=launch_plan,
            bridge_config_path=bridge_config,
        )
        if fixed_scene_enabled
        else {}
    )
    status, reasons = phase1_preflight_status(
        backend=backend,
        fixed_scene_enabled=fixed_scene_enabled,
        backend_preflight=backend_preflight,
        launch_plan=launch_plan,
    )
    now = time.time()
    manifest = build_phase1_scaffold_manifest(
        plan=plan,
        contract=contract,
        launch_plan=launch_plan,
        run_dir=output,
        preflight_status=status,
        start_time_wall_s=now,
        bridge_config_path=bridge_config,
    )
    preflight = {
        "schema_version": (
            APOLLO_FIXED_SCENE_PREFLIGHT_SCHEMA_VERSION
            if backend == "apollo_cyberrt"
            else "phase1_backend_preflight.v1"
        ),
        "backend": backend,
        "status": status,
        "reasons": reasons,
        "backend_preflight": backend_preflight,
        "launch_plan": launch_plan,
        "target_actor_contract": manifest.get("target_actor_contract"),
        "expected_artifacts": launch_plan.get("expected_artifacts") or [],
        "missing_expected_artifacts": missing_expected_artifacts(
            output,
            launch_plan.get("expected_artifacts") or [],
            assume_scaffold_written=True,
        ),
        "offline_fixed_scene_artifacts": offline_fixed_scene_artifacts,
        "apollo_fixed_scene_dispatch_contract": apollo_fixed_scene_dispatch_summary(output),
        "bridge_config_path": bridge_config,
        "fixed_scene_enabled": fixed_scene_enabled,
        "claim_boundary": (
            "This scaffold records backend readiness only. It does not execute CARLA/Apollo "
            "and is not behavior evidence."
        ),
    }
    summary = {
        "schema_version": "phase1_scenario_run_summary.v1",
        "run_id": plan.identity.run_id,
        "scenario_id": plan.scenario.scenario_id,
        "scenario_class": plan.scenario.scenario_class,
        "scenario_case": plan.scenario.scenario_id,
        "backend": backend,
        "backend_name": backend,
        "backend_type": contract.get("backend_type"),
        "success": False,
        "status": "invalid",
        "fail_reason": "backend_not_ready" if status == "backend_not_ready" else "setup_failed",
        "phase1_status": "invalid",
        "phase1_failure_reason": "backend_not_ready" if status == "backend_not_ready" else "setup_failed",
        "starts_runtime": False,
        "sim_duration_s": 0.0,
        "wall_duration_s": 0.0,
        "claim_boundary": "invalid_preflight_scaffold_not_backend_behavior_loss",
    }
    write_json(output / "manifest.json", manifest)
    write_json(output / "preflight.json", preflight)
    write_json(output / "summary.json", summary)
    write_phase1_scaffold_events(output / "events.jsonl", plan, status, reasons)
    phase1_status = build_phase1_scaffold_status(plan, manifest, summary, output, reasons)
    write_phase1_status(phase1_status, output / "analysis" / "phase1_status")
    return {
        "run_dir": str(output),
        "preflight_status": status,
        "phase1_status": phase1_status["status"],
        "failure_reason": phase1_status["failure_reason"],
        "scenario_id": plan.scenario.scenario_id,
        "scenario_class": plan.scenario.scenario_class,
        "backend": backend,
        "exit_code": 0 if status == "ready" else 1,
    }


def default_algorithm_for_backend(backend: str) -> str:
    if backend == "apollo_cyberrt":
        return "apollo/apollo10_carla_gt"
    if backend == "carla_builtin":
        return "builtin/simple_acc_route_follower"
    if backend == "autoware_ros2":
        return "autoware/universe_gt_localization"
    raise ValueError(f"unsupported backend: {backend}")
