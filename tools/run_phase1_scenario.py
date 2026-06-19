#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_status import write_phase1_status  # noqa: E402
from carla_testbed.backends.registry import default_backend_registry  # noqa: E402
from carla_testbed.experiments.phase1_scaffold import (  # noqa: E402
    APOLLO_FIXED_SCENE_PREFLIGHT_SCHEMA_VERSION,
    build_phase1_scaffold_manifest,
    build_phase1_scaffold_status,
    missing_expected_artifacts,
    phase1_preflight_status,
    write_json,
    write_offline_fixed_scene_artifacts,
    write_phase1_scaffold_events,
)
from carla_testbed.platform.compiler import compile_run_plan, write_run_plan  # noqa: E402
from carla_testbed.platform.registry import PlatformRegistry  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create a Phase 1 ScenarioRun scaffold; unavailable runtime is invalid/backend_not_ready."
    )
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--backend", required=True, choices=("apollo_cyberrt", "carla_builtin", "autoware_ros2"))
    parser.add_argument("--algorithm", help="Algorithm profile. Defaults from --backend.")
    parser.add_argument("--record", "--recording", dest="recording", default="none")
    parser.add_argument("--gate", default="scenario_validation")
    parser.add_argument("--traffic", default="none")
    parser.add_argument("--bridge-config", help="Optional Apollo bridge config for fixed-scene readiness preflight.")
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir).expanduser()
    run_dir.mkdir(parents=True, exist_ok=True)
    plan = compile_run_plan(
        {
            "run": {"id": run_dir.name},
            "include": {
                "platform": args.backend,
                "algorithm": args.algorithm or _default_algorithm(args.backend),
                "scenario": args.scenario,
                "recording": args.recording,
                "traffic": args.traffic,
                "gate": args.gate,
            },
        },
        registry=PlatformRegistry(repo_root=REPO_ROOT),
    )
    backend = default_backend_registry().for_plan(plan)
    contract = backend.contract(plan).to_dict()
    backend_preflight = backend.preflight(plan).to_dict()
    launch_plan = backend.build_launch_plan(plan).to_dict()
    write_run_plan(plan, run_dir / "run_plan.resolved.yaml")
    write_run_plan(plan, run_dir / "config.resolved.yaml")

    fixed_scene_enabled = bool(plan.scenario.fixed_scene)
    offline_fixed_scene_artifacts = (
        write_offline_fixed_scene_artifacts(plan, run_dir, repo_root=REPO_ROOT, bridge_config_path=args.bridge_config)
        if fixed_scene_enabled else {}
    )
    status, reasons = phase1_preflight_status(
        backend=args.backend,
        fixed_scene_enabled=fixed_scene_enabled,
        backend_preflight=backend_preflight,
        launch_plan=launch_plan,
    )
    now = time.time()
    manifest = build_phase1_scaffold_manifest(
        plan=plan,
        contract=contract,
        launch_plan=launch_plan,
        run_dir=run_dir,
        preflight_status=status,
        start_time_wall_s=now,
        bridge_config_path=args.bridge_config,
    )
    preflight = {
        "schema_version": APOLLO_FIXED_SCENE_PREFLIGHT_SCHEMA_VERSION
        if args.backend == "apollo_cyberrt"
        else "phase1_backend_preflight.v1",
        "backend": args.backend,
        "status": status,
        "reasons": reasons,
        "backend_preflight": backend_preflight,
        "launch_plan": launch_plan,
        "target_actor_contract": manifest.get("target_actor_contract"),
        "expected_artifacts": launch_plan.get("expected_artifacts") or [],
        "missing_expected_artifacts": missing_expected_artifacts(
            run_dir, launch_plan.get("expected_artifacts") or [], assume_scaffold_written=True
        ),
        "offline_fixed_scene_artifacts": offline_fixed_scene_artifacts,
        "bridge_config_path": args.bridge_config,
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
        "backend": args.backend,
        "backend_name": args.backend,
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
    write_json(run_dir / "manifest.json", manifest)
    write_json(run_dir / "preflight.json", preflight)
    write_json(run_dir / "summary.json", summary)
    write_phase1_scaffold_events(run_dir / "events.jsonl", plan, status, reasons)
    phase1_status = build_phase1_scaffold_status(plan, manifest, summary, run_dir, reasons)
    write_phase1_status(phase1_status, run_dir / "analysis" / "phase1_status")
    result = {
        "run_dir": str(run_dir),
        "preflight_status": status,
        "phase1_status": phase1_status["status"],
        "failure_reason": phase1_status["failure_reason"],
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if status == "ready" else 1


def _default_algorithm(backend: str) -> str:
    if backend == "apollo_cyberrt":
        return "apollo/apollo10_carla_gt"
    if backend == "carla_builtin":
        return "builtin/simple_acc_route_follower"
    if backend == "autoware_ros2":
        return "autoware/universe_gt_localization"
    raise ValueError(f"unsupported backend: {backend}")


if __name__ == "__main__":
    raise SystemExit(main())
