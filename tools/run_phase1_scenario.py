#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_status import PHASE1_STATUS_SCHEMA_VERSION, write_phase1_status  # noqa: E402
from carla_testbed.backends.registry import default_backend_registry  # noqa: E402
from carla_testbed.platform.compiler import compile_run_plan, write_run_plan  # noqa: E402
from carla_testbed.platform.registry import PlatformRegistry  # noqa: E402


APOLLO_FIXED_SCENE_PREFLIGHT_SCHEMA_VERSION = "apollo_fixed_scene_preflight.v1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create a Phase 1 ScenarioRun scaffold. Apollo fixed-scene runs are "
            "not launched here; unavailable runtime is recorded as invalid/backend_not_ready."
        )
    )
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--backend", required=True, choices=("apollo_cyberrt", "carla_builtin", "autoware_ros2"))
    parser.add_argument("--algorithm", help="Algorithm profile. Defaults from --backend.")
    parser.add_argument("--record", "--recording", dest="recording", default="none")
    parser.add_argument("--gate", default="scenario_validation")
    parser.add_argument("--traffic", default="none")
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
    status, reasons = _preflight_status(
        backend=args.backend,
        fixed_scene_enabled=fixed_scene_enabled,
        backend_preflight=backend_preflight,
        launch_plan=launch_plan,
    )
    now = time.time()
    manifest = _manifest(
        plan=plan,
        contract=contract,
        launch_plan=launch_plan,
        run_dir=run_dir,
        preflight_status=status,
        start_time_wall_s=now,
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
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "preflight.json", preflight)
    _write_json(run_dir / "summary.json", summary)
    _write_events(run_dir / "events.jsonl", plan, status, reasons)
    phase1_status = _phase1_status(plan, manifest, summary, run_dir, reasons)
    write_phase1_status(phase1_status, run_dir / "analysis" / "phase1_status")
    print(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "preflight_status": status,
                "phase1_status": phase1_status["status"],
                "failure_reason": phase1_status["failure_reason"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if status == "ready" else 1


def _default_algorithm(backend: str) -> str:
    if backend == "apollo_cyberrt":
        return "apollo/apollo10_carla_gt"
    if backend == "carla_builtin":
        return "builtin/simple_acc_route_follower"
    if backend == "autoware_ros2":
        return "autoware/universe_gt_localization"
    raise ValueError(f"unsupported backend: {backend}")


def _preflight_status(
    *,
    backend: str,
    fixed_scene_enabled: bool,
    backend_preflight: Mapping[str, Any],
    launch_plan: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if backend == "apollo_cyberrt" and fixed_scene_enabled:
        reasons.append("apollo_fixed_scene_runtime_not_migrated")
    if backend_preflight.get("status") not in {"ready", "pass"}:
        reasons.extend(str(item) for item in backend_preflight.get("missing_requirements") or [])
    if not launch_plan.get("starts_runtime"):
        reasons.append("phase1_scaffold_does_not_start_runtime")
    reasons = sorted(set(item for item in reasons if item))
    return ("ready", []) if not reasons else ("backend_not_ready", reasons)


def _manifest(
    *,
    plan: Any,
    contract: Mapping[str, Any],
    launch_plan: Mapping[str, Any],
    run_dir: Path,
    preflight_status: str,
    start_time_wall_s: float,
) -> dict[str, Any]:
    return {
        "schema_version": "phase1_scenario_run_manifest.v1",
        "run_id": plan.identity.run_id,
        "scenario_id": plan.scenario.scenario_id,
        "scenario_class": plan.scenario.scenario_class,
        "scenario_case": plan.scenario.scenario_id,
        "map": plan.scenario.map,
        "backend": contract.get("backend"),
        "backend_name": contract.get("backend_name") or contract.get("backend"),
        "backend_type": contract.get("backend_type"),
        "backend_ready": preflight_status == "ready",
        "starts_runtime": False,
        "starts_carla": contract.get("starts_carla"),
        "starts_apollo": contract.get("starts_apollo"),
        "starts_autoware": contract.get("starts_autoware"),
        "input_contract": contract.get("input_contract"),
        "adapter_path": contract.get("adapter_path"),
        "available_truth_fields": contract.get("available_truth_fields") or [],
        "output_control_mode": contract.get("output_control_mode"),
        "transport_mode": contract.get("transport_mode"),
        "fixed_scene_enabled": bool(plan.scenario.fixed_scene),
        "expected_artifacts": launch_plan.get("expected_artifacts") or [],
        "launch_plan": launch_plan,
        "source_profiles": dict(plan.source_profiles),
        "start_time_wall_s": start_time_wall_s,
        "run_dir": str(run_dir),
        "claim_boundary": "phase1_preflight_manifest_not_behavior_evidence",
    }


def _phase1_status(
    plan: Any,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    run_dir: Path,
    reasons: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": PHASE1_STATUS_SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "run_id": plan.identity.run_id,
        "scenario_id": plan.scenario.scenario_id,
        "scenario_case": plan.scenario.scenario_id,
        "backend": manifest.get("backend"),
        "backend_name": manifest.get("backend_name"),
        "backend_type": manifest.get("backend_type"),
        "status": "invalid",
        "failure_reason": "backend_not_ready",
        "invalid_reasons": ["backend_not_ready"],
        "degraded_reasons": [],
        "failed_reasons": [],
        "original_failure_reason": summary.get("fail_reason"),
        "evaluable": False,
        "evidence_files": [
            str(run_dir / "manifest.json"),
            str(run_dir / "preflight.json"),
            str(run_dir / "summary.json"),
        ],
        "missing_artifacts": [],
        "required_artifacts": {
            "manifest": "present",
            "summary": "present",
            "preflight": "present",
            "timeseries": "missing",
            "v_t_gap_report": "missing",
        },
        "preflight_status": "backend_not_ready",
        "preflight_reasons": reasons,
        "v_t_gap_status": None,
        "summary_status": summary.get("status"),
        "summary_success": summary.get("success"),
        "notes": [
            "invalid_run_is_setup_or_evidence_failure_not_backend_loss",
            "apollo_fixed_scene_scaffold_does_not_execute_runtime",
        ],
        "claim_boundary": "phase1_status_is_scenario_evaluation_not_natural_driving_claim",
    }


def _write_events(path: Path, plan: Any, status: str, reasons: list[str]) -> None:
    rows = [
        {
            "event_type": "phase1_preflight_start",
            "run_id": plan.identity.run_id,
            "backend": plan.platform.name,
            "scenario_id": plan.scenario.scenario_id,
        },
        {
            "event_type": "phase1_preflight_end",
            "run_id": plan.identity.run_id,
            "status": status,
            "reasons": reasons,
        },
    ]
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
