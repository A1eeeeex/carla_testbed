from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs, write_scenario_comparison
from carla_testbed.analysis.phase1_status import classify_phase1_run, write_phase1_status
from carla_testbed.backends.registry import default_backend_registry

from .carla_session import (
    Phase1CarlaStartupError,
    dry_run_carla_session_payload,
    start_phase1_carla_session,
    write_phase1_carla_session_payload,
)
from .compiler import compile_run_plan, write_run_plan
from .executor import ExecutionResult, _rewrite_launch_plan_run_dir, execute_run_plan
from .plan import RunPlan
from .registry import PlatformRegistry
from .runtime_context import RuntimeContext, write_runtime_context_artifacts


PAIR_RUNNER_SCHEMA_VERSION = "phase1_pair_run.v1"


@dataclass(frozen=True)
class Phase1PairRunResult:
    pair_id: str
    out_dir: Path
    plan_paths: list[Path]
    run_dirs: list[Path]
    execution_results: list[ExecutionResult]
    comparison_outputs: dict[str, str]
    manifest_path: Path
    carla_session: dict[str, Any]
    startup_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PAIR_RUNNER_SCHEMA_VERSION,
            "pair_id": self.pair_id,
            "out_dir": str(self.out_dir),
            "plan_paths": [str(path) for path in self.plan_paths],
            "run_dirs": [str(path) for path in self.run_dirs],
            "execution_results": [result.to_dict() for result in self.execution_results],
            "comparison_outputs": dict(self.comparison_outputs),
            "manifest_path": str(self.manifest_path),
            "carla_session": dict(self.carla_session),
            "startup_error": self.startup_error,
        }


def run_phase1_pair(
    *,
    scenario: str | Path,
    out_dir: str | Path,
    apollo_profile: str = "apollo/apollo10_carla_gt",
    planning_profile: str = "builtin/simple_acc_route_follower",
    apollo_platform: str = "apollo_cyberrt",
    planning_platform: str = "carla_builtin",
    recording: str = "metrics",
    gate: str = "scenario_validation",
    pair_id: str | None = None,
    dry_run: bool = False,
    timeout_s: float | None = None,
    start_carla: bool = False,
    carla_root: str | Path | None = None,
    carla_town: str | None = None,
    carla_extra_args: str = "-RenderOffScreen",
    carla_timeout_s: float = 90.0,
    registry: PlatformRegistry | None = None,
) -> Phase1PairRunResult:
    registry = registry or PlatformRegistry(repo_root=".")
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    resolved_pair_id = pair_id or _pair_id(scenario)
    plans_dir = output / "plans"
    runs_dir = output / "runs"
    comparison_dir = output / "comparison"
    plans_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)
    plans = [
        _compile_pair_plan(
            scenario=scenario,
            platform=planning_platform,
            algorithm=planning_profile,
            run_id=f"{resolved_pair_id}__planning_control",
            output_root=runs_dir,
            recording=recording,
            gate=gate,
            registry=registry,
        ),
        _compile_pair_plan(
            scenario=scenario,
            platform=apollo_platform,
            algorithm=apollo_profile,
            run_id=f"{resolved_pair_id}__apollo",
            output_root=runs_dir,
            recording=recording,
            gate=gate,
            registry=registry,
        ),
    ]
    plan_paths: list[Path] = []
    run_dirs: list[Path] = []
    for plan in plans:
        plan_paths.append(write_run_plan(plan, plans_dir / f"{plan.identity.run_id}.plan.resolved.yaml"))
        run_dirs.append(runs_dir / plan.identity.run_id)

    carla_session_obj = None
    resolved_carla_town = carla_town or _infer_carla_town(scenario)
    carla_session = dry_run_carla_session_payload(
        requested=bool(start_carla),
        carla_root=carla_root,
        town=resolved_carla_town,
        extra_args=carla_extra_args,
    )
    carla_session_path = write_phase1_carla_session_payload(
        out_dir=output / "carla_session",
        payload=carla_session,
    )
    startup_error: str | None = None
    if start_carla and not dry_run:
        try:
            carla_session_obj = start_phase1_carla_session(
                out_dir=output / "carla_session",
                carla_root=carla_root,
                town=resolved_carla_town,
                extra_args=carla_extra_args,
                timeout_s=carla_timeout_s,
            )
            carla_session = dict(carla_session_obj.payload)
            carla_session_path = carla_session_obj.status_path
        except Phase1CarlaStartupError as exc:
            startup_error = f"{exc.__class__.__name__}: {exc}"
            try:
                carla_session = json.loads(carla_session_path.read_text(encoding="utf-8"))
            except Exception:
                carla_session = {
                    **dict(carla_session),
                    "status": "startup_failed",
                    "error": startup_error,
                }

    execution_results: list[ExecutionResult] = []
    if startup_error:
        for plan, run_dir in zip(plans, run_dirs):
            execution_results.append(
                _materialize_carla_startup_blocked_result(
                    plan=plan,
                    run_dir=run_dir,
                    startup_error=startup_error,
                    carla_session={
                        **dict(carla_session),
                        "path": str(carla_session_path),
                    },
                )
            )
    else:
        try:
            for plan, run_dir in zip(plans, run_dirs):
                result = execute_run_plan(
                    plan,
                    run_dir=run_dir,
                    dry_run=dry_run,
                    legacy_dispatch=False,
                    timeout_s=timeout_s,
                )
                execution_results.append(result)
        finally:
            if carla_session_obj is not None:
                carla_session = carla_session_obj.stop()

    comparison_report = compare_scenario_runs(run_dirs)
    comparison_outputs = write_scenario_comparison(comparison_report, comparison_dir)
    manifest = {
        "schema_version": PAIR_RUNNER_SCHEMA_VERSION,
        "pair_id": resolved_pair_id,
        "scenario": str(scenario),
        "created_wall_time_s": time.time(),
        "dry_run": bool(dry_run),
        "timeout_s": timeout_s,
        "startup_error": startup_error,
        "carla_session": {
            **dict(carla_session),
            "path": str(carla_session_path),
        },
        "planning_control": {
            "platform": planning_platform,
            "algorithm": planning_profile,
            "run_dir": str(run_dirs[0]),
            "plan_path": str(plan_paths[0]),
            "execution_status": execution_results[0].status,
        },
        "apollo": {
            "platform": apollo_platform,
            "algorithm": apollo_profile,
            "run_dir": str(run_dirs[1]),
            "plan_path": str(plan_paths[1]),
            "execution_status": execution_results[1].status,
        },
        "comparison_outputs": comparison_outputs,
        "claim_boundary": (
            "Phase1 pair runner compares scenario evidence only. Invalid runs are setup/evidence "
            "failures and are not backend behavior losses."
        ),
    }
    manifest_path = output / "phase1_pair_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return Phase1PairRunResult(
        pair_id=resolved_pair_id,
        out_dir=output,
        plan_paths=plan_paths,
        run_dirs=run_dirs,
        execution_results=execution_results,
        comparison_outputs=comparison_outputs,
        manifest_path=manifest_path,
        carla_session={
            **dict(carla_session),
            "path": str(carla_session_path),
        },
        startup_error=startup_error,
    )


def _compile_pair_plan(
    *,
    scenario: str | Path,
    platform: str,
    algorithm: str,
    run_id: str,
    output_root: Path,
    recording: str,
    gate: str,
    registry: PlatformRegistry,
) -> RunPlan:
    return compile_run_plan(
        {
            "run": {
                "id": run_id,
                "output_root": str(output_root),
            },
            "include": {
                "platform": platform,
                "algorithm": algorithm,
                "scenario": str(scenario),
                "recording": recording,
                "traffic": "none",
                "gate": gate,
            },
        },
        registry=registry,
    )


def _pair_id(scenario: str | Path) -> str:
    raw = Path(str(scenario)).stem if str(scenario).endswith((".yaml", ".yml")) else str(scenario)
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in raw.strip().lower())
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    return f"phase1_pair_{cleaned or 'scenario'}_{int(time.time())}"


def _materialize_carla_startup_blocked_result(
    *,
    plan: RunPlan,
    run_dir: Path,
    startup_error: str,
    carla_session: dict[str, Any],
) -> ExecutionResult:
    context = RuntimeContext.from_plan(
        plan,
        run_dir=run_dir,
        dry_run=False,
        legacy_dispatch=False,
    )
    backend = default_backend_registry().for_plan(plan)
    backend_contract = backend.contract(plan).to_dict()
    preflight = backend.preflight(plan).to_dict()
    launch_plan = backend.build_launch_plan(plan).to_dict()
    launch_plan = _rewrite_launch_plan_run_dir(launch_plan, plan=plan, run_dir=context.run_dir)
    _write_json(context.run_dir / "preflight.json", preflight)
    dispatch = {
        "status": "blocked_by_carla_startup",
        "exit_code": 2,
        "error": startup_error,
        "carla_session": dict(carla_session),
        "claim_boundary": (
            "CARLA startup failure is setup/environment evidence and is not a backend behavior loss."
        ),
    }
    artifacts = write_runtime_context_artifacts(
        context,
        launch_plan=launch_plan,
        status="blocked_by_carla_startup",
        compatibility_source=launch_plan.get("compatibility_source"),
        backend_contract=backend_contract,
        summary={
            "success": False,
            "failure_reason": "backend_not_ready",
            "fail_reason": "backend_not_ready",
            "startup_error": startup_error,
            "carla_session": dict(carla_session),
            "preflight": preflight,
            "dispatch": dispatch,
        },
        preserve_existing=False,
    )
    result = ExecutionResult(
        status="blocked_by_carla_startup",
        exit_code=2,
        run_dir=context.run_dir,
        launch_plan=launch_plan,
        preflight=preflight,
        backend_contract=backend_contract,
        artifacts=artifacts,
        dispatch=dispatch,
    )
    _write_json(context.run_dir / "platform_execution_result.json", result.to_dict())
    write_phase1_status(
        classify_phase1_run(context.run_dir),
        context.run_dir / "analysis" / "phase1_status",
    )
    return result


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _infer_carla_town(scenario: str | Path) -> str:
    lowered = str(scenario).lower()
    if "baguang" in lowered or "straight_road_for_baguang" in lowered:
        return "straight_road_for_baguang"
    if "town01" in lowered:
        return "Town01"
    return "Town01"
