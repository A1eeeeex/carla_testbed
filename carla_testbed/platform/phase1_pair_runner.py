from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs, write_scenario_comparison

from .compiler import compile_run_plan, write_run_plan
from .executor import ExecutionResult, execute_run_plan
from .plan import RunPlan
from .registry import PlatformRegistry


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
    execution_results: list[ExecutionResult] = []
    for plan in plans:
        plan_path = write_run_plan(plan, plans_dir / f"{plan.identity.run_id}.plan.resolved.yaml")
        run_dir = runs_dir / plan.identity.run_id
        result = execute_run_plan(
            plan,
            run_dir=run_dir,
            dry_run=dry_run,
            legacy_dispatch=False,
            timeout_s=timeout_s,
        )
        plan_paths.append(plan_path)
        run_dirs.append(run_dir)
        execution_results.append(result)

    comparison_report = compare_scenario_runs(run_dirs)
    comparison_outputs = write_scenario_comparison(comparison_report, comparison_dir)
    manifest = {
        "schema_version": PAIR_RUNNER_SCHEMA_VERSION,
        "pair_id": resolved_pair_id,
        "scenario": str(scenario),
        "created_wall_time_s": time.time(),
        "dry_run": bool(dry_run),
        "timeout_s": timeout_s,
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
