from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.platform.plan import RunPlan

from .runtime_context import RuntimeContext, write_runtime_context_artifacts
from .runtime_adapter import BackendRuntimeAdapter, RuntimeAdapterResult


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    exit_code: int
    run_dir: Path
    launch_plan: dict[str, Any]
    preflight: dict[str, Any]
    backend_contract: dict[str, Any]
    artifacts: dict[str, str]
    dispatch: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "platform_execution_result.v1",
            "status": self.status,
            "exit_code": self.exit_code,
            "run_dir": str(self.run_dir),
            "launch_plan": self.launch_plan,
            "preflight": dict(self.preflight),
            "backend_contract": dict(self.backend_contract),
            "artifacts": dict(self.artifacts),
            "dispatch": dict(self.dispatch),
        }


def execute_run_plan(
    plan: RunPlan,
    *,
    run_dir: str | Path | None = None,
    dry_run: bool = True,
    legacy_dispatch: bool = False,
    timeout_s: float | None = None,
) -> ExecutionResult:
    context = RuntimeContext.from_plan(
        plan,
        run_dir=run_dir,
        dry_run=dry_run,
        legacy_dispatch=legacy_dispatch,
    )
    backend = default_backend_registry().for_plan(plan)
    backend_contract = backend.contract(plan).to_dict()
    preflight = backend.preflight(plan).to_dict()
    launch_plan = backend.build_launch_plan(plan).to_dict()
    launch_plan = _rewrite_launch_plan_run_dir(launch_plan, plan=plan, run_dir=context.run_dir)
    _write_json(context.run_dir / "preflight.json", preflight)
    write_runtime_context_artifacts(
        context,
        launch_plan=launch_plan,
        status="running" if not dry_run else "dry_run",
        compatibility_source=launch_plan.get("compatibility_source"),
        backend_contract=backend_contract,
        summary={
            "preflight": preflight,
            "dispatch": {"status": "starting" if not dry_run else "dry_run"},
        },
        preserve_existing=False,
    )
    adapter_result: RuntimeAdapterResult = BackendRuntimeAdapter().execute(
        context,
        launch_plan,
        timeout_s=timeout_s if timeout_s is not None else plan.world.timeout_s,
    )
    status = adapter_result.status
    artifacts = write_runtime_context_artifacts(
        context,
        launch_plan=launch_plan,
        status=status,
        compatibility_source=launch_plan.get("compatibility_source"),
        backend_contract=backend_contract,
        summary={"preflight": preflight, "dispatch": adapter_result.to_dict()},
        preserve_existing=not dry_run,
    )
    result = ExecutionResult(
        status=status,
        exit_code=adapter_result.exit_code,
        run_dir=context.run_dir,
        launch_plan=launch_plan,
        preflight=preflight,
        backend_contract=backend_contract,
        artifacts=artifacts,
        dispatch=adapter_result.to_dict(),
    )
    result_path = context.run_dir / "platform_execution_result.json"
    result_path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _rewrite_launch_plan_run_dir(
    launch_plan: dict[str, Any],
    *,
    plan: RunPlan,
    run_dir: Path,
) -> dict[str, Any]:
    default_run_dir = str(Path(str(plan.compatibility.get("output_root") or "runs")) / plan.identity.run_id)
    actual_run_dir = str(run_dir)
    if default_run_dir == actual_run_dir:
        return launch_plan

    def rewrite_command(command: list[Any]) -> list[str]:
        return [actual_run_dir if str(part) == default_run_dir else str(part) for part in command]

    rewritten = dict(launch_plan)
    rewritten["commands"] = [
        rewrite_command(command) for command in launch_plan.get("commands", []) if isinstance(command, list)
    ]
    rewritten["postprocess_commands"] = [
        rewrite_command(command)
        for command in launch_plan.get("postprocess_commands", [])
        if isinstance(command, list)
    ]
    warnings = list(launch_plan.get("warnings") or [])
    warnings.append(f"launch_plan_run_dir_rewritten:{default_run_dir}->{actual_run_dir}")
    rewritten["warnings"] = warnings
    return rewritten
