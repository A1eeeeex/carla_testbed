from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.platform.plan import RunPlan

from .legacy_dispatch import LegacyDispatchResult, dispatch_legacy_launch_plan
from .runtime_context import RuntimeContext, write_runtime_context_artifacts


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    exit_code: int
    run_dir: Path
    launch_plan: dict[str, Any]
    artifacts: dict[str, str]
    dispatch: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "platform_execution_result.v1",
            "status": self.status,
            "exit_code": self.exit_code,
            "run_dir": str(self.run_dir),
            "launch_plan": self.launch_plan,
            "artifacts": dict(self.artifacts),
            "dispatch": dict(self.dispatch),
        }


def execute_run_plan(
    plan: RunPlan,
    *,
    run_dir: str | Path | None = None,
    dry_run: bool = True,
    legacy_dispatch: bool = False,
) -> ExecutionResult:
    context = RuntimeContext.from_plan(
        plan,
        run_dir=run_dir,
        dry_run=dry_run,
        legacy_dispatch=legacy_dispatch,
    )
    backend = default_backend_registry().for_plan(plan)
    launch_plan = backend.build_launch_plan(plan).to_dict()
    dispatch_result: LegacyDispatchResult = dispatch_legacy_launch_plan(context, launch_plan)
    status = dispatch_result.status
    artifacts = write_runtime_context_artifacts(
        context,
        launch_plan=launch_plan,
        status=status,
        compatibility_source=launch_plan.get("compatibility_source"),
        summary={"dispatch": dispatch_result.to_dict()},
    )
    result = ExecutionResult(
        status=status,
        exit_code=dispatch_result.exit_code,
        run_dir=context.run_dir,
        launch_plan=launch_plan,
        artifacts=artifacts,
        dispatch=dispatch_result.to_dict(),
    )
    result_path = context.run_dir / "platform_execution_result.json"
    result_path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result
