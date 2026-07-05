from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from carla_testbed.analysis.phase1_artifact_normalization import (
    ensure_phase1_comparison_artifacts,
    normalize_phase1_artifacts,
)
from carla_testbed.analysis.phase1_status import classify_phase1_run, write_phase1_status
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
        command = self.dispatch.get("command") if isinstance(self.dispatch.get("command"), dict) else {}
        postprocess = self.dispatch.get("postprocess") if isinstance(self.dispatch.get("postprocess"), dict) else {}
        cleanup = self.dispatch.get("cleanup") if isinstance(self.dispatch.get("cleanup"), dict) else {}
        return {
            "schema_version": "platform_execution_result.v1",
            "status": self.status,
            "exit_code": self.exit_code,
            "run_dir": str(self.run_dir),
            "launch_plan": self.launch_plan,
            "preflight": dict(self.preflight),
            "preflight_status": self.preflight.get("status"),
            "backend_contract": dict(self.backend_contract),
            "artifacts": dict(self.artifacts),
            "dispatch": dict(self.dispatch),
            "dispatch_status": self.status,
            "runtime_exit_code": command.get("exit_code", self.exit_code),
            "postprocess_status": postprocess.get("status"),
            "cleanup_status": cleanup.get("status"),
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
    requested_timeout_s = timeout_s if timeout_s is not None else plan.world.timeout_s
    effective_timeout_s, launch_plan = _apply_runtime_timeout_policy(
        launch_plan,
        requested_timeout_s=requested_timeout_s,
    )
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
        timeout_s=effective_timeout_s,
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
    normalization_report = normalize_phase1_artifacts(context.run_dir) if not dry_run else {}
    if normalization_report:
        artifacts["phase1_artifact_normalization_report"] = str(
            context.run_dir
            / "analysis"
            / "phase1_artifact_normalization"
            / "phase1_artifact_normalization_report.json"
        )
        artifacts["phase1_artifact_normalization_summary"] = str(
            context.run_dir
            / "analysis"
            / "phase1_artifact_normalization"
            / "phase1_artifact_normalization_summary.md"
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
    phase1_status_paths = _ensure_phase1_status_after_attempt(
        context.run_dir,
        dry_run=dry_run,
        adapter_result=adapter_result,
        artifact_normalization=normalization_report,
    )
    artifacts.update(phase1_status_paths)
    comparison_artifacts = ensure_phase1_comparison_artifacts(context.run_dir) if not dry_run else {}
    artifacts.update(
        {
            f"phase1_comparison_artifact_{key}": value
            for key, value in (comparison_artifacts.get("outputs") or {}).items()
        }
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
    result_path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def _ensure_phase1_status_after_attempt(
    run_dir: Path,
    *,
    dry_run: bool,
    adapter_result: RuntimeAdapterResult,
    artifact_normalization: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Materialize Phase 1 status when safe postprocess did not produce it.

    Legacy online commands may fail before their own postprocess chain reaches
    the Phase 1 classifier. The platform still needs an explicit invalid/failed
    status artifact so ScenarioComparison can avoid treating missing evidence as
    a backend loss or silently dropping the run from review.
    """

    if dry_run:
        return {}
    if (
        not adapter_result.command.command
        and adapter_result.command.status == "completed"
        and adapter_result.exit_code == 0
    ):
        return {}
    status_path = run_dir / "analysis" / "phase1_status" / "phase1_status.json"
    if status_path.exists() and not _phase1_status_needs_refresh(status_path, artifact_normalization):
        return {}
    paths = write_phase1_status(
        classify_phase1_run(run_dir),
        run_dir / "analysis" / "phase1_status",
    )
    return {f"phase1_status_{key}": value for key, value in paths.items()}


def _phase1_status_needs_refresh(
    status_path: Path,
    artifact_normalization: dict[str, Any] | None,
) -> bool:
    try:
        current = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return True
    if not isinstance(current, dict):
        return True
    if current.get("status") != "invalid" or current.get("failure_reason") != "no_timeseries":
        return False
    if (status_path.parents[2] / "timeseries.csv").exists() or (
        status_path.parents[2] / "timeseries.jsonl"
    ).exists():
        return True
    if artifact_normalization and artifact_normalization.get("status") == "promoted":
        return True
    return False


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _apply_runtime_timeout_policy(
    launch_plan: dict[str, Any],
    *,
    requested_timeout_s: float | None,
) -> tuple[float | None, dict[str, Any]]:
    minimum_timeout_s = _optional_float(launch_plan.get("minimum_runtime_timeout_s"))
    effective_timeout_s = requested_timeout_s
    reason = "requested_or_plan_timeout"
    policy_applied = False
    if minimum_timeout_s is not None and (effective_timeout_s is None or effective_timeout_s < minimum_timeout_s):
        effective_timeout_s = minimum_timeout_s
        reason = "backend_minimum_runtime_timeout"
        policy_applied = True

    enriched = dict(launch_plan)
    warnings = list(enriched.get("warnings") or [])
    if policy_applied:
        warnings.append(
            "runtime_timeout_raised_to_backend_minimum:"
            f"{requested_timeout_s}->{effective_timeout_s}"
        )
    enriched["warnings"] = warnings
    enriched["requested_runtime_timeout_s"] = requested_timeout_s
    enriched["effective_runtime_timeout_s"] = effective_timeout_s
    enriched["runtime_timeout_policy"] = {
        "requested_timeout_s": requested_timeout_s,
        "minimum_runtime_timeout_s": minimum_timeout_s,
        "effective_timeout_s": effective_timeout_s,
        "policy_applied": policy_applied,
        "reason": reason,
    }
    return effective_timeout_s, enriched


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
