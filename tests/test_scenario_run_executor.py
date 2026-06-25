from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.executor import execute_run_plan
from carla_testbed.platform.registry import PlatformRegistry
from carla_testbed.platform.runtime_adapter import (
    CleanupResult,
    PostprocessResult,
    RuntimeAdapterResult,
    RuntimeCommandResult,
)


def test_execute_run_plan_without_runtime_command_completes_offline_backend(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="dummy",
        algorithm="dummy",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="smoke",
        registry=PlatformRegistry(repo_root="."),
    )

    result = execute_run_plan(plan, run_dir=tmp_path / "run", dry_run=False)

    assert result.status == "completed"
    assert result.exit_code == 0
    assert result.dispatch["command"]["warnings"] == [
        "launch plan has no runtime command because backend is offline"
    ]
    assert result.preflight["backend"] == "dummy"
    assert result.backend_contract["backend"] == "dummy"
    assert (tmp_path / "run" / "preflight.json").is_file()
    manifest = json.loads((tmp_path / "run" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["backend_contract"]["backend"] == "dummy"
    assert manifest["claim_boundary"]["schema_version"] == "phase1_claim_boundary.v1"
    summary = json.loads((tmp_path / "run" / "summary.json").read_text(encoding="utf-8"))
    assert summary["platform_runtime_status"] == "completed"
    assert summary["preflight"]["backend"] == "dummy"


def test_cli_run_plan_default_uses_executor_instead_of_not_implemented(tmp_path: Path) -> None:
    plan = compile_run_plan(
        platform="dummy",
        algorithm="dummy",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="smoke",
        registry=PlatformRegistry(repo_root="."),
    )
    plan_path = write_run_plan(plan, tmp_path / "plan.yaml")
    run_dir = tmp_path / "run"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "run",
            "--plan",
            str(plan_path),
            "--run-dir",
            str(run_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "runtime dispatch is not implemented" not in result.stderr
    assert "platform_execution_result.v1" in result.stdout
    assert (run_dir / "execution" / "runtime_adapter_result.json").exists()
    payload = json.loads((run_dir / "platform_execution_result.json").read_text(encoding="utf-8"))
    assert payload["preflight"]["backend"] == "dummy"
    assert payload["dispatch"]["cleanup"]["status"] == "not_applicable"


def test_execute_run_plan_materializes_phase1_status_when_postprocess_misses_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="builtin/simple_acc_route_follower",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    class FakeAdapter:
        def execute(self, context, launch_plan, *, timeout_s=None):  # noqa: ANN001
            return RuntimeAdapterResult(
                status="failed",
                exit_code=1,
                command=RuntimeCommandResult(
                    status="failed",
                    exit_code=1,
                    command=["fake-runtime"],
                    warnings=[],
                ),
                commands=[
                    RuntimeCommandResult(
                        status="failed",
                        exit_code=1,
                        command=["fake-runtime"],
                        warnings=[],
                    )
                ],
                postprocess=PostprocessResult(status="completed"),
                cleanup=CleanupResult(status="completed"),
                warnings=[],
            )

    monkeypatch.setattr("carla_testbed.platform.executor.BackendRuntimeAdapter", FakeAdapter)

    result = execute_run_plan(plan, run_dir=tmp_path / "run", dry_run=False)

    assert result.status == "failed"
    status_path = tmp_path / "run" / "analysis" / "phase1_status" / "phase1_status.json"
    assert status_path.exists()
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["status"] == "invalid"
    assert status["failure_reason"] == "no_timeseries"
    platform_result = json.loads((tmp_path / "run" / "platform_execution_result.json").read_text(encoding="utf-8"))
    assert platform_result["postprocess_status"] == "completed"
    assert platform_result["cleanup_status"] == "completed"
    assert "phase1_status_report" in platform_result["artifacts"]


def test_execute_run_plan_promotes_nested_timeseries_and_refreshes_no_timeseries_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compile_run_plan(
        platform="carla_builtin",
        algorithm="builtin/simple_acc_route_follower",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="scenario_validation",
        registry=PlatformRegistry(repo_root="."),
    )

    class FakeAdapter:
        def execute(self, context, launch_plan, *, timeout_s=None):  # noqa: ANN001
            nested = context.run_dir / "legacy_run" / "actual"
            nested_artifacts = nested / "artifacts"
            nested_artifacts.mkdir(parents=True)
            (nested / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0.0,1.0\n", encoding="utf-8")
            (nested / "events.jsonl").write_text(json.dumps({"event": "run_end"}) + "\n", encoding="utf-8")
            (nested_artifacts / "control_apply_trace.jsonl").write_text(
                json.dumps({"sim_time": 0.0, "steer": 0.0}) + "\n",
                encoding="utf-8",
            )
            (context.run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "schema_version": "legacy_runtime_summary.v1",
                        "success": False,
                        "exit_reason": "platform_timeout",
                        "status": "failed",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            stale_out = context.run_dir / "analysis" / "phase1_status"
            stale_out.mkdir(parents=True)
            (stale_out / "phase1_status.json").write_text(
                json.dumps(
                    {
                        "schema_version": "phase1_status.v1",
                        "status": "invalid",
                        "failure_reason": "no_timeseries",
                        "required_artifacts": {"timeseries": "missing"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            return RuntimeAdapterResult(
                status="timeout",
                exit_code=124,
                command=RuntimeCommandResult(
                    status="timeout",
                    exit_code=124,
                    command=["fake-runtime"],
                    timed_out=True,
                    warnings=[],
                ),
                commands=[
                    RuntimeCommandResult(
                        status="timeout",
                        exit_code=124,
                        command=["fake-runtime"],
                        timed_out=True,
                        warnings=[],
                    )
                ],
                postprocess=PostprocessResult(status="completed"),
                cleanup=CleanupResult(status="completed"),
                warnings=[],
            )

    monkeypatch.setattr("carla_testbed.platform.executor.BackendRuntimeAdapter", FakeAdapter)

    result = execute_run_plan(plan, run_dir=tmp_path / "run", dry_run=False)

    assert result.status == "timeout"
    assert (tmp_path / "run" / "timeseries.csv").read_text(encoding="utf-8").startswith("sim_time")
    normalization = json.loads(
        (
            tmp_path
            / "run"
            / "analysis"
            / "phase1_artifact_normalization"
            / "phase1_artifact_normalization_report.json"
        ).read_text(encoding="utf-8")
    )
    assert normalization["status"] == "promoted"
    status = json.loads(
        (tmp_path / "run" / "analysis" / "phase1_status" / "phase1_status.json").read_text(
            encoding="utf-8"
        )
    )
    assert status["status"] == "failed"
    assert status["failure_reason"] == "timeout"
    assert status["run_evaluable"] is True
    assert status["required_artifacts"]["timeseries"] == "present"
    artifact_completeness = json.loads(
        (tmp_path / "run" / "analysis" / "phase1_status" / "artifact_completeness.json").read_text(
            encoding="utf-8"
        )
    )
    assert artifact_completeness["status"] == "pass"
    assert artifact_completeness["artifact_complete"] is True
    assert (tmp_path / "run" / "events.jsonl").exists()
    assert (tmp_path / "run" / "artifacts" / "control_apply_trace.jsonl").exists()
    assert (tmp_path / "run" / "analysis" / "v_t_gap" / "v_t_gap_report.json").exists()
    platform_result = json.loads((tmp_path / "run" / "platform_execution_result.json").read_text(encoding="utf-8"))
    assert "phase1_artifact_normalization_report" in platform_result["artifacts"]
    assert "phase1_comparison_artifact_phase1_status_artifact_completeness" in platform_result["artifacts"]
    assert "phase1_status_report" in platform_result["artifacts"]
