from __future__ import annotations

import sys
from pathlib import Path

from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.runtime_adapter import BackendRuntimeAdapter
from carla_testbed.platform.runtime_context import RuntimeContext


def _context(tmp_path: Path) -> RuntimeContext:
    plan = compile_run_plan(
        platform="dummy",
        algorithm="dummy",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="smoke",
    )
    return RuntimeContext.from_plan(plan, run_dir=tmp_path / "run", dry_run=False)


def test_backend_runtime_adapter_records_successful_subprocess(tmp_path: Path) -> None:
    context = _context(tmp_path)
    launch_plan = {
        "backend": "fake",
        "mode": "test",
        "starts_runtime": True,
        "commands": [[sys.executable, "-c", "print('hello adapter')"]],
        "postprocess_commands": [],
    }

    result = BackendRuntimeAdapter().execute(context, launch_plan, timeout_s=5)

    assert result.status == "completed"
    assert result.exit_code == 0
    assert result.command.pid is not None
    assert result.command.stdout_path is not None
    assert "hello adapter" in Path(result.command.stdout_path).read_text(encoding="utf-8")
    assert (tmp_path / "run" / "execution" / "runtime_adapter_result.json").exists()


def test_backend_runtime_adapter_reports_subprocess_failure(tmp_path: Path) -> None:
    context = _context(tmp_path)
    launch_plan = {
        "backend": "fake",
        "mode": "test",
        "starts_runtime": True,
        "commands": [[sys.executable, "-c", "raise SystemExit(7)"]],
    }

    result = BackendRuntimeAdapter().execute(context, launch_plan, timeout_s=5)

    assert result.status == "failed"
    assert result.exit_code == 7


def test_backend_runtime_adapter_timeout_sends_cleanup_signal(tmp_path: Path) -> None:
    context = _context(tmp_path)
    launch_plan = {
        "backend": "fake",
        "mode": "test",
        "starts_runtime": True,
        "commands": [[sys.executable, "-c", "import time; time.sleep(60)"]],
    }

    result = BackendRuntimeAdapter().execute(context, launch_plan, timeout_s=0.1)

    assert result.status == "timeout"
    assert result.command.timed_out is True
    assert result.exit_code != 0
