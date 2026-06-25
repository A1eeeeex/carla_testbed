from __future__ import annotations

import os
import sys
import time
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
    assert result.cleanup.status == "completed"
    assert result.cleanup.closed_streams == 2
    assert (tmp_path / "run" / "execution" / "runtime_adapter_result.json").exists()


def test_backend_runtime_adapter_reports_subprocess_failure(tmp_path: Path) -> None:
    context = _context(tmp_path)
    postprocess_marker = tmp_path / "postprocess_was_run.txt"
    launch_plan = {
        "backend": "fake",
        "mode": "test",
        "starts_runtime": True,
        "commands": [[sys.executable, "-c", "raise SystemExit(7)"]],
        "postprocess_commands": [
            [sys.executable, "-c", f"from pathlib import Path; Path({str(postprocess_marker)!r}).write_text('ok')"]
        ],
    }

    result = BackendRuntimeAdapter().execute(context, launch_plan, timeout_s=5)

    assert result.status == "failed"
    assert result.exit_code == 7
    assert result.postprocess.status == "completed"
    assert result.cleanup.status == "completed"
    assert result.cleanup.closed_streams == 2
    assert "runtime command failed; running safe postprocess" in result.postprocess.warnings[0]
    assert postprocess_marker.read_text(encoding="utf-8") == "ok"


def test_backend_runtime_adapter_rejects_multiple_runtime_commands_without_partial_execution(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    first_marker = tmp_path / "first.txt"
    second_marker = tmp_path / "second.txt"
    launch_plan = {
        "backend": "fake",
        "mode": "test",
        "starts_runtime": True,
        "commands": [
            [sys.executable, "-c", f"from pathlib import Path; Path({str(first_marker)!r}).write_text('first')"],
            [sys.executable, "-c", f"from pathlib import Path; Path({str(second_marker)!r}).write_text('second')"],
        ],
    }

    result = BackendRuntimeAdapter().execute(context, launch_plan, timeout_s=5)

    assert result.status == "unsupported_multi_command"
    assert result.exit_code == 2
    assert result.cleanup.status == "not_applicable"
    assert "refuses to execute a partial pipeline" in result.warnings[0]
    assert not first_marker.exists()
    assert not second_marker.exists()


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
    assert result.cleanup.status == "completed"
    assert result.cleanup.closed_streams == 2


def test_backend_runtime_adapter_timeout_terminates_child_process_group(tmp_path: Path) -> None:
    context = _context(tmp_path)
    child_pid_path = tmp_path / "child.pid"
    script = (
        "import subprocess, sys, time\n"
        "from pathlib import Path\n"
        f"child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        f"Path({str(child_pid_path)!r}).write_text(str(child.pid))\n"
        "time.sleep(60)\n"
    )
    launch_plan = {
        "backend": "fake",
        "mode": "test",
        "starts_runtime": True,
        "commands": [[sys.executable, "-c", script]],
    }

    result = BackendRuntimeAdapter().execute(context, launch_plan, timeout_s=0.2)

    assert result.status == "timeout"
    assert child_pid_path.exists()
    child_pid = int(child_pid_path.read_text(encoding="utf-8"))
    for _ in range(20):
        if not _pid_exists(child_pid):
            break
        time.sleep(0.1)
    assert not _pid_exists(child_pid)


def test_backend_runtime_adapter_cleans_detached_residual_process_for_run_dir(tmp_path: Path) -> None:
    context = _context(tmp_path)
    child_pid_path = tmp_path / "detached_child.pid"
    script = (
        "import subprocess, sys\n"
        "from pathlib import Path\n"
        "child = subprocess.Popen("
        "[sys.executable, '-c', 'import time; time.sleep(60)', sys.argv[1]], "
        "start_new_session=True"
        ")\n"
        f"Path({str(child_pid_path)!r}).write_text(str(child.pid))\n"
        "raise SystemExit(1)\n"
    )
    launch_plan = {
        "backend": "fake",
        "mode": "test",
        "starts_runtime": True,
        "commands": [[sys.executable, "-c", script, str(context.run_dir)]],
    }

    result = BackendRuntimeAdapter().execute(context, launch_plan, timeout_s=5.0)

    assert result.status == "failed"
    assert result.cleanup.terminated_process is True
    assert any("terminated_residual_processes_for_run_dir" in item for item in result.cleanup.warnings)
    assert child_pid_path.exists()
    child_pid = int(child_pid_path.read_text(encoding="utf-8"))
    for _ in range(20):
        if not _pid_exists(child_pid):
            break
        time.sleep(0.1)
    assert not _pid_exists(child_pid)
    assert (context.run_dir / "execution" / "residual_process_cleanup.json").exists()


def _pid_exists(pid: int) -> bool:
    status_path = Path(f"/proc/{pid}/status")
    if status_path.exists():
        try:
            for line in status_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("State:") and "\tZ" in line:
                    return False
        except OSError:
            pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
