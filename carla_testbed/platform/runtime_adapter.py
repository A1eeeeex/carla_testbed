from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.runtime_context import RuntimeContext
from carla_testbed.utils.env import resolve_repo_root


RUNTIME_ADAPTER_SCHEMA_VERSION = "backend_runtime_adapter.v1"


@dataclass(frozen=True)
class RuntimeCommandResult:
    status: str
    exit_code: int
    command: list[str] = field(default_factory=list)
    pid: int | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    duration_s: float = 0.0
    timed_out: bool = False
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "command": list(self.command),
            "pid": self.pid,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "duration_s": self.duration_s,
            "timed_out": self.timed_out,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class PostprocessResult:
    status: str
    commands: list[RuntimeCommandResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        for command in self.commands:
            if command.exit_code != 0:
                return command.exit_code
        return 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "commands": [command.to_dict() for command in self.commands],
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class CleanupResult:
    status: str
    terminated_process: bool = False
    closed_streams: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "terminated_process": self.terminated_process,
            "closed_streams": self.closed_streams,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class RuntimeAdapterResult:
    status: str
    exit_code: int
    command: RuntimeCommandResult
    postprocess: PostprocessResult
    cleanup: CleanupResult
    commands: list[RuntimeCommandResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": RUNTIME_ADAPTER_SCHEMA_VERSION,
            "status": self.status,
            "exit_code": self.exit_code,
            "command": self.command.to_dict(),
            "commands": [command.to_dict() for command in self.commands],
            "postprocess": self.postprocess.to_dict(),
            "cleanup": self.cleanup.to_dict(),
            "warnings": list(self.warnings),
        }


@dataclass
class RuntimeHandle:
    process: subprocess.Popen[str] | None
    command: list[str]
    stdout_path: Path | None
    stderr_path: Path | None
    stdout_file: Any = None
    stderr_file: Any = None
    started_wall_s: float = 0.0
    immediate_result: RuntimeCommandResult | None = None


class BackendRuntimeAdapter:
    """Thin lifecycle wrapper around a backend LaunchPlan.

    This class intentionally delegates behavior to existing backend commands.
    It records lifecycle evidence and process logs, but it does not reinterpret
    Apollo, CARLA, or fixed-scene runtime semantics.
    """

    def prepare(self, context: RuntimeContext, launch_plan: Mapping[str, Any]) -> dict[str, Any]:
        execution_dir = _execution_dir(context)
        execution_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": RUNTIME_ADAPTER_SCHEMA_VERSION,
            "status": "prepared",
            "run_id": context.plan.identity.run_id,
            "run_dir": str(context.run_dir),
            "backend": launch_plan.get("backend"),
            "mode": launch_plan.get("mode"),
            "starts_runtime": bool(launch_plan.get("starts_runtime")),
            "command_count": len(_commands(launch_plan)),
            "postprocess_command_count": len(_postprocess_commands(launch_plan)),
            "prepared_wall_time_s": time.time(),
        }
        _write_json(execution_dir / "runtime_adapter_state.json", payload)
        return payload

    def start(self, context: RuntimeContext, launch_plan: Mapping[str, Any]) -> RuntimeHandle:
        execution_dir = _execution_dir(context)
        commands = _commands(launch_plan)
        starts_runtime = bool(launch_plan.get("starts_runtime"))
        if context.dry_run:
            return RuntimeHandle(
                process=None,
                command=commands[0] if commands else [],
                stdout_path=None,
                stderr_path=None,
                immediate_result=RuntimeCommandResult(
                    status="dry_run",
                    exit_code=0,
                    command=commands[0] if commands else [],
                    warnings=["dry-run: backend command was not executed"],
                ),
            )
        if not commands:
            status = "missing_command" if starts_runtime else "completed"
            warning = (
                "launch plan starts runtime but has no command"
                if starts_runtime
                else "launch plan has no runtime command because backend is offline"
            )
            return RuntimeHandle(
                process=None,
                command=[],
                stdout_path=None,
                stderr_path=None,
                immediate_result=RuntimeCommandResult(
                    status=status,
                    exit_code=2 if starts_runtime else 0,
                    warnings=[warning],
                ),
            )
        if len(commands) > 1:
            return RuntimeHandle(
                process=None,
                command=[],
                stdout_path=None,
                stderr_path=None,
                immediate_result=RuntimeCommandResult(
                    status="unsupported_multi_command",
                    exit_code=2,
                    command=[],
                    warnings=[
                        "multiple launch commands present; BackendRuntimeAdapter refuses to execute a partial pipeline"
                    ],
                ),
            )
        command = commands[0]
        warnings = []
        stdout_path = execution_dir / "runtime_stdout.log"
        stderr_path = execution_dir / "runtime_stderr.log"
        stdout_file = stdout_path.open("w", encoding="utf-8")
        stderr_file = stderr_path.open("w", encoding="utf-8")
        env = {**os.environ, **{str(k): str(v) for k, v in dict(launch_plan.get("env") or {}).items()}}
        process = subprocess.Popen(
            command,
            cwd=resolve_repo_root(),
            env=env,
            text=True,
            stdout=stdout_file,
            stderr=stderr_file,
        )
        pid_path = execution_dir / "runtime.pid"
        pid_path.write_text(str(process.pid) + "\n", encoding="utf-8")
        _write_json(
            execution_dir / "runtime_adapter_state.json",
            {
                "schema_version": RUNTIME_ADAPTER_SCHEMA_VERSION,
                "status": "running",
                "run_id": context.plan.identity.run_id,
                "backend": launch_plan.get("backend"),
                "pid": process.pid,
                "command": command,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "started_wall_time_s": time.time(),
                "warnings": warnings,
            },
        )
        return RuntimeHandle(
            process=process,
            command=command,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            started_wall_s=time.time(),
        )

    def wait(self, handle: RuntimeHandle, *, timeout_s: float | None = None) -> RuntimeCommandResult:
        if handle.immediate_result is not None:
            return handle.immediate_result
        assert handle.process is not None
        started = handle.started_wall_s or time.time()
        timed_out = False
        try:
            exit_code = handle.process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            _terminate_process(handle.process)
            exit_code = handle.process.returncode
            if exit_code is None:
                exit_code = 124
        duration = time.time() - started
        status = "timeout" if timed_out else ("completed" if exit_code == 0 else "failed")
        return RuntimeCommandResult(
            status=status,
            exit_code=int(exit_code),
            command=list(handle.command),
            pid=handle.process.pid,
            stdout_path=str(handle.stdout_path) if handle.stdout_path else None,
            stderr_path=str(handle.stderr_path) if handle.stderr_path else None,
            duration_s=duration,
            timed_out=timed_out,
        )

    def stop(self, handle: RuntimeHandle) -> CleanupResult:
        terminated_process = False
        closed_streams = 0
        if handle.process is not None and handle.process.poll() is None:
            terminated_process = True
            _terminate_process(handle.process)
        for fh in (handle.stdout_file, handle.stderr_file):
            if fh is not None and not fh.closed:
                fh.close()
                closed_streams += 1
        if handle.process is None and closed_streams == 0:
            return CleanupResult(status="not_applicable")
        return CleanupResult(
            status="completed",
            terminated_process=terminated_process,
            closed_streams=closed_streams,
        )

    def postprocess(
        self,
        context: RuntimeContext,
        launch_plan: Mapping[str, Any],
        command_result: RuntimeCommandResult,
    ) -> PostprocessResult:
        if context.dry_run:
            return PostprocessResult(status="skipped", warnings=["dry-run: postprocess commands were not executed"])
        if not command_result.command:
            return PostprocessResult(
                status="skipped",
                warnings=["no runtime command executed; postprocess commands were not executed"],
            )
        commands = _postprocess_commands(launch_plan)
        if not commands:
            return PostprocessResult(status="not_applicable")
        warnings = []
        if command_result.exit_code != 0:
            warnings.append("runtime command failed; running safe postprocess because a command was attempted")
        results: list[RuntimeCommandResult] = []
        for idx, command in enumerate(commands):
            results.append(_run_postprocess_command(context, command, idx))
            if results[-1].exit_code != 0:
                return PostprocessResult(status="failed", commands=results, warnings=warnings)
        return PostprocessResult(status="completed", commands=results, warnings=warnings)

    def execute(
        self,
        context: RuntimeContext,
        launch_plan: Mapping[str, Any],
        *,
        timeout_s: float | None = None,
    ) -> RuntimeAdapterResult:
        warnings: list[str] = []
        self.prepare(context, launch_plan)
        handle = self.start(context, launch_plan)
        command_result = self.wait(handle, timeout_s=timeout_s)
        cleanup = self.stop(handle)
        postprocess = self.postprocess(context, launch_plan, command_result)
        status = command_result.status
        exit_code = command_result.exit_code
        if command_result.exit_code == 0 and postprocess.exit_code != 0:
            status = "postprocess_failed"
            exit_code = postprocess.exit_code
        warnings.extend(command_result.warnings)
        warnings.extend(postprocess.warnings)
        result = RuntimeAdapterResult(
            status=status,
            exit_code=exit_code,
            command=command_result,
            commands=[command_result],
            postprocess=postprocess,
            cleanup=cleanup,
            warnings=warnings,
        )
        _write_json(_execution_dir(context) / "runtime_adapter_result.json", result.to_dict())
        return result


def _run_postprocess_command(
    context: RuntimeContext,
    command: list[str],
    idx: int,
) -> RuntimeCommandResult:
    execution_dir = _execution_dir(context)
    stdout_path = execution_dir / f"postprocess_{idx}_stdout.log"
    stderr_path = execution_dir / f"postprocess_{idx}_stderr.log"
    started = time.time()
    with stdout_path.open("w", encoding="utf-8") as stdout_fh, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_fh:
        completed = subprocess.run(
            command,
            cwd=resolve_repo_root(),
            text=True,
            stdout=stdout_fh,
            stderr=stderr_fh,
            check=False,
            env=os.environ.copy(),
        )
    return RuntimeCommandResult(
        status="completed" if completed.returncode == 0 else "failed",
        exit_code=int(completed.returncode),
        command=list(command),
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
        duration_s=time.time() - started,
    )


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.SIGINT)
        process.wait(timeout=10.0)
    except Exception:
        if process.poll() is None:
            process.kill()
            try:
                process.wait(timeout=5.0)
            except Exception:
                pass


def _commands(launch_plan: Mapping[str, Any]) -> list[list[str]]:
    commands = launch_plan.get("commands")
    if not isinstance(commands, list):
        return []
    return [[str(part) for part in command] for command in commands if isinstance(command, list) and command]


def _postprocess_commands(launch_plan: Mapping[str, Any]) -> list[list[str]]:
    commands = launch_plan.get("postprocess_commands")
    if not isinstance(commands, list):
        return []
    return [[str(part) for part in command] for command in commands if isinstance(command, list) and command]


def _execution_dir(context: RuntimeContext) -> Path:
    return context.run_dir / "execution"


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
