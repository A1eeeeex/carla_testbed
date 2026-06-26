from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field, replace
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
    timeout_s: float | None = None
    warnings: list[str] = field(default_factory=list)
    completion_marker: dict[str, Any] | None = None

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
            "timeout_s": self.timeout_s,
            "warnings": list(self.warnings),
            "completion_marker": dict(self.completion_marker) if self.completion_marker else None,
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
            start_new_session=True,
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

    def wait(
        self,
        handle: RuntimeHandle,
        *,
        timeout_s: float | None = None,
        marker_root: Path | None = None,
        completion_markers: list[Mapping[str, Any]] | None = None,
        marker_poll_interval_s: float = 1.0,
    ) -> RuntimeCommandResult:
        if handle.immediate_result is not None:
            return replace(handle.immediate_result, timeout_s=timeout_s)
        assert handle.process is not None
        started = handle.started_wall_s or time.time()
        markers = [dict(marker) for marker in (completion_markers or []) if isinstance(marker, Mapping)]
        timed_out = False
        completion_marker: dict[str, Any] | None = None
        if markers and marker_root is not None:
            deadline = (started + float(timeout_s)) if timeout_s is not None else None
            while True:
                exit_code = handle.process.poll()
                if exit_code is not None:
                    break
                completion_marker = _find_completion_marker(marker_root, markers)
                if completion_marker is not None:
                    _terminate_process(handle.process)
                    success = completion_marker.get("success")
                    exit_code = 0 if success is not False else 1
                    break
                if deadline is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        timed_out = True
                        _terminate_process(handle.process)
                        exit_code = handle.process.returncode
                        if exit_code is None:
                            exit_code = 124
                        break
                    time.sleep(min(max(marker_poll_interval_s, 0.05), remaining))
                else:
                    time.sleep(max(marker_poll_interval_s, 0.05))
        else:
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
        warnings = []
        if completion_marker is not None:
            warnings.append(
                "runtime_completed_by_marker:"
                f"{completion_marker.get('id') or completion_marker.get('path')}"
            )
        return RuntimeCommandResult(
            status=status,
            exit_code=int(exit_code),
            command=list(handle.command),
            pid=handle.process.pid,
            stdout_path=str(handle.stdout_path) if handle.stdout_path else None,
            stderr_path=str(handle.stderr_path) if handle.stderr_path else None,
            duration_s=duration,
            timed_out=timed_out,
            timeout_s=timeout_s,
            warnings=warnings,
            completion_marker=completion_marker,
        )

    def stop(self, handle: RuntimeHandle, *, context: RuntimeContext | None = None) -> CleanupResult:
        terminated_process = False
        closed_streams = 0
        warnings: list[str] = []
        if handle.process is not None and handle.process.poll() is None:
            terminated_process = True
            _terminate_process(handle.process)
        if context is not None and handle.command:
            residual = _terminate_residual_processes_for_run_dir(context.run_dir)
            if residual:
                terminated_process = True
                warnings.append(f"terminated_residual_processes_for_run_dir:{len(residual)}")
                _write_json(_execution_dir(context) / "residual_process_cleanup.json", {"processes": residual})
        for fh in (handle.stdout_file, handle.stderr_file):
            if fh is not None and not fh.closed:
                fh.close()
                closed_streams += 1
        if handle.process is None and closed_streams == 0 and not warnings:
            return CleanupResult(status="not_applicable")
        return CleanupResult(
            status="completed",
            terminated_process=terminated_process,
            closed_streams=closed_streams,
            warnings=warnings,
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
        command_result = self.wait(
            handle,
            timeout_s=timeout_s,
            marker_root=context.run_dir,
            completion_markers=_completion_markers(launch_plan),
            marker_poll_interval_s=_completion_marker_poll_interval_s(launch_plan),
        )
        cleanup = self.stop(handle, context=context)
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


def _completion_markers(launch_plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    markers = launch_plan.get("runtime_completion_markers")
    if not isinstance(markers, list):
        return []
    return [dict(marker) for marker in markers if isinstance(marker, Mapping)]


def _completion_marker_poll_interval_s(launch_plan: Mapping[str, Any]) -> float:
    value = launch_plan.get("runtime_completion_marker_poll_interval_s")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = 1.0
    return max(0.05, parsed)


def _find_completion_marker(root: Path, markers: list[Mapping[str, Any]]) -> dict[str, Any] | None:
    for marker in markers:
        path_glob = str(marker.get("path_glob") or "").strip()
        field = str(marker.get("json_field") or "").strip()
        if not path_glob or not field:
            continue
        expected = marker.get("equals")
        for path in sorted(root.glob(path_glob)):
            if not path.is_file():
                continue
            payload = _read_json(path)
            if not isinstance(payload, dict):
                continue
            actual = _nested_get(payload, field)
            if expected is not None and actual != expected:
                continue
            success_field = str(marker.get("success_field") or "success").strip()
            success_value = _nested_get(payload, success_field) if success_field else None
            return {
                "id": marker.get("id"),
                "path": str(path),
                "path_glob": path_glob,
                "json_field": field,
                "expected": expected,
                "actual": actual,
                "success_field": success_field or None,
                "success": success_value if isinstance(success_value, bool) else None,
                "description": marker.get("description"),
            }
    return None


def _nested_get(payload: Mapping[str, Any], dotted_key: str) -> Any:
    current: Any = payload
    for part in dotted_key.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        pgid = os.getpgid(process.pid)
    except Exception:
        pgid = None
    try:
        if pgid is not None:
            os.killpg(pgid, signal.SIGINT)
        else:
            process.send_signal(signal.SIGINT)
        process.wait(timeout=10.0)
    except Exception:
        if process.poll() is None:
            try:
                if pgid is not None:
                    os.killpg(pgid, signal.SIGKILL)
                else:
                    process.kill()
            except Exception:
                process.kill()
            try:
                process.wait(timeout=5.0)
            except Exception:
                pass


def _terminate_residual_processes_for_run_dir(run_dir: Path) -> list[dict[str, Any]]:
    token = str(run_dir.expanduser())
    if not token:
        return []
    own_pid = os.getpid()
    parent_pid = os.getppid()
    try:
        own_pgid = os.getpgrp()
    except Exception:
        own_pgid = None
    matches: list[dict[str, Any]] = []
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        if pid in {own_pid, parent_pid}:
            continue
        try:
            raw = (proc_dir / "cmdline").read_bytes()
        except OSError:
            continue
        cmdline = raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()
        if token not in cmdline:
            continue
        try:
            pgid = os.getpgid(pid)
        except OSError:
            pgid = None
        if pgid is not None and own_pgid is not None and pgid == own_pgid:
            continue
        matches.append({"pid": pid, "pgid": pgid, "cmdline": cmdline[:1000]})
    for item in matches:
        _signal_residual_process(item, signal.SIGINT)
    time.sleep(1.0 if matches else 0.0)
    for item in matches:
        if Path(f"/proc/{item['pid']}").exists():
            _signal_residual_process(item, signal.SIGKILL)
    return matches


def _signal_residual_process(item: Mapping[str, Any], sig: signal.Signals) -> None:
    pid = int(item.get("pid") or 0)
    pgid_value = item.get("pgid")
    pgid = int(pgid_value) if pgid_value is not None else None
    try:
        if pgid is not None:
            os.killpg(pgid, sig)
        elif pid:
            os.kill(pid, sig)
    except ProcessLookupError:
        return
    except PermissionError:
        return
    except OSError:
        if pid:
            try:
                os.kill(pid, sig)
            except OSError:
                return


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
