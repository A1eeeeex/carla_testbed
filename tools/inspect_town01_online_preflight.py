#!/usr/bin/env python3
from __future__ import annotations

import argparse
import errno
import json
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_goal_online_runbook_results import build_results_payload
from tools.inspect_town01_goal_status import (  # noqa: E402
    CONDA_CARLA16_PYTHON,
    DEFAULT_OUTPUT_JSON as DEFAULT_GOAL_STATUS_JSON,
    build_goal_status,
)

DEFAULT_CONFIG = (
    REPO_ROOT / "configs" / "io" / "examples" / "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
)
DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_online_preflight_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_online_preflight_20260522.md"

CommandRunner = Callable[[Sequence[str], float], Dict[str, Any]]
RunbookResultsBuilder = Callable[..., Dict[str, Any]]
SocketProbe = Callable[[], Dict[str, Any]]


def _run_command(cmd: Sequence[str], timeout_s: float = 3.0) -> Dict[str, Any]:
    try:
        completed = subprocess.run(
            list(cmd),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(float(timeout_s), 0.5),
            check=False,
        )
        return {
            "cmd": list(cmd),
            "returncode": int(completed.returncode),
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
    except Exception as exc:
        return {
            "cmd": list(cmd),
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
        }


def _check(name: str, status: str, detail: str, *, severity: str = "info", data: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "severity": severity,
        "detail": detail,
        "data": data or {},
    }


def _read_mem_available_kib(meminfo_path: Path = Path("/proc/meminfo")) -> int | None:
    try:
        for line in meminfo_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1])
    except Exception:
        return None
    return None


def _process_lines(pattern: str, *, runner: CommandRunner) -> List[str]:
    result = runner(["pgrep", "-af", pattern], 3.0)
    stdout = str(result.get("stdout") or "")
    lines = []
    for line in stdout.splitlines():
        text = line.strip()
        if not text:
            continue
        if "inspect_town01_online_preflight.py" in text:
            continue
        lines.append(text)
    return lines


def _port_2000_lines(*, runner: CommandRunner) -> List[str]:
    result = runner(["ss", "-ltnp"], 3.0)
    stdout = str(result.get("stdout") or "")
    return [line.strip() for line in stdout.splitlines() if ":2000" in line]


def _socket_permission_probe() -> Dict[str, Any]:
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.1)
        sock.connect(("127.0.0.1", 9))
        return {"status": "ok", "detail": "TCP socket creation/connect allowed"}
    except ConnectionRefusedError:
        return {"status": "ok", "detail": "TCP socket creation allowed; localhost port 9 refused as expected"}
    except TimeoutError:
        return {"status": "ok", "detail": "TCP socket creation allowed; localhost port 9 timed out"}
    except PermissionError as exc:
        return {"status": "failed", "detail": f"{type(exc).__name__}: {exc}"}
    except OSError as exc:
        if exc.errno in {errno.EPERM, errno.EACCES}:
            return {"status": "failed", "detail": f"{type(exc).__name__}: {exc}"}
        return {"status": "warn", "detail": f"{type(exc).__name__}: {exc}"}
    except Exception as exc:
        return {"status": "warn", "detail": f"{type(exc).__name__}: {exc}"}
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def _load_goal_status_from_file(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_preflight_payload(
    *,
    config_path: Path = DEFAULT_CONFIG,
    python_exec: str = CONDA_CARLA16_PYTHON,
    repo_root: Path = REPO_ROOT,
    goal_status_json: Path = DEFAULT_GOAL_STATUS_JSON,
    refresh_goal_status: bool = True,
    command_runner: CommandRunner = _run_command,
    min_disk_free_gb: float = 8.0,
    min_mem_available_gb: float = 4.0,
    runbook_results_builder: RunbookResultsBuilder = build_results_payload,
    socket_probe: SocketProbe = _socket_permission_probe,
) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    resolved_config = config_path.expanduser().resolve()
    resolved_python = Path(python_exec).expanduser()
    checks.append(
        _check(
            "conda_carla16_python",
            "ok" if resolved_python.exists() else "failed",
            str(resolved_python),
            severity="error" if not resolved_python.exists() else "info",
        )
    )
    checks.append(
        _check(
            "direct_candidate_config",
            "ok" if resolved_config.exists() else "failed",
            str(resolved_config),
            severity="error" if not resolved_config.exists() else "info",
        )
    )

    if refresh_goal_status:
        goal_status = build_goal_status()
    else:
        goal_status = _load_goal_status_from_file(goal_status_json.expanduser().resolve())
    next_command = goal_status.get("next_command") if isinstance(goal_status.get("next_command"), dict) else {}
    checks.append(
        _check(
            "next_online_command",
            "ok" if str(next_command.get("command") or "").strip() else "failed",
            str(next_command.get("key") or ""),
            severity="error" if not str(next_command.get("command") or "").strip() else "info",
            data={"reason": str(next_command.get("reason") or "")},
        )
    )
    runbook_results = runbook_results_builder(python_exec=str(python_exec))
    broader_command = str(runbook_results.get("next_command") or "").strip()
    broader_key = str(runbook_results.get("next_key") or "").strip()
    checks.append(
        _check(
            "broader_validation_next_command",
            "ok" if broader_command else "failed",
            broader_key,
            severity="error" if not broader_command else "info",
            data={"command": broader_command, "runbook_status": str(runbook_results.get("status") or "")},
        )
    )
    expected_prefix = str(resolved_python)
    mismatched_commands = []
    if str(next_command.get("command") or "").strip() and not str(next_command.get("command") or "").strip().startswith(expected_prefix):
        mismatched_commands.append("direct_chain_next")
    if broader_command and not broader_command.startswith(expected_prefix):
        mismatched_commands.append("broader_validation_next")
    checks.append(
        _check(
            "online_commands_use_requested_python",
            "ok" if not mismatched_commands else "failed",
            "all online commands use requested python" if not mismatched_commands else ", ".join(mismatched_commands),
            severity="error" if mismatched_commands else "info",
            data={"python_exec": expected_prefix, "mismatched_commands": mismatched_commands},
        )
    )

    socket_result = socket_probe()
    socket_status = str(socket_result.get("status") or "warn")
    checks.append(
        _check(
            "tcp_socket_permission",
            socket_status if socket_status in {"ok", "warn", "failed"} else "warn",
            str(socket_result.get("detail") or ""),
            severity="error" if socket_status == "failed" else ("warning" if socket_status != "ok" else "info"),
            data={key: value for key, value in socket_result.items() if key not in {"status", "detail"}},
        )
    )

    try:
        disk = shutil.disk_usage(str(repo_root.expanduser().resolve()))
        free_gb = disk.free / (1024.0 ** 3)
        checks.append(
            _check(
                "repo_disk_free",
                "ok" if free_gb >= float(min_disk_free_gb) else "warn",
                f"{free_gb:.1f} GiB free",
                severity="warning" if free_gb < float(min_disk_free_gb) else "info",
                data={"free_gb": round(free_gb, 3), "min_disk_free_gb": float(min_disk_free_gb)},
            )
        )
    except Exception as exc:
        checks.append(_check("repo_disk_free", "warn", str(exc), severity="warning"))

    mem_available_kib = _read_mem_available_kib()
    if mem_available_kib is None:
        checks.append(_check("mem_available", "warn", "MemAvailable not readable", severity="warning"))
    else:
        mem_available_gb = mem_available_kib / (1024.0 ** 2)
        checks.append(
            _check(
                "mem_available",
                "ok" if mem_available_gb >= float(min_mem_available_gb) else "warn",
                f"{mem_available_gb:.1f} GiB available",
                severity="warning" if mem_available_gb < float(min_mem_available_gb) else "info",
                data={"available_gb": round(mem_available_gb, 3), "min_mem_available_gb": float(min_mem_available_gb)},
            )
        )

    carla_lines = _process_lines("CarlaUE4", runner=command_runner)
    checks.append(
        _check(
            "existing_carla_processes",
            "warn" if carla_lines else "ok",
            f"{len(carla_lines)} matching process(es)",
            severity="warning" if carla_lines else "info",
            data={"matches": carla_lines[:8]},
        )
    )
    chain_lines = _process_lines(
        "run_town01_capability_online_chain.py|run_town01_route_health.py|examples.run_followstop",
        runner=command_runner,
    )
    checks.append(
        _check(
            "existing_town01_runner_processes",
            "warn" if chain_lines else "ok",
            f"{len(chain_lines)} matching process(es)",
            severity="warning" if chain_lines else "info",
            data={"matches": chain_lines[:8]},
        )
    )
    port_lines = _port_2000_lines(runner=command_runner)
    checks.append(
        _check(
            "port_2000_listeners",
            "warn" if port_lines else "ok",
            f"{len(port_lines)} listener line(s)",
            severity="warning" if port_lines else "info",
            data={"matches": port_lines[:8]},
        )
    )

    failed = [item for item in checks if item.get("status") == "failed"]
    warnings = [item for item in checks if item.get("status") == "warn"]
    if failed:
        status = "failed"
    elif warnings:
        status = "ready_with_warnings"
    else:
        status = "ready"
    return {
        "status": status,
        "checks": checks,
        "failed_count": len(failed),
        "warning_count": len(warnings),
        "goal_status": {
            "overall_status": goal_status.get("overall_status", ""),
            "blockers": goal_status.get("blockers", []),
            "next_command": next_command,
        },
        "broader_validation": {
            "status": runbook_results.get("status", ""),
            "next_key": broader_key,
            "next_command": broader_command,
        },
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Online Preflight",
        "",
        f"- status: `{payload['status']}`",
        f"- failed_count: `{payload['failed_count']}`",
        f"- warning_count: `{payload['warning_count']}`",
        f"- goal_status: `{payload.get('goal_status', {}).get('overall_status', '')}`",
        f"- goal_blockers: `{', '.join(payload.get('goal_status', {}).get('blockers', [])) or 'none'}`",
        f"- broader_validation_next_key: `{payload.get('broader_validation', {}).get('next_key', '')}`",
        "",
        "## Checks",
        "",
        "| check | status | severity | detail |",
        "|---|---|---|---|",
    ]
    for item in payload.get("checks", []):
        lines.append(
            "| `{}` | `{}` | `{}` | {} |".format(
                item.get("name", ""),
                item.get("status", ""),
                item.get("severity", ""),
                str(item.get("detail", "")).replace("|", "\\|"),
            )
        )
    next_command = payload.get("goal_status", {}).get("next_command", {})
    lines.extend(
        [
            "",
            "## Direct Chain Next Command",
            "",
            f"- key: `{next_command.get('key', '')}`",
            f"- reason: `{next_command.get('reason', '')}`",
            "",
            "```bash",
            str(next_command.get("command", "")),
            "```",
            "",
            "## Broader Validation Next Command",
            "",
            f"- key: `{payload.get('broader_validation', {}).get('next_key', '')}`",
            "",
            "```bash",
            str(payload.get("broader_validation", {}).get("next_command", "")),
            "```",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect local readiness before a Town01 online run without launching CARLA/Apollo.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--goal-status-json", type=Path, default=DEFAULT_GOAL_STATUS_JSON)
    parser.add_argument("--refresh-goal-status", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--min-disk-free-gb", type=float, default=8.0)
    parser.add_argument("--min-mem-available-gb", type=float, default=4.0)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--require-ready", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_preflight_payload(
        config_path=args.config,
        python_exec=str(args.python_exec),
        goal_status_json=args.goal_status_json,
        refresh_goal_status=bool(args.refresh_goal_status),
        min_disk_free_gb=float(args.min_disk_free_gb),
        min_mem_available_gb=float(args.min_mem_available_gb),
    )
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"status": payload["status"], "failed_count": payload["failed_count"], "warning_count": payload["warning_count"]}, indent=2))
    if args.require_ready and payload["status"] != "ready":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
