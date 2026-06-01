from __future__ import annotations

import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from carla_testbed.experiments.natural_driving_runner import (
    DEFAULT_ROUTE_HEALTH_CONFIG,
    NaturalDrivingRunnerConfig,
    build_run_matrix,
    build_suite_manifest,
    postprocess_and_audit_command,
    summarize_suite_coverage,
    write_run_matrix_csv,
    write_suite_manifest,
)
from carla_testbed.experiments.natural_driving_schema import (
    SUPPORTED_SCENARIO_CLASSES,
    load_natural_driving_suite,
)

READINESS_SCHEMA_VERSION = "town01_natural_driving_readiness.v1"
DEFAULT_CARLA16_PYTHON = Path("/home/ubuntu/miniconda3/envs/carla16/bin/python3")
DEFAULT_CARLA_ROOT = Path("/home/ubuntu/CARLA_0.9.16")
DEFAULT_APOLLO_CONTAINER = "apollo_neo_dev_10.0.0_pkg"
DEFAULT_APOLLO_CORE_DIR = Path("/home/ubuntu/Apollo10.0/application-core/data/core")
DEFAULT_MIN_DISK_FREE_GB = 50.0
DEFAULT_MAX_APOLLO_CORE_DUMP_GB = 20.0
REQUIRED_SCENARIO_CLASSES = tuple(sorted(SUPPORTED_SCENARIO_CLASSES))


@dataclass(frozen=True)
class CommandResult:
    return_code: int
    stdout: str = ""
    stderr: str = ""


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True)
class NaturalDrivingReadinessConfig:
    suite_path: Path = Path("configs/scenarios/town01_natural_driving_suite.yaml")
    out_dir: Path = Path("runs/readiness/town01_natural_driving")
    strict_runtime: bool = False
    python_exec: Path = DEFAULT_CARLA16_PYTHON
    carla_root: Path = DEFAULT_CARLA_ROOT
    apollo_container: str = DEFAULT_APOLLO_CONTAINER
    apollo_core_dir: Path = DEFAULT_APOLLO_CORE_DIR
    min_disk_free_gb: float = DEFAULT_MIN_DISK_FREE_GB
    max_apollo_core_dump_gb: float = DEFAULT_MAX_APOLLO_CORE_DUMP_GB
    fixed_delta_seconds: float = 0.05
    route_health_config: str = DEFAULT_ROUTE_HEALTH_CONFIG


def build_town01_natural_driving_readiness(
    config: NaturalDrivingReadinessConfig,
    *,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    runner = command_runner or _run_command
    blockers: list[str] = []
    warnings: list[str] = []

    suite_status, suite, matrix, manifest = _build_suite_snapshot(config, blockers, warnings)
    runtime = _inspect_runtime(config, runner, blockers, warnings)

    status = _overall_status(blockers, warnings)
    next_commands = _next_commands(config)
    report = {
        "schema_version": READINESS_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "strict_runtime": config.strict_runtime,
        "needs_local_carla": True,
        "needs_local_apollo": True,
        "suite": suite_status,
        "runtime": runtime,
        "blockers": blockers,
        "warnings": warnings,
        "claim_boundary": {
            "readiness_proves_behavior": False,
            "readiness_proves_natural_driving": False,
            "required_behavior_artifact": "natural_driving_report.json",
            "notes": (
                "This report only checks whether the Town01 truth-input suite can be planned "
                "and whether local runtime prerequisites look ready. It does not prove Apollo "
                "natural-driving behavior or full perception/localization reproduction."
            ),
        },
        "next_commands": next_commands,
        "remediation": _remediation_actions(config, blockers, warnings, next_commands),
    }
    if suite is not None:
        report["suite"]["mode"] = dict(suite.get("mode") or {})
    if manifest:
        report["suite"]["analysis_commands"] = dict(manifest.get("analysis_commands") or {})
    if matrix:
        report["suite"]["matrix_preview_rows"] = len(matrix)
    return report


def write_readiness_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "town01_natural_driving_readiness.json"
    md_path = output / "town01_natural_driving_readiness.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_readiness_markdown(report), encoding="utf-8")
    return {
        "readiness_json": str(json_path),
        "readiness_md": str(md_path),
    }


def write_readiness_plan_preview(
    config: NaturalDrivingReadinessConfig,
    matrix: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> dict[str, str]:
    output = Path(config.out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    matrix_path = output / "run_matrix.preview.csv"
    manifest_path = output / "suite_manifest.preview.json"
    write_run_matrix_csv(matrix_path, matrix)
    write_suite_manifest(manifest_path, manifest)
    return {
        "run_matrix_preview": str(matrix_path),
        "suite_manifest_preview": str(manifest_path),
    }


def render_readiness_markdown(report: Mapping[str, Any]) -> str:
    suite = report.get("suite") if isinstance(report.get("suite"), Mapping) else {}
    runtime = report.get("runtime") if isinstance(report.get("runtime"), Mapping) else {}
    coverage = suite.get("coverage") if isinstance(suite.get("coverage"), Mapping) else {}
    lines = [
        "# Town01 Natural Driving Readiness",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- strict_runtime: `{report.get('strict_runtime')}`",
        f"- suite: `{suite.get('suite_path')}`",
        f"- scenarios: `{coverage.get('total')}` total, `{coverage.get('runnable')}` runnable",
        f"- python: `{runtime.get('python', {}).get('path') if isinstance(runtime.get('python'), Mapping) else None}`",
        f"- CARLA root: `{runtime.get('carla', {}).get('root') if isinstance(runtime.get('carla'), Mapping) else None}`",
        f"- Apollo container: `{runtime.get('apollo_container', {}).get('status') if isinstance(runtime.get('apollo_container'), Mapping) else None}`",
        "",
        "## Blockers",
    ]
    blockers = list(report.get("blockers") or [])
    lines.extend(f"- {item}" for item in blockers) if blockers else lines.append("- none")
    lines.append("")
    lines.append("## Warnings")
    warnings = list(report.get("warnings") or [])
    lines.extend(f"- {item}" for item in warnings) if warnings else lines.append("- none")
    lines.append("")
    core_dumps = runtime.get("apollo_core_dumps") if isinstance(runtime.get("apollo_core_dumps"), Mapping) else {}
    lines.append("## Apollo Core Dumps")
    lines.append(f"- path: `{core_dumps.get('path')}`")
    lines.append(f"- status: `{core_dumps.get('status')}`")
    lines.append(f"- files: `{core_dumps.get('file_count')}`")
    lines.append(f"- total_gb: `{core_dumps.get('total_gb')}`")
    lines.append(f"- max_total_gb: `{core_dumps.get('max_total_gb')}`")
    lines.append("")
    lines.append("## Remediation")
    remediation = report.get("remediation") if isinstance(report.get("remediation"), Mapping) else {}
    actions = remediation.get("actions") if isinstance(remediation.get("actions"), list) else []
    if actions:
        for action in actions:
            if not isinstance(action, Mapping):
                continue
            lines.append(f"- `{action.get('name')}`: `{action.get('command')}`")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Boundary")
    lines.append(
        "- This readiness report does not prove Apollo natural-driving behavior; "
        "behavior claims require `natural_driving_report.json` and run artifacts."
    )
    lines.append("- Truth-input mode is not full Apollo perception/localization reproduction.")
    lines.append("")
    lines.append("## Next Commands")
    commands = report.get("next_commands") if isinstance(report.get("next_commands"), Mapping) else {}
    if commands:
        for name, command in commands.items():
            lines.append(f"- `{name}`: `{command}`")
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _build_suite_snapshot(
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> tuple[dict[str, Any], dict[str, Any] | None, list[dict[str, Any]], dict[str, Any]]:
    suite_path = Path(config.suite_path).expanduser()
    if not suite_path.exists():
        blockers.append(f"suite_missing:{suite_path}")
        return (
            {
                "suite_path": str(suite_path),
                "load_status": "missing",
                "coverage": {},
                "required_scenario_classes_missing": list(REQUIRED_SCENARIO_CLASSES),
            },
            None,
            [],
            {},
        )
    try:
        suite = load_natural_driving_suite(suite_path)
    except Exception as exc:
        blockers.append(f"suite_invalid:{type(exc).__name__}:{exc}")
        return (
            {
                "suite_path": str(suite_path),
                "load_status": "invalid",
                "error": str(exc),
                "coverage": {},
                "required_scenario_classes_missing": list(REQUIRED_SCENARIO_CLASSES),
            },
            None,
            [],
            {},
        )

    runner_config = NaturalDrivingRunnerConfig(
        suite_path=suite_path,
        out_dir=Path(config.out_dir).expanduser() / "planned_suite",
        dry_run=True,
        continue_on_failure=True,
        python_exec=str(config.python_exec),
        fixed_delta_seconds=float(config.fixed_delta_seconds),
        route_health_config=str(config.route_health_config),
    )
    matrix = build_run_matrix(runner_config)
    manifest = build_suite_manifest(runner_config, matrix)
    coverage = summarize_suite_coverage(matrix)
    online_config = manifest.get("online_config") if isinstance(manifest.get("online_config"), Mapping) else {}
    provenance_issues = _online_config_provenance_issues(matrix, online_config)
    observed_classes = {str(row.get("scenario_class") or "") for row in matrix}
    missing_classes = sorted(set(REQUIRED_SCENARIO_CLASSES) - observed_classes)
    if missing_classes:
        blockers.append(f"suite_missing_scenario_classes:{','.join(missing_classes)}")
    if not matrix:
        blockers.append("suite_matrix_empty")
    if int(coverage.get("unrunnable") or 0) > 0:
        blockers.append("suite_has_unrunnable_scenarios")
    blockers.extend(provenance_issues)

    traffic_light = coverage.get("traffic_light") if isinstance(coverage.get("traffic_light"), Mapping) else {}
    if int(traffic_light.get("claim_grade") or 0) < 3:
        warnings.append("traffic_light_claim_grade_coverage_incomplete")

    return (
        {
            "suite_path": str(suite_path),
            "load_status": "pass",
            "schema_version": suite.get("schema_version"),
            "suite_name": suite.get("name"),
            "map": suite.get("map"),
            "coverage": coverage,
            "online_config": dict(online_config),
            "online_config_provenance_issues": provenance_issues,
            "required_scenario_classes": list(REQUIRED_SCENARIO_CLASSES),
            "required_scenario_classes_missing": missing_classes,
            "dry_run_plan": {
                "out_dir": str(runner_config.out_dir),
                "run_count": len(matrix),
                "runnable_count": int(coverage.get("runnable") or 0),
                "unrunnable_count": int(coverage.get("unrunnable") or 0),
            },
        },
        suite,
        matrix,
        manifest,
    )


def _inspect_runtime(
    config: NaturalDrivingReadinessConfig,
    runner: CommandRunner,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    python = _inspect_python(config, blockers, warnings)
    carla = _inspect_carla(config, blockers, warnings)
    disk = _inspect_disk(config, blockers, warnings)
    core_dumps = _inspect_apollo_core_dumps(config, blockers, warnings)
    docker = _inspect_docker(runner, config, blockers, warnings)
    container = _inspect_apollo_container(runner, config, blockers, warnings)
    processes = _inspect_processes(runner)
    return {
        "python": python,
        "carla": carla,
        "disk": disk,
        "apollo_core_dumps": core_dumps,
        "docker": docker,
        "apollo_container": container,
        "processes": processes,
    }


def _inspect_python(
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    path = Path(config.python_exec).expanduser()
    exists = path.exists()
    version = None
    if exists:
        result = _run_command([str(path), "--version"])
        version = (result.stdout or result.stderr).strip()
    message = f"python_missing:{path}"
    if not exists:
        _runtime_problem(config, blockers, warnings, message)
    return {
        "path": str(path),
        "exists": exists,
        "version": version,
    }


def _inspect_carla(
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    root = Path(config.carla_root).expanduser()
    launcher = root / "CarlaUE4.sh"
    exists = root.exists()
    launcher_exists = launcher.exists()
    if not exists:
        _runtime_problem(config, blockers, warnings, f"carla_root_missing:{root}")
    if exists and not launcher_exists:
        _runtime_problem(config, blockers, warnings, f"carla_launcher_missing:{launcher}")
    return {
        "root": str(root),
        "root_exists": exists,
        "launcher": str(launcher),
        "launcher_exists": launcher_exists,
    }


def _inspect_disk(
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    usage = shutil.disk_usage(Path(config.out_dir).expanduser().anchor or "/")
    free_gb = usage.free / (1024**3)
    if free_gb < config.min_disk_free_gb:
        _runtime_problem(
            config,
            blockers,
            warnings,
            f"disk_free_below_threshold:{free_gb:.1f}GB<{config.min_disk_free_gb:.1f}GB",
        )
    return {
        "path": str(Path(config.out_dir).expanduser()),
        "free_gb": round(free_gb, 3),
        "min_free_gb": float(config.min_disk_free_gb),
        "ok": free_gb >= config.min_disk_free_gb,
    }


def _inspect_apollo_core_dumps(
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    core_dir = Path(config.apollo_core_dir).expanduser()
    if not core_dir.exists():
        return {
            "path": str(core_dir),
            "exists": False,
            "file_count": 0,
            "total_gb": 0.0,
            "max_total_gb": float(config.max_apollo_core_dump_gb),
            "ok": True,
            "status": "missing_ok",
        }
    total_bytes = 0
    file_count = 0
    largest: list[tuple[int, str]] = []
    try:
        for path in core_dir.iterdir():
            if not path.is_file() or not path.name.startswith("core_"):
                continue
            try:
                size = path.stat().st_size
            except OSError:
                continue
            file_count += 1
            total_bytes += size
            largest.append((size, path.name))
    except OSError as exc:
        _runtime_problem(config, blockers, warnings, f"apollo_core_dump_scan_failed:{core_dir}:{exc}")
        return {
            "path": str(core_dir),
            "exists": True,
            "status": "scan_failed",
            "error": str(exc),
            "ok": False,
        }
    total_gb = total_bytes / (1024**3)
    ok = total_gb <= float(config.max_apollo_core_dump_gb)
    if not ok:
        _runtime_problem(
            config,
            blockers,
            warnings,
            (
                "apollo_core_dump_total_above_threshold:"
                f"{total_gb:.1f}GB>{float(config.max_apollo_core_dump_gb):.1f}GB"
            ),
        )
    largest_sorted = sorted(largest, reverse=True)[:5]
    return {
        "path": str(core_dir),
        "exists": True,
        "file_count": file_count,
        "total_gb": round(total_gb, 3),
        "max_total_gb": float(config.max_apollo_core_dump_gb),
        "ok": ok,
        "status": "ok" if ok else "above_threshold",
        "largest_files": [
            {"name": name, "size_gb": round(size / (1024**3), 3)}
            for size, name in largest_sorted
        ],
        "note": (
            "Apollo core dumps are crash diagnostics, not run evidence. Large backlogs "
            "increase disk pressure and should be archived or removed manually before long online suites."
        ),
    }


def _inspect_docker(
    runner: CommandRunner,
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    result = runner(["docker", "info", "--format", "{{.ServerVersion}}"])
    ok = result.return_code == 0
    version = result.stdout.strip() if ok else None
    if not ok:
        _runtime_problem(config, blockers, warnings, f"docker_unavailable:{result.stderr.strip() or result.return_code}")
    return {
        "available": ok,
        "server_version": version,
        "return_code": result.return_code,
        "stderr": result.stderr.strip(),
    }


def _inspect_apollo_container(
    runner: CommandRunner,
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    result = runner(
        [
            "docker",
            "inspect",
            "--format",
            "{{.Name}} {{.State.Status}} {{.State.ExitCode}}",
            config.apollo_container,
        ]
    )
    if result.return_code != 0:
        _runtime_problem(
            config,
            blockers,
            warnings,
            f"apollo_container_missing_or_uninspectable:{config.apollo_container}",
        )
        return {
            "name": config.apollo_container,
            "status": "unknown",
            "running": False,
            "return_code": result.return_code,
            "stderr": result.stderr.strip(),
        }
    parts = result.stdout.strip().split()
    status = parts[1] if len(parts) >= 2 else "unknown"
    exit_code = parts[2] if len(parts) >= 3 else None
    running = status == "running"
    state_details = _inspect_apollo_container_state(runner, config.apollo_container)
    if not running:
        _runtime_problem(
            config,
            blockers,
            warnings,
            f"apollo_container_not_running:{config.apollo_container}:{status}:{exit_code}",
        )
    return {
        "name": config.apollo_container,
        "status": status,
        "exit_code": exit_code,
        "running": running,
        "raw": result.stdout.strip(),
        "state": state_details,
    }


def _inspect_apollo_container_state(
    runner: CommandRunner,
    container_name: str,
) -> dict[str, Any]:
    result = runner(
        [
            "docker",
            "inspect",
            "--format",
            "{{json .State}}",
            container_name,
        ]
    )
    if result.return_code != 0:
        return {
            "available": False,
            "return_code": result.return_code,
            "stderr": result.stderr.strip(),
        }
    try:
        payload = json.loads(result.stdout.strip() or "{}")
    except json.JSONDecodeError:
        return {
            "available": False,
            "raw": result.stdout.strip(),
            "parse_error": "invalid_json",
        }
    return {
        "available": True,
        "status": payload.get("Status"),
        "running": payload.get("Running"),
        "oom_killed": payload.get("OOMKilled"),
        "dead": payload.get("Dead"),
        "exit_code": payload.get("ExitCode"),
        "error": payload.get("Error"),
        "started_at": payload.get("StartedAt"),
        "finished_at": payload.get("FinishedAt"),
    }


def _inspect_processes(runner: CommandRunner) -> dict[str, Any]:
    pattern = "CarlaUE4|carla|apollo|cyber|dreamview|ros2|rviz"
    result = runner(["pgrep", "-af", pattern])
    matches = [line for line in result.stdout.splitlines() if line.strip()]
    return {
        "pattern": pattern,
        "matches": matches,
        "match_count": len(matches),
        "return_code": result.return_code,
    }


def _runtime_problem(
    config: NaturalDrivingReadinessConfig,
    blockers: list[str],
    warnings: list[str],
    message: str,
) -> None:
    if config.strict_runtime:
        blockers.append(message)
    else:
        warnings.append(message)


def _overall_status(blockers: Sequence[str], warnings: Sequence[str]) -> str:
    if blockers:
        return "not_ready"
    if warnings:
        return "warn"
    return "ready"


def _next_commands(config: NaturalDrivingReadinessConfig) -> dict[str, str]:
    python_exec = str(config.python_exec)
    suite_path = str(config.suite_path)
    route_config = str(config.route_health_config)
    dry_out = str(Path(config.out_dir).expanduser() / "dry_run")
    online_out = str(Path(config.out_dir).expanduser() / "online")
    return {
        "restart_apollo_container": f"docker start {config.apollo_container}",
        "verify_apollo_container": (
            "docker inspect --format "
            f"'{{{{.Name}}}} {{{{.State.Status}}}} {{{{.State.ExitCode}}}}' {config.apollo_container}"
        ),
        "rerun_strict_readiness": (
            f"{python_exec} tools/check_town01_natural_driving_readiness.py "
            f"--suite {suite_path} --config {route_config} "
            f"--out {Path(config.out_dir).expanduser()} --strict-runtime"
        ),
        "dry_run_suite": (
            f"{python_exec} tools/run_town01_natural_driving_suite.py "
            f"--suite {suite_path} --config {route_config} "
            f"--out {dry_out} --dry-run --continue-on-failure"
        ),
        "online_single_canary": (
            f"{python_exec} tools/run_town01_natural_driving_suite.py "
            f"--suite {suite_path} --config {route_config} "
            f"--out {online_out} --scenarios lane_keep_097 "
            "--continue-on-failure --postprocess-after-run --audit-after-postprocess"
        ),
        "online_full_suite": (
            f"{python_exec} tools/run_town01_natural_driving_suite.py "
            f"--suite {suite_path} --config {route_config} "
            f"--out {online_out} --continue-on-failure "
            "--postprocess-after-run --audit-after-postprocess"
        ),
        "postprocess_and_audit_strict": postprocess_and_audit_command(
            NaturalDrivingRunnerConfig(
                suite_path=Path(config.suite_path),
                out_dir=Path(online_out),
                dry_run=False,
                continue_on_failure=True,
                python_exec=python_exec,
                fixed_delta_seconds=float(config.fixed_delta_seconds),
                route_health_config=route_config,
            )
        ),
    }


def _online_config_provenance_issues(
    matrix: Sequence[Mapping[str, Any]],
    online_config: Mapping[str, Any],
) -> list[str]:
    issues: list[str] = []
    for warning in online_config.get("warnings") or []:
        issues.append(f"online_config_warning:{warning}")
    required_fields = (
        "online_config_path",
        "online_config_profile_name",
        "transport_mode",
        "transport_mode_source",
    )
    for row in matrix:
        run_id = str(row.get("run_id") or row.get("scenario_id") or "unknown")
        for field in required_fields:
            if row.get(field) in {None, ""}:
                issues.append(f"matrix_missing_online_config_provenance:{run_id}:{field}")
    return sorted(set(issues))


def _remediation_actions(
    config: NaturalDrivingReadinessConfig,
    blockers: Sequence[str],
    warnings: Sequence[str],
    next_commands: Mapping[str, str],
) -> dict[str, Any]:
    issues = list(blockers) + list(warnings)
    actions: list[dict[str, Any]] = []

    if any(item.startswith("apollo_container_not_running:") for item in issues):
        actions.append(
            {
                "name": "restart_apollo_container",
                "command": next_commands["restart_apollo_container"],
                "automatic": False,
                "reason": "Apollo container exists but is not running.",
            }
        )
        actions.append(
            {
                "name": "verify_apollo_container",
                "command": next_commands["verify_apollo_container"],
                "automatic": False,
                "reason": "Confirm the container state before launching CARLA/Apollo suite commands.",
            }
        )
    if any(item.startswith("apollo_container_missing_or_uninspectable:") for item in issues):
        actions.append(
            {
                "name": "inspect_apollo_container_inventory",
                "command": "docker ps -a --format 'table {{.Names}}\\t{{.Status}}\\t{{.Image}}'",
                "automatic": False,
                "reason": "Find the available Apollo container name or recreate it outside this checker.",
            }
        )
    if any(item.startswith("docker_unavailable:") for item in issues):
        actions.append(
            {
                "name": "inspect_docker_daemon",
                "command": "docker info",
                "automatic": False,
                "reason": "Docker must be available before Apollo container checks are meaningful.",
            }
        )
    if any(item.startswith("carla_root_missing:") or item.startswith("carla_launcher_missing:") for item in issues):
        actions.append(
            {
                "name": "inspect_carla_root",
                "command": f"ls -l {Path(config.carla_root).expanduser()}",
                "automatic": False,
                "reason": "CARLA 0.9.16 launcher must be present before online suite launch.",
            }
        )
    if any(item.startswith("python_missing:") for item in issues):
        actions.append(
            {
                "name": "inspect_carla16_python",
                "command": f"ls -l {Path(config.python_exec).expanduser()}",
                "automatic": False,
                "reason": "Online commands should use the configured carla16 Python.",
            }
        )
    if any(item.startswith("apollo_core_dump_total_above_threshold:") for item in issues):
        actions.append(
            {
                "name": "inspect_apollo_core_dumps",
                "command": f"du -sh {Path(config.apollo_core_dir).expanduser()}",
                "automatic": False,
                "reason": (
                    "Apollo core dumps are crash diagnostics and can consume enough disk "
                    "to destabilize long Town01 online suites. Archive or delete them manually if not needed."
                ),
            }
        )

    actions.append(
        {
            "name": "rerun_strict_readiness",
            "command": next_commands["rerun_strict_readiness"],
            "automatic": False,
            "reason": "Re-check runtime prerequisites after any manual recovery step.",
        }
    )
    return {
        "auto_recovery_attempted": False,
        "actions": actions,
        "policy": (
            "The readiness checker is diagnostic only. It never starts/stops Docker, "
            "CARLA, Apollo, CyberRT, ROS2, or Dreamview."
        ),
    }


def _run_command(args: Sequence[str]) -> CommandResult:
    try:
        completed = subprocess.run(
            list(args),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
    except Exception as exc:  # pragma: no cover - defensive local environment guard.
        return CommandResult(return_code=127, stderr=f"{type(exc).__name__}: {exc}")
    return CommandResult(
        return_code=int(completed.returncode),
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


def default_readiness_config(out_dir: str | Path) -> NaturalDrivingReadinessConfig:
    python_exec = DEFAULT_CARLA16_PYTHON if DEFAULT_CARLA16_PYTHON.exists() else Path(sys.executable)
    return NaturalDrivingReadinessConfig(out_dir=Path(out_dir), python_exec=python_exec)
