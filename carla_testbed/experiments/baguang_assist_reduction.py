from __future__ import annotations

import csv
import json
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import yaml

from carla_testbed.autoware.baguang_readiness import (
    BaguangAutowareReadinessConfig,
    check_baguang_autoware_readiness,
)

BAGUANG_ASSIST_REDUCTION_SCHEMA_VERSION = "baguang_assist_reduction.v1"
BAGUANG_ASSIST_REDUCTION_MANIFEST_VERSION = "baguang_assist_reduction_manifest.v1"
DEFAULT_CONFIG_PATH = Path("configs/experiments/baguang_assist_reduction.yaml")

RUN_MATRIX_FIELDS = [
    "batch_id",
    "run_id",
    "stack",
    "profile_id",
    "description",
    "backend",
    "run_dir",
    "status",
    "return_code",
    "failure_reason",
    "expected_assist_count",
    "expected_assists_json",
    "env_json",
    "extra_args_json",
    "summary_path",
    "actual_run_dir",
    "artifact_index_status",
    "summary_success",
    "summary_exit_reason",
    "timeseries_path",
    "video_path",
    "dreamview_video_path",
    "comparison_report_path",
    "command_stdout_path",
    "command_stderr_path",
    "command",
]

CommandRunner = Callable[[str, Path, Path], int]


@dataclass(frozen=True)
class BaguangAssistReductionConfig:
    config_path: Path = DEFAULT_CONFIG_PATH
    out_dir: Path | None = None
    dry_run: bool = False
    continue_on_failure: bool = False
    selected_stacks: tuple[str, ...] = ()
    selected_profiles: tuple[str, ...] = ()
    batch_id: str | None = None


def load_assist_reduction_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    cfg_path = Path(path).expanduser()
    with cfg_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"assist-reduction config must be a mapping: {cfg_path}")
    validation = validate_assist_reduction_config(payload)
    payload["_validation"] = validation
    payload["_source_path"] = str(cfg_path)
    if validation["errors"]:
        raise ValueError("; ".join(validation["errors"]))
    return payload


def validate_assist_reduction_config(config: Mapping[str, Any]) -> dict[str, list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if config.get("schema_version") != BAGUANG_ASSIST_REDUCTION_SCHEMA_VERSION:
        errors.append(f"schema_version must be {BAGUANG_ASSIST_REDUCTION_SCHEMA_VERSION}")
    if not config.get("comparison_suite"):
        errors.append("comparison_suite is required")
    stacks = config.get("stacks")
    if not isinstance(stacks, Mapping):
        errors.append("stacks must be a mapping")
        return {"errors": errors, "warnings": warnings}
    missing_stacks = sorted({"apollo", "autoware"} - set(stacks))
    if missing_stacks:
        errors.append(f"stacks missing: {', '.join(missing_stacks)}")
    for stack_name, stack_cfg_raw in stacks.items():
        stack_cfg = _as_mapping(stack_cfg_raw)
        profiles = stack_cfg.get("profiles")
        if not isinstance(profiles, list) or not profiles:
            errors.append(f"{stack_name}: profiles must be a non-empty list")
            continue
        if stack_name == "apollo" and not stack_cfg.get("base_command"):
            errors.append("apollo.base_command is required")
        if stack_name == "autoware" and not stack_cfg.get("command_source"):
            errors.append("autoware.command_source is required")
        seen_profiles: set[str] = set()
        for profile in profiles:
            if not isinstance(profile, Mapping):
                errors.append(f"{stack_name}: each profile must be a mapping")
                continue
            profile_id = str(profile.get("profile_id") or "")
            if not profile_id:
                errors.append(f"{stack_name}: profile_id is required")
            if profile_id in seen_profiles:
                errors.append(f"{stack_name}: duplicate profile_id {profile_id}")
            seen_profiles.add(profile_id)
            assists = profile.get("expected_assists")
            if not isinstance(assists, list):
                errors.append(f"{stack_name}/{profile_id}: expected_assists must be a list")
            if stack_name == "apollo" and not isinstance(profile.get("env"), Mapping):
                errors.append(f"{stack_name}/{profile_id}: env mapping is required")
            if stack_name == "autoware" and not isinstance(profile.get("extra_args", []), list):
                errors.append(f"{stack_name}/{profile_id}: extra_args must be a list")
        baseline_ids = {str(profile.get("profile_id") or "") for profile in profiles if isinstance(profile, Mapping)}
        expected_baseline = f"{stack_name}_assisted_baseline"
        if expected_baseline not in baseline_ids:
            warnings.append(f"{stack_name}: baseline profile {expected_baseline} missing")
    boundary = _as_mapping(config.get("claim_boundary"))
    if boundary.get("assist_reduction_probe_proves_unassisted_natural_driving") is True:
        errors.append("assist-reduction probes must not prove unassisted natural driving by themselves")
    return {"errors": errors, "warnings": warnings}


def parse_filter(text: str | Sequence[str] | None) -> tuple[str, ...]:
    if text is None:
        return ()
    if isinstance(text, str):
        items = [item.strip() for item in text.split(",")]
    else:
        items = [str(item).strip() for item in text]
    return tuple(item for item in items if item)


def build_assist_reduction_matrix(config: BaguangAssistReductionConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = load_assist_reduction_config(config.config_path)
    out_dir = Path(config.out_dir or payload.get("output_root") or "runs/assist_reduction/baguang").expanduser()
    batch_id = config.batch_id or f"baguang_assist_reduction_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    selected_stacks = set(config.selected_stacks)
    selected_profiles = set(config.selected_profiles)
    rows: list[dict[str, Any]] = []
    stacks = _as_mapping(payload.get("stacks"))
    for stack_name, stack_cfg_raw in stacks.items():
        if selected_stacks and str(stack_name) not in selected_stacks:
            continue
        stack_cfg = _as_mapping(stack_cfg_raw)
        for profile in stack_cfg.get("profiles") or []:
            if not isinstance(profile, Mapping):
                continue
            profile_id = str(profile.get("profile_id") or "")
            if selected_profiles and profile_id not in selected_profiles:
                continue
            run_id = f"{batch_id}__{profile_id}"
            run_dir = out_dir / str(stack_name) / profile_id / run_id
            command = _command_for_profile(
                stack_name=str(stack_name),
                stack_cfg=stack_cfg,
                profile=profile,
                run_dir=run_dir,
                run_id=run_id,
            )
            rows.append(
                {
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "stack": str(stack_name),
                    "profile_id": profile_id,
                    "description": profile.get("description"),
                    "backend": "carla_direct" if stack_name == "apollo" else "ros2_autoware",
                    "run_dir": str(run_dir),
                    "status": "dry_run" if config.dry_run else "planned",
                    "return_code": None,
                    "failure_reason": None,
                    "expected_assist_count": len(profile.get("expected_assists") or []),
                    "expected_assists": list(profile.get("expected_assists") or []),
                    "expected_assists_json": json.dumps(list(profile.get("expected_assists") or []), sort_keys=True),
                    "env": dict(profile.get("env") or {}),
                    "env_json": json.dumps(dict(profile.get("env") or {}), sort_keys=True),
                    "extra_args": list(profile.get("extra_args") or []),
                    "extra_args_json": json.dumps(list(profile.get("extra_args") or []), sort_keys=True),
                    "summary_path": str(run_dir / "summary.json"),
                    "actual_run_dir": None,
                    "artifact_index_status": "not_checked",
                    "summary_success": None,
                    "summary_exit_reason": None,
                    "timeseries_path": str(run_dir / "timeseries.csv"),
                    "video_path": str(run_dir / "video" / "dual_cam" / "demo_third_person.mp4"),
                    "dreamview_video_path": str(run_dir / "artifacts" / "dreamview_capture.mp4"),
                    "comparison_report_path": str(out_dir / "analysis" / "baguang_stack_comparison_report.json"),
                    "command_stdout_path": str(run_dir / "command_stdout.log"),
                    "command_stderr_path": str(run_dir / "command_stderr.log"),
                    "command": command,
                }
            )
    manifest = {
        "schema_version": BAGUANG_ASSIST_REDUCTION_MANIFEST_VERSION,
        "batch_id": batch_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config_path": str(Path(config.config_path).expanduser()),
        "comparison_suite": payload.get("comparison_suite"),
        "out_dir": str(out_dir),
        "dry_run": bool(config.dry_run),
        "continue_on_failure": bool(config.continue_on_failure),
        "run_count": len(rows),
        "claim_boundary": payload.get("claim_boundary") or {},
        "analysis_commands": {
            "comparison_after_runs": (
                "python tools/analyze_baguang_stack_comparison.py "
                f"--suite {payload.get('comparison_suite')} --out {out_dir / 'analysis'}"
            )
        },
        "runs": rows,
    }
    return rows, manifest


def write_assist_reduction_outputs(
    config: BaguangAssistReductionConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    matrix, manifest = build_assist_reduction_matrix(config)
    manifest = _manifest_with_rows(manifest, matrix)
    _persist_outputs(matrix, manifest)
    return matrix, manifest


def refresh_assist_reduction_artifact_index(root: str | Path) -> dict[str, Any]:
    out_dir = Path(root).expanduser()
    matrix_path = out_dir / "assist_reduction_matrix.csv"
    manifest_path = out_dir / "assist_reduction_manifest.json"
    matrix = _read_matrix_csv(matrix_path)
    if not matrix:
        return {
            "status": "no_matrix",
            "assist_reduction_matrix": str(matrix_path),
            "assist_reduction_manifest": str(manifest_path),
            "refreshed_rows": 0,
        }

    refreshed_rows = 0
    for row in matrix:
        if index_assist_reduction_artifacts_for_row(row):
            refreshed_rows += 1
        return_code = _coerce_int(row.get("return_code"))
        if return_code == 0 and row.get("summary_success") is False:
            row["status"] = "failed"
            row["failure_reason"] = _summary_failure_reason(row.get("summary_exit_reason"))
        elif return_code == 0 and row.get("artifact_index_status") == "summary_missing":
            row["status"] = "failed"
            row["failure_reason"] = "artifact_missing"

    manifest = _read_json(manifest_path)
    if not manifest:
        manifest = {
            "schema_version": BAGUANG_ASSIST_REDUCTION_MANIFEST_VERSION,
            "batch_id": out_dir.name,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "out_dir": str(out_dir),
        }
    manifest["artifact_index_refreshed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["artifact_index_refresh_status"] = "refreshed"
    manifest = _manifest_with_rows(manifest, matrix)
    _persist_outputs(matrix, manifest)
    return {
        "status": "refreshed",
        "assist_reduction_matrix": str(matrix_path),
        "assist_reduction_manifest": str(manifest_path),
        "refreshed_rows": refreshed_rows,
    }


def execute_assist_reduction(
    config: BaguangAssistReductionConfig,
    *,
    command_runner: CommandRunner | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if config.dry_run:
        return write_assist_reduction_outputs(config)
    matrix, manifest = build_assist_reduction_matrix(config)
    runner = command_runner or _subprocess_command_runner
    _persist_outputs(matrix, manifest)
    stop_index: int | None = None
    for index, row in enumerate(matrix):
        run_dir = Path(str(row["run_dir"]))
        stdout_path = Path(str(row["command_stdout_path"]))
        stderr_path = Path(str(row["command_stderr_path"]))
        run_dir.mkdir(parents=True, exist_ok=True)
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return_code = int(runner(str(row["command"]), stdout_path, stderr_path))
        except Exception as exc:  # pragma: no cover - defensive for real online runs.
            return_code = 1
            stderr_path.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
        row["return_code"] = return_code
        artifacts_found = index_assist_reduction_artifacts_for_row(row)
        row["status"] = "success" if return_code == 0 else "failed"
        row["failure_reason"] = None if return_code == 0 else "return_code_nonzero"
        if return_code == 0 and not artifacts_found:
            row["status"] = "failed"
            row["failure_reason"] = "artifact_missing"
        elif return_code == 0 and row.get("summary_success") is False:
            row["status"] = "failed"
            row["failure_reason"] = _summary_failure_reason(row.get("summary_exit_reason"))
        _persist_outputs(matrix, _manifest_with_rows(manifest, matrix))
        if row["status"] == "failed" and not config.continue_on_failure:
            stop_index = index
            break
    if stop_index is not None:
        for row in matrix[stop_index + 1 :]:
            if row.get("status") == "planned":
                row["status"] = "skipped"
                row["failure_reason"] = "stopped_after_previous_failure"
        _persist_outputs(matrix, _manifest_with_rows(manifest, matrix))
    manifest = _manifest_with_rows(manifest, matrix)
    _persist_outputs(matrix, manifest)
    return matrix, manifest


def _persist_outputs(matrix: Sequence[Mapping[str, Any]], manifest: Mapping[str, Any]) -> None:
    out_dir = Path(str(manifest["out_dir"])).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "assist_reduction_manifest.json"
    matrix_path = out_dir / "assist_reduction_matrix.csv"
    manifest_path.write_text(json.dumps(dict(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_matrix_csv(matrix_path, matrix)


def _manifest_with_rows(manifest: Mapping[str, Any], matrix: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    payload = dict(manifest)
    payload["runs"] = [dict(row) for row in matrix]
    payload["run_count"] = len(matrix)
    payload["status_counts"] = _status_counts(matrix)
    return payload


def _status_counts(matrix: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in matrix:
        status = str(row.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def index_assist_reduction_artifacts_for_row(row: dict[str, Any]) -> bool:
    summary_path = _find_actual_summary_path(row.get("run_dir") or "")
    if summary_path is None:
        row["actual_run_dir"] = None
        row["artifact_index_status"] = "summary_missing"
        row["summary_success"] = None
        row["summary_exit_reason"] = None
        return False

    actual_run_dir = summary_path.parent
    summary = _read_json(summary_path)
    row["actual_run_dir"] = str(actual_run_dir)
    row["artifact_index_status"] = "found"
    row["summary_path"] = str(summary_path)
    row["summary_success"] = summary.get("success") if isinstance(summary.get("success"), bool) else None
    row["summary_exit_reason"] = summary.get("exit_reason")
    row["timeseries_path"] = _artifact_path(actual_run_dir, ["timeseries.csv", "timeseries.jsonl"])
    row["video_path"] = _artifact_path(
        actual_run_dir,
        [
            "video/dual_cam/demo_third_person.mp4",
            "video/dual_cam/demo_third_person_hud.mp4",
        ],
    )
    row["dreamview_video_path"] = _artifact_path(
        actual_run_dir,
        [
            "artifacts/dreamview_capture.mp4",
            "video/dreamview/dreamview_capture.mp4",
        ],
    )
    return True


def _find_actual_summary_path(run_root: str | Path) -> Path | None:
    root = Path(run_root).expanduser()
    candidates: list[Path] = []
    if root.exists():
        candidates.extend(
            path
            for path in root.rglob("summary.json")
            if path.is_file() and "analysis" not in path.parts
        )
    parent = root.parent
    if parent.exists():
        prefix = root.name
        for sibling in parent.glob(f"{prefix}*"):
            if not sibling.is_dir():
                continue
            if sibling.name == "latest":
                continue
            summary = sibling / "summary.json"
            if summary.is_file():
                candidates.append(summary)
    if not candidates:
        latest = parent / "latest" / "summary.json"
        if latest.is_file():
            candidates.append(latest)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _artifact_path(actual_run_dir: Path, candidates: Sequence[str]) -> str | None:
    for relative in candidates:
        path = actual_run_dir / relative
        if path.exists():
            return str(path)
    basenames = {Path(candidate).name for candidate in candidates}
    for path in sorted(actual_run_dir.rglob("*")):
        if path.is_file() and path.name in basenames:
            return str(path)
    return None


def _summary_failure_reason(exit_reason: Any) -> str:
    text = str(exit_reason or "").strip().lower()
    if not text:
        return "summary_unsuccessful"
    safe = "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")
    return safe or "summary_unsuccessful"


def _command_for_profile(
    *,
    stack_name: str,
    stack_cfg: Mapping[str, Any],
    profile: Mapping[str, Any],
    run_dir: Path,
    run_id: str,
) -> str:
    if stack_name == "apollo":
        env = {str(key): str(value) for key, value in _as_mapping(profile.get("env")).items()}
        env.update({"RUN_ID": run_id, "RUN_DIR": str(run_dir)})
        prefix = " ".join(f"{key}={shlex.quote(value)}" for key, value in sorted(env.items()))
        command = str(stack_cfg.get("base_command"))
        return f"{prefix} bash {shlex.quote(command)}"
    if stack_name == "autoware":
        readiness = check_baguang_autoware_readiness(BaguangAutowareReadinessConfig())
        commands = _as_mapping(readiness.get("next_commands"))
        base = str(commands.get("online_autoware_followstop_demo_recording") or "")
        extra = _shell_args(profile.get("extra_args") or [])
        run_dir_arg = f"--run-dir {shlex.quote(str(run_dir))}"
        # The readiness command carries a timestamped run-dir. Append another
        # explicit run-dir so argparse's final value pins this reduction run.
        return " ".join(part for part in (base, run_dir_arg, extra) if part)
    raise ValueError(f"unsupported stack: {stack_name}")


def _write_matrix_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=RUN_MATRIX_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in RUN_MATRIX_FIELDS})


def _read_matrix_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _read_json(path: str | Path) -> dict[str, Any]:
    input_path = Path(path)
    if not input_path.exists():
        return {}
    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _shell_args(items: Sequence[Any]) -> str:
    tokens: list[str] = []
    for item in items:
        tokens.extend(shlex.split(str(item)))
    return " ".join(shlex.quote(token) for token in tokens)


def _subprocess_command_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w",
        encoding="utf-8",
    ) as stderr:
        completed = subprocess.run(
            command,
            shell=True,
            executable="/bin/bash",
            stdout=stdout,
            stderr=stderr,
            text=True,
            check=False,
        )
    return int(completed.returncode)
