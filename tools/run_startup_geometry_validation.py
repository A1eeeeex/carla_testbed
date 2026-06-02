#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical startup geometry validation.
# Do not add new platform logic here; move reusable code into carla_testbed.analysis.
# Migration target: carla_testbed.analysis.route_start_alignment.
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Sequence

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG = REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt.yaml"


CASE_MATRIX: Dict[str, Dict[str, Any]] = {
    "A": {
        "name": "case_a_baseline",
        "label": "Case A",
        "description": "Current baseline",
        "routing": {
            "localization_back_offset_m": 1.4375,
            "snap_start_to_lane": True,
            "start_nudge_m": 2.0,
            "start_nudge_retry_step_m": 1.5,
            "start_nudge_use_lane_heading": True,
            "freeze_after_success": False,
        },
    },
    "B": {
        "name": "case_b_disable_all",
        "label": "Case B",
        "description": "Disable all startup geometry modifiers",
        "routing": {
            "localization_back_offset_m": 0.0,
            "snap_start_to_lane": False,
            "start_nudge_m": 0.0,
            "start_nudge_retry_step_m": 0.0,
            "start_nudge_use_lane_heading": False,
            "freeze_after_success": False,
        },
    },
    "C": {
        "name": "case_c_back_offset_only",
        "label": "Case C",
        "description": "Keep only localization back offset",
        "routing": {
            "localization_back_offset_m": 1.4375,
            "snap_start_to_lane": False,
            "start_nudge_m": 0.0,
            "start_nudge_retry_step_m": 0.0,
            "start_nudge_use_lane_heading": False,
            "freeze_after_success": False,
        },
    },
    "D": {
        "name": "case_d_snap_only",
        "label": "Case D",
        "description": "Keep only start snap",
        "routing": {
            "localization_back_offset_m": 0.0,
            "snap_start_to_lane": True,
            "start_nudge_m": 0.0,
            "start_nudge_retry_step_m": 0.0,
            "start_nudge_use_lane_heading": False,
            "freeze_after_success": False,
        },
    },
    "E": {
        "name": "case_e_snap_nudge_vehicle_yaw",
        "label": "Case E",
        "description": "Snap plus nudge along vehicle yaw",
        "routing": {
            "localization_back_offset_m": 0.0,
            "snap_start_to_lane": True,
            "start_nudge_m": 2.0,
            "start_nudge_retry_step_m": 1.5,
            "start_nudge_use_lane_heading": False,
            "freeze_after_success": False,
        },
    },
    "F": {
        "name": "case_f_snap_nudge_lane_heading_freeze",
        "label": "Case F",
        "description": "Snap plus lane-heading nudge with routing freeze",
        "routing": {
            "localization_back_offset_m": 0.0,
            "snap_start_to_lane": True,
            "start_nudge_m": 2.0,
            "start_nudge_retry_step_m": 1.5,
            "start_nudge_use_lane_heading": True,
            "freeze_after_success": True,
        },
    },
}


def _deep_clone(payload: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(payload))


def _load_yaml(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise SystemExit(f"invalid yaml root in {path}")
    return payload


def _write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))


def _set_nested(root: Dict[str, Any], keys: Sequence[str], value: Any) -> None:
    cur = root
    for key in keys[:-1]:
        node = cur.get(key)
        if not isinstance(node, dict):
            node = {}
            cur[key] = node
        cur = node
    cur[keys[-1]] = value


def _build_case_config(base_cfg: Dict[str, Any], case_key: str, apollo_root: str, docker_container: str) -> Dict[str, Any]:
    case_cfg = _deep_clone(base_cfg)
    case = CASE_MATRIX[case_key]
    routing = case["routing"]
    _set_nested(
        case_cfg,
        ("algo", "apollo", "bridge", "localization_back_offset_m"),
        float(routing["localization_back_offset_m"]),
    )
    for key in (
        "snap_start_to_lane",
        "start_nudge_m",
        "start_nudge_retry_step_m",
        "start_nudge_use_lane_heading",
        "freeze_after_success",
    ):
        _set_nested(case_cfg, ("algo", "apollo", "routing", key), routing[key])
    _set_nested(case_cfg, ("algo", "apollo", "apollo_root"), apollo_root)
    if docker_container:
        _set_nested(case_cfg, ("algo", "apollo", "docker", "container"), docker_container)
    return case_cfg


def _discover_case_keys(raw: str) -> List[str]:
    keys = []
    for item in raw.split(","):
        key = item.strip().upper()
        if not key:
            continue
        if key not in CASE_MATRIX:
            raise SystemExit(f"unknown case: {key}")
        keys.append(key)
    if not keys:
        raise SystemExit("no cases selected")
    return keys


def _run_command(cmd: List[str], *, cwd: Path) -> int:
    proc = subprocess.run(cmd, cwd=str(cwd))
    return int(proc.returncode)


def _command_text(cmd: Sequence[str]) -> str:
    return " ".join(str(part) for part in cmd)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Apollo startup geometry validation matrix")
    parser.add_argument("--apollo-root", default="", help="Host APOLLO_ROOT passed into generated configs")
    parser.add_argument("--docker-container", default="", help="Optional Apollo Docker container override")
    parser.add_argument("--batch-root", required=True, help="Output batch root, e.g. runs/startup_geometry_validation")
    parser.add_argument("--cases", default="A,B,C,D,E,F", help="Comma-separated case keys")
    parser.add_argument("--skip-run", action="store_true", help="Only generate configs and commands")
    parser.add_argument("--analyze-only", action="store_true", help="Skip generation/run and only analyze existing batch")
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)
    artifacts_dir = batch_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    generated_cfg_dir = artifacts_dir / "generated_configs"
    generated_cfg_dir.mkdir(parents=True, exist_ok=True)

    case_keys = _discover_case_keys(args.cases)

    if args.analyze_only:
        analyze_cmd = [
            "conda",
            "run",
            "-n",
            "carla16",
            "python",
            str(REPO_ROOT / "tools" / "analyze_startup_geometry_validation.py"),
            "--batch-root",
            str(batch_root),
        ]
        return _run_command(analyze_cmd, cwd=REPO_ROOT)

    if not args.apollo_root:
        raise SystemExit("--apollo-root is required unless --analyze-only is used")

    base_cfg = _load_yaml(BASE_CONFIG)
    commands: List[str] = []
    metadata: Dict[str, Any] = {
        "batch_root": str(batch_root),
        "base_config": str(BASE_CONFIG),
        "apollo_root": args.apollo_root,
        "docker_container": args.docker_container,
        "cases": [],
    }

    for case_key in case_keys:
        case = CASE_MATRIX[case_key]
        run_dir = batch_root / case["name"]
        cfg_path = generated_cfg_dir / f"{case['name']}.yaml"
        payload = _build_case_config(base_cfg, case_key, args.apollo_root, args.docker_container)
        _write_yaml(cfg_path, payload)
        cmd = [
            "conda",
            "run",
            "-n",
            "carla16",
            "python",
            "-m",
            "carla_testbed",
            "run",
            "--config",
            str(cfg_path),
            "--run-dir",
            str(run_dir),
        ]
        commands.append(_command_text(cmd))
        metadata["cases"].append(
            {
                "case_key": case_key,
                "label": case["label"],
                "name": case["name"],
                "description": case["description"],
                "run_dir": str(run_dir),
                "config_path": str(cfg_path),
                "command": _command_text(cmd),
                "command_argv": cmd,
                "routing": dict(case["routing"]),
            }
        )

    (artifacts_dir / "startup_geometry_validation_commands.txt").write_text("\n".join(commands) + "\n")
    (artifacts_dir / "startup_geometry_validation_cases.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False)
    )

    if args.skip_run:
        print(f"[startup-geometry] generated configs under {generated_cfg_dir}")
        print(f"[startup-geometry] commands written to {artifacts_dir / 'startup_geometry_validation_commands.txt'}")
        return 0

    overall_rc = 0
    for case_info in metadata["cases"]:
        print(
            f"[startup-geometry] running {case_info['case_key']} "
            f"-> {case_info['run_dir']}"
        )
        rc = _run_command(list(case_info["command_argv"]), cwd=REPO_ROOT)
        case_info["return_code"] = rc
        if rc != 0 and overall_rc == 0:
            overall_rc = rc

    (artifacts_dir / "startup_geometry_validation_cases.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False)
    )

    analyze_cmd = [
        "conda",
        "run",
        "-n",
        "carla16",
        "python",
        str(REPO_ROOT / "tools" / "analyze_startup_geometry_validation.py"),
        "--batch-root",
        str(batch_root),
    ]
    analyze_rc = _run_command(analyze_cmd, cwd=REPO_ROOT)
    if overall_rc == 0:
        overall_rc = analyze_rc
    return overall_rc


if __name__ == "__main__":
    raise SystemExit(main())
