#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical startup geometry fix validation.
# Do not add new platform logic here; move reusable code into carla_testbed.analysis.
# Migration target: carla_testbed.analysis.route_start_alignment.
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
CONDA_PREFIX = ["conda", "run", "-n", "carla16"]
BASE_CONFIG = REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_baseline.yaml"

CASES: List[Dict[str, Any]] = [
    {
        "id": "case_1_baseline_legacy",
        "label": "Case 1",
        "description": "legacy baseline: allow legacy base_map snap and lane-heading nudge without heading gate",
        "overrides": {
            "algo": {
                "apollo": {
                    "bridge": {
                        "localization_back_offset_m": 1.4375,
                    },
                    "routing": {
                        "snap_start_to_lane": True,
                        "snap_goal_to_lane": True,
                        "start_nudge_m": 2.0,
                        "start_nudge_retry_step_m": 1.5,
                        "start_nudge_min_safe_m": 1.5,
                        "start_nudge_max_m": 6.0,
                        "start_nudge_use_lane_heading": True,
                        "freeze_after_success": False,
                        "snap_source_mode": "legacy_base_map_xy",
                        "snap_allow_untrusted_source": True,
                        "snap_heading_diff_max_deg": 180.0,
                        "snap_heading_diff_hard_reject_deg": 180.0,
                        "lane_heading_nudge_max_heading_diff_deg": 180.0,
                        "disable_nudge_when_snap_rejected": False,
                    },
                }
            }
        },
    },
    {
        "id": "case_2_patched_conservative",
        "label": "Case 2",
        "description": "patched conservative: no snap, no nudge",
        "overrides": {
            "algo": {
                "apollo": {
                    "bridge": {
                        "localization_back_offset_m": 0.0,
                    },
                    "routing": {
                        "snap_start_to_lane": False,
                        "snap_goal_to_lane": True,
                        "start_nudge_m": 0.0,
                        "start_nudge_retry_step_m": 0.0,
                        "start_nudge_min_safe_m": 0.0,
                        "start_nudge_max_m": 0.0,
                        "start_nudge_use_lane_heading": False,
                        "freeze_after_success": True,
                        "snap_source_mode": "lane_centerline_only",
                        "snap_allow_untrusted_source": False,
                        "snap_heading_diff_max_deg": 30.0,
                        "snap_heading_diff_hard_reject_deg": 45.0,
                        "lane_heading_nudge_max_heading_diff_deg": 20.0,
                        "disable_nudge_when_snap_rejected": True,
                    },
                }
            }
        },
    },
    {
        "id": "case_3_patched_snap_gate_lane_centerline",
        "label": "Case 3",
        "description": "patched snap retained, lane-centerline-only source, heading gate, no nudge",
        "overrides": {
            "algo": {
                "apollo": {
                    "bridge": {
                        "localization_back_offset_m": 0.0,
                    },
                    "routing": {
                        "snap_start_to_lane": True,
                        "snap_goal_to_lane": True,
                        "start_nudge_m": 0.0,
                        "start_nudge_retry_step_m": 0.0,
                        "start_nudge_min_safe_m": 0.0,
                        "start_nudge_max_m": 0.0,
                        "start_nudge_use_lane_heading": False,
                        "freeze_after_success": False,
                        "snap_source_mode": "lane_centerline_only",
                        "snap_allow_untrusted_source": False,
                        "snap_heading_diff_max_deg": 30.0,
                        "snap_heading_diff_hard_reject_deg": 45.0,
                        "lane_heading_nudge_max_heading_diff_deg": 20.0,
                        "disable_nudge_when_snap_rejected": True,
                    },
                }
            }
        },
    },
    {
        "id": "case_4_patched_snap_vehicle_yaw_nudge",
        "label": "Case 4",
        "description": "patched snap retained with vehicle-yaw nudge only",
        "overrides": {
            "algo": {
                "apollo": {
                    "bridge": {
                        "localization_back_offset_m": 0.0,
                    },
                    "routing": {
                        "snap_start_to_lane": True,
                        "snap_goal_to_lane": True,
                        "start_nudge_m": 2.0,
                        "start_nudge_retry_step_m": 1.5,
                        "start_nudge_min_safe_m": 1.5,
                        "start_nudge_max_m": 6.0,
                        "start_nudge_use_lane_heading": False,
                        "freeze_after_success": False,
                        "snap_source_mode": "lane_centerline_only",
                        "snap_allow_untrusted_source": False,
                        "snap_heading_diff_max_deg": 30.0,
                        "snap_heading_diff_hard_reject_deg": 45.0,
                        "lane_heading_nudge_max_heading_diff_deg": 20.0,
                        "disable_nudge_when_snap_rejected": True,
                    },
                }
            }
        },
    },
]


def _deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _generated_config(case: Dict[str, Any]) -> Dict[str, Any]:
    base = yaml.safe_load(BASE_CONFIG.read_text()) or {}
    config = _deep_merge(base, case["overrides"])
    run_cfg = dict(config.get("run", {}) or {})
    run_cfg["profile_name"] = case["id"]
    config["run"] = run_cfg
    paths_cfg = dict(config.get("paths", {}) or {})
    carla_root = str(os.environ.get("CARLA_ROOT") or "").strip()
    apollo_root = str(os.environ.get("APOLLO_ROOT") or "").strip()
    if carla_root:
        paths_cfg["carla_root"] = carla_root
        config.setdefault("carla", {})["root"] = carla_root
        config.setdefault("runtime", {}).setdefault("carla", {})["root"] = carla_root
        config.setdefault("runtime", {}).setdefault("carla", {})["start"] = True
    if apollo_root:
        paths_cfg["apollo_root"] = apollo_root
        config.setdefault("algo", {}).setdefault("apollo", {})["apollo_root"] = apollo_root
    if paths_cfg:
        config["paths"] = paths_cfg
    return config


def _run_case(case: Dict[str, Any], config_path: Path, run_dir: Path) -> None:
    cmd = CONDA_PREFIX + [
        "python",
        "-m",
        "carla_testbed",
        "run",
        "--config",
        str(config_path),
        "--run-dir",
        str(run_dir),
    ]
    print(f"[startup-geometry-fix] running {case['label']} -> {run_dir}")
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT), env=os.environ.copy())


def _run_analyzer(batch_root: Path) -> None:
    cmd = CONDA_PREFIX + [
        "python",
        str(REPO_ROOT / "tools" / "analyze_startup_geometry_fix.py"),
        "--batch-root",
        str(batch_root),
    ]
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT), env=os.environ.copy())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apollo-root", default="", help="Optional explicit APOLLO_ROOT export")
    parser.add_argument("--docker-container", default="", help="Optional explicit APOLLO_DOCKER_CONTAINER export")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--cases", default="1,2,3,4")
    parser.add_argument("--skip-run", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)
    batch_artifacts = batch_root / "artifacts"
    batch_artifacts.mkdir(parents=True, exist_ok=True)

    if args.apollo_root:
        os.environ["APOLLO_ROOT"] = args.apollo_root
    if args.docker_container:
        os.environ["APOLLO_DOCKER_CONTAINER"] = args.docker_container

    selected = {f"Case {part.strip()}" for part in args.cases.split(",") if part.strip()}
    selected_cases = [case for case in CASES if case["label"] in selected]
    if not selected_cases:
        raise SystemExit("no cases selected")

    commands: List[str] = []
    metadata: List[Dict[str, Any]] = []
    generated_dir = batch_artifacts / "generated_configs"
    generated_dir.mkdir(parents=True, exist_ok=True)

    for case in selected_cases:
        config = _generated_config(case)
        config_path = generated_dir / f"{case['id']}.yaml"
        config_path.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True))
        run_dir = batch_root / case["id"]
        commands.append(
            " ".join(
                CONDA_PREFIX
                + [
                    "python",
                    "-m",
                    "carla_testbed",
                    "run",
                    "--config",
                    str(config_path),
                    "--run-dir",
                    str(run_dir),
                ]
            )
        )
        metadata.append(
            {
                "id": case["id"],
                "label": case["label"],
                "description": case["description"],
                "config_path": str(config_path),
                "run_dir": str(run_dir),
            }
        )

    (batch_artifacts / "startup_geometry_fix_validation_commands.txt").write_text("\n".join(commands) + "\n")
    (batch_artifacts / "startup_geometry_fix_validation_cases.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False)
    )

    if args.analyze_only:
        _run_analyzer(batch_root)
        return

    if args.skip_run:
        print(f"[startup-geometry-fix] commands written to {batch_artifacts / 'startup_geometry_fix_validation_commands.txt'}")
        return

    failures: List[Dict[str, Any]] = []
    for case in selected_cases:
        config_path = generated_dir / f"{case['id']}.yaml"
        run_dir = batch_root / case["id"]
        try:
            _run_case(case, config_path, run_dir)
        except subprocess.CalledProcessError as exc:
            failures.append({"case": case["label"], "returncode": exc.returncode})
            print(f"[startup-geometry-fix][warn] case failed but batch continues: {case['label']} rc={exc.returncode}")

    _run_analyzer(batch_root)
    if failures:
        raise SystemExit(json.dumps({"batch_failures": failures}, ensure_ascii=False))


if __name__ == "__main__":
    main()
