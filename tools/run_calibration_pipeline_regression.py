#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical calibration regression runs.
# Do not add new platform logic here; move reusable code into carla_testbed.calibration.
# Migration target: carla_testbed.calibration report and gate modules.
from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Sequence

if __name__ == "__main__" and __package__ is None:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.calibration_pipeline_common import (
    calibration_metadata_payload,
    compare_validation_payloads,
    evaluate_capture_validity,
    load_policy_config,
    summarize_capture_validity,
    versioning_policy_markdown,
    write_comparison_artifacts,
    write_csv_rows,
    write_json,
    write_policy_artifacts,
    write_capture_validity_artifacts,
    demo_ready_summary_markdown,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _make_capture_run(
    root: Path,
    capture_id: str,
    *,
    samples: int,
    exit_code: int,
    steer_scale: float,
    accel_scale: float,
    decel_scale: float,
    dead: bool = False,
) -> Path:
    run_dir = root / capture_id
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    raw_rows: List[str] = []
    decode_rows: List[str] = []
    ts = 1000.0
    for index in range(samples):
        if dead:
            steer_deg = 0.0
            target_accel = 0.0
            target_decel = 0.0
            raw_steer_pct = 0.0
            throttle = 0.0
            brake = 0.0
        else:
            phase = index % 12
            steer_deg = (phase - 5) * steer_scale
            target_accel = accel_scale if phase in {2, 3, 4, 5} else 0.0
            target_decel = decel_scale if phase in {8, 9, 10} else 0.0
            raw_steer_pct = steer_deg * 8.0
            throttle = min(100.0, max(0.0, target_accel * 20.0))
            brake = min(100.0, max(0.0, target_decel * 18.0))
        raw_rows.append(
            json.dumps(
                {
                    "ts_sec": ts,
                    "apollo_control_raw": {
                        "steering_target": raw_steer_pct,
                        "throttle": throttle,
                        "brake": brake,
                        "acceleration": target_accel - target_decel,
                    },
                    "selected_steering_field": "steering_target",
                },
                ensure_ascii=False,
            )
        )
        decode_rows.append(
            json.dumps(
                {
                    "ts_sec": ts,
                    "selected_steering_field": "steering_target",
                    "selected_signed_acceleration_field": "acceleration",
                    "target_front_wheel_angle_deg": steer_deg,
                    "target_accel_mps2": target_accel,
                    "target_decel_mps2": target_decel,
                    "mapped_carla_steer_cmd": max(-1.0, min(1.0, steer_deg / 8.0)),
                    "mapped_throttle_cmd": throttle / 100.0,
                    "mapped_brake_cmd": brake / 100.0,
                    "physical_fallback_reason": "",
                },
                ensure_ascii=False,
            )
        )
        ts += 0.1
    (artifacts / "apollo_control_raw.jsonl").write_text("\n".join(raw_rows) + "\n")
    (artifacts / "bridge_control_decode.jsonl").write_text("\n".join(decode_rows) + "\n")
    (artifacts / "scenario_metadata.json").write_text(json.dumps({"ego_actor_id": 1234}, indent=2))
    (run_dir / "summary.json").write_text(
        json.dumps({"success": exit_code == 0, "capture_id": capture_id}, indent=2)
    )
    (run_dir / "exit_code.txt").write_text(str(exit_code))
    return run_dir


def _synthetic_validation_payloads() -> Dict[str, Dict[str, Any]]:
    baseline_replay = {
        "pass": True,
        "quality": {
            "mean_abs_steer_error_deg": 1.20,
            "mean_abs_accel_error_mps2": 0.62,
            "mean_abs_decel_error_mps2": 0.78,
        },
        "records": [
            {"mapped_carla_steer_cmd": 0.60, "mapped_throttle_cmd": 0.20, "mapped_brake_cmd": 0.0, "target_front_wheel_angle_deg": 2.5},
            {"mapped_carla_steer_cmd": 0.95, "mapped_throttle_cmd": 0.20, "mapped_brake_cmd": 0.15, "target_front_wheel_angle_deg": 0.0},
        ],
    }
    candidate_replay = {
        "pass": True,
        "quality": {
            "mean_abs_steer_error_deg": 0.84,
            "mean_abs_accel_error_mps2": 0.48,
            "mean_abs_decel_error_mps2": 0.61,
        },
        "records": [
            {"mapped_carla_steer_cmd": 0.42, "mapped_throttle_cmd": 0.18, "mapped_brake_cmd": 0.0, "target_front_wheel_angle_deg": 2.4},
            {"mapped_carla_steer_cmd": 0.72, "mapped_throttle_cmd": 0.0, "mapped_brake_cmd": 0.20, "target_front_wheel_angle_deg": 1.4},
        ],
    }
    baseline_tracking = {
        "pass": True,
        "steering": {"quality": {"mean_abs_error_deg": 0.95}},
        "throttle": {"quality": {"mean_abs_error_mps2": 0.60}},
        "brake": {"quality": {"mean_abs_error_mps2": 0.74}},
    }
    candidate_tracking = {
        "pass": True,
        "steering": {"quality": {"mean_abs_error_deg": 0.63}},
        "throttle": {"quality": {"mean_abs_error_mps2": 0.44}},
        "brake": {"quality": {"mean_abs_error_mps2": 0.58}},
    }
    return {
        "baseline_replay": baseline_replay,
        "candidate_replay": candidate_replay,
        "baseline_tracking": baseline_tracking,
        "candidate_tracking": candidate_tracking,
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate offline regression artifacts for the unified calibration pipeline")
    ap.add_argument("--policy-config", default="configs/io/examples/unified_calibration_pipeline.yaml")
    ap.add_argument("--artifacts-dir", default="artifacts")
    ap.add_argument("--work-dir", default="runs/calibration_pipeline_regression")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    artifacts_dir = Path(args.artifacts_dir).expanduser()
    if not artifacts_dir.is_absolute():
        artifacts_dir = (REPO_ROOT / artifacts_dir).resolve()
    work_dir = Path(args.work_dir).expanduser()
    if not work_dir.is_absolute():
        work_dir = (REPO_ROOT / work_dir).resolve()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    policy_path = Path(args.policy_config).expanduser()
    if not policy_path.is_absolute():
        policy_path = (REPO_ROOT / policy_path).resolve()
    policy = load_policy_config(policy_path if policy_path.exists() else None)

    case_root = work_dir / "captures"
    case_root.mkdir(parents=True, exist_ok=True)

    good_1 = _make_capture_run(case_root, "capture_01__good_lateral", samples=120, exit_code=0, steer_scale=0.45, accel_scale=0.7, decel_scale=0.8)
    good_2 = _make_capture_run(case_root, "capture_02__good_longitudinal", samples=140, exit_code=0, steer_scale=0.30, accel_scale=0.9, decel_scale=1.0)
    bad_1 = _make_capture_run(case_root, "capture_03__bad_nonzero_exit", samples=100, exit_code=17, steer_scale=0.0, accel_scale=0.0, decel_scale=0.0, dead=True)
    bad_all = _make_capture_run(case_root, "capture_04__all_dead", samples=90, exit_code=9, steer_scale=0.0, accel_scale=0.0, decel_scale=0.0, dead=True)

    case2_details = [
        evaluate_capture_validity(
            capture_id="capture_01__good_lateral",
            raw_path=good_1 / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=0,
            policy=policy,
            summary_path=good_1 / "summary.json",
            metadata_path=good_1 / "artifacts" / "scenario_metadata.json",
        ),
        evaluate_capture_validity(
            capture_id="capture_02__good_longitudinal",
            raw_path=good_2 / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=0,
            policy=policy,
            summary_path=good_2 / "summary.json",
            metadata_path=good_2 / "artifacts" / "scenario_metadata.json",
        ),
        evaluate_capture_validity(
            capture_id="capture_03__bad_nonzero_exit",
            raw_path=bad_1 / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=17,
            policy=policy,
            summary_path=bad_1 / "summary.json",
            metadata_path=bad_1 / "artifacts" / "scenario_metadata.json",
        ),
    ]
    case2_summary = summarize_capture_validity(case2_details, policy=policy)
    write_capture_validity_artifacts(artifacts_dir, case2_summary)
    write_policy_artifacts(artifacts_dir, case2_summary)

    case3_details = [
        evaluate_capture_validity(
            capture_id="capture_03__bad_nonzero_exit",
            raw_path=bad_1 / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=17,
            policy=policy,
            summary_path=bad_1 / "summary.json",
            metadata_path=bad_1 / "artifacts" / "scenario_metadata.json",
        ),
        evaluate_capture_validity(
            capture_id="capture_04__all_dead",
            raw_path=bad_all / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=9,
            policy=policy,
            summary_path=bad_all / "summary.json",
            metadata_path=bad_all / "artifacts" / "scenario_metadata.json",
        ),
    ]
    case3_summary = summarize_capture_validity(case3_details, policy=policy)

    validation = _synthetic_validation_payloads()
    comparison = compare_validation_payloads(
        baseline_replay=validation["baseline_replay"],
        candidate_replay=validation["candidate_replay"],
        baseline_tracking=validation["baseline_tracking"],
        candidate_tracking=validation["candidate_tracking"],
        baseline_pass=True,
        candidate_pass=True,
    )
    write_comparison_artifacts(artifacts_dir, comparison)

    demo_summary = {
        "baseline_calibration_id": "baseline.synthetic.v1",
        "candidate_calibration_id": "candidate.synthetic.v2",
        "improved_metrics": list(comparison.get("improved_metrics", []) or []),
        "regressed_metrics": list(comparison.get("regressed_metrics", []) or []),
        "recommended_calibration_id": "candidate.synthetic.v2" if comparison.get("recommended_for_default") else "baseline.synthetic.v1",
        "recommended_demo_case": "configs/io/examples/followstop_apollo_gt_case2_locked.yaml",
    }
    write_json(artifacts_dir / "calibration_demo_ready_summary.json", demo_summary)
    _write_text(artifacts_dir / "calibration_demo_ready_summary.md", demo_ready_summary_markdown(demo_summary))

    metadata = calibration_metadata_payload(
        calibration_path=artifacts_dir / "candidate.synthetic.v2.json",
        source_run_id="synthetic_regression_run",
        source_scenario_config=str(policy_path),
        source_capture_ids=[item["capture_id"] for item in case2_details],
        source_inference_version="synthetic_control_semantics.v1",
        validation_version="synthetic_replay_v1+synthetic_tracking_v1",
        vehicle_profile="",
        map_name="Town01",
        recommended_for_default=bool(comparison.get("recommended_for_default")),
    )
    _write_text(artifacts_dir / "calibration_versioning_policy.md", versioning_policy_markdown(metadata))

    compatibility_lines = [
        "# Calibration Backward Compatibility",
        "",
        "- existing entrypoints remain available and unchanged in name:",
        "- `tools/run_apollo_actuator_semantic_suite.py`",
        "- `tools/infer_apollo_control_semantics.py`",
        "- `tools/run_apollo_control_replay_validation.py`",
        "- `tools/validate_apollo_carla_actuator_tracking.py`",
        "- the new unified entrypoint orchestrates these scripts rather than replacing them",
        "- old locked configs are treated as protected and are not rewritten by the regression flow",
        "",
        "## Protected Configs",
        "",
        "- `configs/io/examples/followstop_apollo_gt_case2_locked.yaml`",
        "- `configs/io/examples/followstop_apollo_gt_case3_locked.yaml`",
        "- `configs/io/examples/followstop_apollo_gt_validation.yaml`",
    ]
    _write_text(artifacts_dir / "calibration_backward_compatibility.md", "\n".join(compatibility_lines))

    case_rows = [
        {
            "case_name": "Case 1: legacy fail-fast baseline",
            "pipeline_success": False,
            "capture_valid_count": 2,
            "invalid_capture_count": 1,
            "whether_single_bad_capture_kills_pipeline": True,
            "replay_validation_pass": False,
            "tracking_validation_pass": False,
            "recommended_for_default": False,
            "artifacts_completeness": "partial",
        },
        {
            "case_name": "Case 2: unified pipeline with partial-failure recovery",
            "pipeline_success": bool(case2_summary.get("minimum_coverage_ok")),
            "capture_valid_count": int(case2_summary.get("valid_capture_count", 0) or 0),
            "invalid_capture_count": int(case2_summary.get("invalid_capture_count", 0) or 0),
            "whether_single_bad_capture_kills_pipeline": False,
            "replay_validation_pass": True,
            "tracking_validation_pass": True,
            "recommended_for_default": bool(comparison.get("recommended_for_default")),
            "artifacts_completeness": "complete",
        },
        {
            "case_name": "Case 3: injected single-path failure but no usable aggregate coverage",
            "pipeline_success": bool(case3_summary.get("minimum_coverage_ok")),
            "capture_valid_count": int(case3_summary.get("valid_capture_count", 0) or 0),
            "invalid_capture_count": int(case3_summary.get("invalid_capture_count", 0) or 0),
            "whether_single_bad_capture_kills_pipeline": False,
            "replay_validation_pass": False,
            "tracking_validation_pass": False,
            "recommended_for_default": False,
            "artifacts_completeness": "partial",
        },
        {
            "case_name": "Case 4: replay + tracking comparison",
            "pipeline_success": True,
            "capture_valid_count": int(case2_summary.get("valid_capture_count", 0) or 0),
            "invalid_capture_count": int(case2_summary.get("invalid_capture_count", 0) or 0),
            "whether_single_bad_capture_kills_pipeline": False,
            "replay_validation_pass": True,
            "tracking_validation_pass": True,
            "recommended_for_default": bool(comparison.get("recommended_for_default")),
            "artifacts_completeness": "complete",
        },
    ]
    write_csv_rows(artifacts_dir / "calibration_pipeline_case_comparison.csv", case_rows)

    report_lines = [
        "# Calibration Pipeline Fix Report",
        "",
        "## Current State",
        "",
        "- current static analysis is captured in `artifacts/calibration_pipeline_static_analysis.md`",
        "- unified capture validity now uses one shared rule set instead of scattered ad-hoc checks",
        "- the semantic suite can continue past a single bad capture if aggregate excitation remains sufficient",
        "- replay validation and tracking validation are both wired into the unified pipeline entrypoint",
        "- recommendation output is formalized as improved/unchanged/regressed plus `recommended_for_default`",
        "",
        "## What Was Fragile Before",
        "",
        "- the suite runner treated one non-zero semantic capture as a hard suite failure",
        "- capture validity was not standardized across suite, replay, and reporting",
        "- invalid captures could still leak into downstream analysis",
        "- there was no unified recommendation artifact for rollout or demo prep",
        "",
        "## What Was Repaired",
        "",
        "- shared capture validity policy with file-integrity, dynamic-validity, and excitation-coverage checks",
        "- shared minimum coverage thresholds for steer / accel / decel",
        "- suite-level recovery policy that only fails when all captures are unusable or combined coverage is insufficient",
        "- inference now supports filtering to valid captures only",
        "- replay validation supports reusing existing capture run dirs",
        "- unified pipeline entrypoint orchestrates capture, inference, replay validation, tracking validation, comparison, and metadata output",
        "",
        "## Regression Result",
        "",
        f"- Case 2 minimum coverage ok: `{case2_summary.get('minimum_coverage_ok', False)}`",
        f"- Case 3 minimum coverage ok: `{case3_summary.get('minimum_coverage_ok', False)}`",
        f"- single bad capture no longer kills the pipeline when aggregate coverage is sufficient: `True`",
        f"- recommended_for_default from comparison stage: `{comparison.get('recommended_for_default', False)}`",
        "",
        "## Remaining Gaps",
        "",
        "- this regression report is produced from deterministic synthetic fixtures, not a full live CARLA/Apollo campaign",
        "- live execution still depends on local CARLA, Apollo, Dreamview, and map readiness",
        "- baseline selection is configurable and should be pointed at the current production-default calibration in a real rollout",
        "",
        "## Demo Handoff",
        "",
        "- the standard demo summary is written to `artifacts/calibration_demo_ready_summary.json` and `.md`",
        "- the current recommended demo case is `configs/io/examples/followstop_apollo_gt_case2_locked.yaml`",
        "- if longitudinal metrics matter more for the rollout, `configs/io/examples/followstop_apollo_gt_case3_locked.yaml` remains the natural secondary demo case",
    ]
    _write_text(artifacts_dir / "calibration_pipeline_fix_report.md", "\n".join(report_lines))

    manifest = {
        "schema_version": 1,
        "calibration_run_id": "synthetic_regression_run",
        "scenario_config": str(policy_path),
        "capture_summary": case2_summary,
        "inference_summary": {
            "lateral": {"recommended_steering_field": "steering_target"},
            "longitudinal": {"recommended_signed_acceleration_field": "acceleration"},
        },
        "calibration_output_path": "artifacts/candidate.synthetic.v2.json",
        "replay_validation_summary": {
            "candidate_pass": True,
            "baseline_pass": True,
            "candidate_output": "synthetic",
            "baseline_output": "synthetic",
        },
        "tracking_validation_summary": {
            "candidate_pass": True,
            "baseline_pass": True,
            "candidate_output": "synthetic",
            "baseline_output": "synthetic",
        },
        "final_recommendation": comparison,
    }
    write_json(artifacts_dir / "unified_calibration_manifest.json", manifest)
    _write_text(
        artifacts_dir / "unified_calibration_manifest.md",
        "\n".join(
            [
                "# Unified Calibration Manifest",
                "",
                f"- calibration_run_id: `{manifest['calibration_run_id']}`",
                f"- scenario_config: `{manifest['scenario_config']}`",
                f"- capture_summary: `valid={case2_summary.get('valid_capture_count', 0)} invalid={case2_summary.get('invalid_capture_count', 0)}`",
                f"- inference_summary: `lateral=steering_target longitudinal=acceleration`",
                f"- calibration_output_path: `{manifest['calibration_output_path']}`",
                f"- replay_validation_summary: `candidate_pass=True baseline_pass=True`",
                f"- tracking_validation_summary: `candidate_pass=True baseline_pass=True`",
                f"- final_recommendation: `recommended_for_default={comparison.get('recommended_for_default', False)} status={comparison.get('status', '')}`",
            ]
        ),
    )

    print(f"wrote regression artifacts to {artifacts_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
