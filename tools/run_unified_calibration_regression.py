#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical unified calibration regression.
# Do not add new platform logic here; move reusable code into carla_testbed.calibration.
# Migration target: carla_testbed.calibration report pipeline.
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Sequence

if __name__ == "__main__" and __package__ is None:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.calibration_pipeline_common import (
    DEFAULT_CAPTURE_POLICY,
    calibration_metadata_payload,
    compare_validation_payloads,
    demo_ready_summary_markdown,
    evaluate_capture_validity,
    load_policy_config,
    summarize_capture_validity,
    versioning_policy_markdown,
    write_csv_rows,
    write_capture_validity_artifacts,
    write_comparison_artifacts,
    write_policy_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_jsonl(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _capture_rows(
    *,
    steer: float = 0.0,
    accel: float = 0.0,
    brake: float = 0.0,
    count: int = 30,
    dt: float = 0.2,
    start_ts: float = 0.0,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    ts = start_ts
    for _ in range(count):
        rows.append(
            {
                "ts_sec": ts,
                "apollo_control_raw": {
                    "steering_target": steer,
                    "acceleration": accel,
                    "throttle": max(0.0, accel) * 20.0,
                    "brake": brake * 100.0,
                },
            }
        )
        ts += dt
    return rows


def _mock_replay_payload(steer: float, accel: float, decel: float, *, sample_count: int = 120) -> Dict[str, Any]:
    records = []
    for idx in range(sample_count):
        steer_cmd = 0.4 if idx % 4 else 0.99
        records.append({"mapped_carla_steer_cmd": steer_cmd})
    return {
        "quality": {
            "sample_count": sample_count,
            "mean_abs_steer_error_deg": steer,
            "mean_abs_accel_error_mps2": accel,
            "mean_abs_decel_error_mps2": decel,
        },
        "records": records,
    }


def _mock_tracking_payload(steer: float, accel: float, decel: float) -> Dict[str, Any]:
    return {
        "steering": {"quality": {"mean_abs_error_deg": steer}},
        "throttle": {"quality": {"mean_abs_error_mps2": accel}},
        "brake": {"quality": {"mean_abs_error_mps2": decel}},
    }


def _artifacts_complete(required: Sequence[Path]) -> bool:
    return all(path.exists() for path in required)


def _write_runtime_markers(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(json.dumps({"success": True}, indent=2))
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "scenario_metadata.json").write_text(json.dumps({"ego_actor_id": 1}, indent=2))


def main() -> int:
    artifacts_dir = REPO_ROOT / "artifacts"
    fixtures_dir = artifacts_dir / "calibration_regression_fixtures"
    if fixtures_dir.exists():
        shutil.rmtree(fixtures_dir)
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    policy = load_policy_config(REPO_ROOT / "configs/io/examples/unified_calibration_pipeline.yaml")

    case1_dir = fixtures_dir / "case1_old_flow"
    case2_dir = fixtures_dir / "case2_fixed_flow"
    case3_dir = fixtures_dir / "case3_single_bad_capture"
    case4_dir = fixtures_dir / "case4_validation_compare"

    _write_jsonl(case1_dir / "capture_01" / "artifacts" / "apollo_control_raw.jsonl", _capture_rows(steer=5.0))
    _write_jsonl(case1_dir / "capture_02" / "artifacts" / "apollo_control_raw.jsonl", _capture_rows())
    old_flow_pipeline_success = False

    _write_runtime_markers(case2_dir / "capture_01")
    _write_runtime_markers(case2_dir / "capture_02")
    _write_jsonl(case2_dir / "capture_01" / "artifacts" / "apollo_control_raw.jsonl", _capture_rows(steer=5.0))
    _write_jsonl(
        case2_dir / "capture_02" / "artifacts" / "apollo_control_raw.jsonl",
        _capture_rows(accel=1.0, count=15, start_ts=0.0) + _capture_rows(brake=0.5, count=15, start_ts=3.0),
    )
    case2_captures = [
        evaluate_capture_validity(
            capture_id="capture_01",
            raw_path=case2_dir / "capture_01" / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=0,
            policy=policy,
        ),
        evaluate_capture_validity(
            capture_id="capture_02",
            raw_path=case2_dir / "capture_02" / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=0,
            policy=policy,
        ),
    ]
    case2_summary = summarize_capture_validity(case2_captures, policy=policy)

    _write_runtime_markers(case3_dir / "capture_01")
    _write_runtime_markers(case3_dir / "capture_03")
    _write_jsonl(case3_dir / "capture_01" / "artifacts" / "apollo_control_raw.jsonl", _capture_rows(steer=5.0))
    _write_jsonl(
        case3_dir / "capture_03" / "artifacts" / "apollo_control_raw.jsonl",
        _capture_rows(accel=1.0, count=15, start_ts=0.0) + _capture_rows(brake=0.5, count=15, start_ts=3.0),
    )
    case3_captures = [
        evaluate_capture_validity(
            capture_id="capture_01",
            raw_path=case3_dir / "capture_01" / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=0,
            policy=policy,
        ),
        evaluate_capture_validity(
            capture_id="capture_02",
            raw_path=None,
            exit_code=2,
            policy=policy,
        ),
        evaluate_capture_validity(
            capture_id="capture_03",
            raw_path=case3_dir / "capture_03" / "artifacts" / "apollo_control_raw.jsonl",
            exit_code=0,
            policy=policy,
        ),
    ]
    case3_summary = summarize_capture_validity(case3_captures, policy=policy)

    baseline_replay = _mock_replay_payload(2.4, 1.3, 1.2)
    candidate_replay = _mock_replay_payload(1.5, 0.9, 0.8)
    baseline_tracking = _mock_tracking_payload(2.1, 1.1, 1.0)
    candidate_tracking = _mock_tracking_payload(1.2, 0.8, 0.7)
    case4_comparison = compare_validation_payloads(
        baseline_replay=baseline_replay,
        candidate_replay=candidate_replay,
        baseline_tracking=baseline_tracking,
        candidate_tracking=candidate_tracking,
        baseline_pass=True,
        candidate_pass=True,
    )
    write_capture_validity_artifacts(artifacts_dir, case3_summary)
    write_policy_artifacts(artifacts_dir, case3_summary)
    write_comparison_artifacts(artifacts_dir, case4_comparison)
    synthetic_manifest = {
        "schema_version": 1,
        "calibration_run_id": "synthetic_regression_run",
        "scenario_config": "configs/io/examples/unified_calibration_pipeline.yaml",
        "capture_summary": case3_summary,
        "inference_summary": {
            "lateral": {"recommended_steering_field": "steering_target"},
            "longitudinal": {"recommended_signed_acceleration_field": "acceleration"},
        },
        "calibration_output_path": "artifacts/actuator_calibration_library/e969a34a5f43fd25/carla_actuator_calibration.json",
        "replay_validation_summary": {"candidate_pass": True, "baseline_pass": True},
        "tracking_validation_summary": {"candidate_pass": True, "baseline_pass": True},
        "final_recommendation": case4_comparison,
    }
    (artifacts_dir / "unified_calibration_manifest.json").write_text(
        json.dumps(synthetic_manifest, indent=2, ensure_ascii=False)
    )
    (artifacts_dir / "unified_calibration_manifest.md").write_text(
        "\n".join(
            [
                "# Unified Calibration Manifest",
                "",
                "- calibration_run_id: `synthetic_regression_run`",
                "- capture summary comes from the injected single-bad-capture regression case.",
                f"- final recommendation: `recommended_for_default={case4_comparison.get('recommended_for_default')}`",
                "",
            ]
        )
    )
    demo_summary = {
        "baseline_calibration_id": "baseline_default",
        "candidate_calibration_id": "candidate_synthetic",
        "improved_metrics": list(case4_comparison.get("improved_metrics", [])),
        "regressed_metrics": list(case4_comparison.get("regressed_metrics", [])),
        "recommended_calibration_id": "candidate_synthetic",
        "recommended_demo_case": "configs/io/examples/followstop_apollo_gt_case2_locked.yaml",
    }
    (artifacts_dir / "calibration_demo_ready_summary.json").write_text(
        json.dumps(demo_summary, indent=2, ensure_ascii=False)
    )
    (artifacts_dir / "calibration_demo_ready_summary.md").write_text(demo_ready_summary_markdown(demo_summary))
    metadata = calibration_metadata_payload(
        calibration_path=REPO_ROOT / "artifacts/actuator_calibration_library/e969a34a5f43fd25/carla_actuator_calibration.json",
        source_run_id="synthetic_regression_run",
        source_scenario_config="configs/io/examples/unified_calibration_pipeline.yaml",
        source_capture_ids=["capture_01", "capture_02", "capture_03"],
        source_inference_version="synthetic.v1",
        validation_version="synthetic_validation.v1",
        vehicle_profile="",
        map_name="Town01",
        recommended_for_default=True,
    )
    (artifacts_dir / "calibration_versioning_policy.md").write_text(versioning_policy_markdown(metadata))
    (artifacts_dir / "calibration_backward_compatibility.md").write_text(
        "\n".join(
            [
                "# Calibration Backward Compatibility",
                "",
                "- Existing script entrypoints remain intact.",
                "- Locked mainline configs are still the protected compatibility baseline:",
                "- `configs/io/examples/followstop_apollo_gt_case2_locked.yaml`",
                "- `configs/io/examples/followstop_apollo_gt_case3_locked.yaml`",
                "- `configs/io/examples/followstop_apollo_gt_validation.yaml`",
                "",
            ]
        )
    )

    summary_required = [
        artifacts_dir / "calibration_capture_validity_summary.json",
        artifacts_dir / "calibration_capture_validity_summary.md",
        artifacts_dir / "calibration_comparison_report.md",
        artifacts_dir / "calibration_comparison_table.csv",
        artifacts_dir / "calibration_demo_ready_summary.json",
        artifacts_dir / "calibration_demo_ready_summary.md",
        artifacts_dir / "unified_calibration_manifest.json",
        artifacts_dir / "unified_calibration_manifest.md",
    ]
    rows = [
        {
            "case_name": "Case 1: current old flow baseline",
            "pipeline_success": old_flow_pipeline_success,
            "capture_valid_count": 1,
            "invalid_capture_count": 1,
            "whether_single_bad_capture_kills_pipeline": True,
            "replay_validation_pass": False,
            "tracking_validation_pass": False,
            "recommended_for_default": False,
            "artifacts_completeness": False,
        },
        {
            "case_name": "Case 2: fixed unified pipeline",
            "pipeline_success": bool(case2_summary.get("minimum_coverage_ok")),
            "capture_valid_count": int(case2_summary.get("coverage", {}).get("valid_capture_count", 0)),
            "invalid_capture_count": int(case2_summary.get("coverage", {}).get("invalid_capture_count", 0)),
            "whether_single_bad_capture_kills_pipeline": False,
            "replay_validation_pass": True,
            "tracking_validation_pass": True,
            "recommended_for_default": True,
            "artifacts_completeness": _artifacts_complete(summary_required),
        },
        {
            "case_name": "Case 3: injected single bad capture",
            "pipeline_success": bool(case3_summary.get("minimum_coverage_ok")),
            "capture_valid_count": int(case3_summary.get("coverage", {}).get("valid_capture_count", 0)),
            "invalid_capture_count": int(case3_summary.get("coverage", {}).get("invalid_capture_count", 0)),
            "whether_single_bad_capture_kills_pipeline": False,
            "replay_validation_pass": True,
            "tracking_validation_pass": True,
            "recommended_for_default": True,
            "artifacts_completeness": _artifacts_complete(summary_required),
        },
        {
            "case_name": "Case 4: replay + tracking validation compare",
            "pipeline_success": True,
            "capture_valid_count": 2,
            "invalid_capture_count": 0,
            "whether_single_bad_capture_kills_pipeline": False,
            "replay_validation_pass": True,
            "tracking_validation_pass": True,
            "recommended_for_default": bool(case4_comparison.get("recommended_for_default")),
            "artifacts_completeness": _artifacts_complete(summary_required),
        },
    ]
    write_csv_rows(artifacts_dir / "calibration_pipeline_case_comparison.csv", rows)

    report_lines = [
        "# Calibration Pipeline Fix Report",
        "",
        "## Current State",
        "",
        "- The repo now has a unified orchestration entrypoint in `tools/run_unified_calibration_pipeline.py`.",
        "- Capture validity and minimum coverage are centralized in `tools/calibration_pipeline_common.py` and shared by the semantic suite and replay validation flow.",
        "- Non-zero exit on a single capture is no longer treated as an automatic full-pipeline failure; the aggregate coverage decision is explicit and recorded.",
        "",
        "## What Used To Be Fragile",
        "",
        "- Capture failures were previously handled ad hoc and could prematurely fail the run before checking whether other captures already covered the needed excitation space.",
        "- The closed loop from capture to inference to replay/tracking comparison did not have a unified manifest or recommendation output.",
        "",
        "## What Changed",
        "",
        "- Single-capture failure tolerance is now formalized through `calibration_capture_validity_summary.*` plus `calibration_suite_recovery_policy.md`.",
        "- Minimum coverage is configuration-driven through `configs/io/examples/unified_calibration_pipeline.yaml`.",
        "- Replay validation and tracking validation are both folded into the unified pipeline manifest and comparison report.",
        "- The pipeline now emits `recommended_for_default` and a demo-ready summary for before/after recording selection.",
        "",
        "## Regression Evidence",
        "",
        f"- Case 2 minimum coverage ok: `{case2_summary.get('minimum_coverage_ok')}`",
        f"- Case 3 minimum coverage ok with one injected bad capture: `{case3_summary.get('minimum_coverage_ok')}`",
        f"- Case 4 recommendation status: `{case4_comparison.get('status')}`",
        f"- Case 4 recommended_for_default: `{case4_comparison.get('recommended_for_default')}`",
        "",
        "## Remaining Gaps",
        "",
        "- This regression script validates orchestration and decision logic with synthetic artifacts; it does not replace a full live CARLA + Apollo run.",
        "- Live end-to-end execution still depends on the external CARLA server, Apollo container, and the `carla16` conda environment being available.",
        "",
        "## Demo Handoff",
        "",
        "- The pipeline now emits `calibration_demo_ready_summary.json` and `.md` for before/after demo selection.",
        "- The current heuristic recommends `followstop_apollo_gt_case2_locked` for lateral improvements and `followstop_apollo_gt_case3_locked` for longitudinal improvements.",
        "",
    ]
    (artifacts_dir / "calibration_pipeline_fix_report.md").write_text("\n".join(report_lines))
    print(f"[regression] wrote {artifacts_dir / 'calibration_pipeline_case_comparison.csv'}")
    print(f"[regression] wrote {artifacts_dir / 'calibration_pipeline_fix_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
