from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.algorithms.gt_replacement_matrix import load_gt_replacement_matrix
from carla_testbed.analysis.prediction_evidence import (
    analyze_prediction_evidence_run_dir,
    write_prediction_evidence_report,
)

REFERENCE = "configs/reference/apollo_reference_chain.yaml"
REPLACEMENT = "configs/reference/apollo_gt_replacement_matrix.yaml"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_dir(tmp_path: Path, *, scenario_class: str = "lane_keep", bypass_reason: str | None = None) -> Path:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "route_id": "097",
            "scenario_id": scenario_class,
            "scenario_class": scenario_class,
            "planning_requires_prediction": True,
            **({"prediction_bypass_reason": bypass_reason} if bypass_reason else {}),
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "route_id": "097",
            "scenario_class": scenario_class,
            **({"prediction_bypass_reason": bypass_reason} if bypass_reason else {}),
        },
    )
    _write_json(
        run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        {"schema_version": "obstacle_gt_contract.v1", "status": "pass"},
    )
    return run_dir


def _channel_stats(run_dir: Path, *, prediction_count: int = 0, obstacle_count: int = 5) -> None:
    channels: dict[str, dict] = {}
    if prediction_count:
        channels["/apollo/prediction"] = {
            "message_count": prediction_count,
            "hz": 10.0,
            "max_gap_ms": 120.0,
            "timestamp_monotonic": True,
            "sequence_monotonic": True,
            "stale_count": 0,
        }
    if obstacle_count:
        channels["/apollo/perception/obstacles"] = {
            "message_count": obstacle_count,
            "hz": 10.0,
            "max_gap_ms": 120.0,
            "timestamp_monotonic": True,
            "sequence_monotonic": True,
            "stale_count": 0,
        }
    _write_json(run_dir / "channel_stats.json", {"schema_version": "channel_stats.v1", "channels": channels})


def test_prediction_native_observed_passes(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _channel_stats(run_dir, prediction_count=25)

    report = analyze_prediction_evidence_run_dir(run_dir)

    assert report["prediction_mode"] == "native_observed"
    assert report["prediction_channel_available"] is True
    assert report["prediction_message_count"] == 25
    assert report["verdict"] == "pass"
    assert report["hard_gate_eligible"] is True


def test_missing_prediction_without_bypass_is_insufficient_or_fail(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path, scenario_class="lane_keep")
    _channel_stats(run_dir, prediction_count=0)

    report = analyze_prediction_evidence_run_dir(run_dir, replacement_matrix_path=None)

    assert report["prediction_mode"] == "missing"
    assert report["verdict"] == "insufficient_data"
    assert "closed_loop" in report["blocking_capabilities"]
    assert report["hard_gate_eligible"] is False


def test_static_lane_keep_uses_default_matrix_bypass_reason(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path, scenario_class="lane_keep")
    _channel_stats(run_dir, prediction_count=0, obstacle_count=4)

    report = analyze_prediction_evidence_run_dir(run_dir)

    assert report["prediction_mode"] == "bypassed_with_gt_obstacles"
    assert report["verdict"] == "warn"
    assert report["hard_gate_eligible"] is False
    assert report["claim_boundary_downgraded"] is True
    assert report["prediction_bypass_scope"] == "static_lane_keep_diagnostic"
    assert "closed_loop" in report["blocking_capabilities"]
    assert report["bypass_reason_source"] == "replacement_matrix"
    assert report["bypass_reason"]


def test_bypassed_with_reason_lane_keep_warns(tmp_path: Path) -> None:
    run_dir = _run_dir(
        tmp_path,
        scenario_class="lane_keep",
        bypass_reason="static lane_keep uses GT obstacles; no dynamic prediction claim",
    )
    _channel_stats(run_dir, prediction_count=0, obstacle_count=4)

    report = analyze_prediction_evidence_run_dir(run_dir)

    assert report["prediction_mode"] == "bypassed_with_gt_obstacles"
    assert report["verdict"] == "warn"
    assert report["hard_gate_eligible"] is False
    assert report["claim_boundary_downgraded"] is True
    assert "closed_loop" in report["blocking_capabilities"]
    assert "perception_obstacles_do_not_count_as_prediction" in report["warnings"]


def test_bypassed_with_reason_dynamic_obstacle_fails_without_override(tmp_path: Path) -> None:
    run_dir = _run_dir(
        tmp_path,
        scenario_class="dynamic_obstacle",
        bypass_reason="diagnostic bypass should not pass dynamic obstacle case",
    )
    _channel_stats(run_dir, prediction_count=0, obstacle_count=4)

    report = analyze_prediction_evidence_run_dir(run_dir)

    assert report["prediction_mode"] == "bypassed_with_gt_obstacles"
    assert report["verdict"] == "fail"
    assert report["hard_gate_eligible"] is False
    assert report["claim_boundary_downgraded"] is True
    assert "closed_loop" in report["blocking_capabilities"]


def test_prediction_log_errors_warn_or_fail(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _channel_stats(run_dir, prediction_count=10)
    log = run_dir / "apollo_prediction.log"
    log.write_text("INFO ok\nERROR prediction fallback\n", encoding="utf-8")

    report = analyze_prediction_evidence_run_dir(run_dir)

    assert report["prediction_mode"] == "native_observed"
    assert report["verdict"] == "warn"
    assert report["prediction_errors"]


def test_prediction_fatal_log_fails(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _channel_stats(run_dir, prediction_count=10)
    log = run_dir / "apollo_prediction.log"
    log.write_text("FATAL prediction crashed\n", encoding="utf-8")

    report = analyze_prediction_evidence_run_dir(run_dir)

    assert report["verdict"] == "fail"
    assert "closed_loop" in report["blocking_capabilities"]


def test_write_prediction_evidence_report(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _channel_stats(run_dir, prediction_count=3)
    report = analyze_prediction_evidence_run_dir(run_dir)

    outputs = write_prediction_evidence_report(report, tmp_path / "out")

    assert Path(outputs["prediction_evidence_report"]).exists()
    assert Path(outputs["prediction_evidence_summary"]).exists()


def test_default_replacement_matrix_prediction_is_not_unknown() -> None:
    matrix = load_gt_replacement_matrix(REPLACEMENT)
    prediction = next(module for module in matrix["modules"] if module["name"] == "prediction")

    assert prediction["replacement_status"] != "unknown"
    assert prediction["bypass_reason"]
    assert "traffic_light" in prediction["blocked_capabilities"]
