from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.natural_driving_probe_plan import (
    ROUTE_START_PROBE_RESULT_SCHEMA_VERSION,
    build_route_start_probe_plan,
    evaluate_route_start_probe_result,
    write_route_start_probe_result,
)


def _report(run: dict) -> dict:
    return {
        "schema_version": "town01_natural_driving_report.v1",
        "run_results": [run],
    }


def _source_run() -> dict:
    return {
        "run_id": "source_lane097",
        "scenario_id": "lane_keep_097",
        "scenario_class": "lane_keep",
        "route_id": "town01_rh_spawn097_goal046",
        "verdict": "fail",
        "failure_reason": "route_start_lane_invasion",
        "route_completion": 0.61,
        "lateral_error_p95": 1.04,
        "heading_error_p95": 0.014,
        "lane_invasion_count": 1,
        "collision_count": 0,
        "failure_route_s": -0.61,
        "recommended_ego_offset_y_delta_m": 0.5089,
        "missing_artifacts": [],
        "missing_fields": [],
    }


def _route_completion_source_run() -> dict:
    payload = _source_run()
    payload.update(
        {
            "failure_reason": "route_completion_too_low",
            "route_completion": 0.45,
            "lane_invasion_count": 0,
            "failure_route_s": 99.85,
            "route_start_alignment_reason": "spawn_lateral_offset_high",
            "static_spawn_lateral_offset_m": -0.5089,
        }
    )
    return payload


def _probe_run(**overrides) -> dict:
    payload = {
        "run_id": "probe_lane097",
        "scenario_id": "lane_keep_097",
        "scenario_class": "lane_keep",
        "route_id": "town01_rh_spawn097_goal046",
        "verdict": "pass",
        "failure_reason": None,
        "route_completion": 0.75,
        "lateral_error_p95": 0.82,
        "heading_error_p95": 0.012,
        "lane_invasion_count": 0,
        "collision_count": 0,
        "missing_artifacts": [],
        "missing_fields": [],
    }
    payload.update(overrides)
    return payload


def test_route_start_probe_positive_when_lane_invasion_resolves_and_completion_improves() -> None:
    result = evaluate_route_start_probe_result(_report(_source_run()), _report(_probe_run()))

    assert result["schema_version"] == ROUTE_START_PROBE_RESULT_SCHEMA_VERSION
    assert result["status"] == "positive"
    assert result["reason"] == "route_start_alignment_probe_supported"
    assert result["comparison"]["lane_invasion_resolved"] is True
    assert result["comparison"]["route_completion_delta"] > 0
    assert result["claim_boundary"]["can_claim_curve_health"] is False
    assert result["claim_boundary"]["can_change_steer_scale"] is False


def test_route_start_probe_plan_uses_alignment_recommendation_for_low_completion() -> None:
    plan = build_route_start_probe_plan(_report(_route_completion_source_run()))

    assert plan["status"] == "ready"
    assert plan["probe_count"] == 1
    assert plan["probes"][0]["candidate_reason"] == "route_start_alignment:spawn_lateral_offset_high"
    assert plan["probes"][0]["failure_reason"] == "route_completion_too_low"
    assert plan["probes"][0]["override"] == "scenario.route_health.ego_offset_y_m=0.5089"
    assert plan["claim_boundary"]["does_not_change_steer_scale"] is True
    assert plan["claim_boundary"]["does_not_enable_physical_mapping"] is True


def test_route_start_probe_plan_handles_spawn_offset_with_later_collision() -> None:
    source = _route_completion_source_run()
    source.update(
        {
            "failure_reason": "collision",
            "route_completion": 0.89,
            "lane_invasion_count": 9,
            "collision_count": 52,
            "failure_route_s": 242.4,
        }
    )

    plan = build_route_start_probe_plan(_report(source))

    assert plan["status"] == "ready"
    assert plan["probe_count"] == 1
    assert plan["probes"][0]["candidate_reason"] == "route_start_alignment:spawn_lateral_offset_high"
    assert plan["probes"][0]["failure_reason"] == "collision"
    assert plan["probes"][0]["override"] == "scenario.route_health.ego_offset_y_m=0.5089"
    assert plan["claim_boundary"]["does_not_change_steer_scale"] is True
    assert plan["claim_boundary"]["does_not_enable_physical_mapping"] is True


def test_route_start_probe_plan_does_not_repeat_compensated_probe() -> None:
    source = _route_completion_source_run()
    source.update(
        {
            "failure_reason": "lane_invasion",
            "route_completion": 0.93,
            "initial_rear_axle_offset_compatible": True,
        }
    )

    plan = build_route_start_probe_plan(_report(source))

    assert plan["status"] == "no_probe_candidates"
    assert plan["probe_count"] == 0


def test_route_start_probe_result_allows_low_completion_alignment_probe() -> None:
    result = evaluate_route_start_probe_result(
        _report(_route_completion_source_run()),
        _report(_probe_run(route_completion=0.67, lane_invasion_count=0)),
    )

    assert result["status"] == "positive"
    assert result["reason"] == "route_start_alignment_probe_supported"
    assert result["comparison"]["source_probe_candidate_reason"] == "route_start_alignment:spawn_lateral_offset_high"
    assert result["claim_boundary"]["can_claim_curve_health"] is False


def test_route_start_probe_result_keeps_positive_with_latency_gap_warning() -> None:
    result = evaluate_route_start_probe_result(
        _report(_route_completion_source_run()),
        _report(
            _probe_run(
                verdict="insufficient_data",
                failure_reason="missing_required_metrics",
                route_completion=0.95,
                missing_fields=["control_latency_p95_ms"],
            )
        ),
    )

    assert result["status"] == "positive"
    assert result["reason"] == "route_start_alignment_probe_supported"
    assert "probe_missing_non_blocking_field:control_latency_p95_ms" in result["evidence_warnings"]
    assert result["claim_boundary"]["can_claim_curve_health"] is False


def test_route_start_probe_result_uses_warn_latency_gap_warning_wording() -> None:
    result = evaluate_route_start_probe_result(
        _report(_route_completion_source_run()),
        _report(
            _probe_run(
                verdict="warn",
                failure_reason="control_latency_missing",
                route_completion=0.95,
                missing_fields=["control_latency_p95_ms"],
            )
        ),
    )

    assert result["status"] == "positive"
    assert "probe_natural_driving_verdict_warn_due_non_blocking_fields" in result["evidence_warnings"]
    assert "probe_natural_driving_verdict_insufficient_data_due_non_blocking_fields" not in result["evidence_warnings"]


def test_route_start_probe_negative_when_route_start_lane_invasion_persists() -> None:
    result = evaluate_route_start_probe_result(
        _report(_source_run()),
        _report(
            _probe_run(
                verdict="fail",
                failure_reason="route_start_lane_invasion",
                route_completion=0.55,
                lane_invasion_count=1,
            )
        ),
    )

    assert result["status"] == "negative"
    assert result["reason"] == "route_start_lane_invasion_persisted"


def test_route_start_probe_inconclusive_for_link_health_failure() -> None:
    result = evaluate_route_start_probe_result(
        _report(_source_run()),
        _report(
            _probe_run(
                verdict="fail",
                failure_reason="planning_missing",
                lane_invasion_count=0,
            )
        ),
    )

    assert result["status"] == "inconclusive"
    assert result["reason"] == "probe_link_health_failure"


def test_route_start_probe_insufficient_when_probe_missing_artifacts() -> None:
    result = evaluate_route_start_probe_result(
        _report(_source_run()),
        _report(_probe_run(missing_artifacts=["summary.json"])),
    )

    assert result["status"] == "insufficient_data"
    assert result["reason"] == "probe_missing_required_evidence"


def test_route_start_probe_result_writer_and_cli(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    probe_path = tmp_path / "probe.json"
    source_path.write_text(json.dumps(_report(_source_run()), indent=2) + "\n", encoding="utf-8")
    probe_path.write_text(json.dumps(_report(_probe_run()), indent=2) + "\n", encoding="utf-8")

    outputs = write_route_start_probe_result(
        evaluate_route_start_probe_result(_report(_source_run()), _report(_probe_run())),
        tmp_path / "direct",
    )
    assert Path(outputs["route_start_probe_result"]).is_file()
    assert Path(outputs["route_start_probe_result_summary"]).is_file()

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_route_start_probe_result.py",
            "--source-report",
            str(source_path),
            "--probe-report",
            str(probe_path),
            "--out",
            str(tmp_path / "cli"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "positive"
    assert Path(payload["outputs"]["route_start_probe_result"]).is_file()
