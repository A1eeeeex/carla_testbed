from __future__ import annotations

import csv
import json
from pathlib import Path

from carla_testbed.experiments.route_start_offset_sweep import (
    ROUTE_START_OFFSET_SWEEP_SCHEMA_VERSION,
    analyze_route_start_offset_sweep,
    infer_offset_from_suite_root,
    write_route_start_offset_sweep_report,
)


def _write_report(
    path: Path,
    *,
    verdict: str,
    failure_reason: str,
    route_completion: float,
    lane_invasion_count: int,
    control_latency_p95_ms: float | None = 2.0,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "natural_driving_report.v1",
                "run_results": [
                    {
                        "run_id": "lane_keep_097_run",
                        "scenario_id": "lane_keep_097",
                        "scenario_class": "lane_keep",
                        "route_id": "town01_rh_spawn097_goal046",
                        "verdict": verdict,
                        "failure_reason": failure_reason,
                        "route_completion": route_completion,
                        "lateral_error_p95": 0.70,
                        "heading_error_p95": 0.006,
                        "control_latency_p95_ms": control_latency_p95_ms,
                        "lane_invasion_count": lane_invasion_count,
                        "collision_count": 0,
                        "recommended_ego_offset_y_delta_m": 0.5089201844029378,
                        "missing_fields": [] if control_latency_p95_ms is not None else ["control_latency_p95_ms"],
                        "missing_artifacts": [],
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_matrix(root: Path, *, offset: float) -> None:
    root.mkdir(parents=True, exist_ok=True)
    with (root / "run_matrix.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["scenario_id", "runner_overrides_json", "command"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "scenario_id": "lane_keep_097",
                "runner_overrides_json": json.dumps(
                    [f"scenario.route_health.ego_offset_y_m={offset}"]
                ),
                "command": "",
            }
        )


def _probe_report_path(root: Path) -> Path:
    return root / "analysis" / "natural_driving" / "natural_driving_report.json"


def test_offset_sweep_detects_repeatability_conflict(tmp_path: Path) -> None:
    source = tmp_path / "source" / "analysis" / "natural_driving" / "natural_driving_report.json"
    _write_report(
        source,
        verdict="fail",
        failure_reason="route_completion_too_low",
        route_completion=0.45,
        lane_invasion_count=0,
        control_latency_p95_ms=None,
    )
    probe_a_root = tmp_path / "probe_a"
    probe_b_root = tmp_path / "probe_b"
    _write_matrix(probe_a_root, offset=0.5089201844029378)
    _write_matrix(probe_b_root, offset=0.5089201844029378)
    _write_report(
        _probe_report_path(probe_a_root),
        verdict="warn",
        failure_reason="control_latency_missing",
        route_completion=0.95,
        lane_invasion_count=0,
        control_latency_p95_ms=None,
    )
    _write_report(
        _probe_report_path(probe_b_root),
        verdict="fail",
        failure_reason="lane_invasion",
        route_completion=0.94,
        lane_invasion_count=1,
    )

    report = analyze_route_start_offset_sweep(
        source,
        [_probe_report_path(probe_a_root), _probe_report_path(probe_b_root)],
        scenario_id="lane_keep_097",
    )

    assert report["schema_version"] == ROUTE_START_OFFSET_SWEEP_SCHEMA_VERSION
    assert report["status"] == "warn"
    assert report["reason"] == "offset_repeatability_conflict"
    assert report["claim_boundary"]["can_claim_lane_keep_fix"] is False
    assert report["summary"]["repeatability_conflict_offsets"] == [0.5089201844029378]
    assert report["summary"]["candidate_positive_count"] == 1
    assert report["summary"]["safety_regressed_count"] == 1
    assert report["required_next_actions"]


def test_offset_sweep_writes_json_csv_and_markdown(tmp_path: Path) -> None:
    source = tmp_path / "source" / "analysis" / "natural_driving" / "natural_driving_report.json"
    probe_root = tmp_path / "probe"
    _write_report(
        source,
        verdict="fail",
        failure_reason="route_completion_too_low",
        route_completion=0.45,
        lane_invasion_count=0,
    )
    _write_matrix(probe_root, offset=0.25)
    _write_report(
        _probe_report_path(probe_root),
        verdict="pass",
        failure_reason="",
        route_completion=0.70,
        lane_invasion_count=0,
    )

    report = analyze_route_start_offset_sweep(source, [_probe_report_path(probe_root)])
    outputs = write_route_start_offset_sweep_report(report, tmp_path / "out")

    assert Path(outputs["route_start_offset_sweep_report"]).is_file()
    assert Path(outputs["route_start_offset_sweep_csv"]).is_file()
    assert Path(outputs["route_start_offset_sweep_summary"]).is_file()
    rows = list(csv.DictReader(open(outputs["route_start_offset_sweep_csv"], encoding="utf-8", newline="")))
    assert rows[0]["offset_y_m"] == "0.25"
    assert rows[0]["status"] == "candidate_positive"


def test_infer_offset_from_suite_run_matrix(tmp_path: Path) -> None:
    _write_matrix(tmp_path, offset=0.382)

    assert infer_offset_from_suite_root(tmp_path, scenario_id="lane_keep_097") == 0.382
