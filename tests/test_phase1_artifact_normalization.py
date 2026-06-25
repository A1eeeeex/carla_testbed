from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.phase1_artifact_normalization import normalize_phase1_artifacts


def test_promotes_unique_nested_timeseries_to_phase1_root(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested.mkdir(parents=True)
    (nested / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,1\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    assert report["status"] == "promoted"
    assert (run / "timeseries.csv").read_text(encoding="utf-8") == "sim_time,ego_speed_mps\n0,1\n"
    assert report["promoted_artifacts"] == [
        {"name": "timeseries", "source": str(nested / "timeseries.csv"), "destination": str(run / "timeseries.csv")}
    ]
    assert "does_not_change_runtime_behavior" in report["claim_boundary"]
    written = json.loads(
        (run / "analysis" / "phase1_artifact_normalization" / "phase1_artifact_normalization_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert written["status"] == "promoted"


def test_existing_root_timeseries_is_preserved(tmp_path: Path) -> None:
    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested.mkdir(parents=True)
    (run / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,root\n", encoding="utf-8")
    (nested / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,nested\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    assert report["status"] == "already_present"
    assert (run / "timeseries.csv").read_text(encoding="utf-8") == "sim_time,ego_speed_mps\n0,root\n"
    assert report["promoted_artifacts"] == []


def test_ambiguous_nested_timeseries_is_not_promoted(tmp_path: Path) -> None:
    run = tmp_path / "run"
    first = run / "legacy_a" / "actual"
    second = run / "legacy_b" / "actual"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / "timeseries.csv").write_text("sim_time\n0\n", encoding="utf-8")
    (second / "timeseries.csv").write_text("sim_time\n1\n", encoding="utf-8")

    report = normalize_phase1_artifacts(run)

    assert report["status"] == "ambiguous"
    assert not (run / "timeseries.csv").exists()
    assert "multiple_nested_timeseries_candidates" in report["warnings"]


def test_file_path_is_reported_as_error_without_crashing(tmp_path: Path) -> None:
    path = tmp_path / "LATEST.txt"
    path.write_text("run\n", encoding="utf-8")

    report = normalize_phase1_artifacts(path)

    assert report["status"] == "error"
    assert "run_dir_is_not_directory" in report["warnings"]


def test_route_only_phase1_comparison_artifacts_can_pass_after_promotion(tmp_path: Path) -> None:
    from carla_testbed.analysis.phase1_artifact_normalization import ensure_phase1_comparison_artifacts

    run = tmp_path / "run"
    nested = run / "legacy" / "actual"
    nested_artifacts = nested / "artifacts"
    nested_artifacts.mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "route_only",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "backend": "apollo_cyberrt",
                "backend_type": "apollo_reference_backend",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps({"success": False, "exit_reason": "platform_timeout"}),
        encoding="utf-8",
    )
    status_dir = run / "analysis" / "phase1_status"
    status_dir.mkdir(parents=True)
    (status_dir / "phase1_status.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_status.v1",
                "status": "failed",
                "failure_reason": "timeout",
                "run_evaluable": True,
                "target_actor_contract": {"status": "not_required", "required": False},
            }
        ),
        encoding="utf-8",
    )
    (nested / "timeseries.csv").write_text("sim_time,ego_speed_mps\n0,1\n", encoding="utf-8")
    (nested / "events.jsonl").write_text(json.dumps({"event": "run_end"}) + "\n", encoding="utf-8")
    (nested_artifacts / "control_apply_trace.jsonl").write_text(
        json.dumps({"sim_time": 0.0, "steer": 0.0}) + "\n",
        encoding="utf-8",
    )

    normalize_phase1_artifacts(run)
    surface = ensure_phase1_comparison_artifacts(run)

    assert surface["status"] == "pass"
    assert surface["artifact_complete"] is True
    assert surface["v_t_gap_status"] == "not_applicable"
    assert (run / "events.jsonl").exists()
    assert (run / "artifacts" / "control_apply_trace.jsonl").exists()
    assert (run / "analysis" / "v_t_gap" / "v_t_gap_report.json").exists()
    legacy = json.loads((run / "analysis" / "phase1_status" / "artifact_completeness.json").read_text())
    assert legacy["schema_version"] == "phase1_artifact_completeness.v1"
    assert legacy["status"] == "pass"
