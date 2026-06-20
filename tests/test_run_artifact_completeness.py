from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from carla_testbed.analysis.artifact_completeness import (
    RUN_ARTIFACT_COMPLETENESS_SCHEMA_VERSION,
    check_run_artifact_completeness,
    write_run_artifact_completeness_report,
)

FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def _copy_run(tmp_path: Path, name: str = "lane_keep_097") -> Path:
    target = tmp_path / name
    shutil.copytree(FIXTURE_ROOT / name, target)
    return target


def _rewrite_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_minimal_phase1_run(
    tmp_path: Path,
    *,
    scenario_case: str = "lane_keep_straight",
    backend_type: str = "planning_control_backend",
    target_required: bool = False,
) -> Path:
    run_dir = tmp_path / f"{scenario_case}_{backend_type}"
    (run_dir / "analysis" / "phase1_status").mkdir(parents=True)
    (run_dir / "analysis" / "v_t_gap").mkdir(parents=True)
    manifest = {
        "schema_version": "phase1_run_manifest.v1",
        "run_id": run_dir.name,
        "scenario_case": scenario_case,
        "backend": "carla_builtin" if backend_type == "planning_control_backend" else "apollo",
        "backend_type": backend_type,
        "route_id": "route_lane_keep_straight",
        "target_actor_contract": {
            "required": target_required,
            "status": "resolved" if target_required else "not_required",
        },
    }
    summary = {
        "run_id": run_dir.name,
        "scenario_id": scenario_case,
        "scenario_class": scenario_case,
        "route_id": "route_lane_keep_straight",
    }
    phase1_status = {
        "schema_version": "phase1_status.v1",
        "status": "evaluable",
        "scenario_case": scenario_case,
        "backend_type": backend_type,
        "target_actor_contract": manifest["target_actor_contract"],
        "target_metric_status": "not_applicable" if not target_required else "evaluable",
    }
    v_t_gap = {
        "schema_version": "v_t_gap.v1",
        "status": "not_applicable" if not target_required else "pass",
        "scenario_case": scenario_case,
        "target_actor_required": target_required,
    }
    _rewrite_json(run_dir / "manifest.json", manifest)
    _rewrite_json(run_dir / "summary.json", summary)
    (run_dir / "timeseries.csv").write_text("sim_time,ego_speed\n0.0,0.0\n", encoding="utf-8")
    _rewrite_json(run_dir / "analysis" / "phase1_status" / "phase1_status.json", phase1_status)
    _rewrite_json(run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json", v_t_gap)
    return run_dir


def test_complete_lane_keep_run_passes_artifact_check(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)

    report = check_run_artifact_completeness(run_dir)

    assert report["schema_version"] == RUN_ARTIFACT_COMPLETENESS_SCHEMA_VERSION
    assert report["run_id"] == "lane_keep_097"
    assert report["scenario_id"] == "lane_keep_097"
    assert report["route_id"] == "lane097"
    assert report["scenario_class"] == "lane_keep"
    assert report["status"] == "pass"
    assert report["artifact_complete"] is True
    assert report["summary_complete"] is True
    assert report["raw_evidence_complete"] is True
    assert report["claim_or_materialization_profile"] is False
    assert report["missing_artifacts"] == []
    assert report["missing_raw_evidence_artifacts"] == []
    assert report["control_trace_available"] is True
    assert report["invalid_report_source_fields"] == []


def test_claim_profile_missing_raw_evidence_does_not_pass_artifact_check(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["claim_profile"] = True
    manifest["online_config_profile_name"] = "town01_route_materialization_probe"
    _rewrite_json(manifest_path, manifest)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["summary_complete"] is True
    assert report["raw_evidence_complete"] is False
    assert report["artifact_complete"] is False
    assert report["claim_or_materialization_profile"] is True
    assert "route.json" in report["missing_raw_evidence_artifacts"]
    assert "artifacts/route_definition_claim.json" in report["missing_raw_evidence_artifacts"]
    assert "artifacts/topic_publish_stats.jsonl" in report["missing_raw_evidence_artifacts"]
    assert "artifacts/routing_response_decoded.jsonl" in report["missing_raw_evidence_artifacts"]
    assert "artifacts/planning_topic_debug.jsonl" in report["missing_raw_evidence_artifacts"]
    assert "artifacts/control_apply_trace.jsonl" in report["missing_raw_evidence_artifacts"]


def test_claim_profile_empty_hdmap_projection_does_not_pass_raw_evidence(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["claim_profile"] = True
    _rewrite_json(manifest_path, manifest)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    (run_dir / "route.json").write_text(
        json.dumps({"route_id": "lane097", "points": [{"x": 0.0, "y": 0.0}]}) + "\n",
        encoding="utf-8",
    )
    (artifacts_dir / "route_definition_claim.json").write_text(
        json.dumps(
            {
                "schema_version": "route_definition_claim.v1",
                "route_id": "lane097",
                "scenario_route_samples": [{"x": 0.0, "y": 0.0, "lane_key": "15:1"}],
                "scenario_lane_sequence": ["15:1"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    raw_files = [
        "topic_publish_stats.jsonl",
        "publish_gap_trace.jsonl",
        "routing_event_debug.jsonl",
        "planning_topic_debug.jsonl",
        "planning_route_segment_debug.jsonl",
        "control_apply_trace.jsonl",
        "control_decode_debug.jsonl",
    ]
    for name in raw_files:
        (artifacts_dir / name).write_text("{}\n", encoding="utf-8")
    (artifacts_dir / "routing_response_decoded.json").write_text(
        json.dumps({"lane_segments": [{"lane_id": "lane_1", "start_s": 0.0, "end_s": 1.0}]}),
        encoding="utf-8",
    )
    (artifacts_dir / "routing_response_decoded.jsonl").write_text(
        json.dumps({"lane_segments": [{"lane_id": "lane_1", "start_s": 0.0, "end_s": 1.0}]}) + "\n",
        encoding="utf-8",
    )
    (artifacts_dir / "apollo_hdmap_projection.jsonl").write_text("", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["raw_evidence_complete"] is False
    assert "artifacts/apollo_hdmap_projection.jsonl:empty" in report["missing_raw_evidence_artifacts"]
    assert str(artifacts_dir / "apollo_hdmap_projection.jsonl") in report["empty_artifacts"]


def test_claim_profile_with_raw_evidence_can_pass_artifact_check(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["claim_profile"] = True
    _rewrite_json(manifest_path, manifest)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    (run_dir / "route.json").write_text(
        json.dumps({"route_id": "lane097", "points": [{"x": 0.0, "y": 0.0}, {"x": 1.0, "y": 0.0}]}) + "\n",
        encoding="utf-8",
    )
    (artifacts_dir / "route_definition_claim.json").write_text(
        json.dumps(
            {
                "schema_version": "route_definition_claim.v1",
                "route_id": "lane097",
                "scenario_route_samples": [{"x": 0.0, "y": 0.0, "lane_key": "15:1"}],
                "scenario_lane_sequence": ["15:1"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    raw_files = [
        "topic_publish_stats.jsonl",
        "publish_gap_trace.jsonl",
        "routing_event_debug.jsonl",
        "planning_topic_debug.jsonl",
        "planning_route_segment_debug.jsonl",
        "control_apply_trace.jsonl",
        "control_decode_debug.jsonl",
        "apollo_hdmap_projection.jsonl",
    ]
    for name in raw_files:
        (artifacts_dir / name).write_text("{}\n", encoding="utf-8")
    (artifacts_dir / "routing_response_decoded.jsonl").write_text(
        json.dumps(
            {
                "status": "pass",
                "lane_segment_count": 1,
                "lane_segments": [{"lane_id": "lane_1", "start_s": 0.0, "end_s": 1.0}],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts_dir / "routing_response_decoded.json").write_text(
        json.dumps({"lane_segments": [{"lane_id": "lane_1", "start_s": 0.0, "end_s": 1.0}]}),
        encoding="utf-8",
    )

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "pass"
    assert report["artifact_complete"] is True
    assert report["raw_evidence_complete"] is True
    assert report["missing_raw_evidence_artifacts"] == []


def test_claim_profile_stale_routing_response_jsonl_does_not_pass_artifact_check(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["claim_profile"] = True
    _rewrite_json(manifest_path, manifest)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    (run_dir / "route.json").write_text(
        json.dumps({"route_id": "lane097", "points": [{"x": 0.0, "y": 0.0}, {"x": 1.0, "y": 0.0}]}) + "\n",
        encoding="utf-8",
    )
    (artifacts_dir / "route_definition_claim.json").write_text(
        json.dumps(
            {
                "schema_version": "route_definition_claim.v1",
                "route_id": "lane097",
                "scenario_route_samples": [{"x": 0.0, "y": 0.0, "lane_key": "15:1"}],
                "scenario_lane_sequence": ["15:1"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    raw_files = [
        "topic_publish_stats.jsonl",
        "publish_gap_trace.jsonl",
        "routing_event_debug.jsonl",
        "planning_topic_debug.jsonl",
        "planning_route_segment_debug.jsonl",
        "control_apply_trace.jsonl",
        "control_decode_debug.jsonl",
        "apollo_hdmap_projection.jsonl",
    ]
    for name in raw_files:
        (artifacts_dir / name).write_text("{}\n", encoding="utf-8")
    (artifacts_dir / "routing_response_decoded.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "lane_segment_count": 1,
                "lane_segments": [{"lane_id": "lane_1", "start_s": 0.0, "end_s": 1.0}],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts_dir / "routing_response_decoded.jsonl").write_text(
        json.dumps(
            {
                "message_count": 0,
                "source": "/apollo/routing_response",
                "status": "insufficient_data",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["raw_evidence_complete"] is False
    assert "artifacts/routing_response_decoded.jsonl:lane_segments_missing" in report[
        "missing_raw_evidence_artifacts"
    ]


def test_artifact_completeness_report_writer_creates_json_and_markdown(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    report = check_run_artifact_completeness(run_dir)

    outputs = write_run_artifact_completeness_report(report, tmp_path / "out")

    payload = json.loads(Path(outputs["artifact_completeness_report"]).read_text(encoding="utf-8"))
    markdown = Path(outputs["artifact_completeness_summary"]).read_text(encoding="utf-8")
    assert payload["schema_version"] == RUN_ARTIFACT_COMPLETENESS_SCHEMA_VERSION
    assert payload["status"] == "pass"
    assert "# Run Artifact Completeness" in markdown
    assert "does not prove Apollo behavior correctness" in markdown


def test_artifact_completeness_cli_writes_run_local_report(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)

    result = subprocess.run(
        [
            "python3",
            "tools/analyze_run_artifact_completeness.py",
            "--run-dir",
            str(run_dir),
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert payload["artifact_complete"] is True
    assert (
        run_dir
        / "analysis"
        / "artifact_completeness"
        / "artifact_completeness_report.json"
    ).is_file()


def test_phase1_profile_passes_minimal_route_only_run(tmp_path: Path) -> None:
    run_dir = _write_minimal_phase1_run(tmp_path)

    report = check_run_artifact_completeness(run_dir, profile="phase1")

    assert report["profile"] == "phase1"
    assert report["status"] == "pass"
    assert report["artifact_complete"] is True
    assert report["target_actor_required"] is False
    assert report["missing_artifacts"] == []
    assert report["missing_manifest_fields"] == []
    assert "phase1_profile_checks_scenario_run_surface_not_natural_driving_claim_artifacts" in report[
        "warnings"
    ]


def test_phase1_profile_requires_target_actor_artifacts_when_target_required(
    tmp_path: Path,
) -> None:
    run_dir = _write_minimal_phase1_run(
        tmp_path,
        scenario_case="follow_stop_static",
        backend_type="apollo_reference_backend",
        target_required=True,
    )

    report = check_run_artifact_completeness(run_dir, profile="phase1")

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert report["target_actor_required"] is True
    assert "artifacts/scenario_actor_trace.jsonl" in report["missing_artifacts"]
    assert "analysis/scenario_actor_contract/scenario_actor_contract_report.json" in report[
        "missing_artifacts"
    ]
    assert "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json" in report[
        "missing_artifacts"
    ]


def test_artifact_completeness_cli_supports_phase1_profile(tmp_path: Path) -> None:
    run_dir = _write_minimal_phase1_run(tmp_path)

    result = subprocess.run(
        [
            "python3",
            "tools/analyze_run_artifact_completeness.py",
            "--profile",
            "phase1",
            "--run-dir",
            str(run_dir),
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert payload["artifact_complete"] is True
    written = run_dir / "analysis" / "artifact_completeness" / "artifact_completeness_report.json"
    assert written.is_file()
    assert json.loads(written.read_text(encoding="utf-8"))["profile"] == "phase1"


def test_missing_required_artifacts_are_reported(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    (run_dir / "config.resolved.yaml").unlink()
    (run_dir / "analysis" / "route_health" / "route_health.csv").unlink()
    (run_dir / "analysis" / "route_health" / "curve_segments.csv").unlink()
    (run_dir / "analysis" / "route_health" / "route_health_summary.md").unlink()
    (run_dir / "analysis" / "control_health" / "control_health_report.json").unlink()
    (run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json").unlink()
    (run_dir / "analysis" / "route_start_alignment" / "route_start_alignment_report.json").unlink()

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "config.resolved.yaml" in report["missing_artifacts"]
    assert "route_health.csv" in report["missing_artifacts"]
    assert "curve_segments.csv" in report["missing_artifacts"]
    assert "route_health_summary.md" in report["missing_artifacts"]
    assert "control_health_report.json" in report["missing_artifacts"]
    assert "failure_timeline_report.json" in report["missing_artifacts"]
    assert "route_start_alignment_report.json" in report["missing_artifacts"]


def test_missing_manifest_reproducibility_fields_are_reported(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("algorithm_variant_id")
    manifest.pop("algorithm_variant_manifest_path")
    manifest.pop("online_config_path")
    manifest.pop("online_config_profile_name")
    manifest.pop("transport_mode_source")
    manifest.pop("truth_input")
    manifest.pop("duration_s")
    manifest.pop("fixed_delta_seconds")
    manifest.pop("ticks")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert {
        "algorithm_variant_id",
        "algorithm_variant_manifest_path",
        "online_config_path",
        "online_config_profile_name",
        "transport_mode_source",
        "truth_input",
        "duration_s",
        "fixed_delta_seconds",
        "ticks",
    }.issubset(set(report["missing_manifest_fields"]))


def test_missing_carla_world_identity_is_reported(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("carla_world")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert "carla_world.loaded_map_name" in report["missing_manifest_fields"]
    assert "carla_world.matches_configured_town" in report["missing_manifest_fields"]


def test_carla_world_mismatch_is_reported(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["carla_world"]["loaded_map_name"] = "straight_road_for_baguang"
    manifest["carla_world"]["matches_configured_town"] = False
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert "carla_world.loaded_map_name" not in report["missing_manifest_fields"]
    assert "carla_world.matches_configured_town" in report["missing_manifest_fields"]


def test_online_config_path_must_exist(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["online_config_path"] = "missing_online_config.yaml"
    _rewrite_json(manifest_path, manifest)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "online_config_path" in report["invalid_manifest_source_fields"]


def test_algorithm_variant_manifest_path_must_exist(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["algorithm_variant_manifest_path"] = "missing_algorithm_variant.yaml"
    _rewrite_json(manifest_path, manifest)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "algorithm_variant_manifest_path" in report["invalid_manifest_source_fields"]


def test_algorithm_variant_manifest_id_must_match_run_manifest(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["algorithm_variant_id"] = "apollo_upstream_10_0_reference"
    _rewrite_json(manifest_path, manifest)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "algorithm_variant_manifest_path.variant_id_mismatch" in report["invalid_manifest_source_fields"]


def test_truth_input_closed_loop_rejects_upstream_algorithm_variant(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["algorithm_variant_id"] = "apollo_upstream_10_0_reference"
    manifest["algorithm_variant_manifest_path"] = "configs/algorithms/apollo_variant.upstream.example.yaml"
    _rewrite_json(manifest_path, manifest)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert (
        "algorithm_variant_manifest_path.variant_type_not_truth_input_closed_loop"
        in report["invalid_manifest_source_fields"]
    )


def test_traffic_light_contract_is_required_for_traffic_light_runs(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    (run_dir / "traffic_light_contract_report.json").unlink()

    report = check_run_artifact_completeness(run_dir)

    assert report["scenario_class"] == "traffic_light_red_stop"
    assert report["status"] == "insufficient_data"
    assert "traffic_light_contract_report.json" in report["missing_artifacts"]


def test_traffic_light_behavior_is_required_for_traffic_light_runs(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    (run_dir / "traffic_light_behavior_report.json").unlink()

    report = check_run_artifact_completeness(run_dir)

    assert report["scenario_class"] == "traffic_light_red_stop"
    assert report["status"] == "insufficient_data"
    assert "traffic_light_behavior_report.json" in report["missing_artifacts"]


def test_traffic_light_expectation_is_required_for_traffic_light_runs(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("traffic_light_expectation")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["scenario_class"] == "traffic_light_red_stop"
    assert report["status"] == "insufficient_data"
    assert "traffic_light_expectation.expected_behavior" in report["missing_manifest_fields"]


def test_traffic_light_expectation_must_match_scenario_class(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["traffic_light_expectation"]["expected_behavior"] = "green_go"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert "traffic_light_expectation.expected_behavior" in report["missing_manifest_fields"]


def test_claim_grade_traffic_light_control_metadata_is_required(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("traffic_light_control")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "traffic_light_control" in report["missing_manifest_fields"]


def test_red_to_green_release_metadata_is_required_for_claim_grade(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["scenario_class"] = "traffic_light_red_to_green_release"
    manifest["traffic_light_expectation"] = {
        "expected_behavior": "red_to_green_release",
        "expected_initial_state": "RED",
        "expected_release_state": "GREEN",
        "stimulus_mode": "deterministic_gt_control",
        "claim_grade": True,
    }
    manifest["traffic_light_control"] = {
        "mode": "deterministic_gt_control",
        "stimulus_mode": "deterministic_gt_control",
        "initial_state": "RED",
        "release_state": "GREEN",
        "initial_affected_count": 1,
        "events": [{"phase": "initial", "state": "RED", "affected_count": 1}],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir, scenario_class="traffic_light_red_to_green_release")

    assert report["status"] == "insufficient_data"
    assert "traffic_light_control.release_frame_id" in report["missing_manifest_fields"]

    manifest["traffic_light_control"]["events"].append(
        {"phase": "release", "state": "GREEN", "affected_count": 1, "frame_id": 42}
    )
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir, scenario_class="traffic_light_red_to_green_release")

    assert "traffic_light_control.release_frame_id" not in report["missing_manifest_fields"]


def test_route_curve_artifact_gap_is_required_for_curve_diagnostic_runs(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["scenario_class"] = "curve_diagnostic"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir, scenario_class="curve_diagnostic")

    assert report["status"] == "insufficient_data"
    assert "route_curve_artifact_gap_report.json" in report["missing_artifacts"]

    gap_dir = run_dir / "analysis" / "route_curve_artifact_gap"
    gap_dir.mkdir(parents=True)
    (gap_dir / "route_curve_artifact_gap_report.json").write_text(
        json.dumps(
            {
                "schema_version": "route_curve_artifact_gap.v1",
                "status": "pass",
                "source": {
                    "timeseries_csv": "timeseries.csv",
                    "summary_json": "summary.json",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = check_run_artifact_completeness(run_dir, scenario_class="curve_diagnostic")

    assert "route_curve_artifact_gap_report.json" not in report["missing_artifacts"]


def test_route_curve_artifact_gap_source_paths_are_required_for_curve_diagnostic_runs(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["scenario_class"] = "curve_diagnostic"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    gap_dir = run_dir / "analysis" / "route_curve_artifact_gap"
    gap_dir.mkdir(parents=True)
    (gap_dir / "route_curve_artifact_gap_report.json").write_text(
        json.dumps(
            {
                "schema_version": "route_curve_artifact_gap.v1",
                "status": "pass",
                "source": {
                    "timeseries_csv": "missing_timeseries.csv",
                    "summary_json": "summary.json",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = check_run_artifact_completeness(run_dir, scenario_class="curve_diagnostic")

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "route_curve_artifact_gap.source.timeseries_csv" in report["invalid_report_source_fields"]


def test_missing_control_trace_fields_are_reported(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    timeseries_path = run_dir / "timeseries.csv"
    lines = timeseries_path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    keep_indexes = [
        index
        for index, name in enumerate(header)
        if name not in {"apollo_steer_raw", "bridge_steer_mapped", "carla_steer_applied"}
    ]
    rewritten = [
        ",".join(header[index] for index in keep_indexes),
        *(",".join(row.split(",")[index] for index in keep_indexes) for row in lines[1:]),
    ]
    timeseries_path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["control_trace_available"] is False
    assert {"apollo_steer_raw", "bridge_steer_mapped", "carla_steer_applied"}.issubset(
        set(report["missing_control_trace_fields"])
    )


def test_nullable_guard_trace_columns_are_not_treated_as_missing_fields(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    timeseries_path = run_dir / "timeseries.csv"
    lines = timeseries_path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    guard_indexes = {
        index
        for index, name in enumerate(header)
        if name in {"lateral_guard_applied", "trajectory_contract_guard_applied"}
    }
    rewritten = [lines[0]]
    for raw_row in lines[1:]:
        values = raw_row.split(",")
        for index in guard_indexes:
            values[index] = ""
        rewritten.append(",".join(values))
    timeseries_path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "pass"
    assert report["control_trace_available"] is True
    assert report["missing_control_trace_fields"] == []


def test_jsonl_timeseries_control_trace_is_supported(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    csv_path = run_dir / "timeseries.csv"
    row = {
        "apollo_steer_raw": 0.1,
        "bridge_steer_mapped": 0.025,
        "carla_steer_applied": 0.025,
        "throttle_raw": 0.2,
        "throttle_mapped": 0.2,
        "throttle_applied": 0.2,
        "brake_raw": 0.0,
        "brake_mapped": 0.0,
        "brake_applied": 0.0,
        "lateral_guard_applied": False,
        "trajectory_contract_guard_applied": False,
    }
    (run_dir / "timeseries.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    csv_path.unlink()
    for report_path in (
        run_dir / "analysis" / "route_health" / "route_health.json",
        run_dir / "analysis" / "control_health" / "control_health_report.json",
        run_dir / "analysis" / "localization_contract" / "localization_contract_report.json",
        run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json",
        run_dir / "analysis" / "route_start_alignment" / "route_start_alignment_report.json",
    ):
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        payload["source"]["timeseries_path"] = "timeseries.jsonl"
        _rewrite_json(report_path, payload)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "pass"
    assert report["artifacts"]["timeseries"].endswith("timeseries.jsonl")


def test_route_health_source_paths_are_required_for_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    route_health_path = run_dir / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["source"]["timeseries_path"] = "missing_timeseries.csv"
    _rewrite_json(route_health_path, route_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "route_health.source.timeseries_path" in report["invalid_report_source_fields"]


def test_route_health_manifest_source_path_is_required_for_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    route_health_path = run_dir / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["source"]["manifest_path"] = "missing_manifest.json"
    _rewrite_json(route_health_path, route_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "route_health.source.manifest_path" in report["invalid_report_source_fields"]


def test_route_health_route_id_must_match_run_for_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    route_health_path = run_dir / "analysis" / "route_health" / "route_health.json"
    route_health = json.loads(route_health_path.read_text(encoding="utf-8"))
    route_health["route_id"] = "other_route"
    _rewrite_json(route_health_path, route_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "route_health.route_id" in report["invalid_report_source_fields"]


def test_channel_health_source_paths_are_required_for_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    channel_health_path = run_dir / "apollo_channel_health_report.json"
    channel_health = json.loads(channel_health_path.read_text(encoding="utf-8"))
    channel_health["source"]["stats_path"] = "missing_channel_stats.json"
    _rewrite_json(channel_health_path, channel_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "apollo_channel_health.source.stats_path" in report["invalid_report_source_fields"]


def test_channel_health_scenario_context_must_match_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    channel_health_path = run_dir / "apollo_channel_health_report.json"
    channel_health = json.loads(channel_health_path.read_text(encoding="utf-8"))
    channel_health["scenario_class"] = "traffic_light_red_stop"
    _rewrite_json(channel_health_path, channel_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "apollo_channel_health.scenario_class" in report["invalid_report_source_fields"]


def test_control_health_source_paths_are_required_for_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    control_health_path = run_dir / "analysis" / "control_health" / "control_health_report.json"
    control_health = json.loads(control_health_path.read_text(encoding="utf-8"))
    control_health["source"]["timeseries_path"] = "missing_timeseries.csv"
    _rewrite_json(control_health_path, control_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "control_health.source.timeseries_path" in report["invalid_report_source_fields"]


def test_control_health_manifest_source_path_is_required_for_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path)
    control_health_path = run_dir / "analysis" / "control_health" / "control_health_report.json"
    control_health = json.loads(control_health_path.read_text(encoding="utf-8"))
    control_health["source"]["manifest_path"] = "missing_manifest.json"
    _rewrite_json(control_health_path, control_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "control_health.source.manifest_path" in report["invalid_report_source_fields"]


def test_control_health_context_must_match_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    control_health_path = run_dir / "analysis" / "control_health" / "control_health_report.json"
    control_health = json.loads(control_health_path.read_text(encoding="utf-8"))
    control_health["scenario_class"] = "traffic_light_red_stop"
    control_health["route_id"] = "traffic_light_red_stop_tbd"
    _rewrite_json(control_health_path, control_health)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "control_health.scenario_class" in report["invalid_report_source_fields"]
    assert "control_health.route_id" in report["invalid_report_source_fields"]


def test_failure_timeline_context_must_match_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    failure_path = run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    failure["scenario_class"] = "traffic_light_red_stop"
    failure["route_id"] = "traffic_light_red_stop_tbd"
    _rewrite_json(failure_path, failure)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "failure_timeline.scenario_class" in report["invalid_report_source_fields"]
    assert "failure_timeline.route_id" in report["invalid_report_source_fields"]


def test_failure_timeline_source_paths_are_required_for_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    failure_path = run_dir / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    failure["source"]["control_health_path"] = "missing_control_health_report.json"
    _rewrite_json(failure_path, failure)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "failure_timeline.source.control_health_path" in report["invalid_report_source_fields"]


def test_route_start_alignment_context_must_match_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    alignment_path = run_dir / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    alignment = json.loads(alignment_path.read_text(encoding="utf-8"))
    alignment["scenario_class"] = "traffic_light_red_stop"
    alignment["route_id"] = "traffic_light_red_stop_tbd"
    _rewrite_json(alignment_path, alignment)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "route_start_alignment.scenario_class" in report["invalid_report_source_fields"]
    assert "route_start_alignment.route_id" in report["invalid_report_source_fields"]


def test_route_start_alignment_source_paths_are_required_for_artifact_completeness(tmp_path: Path) -> None:
    run_dir = _copy_run(tmp_path)
    alignment_path = run_dir / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    alignment = json.loads(alignment_path.read_text(encoding="utf-8"))
    alignment["source"]["failure_timeline_path"] = "missing_failure_timeline_report.json"
    _rewrite_json(alignment_path, alignment)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "route_start_alignment.source.failure_timeline_path" in report["invalid_report_source_fields"]


def test_traffic_light_behavior_source_paths_are_required_for_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    behavior_path = run_dir / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["source"]["events_path"] = "missing_events.jsonl"
    _rewrite_json(behavior_path, behavior)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "traffic_light_behavior.source.events_path" in report["invalid_report_source_fields"]


def test_traffic_light_behavior_manifest_source_path_is_required_for_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    behavior_path = run_dir / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["source"]["manifest_path"] = "missing_manifest.json"
    _rewrite_json(behavior_path, behavior)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "traffic_light_behavior.source.manifest_path" in report["invalid_report_source_fields"]


def test_traffic_light_behavior_context_must_match_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    behavior_path = run_dir / "traffic_light_behavior_report.json"
    behavior = json.loads(behavior_path.read_text(encoding="utf-8"))
    behavior["scenario_class"] = "traffic_light_green_go"
    behavior["route_id"] = "traffic_light_green_go_tbd"
    _rewrite_json(behavior_path, behavior)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "traffic_light_behavior.scenario_class" in report["invalid_report_source_fields"]
    assert "traffic_light_behavior.route_id" in report["invalid_report_source_fields"]


def test_traffic_light_contract_source_paths_are_required_for_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    contract_path = run_dir / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["source"]["town01_contract_path"] = "missing_town01_contract.yaml"
    _rewrite_json(contract_path, contract)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "traffic_light_contract.source.town01_contract_path" in report["invalid_report_source_fields"]


def test_traffic_light_contract_scenario_context_must_match_artifact_completeness(
    tmp_path: Path,
) -> None:
    run_dir = _copy_run(tmp_path, "traffic_light_red_stop")
    contract_path = run_dir / "traffic_light_contract_report.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["scenario_class"] = "traffic_light_green_go"
    _rewrite_json(contract_path, contract)

    report = check_run_artifact_completeness(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["artifact_complete"] is False
    assert "traffic_light_contract.scenario_class" in report["invalid_report_source_fields"]
