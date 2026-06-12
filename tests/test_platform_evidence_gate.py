from __future__ import annotations

import json
import subprocess
import sys
import tarfile
from pathlib import Path

import yaml

from carla_testbed.evidence.bundle import build_and_write_evidence_bundle, build_evidence_bundle
from carla_testbed.evidence.gate_runner import run_gate
from carla_testbed.platform.compiler import compile_run_plan, write_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def _write_fake_run(run_dir: Path) -> None:
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "fake_run",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "route_id": "route097",
                "backend": "apollo_cyberrt",
            }
        ),
        encoding="utf-8",
    )


def _write_claim_required_summary_files(run_dir: Path) -> None:
    (run_dir / "timeseries.csv").write_text("sim_time,route_s\n0.0,0.0\n", encoding="utf-8")
    for rel, payload in {
        "analysis/natural_driving/natural_driving_report.json": {"verdict": {"status": "fail"}},
        "analysis/assist_ledger/assist_ledger.json": {
            "schema_version": "assist_ledger.v1",
            "active_assists": [],
        },
        "analysis/artifact_completeness/artifact_completeness_report.json": {
            "schema_version": "run_artifact_completeness.v1",
            "status": "insufficient_data",
        },
    }.items():
        path = run_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "fake_run",
                "scenario_id": "town01_lane_keep_097",
                "scenario_class": "lane_keep",
                "route_id": "route097",
                "backend": "apollo_cyberrt",
            }
        ),
        encoding="utf-8",
    )


def test_evidence_bundle_indexes_missing_required_reports(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )

    bundle = build_evidence_bundle(run_dir, plan=plan)
    gate = run_gate(run_dir, plan=plan)

    assert bundle["status"] == "insufficient_data"
    assert "route_health" in bundle["missing_required_evidence"]
    assert gate["status"] == "fail"
    assert gate["can_claim_unassisted_natural_driving"] is False


def test_runtime_claim_boundary_requires_typed_config_and_no_legacy_fallback(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["typed_config_loaded"] = True
    manifest["legacy_fallback_used"] = False
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    bundle = build_evidence_bundle(run_dir)

    assert bundle["artifacts"]["runtime_claim_boundary"]["status"] == "pass"


def test_runtime_claim_boundary_blocks_legacy_fallback(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["typed_config_loaded"] = False
    manifest["legacy_fallback_used"] = True
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    bundle = build_evidence_bundle(run_dir)
    boundary = bundle["artifacts"]["runtime_claim_boundary"]

    assert boundary["status"] == "fail"
    assert "typed_config_not_loaded" in boundary["summary"]["blocking_reasons"]
    assert "legacy_fallback_used" in boundary["summary"]["blocking_reasons"]


def test_runtime_claim_boundary_missing_fields_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)

    bundle = build_evidence_bundle(run_dir)
    boundary = bundle["artifacts"]["runtime_claim_boundary"]

    assert boundary["status"] == "insufficient_data"
    assert set(boundary["summary"]["missing_fields"]) == {"typed_config_loaded", "legacy_fallback_used"}



def _write_claim_grade_synthetic_run(run_dir: Path) -> None:
    run_dir.mkdir(parents=True)
    scenario_class = "lane_keep"
    route_id = "lane097"
    run_id = "synthetic_claim_pass"
    manifest = {
        "run_id": run_id,
        "scenario_id": "lane_keep_097",
        "scenario_class": scenario_class,
        "route_id": route_id,
        "backend": "apollo_cyberrt",
        "typed_config_loaded": True,
        "legacy_fallback_used": False,
        "algorithm_variant_id": "apollo_10_0_carla_gt_town01_reference",
        "algorithm_variant_manifest_path": "configs/algorithms/apollo_variant.carla_gt.example.yaml",
        "online_config_path": "configs/examples/smoke.yaml",
        "online_config_profile_name": "synthetic_claim",
        "map": "Town01",
        "transport_mode": "apollo_cyberrt_gt",
        "transport_mode_source": "synthetic_test",
        "truth_input": True,
        "duration_s": 70.0,
        "fixed_delta_seconds": 0.05,
        "ticks": 1400,
        "carla_world": {"loaded_map_name": "Town01", "matches_configured_town": True},
    }
    summary = {
        "run_id": run_id,
        "scenario_id": "lane_keep_097",
        "scenario_class": scenario_class,
        "route_id": route_id,
        "backend": "apollo_cyberrt",
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (run_dir / "config.resolved.yaml").write_text("run_id: synthetic_claim_pass\n", encoding="utf-8")
    (run_dir / "events.jsonl").write_text('{"event_type":"start"}\n', encoding="utf-8")
    (run_dir / "route.json").write_text(json.dumps({"route_id": route_id}), encoding="utf-8")
    fields = [
        "sim_time",
        "route_s",
        "apollo_steer_raw",
        "bridge_steer_mapped",
        "carla_steer_applied",
        "throttle_raw",
        "throttle_mapped",
        "throttle_applied",
        "brake_raw",
        "brake_mapped",
        "brake_applied",
        "lateral_guard_applied",
        "trajectory_contract_guard_applied",
    ]
    (run_dir / "timeseries.csv").write_text(
        ",".join(fields) + "\n" + ",".join("0" for _ in fields) + "\n",
        encoding="utf-8",
    )
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(exist_ok=True)
    for name in [
        "topic_publish_stats.jsonl",
        "publish_gap_trace.jsonl",
        "routing_event_debug.jsonl",
        "planning_topic_debug.jsonl",
        "planning_route_segment_debug.jsonl",
        "control_apply_trace.jsonl",
        "control_decode_debug.jsonl",
        "apollo_hdmap_projection.jsonl",
    ]:
        (artifacts / name).write_text('{"ok":true}\n', encoding="utf-8")
    (artifacts / "routing_response_decoded.json").write_text(
        json.dumps({"lane_segments": [{"lane_id": "lane_1", "start_s": 0.0, "end_s": 1.0}]}),
        encoding="utf-8",
    )

    def write_report(rel: str, payload: dict) -> None:
        path = run_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    common_context = {"scenario_class": scenario_class, "route_id": route_id}
    write_report(
        "analysis/route_health/route_health.json",
        {
            "schema_version": "route_health.v1",
            "status": "pass",
            "route_id": route_id,
            "route_source": "configured_route_file",
            "evidence_level": "claim_grade_manifest_route",
            "hard_gate_eligible": True,
            "source": {
                "manifest_path": "manifest.json",
                "summary_path": "summary.json",
                "timeseries_path": "timeseries.csv",
                "route_path": "route.json",
            },
        },
    )
    (run_dir / "analysis/route_health/route_health.csv").write_text(
        f"route_id,status\n{route_id},pass\n",
        encoding="utf-8",
    )
    (run_dir / "analysis/route_health/curve_segments.csv").write_text("segment_id\n", encoding="utf-8")
    (run_dir / "analysis/route_health/route_health_summary.md").write_text("ok\n", encoding="utf-8")
    write_report(
        "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "channel.v1",
            "status": "pass",
            **common_context,
            "source": {
                "config_path": "config.resolved.yaml",
                "stats_path": "artifacts/topic_publish_stats.jsonl",
            },
        },
    )
    write_report(
        "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "loc.v1",
            "status": {"measurement_time_available": True},
            "claim_grade": True,
            "verdict": {"status": "pass"},
            "source": {
                "timeseries_path": "timeseries.csv",
                "channel_stats_path": "artifacts/topic_publish_stats.jsonl",
                "route_health_path": "analysis/route_health/route_health.json",
            },
        },
    )
    write_report(
        "analysis/chassis_gt_contract/chassis_gt_contract_report.json",
        {"schema_version": "chassis.v1", "status": "pass", "claim_grade": True},
    )
    write_report(
        "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
        {"schema_version": "hd.v1", "status": "pass", "claim_grade": True},
    )
    write_report(
        "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {"schema_version": "route_contract.v1", "status": "pass"},
    )
    write_report(
        "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {"schema_version": "refline.v1", "status": "pass", **common_context, "source": {"run_dir": "."}},
    )
    write_report(
        "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "pm.v1",
            "status": "pass",
            "metrics": {"nonempty_trajectory_ratio": 0.95},
            "route_establishment": {"route_established": True},
        },
    )
    write_report(
        "analysis/apollo_module_consumption/apollo_module_consumption_report.json",
        {"schema_version": "amc.v1", "status": "pass"},
    )
    write_report(
        "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
        {"schema_version": "handoff.v1", "status": "pass", "verdict": "pass"},
    )
    write_report(
        "analysis/control_health/control_health_report.json",
        {
            "schema_version": "ctrl.v1",
            "status": "pass",
            **common_context,
            "source": {
                "summary_path": "summary.json",
                "manifest_path": "manifest.json",
                "timeseries_path": "timeseries.csv",
            },
        },
    )
    write_report(
        "analysis/control_attribution/control_attribution_report.json",
        {"schema_version": "attr.v1", "status": "pass", "applied_control_source": "apollo_control"},
    )
    write_report(
        "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "pred.v1",
            "status": "pass",
            "verdict": "pass",
            "prediction_mode": "native_observed",
        },
    )
    write_report(
        "analysis/assist_ledger/assist_ledger.json",
        {"schema_version": "assist.v1", "status": "pass", "blocking_assists": []},
    )
    write_report(
        "analysis/apollo_link_health/apollo_link_health_report.json",
        {"schema_version": "link.v1", "status": "pass"},
    )
    write_report(
        "analysis/natural_driving/natural_driving_report.json",
        {"schema_version": "natural.v1", "status": "pass"},
    )
    write_report(
        "analysis/failure_timeline/failure_timeline_report.json",
        {
            "schema_version": "failure.v1",
            "status": "pass",
            **common_context,
            "source": {
                "manifest_path": "manifest.json",
                "summary_path": "summary.json",
                "events_path": "events.jsonl",
                "timeseries_path": "timeseries.csv",
                "route_health_path": "analysis/route_health/route_health.json",
                "control_health_path": "analysis/control_health/control_health_report.json",
            },
        },
    )
    write_report(
        "analysis/route_start_alignment/route_start_alignment_report.json",
        {
            "schema_version": "start.v1",
            "status": "pass",
            **common_context,
            "source": {
                "manifest_path": "manifest.json",
                "summary_path": "summary.json",
                "timeseries_path": "timeseries.csv",
                "failure_timeline_path": "analysis/failure_timeline/failure_timeline_report.json",
            },
        },
    )


def test_claim_gate_can_pass_synthetic_claim_grade_reports(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_claim_grade_synthetic_run(run_dir)
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="claim",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )

    gate = run_gate(run_dir, plan=plan)

    assert gate["status"] == "pass"
    assert gate["can_claim_unassisted_natural_driving"] is True
    assert all(rule["status"] == "pass" for rule in gate["rules"])


def test_runtime_claim_boundary_rule_uses_synthesized_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_claim_grade_synthetic_run(run_dir)
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="claim",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )

    gate = run_gate(run_dir, plan=plan)
    runtime_rule = next(rule for rule in gate["rules"] if rule["id"] == "runtime_claim_boundary_pass")

    assert runtime_rule["status"] == "pass"
    assert runtime_rule["actual"] == "pass"


def test_runtime_claim_boundary_report_is_materialized_with_evidence_bundle(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_claim_grade_synthetic_run(run_dir)
    out_dir = tmp_path / "analysis" / "evidence_bundle"

    outputs = build_and_write_evidence_bundle(run_dir, out_dir=out_dir)

    report_path = Path(outputs["runtime_claim_boundary_report"])
    bundle_path = Path(outputs["evidence_bundle"])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    boundary = bundle["artifacts"]["runtime_claim_boundary"]

    assert report_path.is_file()
    assert report["schema_version"] == "runtime_claim_boundary.v1"
    assert report["status"] == "pass"
    assert report["claim_grade"] is True
    assert report["typed_config_loaded"] is True
    assert report["legacy_fallback_used"] is False
    assert boundary["path"] == str(report_path)

def test_gate_rule_fails_even_when_report_status_is_pass(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    report_dir = run_dir / "analysis" / "planning_materialization"
    report_dir.mkdir(parents=True)
    (report_dir / "planning_materialization_report.json").write_text(
        json.dumps(
            {
                "schema_version": "planning_materialization.v1",
                "status": "pass",
                "metrics": {"nonempty_trajectory_ratio": 0.02},
                "route_establishment": {"route_established": True},
            }
        ),
        encoding="utf-8",
    )
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="claim",
        gate="claim_natural_driving",
        registry=PlatformRegistry(repo_root="."),
    )

    gate = run_gate(run_dir, plan=plan)

    assert gate["status"] == "fail"
    planning_rule = next(rule for rule in gate["rules"] if rule["id"] == "planning_nonempty_ratio")
    assert planning_rule["status"] == "fail"
    assert planning_rule["actual"] == 0.02


def test_cli_analyze_writes_bundle_and_gate(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )
    plan_path = write_run_plan(plan, tmp_path / "plan.yaml")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "analyze",
            "--run-dir",
            str(run_dir),
            "--plan",
            str(plan_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert Path(payload["evidence_bundle"]["evidence_bundle"]).exists()
    assert Path(payload["gate"]["gate_report"]).exists()


def test_cli_pack_includes_analysis_but_skips_large_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    analysis = run_dir / "analysis" / "gate"
    analysis.mkdir(parents=True)
    (analysis / "gate_report.json").write_text(json.dumps({"status": "insufficient_data"}), encoding="utf-8")
    video_dir = run_dir / "video" / "dual_cam"
    video_dir.mkdir(parents=True)
    (video_dir / "demo_third_person.mp4").write_bytes(b"fake")
    out = tmp_path / "review.tar.gz"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "pack",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert out.exists()
    assert "analysis/gate/gate_report.json" in payload["included_files"]
    assert all("video/" not in item for item in payload["included_files"])
    assert payload["status"] == "insufficient_data"
    assert "artifacts/topic_publish_stats.jsonl" in payload["missing_required_files"]


def test_claim_pack_includes_row_level_evidence_when_present(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    _write_claim_required_summary_files(run_dir)
    for rel in [
        "artifacts/topic_publish_stats.jsonl",
        "artifacts/publish_gap_trace.jsonl",
        "artifacts/control_apply_trace.jsonl",
        "artifacts/control_decode_debug.jsonl",
        "artifacts/planning_topic_debug.jsonl",
        "artifacts/routing_event_debug.jsonl",
        "artifacts/planning_route_segment_debug.jsonl",
    ]:
        path = run_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"ok": true}\n', encoding="utf-8")
    out = tmp_path / "claim.tar.gz"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "pack",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
            "--profile",
            "claim",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "pass"
    assert payload["claim_reproducibility_level"] == "row_level_evidence_present"
    assert "artifacts/topic_publish_stats.jsonl" in payload["included_files"]
    assert "artifacts/routing_event_debug.jsonl" in payload["included_files"]
    assert "artifacts/planning_route_segment_debug.jsonl" in payload["included_files"]
    assert "package_manifest.json" in payload["included_files"]
    assert "row_level_evidence_index.json" in payload["included_files"]
    assert (
        "row_level_samples/artifacts/topic_publish_stats.jsonl.head.jsonl"
        in payload["included_files"]
    )
    with tarfile.open(out, "r:gz") as archive:
        index_member = archive.extractfile("run/row_level_evidence_index.json")
        assert index_member is not None
        index = json.loads(index_member.read().decode("utf-8"))
        topic_entry = next(
            item
            for item in index["files"]
            if item["path"] == "artifacts/topic_publish_stats.jsonl"
        )
        routing_entry = next(
            item
            for item in index["files"]
            if item["path"] == "artifacts/routing_event_debug.jsonl"
        )
        segment_entry = next(
            item
            for item in index["files"]
            if item["path"] == "artifacts/planning_route_segment_debug.jsonl"
        )
        decode_entry = next(
            item
            for item in index["files"]
            if item["path"] == "artifacts/control_decode_debug.jsonl"
        )
        assert topic_entry["row_count"] == 1
        assert routing_entry["row_count"] == 1
        assert segment_entry["row_count"] == 1
        assert decode_entry["row_count"] == 1
        assert topic_entry["sha256"]
        assert topic_entry["head_sample"] == (
            "row_level_samples/artifacts/topic_publish_stats.jsonl.head.jsonl"
        )
        sample = archive.extractfile(f"run/{topic_entry['head_sample']}")
        assert sample is not None
        assert json.loads(sample.read().decode("utf-8")) == {"ok": True}


def test_transition_claim_pack_declares_source_context_requirements(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest.update(
        {
            "runtime_dispatch_kind": "typed_apollo_claim_runtime",
            "transport_mode": "apollo_cyberrt_gt_over_ros2_transition",
            "compat_layers": ["ros2_gt_transition", "legacy_route_health_transition"],
        }
    )
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    out = tmp_path / "claim.tar.gz"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "pack",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
            "--profile",
            "claim",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    with tarfile.open(out, "r:gz") as archive:
        manifest_payload = json.loads(
            archive.extractfile("run/package_manifest.json").read().decode("utf-8")
        )

    assert "examples/" in manifest_payload["source_context_requirements"]
    assert "configs/io/" in manifest_payload["source_context_requirements"]


def test_claim_pack_marks_fixed_scene_summary_only_when_row_level_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_fake_run(run_dir)
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "fixed_scene_resolved.json").write_text(json.dumps({"scene_id": "fixed_scene"}), encoding="utf-8")
    for rel in [
        "artifacts/topic_publish_stats.jsonl",
        "artifacts/publish_gap_trace.jsonl",
        "artifacts/control_apply_trace.jsonl",
        "artifacts/planning_topic_debug.jsonl",
    ]:
        path = run_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"ok": true}\n', encoding="utf-8")
    out = tmp_path / "fixed_scene_claim.tar.gz"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "pack",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
            "--profile",
            "claim",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "insufficient_data"
    assert payload["claim_reproducibility_level"] == "summary_only_missing_fixed_scene_row_level"
    assert "artifacts/fixed_scene_runtime_state.json" in payload["missing_required_files"]
    assert "artifacts/scenario_actor_trace.jsonl" in payload["missing_required_files"]
    assert "artifacts/scenario_phase_events.jsonl" in payload["missing_required_files"]
