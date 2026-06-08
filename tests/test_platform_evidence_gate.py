from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml

from carla_testbed.evidence.bundle import build_evidence_bundle
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
    for rel in [
        "artifacts/topic_publish_stats.jsonl",
        "artifacts/publish_gap_trace.jsonl",
        "artifacts/control_apply_trace.jsonl",
        "artifacts/planning_topic_debug.jsonl",
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
    assert "package_manifest.json" in payload["included_files"]
