from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.autoware_evidence import (
    AUTOWARE_EVIDENCE_SCHEMA_VERSION,
    analyze_autoware_evidence_run_dir,
    write_autoware_evidence_report,
)
from carla_testbed.analysis.natural_driving import analyze_natural_driving_suite

DEMO_FIXTURE = Path("tests/fixtures/autoware/demo_recording")
NATURAL_LANE_FIXTURE = Path("tests/fixtures/natural_driving/simple_suite/lane_keep_097")
NATURAL_TL_FIXTURE = Path("tests/fixtures/natural_driving/simple_suite/traffic_light_red_stop")


def _add_recording_artifacts(run_dir: Path) -> None:
    (run_dir / "video" / "rviz").mkdir(parents=True, exist_ok=True)
    (run_dir / "video" / "rviz" / "autoware_rviz.mp4").write_text("rviz mp4\n", encoding="utf-8")
    (run_dir / "video" / "dual_cam").mkdir(parents=True, exist_ok=True)
    (run_dir / "video" / "dual_cam" / "demo_third_person.mp4").write_text("carla mp4\n", encoding="utf-8")
    (run_dir / "rosbag2" / "autoware_demo").mkdir(parents=True, exist_ok=True)
    (run_dir / "rosbag2" / "autoware_demo" / "metadata.yaml").write_text(
        "\n".join(
            [
                "topics_with_message_count:",
                "  - topic_metadata:",
                "      name: /clock",
                "  - topic_metadata:",
                "      name: /tf",
                "  - topic_metadata:",
                "      name: /tf_static",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (run_dir / "artifacts" / "autoware_control.jsonl").write_text("{}\n", encoding="utf-8")


def _mark_autoware(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["backend"] = "autoware"
    manifest["transport_mode"] = "ros2_autoware"
    manifest["transport_mode_source"] = "fixture"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _add_control_attribution(run_dir: Path, *, status: str = "pass") -> None:
    out = run_dir / "analysis" / "control_attribution"
    out.mkdir(parents=True, exist_ok=True)
    (out / "control_attribution_report.json").write_text(
        json.dumps(
            {
                "schema_version": "control_attribution.v1",
                "run_id": run_dir.name,
                "status": status,
                "verdict": {"status": status},
                "attribution": {"dominant_breakpoint": "source_control_semantics"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _add_natural_driving_marker(run_dir: Path) -> None:
    out = run_dir / "analysis" / "natural_driving"
    out.mkdir(parents=True, exist_ok=True)
    (out / "natural_driving_report.json").write_text(
        json.dumps(
            {
                "schema_version": "town01_natural_driving_report.v1",
                "run_results": [{"run_id": run_dir.name, "verdict": "pass"}],
                "verdict": {"status": "pass"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def test_recording_only_evidence_is_not_comparable_with_apollo() -> None:
    report = analyze_autoware_evidence_run_dir(DEMO_FIXTURE)

    assert report["schema_version"] == AUTOWARE_EVIDENCE_SCHEMA_VERSION
    assert report["rviz_video_present"] is True
    assert report["rosbag_present"] is True
    assert report["rosbag_inspected"] is True
    assert report["rosbag_required_topics_present"] is True
    assert report["carla_third_person_video_present"] is True
    assert report["artifact_completeness_status"] == "recording_only"
    assert report["can_compare_with_apollo"] is False
    assert "route_health.json" in report["missing_artifacts"]


def test_gate_artifacts_make_autoware_comparable_with_apollo(tmp_path: Path) -> None:
    run_dir = tmp_path / "autoware_lane_keep_097"
    shutil.copytree(NATURAL_LANE_FIXTURE, run_dir)
    _mark_autoware(run_dir)
    _add_recording_artifacts(run_dir)
    _add_control_attribution(run_dir)
    _add_natural_driving_marker(run_dir)

    report = analyze_autoware_evidence_run_dir(run_dir)
    outputs = write_autoware_evidence_report(report, run_dir / "analysis" / "autoware_evidence")

    assert report["artifact_completeness_status"] == "ready_for_comparison"
    assert report["gate_artifacts_ready"] is True
    assert report["can_compare_with_apollo"] is True
    assert Path(outputs["autoware_evidence_report"]).exists()


def test_missing_route_health_blocks_autoware_comparison(tmp_path: Path) -> None:
    run_dir = tmp_path / "autoware_lane_keep_097"
    shutil.copytree(NATURAL_LANE_FIXTURE, run_dir)
    _mark_autoware(run_dir)
    _add_recording_artifacts(run_dir)
    _add_control_attribution(run_dir)
    _add_natural_driving_marker(run_dir)
    shutil.rmtree(run_dir / "analysis" / "route_health")

    report = analyze_autoware_evidence_run_dir(run_dir)

    assert report["can_compare_with_apollo"] is False
    assert "route_health.json" in report["missing_artifacts"]


def test_autoware_natural_driving_requires_control_attribution(tmp_path: Path) -> None:
    run_dir = tmp_path / "autoware_lane_keep_097"
    shutil.copytree(NATURAL_LANE_FIXTURE, run_dir)
    _mark_autoware(run_dir)
    _add_recording_artifacts(run_dir)

    report = analyze_natural_driving_suite(run_dir)

    assert report["run_results"][0]["backend"] == "autoware"
    assert report["run_results"][0]["verdict"] == "insufficient_data"
    assert report["run_results"][0]["failure_reason"] == "control_attribution_missing_status"


def test_autoware_natural_driving_can_pass_with_same_gate_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "autoware_lane_keep_097"
    shutil.copytree(NATURAL_LANE_FIXTURE, run_dir)
    _mark_autoware(run_dir)
    _add_recording_artifacts(run_dir)
    _add_control_attribution(run_dir)

    report = analyze_natural_driving_suite(run_dir)

    assert report["run_results"][0]["backend"] == "autoware"
    assert report["run_results"][0]["verdict"] == "pass"


def test_traffic_light_missing_traffic_artifact_cannot_pass(tmp_path: Path) -> None:
    run_dir = tmp_path / "autoware_red_stop"
    shutil.copytree(NATURAL_TL_FIXTURE, run_dir)
    _mark_autoware(run_dir)
    _add_recording_artifacts(run_dir)
    _add_control_attribution(run_dir)
    (run_dir / "traffic_light_contract_report.json").unlink()

    evidence = analyze_autoware_evidence_run_dir(run_dir)
    natural = analyze_natural_driving_suite(run_dir)

    assert evidence["can_compare_with_apollo"] is False
    assert "traffic_light_contract_report.json" in evidence["missing_artifacts"]
    assert natural["run_results"][0]["verdict"] == "insufficient_data"
    assert natural["run_results"][0]["failure_reason"] == "missing_required_artifacts"


def test_autoware_evidence_cli_writes_report(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_autoware_evidence.py",
            "--run-dir",
            str(DEMO_FIXTURE),
            "--out",
            str(out_dir),
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert (out_dir / "autoware_evidence_report.json").exists()
    payload = json.loads((out_dir / "autoware_evidence_report.json").read_text(encoding="utf-8"))
    assert payload["artifact_completeness_status"] == "recording_only"
