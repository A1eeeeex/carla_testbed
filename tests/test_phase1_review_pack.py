from __future__ import annotations

import json
import tarfile
from pathlib import Path

from carla_testbed.analysis.phase1_review_pack import (
    build_phase1_review_pack,
    write_phase1_review_archive,
)


def test_phase1_review_pack_copies_exact_accepted_bundle_and_hashes(tmp_path: Path) -> None:
    catalog = _write_catalog(tmp_path, "follow_stop_static", "runs/acceptance/follow/acceptance/phase1_acceptance_report.json")
    _write_acceptance_bundle(tmp_path / "runs" / "acceptance" / "follow" / "acceptance")

    manifest = build_phase1_review_pack(catalog, tmp_path / "review", repo_root=tmp_path)

    assert manifest["schema_version"] == "phase1_review_pack.v1"
    assert manifest["status"] == "DONE"
    assert manifest["scenario_count"] == 1
    assert manifest["done_scenario_count"] == 1
    assert manifest["missing_required_items"] == []
    scenario = manifest["scenarios"][0]
    assert scenario["scenario"] == "follow_stop_static"
    assert scenario["status"] == "DONE"
    assert scenario["apollo_run_id"] == "apollo_run"
    assert scenario["planning_control_run_id"] == "builtin_run"
    copied_paths = {item["path"] for item in manifest["copied_files"]}
    assert "catalog/phase1_scenario_catalog.json" in copied_paths
    assert "acceptance/follow_stop_static/phase1_acceptance_report.json" in copied_paths
    assert "acceptance/follow_stop_static/evidence/runs/apollo_run/timeseries.csv" in copied_paths
    assert all(item["sha256"] and item["size"] > 0 for item in manifest["copied_files"])
    written = json.loads((tmp_path / "review" / "phase1_review_pack_manifest.json").read_text(encoding="utf-8"))
    assert written["status"] == "DONE"


def test_phase1_review_pack_is_partial_when_catalog_bundle_missing(tmp_path: Path) -> None:
    catalog = _write_catalog(tmp_path, "cut_in_simple", "runs/missing/acceptance/phase1_acceptance_report.json")

    manifest = build_phase1_review_pack(catalog, tmp_path / "review", repo_root=tmp_path)

    assert manifest["status"] == "PARTIAL"
    assert manifest["partial_scenario_count"] == 1
    assert manifest["missing_required_items"][0]["role"] == "accepted_bundle_report"
    assert manifest["scenarios"][0]["blocking_reasons"] == ["accepted_bundle_report_missing"]


def test_phase1_review_pack_is_partial_for_non_self_contained_bundle(tmp_path: Path) -> None:
    catalog = _write_catalog(tmp_path, "lead_hard_brake", "runs/acceptance/hard_brake/acceptance/phase1_acceptance_report.json")
    _write_acceptance_bundle(
        tmp_path / "runs" / "acceptance" / "hard_brake" / "acceptance",
        status="DONE",
        self_contained=False,
    )

    manifest = build_phase1_review_pack(catalog, tmp_path / "review", repo_root=tmp_path)

    assert manifest["status"] == "PARTIAL"
    assert manifest["scenarios"][0]["status"] == "PARTIAL"
    assert "accepted_bundle_not_self_contained" in manifest["scenarios"][0]["blocking_reasons"]


def test_phase1_review_pack_archive_writes_tar_and_sha(tmp_path: Path) -> None:
    catalog = _write_catalog(tmp_path, "follow_stop_static", "runs/acceptance/follow/acceptance/phase1_acceptance_report.json")
    _write_acceptance_bundle(tmp_path / "runs" / "acceptance" / "follow" / "acceptance")
    build_phase1_review_pack(catalog, tmp_path / "review", repo_root=tmp_path)

    outputs = write_phase1_review_archive(tmp_path / "review")

    archive = Path(outputs["archive"])
    sha = Path(outputs["sha256"])
    assert archive.exists()
    assert sha.exists()
    assert outputs["sha256_digest"] in sha.read_text(encoding="utf-8")
    with tarfile.open(archive, "r:gz") as tar:
        names = set(tar.getnames())
    assert "review/phase1_review_pack_manifest.json" in names
    assert "review/acceptance/follow_stop_static/phase1_acceptance_report.json" in names


def _write_catalog(tmp_path: Path, scenario: str, accepted_bundle_path: str) -> Path:
    catalog = tmp_path / "artifacts" / "phase1_scenario_catalog_current" / "phase1_scenario_catalog.json"
    catalog.parent.mkdir(parents=True)
    payload = {
        "schema_version": "phase1_scenario_catalog.v1",
        "summary": {"total": 1, "done": 1, "partial": 0, "not_yet": 0, "unknown": 0},
        "scenarios": [
            {
                "scenario": scenario,
                "overall_status": "DONE",
                "accepted_bundle_status": "DONE",
                "accepted_bundle_path": accepted_bundle_path,
            }
        ],
    }
    catalog.write_text(json.dumps(payload), encoding="utf-8")
    catalog.with_suffix(".md").write_text("# catalog\n", encoding="utf-8")
    return catalog


def _write_acceptance_bundle(path: Path, *, status: str = "DONE", self_contained: bool = True) -> None:
    (path / "evidence" / "runs" / "apollo_run").mkdir(parents=True)
    (path / "evidence" / "comparison").mkdir(parents=True)
    (path / "evidence" / "runs" / "apollo_run" / "timeseries.csv").write_text(
        "sim_time,ego_speed_mps\n0,0\n",
        encoding="utf-8",
    )
    (path / "evidence" / "comparison" / "comparison_summary.json").write_text(
        json.dumps({"comparison_id": "cmp"}),
        encoding="utf-8",
    )
    (path / "phase1_acceptance_report.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1_acceptance.v1",
                "status": status,
                "comparison_id": "cmp",
                "apollo_run_id": "apollo_run",
                "planning_control_run_id": "builtin_run",
                "gates": {"bundle_self_contained": self_contained},
                "bundle_materialization": {"self_contained": self_contained},
            }
        ),
        encoding="utf-8",
    )
    (path / "phase1_acceptance_summary.md").write_text("# acceptance\n", encoding="utf-8")
