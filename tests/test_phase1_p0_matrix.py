from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.platform.phase1_p0_matrix import DEFAULT_PHASE1_P0_SCENARIOS, run_phase1_p0_matrix


def test_phase1_p0_matrix_dry_run_materializes_five_pairs(tmp_path: Path) -> None:
    result = run_phase1_p0_matrix(
        out_dir=tmp_path / "p0_matrix",
        matrix_id="p0_ci",
        dry_run=True,
    )

    assert result.matrix_id == "p0_ci"
    assert result.manifest_path.exists()
    assert result.csv_path.exists()
    assert len(result.rows) == 5
    assert {row["canonical_case"] for row in result.rows} == {
        item.canonical_case for item in DEFAULT_PHASE1_P0_SCENARIOS
    }
    assert all(row["status"] == "dry_run" for row in result.rows)
    assert all(row["pair_manifest_path"] for row in result.rows)

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "phase1_p0_matrix.v1"
    assert manifest["dry_run"] is True
    assert manifest["summary"]["all_pairs_materialized"] is True
    assert manifest["summary"]["all_dry_run"] is True
    assert "orchestration evidence only" in manifest["claim_boundary"]


def test_phase1_p0_matrix_pair_outputs_keep_backend_contract_boundary(tmp_path: Path) -> None:
    result = run_phase1_p0_matrix(
        out_dir=tmp_path / "p0_matrix",
        matrix_id="p0_contract",
        dry_run=True,
    )

    run_manifests = []
    for row in result.rows:
        pair_manifest = json.loads(Path(row["pair_manifest_path"]).read_text(encoding="utf-8"))
        assert pair_manifest["dry_run"] is True
        assert "Invalid runs are setup/evidence failures" in pair_manifest["claim_boundary"]
        assert pair_manifest["planning_control"]["execution_status"] == "dry_run"
        assert pair_manifest["apollo"]["execution_status"] == "dry_run"
        run_manifests.extend(Path(run_dir) / "manifest.json" for run_dir in row["run_dirs"])

    assert len(run_manifests) == 10
    for manifest_path in run_manifests:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert payload["legacy_dispatch"] is False
        assert payload["backend_contract"]["backend"] in {"carla_builtin", "apollo_cyberrt"}
        assert payload["claim_boundary"]["schema_version"] == "phase1_claim_boundary.v1"
        assert payload["backend_type"] in {"planning_control_backend", "apollo_reference_backend"}


def test_cli_phase1_run_p0_matrix_dry_run(tmp_path: Path) -> None:
    out = tmp_path / "p0_cli"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "phase1",
            "run-p0-matrix",
            "--out",
            str(out),
            "--matrix-id",
            "p0_cli",
            "--dry-run",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "phase1_p0_matrix.v1" in completed.stdout
    assert (out / "phase1_p0_matrix_manifest.json").exists()
    assert (out / "phase1_p0_matrix.csv").exists()
    manifest = json.loads((out / "phase1_p0_matrix_manifest.json").read_text(encoding="utf-8"))
    assert manifest["scenario_count"] == 5
    assert manifest["summary"]["all_pairs_materialized"] is True
