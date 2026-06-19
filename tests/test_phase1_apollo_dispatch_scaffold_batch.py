from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.phase1_apollo_dispatch_scaffold_batch import (
    write_apollo_dispatch_scaffold_batch,
)


def test_batch_writes_invalid_dynamic_dispatch_scaffolds_without_backend_loss(tmp_path: Path) -> None:
    out = tmp_path / "batch"

    report = write_apollo_dispatch_scaffold_batch(
        out_dir=out,
        scenarios=[
            "baguang_lead_decel_70_to_40_20m",
            "baguang_cut_in_35kph_left_to_right_10m",
        ],
        repo_root=".",
    )

    assert report["status"] == "ok"
    assert len(report["rows"]) == 2
    assert "must not be counted as Apollo backend losses" in report["claim_boundary"]
    manifest_path = Path(report["outputs"]["manifest"])
    matrix_path = Path(report["outputs"]["matrix"])
    assert manifest_path.exists()
    assert matrix_path.exists()

    rows = {row["scenario"]: row for row in report["rows"]}
    lead = rows["baguang_lead_decel_70_to_40_20m"]
    cut_in = rows["baguang_cut_in_35kph_left_to_right_10m"]

    assert lead["scaffold_status"] == "invalid_written"
    assert lead["phase1_status"] == "invalid"
    assert lead["failure_reason"] == "backend_not_ready"
    assert lead["dispatch_status"] == "partial"
    assert lead["dispatch_mode"] == "runtime_migration_required"
    assert "speed_profile_non_ego_actor_control" in lead["runtime_migration_requirements"]
    assert "lane_change_non_ego_actor_playback" in cut_in["runtime_migration_requirements"]
    assert "target_actor_active_after_phase:cut_in_lane_change" in cut_in["runtime_migration_requirements"]

    phase1_status = json.loads(
        (
            out
            / "baguang_lead_decel_70_to_40_20m"
            / "analysis"
            / "phase1_status"
            / "phase1_status.json"
        ).read_text(encoding="utf-8")
    )
    assert phase1_status["status"] == "invalid"
    assert phase1_status["evaluable"] is False
    assert phase1_status["failure_reason"] == "backend_not_ready"

    matrix_rows = list(csv.DictReader(matrix_path.open(encoding="utf-8")))
    assert len(matrix_rows) == 2
    assert matrix_rows[0]["dispatch_mode"] == "runtime_migration_required"


def test_batch_cli_writes_manifest_and_matrix(tmp_path: Path) -> None:
    out = tmp_path / "cli_batch"
    result = subprocess.run(
        [
            sys.executable,
            "tools/run_phase1_apollo_dispatch_scaffold_batch.py",
            "--scenario",
            "baguang_lead_decel_70_to_40_20m",
            "--scenario",
            "baguang_cut_in_35kph_left_to_right_10m",
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["row_count"] == 2
    assert (out / "phase1_apollo_dispatch_scaffold_batch_manifest.json").exists()
    assert (out / "phase1_apollo_dispatch_scaffold_matrix.csv").exists()
