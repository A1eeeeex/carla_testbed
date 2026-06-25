from __future__ import annotations

import json
import subprocess
import sys
import types
from pathlib import Path

import pytest

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
    assert "comparison_status_counts" in manifest["summary"]
    assert "comparison_target_status_counts" in manifest["summary"]
    assert "orchestration evidence only" in manifest["claim_boundary"]
    for row in manifest["rows"]:
        assert "comparison_status" in row
        assert "comparison_target_status" in row
        assert "backend_phase1_statuses" in row


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
    assert "comparison_status" in manifest["rows"][0]
    csv_text = (out / "phase1_p0_matrix.csv").read_text(encoding="utf-8")
    assert "comparison_status" in csv_text
    assert "backend_phase1_statuses" in csv_text


def test_phase1_p0_matrix_start_carla_dry_run_records_matrix_session(tmp_path: Path) -> None:
    result = run_phase1_p0_matrix(
        out_dir=tmp_path / "p0_matrix_start_carla",
        matrix_id="p0_start_carla",
        dry_run=True,
        start_carla=True,
        carla_root="/tmp/nonexistent-cached-for-dry-run",
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["carla_session"]["requested"] is True
    assert manifest["carla_session"]["status"] == "dry_run_not_started"
    assert manifest["carla_session"]["town"] == "straight_road_for_baguang"
    session_path = Path(manifest["carla_session"]["path"])
    assert session_path.exists()
    session_payload = json.loads(session_path.read_text(encoding="utf-8"))
    assert session_payload["status"] == "dry_run_not_started"
    assert all(row["status"] == "dry_run" for row in result.rows)


def test_cli_phase1_run_p0_matrix_start_carla_dry_run(tmp_path: Path) -> None:
    out = tmp_path / "p0_cli_start_carla"
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
            "p0_cli_start_carla",
            "--dry-run",
            "--start-carla",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "dry_run_not_started" in completed.stdout
    manifest = json.loads((out / "phase1_p0_matrix_manifest.json").read_text(encoding="utf-8"))
    assert manifest["carla_session"]["status"] == "dry_run_not_started"


def test_phase1_p0_matrix_start_carla_failure_writes_matrix_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    carla_root = tmp_path / "CARLA_0.9.16"
    carla_root.mkdir()

    class FakeLauncher:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def start(self) -> None:
            return None

        def wait_ready(self, *, timeout_s: float, poll_s: float) -> bool:
            return False

        def diagnostics_snapshot(self, *, probe_rpc: bool) -> dict[str, object]:
            return {"rpc_handshake_ready": False, "process_alive": True}

        def stop(self) -> None:
            return None

    tbio_mod = types.ModuleType("tbio")
    carla_mod = types.ModuleType("tbio.carla")
    launcher_mod = types.ModuleType("tbio.carla.launcher")
    launcher_mod.CarlaLauncher = FakeLauncher
    monkeypatch.setitem(sys.modules, "tbio", tbio_mod)
    monkeypatch.setitem(sys.modules, "tbio.carla", carla_mod)
    monkeypatch.setitem(sys.modules, "tbio.carla.launcher", launcher_mod)

    result = run_phase1_p0_matrix(
        out_dir=tmp_path / "p0_startup_failure",
        matrix_id="p0_startup_failure",
        dry_run=False,
        start_carla=True,
        carla_root=carla_root,
        carla_timeout_s=0.01,
    )

    assert len(result.rows) == len(DEFAULT_PHASE1_P0_SCENARIOS)
    assert all(row["status"] == "partial_or_failed" for row in result.rows)
    assert all(row["pair_manifest_path"] for row in result.rows)
    assert all(row["comparison_status"] == "invalid" for row in result.rows)
    assert all(row["comparison_reason"] == "all_runs_invalid" for row in result.rows)
    assert all(row["invalid_run_count"] == 2 for row in result.rows)
    assert all(set(row["backend_phase1_statuses"].values()) == {"invalid"} for row in result.rows)
    assert all(
        set(row["backend_failure_reasons"].values()) == {"backend_not_ready"} for row in result.rows
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["carla_session"]["status"] == "per_pair_isolated"
    assert manifest["carla_session"]["isolation_mode"] == "per_pair"
    assert manifest["summary"]["any_error"] is False
    assert manifest["summary"]["invalid_pair_count"] == len(DEFAULT_PHASE1_P0_SCENARIOS)
    assert result.carla_session["status"] == "per_pair_isolated"

    first_pair_manifest = json.loads(Path(result.rows[0]["pair_manifest_path"]).read_text(encoding="utf-8"))
    assert first_pair_manifest["startup_error"].startswith("Phase1CarlaStartupError:")
    assert first_pair_manifest["carla_session"]["status"] == "not_ready"
    assert first_pair_manifest["carla_session"]["stop"]["status"] == "stopped_after_startup_failure"
    for run_dir in result.rows[0]["run_dirs"]:
        status_path = Path(run_dir) / "analysis" / "phase1_status" / "phase1_status.json"
        execution_path = Path(run_dir) / "platform_execution_result.json"
        assert json.loads(status_path.read_text(encoding="utf-8"))["status"] == "invalid"
        assert json.loads(execution_path.read_text(encoding="utf-8"))["status"] == "blocked_by_carla_startup"
