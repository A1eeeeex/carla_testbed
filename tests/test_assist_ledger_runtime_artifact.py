from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.assist_ledger import read_assist_ledger_from_run_dir
from carla_testbed.record import RunArtifactStore, build_manifest, build_summary


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_manifest_records_direct_transport_assist(tmp_path: Path) -> None:
    store = RunArtifactStore(tmp_path).ensure()

    store.write_manifest(
        build_manifest(
            run_id="direct_run",
            backend_name="carla_direct",
            scenario_name="lane_keep_097",
        )
    )

    manifest = _read_json(tmp_path / "manifest.json")
    ledger = manifest["assist_ledger"]
    assert ledger["schema_version"] == "assist_ledger.v1"
    assert ledger["active_assists"] == ["carla_direct_transport"]
    assert ledger["blocking_assists"] == []
    assert ledger["non_blocking_assists"] == ["carla_direct_transport"]
    assert ledger["assist_confidence"] == "inferred"
    assert ledger["source_artifact"] == "manifest"
    assert ledger["can_claim_unassisted_natural_driving"] is True


def test_unknown_assist_state_is_not_claimed_as_unassisted(tmp_path: Path) -> None:
    store = RunArtifactStore(tmp_path).ensure()

    store.write_manifest(build_manifest(run_id="unknown_run"))
    store.write_summary(
        build_summary(
            success=True,
            exit_reason="ok",
            frames=1,
            sim_duration_s=0.05,
            wall_duration_s=0.1,
        )
    )

    summary = _read_json(tmp_path / "summary.json")
    ledger = summary["assist_ledger"]
    assert ledger["active_assists"] == []
    assert ledger["assist_confidence"] == "unknown"
    assert ledger["can_claim_unassisted_natural_driving"] is False
    assert "assist_ledger_unknown_unassisted_claim_not_allowed" in summary["warnings"]

    analyzed = read_assist_ledger_from_run_dir(tmp_path)
    assert analyzed["assist_confidence"] == "unknown"
    assert analyzed["can_claim_unassisted_natural_driving"] is False


def test_summary_records_blocking_assist_from_runtime_fields(tmp_path: Path) -> None:
    store = RunArtifactStore(tmp_path).ensure()
    store.write_manifest(build_manifest(run_id="assisted_run"))

    summary = build_summary(
        success=True,
        exit_reason="ok",
        frames=10,
        sim_duration_s=0.5,
        wall_duration_s=0.6,
    )
    summary["straight_lane_lateral_stabilizer_apply_count"] = 7
    store.write_summary(summary)

    payload = _read_json(tmp_path / "summary.json")
    ledger = payload["assist_ledger"]
    assert ledger["active_assists"] == ["straight_lane_lateral_stabilizer"]
    assert ledger["blocking_assists"] == ["straight_lane_lateral_stabilizer"]
    assert ledger["assist_confidence"] == "inferred"
    assert ledger["source_artifact"] == "summary"
    assert ledger["can_claim_unassisted_natural_driving"] is False

    analyzed = read_assist_ledger_from_run_dir(tmp_path)
    assert analyzed["blocking_assists"] == ["straight_lane_lateral_stabilizer"]
