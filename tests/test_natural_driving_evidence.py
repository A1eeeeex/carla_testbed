from __future__ import annotations

import json
import shutil
from pathlib import Path

from carla_testbed.analysis.natural_driving_evidence import (
    NATURAL_DRIVING_EVIDENCE_SCHEMA_VERSION,
    build_natural_driving_evidence_from_run_dir,
)

FIXTURE_ROOT = Path("tests/fixtures/natural_driving/simple_suite")


def copy_run(tmp_path: Path, name: str = "lane_keep_097") -> Path:
    target = tmp_path / name
    shutil.copytree(FIXTURE_ROOT / name, target)
    return target


def add_minimal_control_attribution(run_dir: Path) -> None:
    out_dir = run_dir / "analysis" / "control_attribution"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "control_attribution_report.json").write_text(
        json.dumps(
            {
                "schema_version": "control_attribution.v1",
                "status": "pass",
                "verdict": {"status": "pass"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def set_assist_ledger(run_dir: Path, active_assists: list[str] | None = None) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    active_assists = active_assists or []
    blocking = [assist for assist in active_assists if assist != "carla_direct_transport"]
    manifest["assist_ledger"] = {
        "schema_version": "assist_ledger.v1",
        "active_assists": active_assists,
        "blocking_assists": blocking,
        "non_blocking_assists": [
            assist for assist in active_assists if assist == "carla_direct_transport"
        ],
        "assist_confidence": "explicit",
        "source_artifact": "manifest",
        "can_claim_unassisted_natural_driving": not blocking,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_full_fixture_evidence_complete(tmp_path: Path) -> None:
    run_dir = copy_run(tmp_path)
    add_minimal_control_attribution(run_dir)
    set_assist_ledger(run_dir)

    evidence = build_natural_driving_evidence_from_run_dir(run_dir).to_dict()

    assert evidence["schema_version"] == NATURAL_DRIVING_EVIDENCE_SCHEMA_VERSION
    assert evidence["scenario_id"] == "lane_keep_097"
    assert evidence["route_id"] == "lane097"
    assert evidence["scenario_class"] == "lane_keep"
    assert evidence["route_health_status"] == "pass"
    assert evidence["route_hard_gate_eligible"] is True
    assert evidence["channel_health_status"] == "pass"
    assert evidence["control_attribution_status"] == "pass"
    assert evidence["traffic_light_evidence_status"] == "not_required"
    assert evidence["assist_ledger_status"] == "pass"
    assert evidence["artifact_completeness_status"] == "pass"
    assert evidence["missing_artifacts"] == []
    assert evidence["can_claim_unassisted_natural_driving"] is True
    assert evidence["can_claim_truth_input_natural_driving"] is True


def test_missing_route_health_is_recorded_as_missing_artifact(tmp_path: Path) -> None:
    run_dir = copy_run(tmp_path)
    add_minimal_control_attribution(run_dir)
    set_assist_ledger(run_dir)
    (run_dir / "analysis" / "route_health" / "route_health.json").unlink()

    evidence = build_natural_driving_evidence_from_run_dir(run_dir).to_dict()

    assert evidence["route_health_status"] == "insufficient_data"
    assert "route_health.json" in evidence["missing_artifacts"]
    assert evidence["artifact_completeness_status"] == "insufficient_data"
    assert evidence["can_claim_truth_input_natural_driving"] is False


def test_blocking_assist_blocks_unassisted_claim(tmp_path: Path) -> None:
    run_dir = copy_run(tmp_path)
    add_minimal_control_attribution(run_dir)
    set_assist_ledger(run_dir, ["straight_lane_lateral_stabilizer"])

    evidence = build_natural_driving_evidence_from_run_dir(run_dir).to_dict()

    assert evidence["active_assists"] == ["straight_lane_lateral_stabilizer"]
    assert evidence["blocking_assists"] == ["straight_lane_lateral_stabilizer"]
    assert evidence["assist_ledger_status"] == "warn"
    assert evidence["can_claim_unassisted_natural_driving"] is False
    assert evidence["can_claim_truth_input_natural_driving"] is False


def test_traffic_light_scenario_missing_traffic_evidence_is_incomplete(tmp_path: Path) -> None:
    run_dir = copy_run(tmp_path, "traffic_light_red_stop")
    add_minimal_control_attribution(run_dir)
    set_assist_ledger(run_dir)
    (run_dir / "traffic_light_behavior_report.json").unlink()
    (run_dir / "traffic_light_contract_report.json").unlink()

    evidence = build_natural_driving_evidence_from_run_dir(run_dir).to_dict()

    assert evidence["scenario_class"] == "traffic_light_red_stop"
    assert evidence["traffic_light_evidence_status"] == "insufficient_data"
    assert "traffic_light_evidence_report.json" in evidence["missing_artifacts"]
    assert evidence["artifact_completeness_status"] == "insufficient_data"
    assert evidence["can_claim_truth_input_natural_driving"] is False
