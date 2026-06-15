from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.gt_replacement_evidence import (
    analyze_gt_replacement_evidence_run_dir,
    write_gt_replacement_evidence_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "route_id": "097",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "planning_requires_prediction": True,
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "route_id": "097",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "backend": "apollo_cyberrt",
        },
    )
    return run_dir


def test_gt_replacement_report_blocks_missing_runtime_contracts(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_gt_replacement_evidence_run_dir(run_dir)

    assert report["schema_version"] == "gt_replacement_evidence.v1"
    assert report["gt_replacements_claim_grade"] is False
    assert report["can_claim_unassisted_natural_driving"] is False
    assert "localization" in report["blocking_replacements"]
    localization = next(module for module in report["replacement_modules"] if module["name"] == "localization")
    assert "localization_contract_report.json" in localization["missing_evidence"]


def test_gt_replacement_report_exposes_claim_grade_localization_and_chassis(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "apollo_localization_contract.v1",
            "status": "pass",
            "verdict": {"status": "pass", "blocking_reasons": []},
            "reference_point": {"vehicle_reference_hard_gate_eligible": True},
        },
    )
    _write_json(
        run_dir / "analysis/chassis_gt_contract/chassis_gt_contract_report.json",
        {
            "schema_version": "chassis_gt_contract.v1",
            "status": "pass",
            "claim_grade": True,
            "blocking_reasons": [],
            "missing_fields": [],
        },
    )

    report = analyze_gt_replacement_evidence_run_dir(run_dir)

    modules = {module["name"]: module for module in report["modules"]}
    assert modules["localization"]["run_claim_grade"] is True
    assert modules["localization"]["hard_gate_eligible"] is True
    assert modules["chassis"]["run_claim_grade"] is True
    assert modules["chassis"]["hard_gate_eligible"] is True
    assert report["gt_replacements_claim_grade"] is False
    assert "prediction" in report["blocking_replacements"]


def test_write_gt_replacement_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    report = analyze_gt_replacement_evidence_run_dir(run_dir)

    outputs = write_gt_replacement_evidence_report(report, tmp_path / "out")

    assert Path(outputs["gt_replacement_evidence_report"]).exists()
    assert Path(outputs["gt_replacement_evidence_summary"]).exists()
