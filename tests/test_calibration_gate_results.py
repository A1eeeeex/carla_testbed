from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.transport_ab import analyze_ab_manifest
from carla_testbed.calibration.gates import build_gate_results_from_ab_report


FIXTURE_BATCH = Path("tests/fixtures/ab/simple_batch")
SCRIPT = Path("tools/build_calibration_gate_results.py")


def test_build_gate_results_from_ab_report_maps_hard_gates() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    gate_results = build_gate_results_from_ab_report(report)

    assert gate_results["schema_version"] == "calibration_gate_results.v1"
    assert gate_results["promotion_allowed"] is False
    assert gate_results["gates"]["097"]["status"] == "pass"
    assert gate_results["gates"]["097"]["route_id"] == "lane097"
    assert gate_results["gates"]["097"]["direct_transport_contract_status"] == "aligned"
    assert gate_results["gates"]["097"]["cadence_comparison"]["bridge_loc_hz_ratio"] == 0.9
    assert gate_results["gates"]["217"]["status"] == "insufficient_data"
    assert gate_results["gates"]["031"]["status"] == "insufficient_data"
    assert "vehicle motion alone" not in gate_results["claim_supported"]
    assert gate_results["gates"]["097"]["route_health_path"].endswith(
        "analysis/route_health/route_health.json"
    )


def test_build_gate_results_missing_routes_are_explicit() -> None:
    gate_results = build_gate_results_from_ab_report({"comparisons": [], "run_results": []})

    assert gate_results["promotion_allowed"] is False
    assert set(gate_results["gates"]) == {"097", "217", "031"}
    assert all(gate["status"] == "missing" for gate in gate_results["gates"].values())


def test_build_gate_results_cli_writes_json(tmp_path: Path) -> None:
    ab_report = tmp_path / "ab_report.json"
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")
    ab_report.write_text(json.dumps(report), encoding="utf-8")
    out = tmp_path / "gate_results.json"

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--ab-report", str(ab_report), "--out", str(out)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["promotion_allowed"] is False
    assert payload["gates"]["097"] == "pass"
    written = json.loads(out.read_text(encoding="utf-8"))
    assert written["gates"]["217"]["status"] == "insufficient_data"


def test_candidate_positive_with_low_direct_cadence_is_not_promotion_gate_pass() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")
    report = {
        **report,
        "comparisons": [
            {
                **comparison,
                "cadence_comparison": {
                    **(comparison.get("cadence_comparison") or {}),
                    "bridge_loc_hz_ratio": 0.3,
                    "bridge_chassis_hz_ratio": 0.3,
                },
            }
            if comparison.get("route_id") == "lane097"
            else comparison
            for comparison in report["comparisons"]
        ],
    }

    gate_results = build_gate_results_from_ab_report(report)

    gate = gate_results["gates"]["097"]
    assert gate["status"] == "insufficient_data"
    assert gate_results["promotion_allowed"] is False
    assert any("direct_bridge_cadence_ratio below threshold" in item for item in gate["direct_evidence_issues"])
    assert "direct_bridge_cadence_ratio below threshold" in gate["reason"]


def test_build_gate_results_cli_require_pass_fails_on_incomplete_fixture(tmp_path: Path) -> None:
    ab_report = tmp_path / "ab_report.json"
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")
    ab_report.write_text(json.dumps(report), encoding="utf-8")
    out = tmp_path / "gate_results.json"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--ab-report",
            str(ab_report),
            "--out",
            str(out),
            "--require-pass",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert out.is_file()
