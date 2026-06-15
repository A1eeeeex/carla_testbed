from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_map_route_alignment import (
    MAP_ROUTE_ALIGNMENT_SCHEMA_VERSION,
    analyze_apollo_map_route_alignment_run_dir,
    write_apollo_map_route_alignment_report,
)

SCRIPT = Path("tools/analyze_apollo_map_route_alignment.py")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    map_dir = tmp_path / "apollo" / "modules" / "map" / "data" / "carla_town01"
    map_dir.mkdir(parents=True)
    for name in ("base_map.txt", "routing_map.txt", "sim_map.txt"):
        (map_dir / name).write_text(f"{name}\n", encoding="utf-8")
    _write_json(
        run_dir / "manifest.json",
        {"map": "Town01", "carla_world": {"loaded_map_short_name": "Town01"}},
    )
    _write_json(run_dir / "summary.json", {"map": "Town01"})
    _write_json(
        run_dir / "artifacts/map_contract_guard.json",
        {
            "map_contract_invalid": False,
            "high_risk_mismatch": False,
            "dreamview_selected_map": "Town01",
            "runtime_map_dir": str(map_dir),
            "effective_bridge_map_root": str(map_dir),
            "effective_bridge_map_file": str(map_dir / "base_map.txt"),
            "runtime_component_paths": {
                "base_map": str(map_dir / "base_map.txt"),
                "routing_map": str(map_dir / "routing_map.txt"),
                "sim_map": str(map_dir / "sim_map.txt"),
            },
            "same_derivation_chain": True,
        },
    )
    _write_json(
        run_dir / "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
        {
            "schema_version": "apollo_hdmap_projection_report.v1",
            "status": "pass",
            "claim_grade": True,
            "projection": {
                "status": "pass",
                "claim_grade": True,
                "official_source_available": True,
                "heading_error_p95_rad": 0.01,
                "lateral_error_p95_m": 0.05,
                "blocking_reasons": [],
                "warnings": [],
            },
            "blocking_reasons": [],
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract.v1",
            "status": "pass",
            "route_id": "route_1",
            "scenario_lane_namespace": "apollo_hdmap",
            "apollo_lane_namespace": "apollo_hdmap",
            "lane_equivalence_status": "direct_match",
            "scenario_route_lane_sequence": ["15_1_1"],
            "apollo_routing_lane_sequence": ["15_1_1"],
            "scenario_route_length_m": 100.0,
            "apollo_routing_total_length_m": 100.0,
            "blocking_reasons": [],
            "warnings": [],
            "missing_fields": [],
            "lane_equivalence": {"status": "direct_match"},
        },
    )
    _write_json(
        run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {
            "schema_version": "apollo_reference_line_contract.v1",
            "status": "pass",
            "blocking_reasons": [],
            "warnings": [],
            "contracts": {
                "planning_trajectory": {"status": "pass"},
                "control_reference": {"status": "pass"},
                "apollo_hdmap_projection": {"status": "pass"},
            },
        },
    )
    return run_dir


def test_map_route_alignment_passes_complete_direct_match(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_apollo_map_route_alignment_run_dir(run_dir)
    outputs = write_apollo_map_route_alignment_report(report, tmp_path / "out")

    assert report["schema_version"] == MAP_ROUTE_ALIGNMENT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["diagnosis"] == "pass"
    assert Path(outputs["apollo_map_route_alignment_report"]).is_file()


def test_projection_missing_is_insufficient_not_map_failure(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json").unlink()

    report = analyze_apollo_map_route_alignment_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["diagnosis"] == "projection_missing"
    assert "apollo_hdmap_projection_report" in report["missing_artifacts"]


def test_projection_geometry_bad_fails_alignment(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
        {
            "schema_version": "apollo_hdmap_projection_report.v1",
            "status": "fail",
            "projection": {
                "status": "fail",
                "blocking_reasons": ["apollo_hdmap_projection_heading_error_high"],
                "heading_error_p95_rad": 0.2,
            },
            "blocking_reasons": ["apollo_hdmap_projection_heading_error_high"],
        },
    )

    report = analyze_apollo_map_route_alignment_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["diagnosis"] == "projection_geometry_bad"
    assert "apollo_hdmap_projection_heading_error_high" in report["blocking_reasons"]


def test_lane_namespace_unmapped_is_not_pass(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "warn",
            "scenario_lane_namespace": "carla_waypoint",
            "apollo_lane_namespace": "apollo_hdmap",
            "lane_equivalence_status": "cross_namespace_unverified",
            "scenario_route_lane_sequence": ["15:0:1", "117:0:1"],
            "apollo_routing_lane_sequence": ["15_1_1", "82_4_1"],
            "lane_equivalence": {
                "status": "cross_namespace_unverified",
                "first_mismatch": {"index": 1, "carla": "117:0:1", "apollo": "82_4_1"},
                "projection_source": "apollo_hdmap_api",
            },
            "warnings": ["scenario_apollo_lane_namespace_equivalence_unverified"],
        }
    )
    _write_json(route_contract_path, route_contract)

    report = analyze_apollo_map_route_alignment_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["diagnosis"] == "lane_equivalence_missing"
    assert report["lane_equivalence"]["status"] == "insufficient_data"
    assert "verified_lane_equivalence_table" in report["lane_equivalence"]["missing_fields"]


def test_lane_namespace_equivalence_can_be_verified_from_projection_rows(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    route_contract_path = run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json"
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    route_contract.update(
        {
            "status": "warn",
            "scenario_lane_namespace": "carla_waypoint",
            "apollo_lane_namespace": "apollo_hdmap",
            "lane_equivalence_status": "cross_namespace_unverified",
            "scenario_route_lane_sequence": ["15:1", "117:1", "83:1"],
            "apollo_routing_lane_sequence": ["15:1", "82:1", "31:-1"],
            "lane_equivalence": {
                "status": "cross_namespace_unverified",
                "projection_source": "apollo_hdmap_api",
            },
            "warnings": ["scenario_apollo_lane_namespace_equivalence_unverified"],
        }
    )
    _write_json(route_contract_path, route_contract)
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        _lane_equivalence_projection_rows(
            [
                ("15:1", "15_1_1"),
                ("117:1", "82_4_1"),
                ("83:1", "31_1_-1"),
            ]
        ),
    )

    report = analyze_apollo_map_route_alignment_run_dir(run_dir)

    assert report["diagnosis"] == "pass"
    assert report["lane_equivalence"]["status"] == "pass"
    pairs = {
        row["scenario_lane_key"]: row["apollo_lane_key"]
        for row in report["lane_equivalence"]["equivalence"]
    }
    assert pairs["117:1"] == "82:1"
    assert pairs["83:1"] == "31:-1"


def test_reference_line_missing_fails_after_route_alignment_inputs_exist(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {
            "schema_version": "apollo_reference_line_contract.v1",
            "status": "insufficient_data",
            "warnings": ["planning_nonempty_but_reference_line_unproven"],
            "blocking_reasons": [],
            "contracts": {"planning_trajectory": {"status": "insufficient_data"}},
        },
    )

    report = analyze_apollo_map_route_alignment_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["diagnosis"] == "reference_line_missing"


def test_cli_writes_alignment_and_lane_equivalence(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    out = tmp_path / "alignment"

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--run-dir", str(run_dir), "--out", str(out)],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "pass"
    assert (out / "apollo_map_route_alignment_report.json").is_file()
    assert (run_dir / "artifacts" / "map_identity_report.json").is_file()
    assert (run_dir / "artifacts" / "lane_equivalence_town01.json").is_file()


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _lane_equivalence_projection_rows(pairs: list[tuple[str, str]]) -> list[dict]:
    rows: list[dict] = []
    for index, (carla_lane_key, apollo_lane_id) in enumerate(pairs):
        rows.append(
            {
                "source": "apollo_hdmap_api",
                "status": "ok",
                "projection_status": "ok",
                "sample_type": "route",
                "route_index": index,
                "route_s": float(index * 20),
                "carla_lane_key": carla_lane_key,
                "nearest_lane_id": apollo_lane_id,
                "projection_s": float(index * 10),
                "projection_l": 0.05,
                "lateral_error_m": 0.05,
                "heading_error_rad": 0.002,
            }
        )
    return rows
