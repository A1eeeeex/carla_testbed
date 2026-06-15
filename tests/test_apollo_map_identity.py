from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.apollo_map_identity import (
    MAP_IDENTITY_SCHEMA_VERSION,
    analyze_apollo_map_identity_run_dir,
    write_apollo_map_identity_report,
)


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
        {
            "map": "Town01",
            "carla_world": {"loaded_map_short_name": "Town01"},
            "metadata": {"scenario_metadata": {"map": "Town01"}},
        },
    )
    _write_json(run_dir / "summary.json", {"map": "Town01"})
    _write_json(
        run_dir / "artifacts" / "map_contract_guard.json",
        {
            "map_contract_invalid": False,
            "high_risk_mismatch": False,
            "mismatch_classification": "aligned",
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
            "hash_summary": {
                "base_map": {"hash_equal": True},
                "routing_map": {"hash_equal": True},
                "sim_map": {"hash_equal": True},
            },
        },
    )
    return run_dir


def test_map_identity_passes_with_complete_consistent_map_root(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis" / "apollo_hdmap_projection" / "apollo_hdmap_projection_report.json",
        {
            "schema_version": "apollo_hdmap_projection_report.v1",
            "projection": {
                "map_name_topk": ["Town01"],
                "map_dir_topk": ["/apollo/modules/map/data/carla_town01"],
            },
        },
    )

    report = analyze_apollo_map_identity_run_dir(run_dir)
    outputs = write_apollo_map_identity_report(report, tmp_path / "out")

    assert report["schema_version"] == MAP_IDENTITY_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["map_root_consistent"] is True
    assert report["base_map_exists"] is True
    assert report["routing_map_exists"] is True
    assert report["sim_map_exists"] is True
    assert report["selected_apollo_map_root"].endswith("carla_town01")
    assert Path(outputs["map_identity_report"]).is_file()


def test_base_map_missing_fails_map_identity(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    map_dir = Path(json.loads((run_dir / "artifacts" / "map_contract_guard.json").read_text())["runtime_map_dir"])
    (map_dir / "base_map.txt").unlink()

    report = analyze_apollo_map_identity_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["base_map_exists"] is False
    assert "map_files_missing" in report["blocking_reasons"]


def test_bridge_and_projection_map_root_mismatch_fails(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "artifacts" / "apollo_hdmap_projection.jsonl").write_text(
        json.dumps(
            {
                "source": "apollo_hdmap_api",
                "status": "ok",
                "map_name": "Town02",
                "map_dir": "/apollo/modules/map/data/carla_town02",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = analyze_apollo_map_identity_run_dir(run_dir)

    assert report["status"] == "fail"
    assert "mixed_map_root" in report["blocking_reasons"]
    assert "/apollo/modules/map/data/carla_town02" in report["apollo_map_root_candidates"]


def test_hash_mismatch_fails_map_identity(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    guard_path = run_dir / "artifacts" / "map_contract_guard.json"
    guard = json.loads(guard_path.read_text(encoding="utf-8"))
    guard["hash_summary"]["routing_map"]["hash_equal"] = False
    _write_json(guard_path, guard)

    report = analyze_apollo_map_identity_run_dir(run_dir)

    assert report["status"] == "fail"
    assert "map_hash_mismatch" in report["blocking_reasons"]
