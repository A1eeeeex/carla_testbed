from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.pedestrian_flow_contract import (
    PEDESTRIAN_FLOW_CONTRACT_SCHEMA_VERSION,
    analyze_pedestrian_flow_contract_files,
    analyze_pedestrian_flow_contract,
)


def _manifest(**updates: object) -> dict:
    payload = {
        "schema_version": "traffic_flow_manifest.v1",
        "provider": "carla_walker_ai_controller",
        "enabled": True,
        "seed": 52,
        "world_pedestrians_seed": 52,
        "walker_cross_factor": 0.0,
        "requested_walker_count": 2,
        "spawned_walker_count": 2,
        "controller_count": 2,
        "controller_started_count": 2,
        "walkers": [
            {
                "actor_id": 10,
                "role_name": "background_walker_0",
                "control_source": "carla_walker_ai_controller",
                "behavior": {"controller_id": 20, "controller_started": True},
            },
            {
                "actor_id": 11,
                "role_name": "background_walker_1",
                "control_source": "carla_walker_ai_controller",
                "behavior": {"controller_id": 21, "controller_started": True},
            },
        ],
    }
    payload.update(updates)
    return payload


def test_pedestrian_flow_contract_passes_for_started_controllers() -> None:
    report = analyze_pedestrian_flow_contract(manifest=_manifest())

    assert report["schema_version"] == PEDESTRIAN_FLOW_CONTRACT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["metrics"]["spawned_walker_count"] == 2
    assert report["metrics"]["controller_started_count"] == 2


def test_pedestrian_flow_contract_fails_missing_controller() -> None:
    report = analyze_pedestrian_flow_contract(
        manifest=_manifest(controller_count=1, controller_started_count=1)
    )

    assert report["status"] == "fail"
    assert "walker_ai_controller_missing" in report["blocking_reasons"]
    assert "walker_ai_controller_not_started" in report["blocking_reasons"]


def test_pedestrian_flow_contract_warns_cross_factor() -> None:
    report = analyze_pedestrian_flow_contract(manifest=_manifest(walker_cross_factor=0.5))

    assert report["status"] == "warn"
    assert "walker_cross_factor_nonzero_builtin_crossing" in report["warnings"]


def test_pedestrian_flow_contract_detects_role_collision() -> None:
    manifest = _manifest()
    manifest["walkers"][1]["role_name"] = "background_walker_0"

    report = analyze_pedestrian_flow_contract(manifest=manifest)

    assert report["status"] == "fail"
    assert "walker_role_name_not_unique" in report["blocking_reasons"]


def test_pedestrian_flow_contract_missing_manifest_insufficient(tmp_path: Path) -> None:
    report = analyze_pedestrian_flow_contract_files(run_dir=tmp_path / "missing")

    assert report["status"] == "insufficient_data"
    assert "traffic_flow_manifest_missing" in report["blocking_reasons"]


def test_pedestrian_flow_contract_cli_writes_report(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "traffic_flow_manifest.json").write_text(
        json.dumps(_manifest(), indent=2) + "\n",
        encoding="utf-8",
    )
    (artifacts / "traffic_flow_events.jsonl").write_text(
        json.dumps({"event": "walker_controller_started"}) + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_pedestrian_flow_contract.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert (out_dir / "pedestrian_flow_contract_report.json").is_file()
    assert (out_dir / "pedestrian_flow_contract_summary.md").is_file()
