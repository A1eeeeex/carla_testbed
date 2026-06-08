from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.traffic_flow_contract import (
    analyze_traffic_flow_contract_files,
    write_traffic_flow_contract_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _manifest(**overrides: object) -> dict:
    payload = {
        "schema_version": "traffic_flow_manifest.v1",
        "provider": "carla_traffic_manager",
        "enabled": True,
        "seed": 42,
        "tm_port": 8000,
        "world_synchronous_mode": True,
        "tm_synchronous_mode_requested": "follow_world",
        "tm_synchronous_mode_effective": True,
        "requested_vehicle_count": 2,
        "spawned_vehicle_count": 2,
        "actors": [
            {"actor_id": 1, "role_name": "background_vehicle_0", "control_source": "carla_traffic_manager"},
            {"actor_id": 2, "role_name": "background_vehicle_1", "control_source": "carla_traffic_manager"},
        ],
    }
    payload.update(overrides)
    return payload


def _run_dir(tmp_path: Path, manifest: dict) -> Path:
    run_dir = tmp_path / "run"
    _write_json(run_dir / "artifacts/traffic_flow_manifest.json", manifest)
    _write_jsonl(
        run_dir / "artifacts/traffic_flow_events.jsonl",
        [
            {"event": "autopilot_enabled", "actor_id": 1},
            {"event": "autopilot_enabled", "actor_id": 2},
        ],
    )
    return run_dir


def test_traffic_flow_contract_pass(tmp_path: Path) -> None:
    report = analyze_traffic_flow_contract_files(run_dir=_run_dir(tmp_path, _manifest()))

    assert report["status"] == "pass"
    assert report["metrics"]["spawned_vehicle_count"] == 2
    assert report["metrics"]["tm_sync_matches_world"] is True
    assert report["metrics"]["ego_registered_to_tm"] is False
    assert report["metrics"]["background_vehicle_all_tm_controlled"] is True


def test_traffic_flow_contract_fail_ego_registered(tmp_path: Path) -> None:
    report = analyze_traffic_flow_contract_files(
        run_dir=_run_dir(
            tmp_path,
            _manifest(actors=[{"actor_id": 1, "role_name": "hero", "control_source": "carla_traffic_manager"}]),
        )
    )

    assert report["status"] == "fail"
    assert "ego_registered_to_tm" in report["blocking_reasons"]


def test_traffic_flow_contract_fail_sync_mismatch(tmp_path: Path) -> None:
    report = analyze_traffic_flow_contract_files(
        run_dir=_run_dir(tmp_path, _manifest(tm_synchronous_mode_effective=False))
    )

    assert report["status"] == "fail"
    assert "tm_sync_mismatch" in report["blocking_reasons"]


def test_traffic_flow_contract_fail_background_not_tm_controlled(tmp_path: Path) -> None:
    report = analyze_traffic_flow_contract_files(
        run_dir=_run_dir(
            tmp_path,
            _manifest(
                actors=[
                    {
                        "actor_id": 1,
                        "role_name": "background_vehicle_0",
                        "control_source": "scripted_template",
                    },
                    {
                        "actor_id": 2,
                        "role_name": "background_vehicle_1",
                        "control_source": "carla_traffic_manager",
                    },
                ]
            ),
        )
    )

    assert report["status"] == "fail"
    assert "background_vehicle_not_registered_to_tm" in report["blocking_reasons"]


def test_traffic_flow_contract_missing_manifest_insufficient(tmp_path: Path) -> None:
    report = analyze_traffic_flow_contract_files(run_dir=tmp_path / "missing")

    assert report["status"] == "insufficient_data"
    assert "traffic_flow_manifest_missing" in report["blocking_reasons"]


def test_traffic_flow_contract_writer(tmp_path: Path) -> None:
    report = analyze_traffic_flow_contract_files(run_dir=_run_dir(tmp_path, _manifest()))

    outputs = write_traffic_flow_contract_report(report, tmp_path / "out")

    assert Path(outputs["traffic_flow_contract_report"]).is_file()
    assert Path(outputs["traffic_flow_contract_summary"]).is_file()
