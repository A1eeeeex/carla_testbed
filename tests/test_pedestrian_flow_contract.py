from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from carla_testbed.analysis.pedestrian_flow_contract import (
    PEDESTRIAN_FLOW_CONTRACT_SCHEMA_VERSION,
    analyze_pedestrian_flow_contract_files,
    analyze_pedestrian_flow_contract,
)
from carla_testbed.traffic.base import TrafficActorInfo, TrafficFlowState
from carla_testbed.traffic.carla_walker_flow import CarlaWalkerFlow


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


def _walker_trace() -> list[dict]:
    return [
        {"event_type": "walker_state", "actor_id": 10, "sim_time_sec": 0.0, "x": 0.0, "y": 0.0, "z": 0.0, "speed_mps": 1.0},
        {"event_type": "walker_state", "actor_id": 10, "sim_time_sec": 1.0, "x": 1.0, "y": 0.0, "z": 0.0, "speed_mps": 1.0},
        {"event_type": "walker_state", "actor_id": 11, "sim_time_sec": 0.0, "x": 0.0, "y": 1.0, "z": 0.0, "speed_mps": 1.2},
        {"event_type": "walker_state", "actor_id": 11, "sim_time_sec": 1.0, "x": 0.0, "y": 2.2, "z": 0.0, "speed_mps": 1.2},
    ]


def test_pedestrian_flow_contract_passes_for_started_controllers() -> None:
    report = analyze_pedestrian_flow_contract(manifest=_manifest(), walker_trace=_walker_trace())

    assert report["schema_version"] == PEDESTRIAN_FLOW_CONTRACT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["metrics"]["spawned_walker_count"] == 2
    assert report["metrics"]["controller_started_count"] == 2
    assert report["metrics"]["walker_trace_row_count"] == 4
    assert report["metrics"]["moving_walker_count"] == 2
    assert report["claim_blocking_reasons"] == []


def test_pedestrian_flow_contract_requires_movement_trace() -> None:
    report = analyze_pedestrian_flow_contract(manifest=_manifest())

    assert report["status"] == "insufficient_data"
    assert "walker_flow_trace" in report["missing_fields"]
    assert "walker_flow_trace_missing" in report["claim_blocking_reasons"]


def test_pedestrian_flow_contract_links_trace_to_manifest_actor_ids() -> None:
    trace = [
        {"event_type": "walker_state", "actor_id": 10, "sim_time_sec": 0.0, "x": 0.0, "y": 0.0, "speed_mps": 1.0},
        {"event_type": "walker_state", "actor_id": 10, "sim_time_sec": 1.0, "x": 1.0, "y": 0.0, "speed_mps": 1.0},
        {"event_type": "walker_state", "actor_id": 99, "sim_time_sec": 0.0, "x": 0.0, "y": 1.0, "speed_mps": 1.0},
        {"event_type": "walker_state", "actor_id": 99, "sim_time_sec": 1.0, "x": 0.0, "y": 2.0, "speed_mps": 1.0},
    ]

    report = analyze_pedestrian_flow_contract(manifest=_manifest(), walker_trace=trace)

    assert report["status"] == "warn"
    assert report["metrics"]["walker_trace_missing_actor_ids"] == ["11"]
    assert "walker_flow_trace_actor_linkage_incomplete" in report["claim_blocking_reasons"]


def test_pedestrian_flow_contract_fails_missing_controller() -> None:
    report = analyze_pedestrian_flow_contract(
        manifest=_manifest(controller_count=1, controller_started_count=1)
    )

    assert report["status"] == "fail"
    assert "walker_ai_controller_missing" in report["blocking_reasons"]
    assert "walker_ai_controller_not_started" in report["blocking_reasons"]


def test_pedestrian_flow_contract_warns_cross_factor() -> None:
    report = analyze_pedestrian_flow_contract(manifest=_manifest(walker_cross_factor=0.5), walker_trace=_walker_trace())

    assert report["status"] == "warn"
    assert "walker_cross_factor_nonzero_builtin_crossing" in report["warnings"]
    assert "walker_cross_factor_nonzero_requires_explicit_allowance" in report["claim_blocking_reasons"]


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
    (artifacts / "walker_flow_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in _walker_trace()) + "\n",
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


def test_walker_flow_tick_writes_movement_trace(tmp_path: Path) -> None:
    flow = CarlaWalkerFlow()
    walker = _FakeWalker(actor_id=42, x=3.0, y=4.0, speed=1.5)
    flow._walkers = [walker]
    flow._state = TrafficFlowState(
        provider=flow.name,
        enabled=True,
        seed=1,
        requested_count=1,
        spawned_count=1,
        actors=[
            TrafficActorInfo(
                actor_id=42,
                role_name="background_walker_0",
                blueprint_id="walker.pedestrian.0001",
                provider=flow.name,
            )
        ],
    )

    flow.tick(SimpleNamespace(run_dir=tmp_path, sim_time_sec=2.5, world_frame=17))

    rows = [
        json.loads(line)
        for line in (tmp_path / "artifacts" / "walker_flow_trace.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert rows[0]["actor_id"] == 42
    assert rows[0]["role_name"] == "background_walker_0"
    assert rows[0]["sim_time_sec"] == 2.5
    assert rows[0]["world_frame"] == 17
    assert rows[0]["velocity_source"] == "carla_actor_velocity"
    assert rows[0]["speed_mps"] == 1.5


class _FakeWalker:
    def __init__(self, *, actor_id: int, x: float, y: float, speed: float) -> None:
        self.id = actor_id
        self._location = SimpleNamespace(x=x, y=y, z=0.0)
        self._velocity = SimpleNamespace(x=speed, y=0.0, z=0.0)

    def get_transform(self) -> SimpleNamespace:
        return SimpleNamespace(location=self._location)

    def get_velocity(self) -> SimpleNamespace:
        return self._velocity
