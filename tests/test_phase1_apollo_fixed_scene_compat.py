from __future__ import annotations

import json
import math
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.phase1_apollo_fixed_scene_compat import (
    derive_apollo_fixed_scene_artifacts,
)


def test_derives_fixed_scene_artifacts_from_target_obstacle_rows(tmp_path: Path) -> None:
    run = _write_apollo_fixed_scene_run(tmp_path)
    _write_obstacle_rows(
        run,
        [
            {"is_ego": True, "carla_actor_id": "ego", "front_obstacle_actor_role": "lead_vehicle"},
            {"carla_actor_id": "ignored", "front_obstacle_actor_role": "other_vehicle"},
            _lead_row(actor_id="101", role="lead_vehicle", yaw_deg=90.0),
        ],
    )

    report = derive_apollo_fixed_scene_artifacts(run)

    assert report["status"] == "pass"
    assert report["row_count"] == 3
    assert report["derived_trace_rows"] == 1
    assert report["actor_roles"] == {"lead_vehicle": "101"}
    assert _report_path(run).exists()

    runtime_state = _read_json(run / "artifacts" / "fixed_scene_runtime_state.json")
    trace_rows = _read_jsonl(run / "artifacts" / "scenario_actor_trace.jsonl")
    phase_events = _read_jsonl(run / "artifacts" / "scenario_phase_events.jsonl")

    assert runtime_state["status"] == "derived_from_apollo_obstacle_gt_contract"
    assert runtime_state["actor_roles"] == {"lead_vehicle": "101"}
    assert trace_rows == [
        {
            "schema_version": "scenario_actor_trace.v1",
            "sim_time_sec": 1.0,
            "actor_role": "lead_vehicle",
            "actor_id": "101",
            "actual_speed_mps": 0.0,
            "x": 42.0,
            "y": 3.0,
            "yaw_rad": math.pi / 2.0,
            "length_m": 4.2,
            "width_m": 1.9,
            "height_m": 1.6,
            "longitudinal_to_ego_m": 20.0,
            "lateral_to_ego_m": 0.2,
            "distance_to_ego_m": 20.1,
            "phase": "lead_hold_stop",
            "source": "derived_from_apollo_obstacle_gt_contract",
        }
    ]
    assert phase_events[0]["phase"] == "lead_hold_stop"
    assert phase_events[0]["source"] == "derived_from_apollo_obstacle_gt_contract"
    assert "not fixed_scene_player playback proof" in phase_events[0]["reason"]


def test_missing_obstacle_rows_writes_insufficient_report(tmp_path: Path) -> None:
    run = _write_apollo_fixed_scene_run(tmp_path)

    report = derive_apollo_fixed_scene_artifacts(run)

    assert report["status"] == "insufficient_data"
    assert "artifacts/obstacle_gt_contract.jsonl" in report["missing_fields"]
    written = _read_json(_report_path(run))
    assert written["status"] == "insufficient_data"
    assert written["derived_artifacts"] == {}


def test_role_alias_maps_legacy_front_role_to_target_actor(tmp_path: Path) -> None:
    run = _write_apollo_fixed_scene_run(tmp_path, role_aliases={"lead_vehicle": "front"})
    _write_obstacle_rows(run, [_lead_row(actor_id="212", role="front")])

    report = derive_apollo_fixed_scene_artifacts(run)
    trace_rows = _read_jsonl(run / "artifacts" / "scenario_actor_trace.jsonl")

    assert report["status"] == "pass"
    assert report["actor_roles"] == {"lead_vehicle": "212"}
    assert trace_rows[0]["actor_role"] == "lead_vehicle"
    assert trace_rows[0]["source_actor_role"] == "front"


def test_existing_artifacts_are_preserved_without_overwrite(tmp_path: Path) -> None:
    run = _write_apollo_fixed_scene_run(tmp_path)
    _write_obstacle_rows(run, [_lead_row(actor_id="101", role="lead_vehicle")])
    trace_path = run / "artifacts" / "scenario_actor_trace.jsonl"
    trace_path.write_text(json.dumps({"sentinel": True}) + "\n", encoding="utf-8")

    report = derive_apollo_fixed_scene_artifacts(run)

    assert report["status"] == "pass"
    assert "scenario_actor_trace_exists_preserved" in report["warnings"]
    assert report["artifact_sources"]["scenario_actor_trace"] == "preserved_existing"
    assert _read_jsonl(trace_path) == [{"sentinel": True}]

    overwritten = derive_apollo_fixed_scene_artifacts(run, overwrite=True)

    assert overwritten["artifact_sources"]["scenario_actor_trace"] == "derived_from_apollo_obstacle_gt_contract"
    assert _read_jsonl(trace_path)[0]["actor_id"] == "101"


def test_non_apollo_backend_is_not_applicable_but_reported(tmp_path: Path) -> None:
    run = _write_apollo_fixed_scene_run(tmp_path, backend="carla_builtin", backend_type="planning_control_backend")

    report = derive_apollo_fixed_scene_artifacts(run)

    assert report["status"] == "not_applicable"
    assert "not_apollo_reference_backend" in report["warnings"]
    assert _read_json(_report_path(run))["status"] == "not_applicable"


def test_compat_cli_returns_nonzero_for_missing_rows(tmp_path: Path) -> None:
    run = _write_apollo_fixed_scene_run(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            "tools/derive_phase1_apollo_fixed_scene_compat.py",
            "--run-dir",
            str(run),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "insufficient_data"
    assert _report_path(run).exists()


def _write_apollo_fixed_scene_run(
    tmp_path: Path,
    *,
    backend: str = "apollo_cyberrt",
    backend_type: str = "apollo_reference_backend",
    role_aliases: dict[str, str] | None = None,
) -> Path:
    run = tmp_path / "run"
    artifacts = run / "artifacts"
    artifacts.mkdir(parents=True)
    target_contract = {
        "schema_version": "target_actor_contract.v1",
        "status": "resolved",
        "required": True,
        "source": "scenario_case_explicit",
        "target_actor_role": "lead_vehicle",
        "role_aliases": role_aliases or {},
        "scenario_class": "follow_stop_static",
    }
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run",
                "backend": backend,
                "backend_type": backend_type,
                "scenario_case": "baguang_follow_stop_static_300m",
                "target_actor_contract": target_contract,
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(json.dumps({"success": True}), encoding="utf-8")
    (artifacts / "fixed_scene_resolved.json").write_text(
        json.dumps(
            {
                "schema_version": "fixed_scene_storyboard.v1",
                "scene_id": "baguang_follow_stop_static_300m",
                "template": "static_lead_stop",
                "target_actor_contract": target_contract,
                "storyboard": {"phases": [{"id": "lead_hold_stop"}]},
            }
        ),
        encoding="utf-8",
    )
    return run


def _lead_row(*, actor_id: str, role: str, yaw_deg: float = 0.0) -> dict[str, object]:
    return {
        "timestamp": 1.0,
        "carla_actor_id": actor_id,
        "front_obstacle_actor_id": actor_id,
        "front_obstacle_actor_role": role,
        "front_obstacle_actor_x": 42.0,
        "front_obstacle_actor_y": 3.0,
        "front_obstacle_actor_yaw_deg": yaw_deg,
        "front_obstacle_actor_speed_mps": 0.0,
        "front_obstacle_gap_lon_m": 20.0,
        "front_obstacle_gap_lat_m": 0.2,
        "front_obstacle_gap_distance_m": 20.1,
        "length": 4.2,
        "width": 1.9,
        "height": 1.6,
        "is_ego": False,
    }


def _write_obstacle_rows(run: Path, rows: list[dict[str, object]]) -> None:
    (run / "artifacts" / "obstacle_gt_contract.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _report_path(run: Path) -> Path:
    return run / "analysis" / "phase1_apollo_fixed_scene_compat" / "phase1_apollo_fixed_scene_compat_report.json"


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
