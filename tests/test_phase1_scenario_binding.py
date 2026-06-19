from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.phase1_scenario_binding import bind_phase1_scenario_to_run


SCENARIO = "configs/scenarios/baguang/follow_stop_static_300m.yaml"


def test_binds_phase1_scenario_to_legacy_apollo_run(tmp_path: Path) -> None:
    run = _write_run(tmp_path, backend="apollo_cyberrt")

    report = bind_phase1_scenario_to_run(run, SCENARIO)

    manifest = _read_json(run / "manifest.json")
    fixed_scene = _read_json(run / "artifacts" / "fixed_scene_resolved.json")
    written = _read_json(run / "analysis" / "phase1_scenario_binding" / "phase1_scenario_binding_report.json")

    assert report["status"] == "pass"
    assert written["schema_version"] == "phase1_scenario_binding.v1"
    assert manifest["backend_type"] == "apollo_reference_backend"
    assert manifest["scenario_case"] == "baguang_follow_stop_static_300m"
    assert manifest["scenario_class"] == "follow_stop_static"
    assert manifest["fixed_scene_enabled"] is True
    assert manifest["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert manifest["artifact_contract_version"]
    assert fixed_scene["scene_id"] == "baguang_follow_stop_static_300m"
    assert fixed_scene["target_actor_contract"]["status"] == "resolved"
    assert "does not verify fixed-scene playback" in manifest["phase1_scenario_binding"]["claim_boundary"]


def test_role_alias_is_operator_declared_and_reported(tmp_path: Path) -> None:
    run = _write_run(tmp_path, backend="apollo_cyberrt")

    report = bind_phase1_scenario_to_run(run, SCENARIO, role_aliases={"lead_vehicle": "front"})

    manifest = _read_json(run / "manifest.json")
    target_contract = manifest["target_actor_contract"]

    assert report["role_aliases"] == {"lead_vehicle": "front"}
    assert report["warnings"] == ["role_aliases_are_operator_declared_not_runtime_verified"]
    assert target_contract["role_aliases"] == {"lead_vehicle": "front"}
    assert "phase1_scenario_binding_role_aliases_operator_declared" in target_contract["warnings"]


def test_existing_fixed_scene_is_preserved_without_overwrite(tmp_path: Path) -> None:
    run = _write_run(tmp_path, backend="carla_builtin")
    existing = run / "artifacts" / "fixed_scene_resolved.json"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text(json.dumps({"sentinel": True}) + "\n", encoding="utf-8")

    report = bind_phase1_scenario_to_run(run, SCENARIO)

    assert report["fixed_scene_resolved_written"] is False
    assert report["fixed_scene_resolved_preserved"] is True
    assert "fixed_scene_resolved_exists_preserved" in report["warnings"]
    assert _read_json(existing) == {"sentinel": True}
    assert _read_json(run / "manifest.json")["backend_type"] == "planning_control_backend"


def test_overwrite_replaces_existing_fixed_scene(tmp_path: Path) -> None:
    run = _write_run(tmp_path, backend="carla_builtin")
    existing = run / "artifacts" / "fixed_scene_resolved.json"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text(json.dumps({"sentinel": True}) + "\n", encoding="utf-8")

    report = bind_phase1_scenario_to_run(run, SCENARIO, overwrite=True)

    assert report["fixed_scene_resolved_written"] is True
    assert report["fixed_scene_resolved_preserved"] is False
    assert _read_json(existing)["scene_id"] == "baguang_follow_stop_static_300m"


def test_binding_cli_parses_role_alias(tmp_path: Path) -> None:
    run = _write_run(tmp_path, backend="apollo_cyberrt")

    result = subprocess.run(
        [
            sys.executable,
            "tools/bind_phase1_scenario_run.py",
            "--run-dir",
            str(run),
            "--scenario",
            SCENARIO,
            "--role-alias",
            "lead_vehicle=front",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["binding"]["status"] == "pass"
    assert _read_json(run / "manifest.json")["target_actor_contract"]["role_aliases"] == {
        "lead_vehicle": "front"
    }


def _write_run(tmp_path: Path, *, backend: str) -> Path:
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(
        json.dumps({"run_id": "run", "backend": backend, "backend_name": backend}) + "\n",
        encoding="utf-8",
    )
    return run


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
