from __future__ import annotations

import json
import subprocess
import sys

from carla_testbed.analysis.scenario_comparison import compare_scenario_runs


def test_apollo_fixed_scene_scaffold_writes_invalid_backend_not_ready(tmp_path) -> None:
    run_dir = tmp_path / "apollo_fixed_scene"
    result = subprocess.run(
        [
            sys.executable,
            "tools/run_phase1_scenario.py",
            "--scenario",
            "configs/scenarios/baguang/follow_stop_static_300m.yaml",
            "--backend",
            "apollo_cyberrt",
            "--run-dir",
            str(run_dir),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    preflight = json.loads((run_dir / "preflight.json").read_text(encoding="utf-8"))
    phase1_status = json.loads(
        (run_dir / "analysis" / "phase1_status" / "phase1_status.json").read_text(encoding="utf-8")
    )

    assert manifest["backend"] == "apollo_cyberrt"
    assert manifest["backend_type"] == "apollo_reference_backend"
    assert manifest["backend_ready"] is False
    assert manifest["starts_runtime"] is False
    assert preflight["schema_version"] == "apollo_fixed_scene_preflight.v1"
    assert preflight["status"] == "backend_not_ready"
    assert "apollo_fixed_scene_runtime_not_migrated" in preflight["reasons"]
    assert phase1_status["status"] == "invalid"
    assert phase1_status["failure_reason"] == "backend_not_ready"
    assert phase1_status["evaluable"] is False


def test_apollo_fixed_scene_invalid_scaffold_is_comparison_ingestible(tmp_path) -> None:
    apollo = tmp_path / "apollo"
    subprocess.run(
        [
            sys.executable,
            "tools/run_phase1_scenario.py",
            "--scenario",
            "configs/scenarios/baguang/follow_stop_static_300m.yaml",
            "--backend",
            "apollo_cyberrt",
            "--run-dir",
            str(apollo),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    builtin = tmp_path / "builtin"
    (builtin / "analysis" / "phase1_status").mkdir(parents=True)
    (builtin / "analysis" / "v_t_gap").mkdir(parents=True)
    (builtin / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "builtin",
                "scenario_id": "baguang_follow_stop_static_300m",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
            }
        ),
        encoding="utf-8",
    )
    (builtin / "summary.json").write_text(json.dumps({"success": True}), encoding="utf-8")
    (builtin / "analysis" / "phase1_status" / "phase1_status.json").write_text(
        json.dumps(
            {
                "run_id": "builtin",
                "scenario_id": "baguang_follow_stop_static_300m",
                "backend": "carla_builtin",
                "backend_type": "planning_control_backend",
                "status": "success",
                "evaluable": True,
            }
        ),
        encoding="utf-8",
    )
    (builtin / "analysis" / "v_t_gap" / "v_t_gap_report.json").write_text(
        json.dumps({"status": "pass", "rows": []}),
        encoding="utf-8",
    )

    report = compare_scenario_runs([apollo, builtin])

    assert report["comparison_status"] == "partially_evaluable"
    assert report["comparison_target_status"] == "missing_evaluable_apollo_reference_backend"
    assert report["backend_coverage"]["apollo_reference_backend"] == 1
    assert report["backend_coverage"]["evaluable_apollo_reference_backend"] == 0
