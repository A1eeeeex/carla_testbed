from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.platform.compiler import compile_suite_matrix
from carla_testbed.platform.registry import PlatformRegistry


def test_platform_suite_matrix_compiles_apollo_and_autoware_profiles() -> None:
    plans = compile_suite_matrix(
        "configs/suites/town01_natural_driving.platform.yaml",
        registry=PlatformRegistry(repo_root="."),
    )

    assert len(plans) == 16
    platforms = {plan.platform.name for plan in plans}
    recordings = {plan.recording.profile for plan in plans}
    scenario_ids = {plan.scenario.scenario_id for plan in plans}
    assert platforms == {"apollo_cyberrt", "autoware_ros2"}
    assert recordings == {"none", "demo"}
    assert all(plan.identity.suite_id == "town01_natural_driving_platform_p0" for plan in plans)
    assert scenario_ids == {
        "town01_lane_keep_097",
        "town01_curve217_diagnostic",
        "town01_junction_031",
        "town01_traffic_light_red_stop",
    }


def test_cli_suite_plan_writes_one_file_per_matrix_entry(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "plan",
            "--suite",
            "configs/suites/town01_natural_driving.platform.yaml",
            "--out",
            str(tmp_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["plan_count"] == 16
    assert len(list(Path(tmp_path).glob("*.plan.resolved.yaml"))) == 16


def test_cli_suite_dry_run_writes_manifest_and_matrix(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "suite",
            "dry-run",
            "--suite",
            "configs/suites/town01_natural_driving.platform.yaml",
            "--out",
            str(tmp_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["schema_version"] == "suite_dry_run_manifest.v1"
    assert payload["starts_runtime"] is False
    assert (tmp_path / "suite_manifest.json").exists()
    assert (tmp_path / "run_matrix.csv").exists()
    assert len(list((tmp_path / "plans").glob("*.plan.resolved.yaml"))) == 16


def test_cli_suite_run_legacy_dispatch_does_not_report_runtime_success(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "carla_testbed",
            "suite",
            "run",
            "--suite",
            "configs/suites/town01_natural_driving.platform.yaml",
            "--out",
            str(tmp_path),
            "--legacy-dispatch",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "real RunPlan suite dispatch is not implemented" in result.stderr
    manifest = json.loads((tmp_path / "suite_manifest.json").read_text(encoding="utf-8"))
    assert manifest["action"] == "run"
    assert manifest["dry_run"] is True
    assert manifest["starts_runtime"] is False
