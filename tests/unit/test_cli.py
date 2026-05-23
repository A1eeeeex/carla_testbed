from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "carla_testbed", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_smoke_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "smoke.yaml"
    config_path.write_text(
        "\n".join(
            [
                "run:",
                "  id: cli_smoke",
                "  max_ticks: 5",
                "  fixed_dt_s: 0.05",
                f"  output_root: {tmp_path.as_posix()}",
                "sim:",
                "  host: localhost",
                "  port: 2000",
                "  town: Town01",
                "scenario:",
                "  name: follow_stop",
                "backend:",
                "  name: dummy",
                "",
            ]
        )
    )
    return config_path


def test_cli_help() -> None:
    result = _run_cli("--help")

    assert result.returncode == 0
    assert "run" in result.stdout
    assert "smoke" in result.stdout
    assert "inspect-run" in result.stdout
    assert "config-validate" in result.stdout


def test_config_validate_smoke_config() -> None:
    result = _run_cli("config-validate", "configs/examples/smoke.yaml")

    assert result.returncode == 0
    assert "[config-validate] ok" in result.stdout
    assert "scenario=follow_stop" in result.stdout


def test_smoke_creates_artifacts_without_runtime(tmp_path: Path) -> None:
    config_path = _write_smoke_config(tmp_path)

    result = _run_cli("smoke", "--config", str(config_path))

    assert result.returncode == 0
    assert "[smoke] ok" in result.stdout
    assert "no CARLA/Apollo runtime was started" in result.stdout
    run_dir = tmp_path / "cli_smoke"
    manifest = json.loads((run_dir / "manifest.json").read_text())
    summary = json.loads((run_dir / "summary.json").read_text())
    assert manifest["scenario_name"] == "follow_stop"
    assert manifest["backend_name"] == "dummy"
    assert summary["success"] is True
    assert summary["exit_reason"] == "smoke_config_ok"


def test_inspect_run_reads_fake_manifest_and_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(
        json.dumps({"run_id": "fake", "scenario_name": "follow_stop", "backend_name": "dummy"})
    )
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "success": True,
                "exit_reason": "success",
                "frames": 3,
                "metrics": {"avg_speed_mps": 1.2, "max_speed_mps": 2.3, "collision_count": 0},
            }
        )
    )

    result = _run_cli("inspect-run", str(run_dir))

    assert result.returncode == 0
    assert "run_id=fake" in result.stdout
    assert "success=True" in result.stdout
    assert "avg_speed_mps=1.2" in result.stdout


def test_run_dry_run_loads_typed_config(tmp_path: Path) -> None:
    config_path = _write_smoke_config(tmp_path)

    result = _run_cli("run", "--config", str(config_path), "--dry-run")

    assert result.returncode == 0
    assert "[run] ok" in result.stdout
    assert "dry-run ok" in result.stdout


def test_run_dry_run_dispatches_legacy_followstop_config(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_followstop"

    result = _run_cli(
        "run",
        "--config",
        "configs/io/examples/followstop_apollo_gt_lateral_enabled_stitcher_v1.yaml",
        "--run-dir",
        str(run_dir),
        "--dry-run",
    )

    assert result.returncode == 0
    assert "falling back to legacy runner" in result.stderr
    assert "[dry-run] artifacts at" in result.stdout
    assert (run_dir / "effective.yaml").exists()
