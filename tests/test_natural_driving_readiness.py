from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.natural_driving_readiness import (
    CommandResult,
    NaturalDrivingReadinessConfig,
    build_town01_natural_driving_readiness,
    write_readiness_plan_preview,
    write_readiness_report,
)
from carla_testbed.experiments.natural_driving_runner import (
    NaturalDrivingRunnerConfig,
    build_run_matrix,
    build_suite_manifest,
)

SUITE_PATH = Path("configs/scenarios/town01_natural_driving_suite.yaml")
SCRIPT = Path("tools/check_town01_natural_driving_readiness.py")


def _carla_root(tmp_path: Path) -> Path:
    root = tmp_path / "CARLA_0.9.16"
    root.mkdir()
    launcher = root / "CarlaUE4.sh"
    launcher.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    launcher.chmod(0o755)
    return root


def _apollo_core_dir(tmp_path: Path) -> Path:
    root = tmp_path / "apollo_core"
    root.mkdir()
    return root


def _fake_runtime_running(args: list[str] | tuple[str, ...]) -> CommandResult:
    argv = list(args)
    if argv[:2] == ["docker", "info"]:
        return CommandResult(return_code=0, stdout="29.1.5\n")
    if argv[:3] == ["docker", "inspect", "--format"] and "{{json .State}}" in argv:
        return CommandResult(
            return_code=0,
            stdout=(
                '{"Status":"running","Running":true,"OOMKilled":false,'
                '"Dead":false,"ExitCode":0,"StartedAt":"2026-05-25T00:00:00Z"}\n'
            ),
        )
    if argv[:2] == ["docker", "inspect"]:
        return CommandResult(return_code=0, stdout="/apollo_neo_dev_10.0.0_pkg running 0\n")
    if argv[:2] == ["pgrep", "-af"]:
        return CommandResult(return_code=1, stdout="")
    return CommandResult(return_code=127, stderr="unexpected command")


def _fake_runtime_exited(args: list[str] | tuple[str, ...]) -> CommandResult:
    argv = list(args)
    if argv[:2] == ["docker", "info"]:
        return CommandResult(return_code=0, stdout="29.1.5\n")
    if argv[:3] == ["docker", "inspect", "--format"] and "{{json .State}}" in argv:
        return CommandResult(
            return_code=0,
            stdout=(
                '{"Status":"exited","Running":false,"OOMKilled":false,'
                '"Dead":false,"ExitCode":137,"StartedAt":"2026-05-24T08:22:06Z",'
                '"FinishedAt":"2026-05-24T10:20:03Z"}\n'
            ),
        )
    if argv[:2] == ["docker", "inspect"]:
        return CommandResult(return_code=0, stdout="/apollo_neo_dev_10.0.0_pkg exited 137\n")
    if argv[:2] == ["pgrep", "-af"]:
        return CommandResult(return_code=1, stdout="")
    return CommandResult(return_code=127, stderr="unexpected command")


def test_readiness_passes_with_complete_suite_and_running_fake_runtime(tmp_path: Path) -> None:
    config = NaturalDrivingReadinessConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "readiness",
        strict_runtime=True,
        python_exec=Path(sys.executable),
        carla_root=_carla_root(tmp_path),
        apollo_core_dir=_apollo_core_dir(tmp_path),
        min_disk_free_gb=0.001,
    )

    report = build_town01_natural_driving_readiness(config, command_runner=_fake_runtime_running)

    assert report["schema_version"] == "town01_natural_driving_readiness.v1"
    assert report["status"] == "ready"
    assert report["needs_local_carla"] is True
    assert report["needs_local_apollo"] is True
    assert report["blockers"] == []
    assert report["suite"]["coverage"]["total"] == 8
    assert report["suite"]["coverage"]["runnable"] == 8
    assert report["suite"]["required_scenario_classes_missing"] == []
    assert report["suite"]["online_config"]["transport_mode"] == "ros2_gt"
    assert report["suite"]["online_config"]["transport_mode_source"] == "online_config.scenario.publish_ros2_gt"
    assert report["suite"]["online_config_provenance_issues"] == []
    assert report["runtime"]["apollo_container"]["running"] is True
    assert report["runtime"]["apollo_container"]["state"]["oom_killed"] is False
    assert report["runtime"]["apollo_core_dumps"]["status"] == "ok"
    assert report["claim_boundary"]["readiness_proves_behavior"] is False
    assert "natural_driving_report.json" in report["claim_boundary"]["required_behavior_artifact"]


def test_readiness_supports_direct_candidate_config_without_default_promotion(tmp_path: Path) -> None:
    direct_config = (
        "configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
    )
    config = NaturalDrivingReadinessConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "readiness",
        strict_runtime=True,
        python_exec=Path(sys.executable),
        carla_root=_carla_root(tmp_path),
        apollo_core_dir=_apollo_core_dir(tmp_path),
        min_disk_free_gb=0.001,
        route_health_config=direct_config,
    )

    report = build_town01_natural_driving_readiness(config, command_runner=_fake_runtime_running)

    assert report["status"] == "ready"
    assert report["suite"]["mode"]["default_backend"] == "ros2_gt"
    assert report["suite"]["online_config"]["transport_mode"] == "carla_direct"
    assert (
        report["suite"]["online_config"]["transport_mode_source"]
        == "online_config.algo.apollo.transport_mode"
    )
    assert report["suite"]["online_config_provenance_issues"] == []
    assert f"--config {direct_config}" in report["next_commands"]["online_single_canary"]
    assert f"--config {direct_config}" in report["next_commands"]["dry_run_suite"]


def test_strict_readiness_fails_when_apollo_container_is_not_running(tmp_path: Path) -> None:
    config = NaturalDrivingReadinessConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "readiness",
        strict_runtime=True,
        python_exec=Path(sys.executable),
        carla_root=_carla_root(tmp_path),
        apollo_core_dir=_apollo_core_dir(tmp_path),
        min_disk_free_gb=0.001,
    )

    report = build_town01_natural_driving_readiness(config, command_runner=_fake_runtime_exited)

    assert report["status"] == "not_ready"
    assert "apollo_container_not_running:apollo_neo_dev_10.0.0_pkg:exited:137" in report["blockers"]
    assert report["runtime"]["apollo_container"]["status"] == "exited"
    assert report["runtime"]["apollo_container"]["state"]["oom_killed"] is False
    remediation = report["remediation"]
    assert remediation["auto_recovery_attempted"] is False
    action_names = {item["name"] for item in remediation["actions"]}
    assert "restart_apollo_container" in action_names
    assert "verify_apollo_container" in action_names
    assert "rerun_strict_readiness" in action_names


def test_non_strict_runtime_problems_are_warnings(tmp_path: Path) -> None:
    def fake_unavailable(args: list[str] | tuple[str, ...]) -> CommandResult:
        argv = list(args)
        if argv[:2] == ["pgrep", "-af"]:
            return CommandResult(return_code=1, stdout="")
        return CommandResult(return_code=127, stderr="missing")

    config = NaturalDrivingReadinessConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "readiness",
        strict_runtime=False,
        python_exec=tmp_path / "missing_python",
        carla_root=tmp_path / "missing_carla",
        apollo_core_dir=_apollo_core_dir(tmp_path),
        min_disk_free_gb=0.001,
    )

    report = build_town01_natural_driving_readiness(config, command_runner=fake_unavailable)

    assert report["status"] == "warn"
    assert report["blockers"] == []
    assert any(item.startswith("python_missing:") for item in report["warnings"])
    assert any(item.startswith("carla_root_missing:") for item in report["warnings"])
    assert any(item.startswith("docker_unavailable:") for item in report["warnings"])
    assert any(item.startswith("apollo_container_missing_or_uninspectable:") for item in report["warnings"])


def test_readiness_writer_creates_json_markdown_and_plan_preview(tmp_path: Path) -> None:
    config = NaturalDrivingReadinessConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "readiness",
        strict_runtime=True,
        python_exec=Path(sys.executable),
        carla_root=_carla_root(tmp_path),
        apollo_core_dir=_apollo_core_dir(tmp_path),
        min_disk_free_gb=0.001,
    )
    report = build_town01_natural_driving_readiness(config, command_runner=_fake_runtime_running)
    outputs = write_readiness_report(report, config.out_dir)

    runner_config = NaturalDrivingRunnerConfig(
        suite_path=config.suite_path,
        out_dir=config.out_dir / "planned_suite",
        dry_run=True,
        continue_on_failure=True,
        python_exec=str(config.python_exec),
    )
    matrix = build_run_matrix(runner_config)
    manifest = build_suite_manifest(runner_config, matrix)
    outputs.update(write_readiness_plan_preview(config, matrix, manifest))

    payload = json.loads(Path(outputs["readiness_json"]).read_text(encoding="utf-8"))
    markdown = Path(outputs["readiness_md"]).read_text(encoding="utf-8")

    assert payload["status"] == "ready"
    assert "does not prove Apollo natural-driving behavior" in markdown
    assert "restart_apollo_container" in markdown or "rerun_strict_readiness" in markdown
    assert Path(outputs["run_matrix_preview"]).is_file()
    assert Path(outputs["suite_manifest_preview"]).is_file()
    preview_manifest = json.loads(Path(outputs["suite_manifest_preview"]).read_text(encoding="utf-8"))
    assert preview_manifest["online_config"]["transport_mode"] == "ros2_gt"


def test_strict_readiness_blocks_large_apollo_core_dump_backlog(tmp_path: Path) -> None:
    core_dir = _apollo_core_dir(tmp_path)
    (core_dir / "core_mainboard.123").write_bytes(b"x" * 2048)
    (core_dir / "not_a_core.txt").write_text("ignored\n", encoding="utf-8")
    config = NaturalDrivingReadinessConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "readiness",
        strict_runtime=True,
        python_exec=Path(sys.executable),
        carla_root=_carla_root(tmp_path),
        apollo_core_dir=core_dir,
        min_disk_free_gb=0.001,
        max_apollo_core_dump_gb=0.000001,
    )

    report = build_town01_natural_driving_readiness(config, command_runner=_fake_runtime_running)

    assert report["status"] == "not_ready"
    assert any(item.startswith("apollo_core_dump_total_above_threshold:") for item in report["blockers"])
    assert report["runtime"]["apollo_core_dumps"]["file_count"] == 1
    action_names = {item["name"] for item in report["remediation"]["actions"]}
    assert "inspect_apollo_core_dumps" in action_names


def test_non_strict_large_apollo_core_dump_backlog_warns(tmp_path: Path) -> None:
    core_dir = _apollo_core_dir(tmp_path)
    (core_dir / "core_dreamview_plus.456").write_bytes(b"x" * 2048)
    config = NaturalDrivingReadinessConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "readiness",
        strict_runtime=False,
        python_exec=Path(sys.executable),
        carla_root=_carla_root(tmp_path),
        apollo_core_dir=core_dir,
        min_disk_free_gb=0.001,
        max_apollo_core_dump_gb=0.000001,
    )

    report = build_town01_natural_driving_readiness(config, command_runner=_fake_runtime_running)

    assert report["status"] == "warn"
    assert any(item.startswith("apollo_core_dump_total_above_threshold:") for item in report["warnings"])
    assert report["runtime"]["apollo_core_dumps"]["status"] == "above_threshold"


def test_readiness_cli_writes_report_without_starting_online_stack(tmp_path: Path) -> None:
    core_dir = _apollo_core_dir(tmp_path)
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(tmp_path / "cli_readiness"),
            "--config",
            "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
            "--no-plan-preview",
            "--min-disk-free-gb",
            "0.001",
            "--apollo-core-dir",
            str(core_dir),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0
    report_path = tmp_path / "cli_readiness" / "town01_natural_driving_readiness.json"
    assert report_path.is_file()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "town01_natural_driving_readiness.v1"
    assert payload["status"] in {"ready", "warn"}
    assert payload["suite"]["online_config"]["transport_mode"] == "ros2_gt"
    assert payload["claim_boundary"]["readiness_proves_natural_driving"] is False
