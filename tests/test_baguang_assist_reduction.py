from __future__ import annotations

import csv
import json
import shlex
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.baguang_assist_reduction import (
    BaguangAssistReductionConfig,
    build_assist_reduction_matrix,
    execute_assist_reduction,
    refresh_assist_reduction_artifact_index,
    load_assist_reduction_config,
    validate_assist_reduction_config,
    write_assist_reduction_outputs,
)

CONFIG_PATH = Path("configs/experiments/baguang_assist_reduction.yaml")


def _test_run_dir_from_command(command: str) -> Path:
    if "RUN_DIR=" in command:
        return Path(command.split("RUN_DIR=", 1)[1].split(" ", 1)[0])
    tokens = shlex.split(command)
    return Path(tokens[len(tokens) - 1 - tokens[::-1].index("--run-dir") + 1])


def test_assist_reduction_config_loads() -> None:
    cfg = load_assist_reduction_config(CONFIG_PATH)

    assert cfg["schema_version"] == "baguang_assist_reduction.v1"
    assert cfg["_validation"]["errors"] == []
    assert set(cfg["stacks"]) == {"apollo", "autoware"}
    assert cfg["claim_boundary"]["assist_reduction_probe_proves_unassisted_natural_driving"] is False


def test_build_assist_reduction_matrix_contains_reduction_profiles(tmp_path: Path) -> None:
    matrix, manifest = build_assist_reduction_matrix(
        BaguangAssistReductionConfig(config_path=CONFIG_PATH, out_dir=tmp_path, dry_run=True)
    )

    assert manifest["schema_version"] == "baguang_assist_reduction_manifest.v1"
    assert len(matrix) == 9
    profile_ids = {row["profile_id"] for row in matrix}
    assert "apollo_no_lateral_stabilizer" in profile_ids
    assert "apollo_no_straight_acc_override" in profile_ids
    assert "autoware_default_timeout_probe" in profile_ids
    assert "autoware_no_bridge_smoothing_probe" in profile_ids
    assert len({row["run_id"] for row in matrix}) == len(matrix)
    assert len({row["run_dir"] for row in matrix}) == len(matrix)
    assert all(row["command_stdout_path"].endswith("command_stdout.log") for row in matrix)
    assert all(row["command_stderr_path"].endswith("command_stderr.log") for row in matrix)


def test_apollo_reduction_commands_toggle_wrapper_env(tmp_path: Path) -> None:
    matrix, _ = build_assist_reduction_matrix(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=tmp_path,
            dry_run=True,
            selected_profiles=("apollo_no_lateral_stabilizer", "apollo_no_straight_acc_override"),
        )
    )
    commands = {row["profile_id"]: row["command"] for row in matrix}

    assert "APOLLO_ENABLE_LATERAL_STABILIZER=0" in commands["apollo_no_lateral_stabilizer"]
    assert "APOLLO_ENABLE_STRAIGHT_ACC_OVERRIDE=0" in commands["apollo_no_straight_acc_override"]
    assert "START_CARLA=1" in commands["apollo_no_lateral_stabilizer"]
    assert "UNSET_SDL_VIDEODRIVER=1" in commands["apollo_no_lateral_stabilizer"]
    assert "RECORD_DEMO=1" in commands["apollo_no_lateral_stabilizer"]
    assert "RUN_DIR=" in commands["apollo_no_lateral_stabilizer"]
    assert "bash tools/run_baguang_apollo_followstop_80kph_demo.sh" in commands[
        "apollo_no_lateral_stabilizer"
    ]


def test_autoware_reduction_commands_append_overrides(tmp_path: Path) -> None:
    matrix, _ = build_assist_reduction_matrix(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=tmp_path,
            dry_run=True,
            selected_profiles=("autoware_default_timeout_probe", "autoware_lane_invasion_enabled_probe"),
        )
    )
    commands = {row["profile_id"]: row["command"] for row in matrix}

    assert "--override algo.autoware.carla_control_bridge.timeout_sec=0.8" in commands[
        "autoware_default_timeout_probe"
    ]
    assert "--rig-override events.lane_invasion=true" in commands["autoware_lane_invasion_enabled_probe"]
    assert "--run-dir" in commands["autoware_default_timeout_probe"]


def test_assist_reduction_outputs_and_cli(tmp_path: Path) -> None:
    out = tmp_path / "dry"
    matrix, manifest = write_assist_reduction_outputs(
        BaguangAssistReductionConfig(config_path=CONFIG_PATH, out_dir=out, dry_run=True)
    )

    manifest_path = out / "assist_reduction_manifest.json"
    matrix_path = out / "assist_reduction_matrix.csv"
    assert manifest_path.exists()
    assert matrix_path.exists()
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["run_count"] == len(matrix)
    rows = list(csv.DictReader(matrix_path.open()))
    assert len(rows) == len(matrix)
    assert manifest["analysis_commands"]["comparison_after_runs"]

    cli_out = tmp_path / "cli"
    result = subprocess.run(
        [
            sys.executable,
            "tools/run_baguang_assist_reduction.py",
            "--config",
            str(CONFIG_PATH),
            "--out",
            str(cli_out),
            "--dry-run",
            "--profiles",
            "apollo_no_lateral_stabilizer,autoware_default_timeout_probe",
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    assert '"matrix_rows": 2' in result.stdout
    assert (cli_out / "assist_reduction_manifest.json").exists()
    assert (cli_out / "assist_reduction_matrix.csv").exists()


def test_execute_assist_reduction_with_fake_runner_success(tmp_path: Path) -> None:
    calls: list[str] = []

    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        calls.append(command)
        run_dir = _test_run_dir_from_command(command)
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "summary.json").write_text('{"success": true, "exit_reason": "completed"}\n', encoding="utf-8")
        stdout_path.write_text("ok\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        return 0

    matrix, manifest = execute_assist_reduction(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=tmp_path / "exec_success",
            selected_profiles=("apollo_no_lateral_stabilizer", "autoware_default_timeout_probe"),
        ),
        command_runner=fake_runner,
    )

    assert len(calls) == 2
    assert {row["status"] for row in matrix} == {"success"}
    assert manifest["status_counts"] == {"success": 2}
    assert all(Path(row["command_stdout_path"]).exists() for row in matrix)


def test_execute_indexes_suffixed_actual_run_dir(tmp_path: Path) -> None:
    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        run_dir = _test_run_dir_from_command(command)
        actual = run_dir.with_name(run_dir.name + "__02")
        (actual / "video" / "dual_cam").mkdir(parents=True)
        (actual / "artifacts").mkdir(parents=True)
        (actual / "summary.json").write_text('{"success": true, "exit_reason": "completed"}\n', encoding="utf-8")
        (actual / "timeseries.csv").write_text("frame_id\n1\n", encoding="utf-8")
        (actual / "video" / "dual_cam" / "demo_third_person.mp4").write_bytes(b"mp4")
        (actual / "artifacts" / "dreamview_capture.mp4").write_bytes(b"dv")
        stdout_path.write_text("ok\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        return 0

    matrix, manifest = execute_assist_reduction(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=tmp_path / "suffixed",
            selected_profiles=("apollo_no_lateral_stabilizer",),
        ),
        command_runner=fake_runner,
    )

    row = matrix[0]
    assert row["status"] == "success"
    assert row["artifact_index_status"] == "found"
    assert row["actual_run_dir"].endswith("__02")
    assert row["summary_path"].endswith("__02/summary.json")
    assert row["timeseries_path"].endswith("__02/timeseries.csv")
    assert row["video_path"].endswith("__02/video/dual_cam/demo_third_person.mp4")
    assert row["dreamview_video_path"].endswith("__02/artifacts/dreamview_capture.mp4")
    assert manifest["status_counts"] == {"success": 1}


def test_execute_uses_summary_failure_even_when_command_succeeds(tmp_path: Path) -> None:
    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        run_dir = _test_run_dir_from_command(command)
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "summary.json").write_text(
            '{"success": false, "exit_reason": "COLLISION"}\n',
            encoding="utf-8",
        )
        stdout_path.write_text("ok\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        return 0

    matrix, manifest = execute_assist_reduction(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=tmp_path / "summary_failed",
            selected_profiles=("apollo_no_lateral_stabilizer",),
        ),
        command_runner=fake_runner,
    )

    assert matrix[0]["status"] == "failed"
    assert matrix[0]["failure_reason"] == "collision"
    assert matrix[0]["summary_success"] is False
    assert matrix[0]["summary_exit_reason"] == "COLLISION"
    assert manifest["status_counts"] == {"failed": 1}


def test_refresh_artifact_index_corrects_existing_success_row(tmp_path: Path) -> None:
    out = tmp_path / "refresh"
    matrix, _ = write_assist_reduction_outputs(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=out,
            selected_profiles=("apollo_no_lateral_stabilizer",),
        )
    )
    row = matrix[0]
    planned = Path(row["run_dir"])
    actual = planned.with_name(planned.name + "__02")
    actual.mkdir(parents=True)
    (actual / "summary.json").write_text(
        '{"success": false, "exit_reason": "COLLISION"}\n',
        encoding="utf-8",
    )

    manifest_path = out / "assist_reduction_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["runs"][0]["status"] = "success"
    payload["runs"][0]["return_code"] = 0
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    matrix_path = out / "assist_reduction_matrix.csv"
    rows = list(csv.DictReader(matrix_path.open()))
    rows[0]["status"] = "success"
    rows[0]["return_code"] = "0"
    with matrix_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    result = refresh_assist_reduction_artifact_index(out)
    refreshed = json.loads(manifest_path.read_text(encoding="utf-8"))["runs"][0]

    assert result["status"] == "refreshed"
    assert refreshed["status"] == "failed"
    assert refreshed["failure_reason"] == "collision"
    assert refreshed["actual_run_dir"].endswith("__02")


def test_execute_assist_reduction_stops_after_failure_without_continue(tmp_path: Path) -> None:
    calls: list[str] = []

    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        calls.append(command)
        stdout_path.write_text("boom\n", encoding="utf-8")
        stderr_path.write_text("failed\n", encoding="utf-8")
        return 7

    matrix, manifest = execute_assist_reduction(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=tmp_path / "exec_fail",
            selected_profiles=("apollo_no_lateral_stabilizer", "autoware_default_timeout_probe"),
            continue_on_failure=False,
        ),
        command_runner=fake_runner,
    )

    assert len(calls) == 1
    assert matrix[0]["status"] == "failed"
    assert matrix[0]["return_code"] == 7
    assert matrix[1]["status"] == "skipped"
    assert matrix[1]["failure_reason"] == "stopped_after_previous_failure"
    assert manifest["status_counts"] == {"failed": 1, "skipped": 1}


def test_execute_assist_reduction_continues_after_failure_when_requested(tmp_path: Path) -> None:
    calls: list[str] = []

    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        calls.append(command)
        stdout_path.write_text("fail\n", encoding="utf-8")
        stderr_path.write_text("fail\n", encoding="utf-8")
        return 1

    matrix, manifest = execute_assist_reduction(
        BaguangAssistReductionConfig(
            config_path=CONFIG_PATH,
            out_dir=tmp_path / "exec_continue",
            selected_profiles=("apollo_no_lateral_stabilizer", "autoware_default_timeout_probe"),
            continue_on_failure=True,
        ),
        command_runner=fake_runner,
    )

    assert len(calls) == 2
    assert {row["status"] for row in matrix} == {"failed"}
    assert manifest["status_counts"] == {"failed": 2}


def test_validation_rejects_overclaim() -> None:
    cfg = load_assist_reduction_config(CONFIG_PATH)
    cfg.pop("_validation", None)
    cfg.pop("_source_path", None)
    cfg["claim_boundary"]["assist_reduction_probe_proves_unassisted_natural_driving"] = True

    validation = validate_assist_reduction_config(cfg)

    assert validation["errors"]
    assert any("must not prove unassisted" in error for error in validation["errors"])


def test_apollo_wrapper_shell_syntax_and_env_toggles() -> None:
    script = Path("tools/run_baguang_apollo_followstop_80kph_demo.sh")
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "APOLLO_ENABLE_LATERAL_STABILIZER" in text
    assert "APOLLO_ENABLE_STRAIGHT_ACC_OVERRIDE" in text
    assert "APOLLO_ENABLE_TERMINAL_STOP_HOLD" in text
    assert "APOLLO_DISABLE_LANE_INVASION" in text
    assert "START_CARLA" in text
    assert "UNSET_SDL_VIDEODRIVER" in text
    assert "--start-carla" in text
