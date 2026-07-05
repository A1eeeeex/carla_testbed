from __future__ import annotations

import csv
import json
import shlex
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.ab_manifest import load_ab_manifest
from carla_testbed.experiments.ab_runner import (
    ABRunnerConfig,
    build_ab_manifest,
    build_run_matrix,
    execute_matrix,
    finalize_route_health_artifacts_for_row,
)


ROUTE_CONFIG = Path("configs/routes/town01/canonical_five.yaml")
RANDOM_ROUTE_CONFIG = Path("configs/routes/town01/random_regression_pool_20260416.yaml")
SCRIPT = Path("tools/run_town01_direct_ab.py")


def _config(tmp_path: Path, *, include_diagnostic_curves: bool = False, routes: tuple[str, ...] = ()) -> ABRunnerConfig:
    return ABRunnerConfig(
        route_config=ROUTE_CONFIG,
        durations_s=(30.0, 60.0, 120.0),
        baseline_backend="ros2_gt",
        candidate_backend="carla_direct",
        out_dir=tmp_path / "ab",
        fixed_delta_seconds=0.05,
        include_diagnostic_curves=include_diagnostic_curves,
        selected_routes=routes,
        dry_run=True,
        batch_id="test_batch",
    )


def test_dry_run_matrix_defaults_to_hard_gates(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path))

    assert len(rows) == 3 * 2 * 3
    assert {row["route_id"] for row in rows} == {"lane097", "lane217", "junction031"}
    assert {row["backend"] for row in rows} == {"ros2_gt", "carla_direct"}
    assert {row["duration_s"] for row in rows} == {30.0, 60.0, 120.0}
    assert {row["ticks"] for row in rows} == {600, 1200, 2400}
    assert all(row["status"] == "dry_run" for row in rows)


def test_dry_run_records_transport_context(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path, routes=("097",)))
    by_backend = {row["backend"]: row for row in rows if row["duration_s"] == 30.0}

    assert by_backend["ros2_gt"]["transport_mode"] == "ros2_gt"
    assert by_backend["ros2_gt"]["steering_percent_normalization"] == "legacy_double_percent"
    assert by_backend["carla_direct"]["transport_mode"] == "carla_direct"
    assert by_backend["carla_direct"]["direct_control_apply_mode"] == "frame_flush_only"
    assert by_backend["carla_direct"]["direct_stale_world_frame_policy"] == "always_republish"
    assert by_backend["carla_direct"]["steering_percent_normalization"] == "legacy_double_percent"

    manifest = build_ab_manifest(_config(tmp_path, routes=("097",)), rows)
    direct_run = next(run for run in manifest.runs if run.backend == "carla_direct")
    assert direct_run.direct_stale_world_frame_policy == "always_republish"
    assert direct_run.direct_control_apply_mode == "frame_flush_only"
    assert direct_run.apollo_channel_health_path.endswith(
        "analysis/apollo_channel_health/apollo_channel_health_report.json"
    )
    assert direct_run.control_health_path.endswith("analysis/control_health/control_health_report.json")
    assert direct_run.return_code is None
    assert direct_run.actual_run_dir is None
    assert direct_run.command_stdout_path.endswith("command_stdout.log")
    assert direct_run.command_stderr_path.endswith("command_stderr.log")


def test_online_commands_force_strict_runtime_overrides(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path, routes=("097",)))
    by_backend = {row["backend"]: row for row in rows if row["duration_s"] == 30.0}
    baseline_command = by_backend["ros2_gt"]["command"]
    direct_command = by_backend["carla_direct"]["command"]

    assert "--override algo.apollo.control_mapping.steering_percent_normalization=legacy_double_percent" in baseline_command
    assert "--override algo.apollo.control_mapping.steering_percent_normalization=legacy_double_percent" in direct_command
    assert "--override algo.apollo.direct_bridge.control_apply_mode=frame_flush_only" in direct_command
    assert "--override algo.apollo.direct_bridge.stale_world_frame_policy=always_republish" in direct_command
    assert "algo.apollo.direct_bridge.stale_world_frame_policy" not in baseline_command


def test_run_id_and_run_dir_are_unique(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path))

    assert len({row["run_id"] for row in rows}) == len(rows)
    assert len({row["run_dir"] for row in rows}) == len(rows)
    assert all(row["run_id"] in row["run_dir"] for row in rows)


def test_online_commands_target_chain_runner_without_transport_ab_only_flags(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path, routes=("097",)))
    command = rows[0]["command"]

    assert "tools/run_town01_capability_online_chain.py" in command
    assert "--step lane_keep:town01_rh_spawn097_goal046" in command
    assert (
        "--config configs/io/examples/"
        "town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    ) in command
    assert "--backend" not in command
    assert "--out" not in command
    assert "--route-set" not in command


def test_carla_memory_preflight_bypass_is_forwarded(tmp_path: Path) -> None:
    cfg = _config(tmp_path, routes=("097",))
    cfg = ABRunnerConfig(**{**cfg.__dict__, "carla_ignore_memory_preflight": True})
    rows = build_run_matrix(cfg)

    assert "--carla-ignore-memory-preflight" in rows[0]["command"]


def test_include_diagnostic_curves_adds_curve_routes(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path, include_diagnostic_curves=True))

    assert len(rows) == 5 * 2 * 3
    assert {"curve217", "curve213"}.issubset({row["route_id"] for row in rows})


def test_include_informational_routes_adds_random_pool(tmp_path: Path) -> None:
    cfg = ABRunnerConfig(
        route_config=RANDOM_ROUTE_CONFIG,
        durations_s=(30.0,),
        baseline_backend="ros2_gt",
        candidate_backend="carla_direct",
        out_dir=tmp_path / "ab",
        include_informational_routes=True,
        dry_run=True,
        batch_id="random_batch",
    )

    rows = build_run_matrix(cfg)

    assert len(rows) == 6 * 2
    assert {row["gate_role"] for row in rows} == {"informational"}
    assert "random_curve_219_048" in {row["route_id"] for row in rows}


def test_route_filter_selects_requested_routes(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path, routes=("097", "217", "031")))

    assert {row["route_id"] for row in rows} == {"lane097", "lane217", "junction031"}


def test_route_filter_accepts_curve_aliases_and_stable_ids(tmp_path: Path) -> None:
    rows = build_run_matrix(
        _config(
            tmp_path,
            include_diagnostic_curves=True,
            routes=("curve217", "213", "town01_rh_spawn031_goal056"),
        )
    )

    assert {row["route_id"] for row in rows} == {"junction031", "curve217", "curve213"}


def test_cli_dry_run_writes_manifest_and_matrix(tmp_path: Path) -> None:
    out = tmp_path / "direct_ab"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(ROUTE_CONFIG),
            "--durations",
            "30,60,120",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--dry-run",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["matrix_rows"] == 18
    assert "tools/analyze_ab_report.py" in payload["analysis_command"]
    assert "--require-hard-gate-pass" in payload["strict_gate_command"]
    assert "--require-steering-normalization-mode legacy_double_percent" in payload["strict_gate_command"]
    assert "--require-direct-control-apply-mode frame_flush_only" in payload["strict_gate_command"]
    assert "--require-direct-stale-world-frame-policy always_republish" in payload["strict_gate_command"]
    assert "--require-direct-transport-contract-aligned" in payload["strict_gate_command"]
    assert "--require-direct-bridge-cadence-ratio-min 0.8" in payload["strict_gate_command"]
    manifest_path = out / "ab_manifest.json"
    matrix_path = out / "ab_matrix.csv"
    assert manifest_path.is_file()
    assert matrix_path.is_file()

    manifest = load_ab_manifest(manifest_path)
    assert len(manifest.runs) == 18
    assert "--require-hard-gate-pass" in manifest.analysis_commands["strict_hard_gate_steering_norm"]
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 18
    assert rows[0]["status"] == "dry_run"
    assert rows[0]["command_stdout_path"].endswith("command_stdout.log")
    assert rows[0]["command_stderr_path"].endswith("command_stderr.log")
    assert rows[0]["apollo_channel_health_path"].endswith(
        "analysis/apollo_channel_health/apollo_channel_health_report.json"
    )
    assert rows[0]["control_health_path"].endswith("analysis/control_health/control_health_report.json")


def test_manifest_records_standard_and_strict_analysis_commands(tmp_path: Path) -> None:
    cfg = _config(tmp_path, routes=("097",))
    matrix = build_run_matrix(cfg)
    manifest = build_ab_manifest(cfg, matrix)

    assert manifest.analysis_commands["standard"].endswith(f"--batch-root {cfg.out_dir} --out {cfg.out_dir / 'analysis'}")
    strict = manifest.analysis_commands["strict_hard_gate_steering_norm"]
    assert "--require-hard-gate-pass" in strict
    assert "--require-steering-normalization-mode legacy_double_percent" in strict
    assert "--require-direct-control-apply-mode frame_flush_only" in strict
    assert "--require-direct-stale-world-frame-policy always_republish" in strict
    assert "--require-direct-transport-contract-aligned" in strict
    assert "--require-direct-bridge-cadence-ratio-min 0.8" in strict


def test_cli_include_diagnostic_curves_writes_larger_matrix(tmp_path: Path) -> None:
    out = tmp_path / "direct_ab_curves"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(ROUTE_CONFIG),
            "--durations",
            "30,60,120",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--include-diagnostic-curves",
            "--dry-run",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    with (out / "ab_matrix.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 30
    assert {"curve217", "curve213"}.issubset({row["route_id"] for row in rows})


def test_cli_include_informational_routes_writes_random_matrix(tmp_path: Path) -> None:
    out = tmp_path / "random_ab"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(RANDOM_ROUTE_CONFIG),
            "--durations",
            "30",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--include-informational-routes",
            "--dry-run",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    with (out / "ab_matrix.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 12
    assert {"random_lane_183_044", "random_curve_177_051"}.issubset({row["route_id"] for row in rows})


def test_cli_can_analyze_after_dry_run_without_requirements(tmp_path: Path) -> None:
    out = tmp_path / "direct_ab_analyzed"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(ROUTE_CONFIG),
            "--durations",
            "30",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--routes",
            "097",
            "--dry-run",
            "--analyze-after-run",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["analysis"]["verdict"]["status"] == "insufficient_data"
    assert (out / "analysis" / "ab_report.json").is_file()
    assert (out / "analysis" / "ab_report_summary.md").is_file()
    report = json.loads((out / "analysis" / "ab_report.json").read_text(encoding="utf-8"))
    direct = next(row for row in report["run_results"] if row["backend"] == "carla_direct")
    assert direct["direct_stale_world_frame_policy"] == "always_republish"
    assert direct["direct_control_apply_mode"] == "frame_flush_only"
    assert direct["steering_percent_normalization_planned"] == "legacy_double_percent"


def test_cli_strict_after_dry_run_returns_nonzero(tmp_path: Path) -> None:
    out = tmp_path / "direct_ab_strict"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(ROUTE_CONFIG),
            "--durations",
            "30",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--routes",
            "097",
            "--dry-run",
            "--require-hard-gate-pass",
            "--require-steering-normalization-mode",
            "single_percent_at_select",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["analysis"]["requirement_check"]["passed"] is False
    assert any("hard_gate_summary.status" in item for item in payload["analysis"]["requirement_check"]["failures"])


def test_cli_direct_policy_requirement_checks_manifest_fallback(tmp_path: Path) -> None:
    out = tmp_path / "direct_ab_policy_req"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(ROUTE_CONFIG),
            "--durations",
            "30",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--routes",
            "097",
            "--dry-run",
            "--require-direct-control-apply-mode",
            "frame_flush_only",
            "--require-direct-stale-world-frame-policy",
            "always_republish",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["analysis"]["requirement_check"]["passed"] is True


def test_cli_direct_policy_requirement_fails_on_mismatch(tmp_path: Path) -> None:
    out = tmp_path / "direct_ab_policy_req_bad"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(ROUTE_CONFIG),
            "--durations",
            "30",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--routes",
            "097",
            "--dry-run",
            "--require-direct-stale-world-frame-policy",
            "until_control",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["analysis"]["requirement_check"]["passed"] is False
    assert any("direct_stale_world_frame_policy mismatch" in item for item in payload["analysis"]["requirement_check"]["failures"])


def test_cli_direct_transport_contract_alignment_fails_on_dry_run(tmp_path: Path) -> None:
    out = tmp_path / "direct_ab_contract_aligned"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--route-config",
            str(ROUTE_CONFIG),
            "--durations",
            "30",
            "--baseline",
            "ros2_gt",
            "--candidate",
            "carla_direct",
            "--routes",
            "097",
            "--dry-run",
            "--require-direct-transport-contract-aligned",
            "--out",
            str(out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert any(
        "direct_transport_contract_status mismatch" in item
        for item in payload["analysis"]["requirement_check"]["failures"]
    )


def test_finalize_route_health_artifacts_updates_nested_run_paths(tmp_path: Path) -> None:
    run_root = tmp_path / "ab" / "ros2_gt" / "lane217" / "30s" / "batch__ros2_gt__lane217__30s"
    actual_run = run_root / "lane_keep_seed" / "lane_keep_seed__02"
    actual_run.mkdir(parents=True)
    (actual_run / "manifest.json").write_text("{}", encoding="utf-8")
    (actual_run / "summary.json").write_text("{}", encoding="utf-8")
    (actual_run / "timeseries.csv").write_text(
        "\n".join(
            [
                "frame_id,sim_time,route_id,nearest_route_index,route_s,route_x,route_y,route_z,route_heading,route_curvature,ego_x,ego_y,ego_heading,cross_track_error,heading_error",
                "0,0.00,town01_fixture,0,0.0,0.0,0.0,0.0,0.0,0.0,0.1,0.0,0.0,0.1,0.01",
                "1,0.05,town01_fixture,1,1.0,1.0,0.0,0.0,0.0,0.01,1.0,0.2,0.0,0.2,0.02",
            ]
        ),
        encoding="utf-8",
    )
    row = {
        "run_dir": str(run_root),
        "summary_path": str(run_root / "summary.json"),
        "route_health_path": str(run_root / "analysis" / "route_health" / "route_health.json"),
        "failure_reason": None,
    }

    assert finalize_route_health_artifacts_for_row(row) is True

    assert row["summary_path"] == str(actual_run / "summary.json")
    assert row["actual_run_dir"] == str(actual_run)
    assert row["route_health_path"] == str(actual_run / "analysis" / "route_health" / "route_health.json")
    assert row["apollo_channel_health_path"] == str(
        actual_run / "analysis" / "apollo_channel_health" / "apollo_channel_health_report.json"
    )
    assert row["control_health_path"] == str(
        actual_run / "analysis" / "control_health" / "control_health_report.json"
    )
    assert Path(row["route_health_path"]).is_file()
    assert Path(row["apollo_channel_health_path"]).is_file()
    assert Path(row["control_health_path"]).is_file()


def test_finalize_writes_insufficient_channel_health_when_stats_are_missing(tmp_path: Path) -> None:
    run_root = tmp_path / "ab" / "ros2_gt" / "lane217" / "30s" / "batch__ros2_gt__lane217__30s"
    actual_run = run_root / "lane_keep_seed" / "lane_keep_seed__02"
    actual_run.mkdir(parents=True)
    (actual_run / "manifest.json").write_text("{}", encoding="utf-8")
    (actual_run / "summary.json").write_text("{}", encoding="utf-8")
    (actual_run / "timeseries.csv").write_text(
        "\n".join(
            [
                (
                    "frame_id,sim_time,route_id,nearest_route_index,route_s,route_x,route_y,"
                    "route_z,route_heading,route_curvature,ego_x,ego_y,ego_heading,"
                    "cross_track_error,heading_error"
                ),
                "0,0.00,town01_fixture,0,0.0,0.0,0.0,0.0,0.0,0.0,0.1,0.0,0.0,0.1,0.01",
                "1,0.05,town01_fixture,1,1.0,1.0,0.0,0.0,0.0,0.01,1.0,0.2,0.0,0.2,0.02",
            ]
        ),
        encoding="utf-8",
    )
    row = {
        "run_dir": str(run_root),
        "route_id": "lane217",
        "summary_path": str(run_root / "summary.json"),
        "route_health_path": str(run_root / "analysis" / "route_health" / "route_health.json"),
        "apollo_channel_health_path": str(
            run_root / "analysis" / "apollo_channel_health" / "apollo_channel_health_report.json"
        ),
        "control_health_path": str(run_root / "analysis" / "control_health" / "control_health_report.json"),
        "failure_reason": None,
    }

    assert finalize_route_health_artifacts_for_row(row) is True

    report = json.loads(Path(row["apollo_channel_health_path"]).read_text(encoding="utf-8"))
    assert report["status"] == "insufficient_data"
    assert report["missing_inputs"] == ["channel_stats"]
    assert row["apollo_channel_health_error"] == "channel_stats missing"
    assert row["failure_reason"] == "artifact_missing"


def test_execute_matrix_records_return_code_and_skips_remaining(tmp_path: Path, monkeypatch, capsys) -> None:
    cfg = ABRunnerConfig(
        route_config=ROUTE_CONFIG,
        durations_s=(30.0,),
        baseline_backend="ros2_gt",
        candidate_backend="carla_direct",
        out_dir=tmp_path / "ab",
        selected_routes=("097",),
        continue_on_failure=False,
        dry_run=False,
        batch_id="execute_fail",
    )

    class Result:
        returncode = 3

    def fake_run(command: str, *, shell: bool, check: bool, stdout, stderr, text: bool) -> Result:
        assert shell is True
        assert check is False
        assert text is True
        stdout.write("fake stdout\n")
        stderr.write("fake stderr\n")
        parts = shlex.split(command)
        run_root = Path(parts[parts.index("--batch-root-parent") + 1])
        actual_run = run_root / "lane_keep_seed" / "lane_keep_seed__02"
        actual_run.mkdir(parents=True)
        (actual_run / "summary.json").write_text("{}", encoding="utf-8")
        (actual_run / "timeseries.csv").write_text(
            "\n".join(
                [
                    "frame_id,sim_time,route_id,nearest_route_index,route_s,route_x,route_y,route_z,route_heading,route_curvature,ego_x,ego_y,ego_heading,cross_track_error,heading_error",
                    "0,0.00,town01_fixture,0,0.0,0.0,0.0,0.0,0.0,0.0,0.1,0.0,0.0,0.1,0.01",
                    "1,0.05,town01_fixture,1,1.0,1.0,0.0,0.0,0.0,0.01,1.0,0.2,0.0,0.2,0.02",
                ]
            ),
            encoding="utf-8",
        )
        return Result()

    monkeypatch.setattr("carla_testbed.experiments.ab_runner.subprocess.run", fake_run)

    matrix, manifest = execute_matrix(cfg)

    assert matrix[0]["status"] == "failed"
    assert matrix[0]["return_code"] == 3
    assert Path(matrix[0]["command_stdout_path"]).read_text(encoding="utf-8") == "fake stdout\n"
    assert Path(matrix[0]["command_stderr_path"]).read_text(encoding="utf-8") == "fake stderr\n"
    assert matrix[0]["actual_run_dir"]
    assert matrix[0]["route_health_path"].endswith("analysis/route_health/route_health.json")
    assert matrix[0]["apollo_channel_health_path"].endswith(
        "analysis/apollo_channel_health/apollo_channel_health_report.json"
    )
    assert matrix[0]["control_health_path"].endswith("analysis/control_health/control_health_report.json")
    assert matrix[1]["status"] == "skipped"
    assert matrix[1]["failure_reason"] == "stopped_after_previous_failure"
    assert manifest.runs[0].return_code == 3
    assert manifest.runs[0].actual_run_dir == matrix[0]["actual_run_dir"]
    assert manifest.runs[0].command_stdout_path == matrix[0]["command_stdout_path"]
    assert manifest.runs[0].command_stderr_path == matrix[0]["command_stderr_path"]
    assert manifest.runs[0].apollo_channel_health_path == matrix[0]["apollo_channel_health_path"]
    assert manifest.runs[0].control_health_path == matrix[0]["control_health_path"]
    assert manifest.runs[1].status == "skipped"

    stderr = capsys.readouterr().err
    assert "[town01_direct_ab] running execute_fail__ros2_gt__lane097__30s" in stderr
    assert "completed execute_fail__ros2_gt__lane097__30s" in stderr
    assert "status=failed return_code=3" in stderr
    assert "completed execute_fail__carla_direct__lane097__30s" in stderr
    assert "status=skipped" in stderr
    assert "command_stdout.log" in stderr
    assert "command_stderr.log" in stderr
