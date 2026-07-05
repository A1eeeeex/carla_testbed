from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import carla_testbed.experiments.natural_driving_runner as natural_runner_module
from carla_testbed.experiments.natural_driving_runner import (
    NaturalDrivingRunnerConfig,
    STRICT_POSTPROCESS_FAIL_STATUSES,
    audit_suite_goal_outputs,
    build_run_matrix,
    build_suite_manifest,
    execute_suite,
    goal_audit_command,
    index_run_artifacts_for_row,
    parse_class_filter,
    parse_scenario_filter,
    postprocess_and_audit_command,
    postprocess_suite_outputs,
    read_run_matrix_csv,
    refresh_suite_artifact_index,
    summarize_suite_coverage,
    write_dry_run_outputs,
)

SUITE_PATH = Path("configs/scenarios/town01_natural_driving_suite.yaml")
SCRIPT = Path("tools/run_town01_natural_driving_suite.py")


def _write_minimal_online_artifacts(stdout_path: Path, run_leaf: str, *, run_id: str = "nested_run") -> None:
    actual = stdout_path.parent / run_leaf
    actual.mkdir(parents=True, exist_ok=True)
    (actual / "summary.json").write_text(json.dumps({"run_id": run_id}) + "\n", encoding="utf-8")
    (actual / "manifest.json").write_text("{}\n", encoding="utf-8")
    (actual / "timeseries.csv").write_text("frame_id\n1\n", encoding="utf-8")


def _config(
    tmp_path: Path,
    *,
    classes: tuple[str, ...] = (),
    dry_run: bool = True,
) -> NaturalDrivingRunnerConfig:
    return NaturalDrivingRunnerConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "natural",
        dry_run=dry_run,
        continue_on_failure=True,
        scenario_classes=classes,
        batch_id="test_natural",
    )


def test_parse_class_filter() -> None:
    assert parse_class_filter(None) == ()
    assert parse_class_filter("lane_keep,traffic_light_red_stop") == (
        "lane_keep",
        "traffic_light_red_stop",
    )
    assert parse_scenario_filter("lane_keep_097,junction_turn_031") == (
        "lane_keep_097",
        "junction_turn_031",
    )


def test_dry_run_matrix_includes_all_suite_scenarios(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path))

    assert len(rows) == 8
    assert {"lane_keep", "junction_turn", "traffic_light_red_stop"}.issubset(
        {row["scenario_class"] for row in rows}
    )
    assert all(row["status"] == "dry_run" for row in rows)
    assert all(row["return_code"] is None for row in rows)
    assert all(row["algorithm_variant_id"] == "apollo_10_0_carla_gt_town01_reference" for row in rows)
    assert all(
        row["algorithm_variant_manifest_path"] == "configs/algorithms/apollo_variant.carla_gt.example.yaml"
        for row in rows
    )
    assert all(
        row["online_config_path"]
        == "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
        for row in rows
    )
    assert all(
        row["online_config_profile_name"]
        == "town01_apollo_route_health_behavior_recovery_stitcher_v1"
        for row in rows
    )
    assert all(row["transport_mode"] == "ros2_gt" for row in rows)
    assert all(
        row["transport_mode_source"] == "online_config.scenario.publish_ros2_gt"
        for row in rows
    )
    assert all(row["backend"] == "apollo_cyberrt" for row in rows)
    assert all(row["truth_input"] is True for row in rows)
    assert all(row["fixed_delta_seconds"] == 0.05 for row in rows)
    assert all(
        "tools/run_town01_capability_online_chain.py" in row["command"]
        for row in rows
        if row["runnable"]
    )
    assert all(
        row["command"].startswith("UNRUNNABLE_PLACEHOLDER_ROUTE")
        for row in rows
        if not row["runnable"]
    )


def test_scenario_class_filter_selects_subset(tmp_path: Path) -> None:
    rows = build_run_matrix(
        _config(tmp_path, classes=("lane_keep", "traffic_light_red_stop"))
    )

    assert len(rows) == 3
    assert {row["scenario_class"] for row in rows} == {
        "lane_keep",
        "traffic_light_red_stop",
    }


def test_scenario_id_filter_selects_targeted_canary(tmp_path: Path) -> None:
    config = NaturalDrivingRunnerConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "natural",
        dry_run=True,
        continue_on_failure=True,
        scenario_ids=("lane_keep_097",),
        batch_id="test_natural",
    )

    rows = build_run_matrix(config)
    manifest = build_suite_manifest(config, rows)

    assert len(rows) == 1
    assert rows[0]["scenario_id"] == "lane_keep_097"
    assert rows[0]["route_id"] == "lane097"
    assert manifest["scenario_id_filter"] == ["lane_keep_097"]
    assert manifest["coverage"]["by_scenario_class"]["lane_keep"]["total"] == 1


def test_run_id_and_run_dir_are_unique(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path))

    assert len({row["run_id"] for row in rows}) == len(rows)
    assert len({row["run_dir"] for row in rows}) == len(rows)
    assert all(row["run_id"] in row["run_dir"] for row in rows)


def test_matrix_declares_required_artifact_paths(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path))

    for row in rows:
        assert "ticks" in row
        assert "runnable" in row
        assert "capability_profile" in row
        assert "route_step" in row
        assert "traffic_light_expectation_json" in row
        assert "algorithm_variant_id" in row
        assert "online_config_path" in row
        assert "online_config_profile_name" in row
        assert "transport_mode" in row
        assert "transport_mode_source" in row
        assert "backend" in row
        assert "truth_input" in row
        assert "fixed_delta_seconds" in row
        assert row["manifest_path"].endswith("/manifest.json")
        assert row["config_resolved_path"].endswith("/config.resolved.yaml")
        assert row["events_path"].endswith("/events.jsonl")
        assert row["timeseries_path"].endswith("/timeseries.csv")
        assert row["route_health_path"].endswith("/analysis/route_health/route_health.json")
        assert row["route_health_csv_path"].endswith("/analysis/route_health/route_health.csv")
        assert row["curve_segments_path"].endswith("/analysis/route_health/curve_segments.csv")
        assert row["route_health_summary_path"].endswith(
            "/analysis/route_health/route_health_summary.md"
        )
        assert row["channel_stats_path"].endswith("/channel_stats.json")
        assert row["apollo_channel_health_path"].endswith(
            "/analysis/apollo_channel_health/apollo_channel_health_report.json"
        )
        assert row["control_health_path"].endswith(
            "/analysis/control_health/control_health_report.json"
        )
        assert row["failure_timeline_path"].endswith(
            "/analysis/failure_timeline/failure_timeline_report.json"
        )
        assert row["route_start_alignment_path"].endswith(
            "/analysis/route_start_alignment/route_start_alignment_report.json"
        )
        assert row["traffic_light_behavior_path"].endswith(
            "/analysis/traffic_light/traffic_light_behavior_report.json"
        )
        assert row["artifact_completeness_path"].endswith(
            "/analysis/artifact_completeness/artifact_completeness_report.json"
        )
        assert row["command_stdout_path"].endswith("/command_stdout.log")
        assert row["command_stderr_path"].endswith("/command_stderr.log")
        if row["scenario_class"].startswith("traffic_light"):
            assert row["traffic_light_contract_path"].endswith(
                "/analysis/traffic_light/traffic_light_contract_report.json"
            )
            expectation = json.loads(row["traffic_light_expectation_json"])
            assert expectation["expected_behavior"]
            assert row["traffic_light_stimulus_mode"] == "deterministic_gt_control"
            assert row["traffic_light_claim_grade"] is True
            overrides = json.loads(row["traffic_light_control_overrides_json"])
            assert "scenario.traffic_lights.control_mode=deterministic_gt_control" in overrides


def test_index_run_artifacts_updates_nested_online_chain_paths(tmp_path: Path) -> None:
    row = build_run_matrix(_config(tmp_path, classes=("lane_keep",)))[0]
    actual = Path(row["run_dir"]) / "lane_keep__adhoc__town01_rh_spawn097_goal046__seed" / "leaf"
    (actual / "analysis" / "route_health").mkdir(parents=True)
    (actual / "analysis" / "apollo_channel_health").mkdir(parents=True)
    (actual / "analysis" / "control_health").mkdir(parents=True)
    (actual / "analysis" / "failure_timeline").mkdir(parents=True)
    (actual / "analysis" / "route_start_alignment").mkdir(parents=True)
    (actual / "analysis" / "traffic_light").mkdir(parents=True)
    (actual / "analysis" / "artifact_completeness").mkdir(parents=True)
    for relative in [
        "manifest.json",
        "summary.json",
        "config.resolved.yaml",
        "events.jsonl",
        "timeseries.csv",
        "channel_stats.json",
        "analysis/route_health/route_health.json",
        "analysis/route_health/route_health.csv",
        "analysis/route_health/curve_segments.csv",
        "analysis/route_health/route_health_summary.md",
        "analysis/apollo_channel_health/apollo_channel_health_report.json",
        "analysis/control_health/control_health_report.json",
        "analysis/failure_timeline/failure_timeline_report.json",
        "analysis/route_start_alignment/route_start_alignment_report.json",
        "analysis/traffic_light/traffic_light_contract_report.json",
        "analysis/traffic_light/traffic_light_behavior_report.json",
        "analysis/artifact_completeness/artifact_completeness_report.json",
    ]:
        (actual / relative).write_text("{}\n", encoding="utf-8")

    assert index_run_artifacts_for_row(row) is True

    assert row["artifact_index_status"] == "found"
    assert row["actual_run_dir"] == str(actual)
    assert row["summary_path"] == str(actual / "summary.json")
    assert row["route_health_path"] == str(actual / "analysis" / "route_health" / "route_health.json")
    assert row["route_health_csv_path"] == str(actual / "analysis" / "route_health" / "route_health.csv")
    assert row["curve_segments_path"] == str(actual / "analysis" / "route_health" / "curve_segments.csv")
    assert row["apollo_channel_health_path"] == str(
        actual / "analysis" / "apollo_channel_health" / "apollo_channel_health_report.json"
    )
    assert row["control_health_path"] == str(
        actual / "analysis" / "control_health" / "control_health_report.json"
    )
    assert row["failure_timeline_path"] == str(
        actual / "analysis" / "failure_timeline" / "failure_timeline_report.json"
    )
    assert row["route_start_alignment_path"] == str(
        actual / "analysis" / "route_start_alignment" / "route_start_alignment_report.json"
    )
    assert row["traffic_light_behavior_path"] == str(
        actual / "analysis" / "traffic_light" / "traffic_light_behavior_report.json"
    )
    assert row["artifact_completeness_path"] == str(
        actual / "analysis" / "artifact_completeness" / "artifact_completeness_report.json"
    )
    manifest = json.loads((actual / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["algorithm_variant_id"] == "apollo_10_0_carla_gt_town01_reference"
    assert manifest["transport_mode"] == "ros2_gt"
    assert (
        manifest["online_config_path"]
        == "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    )
    assert (
        manifest["online_config_profile_name"]
        == "town01_apollo_route_health_behavior_recovery_stitcher_v1"
    )
    assert manifest["transport_mode_source"] == "online_config.scenario.publish_ros2_gt"
    assert manifest["backend"] == "apollo_cyberrt"
    assert manifest["truth_input"] is True


def test_non_dry_run_matrix_is_planned_not_executed(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path, dry_run=False))

    assert {row["status"] for row in rows} == {"planned"}


def test_manifest_records_analysis_command_and_no_default_direct_claim(tmp_path: Path) -> None:
    config = _config(tmp_path)
    rows = build_run_matrix(config)
    manifest = build_suite_manifest(config, rows)

    assert manifest["schema_version"] == "natural_driving_suite_manifest.v1"
    assert manifest["mode"]["algorithm_variant_id"] == "apollo_10_0_carla_gt_town01_reference"
    assert manifest["mode"]["algorithm_variant_manifest_path"] == "configs/algorithms/apollo_variant.carla_gt.example.yaml"
    assert (
        manifest["online_config"]["path"]
        == "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    )
    assert manifest["online_config"]["transport_mode"] == "ros2_gt"
    assert manifest["online_config"]["transport_mode_source"] == "online_config.scenario.publish_ros2_gt"
    assert manifest["mode"]["truth_input"] is True
    assert manifest["run_count"] == len(rows)
    assert "analysis/failure_timeline/failure_timeline_report.json" in manifest["claim_boundary"]["required_evidence"]
    assert (
        "analysis/route_start_alignment/route_start_alignment_report.json"
        in manifest["claim_boundary"]["required_evidence"]
    )
    assert "tools/analyze_town01_natural_driving.py" in manifest["analysis_commands"]["natural_driving"]
    assert "tools/postprocess_town01_natural_driving.py" in manifest["analysis_commands"]["postprocess"]
    assert "--require-full-target-coverage" in manifest["analysis_commands"]["postprocess_strict"]
    assert f"--fail-on-status {STRICT_POSTPROCESS_FAIL_STATUSES}" in manifest["analysis_commands"]["postprocess_strict"]
    assert "tools/audit_town01_goal.py" in manifest["analysis_commands"]["goal_audit_strict"]
    assert "--natural-driving-report" in manifest["analysis_commands"]["goal_audit_strict"]
    assert "--fail-on-status incomplete" in manifest["analysis_commands"]["goal_audit_strict"]
    strict_combo = manifest["analysis_commands"]["postprocess_and_audit_strict"]
    assert "tools/run_town01_natural_driving_suite.py" in strict_combo
    assert "--postprocess-existing" in strict_combo
    assert "--audit-after-postprocess" in strict_combo
    assert "--require-full-target-coverage" in strict_combo
    assert f"--fail-on-postprocess-status {STRICT_POSTPROCESS_FAIL_STATUSES}" in strict_combo
    assert "--fail-on-audit-status incomplete" in strict_combo
    assert "does not set carla_direct as default" in manifest["notes"]
    assert manifest["coverage"]["total"] == len(rows)
    assert manifest["coverage"]["runnable"] == len(rows)
    assert manifest["coverage"]["by_gate_role"]["hard_gate"]["total"] == 3
    assert manifest["coverage"]["by_gate_role"]["diagnostic_gate"]["total"] == 2
    assert manifest["coverage"]["traffic_light"]["total"] == 3
    assert manifest["coverage"]["traffic_light"]["claim_grade"] == 3
    assert manifest["coverage"]["traffic_light"]["deterministic_gt_control"] == 3
    assert manifest["coverage"]["traffic_light"]["non_claim_grade"] == 0
    assert manifest["coverage"]["traffic_light"]["gate_roles"] == ["informational"]
    assert manifest["claim_boundary"]["can_claim_natural_driving_from_manifest"] is False
    assert manifest["claim_boundary"]["postprocess_required_for_claim"] is True
    assert manifest["claim_boundary"]["postprocess_evidence_status"] == "not_run"
    assert manifest["claim_boundary"]["required_report"] == "natural_driving_report.json"


def test_direct_candidate_config_records_candidate_transport_without_default_promotion(
    tmp_path: Path,
) -> None:
    config = NaturalDrivingRunnerConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "natural",
        dry_run=True,
        continue_on_failure=True,
        scenario_ids=("lane_keep_097",),
        batch_id="test_natural_direct",
        route_health_config=(
            "configs/io/examples/"
            "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
        ),
    )

    rows, manifest = write_dry_run_outputs(config)
    row = rows[0]
    run_manifest = json.loads((Path(row["run_dir"]) / "manifest.json").read_text(encoding="utf-8"))

    assert row["transport_mode"] == "carla_direct"
    assert row["transport_mode_source"] == "online_config.algo.apollo.transport_mode"
    assert row["online_config_profile_name"] == (
        "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate"
    )
    assert "--config configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml" in row["command"]
    assert manifest["mode"]["default_backend"] == "ros2_gt"
    assert manifest["online_config"]["transport_mode"] == "carla_direct"
    assert manifest["online_config"]["transport_mode_source"] == "online_config.algo.apollo.transport_mode"
    assert manifest["notes"].find("does not set carla_direct as default") >= 0
    assert run_manifest["transport_mode"] == "carla_direct"
    assert run_manifest["transport_mode_source"] == "online_config.algo.apollo.transport_mode"
    assert run_manifest["online_config_path"].endswith("_direct_candidate.yaml")


def test_goal_audit_command_points_at_postprocess_report(tmp_path: Path) -> None:
    config = _config(tmp_path)
    command = goal_audit_command(config)

    assert command.startswith(config.python_exec)
    assert "tools/audit_town01_goal.py" in command
    assert "--natural-driving-report" in command
    assert str(config.out_dir / "analysis" / "natural_driving" / "natural_driving_report.json") in command
    assert "--fail-on-status incomplete" in command


def test_postprocess_and_audit_command_points_at_suite_runner(tmp_path: Path) -> None:
    config = _config(tmp_path)
    command = postprocess_and_audit_command(config)

    assert command.startswith(config.python_exec)
    assert "tools/run_town01_natural_driving_suite.py" in command
    assert f"--suite {config.suite_path}" in command
    assert f"--out {config.out_dir}" in command
    assert "--postprocess-existing" in command
    assert "--audit-after-postprocess" in command
    assert "--require-full-target-coverage" in command
    assert f"--fail-on-postprocess-status {STRICT_POSTPROCESS_FAIL_STATUSES}" in command
    assert "--fail-on-audit-status incomplete" in command


def test_suite_coverage_records_unrunnable_placeholder_routes(tmp_path: Path) -> None:
    rows = build_run_matrix(_config(tmp_path))
    rows[0]["runnable"] = False
    rows[0]["failure_reason"] = "placeholder_route_ref_not_runnable"

    coverage = summarize_suite_coverage(rows)

    assert coverage["total"] == len(rows)
    assert coverage["unrunnable"] == 1
    assert coverage["by_scenario_class"]["lane_keep"]["unrunnable"] == 1
    assert coverage["by_gate_role"]["hard_gate"]["unrunnable"] == 1
    assert coverage["unrunnable_scenarios"][0]["scenario_id"] == "lane_keep_097"
    assert coverage["unrunnable_scenarios"][0]["failure_reason"] == "placeholder_route_ref_not_runnable"


def test_write_dry_run_outputs_creates_manifest_matrix_and_run_dirs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    rows, manifest = write_dry_run_outputs(config)

    manifest_path = config.out_dir / "suite_manifest.json"
    matrix_path = config.out_dir / "run_matrix.csv"
    assert manifest_path.is_file()
    assert matrix_path.is_file()
    assert manifest["run_count"] == len(rows)
    assert all(Path(row["run_dir"]).is_dir() for row in rows)
    first_run_manifest = json.loads((Path(rows[0]["run_dir"]) / "manifest.json").read_text(encoding="utf-8"))
    assert first_run_manifest["schema_version"] == "natural_driving_run_manifest.v1"
    assert first_run_manifest["algorithm_variant_id"] == "apollo_10_0_carla_gt_town01_reference"
    assert (
        first_run_manifest["algorithm_variant_manifest_path"]
        == "configs/algorithms/apollo_variant.carla_gt.example.yaml"
    )
    assert first_run_manifest["map"] == "Town01"
    assert first_run_manifest["truth_input"] is True
    assert first_run_manifest["duration_s"] == rows[0]["duration_s"]
    assert first_run_manifest["fixed_delta_seconds"] == 0.05
    assert first_run_manifest["ticks"] == rows[0]["ticks"]
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        csv_rows = list(csv.DictReader(handle))
    assert len(csv_rows) == len(rows)
    assert csv_rows[0]["status"] == "dry_run"
    assert "events_path" in csv_rows[0]
    assert "config_resolved_path" in csv_rows[0]
    assert "command_stdout_path" in csv_rows[0]


def test_traffic_light_expectation_is_written_to_matrix_and_run_manifest(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("traffic_light_red_stop",))
    rows, _manifest = write_dry_run_outputs(config)

    assert len(rows) == 1
    row = rows[0]
    expectation = row["traffic_light_expectation"]
    csv_expectation = json.loads(row["traffic_light_expectation_json"])
    run_manifest = json.loads((Path(row["run_dir"]) / "manifest.json").read_text(encoding="utf-8"))

    assert expectation["expected_behavior"] == "red_stop"
    assert row["traffic_light_stimulus_mode"] == "deterministic_gt_control"
    assert row["traffic_light_claim_grade"] is True
    assert csv_expectation["expected_initial_state"] == "RED"
    assert csv_expectation["stimulus_mode"] == "deterministic_gt_control"
    assert run_manifest["traffic_light_expectation"] == expectation
    assert run_manifest["traffic_light_stimulus_mode"] == "deterministic_gt_control"
    assert run_manifest["traffic_light_claim_grade"] is True
    assert "scenario.traffic_lights.initial_state=RED" in run_manifest["traffic_light_control_overrides"]


def test_execute_suite_runs_commands_and_continues_on_failure(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("lane_keep", "junction_turn"), dry_run=False)
    calls: list[str] = []

    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        calls.append(command)
        stdout_path.write_text("stdout\n", encoding="utf-8")
        stderr_path.write_text("stderr\n", encoding="utf-8")
        if "--step lane_keep:town01_rh_spawn097_goal046" in command:
            _write_minimal_online_artifacts(
                stdout_path,
                "lane_keep__adhoc__town01_rh_spawn097_goal046__seed",
                run_id="nested_lane_097",
            )
        elif "--step lane_keep:town01_rh_spawn217_goal046" in command:
            _write_minimal_online_artifacts(
                stdout_path,
                "lane_keep__adhoc__town01_rh_spawn217_goal046__seed",
                run_id="nested_lane_217",
            )
        return 1 if "--step junction_traverse:town01_rh_spawn031_goal056" in command else 0

    rows, manifest = execute_suite(config, command_runner=fake_runner)

    assert len(rows) == 3
    assert len(calls) == 3
    assert {row["status"] for row in rows} == {"success", "failed"}
    failed = next(row for row in rows if row["status"] == "failed")
    assert failed["return_code"] == 1
    assert Path(failed["command_stdout_path"]).is_file()
    assert Path(failed["command_stderr_path"]).is_file()
    assert (config.out_dir / "suite_manifest.json").is_file()
    assert manifest["runs"][-1]["status"] in {"success", "failed"}


def test_execute_suite_persists_progress_before_and_between_online_runs(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("lane_keep",), dry_run=False)
    calls = 0

    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        nonlocal calls
        calls += 1
        persisted_rows = read_run_matrix_csv(config.out_dir / "run_matrix.csv")
        persisted_manifest = json.loads((config.out_dir / "suite_manifest.json").read_text(encoding="utf-8"))

        assert len(persisted_rows) == 2
        assert persisted_manifest["run_count"] == 2
        if calls == 1:
            assert [row["status"] for row in persisted_rows] == ["planned", "planned"]
            assert persisted_manifest["runs"][0]["status"] == "planned"
            _write_minimal_online_artifacts(
                stdout_path,
                "lane_keep__adhoc__town01_rh_spawn097_goal046__seed",
                run_id="nested_lane_097",
            )
        else:
            assert persisted_rows[0]["status"] == "success"
            assert persisted_rows[0]["artifact_index_status"] == "found"
            assert persisted_rows[0]["actual_run_dir"]
            assert persisted_rows[1]["status"] == "planned"
            assert persisted_manifest["runs"][0]["status"] == "success"
            _write_minimal_online_artifacts(
                stdout_path,
                "lane_keep__adhoc__town01_rh_spawn217_goal046__seed",
                run_id="nested_lane_217",
            )
        stdout_path.write_text("stdout\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        return 0

    rows, manifest = execute_suite(config, command_runner=fake_runner)
    persisted_final = read_run_matrix_csv(config.out_dir / "run_matrix.csv")

    assert calls == 2
    assert [row["status"] for row in rows] == ["success", "success"]
    assert [row["status"] for row in persisted_final] == ["success", "success"]
    assert manifest["runs"][1]["status"] == "success"


def test_execute_suite_indexes_nested_artifacts_from_fake_online_run(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("lane_keep",), dry_run=False)

    def fake_runner(_command: str, stdout_path: Path, stderr_path: Path) -> int:
        stdout_path.write_text("stdout\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        _write_minimal_online_artifacts(
            stdout_path,
            "lane_keep__adhoc__town01_rh_spawn097_goal046__seed",
            run_id="nested_lane",
        )
        return 0

    rows, manifest = execute_suite(config, command_runner=fake_runner)

    first = rows[0]
    assert first["status"] == "success"
    assert first["artifact_index_status"] == "found"
    assert first["actual_run_dir"].endswith("lane_keep__adhoc__town01_rh_spawn097_goal046__seed")
    assert first["summary_path"].endswith("summary.json")
    assert manifest["runs"][0]["actual_run_dir"] == first["actual_run_dir"]


def test_refresh_suite_artifact_index_updates_postprocess_outputs(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("traffic_light_red_stop",), dry_run=False)

    def fake_runner(_command: str, stdout_path: Path, stderr_path: Path) -> int:
        stdout_path.write_text("stdout\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        _write_minimal_online_artifacts(
            stdout_path,
            "traffic_light__adhoc__town01_rh_spawn129_goal051__seed",
            run_id="nested_traffic_light",
        )
        return 0

    rows, _manifest = execute_suite(config, command_runner=fake_runner)
    first = rows[0]
    actual = Path(str(first["actual_run_dir"]))
    assert first["route_health_path"] is None
    actual_manifest = json.loads((actual / "manifest.json").read_text(encoding="utf-8"))
    assert actual_manifest["traffic_light_expectation"]["expected_behavior"] == "red_stop"

    for relative in [
        "analysis/route_health/route_health.json",
        "analysis/route_health/route_health.csv",
        "analysis/route_health/curve_segments.csv",
        "analysis/route_health/route_health_summary.md",
        "analysis/apollo_channel_health/apollo_channel_health_report.json",
        "analysis/control_health/control_health_report.json",
        "analysis/failure_timeline/failure_timeline_report.json",
        "analysis/route_start_alignment/route_start_alignment_report.json",
        "analysis/traffic_light/traffic_light_contract_report.json",
        "analysis/traffic_light/traffic_light_behavior_report.json",
    ]:
        path = actual / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")

    refresh = refresh_suite_artifact_index(config)
    refreshed_rows = read_run_matrix_csv(config.out_dir / "run_matrix.csv")
    refreshed = refreshed_rows[0]
    manifest = json.loads((config.out_dir / "suite_manifest.json").read_text(encoding="utf-8"))

    assert refresh["status"] == "refreshed"
    assert refresh["refreshed_rows"] == 1
    assert refreshed["route_health_path"].endswith("analysis/route_health/route_health.json")
    assert refreshed["route_health_csv_path"].endswith("analysis/route_health/route_health.csv")
    assert refreshed["curve_segments_path"].endswith("analysis/route_health/curve_segments.csv")
    assert refreshed["apollo_channel_health_path"].endswith(
        "analysis/apollo_channel_health/apollo_channel_health_report.json"
    )
    assert refreshed["control_health_path"].endswith(
        "analysis/control_health/control_health_report.json"
    )
    assert refreshed["failure_timeline_path"].endswith(
        "analysis/failure_timeline/failure_timeline_report.json"
    )
    assert refreshed["route_start_alignment_path"].endswith(
        "analysis/route_start_alignment/route_start_alignment_report.json"
    )
    assert refreshed["traffic_light_behavior_path"].endswith(
        "analysis/traffic_light/traffic_light_behavior_report.json"
    )
    assert manifest["artifact_index_refresh_status"] == "refreshed"
    assert manifest["runs"][0]["route_health_path"] == refreshed["route_health_path"]


def test_postprocess_suite_outputs_refreshes_index_before_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path, classes=("lane_keep",), dry_run=False)
    calls: list[str] = []

    def fake_refresh(received_config: NaturalDrivingRunnerConfig) -> dict[str, object]:
        assert received_config is config
        calls.append("refresh")
        return {
            "status": "refreshed",
            "call_index": len(calls),
        }

    def fake_postprocess(
        suite_root: Path,
        *,
        out_dir: Path,
        require_full_target_coverage: bool | None,
        refresh: bool,
    ) -> dict[str, object]:
        calls.append("postprocess")
        assert calls == ["refresh", "postprocess"]
        assert suite_root == config.out_dir
        assert out_dir == tmp_path / "postprocess"
        assert require_full_target_coverage is True
        assert refresh is True
        return {
            "natural_driving": {
                "status": "insufficient_data",
                "summary": {},
                "verdict": {},
                "capability_coverage": {},
            },
            "outputs": {},
        }

    monkeypatch.setattr(natural_runner_module, "refresh_suite_artifact_index", fake_refresh)
    monkeypatch.setattr(natural_runner_module, "postprocess_natural_driving_runs", fake_postprocess)

    report = postprocess_suite_outputs(
        config,
        out_dir=tmp_path / "postprocess",
        require_full_target_coverage=True,
        refresh=True,
    )

    assert calls == ["refresh", "postprocess", "refresh"]
    assert report["artifact_index_pre_refresh"]["call_index"] == 1
    assert report["artifact_index_refresh"]["call_index"] == 3
    assert report["suite_manifest_postprocess_update"]["status"] == "suite_manifest_missing"


def test_postprocess_suite_outputs_persists_manifest_evidence_status(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("lane_keep",))
    write_dry_run_outputs(config)

    report = postprocess_suite_outputs(config)
    manifest = json.loads((config.out_dir / "suite_manifest.json").read_text(encoding="utf-8"))

    assert report["suite_manifest_postprocess_update"]["status"] == "updated"
    assert manifest["postprocess_status"] == report["natural_driving"]["status"]
    assert manifest["postprocess_outputs"]["natural_driving_report"].endswith(
        "analysis/natural_driving/natural_driving_report.json"
    )
    evidence = manifest["postprocess_evidence"]
    assert evidence["schema_version"] == "natural_driving_postprocess_evidence.v1"
    assert evidence["status"] == report["natural_driving"]["status"]
    assert evidence["can_claim_full_natural_driving"] is False
    assert evidence["problem_run_count"] == report["natural_driving"]["problem_run_count"]
    assert "runner return_code" in evidence["interpretation_boundary"]
    claim_boundary = manifest["claim_boundary"]
    assert claim_boundary["can_claim_natural_driving_from_manifest"] is False
    assert claim_boundary["postprocess_required_for_claim"] is True
    assert claim_boundary["postprocess_evidence_status"] == report["natural_driving"]["status"]


def test_execute_suite_runs_traffic_light_probe_routes(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("traffic_light_red_stop",), dry_run=False)
    calls: list[str] = []

    def fake_runner(command: str, stdout_path: Path, stderr_path: Path) -> int:
        calls.append(command)
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        _write_minimal_online_artifacts(
            stdout_path,
            "traffic_light__adhoc__town01_rh_spawn129_goal051__seed",
            run_id="nested_traffic_light",
        )
        return 0

    rows, _manifest = execute_suite(config, command_runner=fake_runner)

    assert len(rows) == 1
    assert len(calls) == 1
    assert "--step traffic_light_actual:town01_rh_spawn129_goal051" in calls[0]
    assert rows[0]["status"] == "success"


def test_execute_suite_requires_summary_artifact_for_success(tmp_path: Path) -> None:
    config = _config(tmp_path, classes=("lane_keep",), dry_run=False)

    def fake_runner(_command: str, stdout_path: Path, stderr_path: Path) -> int:
        stdout_path.write_text("stdout\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        return 0

    rows, manifest = execute_suite(config, command_runner=fake_runner)

    assert rows[0]["return_code"] == 0
    assert rows[0]["status"] == "failed"
    assert rows[0]["failure_reason"] == "artifact_missing"
    assert rows[0]["artifact_index_status"] == "summary_missing"
    assert manifest["runs"][0]["status"] == "failed"


def test_execute_suite_stops_after_failure_without_continue(tmp_path: Path) -> None:
    config = NaturalDrivingRunnerConfig(
        suite_path=SUITE_PATH,
        out_dir=tmp_path / "natural",
        dry_run=False,
        continue_on_failure=False,
        scenario_classes=("lane_keep", "traffic_light_red_stop"),
        batch_id="test_natural",
    )

    def fail_first(_command: str, stdout_path: Path, stderr_path: Path) -> int:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("fail\n", encoding="utf-8")
        return 1

    rows, _manifest = execute_suite(config, command_runner=fail_first)

    assert rows[0]["status"] == "failed"
    assert all(row["status"] == "skipped" for row in rows[1:])


def test_postprocess_suite_outputs_for_existing_fixture(tmp_path: Path) -> None:
    import shutil

    fixture = Path("tests/fixtures/natural_driving/simple_suite")
    suite_root = tmp_path / "natural_existing"
    shutil.copytree(fixture, suite_root)
    config = NaturalDrivingRunnerConfig(
        suite_path=SUITE_PATH,
        out_dir=suite_root,
        dry_run=True,
        continue_on_failure=True,
    )

    report = postprocess_suite_outputs(config, out_dir=tmp_path / "postprocess")

    assert report["natural_driving"]["status"] == "pass"
    assert Path(report["outputs"]["natural_driving_postprocess_json"]).is_file()
    assert report["artifact_index_pre_refresh"]["status"] == "no_run_matrix"
    assert report["artifact_index_refresh"]["status"] == "no_run_matrix"


def test_audit_suite_goal_outputs_writes_goal_audit_for_postprocessed_suite(tmp_path: Path) -> None:
    import shutil

    fixture = Path("tests/fixtures/natural_driving/simple_suite")
    suite_root = tmp_path / "natural_existing"
    shutil.copytree(fixture, suite_root)
    config = NaturalDrivingRunnerConfig(
        suite_path=SUITE_PATH,
        out_dir=suite_root,
        dry_run=True,
        continue_on_failure=True,
    )
    postprocess_suite_outputs(config)

    audit_report = audit_suite_goal_outputs(
        config,
        ab_root=tmp_path / "missing_ab",
        calibration_root=tmp_path / "missing_calibration",
        demo_root=tmp_path / "missing_demo",
    )

    assert audit_report["status"] == "incomplete"
    assert Path(audit_report["outputs"]["town01_goal_audit_json"]).is_file()
    assert Path(audit_report["outputs"]["town01_goal_audit_md"]).is_file()
    audit = audit_report["audit"]
    assert audit["sources"]["natural_driving_report"] == str(
        suite_root / "analysis" / "natural_driving" / "natural_driving_report.json"
    )
    assert audit["sections"]["natural_driving"]["status"] == "incomplete"
    assert "natural_driving_report.json" not in audit["missing_evidence"]
    assert any("A/B" in item for item in audit["missing_evidence"])


def test_cli_dry_run_writes_manifest_and_matrix(tmp_path: Path) -> None:
    out = tmp_path / "natural_cli"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--dry-run",
            "--continue-on-failure",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["matrix_rows"] == 8
    assert payload["coverage"]["runnable"] == 8
    assert payload["coverage"]["traffic_light"]["total"] == 3
    assert payload["postprocess_command"].endswith("analysis/natural_driving")
    assert "--require-full-target-coverage" in payload["postprocess_strict_command"]
    assert f"--fail-on-status {STRICT_POSTPROCESS_FAIL_STATUSES}" in payload["postprocess_strict_command"]
    assert "tools/audit_town01_goal.py" in payload["goal_audit_strict_command"]
    assert "natural_driving_report.json" in payload["goal_audit_strict_command"]
    assert "--audit-after-postprocess" in payload["postprocess_and_audit_strict_command"]
    assert "--fail-on-audit-status incomplete" in payload["postprocess_and_audit_strict_command"]
    assert (out / "suite_manifest.json").is_file()
    assert (out / "run_matrix.csv").is_file()


def test_cli_class_filter_writes_subset(tmp_path: Path) -> None:
    out = tmp_path / "natural_filtered"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--dry-run",
            "--classes",
            "lane_keep,traffic_light_red_stop",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["matrix_rows"] == 3
    with (out / "run_matrix.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["scenario_class"] for row in rows} == {
        "lane_keep",
        "traffic_light_red_stop",
    }


def test_cli_scenario_filter_writes_single_canary(tmp_path: Path) -> None:
    out = tmp_path / "natural_canary"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--dry-run",
            "--scenarios",
            "lane_keep_097",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["matrix_rows"] == 1
    with (out / "suite_manifest.json").open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    with (out / "run_matrix.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert manifest["scenario_id_filter"] == ["lane_keep_097"]
    assert len(rows) == 1
    assert rows[0]["scenario_id"] == "lane_keep_097"


def test_cli_python_override_controls_generated_online_commands(tmp_path: Path) -> None:
    out = tmp_path / "natural_python_override"
    custom_python = "/opt/test-python/bin/python3"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--dry-run",
            "--classes",
            "lane_keep",
            "--python",
            custom_python,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["analysis_command"].startswith(custom_python)
    assert payload["postprocess_strict_command"].startswith(custom_python)
    assert payload["goal_audit_strict_command"].startswith(custom_python)
    assert payload["postprocess_and_audit_strict_command"].startswith(custom_python)
    with (out / "run_matrix.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert all(row["command"].startswith(custom_python) for row in rows)


def test_cli_runner_override_is_forwarded_to_online_commands(tmp_path: Path) -> None:
    out = tmp_path / "natural_runner_override"
    override = "scenario.route_health.ego_offset_y_m=0.5089201844"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--dry-run",
            "--scenarios",
            "lane_keep_097",
            "--override",
            override,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    with (out / "suite_manifest.json").open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    with (out / "run_matrix.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert manifest["runner_overrides"] == [override]
    assert rows
    assert json.loads(rows[0]["runner_overrides_json"]) == [override]
    assert f"--override {override}" in rows[0]["command"]


def test_cli_postprocess_existing_writes_outputs(tmp_path: Path) -> None:
    import shutil

    fixture = Path("tests/fixtures/natural_driving/simple_suite")
    out = tmp_path / "natural_existing"
    shutil.copytree(fixture, out)
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--postprocess-existing",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["matrix_rows"] == 0
    assert payload["postprocess_status"] == "pass"
    assert payload["artifact_index_pre_refresh"]["status"] == "no_run_matrix"
    assert payload["artifact_index_refresh"]["status"] == "no_run_matrix"
    assert "suite_plan_missing" in payload["coverage_check"]
    assert "missing_required_scenario_ids" in payload["coverage_check"]
    assert "unproven_required_scenario_ids" in payload["coverage_check"]
    assert "scenario_identity_mismatches" in payload["coverage_check"]
    assert Path(payload["postprocess_outputs"]["natural_driving_postprocess_json"]).is_file()


def test_cli_postprocess_existing_reports_existing_matrix_rows(tmp_path: Path) -> None:
    out = tmp_path / "natural_dry"
    dry_run = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--dry-run",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert dry_run.returncode == 0, dry_run.stderr

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--postprocess-existing",
            "--fail-on-postprocess-status",
            "fail,insufficient_data",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["matrix_rows"] == 8
    assert payload["postprocess_status"] == "insufficient_data"
    assert Path(payload["postprocess_outputs"]["natural_driving_report"]).is_file()
    assert payload["suite_manifest_postprocess_update"]["status"] == "updated"
    assert payload["artifact_index_pre_refresh"]["status"] == "refreshed"
    assert "missing_required_scenario_ids" in payload["coverage_check"]
    assert "goal_audit_strict_command" in payload
    manifest = json.loads((out / "suite_manifest.json").read_text(encoding="utf-8"))
    assert manifest["postprocess_status"] == "insufficient_data"
    assert manifest["postprocess_evidence"]["status"] == "insufficient_data"
    assert manifest["claim_boundary"]["postprocess_evidence_status"] == "insufficient_data"


def test_cli_postprocess_existing_can_write_goal_audit(tmp_path: Path) -> None:
    import shutil

    fixture = Path("tests/fixtures/natural_driving/simple_suite")
    out = tmp_path / "natural_existing_audit"
    shutil.copytree(fixture, out)
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--postprocess-existing",
            "--audit-after-postprocess",
            "--ab-root",
            str(tmp_path / "missing_ab"),
            "--calibration-root",
            str(tmp_path / "missing_calibration"),
            "--demo-root",
            str(tmp_path / "missing_demo"),
            "--fail-on-audit-status",
            "incomplete",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 3
    payload = json.loads(result.stdout)
    assert payload["postprocess_status"] == "pass"
    assert payload["goal_audit_status"] == "incomplete"
    assert any("A/B" in item for item in payload["goal_audit_missing_evidence"])
    assert Path(payload["goal_audit_outputs"]["town01_goal_audit_json"]).is_file()
    assert Path(payload["goal_audit_outputs"]["town01_goal_audit_md"]).is_file()


def test_cli_postprocess_existing_can_gate_on_status(tmp_path: Path) -> None:
    import shutil

    fixture = Path("tests/fixtures/natural_driving/simple_suite")
    out = tmp_path / "natural_existing"
    shutil.copytree(fixture, out)
    (out / "lane_keep_097" / "events.jsonl").unlink()

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--postprocess-existing",
            "--fail-on-postprocess-status",
            "fail,insufficient_data",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["matrix_rows"] == 0
    assert payload["postprocess_status"] == "insufficient_data"
    assert payload["problem_run_count"] >= 1
    assert "lane_keep_097" in payload["insufficient_data_runs"]
    problem = next(run for run in payload["problem_runs"] if run["run_id"] == "lane_keep_097")
    assert problem["failure_reason"] == "missing_required_artifacts"
    assert "events.jsonl" in problem["missing_artifacts"]
    assert payload["artifact_index_pre_refresh"]["status"] == "no_run_matrix"
    assert payload["artifact_index_refresh"]["status"] == "no_run_matrix"


def test_cli_postprocess_existing_can_gate_warn_status(tmp_path: Path) -> None:
    import shutil

    fixture = Path("tests/fixtures/natural_driving/simple_suite")
    out = tmp_path / "natural_warn"
    shutil.copytree(fixture, out)
    contract = out / "traffic_light_red_stop" / "traffic_light_contract_report.json"
    report = json.loads(contract.read_text(encoding="utf-8"))
    report["status"] = "warn"
    report["warnings"] = ["placeholder_signal_mapping"]
    contract.write_text(json.dumps(report), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--suite",
            str(SUITE_PATH),
            "--out",
            str(out),
            "--postprocess-existing",
            "--fail-on-postprocess-status",
            STRICT_POSTPROCESS_FAIL_STATUSES,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["matrix_rows"] == 0
    assert payload["postprocess_status"] == "warn"
    assert payload["problem_run_count"] >= 1
    assert "traffic_light_red_stop" in payload["warning_runs"]
    problem = next(run for run in payload["problem_runs"] if run["run_id"] == "traffic_light_red_stop")
    assert problem["verdict"] == "warn"
    assert problem["failure_reason"] == "upstream_artifact_warn"
    assert payload["artifact_index_pre_refresh"]["status"] == "no_run_matrix"
    assert payload["artifact_index_refresh"]["status"] == "no_run_matrix"
