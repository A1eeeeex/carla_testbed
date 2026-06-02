from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.experiments.followstop_combo_matrix import (
    FollowStopComboConfig,
    build_followstop_combo_matrix,
    build_followstop_combo_manifest,
    estimate_followstop_combo_counts,
    load_followstop_combo_plan,
    validate_followstop_combo_plan,
    write_followstop_combo_outputs,
)

PLAN_PATH = Path("configs/experiments/followstop_combo_matrix.yaml")
SCRIPT = Path("tools/plan_followstop_combo_matrix.py")


def _config(tmp_path: Path, *, include_transport_ab: bool = False) -> FollowStopComboConfig:
    return FollowStopComboConfig(
        plan_path=PLAN_PATH,
        out_dir=tmp_path / "followstop_combo",
        dry_run=True,
        include_transport_ab=include_transport_ab,
        batch_id="test_followstop_combo",
    )


def test_plan_loads_and_validates() -> None:
    plan = load_followstop_combo_plan(PLAN_PATH)
    result = validate_followstop_combo_plan(plan)

    assert result.ok, result.errors
    assert plan["scenario"]["front_target_ahead_m"] == 300.0
    assert plan["scenario"]["target_speed_mps"] == 22.22
    assert plan["fixed_conditions"]["steer_scale"] == 0.25
    assert plan["fixed_conditions"]["physical_mapping_enabled"] is False


def test_core_matrix_has_14_groups(tmp_path: Path) -> None:
    rows = build_followstop_combo_matrix(_config(tmp_path))

    assert len(rows) == 14
    assert sum(1 for row in rows if row["combo_role"] == "baseline_sanity") == 2
    assert sum(1 for row in rows if row["stack"] == "apollo" and row["combo_role"] == "screening") == 6
    assert sum(1 for row in rows if row["stack"] == "autoware" and row["combo_role"] == "screening") == 6
    assert {row["transport_mode"] for row in rows if row["stack"] == "apollo"} == {"ros2_gt"}
    assert len({row["run_id"] for row in rows}) == len(rows)
    assert len({row["run_dir"] for row in rows}) == len(rows)


def test_transport_ab_expands_to_20_groups(tmp_path: Path) -> None:
    rows = build_followstop_combo_matrix(_config(tmp_path, include_transport_ab=True))

    assert len(rows) == 20
    assert sum(1 for row in rows if row["stack"] == "apollo" and row["combo_role"] == "screening") == 12
    assert {"ros2_gt", "carla_direct"} == {
        row["transport_mode"]
        for row in rows
        if row["stack"] == "apollo" and row["combo_role"] == "screening"
    }
    direct_rows = [row for row in rows if row["transport_mode"] == "carla_direct"]
    assert direct_rows
    assert all("algo.apollo.transport_mode=carla_direct" in row["command"] for row in direct_rows)


def test_count_estimate_includes_confirmation_runs() -> None:
    plan = load_followstop_combo_plan(PLAN_PATH)

    core = estimate_followstop_combo_counts(plan, include_transport_ab=False)
    expanded = estimate_followstop_combo_counts(plan, include_transport_ab=True)

    assert core["first_round_groups"] == 14
    assert core["confirmation_runs"] == 12
    assert core["estimated_total_runs_after_confirmation"] == 26
    assert expanded["first_round_groups"] == 20
    assert expanded["estimated_total_runs_after_confirmation"] == 32


def test_commands_keep_fixed_followstop_conditions(tmp_path: Path) -> None:
    rows = build_followstop_combo_matrix(_config(tmp_path))

    assert all("--follow-spectator" in row["command"] for row in rows)
    assert all("scenario.front_target_ahead_m=300.0" in row["command"] for row in rows)
    assert all("run.ticks=1800" in row["command"] for row in rows)
    apollo_rows = [row for row in rows if row["stack"] == "apollo"]
    assert all("algo.apollo.control_mapping.steer_scale=0.25" in row["command"] for row in apollo_rows)
    assert all("actuator_mapping_mode=legacy" in row["command"] for row in apollo_rows)
    assert not any("physical_mapping_enabled=true" in row["command"] for row in rows)


def test_online_commands_can_start_carla_explicitly(tmp_path: Path) -> None:
    config = FollowStopComboConfig(
        plan_path=PLAN_PATH,
        out_dir=tmp_path / "followstop_combo",
        dry_run=True,
        batch_id="test_followstop_combo",
        start_carla=True,
        carla_root="/home/ubuntu/CARLA_0.9.16",
    )
    rows = build_followstop_combo_matrix(config)

    assert all("--start-carla" in row["command"] for row in rows)
    assert all("--carla-root /home/ubuntu/CARLA_0.9.16" in row["command"] for row in rows)


def test_autoware_matrix_uses_local_launch_args(tmp_path: Path) -> None:
    rows = build_followstop_combo_matrix(_config(tmp_path))
    analytical = [
        row
        for row in rows
        if row["stack"] == "autoware"
        and row["autoware_control_mode"] == "pure_pursuit_pid"
        and row["velocity_smoother"] == "Analytical"
    ]

    assert len(analytical) == 1
    command = analytical[0]["command"]
    assert "lateral_controller_mode:=pure_pursuit" in command
    assert "velocity_smoother_type:=Analytical" in command
    assert "algo.autoware.planning_common_max_vel_mps=22.22" in command


def test_manifest_and_csv_are_written(tmp_path: Path) -> None:
    config = _config(tmp_path, include_transport_ab=True)
    rows, manifest = write_followstop_combo_outputs(config)

    manifest_path = config.out_dir / "followstop_combo_manifest.json"
    matrix_path = config.out_dir / "followstop_combo_matrix.csv"
    assert manifest_path.is_file()
    assert matrix_path.is_file()
    assert manifest["expected_counts"]["first_round_groups"] == 20
    assert manifest["claim_boundary"]["carla_direct_not_default"] is True
    assert len(rows) == 20
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        csv_rows = list(csv.DictReader(handle))
    assert len(csv_rows) == 20
    assert csv_rows[0]["status"] == "dry_run"


def test_manifest_marks_apollo_internal_variants_as_patch_required(tmp_path: Path) -> None:
    rows = build_followstop_combo_matrix(_config(tmp_path))
    manifest = build_followstop_combo_manifest(_config(tmp_path), rows)
    apollo_baseline = next(row for row in manifest["runs"] if row["group_id"] == "apollo_baseline_current")
    screening_apollo = [
        row for row in manifest["runs"] if row["stack"] == "apollo" and row["combo_role"] == "screening"
    ]

    assert apollo_baseline["runnable"] is True
    assert apollo_baseline["claim_boundary"] == "screening_run_only_not_capability_claim"
    assert screening_apollo
    assert all(row["runnable"] is True for row in screening_apollo)
    assert all(row["runtime_wiring_status"] == "config_override" for row in screening_apollo)
    assert all("algo.apollo.planning.smoother=" in row["command"] for row in screening_apollo)
    assert all("algo.apollo.control_pipeline.mode=" in row["command"] for row in screening_apollo)


def test_cli_writes_dry_run_outputs(tmp_path: Path) -> None:
    out = tmp_path / "combo_cli"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--plan",
            str(PLAN_PATH),
            "--out",
            str(out),
            "--include-transport-ab",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["matrix_rows"] == 20
    assert payload["expected_counts"]["estimated_total_runs_after_confirmation"] == 32
    assert Path(payload["manifest"]).is_file()
    assert Path(payload["matrix"]).is_file()
