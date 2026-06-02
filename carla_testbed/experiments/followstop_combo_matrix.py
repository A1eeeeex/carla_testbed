from __future__ import annotations

import csv
import json
import shlex
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

SCHEMA_VERSION = "followstop_combo_plan.v1"
MANIFEST_SCHEMA_VERSION = "followstop_combo_manifest.v1"

MATRIX_FIELDS = [
    "batch_id",
    "run_id",
    "group_id",
    "combo_role",
    "stack",
    "backend",
    "transport_mode",
    "planning_smoother",
    "apollo_control_mode",
    "autoware_control_mode",
    "velocity_smoother",
    "speed_policy",
    "map",
    "ego_idx",
    "front_idx",
    "front_target_ahead_m",
    "target_speed_mps",
    "duration_s",
    "fixed_delta_seconds",
    "ticks",
    "config_path",
    "run_dir",
    "status",
    "runnable",
    "runtime_wiring_status",
    "requires_local_carla",
    "requires_local_apollo",
    "requires_local_autoware",
    "claim_boundary",
    "command",
]

APOLLO_TRANSPORT_OVERRIDES = {
    "ros2_gt": [
        "scenario.publish_ros2_gt=true",
    ],
    "carla_direct": [
        "scenario.publish_ros2_gt=false",
        "algo.apollo.transport_mode=carla_direct",
        "algo.apollo.direct_bridge.require_no_ros2_runtime=true",
        "algo.apollo.direct_bridge.control_apply_mode=frame_flush_only",
        "algo.apollo.direct_bridge.stale_world_frame_policy=always_republish",
    ],
}


@dataclass(frozen=True)
class FollowStopComboConfig:
    plan_path: Path
    out_dir: Path
    dry_run: bool = True
    include_transport_ab: bool = False
    include_confirmation_plan: bool = True
    batch_id: str | None = None
    python_exec: str = sys.executable
    runner_script: str = "examples/run_followstop.py"
    start_carla: bool = False
    carla_root: str | None = None


@dataclass(frozen=True)
class FollowStopComboValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": list(self.errors), "warnings": list(self.warnings)}


def load_followstop_combo_plan(path: str | Path) -> dict[str, Any]:
    plan_path = Path(path)
    payload = yaml.safe_load(plan_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"follow-stop combo plan must be a mapping: {plan_path}")
    return payload


def validate_followstop_combo_plan(plan: Mapping[str, Any]) -> FollowStopComboValidation:
    errors: list[str] = []
    warnings: list[str] = []
    if plan.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")

    scenario = _as_mapping(plan.get("scenario"))
    if scenario.get("map") != "Town01":
        errors.append("scenario.map must be Town01")
    if _num(scenario.get("front_target_ahead_m")) != 300.0:
        errors.append("scenario.front_target_ahead_m must be 300.0")
    if round(_num(scenario.get("target_speed_mps")) or 0.0, 2) != 22.22:
        errors.append("scenario.target_speed_mps must be 22.22 for the 80 kph probe")

    fixed = _as_mapping(plan.get("fixed_conditions"))
    if fixed.get("actuator_mapping_mode") != "legacy":
        errors.append("fixed_conditions.actuator_mapping_mode must stay legacy")
    if _num(fixed.get("steer_scale")) != 0.25:
        errors.append("fixed_conditions.steer_scale must stay 0.25")
    if fixed.get("physical_mapping_enabled") is not False:
        errors.append("fixed_conditions.physical_mapping_enabled must be false")

    apollo = _as_mapping(plan.get("apollo"))
    apollo_smoothers = _list_of_mappings(apollo.get("planning_smoothers"))
    apollo_controls = _list_of_mappings(apollo.get("control_modes"))
    if len(apollo_smoothers) != 3:
        errors.append("apollo.planning_smoothers must define exactly 3 first-round options")
    if len(apollo_controls) != 2:
        errors.append("apollo.control_modes must define exactly 2 first-round options")
    if not apollo.get("baseline_config"):
        errors.append("apollo.baseline_config is required")

    autoware = _as_mapping(plan.get("autoware"))
    autoware_controls = _list_of_mappings(autoware.get("control_modes"))
    velocity_smoothers = _list_of_mappings(autoware.get("velocity_smoothers"))
    if len(autoware_controls) != 3:
        errors.append("autoware.control_modes must define exactly 3 first-round options")
    if len(velocity_smoothers) != 2:
        errors.append("autoware.velocity_smoothers must define exactly 2 first-round options")
    if not autoware.get("baseline_config"):
        errors.append("autoware.baseline_config is required")

    if "Linf" in {str(item.get("id")) for item in velocity_smoothers}:
        warnings.append("Autoware Linf velocity smoother is marked unstable and should stay out of first round")
    return FollowStopComboValidation(tuple(errors), tuple(warnings))


def build_followstop_combo_matrix(config: FollowStopComboConfig) -> list[dict[str, Any]]:
    plan = load_followstop_combo_plan(config.plan_path)
    validation = validate_followstop_combo_plan(plan)
    if not validation.ok:
        raise ValueError("; ".join(validation.errors))
    batch_id = config.batch_id or config.out_dir.name or _batch_id()
    scenario = _as_mapping(plan.get("scenario"))
    fixed_delta = float(scenario.get("fixed_delta_seconds") or 0.05)
    duration_s = float(scenario.get("duration_s") or 90.0)
    ticks = int(scenario.get("ticks") or round(duration_s / fixed_delta))
    rows: list[dict[str, Any]] = []

    apollo = _as_mapping(plan.get("apollo"))
    autoware = _as_mapping(plan.get("autoware"))
    rows.append(
        _row(
            config,
            batch_id=batch_id,
            plan=plan,
            combo_role="baseline_sanity",
            stack="apollo",
            group_id="apollo_baseline_current",
            config_path=str(apollo["baseline_config"]),
            backend="apollo",
            transport_mode=str(apollo.get("primary_transport") or "ros2_gt"),
            runtime_wiring_status="current_config",
            ticks=ticks,
        )
    )
    rows.append(
        _row(
            config,
            batch_id=batch_id,
            plan=plan,
            combo_role="baseline_sanity",
            stack="autoware",
            group_id="autoware_baseline_current",
            config_path=str(autoware["baseline_config"]),
            backend="autoware",
            transport_mode="autoware_direct",
            runtime_wiring_status="current_config",
            ticks=ticks,
        )
    )

    transports = [str(apollo.get("primary_transport") or "ros2_gt")]
    if config.include_transport_ab:
        optional = str(apollo.get("optional_transport") or "carla_direct")
        if optional not in transports:
            transports.append(optional)
    for transport in transports:
        for smoother in _list_of_mappings(apollo.get("planning_smoothers")):
            for control in _list_of_mappings(apollo.get("control_modes")):
                group_id = f"apollo__{transport}__{smoother['id']}__{control['id']}"
                rows.append(
                    _row(
                        config,
                        batch_id=batch_id,
                        plan=plan,
                        combo_role="screening",
                        stack="apollo",
                        group_id=group_id,
                        config_path=str(apollo["baseline_config"]),
                        backend="apollo",
                        transport_mode=transport,
                        planning_smoother=str(smoother["id"]),
                        apollo_control_mode=str(control["id"]),
                        runtime_wiring_status="config_override",
                        ticks=ticks,
                    )
                )

    for control in _list_of_mappings(autoware.get("control_modes")):
        for smoother in _list_of_mappings(autoware.get("velocity_smoothers")):
            group_id = f"autoware__{control['id']}__{smoother['id']}"
            rows.append(
                _row(
                    config,
                    batch_id=batch_id,
                    plan=plan,
                    combo_role="screening",
                    stack="autoware",
                    group_id=group_id,
                    config_path=str(autoware["baseline_config"]),
                    backend="autoware",
                    transport_mode="autoware_direct",
                    autoware_control_mode=str(control["id"]),
                    velocity_smoother=str(smoother["id"]),
                    runtime_wiring_status="launch_arg_overrides",
                    ticks=ticks,
                )
            )
    return rows


def build_followstop_combo_manifest(
    config: FollowStopComboConfig,
    matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    plan = load_followstop_combo_plan(config.plan_path)
    validation = validate_followstop_combo_plan(plan)
    counts = estimate_followstop_combo_counts(plan, include_transport_ab=config.include_transport_ab)
    batch_id = config.batch_id or config.out_dir.name or _batch_id()
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "batch_id": batch_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "plan_path": str(config.plan_path),
        "dry_run": config.dry_run,
        "start_carla": config.start_carla,
        "carla_root": config.carla_root,
        "include_transport_ab": config.include_transport_ab,
        "matrix_rows": len(matrix),
        "expected_counts": counts,
        "validation": validation.to_dict(),
        "scenario": dict(_as_mapping(plan.get("scenario"))),
        "fixed_conditions": dict(_as_mapping(plan.get("fixed_conditions"))),
        "required_artifacts": list(plan.get("required_artifacts") or []),
        "acceptance_metrics": list(plan.get("acceptance_metrics") or []),
        "claim_boundary": {
            "screening_only": True,
            "does_not_prove_natural_driving": True,
            "does_not_change_steer_scale": True,
            "does_not_enable_physical_mapping": True,
            "carla_direct_not_default": True,
            "same_spawn_required_for_stack_comparison": True,
            "confirmation_runs_required_for_candidates": True,
        },
        "runs": [dict(row) for row in matrix],
        "analysis_notes": (
            "One matrix row is one configuration group. Confirmation repeats are counted "
            "in expected_counts but are selected only after screening artifacts identify top candidates."
        ),
    }


def estimate_followstop_combo_counts(plan: Mapping[str, Any], *, include_transport_ab: bool = False) -> dict[str, Any]:
    apollo = _as_mapping(plan.get("apollo"))
    autoware = _as_mapping(plan.get("autoware"))
    apollo_transports = 2 if include_transport_ab else 1
    apollo_screening = (
        len(_list_of_mappings(apollo.get("planning_smoothers")))
        * len(_list_of_mappings(apollo.get("control_modes")))
        * apollo_transports
    )
    autoware_screening = (
        len(_list_of_mappings(autoware.get("control_modes")))
        * len(_list_of_mappings(autoware.get("velocity_smoothers")))
    )
    baseline = 2
    confirmation = _as_mapping(plan.get("confirmation"))
    top_per_stack = int(confirmation.get("top_candidates_per_stack") or 2)
    repeats = int(confirmation.get("repeats_per_candidate") or 3)
    stack_count = 2
    confirmation_runs = top_per_stack * repeats * stack_count
    first_round = baseline + apollo_screening + autoware_screening
    return {
        "baseline_sanity": baseline,
        "apollo_screening": apollo_screening,
        "autoware_screening": autoware_screening,
        "first_round_groups": first_round,
        "confirmation_runs": confirmation_runs,
        "estimated_total_runs_after_confirmation": first_round + confirmation_runs,
        "apollo_transport_ab_included": include_transport_ab,
    }


def write_followstop_combo_outputs(
    config: FollowStopComboConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    matrix = build_followstop_combo_matrix(config)
    manifest = build_followstop_combo_manifest(config, matrix)
    config.out_dir.mkdir(parents=True, exist_ok=True)
    write_followstop_combo_matrix_csv(config.out_dir / "followstop_combo_matrix.csv", matrix)
    (config.out_dir / "followstop_combo_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return matrix, manifest


def write_followstop_combo_matrix_csv(path: str | Path, rows: Sequence[Mapping[str, Any]]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MATRIX_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in MATRIX_FIELDS})


def _row(
    config: FollowStopComboConfig,
    *,
    batch_id: str,
    plan: Mapping[str, Any],
    combo_role: str,
    stack: str,
    group_id: str,
    config_path: str,
    backend: str,
    transport_mode: str,
    runtime_wiring_status: str,
    ticks: int,
    planning_smoother: str | None = None,
    apollo_control_mode: str | None = None,
    autoware_control_mode: str | None = None,
    velocity_smoother: str | None = None,
) -> dict[str, Any]:
    scenario = _as_mapping(plan.get("scenario"))
    fixed_delta = float(scenario.get("fixed_delta_seconds") or 0.05)
    duration_s = float(scenario.get("duration_s") or (ticks * fixed_delta))
    run_id = f"{batch_id}__{group_id}"
    run_dir = config.out_dir / stack / group_id / run_id
    runnable = runtime_wiring_status in {"current_config", "launch_arg_overrides"}
    if stack == "apollo" and combo_role == "screening":
        runnable = runtime_wiring_status == "config_override"
    command = _command_for_row(
        config,
        plan=plan,
        stack=stack,
        config_path=config_path,
        run_dir=run_dir,
        transport_mode=transport_mode,
        ticks=ticks,
        planning_smoother=planning_smoother,
        apollo_control_mode=apollo_control_mode,
        autoware_control_mode=autoware_control_mode,
        velocity_smoother=velocity_smoother,
    )
    return {
        "batch_id": batch_id,
        "run_id": run_id,
        "group_id": group_id,
        "combo_role": combo_role,
        "stack": stack,
        "backend": backend,
        "transport_mode": transport_mode,
        "planning_smoother": planning_smoother,
        "apollo_control_mode": apollo_control_mode,
        "autoware_control_mode": autoware_control_mode,
        "velocity_smoother": velocity_smoother,
        "speed_policy": "target_80kph",
        "map": scenario.get("map"),
        "ego_idx": scenario.get("ego_idx"),
        "front_idx": scenario.get("front_idx"),
        "front_target_ahead_m": scenario.get("front_target_ahead_m"),
        "target_speed_mps": scenario.get("target_speed_mps"),
        "duration_s": duration_s,
        "fixed_delta_seconds": fixed_delta,
        "ticks": ticks,
        "config_path": config_path,
        "run_dir": str(run_dir),
        "status": "dry_run" if config.dry_run else "planned",
        "runnable": runnable,
        "runtime_wiring_status": runtime_wiring_status,
        "requires_local_carla": True,
        "requires_local_apollo": stack == "apollo",
        "requires_local_autoware": stack == "autoware",
        "claim_boundary": _claim_boundary(stack, runtime_wiring_status),
        "command": command if runnable else f"NOT_RUNNABLE_YET {command}",
    }


def _command_for_row(
    config: FollowStopComboConfig,
    *,
    plan: Mapping[str, Any],
    stack: str,
    config_path: str,
    run_dir: Path,
    transport_mode: str,
    ticks: int,
    planning_smoother: str | None,
    apollo_control_mode: str | None,
    autoware_control_mode: str | None,
    velocity_smoother: str | None,
) -> str:
    overrides = _common_overrides(plan, ticks)
    if stack == "apollo":
        overrides.extend(_apollo_overrides(plan, transport_mode, planning_smoother, apollo_control_mode))
    elif stack == "autoware":
        overrides.extend(_autoware_overrides(plan, autoware_control_mode, velocity_smoother))
    parts = [
        str(config.python_exec),
        str(config.runner_script),
        "--config",
        config_path,
        "--run-dir",
        str(run_dir),
        "--follow-spectator",
    ]
    if config.start_carla:
        parts.append("--start-carla")
    if config.carla_root:
        parts.extend(["--carla-root", config.carla_root])
    for override in overrides:
        parts.extend(["--override", override])
    return " ".join(shlex.quote(str(part)) for part in parts)


def _common_overrides(plan: Mapping[str, Any], ticks: int) -> list[str]:
    scenario = _as_mapping(plan.get("scenario"))
    return [
        f"run.map={scenario.get('map', 'Town01')}",
        f"run.ticks={ticks}",
        f"scenario.ego_idx={scenario.get('ego_idx')}",
        f"scenario.front_idx={scenario.get('front_idx')}",
        "scenario.auto_align_front_spawn=false",
        "scenario.require_aligned_front_spawn=true",
        f"scenario.front_target_ahead_m={scenario.get('front_target_ahead_m')}",
        "scenario.front_min_ahead_m=20.0",
        "scenario.front_max_ahead_m=380.0",
        "scenario.front_max_lateral_m=4.0",
        "run.fail_strategy=log_and_continue",
    ]


def _apollo_overrides(
    plan: Mapping[str, Any],
    transport_mode: str,
    planning_smoother: str | None,
    control_mode: str | None,
) -> list[str]:
    scenario = _as_mapping(plan.get("scenario"))
    target = float(scenario.get("target_speed_mps") or 22.22)
    overrides = list(APOLLO_TRANSPORT_OVERRIDES.get(transport_mode, []))
    overrides.extend(
        [
            f"algo.apollo.routing.target_speed_mps={target}",
            f"algo.apollo.routing.scenario_goal_ahead_m={scenario.get('route_goal_ahead_m', 330.0)}",
            "algo.apollo.routing.scenario_goal_force_beyond_front=true",
            "algo.apollo.routing.scenario_goal_min_front_margin_m=20.0",
            f"algo.apollo.planning.default_cruise_speed_mps={target}",
            "algo.apollo.planning.speed_bounds_decider_lowest_speed_mps=23.61",
            "algo.apollo.control_mapping.steer_scale=0.25",
            "algo.apollo.control_mapping.actuator_mapping_mode=legacy",
        ]
    )
    if planning_smoother:
        overrides.append(f"algo.apollo.planning.smoother={planning_smoother}")
    if control_mode:
        overrides.append(f"algo.apollo.control_pipeline.mode={control_mode}")
    return overrides


def _autoware_overrides(
    plan: Mapping[str, Any],
    control_mode: str | None,
    velocity_smoother: str | None,
) -> list[str]:
    scenario = _as_mapping(plan.get("scenario"))
    target = float(scenario.get("target_speed_mps") or 22.22)
    launch_args = _autoware_launch_args(control_mode, velocity_smoother)
    return [
        f"algo.autoware.planning_common_max_vel_mps={target}",
        "algo.autoware.velocity_limit_probe.enabled=true",
        f"algo.autoware.velocity_limit_probe.max_velocity_mps={target}",
        "algo.autoware.velocity_limit_probe.duration_s=90.0",
        f"algo.autoware.launch_extra_args={launch_args}",
    ]


def _autoware_launch_args(control_mode: str | None, velocity_smoother: str | None) -> str:
    base = [
        "launch_sensing:=false",
        "launch_perception:=false",
        "launch_localization:=false",
        "launch_planning:=true",
        "launch_control:=true",
        "launch_map:=true",
        "launch_vehicle:=true",
        "is_simulation:=true",
        "launch_system_monitor:=false",
        "launch_dummy_diag_publisher:=true",
        "rviz:=false",
        "rviz_respawn:=false",
    ]
    control_launch_args = {
        "mpc_pid": [
            "trajectory_follower_mode:=trajectory_follower_node",
            "lateral_controller_mode:=mpc",
            "longitudinal_controller_mode:=pid",
        ],
        "pure_pursuit_pid": [
            "trajectory_follower_mode:=trajectory_follower_node",
            "lateral_controller_mode:=pure_pursuit",
            "longitudinal_controller_mode:=pid",
        ],
        "smart_mpc": [
            "trajectory_follower_mode:=smart_mpc_trajectory_follower",
        ],
    }
    base.extend(control_launch_args.get(str(control_mode or ""), []))
    if velocity_smoother:
        base.append(f"velocity_smoother_type:={velocity_smoother}")
    return " ".join(base)


def _runtime_status(*items: Mapping[str, Any]) -> str:
    statuses = {str(item.get("runtime_wiring_status") or "manual_config_patch_required") for item in items}
    if len(statuses) == 1:
        return next(iter(statuses))
    if "manual_config_patch_required" in statuses:
        return "manual_config_patch_required"
    return sorted(statuses)[0]


def _claim_boundary(stack: str, runtime_wiring_status: str) -> str:
    if stack == "apollo" and runtime_wiring_status == "manual_config_patch_required":
        return "planned_internal_variant_requires_explicit_apollo_config_patch_before_online_run"
    return "screening_run_only_not_capability_claim"


def _batch_id() -> str:
    return f"followstop_combo_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list_of_mappings(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
