from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, LaunchPlan, StackContract


class ApolloCyberRTBackend:
    name = "apollo_cyberrt"
    _STATIC_FOLLOW_STOP_COMPAT_CONFIGS = {
        "baguang_follow_stop_static_300m": (
            "configs/io/examples/phase1_baguang_apollo_followstop_static_control_overlay_paced_compat.yaml"
        ),
        "baguang_follow_stop_static_300m_spawn2m": (
            "configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml"
        ),
    }
    _BAGUANG_FIXED_SCENE_SIDECAR_BASE_CONFIG = (
        "configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml"
    )

    def contract(self, plan: RunPlan | None = None) -> StackContract:
        expected = list(plan.platform.expected_outputs) if plan else ["routing_response", "planning", "control"]
        recorders = list(plan.recording.recorders) if plan else []
        return StackContract(
            backend=self.name,
            starts_carla=True,
            starts_external_stack=True,
            backend_type="apollo_reference_backend",
            middleware="cyberrt",
            input_contract="apollo_truth_input_gt_replacement",
            adapter_path="tools/apollo10_cyber_bridge",
            available_truth_fields=[
                "gt_localization",
                "gt_chassis",
                "gt_obstacles",
                "gt_traffic_lights_when_configured",
                "routing_goal",
            ],
            output_control_mode="apollo_control_to_carla_vehicle_control",
            transport_mode="cyberrt",
            required_inputs=[
                "/apollo/localization/pose",
                "/apollo/canbus/chassis",
                "/apollo/perception/obstacles",
                "/apollo/routing_request",
            ],
            expected_outputs=expected,
            required_recorders=recorders,
            needs_local_carla=True,
            needs_local_apollo=True,
        )

    def preflight(self, plan: RunPlan | None = None) -> BackendPreflightResult:
        return BackendPreflightResult(
            backend=self.name,
            status="local_required",
            starts_runtime=False,
            missing_requirements=["local CARLA/Apollo preflight is not executed by CI-safe backend facade"],
            warnings=["use legacy dispatch or online runner for real execution"],
        )

    def diagnostics(self, run_dir: str | Path) -> BackendDiagnostics:
        root = Path(run_dir)
        paths = [
            root / "artifacts" / "cyber_bridge_stats.json",
            root / "analysis" / "apollo_link_health" / "apollo_link_health_report.json",
        ]
        present = [str(path) for path in paths if path.exists()]
        return BackendDiagnostics(
            backend=self.name,
            status="pass" if present else "insufficient_data",
            artifact_paths=present,
            warnings=[] if present else ["no Apollo backend diagnostics artifacts found"],
        )

    def legacy_dispatch_hint(self, plan: RunPlan) -> Mapping[str, Any]:
        compat_config = _static_follow_stop_compat_config(plan)
        fixed_scene_compat = compat_config is not None
        return {
            "runtime_dispatched": False,
            "legacy_dispatch": (
                "phase1 static follow-stop compatibility command is available"
                if fixed_scene_compat
                else "tools/apollo10_cyber_bridge or existing configs/io runner"
            ),
            "compatibility_backend": plan.platform.params.get("compatibility_backend"),
            "backend_type": "apollo_reference_backend",
            "input_contract": "apollo_truth_input_gt_replacement",
            "fixed_scene_compatibility": fixed_scene_compat,
            "fixed_scene_compatibility_config": compat_config,
        }

    def build_launch_plan(self, plan: RunPlan) -> LaunchPlan:
        run_dir = str(Path(str(plan.compatibility.get("output_root") or "runs")) / plan.identity.run_id)
        fixed_scene_enabled = (
            bool((plan.scenario.fixed_scene or {}).get("enabled", True))
            if plan.scenario.fixed_scene
            else False
        )
        compat_config = _static_follow_stop_compat_config(plan)
        fixed_scene_compat = compat_config is not None
        dynamic_sidecar_config = _dynamic_fixed_scene_sidecar_config(plan)
        fixed_scene_command = [
            "python3",
            "-m",
            "carla_testbed",
            "run",
            "--config",
            compat_config or "",
            "--run-dir",
            run_dir,
            "--legacy-dispatch",
        ]
        if dynamic_sidecar_config is not None:
            fixed_scene_command = _dynamic_fixed_scene_sidecar_command(
                plan,
                run_dir=run_dir,
                base_config=dynamic_sidecar_config,
            )
        command = (
            fixed_scene_command
            if fixed_scene_compat or dynamic_sidecar_config is not None
            else []
            if fixed_scene_enabled
            else _town01_route_only_command(plan, run_dir=run_dir)
        )
        expected_topics = [
            "/apollo/localization/pose",
            "/apollo/canbus/chassis",
            "/apollo/perception/obstacles",
            "/apollo/planning",
            "/apollo/control",
        ]
        if plan.scenario.requirements.get("traffic_light_required"):
            expected_topics.append("/apollo/perception/traffic_light")
        expected_artifacts = [
            "manifest.json",
            "summary.json",
            "timeseries.csv",
            "artifacts/cyber_bridge_stats.json",
            "analysis/apollo_link_health/apollo_link_health_report.json",
        ]
        if fixed_scene_enabled:
            expected_artifacts.extend(
                [
                    "artifacts/fixed_scene_resolved.json",
                    "artifacts/fixed_scene_runtime_state.json",
                    "artifacts/scenario_actor_trace.jsonl",
                    "artifacts/scenario_phase_events.jsonl",
                    "artifacts/obstacle_gt_contract.jsonl",
                    "analysis/fixed_scene_contract/fixed_scene_contract_report.json",
                    "analysis/scenario_actor_contract/scenario_actor_contract_report.json",
                    "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
                    "analysis/v_t_gap/v_t_gap_report.json",
                    "analysis/phase1_status/phase1_status.json",
                ]
            )
        if fixed_scene_compat:
            expected_artifacts.extend(
                [
                    "artifacts/apollo_control_deferred_start.log",
                    "artifacts/apollo_control_deferred_survival.json",
                    "artifacts/apollo_control_runtime_overlay_manifest.json",
                    "artifacts/apollo_control_runtime_overlay_restore.json",
                    "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
                    "analysis/phase1_apollo_fixed_scene_readiness/phase1_apollo_fixed_scene_readiness_report.json",
                ]
            )
        if dynamic_sidecar_config is not None:
            expected_artifacts.extend(
                [
                    "artifacts/fixed_scene_runtime_hook.json",
                    "artifacts/ego_initial_state_materialization.json",
                    "artifacts/apollo_control_deferred_start.log",
                    "artifacts/apollo_control_deferred_survival.json",
                    "artifacts/apollo_control_runtime_overlay_manifest.json",
                    "artifacts/apollo_control_runtime_overlay_restore.json",
                    "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
                ]
            )
        if plan.traffic_flow.enabled:
            expected_artifacts.extend(["artifacts/traffic_flow_manifest.json", "artifacts/traffic_flow_events.jsonl"])
            if int(plan.traffic_flow.vehicles.get("count", 0) or 0) > 0:
                expected_artifacts.extend(
                    [
                        "artifacts/traffic_spawn_candidates.jsonl",
                        "analysis/traffic_flow_contract/traffic_flow_contract_report.json",
                    ]
                )
            if int(plan.traffic_flow.walkers.get("count", 0) or 0) > 0:
                expected_artifacts.extend(
                    [
                        "artifacts/walker_spawn_candidates.jsonl",
                        "analysis/pedestrian_flow_contract/pedestrian_flow_contract_report.json",
                    ]
                )
        return LaunchPlan(
            backend=self.name,
            mode="legacy_apollo_cyberrt_compat",
            commands=[command] if command else [],
            env={
                "CARLA_TESTBED_RUN_ID": plan.identity.run_id,
                "CARLA_TESTBED_PLAN_SCHEMA_VERSION": plan.schema_version,
            },
            required_ports=[2000],
            expected_topics=expected_topics,
            expected_artifacts=expected_artifacts,
            shutdown_hooks=["stop_apollo_bridge", "stop_recorders", "leave_carla_running_policy_dependent"],
            postprocess_commands=[
                ["python3", "tools/analyze_apollo_link_health.py", "--run-dir", run_dir],
                ["python3", "-m", "carla_testbed", "analyze", "--run-dir", run_dir],
            ],
            starts_runtime=(not fixed_scene_enabled) or fixed_scene_compat or dynamic_sidecar_config is not None,
            compatibility_source=(
                "phase1_static_follow_stop_legacy_transition"
                if fixed_scene_compat
                else "phase1_fixed_scene_runtime_sidecar_transition"
                if dynamic_sidecar_config is not None
                else "fixed-scene Apollo runtime migration required"
                if fixed_scene_enabled
                else "tools/run_town01_capability_online_chain.py"
            ),
            warnings=[
                "LaunchPlan is a compatibility description; executor does not rewrite Apollo bridge runtime.",
                *(
                    [
                        "Apollo static follow-stop fixed-scene compatibility uses a guarded legacy transition config; this is not generic fixed-scene runtime migration.",
                        "The default Phase 1 static follow-stop compatibility profile uses the diagnostic control-runtime overlay because the non-overlay Apollo control process crashes with tcmalloc_invalid_free before producing /apollo/control on current Baguang runs.",
                        "The transition remains diagnostic Phase 1 compatibility evidence until obstacle GT, v-t-gap, phase1_status, and comparison artifacts make the run evaluable.",
                    ]
                    if fixed_scene_compat
                    else []
                ),
                *(
                    [
                        "Apollo dynamic fixed-scene command uses a guarded sidecar hook in the legacy follow-stop runner; it is a migration path, not completed online evidence.",
                        "The sidecar must produce fixed_scene_runtime_state, scenario_actor_trace, scenario_phase_events, obstacle GT, v-t-gap, and phase1_status before the run is evaluable.",
                        "The sidecar uses the diagnostic control-runtime overlay and wall-time pacing path because the non-overlay Apollo control process crashes before producing /apollo/control on current Baguang compatibility runs.",
                    ]
                    if dynamic_sidecar_config is not None
                    else []
                ),
                *(
                    [
                        "Apollo fixed-scene runtime is not migrated behind this facade; do not legacy-dispatch this plan as capability evidence.",
                        "Claim-grade fixed-scene Apollo runs must produce obstacle_gt_contract records linking scenario actor ids to /apollo/perception/obstacles.",
                    ]
                    if fixed_scene_enabled and not fixed_scene_compat and dynamic_sidecar_config is None
                    else []
                ),
            ],
        )


def _static_follow_stop_compat_config(plan: RunPlan) -> str | None:
    """Return the guarded legacy config for supported Phase 1 Apollo scenes.

    Baguang static follow-stop maps cleanly onto the existing legacy
    `carla_followstop` transition path because the lead actor is stationary and
    can be placed by waypoint-ahead distance. Dynamic lead accel/decel and
    cut-in/cut-out scenes still need true fixed-scene runtime migration.
    """

    fixed_scene = plan.scenario.fixed_scene or {}
    template = str(fixed_scene.get("template") or "").strip()
    scenario_class = str(plan.scenario.scenario_class or "").strip()
    scenario_id = str(plan.scenario.scenario_id or "").strip()
    map_name = str(plan.scenario.map or "").strip()
    if not (
        map_name == "straight_road_for_baguang"
        and scenario_class == "follow_stop_static"
        and template == "static_lead_stop"
    ):
        return None
    return ApolloCyberRTBackend._STATIC_FOLLOW_STOP_COMPAT_CONFIGS.get(scenario_id)


def _static_follow_stop_compat(plan: RunPlan) -> bool:
    return _static_follow_stop_compat_config(plan) is not None


def _dynamic_fixed_scene_sidecar_config(plan: RunPlan) -> str | None:
    fixed_scene = plan.scenario.fixed_scene or {}
    template = str(fixed_scene.get("template") or "").strip()
    scenario_class = str(plan.scenario.scenario_class or "").strip()
    map_name = str(plan.scenario.map or "").strip()
    if bool(plan.gate.can_claim_natural_driving):
        return None
    if map_name != "straight_road_for_baguang":
        return None
    if scenario_class in {"follow_stop_static", "static_lead_stop"}:
        return None
    if template not in {"lead_vehicle_accel_decel", "cut_in", "cut_out"}:
        return None
    scenario_path = str(plan.source_profiles.get("scenario") or "").strip()
    if not scenario_path:
        return None
    return ApolloCyberRTBackend._BAGUANG_FIXED_SCENE_SIDECAR_BASE_CONFIG


def _dynamic_fixed_scene_sidecar_command(
    plan: RunPlan,
    *,
    run_dir: str,
    base_config: str,
) -> list[str]:
    scenario_path = str(plan.source_profiles.get("scenario") or "").strip()
    overrides = {
        "run.id": plan.identity.run_id,
        "run.scenario_id": plan.scenario.scenario_id,
        "run.scenario_class": plan.scenario.scenario_class,
        "run.route_id": plan.scenario.route_id,
        "run.capability_profile": "phase1_fixed_scene_sidecar",
        "run.profile_name": f"phase1_baguang_apollo_{plan.scenario.scenario_id}_sidecar",
        "scenario.spawn_legacy_front": False,
        "runtime.fixed_scene_player.enabled": True,
        "runtime.fixed_scene_player.scenario_path": scenario_path,
        "runtime.fixed_scene_player.replace_legacy_front": True,
        "runtime.fixed_scene_player.require_setup_success": True,
        "runtime.fixed_scene_player.materialize_ego_initial_speed": True,
        "runtime.postprocess.phase1_scenario_path": scenario_path,
        "recording.artifacts.phase1_scenario_path": scenario_path,
        "backend.params.legacy_run.scenario_id": plan.scenario.scenario_id,
        "backend.params.legacy_run.scenario_class": plan.scenario.scenario_class,
        "backend.params.legacy_run.route_id": plan.scenario.route_id,
        "backend.params.legacy_run.capability_profile": "phase1_fixed_scene_sidecar",
    }
    command = [
        "python3",
        "-m",
        "carla_testbed",
        "run",
        "--config",
        base_config,
        "--run-dir",
        run_dir,
        "--legacy-dispatch",
    ]
    for key, value in overrides.items():
        command.extend(["--override", f"{key}={_override_value(value)}"])
    return command


_TOWN01_ROUTE_REF_OVERRIDES = {
    "town01_curve217": "town01_rh_spawn217_goal048",
    "curve217": "town01_rh_spawn217_goal048",
}


def _town01_route_only_command(plan: RunPlan, *, run_dir: str) -> list[str]:
    step = _town01_capability_step(plan)
    if step is None:
        return ["python3", "tools/run_town01_capability_online_chain.py"]
    capability_profile, route_id = step
    return [
        "python3",
        "tools/run_town01_capability_online_chain.py",
        "--step",
        f"{capability_profile}:{route_id}",
        "--batch-root-parent",
        run_dir,
        "--comparison-label-suffix",
        plan.identity.run_id,
        "--continue-on-failure",
    ]


def _town01_capability_step(plan: RunPlan) -> tuple[str, str] | None:
    if str(plan.scenario.map or "").strip() != "Town01":
        return None
    scenario_class = str(plan.scenario.scenario_class or "").strip()
    if scenario_class == "lane_keep":
        capability = "lane_keep"
    elif scenario_class in {"curve_diagnostic", "curve_lane_follow"}:
        capability = "curve_lane_follow"
    elif scenario_class in {"junction", "junction_turn", "junction_traverse"}:
        capability = "junction_traverse"
    elif scenario_class in {"traffic_light", "traffic_light_red_stop", "traffic_light_green_go"}:
        capability = "traffic_light_actual"
    else:
        return None
    route_id = (
        plan.scenario.route_ref
        or plan.scenario.route_id
        or plan.scenario.goal_ref
        or plan.scenario.scenario_id
    )
    route_id = _TOWN01_ROUTE_REF_OVERRIDES.get(str(route_id), str(route_id))
    return capability, route_id


def _override_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
