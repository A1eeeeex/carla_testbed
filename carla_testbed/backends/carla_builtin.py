from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, LaunchPlan, StackContract


class CarlaBuiltinBackend:
    name = "carla_builtin"

    def contract(self, plan: RunPlan | None = None) -> StackContract:
        recorders = list(plan.recording.recorders) if plan else []
        return StackContract(
            backend=self.name,
            starts_carla=True,
            starts_external_stack=False,
            backend_type="planning_control_backend",
            middleware="none",
            input_contract="scene_truth_direct",
            adapter_path="carla_testbed.scenario_player.builtin_ego_runner",
            available_truth_fields=[
                "ego_state",
                "target_actor_state",
                "fixed_scene_roles",
                "route_waypoint_context",
            ],
            output_control_mode="carla_vehicle_control",
            transport_mode="direct_python_api",
            required_inputs=["fixed_scene_storyboard", "carla_world"],
            expected_outputs=[
                "manifest.json",
                "summary.json",
                "timeseries.csv",
                "artifacts/ego_control_trace.jsonl",
                "artifacts/scenario_actor_trace.jsonl",
            ],
            required_recorders=recorders,
            needs_local_carla=True,
            needs_local_apollo=False,
            needs_local_autoware=False,
        )

    def preflight(self, plan: RunPlan | None = None) -> BackendPreflightResult:
        return BackendPreflightResult(
            backend=self.name,
            status="local_required",
            starts_runtime=False,
            warnings=["carla_builtin requires local CARLA for real execution; CI only checks launch plans"],
            notes=["diagnostic ego controller output is not Apollo/Autoware autonomy evidence"],
        )

    def diagnostics(self, run_dir: str | Path) -> BackendDiagnostics:
        root = Path(run_dir)
        paths = [
            root / "artifacts" / "ego_control_trace.jsonl",
            root / "analysis" / "fixed_scene_contract" / "fixed_scene_contract_report.json",
            root / "analysis" / "scenario_actor_contract" / "scenario_actor_contract_report.json",
        ]
        present = [str(path) for path in paths if path.exists()]
        return BackendDiagnostics(
            backend=self.name,
            status="pass" if len(present) == len(paths) else "insufficient_data",
            artifact_paths=present,
            warnings=[] if len(present) == len(paths) else ["builtin ego fixed-scene artifacts are incomplete"],
        )

    def legacy_dispatch_hint(self, plan: RunPlan) -> Mapping[str, Any]:
        return {
            "runtime_dispatched": False,
            "legacy_dispatch": "tools/run_builtin_ego_fixed_scene.py",
            "claim_boundary": "diagnostic_only_not_natural_driving_evidence",
            "backend_type": "planning_control_backend",
            "input_contract": "scene_truth_direct",
        }

    def build_launch_plan(self, plan: RunPlan) -> LaunchPlan:
        run_dir = str(Path(str(plan.compatibility.get("output_root") or "runs")) / plan.identity.run_id)
        scenario_path = plan.source_profiles.get("scenario") or plan.scenario.scenario_id
        runtime_python = _runtime_python()
        command = [
            runtime_python,
            "tools/run_builtin_ego_fixed_scene.py",
            "--template",
            str(scenario_path),
            "--run-dir",
            run_dir,
            "--town",
            plan.world.town,
            "--fixed-dt-s",
            str(plan.world.fixed_dt_s),
        ]
        scenario_params = plan.scenario.fixed_scene.get("params") if isinstance(plan.scenario.fixed_scene, Mapping) else {}
        target_speed = None
        if isinstance(scenario_params, Mapping):
            target_speed = scenario_params.get("ego_target_speed_mps")
        if target_speed is None:
            target_speed = _scenario_target_speed_mps(plan.scenario.success_intent)
        if target_speed is None:
            target_speed = plan.algorithm.params.get("target_speed_mps")
        if target_speed is not None:
            command.extend(["--target-speed-mps", str(target_speed)])
        return LaunchPlan(
            backend=self.name,
            mode="carla_builtin_diagnostic_fixed_scene",
            commands=[command],
            required_ports=[int(plan.world.port)],
            expected_artifacts=[
                "manifest.json",
                "summary.json",
                "timeseries.csv",
                "artifacts/ego_control_trace.jsonl",
                "artifacts/fixed_scene_resolved.json",
                "artifacts/fixed_scene_runtime_state.json",
                "artifacts/scenario_actor_trace.jsonl",
                "artifacts/scenario_phase_events.jsonl",
                "analysis/fixed_scene_contract/fixed_scene_contract_report.json",
                "analysis/scenario_actor_contract/scenario_actor_contract_report.json",
            ],
            postprocess_commands=[
                ["python3", "-m", "carla_testbed", "analyze", "--run-dir", run_dir, "--gate", "scenario_validation"]
            ],
            starts_runtime=True,
            compatibility_source="tools/run_builtin_ego_fixed_scene.py",
            warnings=[
                "builtin ego controller is diagnostic-only and must not be used as Apollo/Autoware natural-driving evidence",
                f"builtin fixed-scene runtime python: {runtime_python}",
            ],
        )


def _runtime_python() -> str:
    for raw in (
        os.environ.get("CARLA_TESTBED_CARLA_PYTHON"),
        os.environ.get("CARLA16_PYTHON"),
        "/home/ubuntu/miniconda3/envs/carla16/bin/python",
        "/home/ubuntu/anaconda3/envs/carla16/bin/python",
    ):
        candidate = os.path.expanduser(os.path.expandvars(str(raw or "").strip()))
        if candidate and "${" not in candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return candidate
    return "python3"


def _scenario_target_speed_mps(success_intent: Mapping[str, Any]) -> Any:
    for key in ("ego_target_speed_mps", "target_speed_mps", "desired_speed_mps"):
        if success_intent.get(key) is not None:
            return success_intent[key]
    return None
