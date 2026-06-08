from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, LaunchPlan, StackContract


class AutowareRos2Backend:
    name = "autoware_ros2"

    def contract(self, plan: RunPlan | None = None) -> StackContract:
        expected = list(plan.platform.expected_outputs) if plan else ["trajectory", "control"]
        recorders = list(plan.recording.recorders) if plan else []
        return StackContract(
            backend=self.name,
            starts_carla=True,
            starts_external_stack=True,
            middleware="ros2",
            required_inputs=[
                "/tf",
                "/localization/kinematic_state",
                "/planning/mission_planning/route",
            ],
            expected_outputs=expected,
            required_recorders=recorders,
            needs_local_carla=True,
            needs_local_autoware=True,
        )

    def preflight(self, plan: RunPlan | None = None) -> BackendPreflightResult:
        return BackendPreflightResult(
            backend=self.name,
            status="local_required",
            starts_runtime=False,
            missing_requirements=["local CARLA/Autoware preflight is not executed by CI-safe backend facade"],
            warnings=["use legacy dispatch or online runner for real execution"],
        )

    def diagnostics(self, run_dir: str | Path) -> BackendDiagnostics:
        root = Path(run_dir)
        paths = [
            root / "analysis" / "autoware_evidence" / "autoware_evidence_report.json",
            root / "rosbag2" / "autoware_demo",
        ]
        present = [str(path) for path in paths if path.exists()]
        return BackendDiagnostics(
            backend=self.name,
            status="pass" if present else "insufficient_data",
            artifact_paths=present,
            warnings=[] if present else ["no Autoware backend diagnostics artifacts found"],
        )

    def legacy_dispatch_hint(self, plan: RunPlan) -> Mapping[str, Any]:
        return {
            "runtime_dispatched": False,
            "legacy_dispatch": "existing Autoware adapter/runner",
            "compatibility_backend": plan.platform.params.get("compatibility_backend"),
        }

    def build_launch_plan(self, plan: RunPlan) -> LaunchPlan:
        run_dir = f"runs/{plan.identity.run_id}"
        expected_artifacts = [
            "manifest.json",
            "summary.json",
            "timeseries.csv",
            "analysis/autoware_evidence/autoware_evidence_report.json",
        ]
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
            mode="legacy_autoware_ros2_compat",
            commands=[
                [
                    "python",
                    "tools/run_town01_capability_online_chain.py",
                    "--stack",
                    "autoware",
                    "--scenario",
                    plan.scenario.scenario_id,
                    "--run-dir",
                    run_dir,
                ]
            ],
            env={
                "CARLA_TESTBED_RUN_ID": plan.identity.run_id,
                "ROS_DOMAIN_ID": "0",
            },
            required_ports=[2000],
            expected_topics=[
                "/tf",
                "/tf_static",
                "/localization/kinematic_state",
                "/planning/scenario_planning/trajectory",
                "/control/command/control_cmd",
            ],
            expected_artifacts=expected_artifacts,
            shutdown_hooks=["stop_autoware_launch", "stop_recorders", "leave_carla_running_policy_dependent"],
            postprocess_commands=[
                ["python", "tools/analyze_autoware_evidence.py", "--run-dir", run_dir],
                ["python", "-m", "carla_testbed", "analyze", "--run-dir", run_dir],
            ],
            starts_runtime=True,
            compatibility_source="existing Autoware adapter/runner",
            warnings=[
                "LaunchPlan is a compatibility description; Autoware runtime dispatch remains legacy."
            ],
        )
