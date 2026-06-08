from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, LaunchPlan, StackContract


class ApolloCyberRTBackend:
    name = "apollo_cyberrt"

    def contract(self, plan: RunPlan | None = None) -> StackContract:
        expected = list(plan.platform.expected_outputs) if plan else ["routing_response", "planning", "control"]
        recorders = list(plan.recording.recorders) if plan else []
        return StackContract(
            backend=self.name,
            starts_carla=True,
            starts_external_stack=True,
            middleware="cyberrt",
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
        return {
            "runtime_dispatched": False,
            "legacy_dispatch": "tools/apollo10_cyber_bridge or existing configs/io runner",
            "compatibility_backend": plan.platform.params.get("compatibility_backend"),
        }

    def build_launch_plan(self, plan: RunPlan) -> LaunchPlan:
        run_dir = f"runs/{plan.identity.run_id}"
        command = [
            "python",
            "tools/run_town01_capability_online_chain.py",
            "--scenario",
            plan.scenario.scenario_id,
            "--run-dir",
            run_dir,
        ]
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
            commands=[command],
            env={
                "CARLA_TESTBED_RUN_ID": plan.identity.run_id,
                "CARLA_TESTBED_PLAN_SCHEMA_VERSION": plan.schema_version,
            },
            required_ports=[2000],
            expected_topics=expected_topics,
            expected_artifacts=expected_artifacts,
            shutdown_hooks=["stop_apollo_bridge", "stop_recorders", "leave_carla_running_policy_dependent"],
            postprocess_commands=[
                ["python", "tools/analyze_apollo_link_health.py", "--run-dir", run_dir],
                ["python", "-m", "carla_testbed", "analyze", "--run-dir", run_dir],
            ],
            starts_runtime=True,
            compatibility_source="tools/run_town01_capability_online_chain.py",
            warnings=[
                "LaunchPlan is a compatibility description; executor does not rewrite Apollo bridge runtime."
            ],
        )
