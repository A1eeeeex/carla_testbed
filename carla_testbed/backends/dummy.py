from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, LaunchPlan, StackContract


class DummyBackend:
    name = "dummy"

    def contract(self, plan: RunPlan | None = None) -> StackContract:
        recorders = list(plan.recording.recorders) if plan else []
        return StackContract(
            backend=self.name,
            starts_carla=False,
            starts_external_stack=False,
            middleware="none",
            required_inputs=[],
            expected_outputs=[],
            required_recorders=recorders,
        )

    def preflight(self, plan: RunPlan | None = None) -> BackendPreflightResult:
        return BackendPreflightResult(
            backend=self.name,
            status="pass",
            starts_runtime=False,
            notes=["dummy backend is offline-only"],
        )

    def diagnostics(self, run_dir: str | Path) -> BackendDiagnostics:
        return BackendDiagnostics(backend=self.name, status="not_applicable")

    def legacy_dispatch_hint(self, plan: RunPlan) -> Mapping[str, Any]:
        return {"runtime_dispatched": False, "legacy_dispatch": None}

    def build_launch_plan(self, plan: RunPlan) -> LaunchPlan:
        expected = ["manifest.json", "summary.json"]
        if plan.traffic_flow.enabled:
            expected.extend(["artifacts/traffic_flow_manifest.json", "artifacts/traffic_flow_events.jsonl"])
            if int(plan.traffic_flow.vehicles.get("count", 0) or 0) > 0:
                expected.extend(
                    [
                        "artifacts/traffic_spawn_candidates.jsonl",
                        "analysis/traffic_flow_contract/traffic_flow_contract_report.json",
                    ]
                )
            if int(plan.traffic_flow.walkers.get("count", 0) or 0) > 0:
                expected.extend(
                    [
                        "artifacts/walker_spawn_candidates.jsonl",
                        "analysis/pedestrian_flow_contract/pedestrian_flow_contract_report.json",
                    ]
                )
        return LaunchPlan(
            backend=self.name,
            mode="in_process_dummy",
            starts_runtime=False,
            expected_artifacts=expected,
            postprocess_commands=[
                ["python", "-m", "carla_testbed", "analyze", "--run-dir", f"runs/{plan.identity.run_id}"]
            ],
            compatibility_source="dummy_backend",
        )
