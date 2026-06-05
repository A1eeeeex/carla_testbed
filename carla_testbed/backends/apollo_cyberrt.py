from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, StackContract


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
