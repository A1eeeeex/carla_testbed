from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, LaunchPlan, StackContract


class ReplayBackend:
    name = "replay"

    def contract(self, plan: RunPlan | None = None) -> StackContract:
        return StackContract(
            backend=self.name,
            starts_carla=False,
            starts_external_stack=False,
            middleware="none",
            required_inputs=["existing run artifacts"],
            expected_outputs=["analysis reports"],
            required_recorders=[],
        )

    def preflight(self, plan: RunPlan | None = None) -> BackendPreflightResult:
        return BackendPreflightResult(backend=self.name, status="pass", starts_runtime=False)

    def diagnostics(self, run_dir: str | Path) -> BackendDiagnostics:
        root = Path(run_dir)
        return BackendDiagnostics(
            backend=self.name,
            status="pass" if (root / "summary.json").exists() else "insufficient_data",
            artifact_paths=[str(root / "summary.json")] if (root / "summary.json").exists() else [],
        )

    def legacy_dispatch_hint(self, plan: RunPlan) -> Mapping[str, Any]:
        return {"runtime_dispatched": False, "legacy_dispatch": None}

    def build_launch_plan(self, plan: RunPlan) -> LaunchPlan:
        return LaunchPlan(
            backend=self.name,
            mode="offline_replay",
            starts_runtime=False,
            expected_artifacts=["manifest.json", "summary.json", "analysis/evidence_bundle/evidence_bundle.json"],
            postprocess_commands=[
                ["python", "-m", "carla_testbed", "analyze", "--run-dir", f"runs/{plan.identity.run_id}"]
            ],
            compatibility_source="offline_replay",
        )
