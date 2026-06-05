from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, StackContract


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
