from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan

from .base import BackendDiagnostics, BackendPreflightResult, StackContract


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
