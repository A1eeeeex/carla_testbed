from __future__ import annotations

from .apollo_cyberrt import ApolloCyberRTBackend
from .base import BackendPreflightResult, StackContract


class CarlaDirectBackend(ApolloCyberRTBackend):
    name = "carla_direct"

    def contract(self, plan=None) -> StackContract:
        base = super().contract(plan)
        return StackContract(
            backend=self.name,
            starts_carla=base.starts_carla,
            starts_external_stack=base.starts_external_stack,
            middleware=base.middleware,
            required_inputs=base.required_inputs,
            expected_outputs=base.expected_outputs,
            required_recorders=base.required_recorders,
            needs_local_carla=base.needs_local_carla,
            needs_local_apollo=base.needs_local_apollo,
        )

    def preflight(self, plan=None) -> BackendPreflightResult:
        return BackendPreflightResult(
            backend=self.name,
            status="local_required",
            starts_runtime=False,
            warnings=["carla_direct is an experimental transport candidate, not the default backend"],
        )
