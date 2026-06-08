from __future__ import annotations

from .apollo_cyberrt import ApolloCyberRTBackend
from .base import BackendPreflightResult, LaunchPlan, StackContract


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

    def build_launch_plan(self, plan=None) -> LaunchPlan:
        base = super().build_launch_plan(plan)
        return LaunchPlan(
            backend=self.name,
            mode="legacy_apollo_carla_direct_experimental",
            commands=base.commands,
            env={**base.env, "CARLA_TESTBED_TRANSPORT_MODE": "carla_direct"},
            required_ports=base.required_ports,
            required_volumes=base.required_volumes,
            expected_topics=base.expected_topics,
            expected_artifacts=base.expected_artifacts,
            shutdown_hooks=base.shutdown_hooks,
            postprocess_commands=base.postprocess_commands,
            starts_runtime=base.starts_runtime,
            compatibility_source=base.compatibility_source,
            warnings=[
                "carla_direct is experimental and must not be treated as the default backend.",
                *base.warnings,
            ],
        )
