from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol

from carla_testbed.platform.plan import RunPlan


@dataclass(frozen=True)
class StackContract:
    """Metadata-only description of a backend's expected runtime contract."""

    backend: str
    starts_carla: bool
    starts_external_stack: bool
    backend_type: str = "unknown"
    middleware: str = "none"
    input_contract: str = "unspecified"
    adapter_path: str = "unspecified"
    available_truth_fields: list[str] = field(default_factory=list)
    output_control_mode: str = "unspecified"
    transport_mode: str = "none"
    required_inputs: list[str] = field(default_factory=list)
    expected_outputs: list[str] = field(default_factory=list)
    required_recorders: list[str] = field(default_factory=list)
    needs_local_carla: bool = False
    needs_local_apollo: bool = False
    needs_local_autoware: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "backend_type": self.backend_type,
            "starts_carla": self.starts_carla,
            "starts_external_stack": self.starts_external_stack,
            "middleware": self.middleware,
            "input_contract": self.input_contract,
            "adapter_path": self.adapter_path,
            "available_truth_fields": list(self.available_truth_fields),
            "output_control_mode": self.output_control_mode,
            "transport_mode": self.transport_mode,
            "required_inputs": list(self.required_inputs),
            "expected_outputs": list(self.expected_outputs),
            "required_recorders": list(self.required_recorders),
            "needs_local_carla": self.needs_local_carla,
            "needs_local_apollo": self.needs_local_apollo,
            "needs_local_autoware": self.needs_local_autoware,
        }


@dataclass(frozen=True)
class BackendPreflightResult:
    backend: str
    status: str
    starts_runtime: bool = False
    missing_requirements: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "status": self.status,
            "starts_runtime": self.starts_runtime,
            "missing_requirements": list(self.missing_requirements),
            "warnings": list(self.warnings),
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class BackendDiagnostics:
    backend: str
    status: str
    artifact_paths: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    blocking_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "status": self.status,
            "artifact_paths": list(self.artifact_paths),
            "warnings": list(self.warnings),
            "blocking_reasons": list(self.blocking_reasons),
        }


@dataclass(frozen=True)
class LaunchPlan:
    """Dry-run-safe launch description for a backend.

    A LaunchPlan is not process execution. It is the resolved contract that an
    executor or legacy wrapper can use to decide what to run locally.
    """

    backend: str
    mode: str
    commands: list[list[str]] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    required_ports: list[int] = field(default_factory=list)
    required_volumes: list[str] = field(default_factory=list)
    expected_topics: list[str] = field(default_factory=list)
    expected_artifacts: list[str] = field(default_factory=list)
    shutdown_hooks: list[str] = field(default_factory=list)
    postprocess_commands: list[list[str]] = field(default_factory=list)
    starts_runtime: bool = False
    compatibility_source: str | None = None
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "mode": self.mode,
            "commands": [list(command) for command in self.commands],
            "env": dict(self.env),
            "required_ports": list(self.required_ports),
            "required_volumes": list(self.required_volumes),
            "expected_topics": list(self.expected_topics),
            "expected_artifacts": list(self.expected_artifacts),
            "shutdown_hooks": list(self.shutdown_hooks),
            "postprocess_commands": [list(command) for command in self.postprocess_commands],
            "starts_runtime": self.starts_runtime,
            "compatibility_source": self.compatibility_source,
            "warnings": list(self.warnings),
        }


class StackBackend(Protocol):
    """Backend facade used by platform planning.

    Implementations in this package intentionally do not import CARLA, ROS2,
    CyberRT, Autoware, or Apollo protobufs. Runtime launch remains in legacy
    adapters/tools until it is migrated behind these facades.
    """

    name: str

    def contract(self, plan: RunPlan | None = None) -> StackContract:
        ...

    def preflight(self, plan: RunPlan | None = None) -> BackendPreflightResult:
        ...

    def diagnostics(self, run_dir: str | Path) -> BackendDiagnostics:
        ...

    def legacy_dispatch_hint(self, plan: RunPlan) -> Mapping[str, Any]:
        ...

    def build_launch_plan(self, plan: RunPlan) -> LaunchPlan:
        ...
