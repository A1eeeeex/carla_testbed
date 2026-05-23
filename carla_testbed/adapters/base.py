from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class BackendState:
    name: str
    running: bool = False
    ready: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BackendDiagnostics:
    name: str
    state: BackendState | None = None
    counters: dict[str, int | float] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": None
            if self.state is None
            else {
                "name": self.state.name,
                "running": bool(self.state.running),
                "ready": bool(self.state.ready),
                "metadata": dict(self.state.metadata),
            },
            "counters": dict(self.counters),
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "metadata": dict(self.metadata),
        }


@runtime_checkable
class ADStackBackend(Protocol):
    """Simulator-neutral adapter contract for an autonomous-driving stack.

    Implementations may use ROS2, CyberRT, direct CARLA transport, or another
    runtime internally. The core runner should depend on this protocol, not on
    concrete ROS2/CyberRT modules or stack-specific protobufs.
    """

    @property
    def name(self) -> str:
        ...

    def prepare(self, context: Any) -> None:
        ...

    def start(self) -> None:
        ...

    def publish_inputs(self, frame_context: Any) -> None:
        ...

    def poll_control(self, timeout_s: float | None = None) -> Any | None:
        ...

    def collect_diagnostics(self) -> BackendDiagnostics | dict[str, Any]:
        ...

    def stop(self) -> None:
        ...
