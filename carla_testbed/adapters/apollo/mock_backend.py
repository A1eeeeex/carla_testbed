from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterable

from carla_testbed.adapters.base import BackendDiagnostics, BackendState
from carla_testbed.contracts import ControlCommand

from .config import ApolloMVPConfig


@dataclass
class MockApolloBackend:
    """CI-safe Apollo backend implementing the adapter contract without runtime IO."""

    config: ApolloMVPConfig = field(default_factory=ApolloMVPConfig)
    controls: Iterable[ControlCommand] | None = None

    def __post_init__(self) -> None:
        self.config.validate()
        self._controls = deque(self.controls or [])
        self._running = False
        self._prepared = False
        self._context: Any | None = None
        self._last_frame_context: Any | None = None
        self._publish_count = 0
        self._control_poll_count = 0
        self._control_return_count = 0

    @property
    def name(self) -> str:
        return "apollo_mvp_mock"

    @property
    def last_frame_context(self) -> Any | None:
        return self._last_frame_context

    def queue_control(self, command: ControlCommand) -> None:
        command.validate()
        self._controls.append(command)

    def prepare(self, context: Any) -> None:
        self._context = context
        self._prepared = True

    def start(self) -> None:
        if not self._prepared:
            self.prepare(context=None)
        self._running = True

    def publish_inputs(self, frame_context: Any) -> None:
        if not self._running:
            raise RuntimeError("MockApolloBackend.publish_inputs called before start")
        self._last_frame_context = frame_context
        self._publish_count += 1

    def poll_control(self, timeout_s: float | None = None) -> ControlCommand | None:
        del timeout_s
        self._control_poll_count += 1
        if not self._controls:
            return None
        self._control_return_count += 1
        command = self._controls.popleft()
        command.validate()
        return command

    def collect_diagnostics(self) -> BackendDiagnostics:
        return BackendDiagnostics(
            name=self.name,
            state=BackendState(
                name=self.name,
                running=self._running,
                ready=self._running,
                metadata={
                    "prepared": self._prepared,
                    "queued_controls": len(self._controls),
                    "channels": self.config.channels.to_dict(),
                    "time_source": self.config.time_source,
                },
            ),
            counters={
                "publish_count": self._publish_count,
                "control_poll_count": self._control_poll_count,
                "control_return_count": self._control_return_count,
            },
        )

    def stop(self) -> None:
        self._running = False
