from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

from carla_testbed.contracts import ControlCommand

from .control_mapping import ApolloControlMappingConfig, map_apollo_control_dict_to_command


@dataclass
class ControlCommandSubscriber:
    channel: str
    mapping_config: ApolloControlMappingConfig = field(default_factory=ApolloControlMappingConfig)
    reader: Any | None = None
    _queue: deque[dict[str, Any] | ControlCommand] = field(default_factory=deque, init=False)
    poll_count: int = 0
    receive_count: int = 0

    def push_mock_message(self, raw: dict[str, Any] | ControlCommand) -> None:
        self._queue.append(raw)

    def poll(self, timeout_s: float | None = None) -> ControlCommand | None:
        del timeout_s
        self.poll_count += 1
        raw = None
        if self._queue:
            raw = self._queue.popleft()
        elif self.reader is not None:
            read = getattr(self.reader, "read", None)
            raw = read() if callable(read) else None
        if raw is None:
            return None
        self.receive_count += 1
        if isinstance(raw, ControlCommand):
            raw.validate()
            return raw
        return map_apollo_control_dict_to_command(raw, self.mapping_config)


@dataclass
class PlanningTrajectorySubscriber:
    channel: str
    reader: Any | None = None
    last_message: Any | None = None
    poll_count: int = 0

    def poll(self, timeout_s: float | None = None) -> Any | None:
        del timeout_s
        self.poll_count += 1
        if self.reader is None:
            return self.last_message
        read = getattr(self.reader, "read", None)
        if callable(read):
            self.last_message = read()
        return self.last_message
