from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class TrafficActorInfo:
    actor_id: int
    role_name: str
    blueprint_id: str
    provider: str
    control_source: str | None = None
    tm_port: int | None = None
    spawn_point_index: int | None = None
    spawn_transform: dict[str, Any] | None = None
    behavior: dict[str, Any] = field(default_factory=dict)


@dataclass
class TrafficFlowState:
    provider: str
    enabled: bool
    seed: int | None
    requested_count: int
    spawned_count: int
    actors: list[TrafficActorInfo] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class TrafficFlowProvider(Protocol):
    name: str

    def setup(self, context: Any, config: dict[str, Any]) -> TrafficFlowState:
        ...

    def tick(self, context: Any) -> None:
        ...

    def teardown(self, context: Any) -> None:
        ...
