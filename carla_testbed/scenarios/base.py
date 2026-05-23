from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol


class Scenario(Protocol):
    def build(self, world: Any, carla_map: Any, bp_lib: Any):
        ...

    def reset(self):
        ...

    def destroy(self):
        ...


@dataclass
class ActorRefs:
    ego: Any
    front: Any | None


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    town: str | None = None
    ego_spawn: Mapping[str, Any] = field(default_factory=dict)
    actors: Mapping[str, Any] = field(default_factory=dict)
    duration_s: float | None = None
    max_ticks: int | None = None
    params: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class ScenarioState:
    ego_actor_id: int | str | None = None
    actor_ids: list[int | str] = field(default_factory=list)
    route_metadata: dict[str, Any] = field(default_factory=dict)
    status: str = "created"
    metadata: dict[str, Any] = field(default_factory=dict)


class ScenarioRunner(Protocol):
    def setup(self, world: Any, context: Any, config: Any) -> ScenarioState:
        ...

    def tick(self, frame_context: Any) -> ScenarioState:
        ...

    def teardown(self) -> None:
        ...
