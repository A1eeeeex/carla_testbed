from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .base import TrafficFlowProvider
from .carla_walker_flow import CarlaWalkerFlow
from .carla_tm_flow import CarlaTrafficManagerFlow
from .mixed_flow import MixedCarlaFlow

ProviderFactory = Callable[[], TrafficFlowProvider]


@dataclass(frozen=True)
class TrafficFlowRegistryEntry:
    name: str
    factory: ProviderFactory
    aliases: tuple[str, ...] = ()


class TrafficFlowRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, TrafficFlowRegistryEntry] = {}

    def register(self, entry: TrafficFlowRegistryEntry) -> None:
        self._entries[_key(entry.name)] = entry
        for alias in entry.aliases:
            self._entries[_key(alias)] = entry

    def get(self, name: str) -> TrafficFlowProvider:
        try:
            return self._entries[_key(name)].factory()
        except KeyError as exc:
            available = ", ".join(self.names()) or "none"
            raise KeyError(f"unknown traffic flow provider '{name}'. Available: {available}") from exc

    def names(self) -> tuple[str, ...]:
        return tuple(sorted({entry.name for entry in self._entries.values()}))


def default_traffic_flow_registry() -> TrafficFlowRegistry:
    registry = TrafficFlowRegistry()
    registry.register(TrafficFlowRegistryEntry("none", _NoneTrafficFlowProvider))
    registry.register(
        TrafficFlowRegistryEntry(
            "carla_traffic_manager",
            CarlaTrafficManagerFlow,
            aliases=("carla_tm", "traffic_manager"),
        )
    )
    registry.register(
        TrafficFlowRegistryEntry(
            "carla_walker_ai_controller",
            CarlaWalkerFlow,
            aliases=("walker_ai", "walkers", "carla_walkers"),
        )
    )
    registry.register(
        TrafficFlowRegistryEntry(
            "mixed_carla_flow",
            MixedCarlaFlow,
            aliases=("mixed", "carla_tm_walkers"),
        )
    )
    return registry


class _NoneTrafficFlowProvider:
    name = "none"

    def setup(self, context: object, config: dict) -> object:
        del context, config
        from .base import TrafficFlowState

        return TrafficFlowState(
            provider=self.name,
            enabled=False,
            seed=None,
            requested_count=0,
            spawned_count=0,
        )

    def tick(self, context: object) -> None:
        del context

    def teardown(self, context: object) -> None:
        del context


def _key(value: str) -> str:
    return str(value).strip().lower().replace("-", "_")
