from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from carla_testbed.platform.plan import RunPlan

from .apollo_cyberrt import ApolloCyberRTBackend
from .autoware_ros2 import AutowareRos2Backend
from .base import StackBackend
from .carla_direct import CarlaDirectBackend
from .dummy import DummyBackend
from .replay import ReplayBackend

BackendFactory = Callable[[], StackBackend]


@dataclass(frozen=True)
class BackendRegistryEntry:
    name: str
    factory: BackendFactory
    aliases: tuple[str, ...] = ()


class BackendRegistry:
    """CI-safe backend facade registry."""

    def __init__(self) -> None:
        self._entries: dict[str, BackendRegistryEntry] = {}

    def register(self, entry: BackendRegistryEntry) -> None:
        self._entries[_key(entry.name)] = entry
        for alias in entry.aliases:
            self._entries[_key(alias)] = entry

    def get(self, name: str) -> StackBackend:
        try:
            return self._entries[_key(name)].factory()
        except KeyError as exc:
            available = ", ".join(self.names())
            raise KeyError(f"unknown backend '{name}'. Available: {available}") from exc

    def for_plan(self, plan: RunPlan) -> StackBackend:
        return self.get(plan.platform.adapter or plan.platform.name)

    def names(self) -> tuple[str, ...]:
        names = {entry.name for entry in self._entries.values()}
        return tuple(sorted(names))


def default_backend_registry() -> BackendRegistry:
    registry = BackendRegistry()
    registry.register(BackendRegistryEntry("dummy", DummyBackend, aliases=("in_process",)))
    registry.register(BackendRegistryEntry("replay", ReplayBackend, aliases=("offline_replay",)))
    registry.register(BackendRegistryEntry("apollo_cyberrt", ApolloCyberRTBackend, aliases=("apollo",)))
    registry.register(BackendRegistryEntry("carla_direct", CarlaDirectBackend, aliases=("direct",)))
    registry.register(BackendRegistryEntry("autoware_ros2", AutowareRos2Backend, aliases=("autoware",)))
    return registry


def backend_contract_for_plan(plan: RunPlan) -> dict:
    backend = default_backend_registry().for_plan(plan)
    return backend.contract(plan).to_dict()


def backend_preflight_for_plan(plan: RunPlan) -> dict:
    backend = default_backend_registry().for_plan(plan)
    return backend.preflight(plan).to_dict()


def backend_diagnostics_for_plan(plan: RunPlan, run_dir: str | Path) -> dict:
    backend = default_backend_registry().for_plan(plan)
    return backend.diagnostics(run_dir).to_dict()


def _key(value: str) -> str:
    return str(value).strip().lower().replace("-", "_")
