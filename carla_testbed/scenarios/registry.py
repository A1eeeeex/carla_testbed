from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .base import ScenarioSpec
from .empty_drive import EMPTY_DRIVE_SPEC, build_empty_drive_scenario
from .follow_stop import FOLLOW_STOP_SPEC, build_follow_stop_scenario

ScenarioBuilder = Callable[[Mapping[str, Any] | None], Any]


@dataclass(frozen=True)
class ScenarioEntry:
    name: str
    spec: ScenarioSpec
    builder: ScenarioBuilder
    aliases: tuple[str, ...] = ()


class ScenarioRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, ScenarioEntry] = {}
        self._aliases: dict[str, str] = {}

    def register(self, entry: ScenarioEntry) -> None:
        key = _normalize_name(entry.name)
        self._entries[key] = entry
        for alias in entry.aliases:
            self._aliases[_normalize_name(alias)] = key

    def get(self, name: str) -> ScenarioEntry:
        key = self._resolve_key(name)
        try:
            return self._entries[key]
        except KeyError as exc:
            available = ", ".join(self.names())
            raise KeyError(f"Unknown scenario '{name}'. Available scenarios: {available}") from exc

    def build(self, name: str, config: Mapping[str, Any] | None = None) -> Any:
        entry = self.get(name)
        return entry.builder(config)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(entry.name for entry in self._entries.values()))

    def _resolve_key(self, name: str) -> str:
        key = _normalize_name(name)
        return self._aliases.get(key, key)


def _normalize_name(name: str) -> str:
    return str(name).strip().lower().replace("-", "_")


def default_scenario_registry() -> ScenarioRegistry:
    registry = ScenarioRegistry()
    registry.register(
        ScenarioEntry(
            name="follow_stop",
            spec=FOLLOW_STOP_SPEC,
            builder=build_follow_stop_scenario,
            aliases=("followstop", "follow-stop"),
        )
    )
    registry.register(
        ScenarioEntry(
            name="empty_drive",
            spec=EMPTY_DRIVE_SPEC,
            builder=build_empty_drive_scenario,
            aliases=("smoke_empty", "empty-drive"),
        )
    )
    return registry


DEFAULT_SCENARIO_REGISTRY = default_scenario_registry()


def get_scenario_entry(name: str) -> ScenarioEntry:
    return DEFAULT_SCENARIO_REGISTRY.get(name)


def get_scenario_builder(name: str) -> ScenarioBuilder:
    return get_scenario_entry(name).builder


def build_scenario(name: str, config: Mapping[str, Any] | None = None) -> Any:
    return DEFAULT_SCENARIO_REGISTRY.build(name, config)
