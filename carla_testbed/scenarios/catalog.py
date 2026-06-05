from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml


class ScenarioCatalogError(ValueError):
    pass


@dataclass(frozen=True)
class ScenarioCatalogEntry:
    scenario_id: str
    scenario_class: str
    map: str
    path: Path
    gate_role: str | None = None
    route_ref: str | None = None
    spawn_ref: str | None = None
    goal_ref: str | None = None
    requirements: dict[str, Any] = field(default_factory=dict)
    required_artifacts: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "scenario_class": self.scenario_class,
            "map": self.map,
            "path": str(self.path),
            "gate_role": self.gate_role,
            "route_ref": self.route_ref,
            "spawn_ref": self.spawn_ref,
            "goal_ref": self.goal_ref,
            "requirements": dict(self.requirements),
            "required_artifacts": list(self.required_artifacts),
            "tags": list(self.tags),
        }


class ScenarioCatalog:
    """YAML-backed scenario catalog for platform planning.

    This is intentionally separate from `carla_testbed.scenarios.registry`, which
    contains runtime scenario builders.
    """

    def __init__(self, *, repo_root: str | Path = ".") -> None:
        self.repo_root = Path(repo_root).expanduser()
        self._entries: dict[str, ScenarioCatalogEntry] | None = None

    def list(self) -> list[ScenarioCatalogEntry]:
        return sorted(self._load().values(), key=lambda entry: entry.scenario_id)

    def get(self, name: str) -> ScenarioCatalogEntry:
        key = _key(name)
        entries = self._load()
        if key in entries:
            return entries[key]
        available = ", ".join(entry.scenario_id for entry in self.list())
        raise ScenarioCatalogError(f"unknown scenario '{name}'. Available: {available}")

    def _load(self) -> dict[str, ScenarioCatalogEntry]:
        if self._entries is None:
            self._entries = {}
            root = self.repo_root / "configs" / "scenarios"
            if root.exists():
                for path in sorted(root.rglob("*.yaml")):
                    payload = _read_yaml(path)
                    if payload.get("schema_version") != "scenario_spec.v1":
                        continue
                    entry = _entry_from_payload(path, payload)
                    self._entries[_key(entry.scenario_id)] = entry
                    rel = path.relative_to(root).with_suffix("")
                    self._entries.setdefault(_key(str(rel)), entry)
        return self._entries


def default_scenario_catalog(*, repo_root: str | Path = ".") -> ScenarioCatalog:
    return ScenarioCatalog(repo_root=repo_root)


def _entry_from_payload(path: Path, payload: Mapping[str, Any]) -> ScenarioCatalogEntry:
    route = payload.get("route") if isinstance(payload.get("route"), Mapping) else {}
    return ScenarioCatalogEntry(
        scenario_id=str(payload.get("scenario_id") or path.stem),
        scenario_class=str(payload.get("scenario_class") or payload.get("class") or "unknown"),
        map=str(payload.get("map") or payload.get("town") or "Town01"),
        path=path,
        gate_role=_str_or_none(payload.get("gate_role")),
        route_ref=_str_or_none(route.get("route_ref") or payload.get("route_ref")),
        spawn_ref=_str_or_none(route.get("spawn_ref") or payload.get("spawn_ref")),
        goal_ref=_str_or_none(route.get("goal_ref") or payload.get("goal_ref")),
        requirements=dict(payload.get("requirements") or {}),
        required_artifacts=[str(item) for item in payload.get("required_artifacts") or []],
        tags=[str(item) for item in payload.get("tags") or []],
        payload=dict(payload),
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ScenarioCatalogError(f"failed to parse YAML in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ScenarioCatalogError(f"{path}: root must be a mapping")
    return payload


def _str_or_none(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _key(value: str) -> str:
    return str(value).strip().lower().replace("-", "_")
