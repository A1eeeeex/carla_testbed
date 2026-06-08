from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml


class PlatformRegistryError(ValueError):
    """Raised when a platform profile cannot be resolved."""


@dataclass(frozen=True)
class RegistryEntry:
    kind: str
    name: str
    path: Path
    payload: dict[str, Any]
    aliases: tuple[str, ...] = ()


class PlatformRegistry:
    """Filesystem-backed registry for RunPlan profile inputs.

    The registry is intentionally metadata-only. It must remain safe in CI and
    must not import CARLA, CyberRT, ROS2, Autoware, or Apollo protobuf modules.
    """

    DEFAULT_DIRECTORIES = {
        "platform": ("configs/platforms",),
        "algorithm": ("configs/algorithms",),
        "scenario": ("configs/scenarios",),
        "recording": ("configs/recording",),
        "traffic": ("configs/traffic",),
        "gate": ("configs/gates",),
        "suite": ("configs/suites",),
        "fixed_scene": ("configs/scenario_templates",),
    }

    KIND_ALIASES = {
        "platforms": "platform",
        "algorithms": "algorithm",
        "scenarios": "scenario",
        "record": "recording",
        "records": "recording",
        "recordings": "recording",
        "traffic_flow": "traffic",
        "traffics": "traffic",
        "gates": "gate",
        "suites": "suite",
        "fixed-scenes": "fixed_scene",
        "fixed_scenes": "fixed_scene",
    }

    def __init__(self, *, repo_root: str | Path = ".") -> None:
        self.repo_root = Path(repo_root).expanduser()
        self._entries: dict[str, dict[str, RegistryEntry]] = {}

    def kind(self, kind: str) -> str:
        normalized = str(kind).strip().lower()
        return self.KIND_ALIASES.get(normalized, normalized)

    def list(self, kind: str) -> list[RegistryEntry]:
        normalized = self.kind(kind)
        entries = self._entries_for_kind(normalized)
        unique: dict[Path, RegistryEntry] = {}
        for entry in entries.values():
            unique.setdefault(entry.path, entry)
        return sorted(unique.values(), key=lambda entry: entry.name)

    def names(self, kind: str) -> tuple[str, ...]:
        return tuple(entry.name for entry in self.list(kind))

    def get(self, kind: str, name_or_path: str | Path) -> RegistryEntry:
        normalized = self.kind(kind)
        text = str(name_or_path)
        path = self._resolve_path(text)
        if path is not None:
            return self._entry_from_path(normalized, path)
        key = _normalize_key(text)
        entries = self._entries_for_kind(normalized)
        try:
            return entries[key]
        except KeyError as exc:
            available = ", ".join(self.names(normalized)) or "none"
            raise PlatformRegistryError(
                f"Unknown {normalized} profile '{text}'. Available: {available}"
            ) from exc

    def _entries_for_kind(self, kind: str) -> dict[str, RegistryEntry]:
        if kind not in self._entries:
            self._entries[kind] = self._scan_kind(kind)
        return self._entries[kind]

    def _scan_kind(self, kind: str) -> dict[str, RegistryEntry]:
        entries: dict[str, RegistryEntry] = {}
        for directory in self.DEFAULT_DIRECTORIES.get(kind, ()):
            root = self.repo_root / directory
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.yaml")):
                entry = self._entry_from_path(kind, path)
                if kind == "scenario" and entry.payload.get("schema_version") != "scenario_spec.v1":
                    continue
                for key in _entry_keys(entry):
                    entries.setdefault(key, entry)
        return entries

    def _entry_from_path(self, kind: str, path: Path) -> RegistryEntry:
        payload = _read_yaml(path)
        name = _profile_name(kind, payload, path)
        aliases = tuple(str(item) for item in payload.get("aliases") or [] if item)
        return RegistryEntry(kind=kind, name=name, path=path, payload=payload, aliases=aliases)

    def _resolve_path(self, value: str) -> Path | None:
        candidate = Path(value).expanduser()
        if candidate.exists():
            return candidate
        candidate = self.repo_root / value
        if candidate.exists():
            return candidate
        return None


def default_platform_registry(*, repo_root: str | Path = ".") -> PlatformRegistry:
    return PlatformRegistry(repo_root=repo_root)


def _entry_keys(entry: RegistryEntry) -> Iterable[str]:
    yield _normalize_key(entry.name)
    yield _normalize_key(entry.path.stem)
    parent = entry.path.parent.name
    if parent and parent not in {"configs", entry.kind + "s"}:
        yield _normalize_key(f"{parent}/{entry.path.stem}")
        yield _normalize_key(f"{parent}_{entry.path.stem}")
    for alias in entry.aliases:
        yield _normalize_key(alias)


def _normalize_key(value: str) -> str:
    return str(value).strip().lower().replace("-", "_")


def _profile_name(kind: str, payload: Mapping[str, Any], path: Path) -> str:
    if kind == "fixed_scene" and payload.get("template"):
        return str(payload["template"])
    for key in ("name", "profile", "variant_id", "scenario_id", "suite_id"):
        value = payload.get(key)
        if value:
            return str(value)
    if kind == "recording":
        return str(payload.get("recording_profile") or path.stem)
    if kind == "gate":
        return str(payload.get("gate_profile") or path.stem)
    return path.stem


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise PlatformRegistryError(f"failed to parse YAML in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise PlatformRegistryError(f"{path}: root must be a mapping")
    return payload
