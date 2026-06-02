from __future__ import annotations

import dataclasses
import json
import time
from pathlib import Path
from typing import Any, Mapping

import yaml

from carla_testbed.analysis.assist_ledger import build_runtime_assist_ledger

from .route_curve_fields import ROUTE_CURVE_FIELDS_SCHEMA_VERSION


def _json_safe(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return _json_safe(dataclasses.asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(text)
    tmp_path.replace(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(_json_safe(payload), indent=2, sort_keys=True))


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _append_warning(payload: dict[str, Any], warning: str) -> None:
    existing = payload.get("warnings")
    if isinstance(existing, list):
        warnings = [str(item) for item in existing]
    elif existing in (None, ""):
        warnings = []
    else:
        warnings = [str(existing)]
    if warning not in warnings:
        warnings.append(warning)
    payload["warnings"] = warnings


def _normalize_carla_map_name(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    return text.replace("\\", "/").rstrip("/").split("/")[-1]


def build_carla_world_identity(
    *,
    configured_town: str | None = None,
    loaded_map_name: str | None = None,
    spawn_point_count: int | None = None,
    source: str = "runtime",
    warnings: list[str] | None = None,
) -> dict:
    """Describe the CARLA world actually observed by runtime code.

    This is deliberately CARLA-free so unit tests and offline artifact tooling
    can use the same schema without importing the simulator runtime.
    """
    configured_short = _normalize_carla_map_name(configured_town)
    loaded_short = _normalize_carla_map_name(loaded_map_name)
    matches_config = None
    if configured_short is not None and loaded_short is not None:
        matches_config = configured_short == loaded_short
    return {
        "schema_version": "carla_world_identity.v1",
        "configured_town": configured_town,
        "configured_map_short_name": configured_short,
        "loaded_map_name": loaded_map_name,
        "loaded_map_short_name": loaded_short,
        "matches_configured_town": matches_config,
        "spawn_point_count": None if spawn_point_count is None else int(spawn_point_count),
        "source": source,
        "warnings": list(warnings or []),
    }


class JsonlWriter:
    def __init__(self, path: Path, *, append: bool = True):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._f = self.path.open("a" if append else "w", encoding="utf-8")
        self._closed = False

    def append(self, payload: Mapping[str, Any]) -> None:
        if self._closed:
            raise RuntimeError(f"{self.__class__.__name__} is closed: {self.path}")
        self._f.write(json.dumps(_json_safe(payload), sort_keys=True) + "\n")
        self._f.flush()

    def close(self) -> None:
        if not self._closed:
            self._f.flush()
            self._f.close()
            self._closed = True


class EventsWriter(JsonlWriter):
    pass


class TimeseriesJsonlWriter(JsonlWriter):
    pass


class RunArtifactStore:
    """Standard run-output writer for manifest/config/summary/events/timeseries."""

    def __init__(self, run_dir: str | Path):
        self.run_dir = Path(run_dir)
        self.manifest_path = self.run_dir / "manifest.json"
        self.resolved_config_path = self.run_dir / "config.resolved.yaml"
        self.summary_path = self.run_dir / "summary.json"
        self.events_path = self.run_dir / "events.jsonl"
        self.timeseries_jsonl_path = self.run_dir / "timeseries.jsonl"
        self.timeseries_csv_path = self.run_dir / "timeseries.csv"
        self.logs_dir = self.run_dir / "logs"
        self.route_health_dir = self.run_dir / "analysis" / "route_health"
        self.route_health_json_path = self.route_health_dir / "route_health.json"
        self.route_health_summary_path = self.route_health_dir / "route_health_summary.md"
        self.apollo_channel_health_dir = self.run_dir / "analysis" / "apollo_channel_health"
        self.apollo_channel_health_report_path = (
            self.apollo_channel_health_dir / "apollo_channel_health_report.json"
        )
        self.traffic_light_dir = self.run_dir / "analysis" / "traffic_light"
        self.traffic_light_contract_report_path = (
            self.traffic_light_dir / "traffic_light_contract_report.json"
        )

    def _first_existing_json_mapping(self, *relative_paths: str) -> dict[str, Any]:
        for relative in relative_paths:
            payload = _read_json_mapping(self.run_dir / relative)
            if payload:
                return payload
        return {}

    def _runtime_assist_ledger_for_payload(
        self,
        payload: Mapping[str, Any],
        *,
        payload_role: str,
    ) -> dict[str, Any]:
        manifest = _read_json_mapping(self.manifest_path)
        summary = _read_json_mapping(self.summary_path)
        if payload_role == "manifest":
            manifest = dict(payload)
        elif payload_role == "summary":
            summary = dict(payload)
        config = _read_yaml_mapping(self.resolved_config_path)
        bridge_stats = self._first_existing_json_mapping(
            "artifacts/direct_bridge_stats.json",
            "artifacts/cyber_bridge_stats.json",
            "direct_bridge_stats.json",
            "cyber_bridge_stats.json",
        )
        return build_runtime_assist_ledger(
            config=config or None,
            bridge_stats=bridge_stats or None,
            summary=summary or None,
            manifest=manifest or None,
        )

    def _with_runtime_assist_ledger(
        self,
        payload: Mapping[str, Any],
        *,
        payload_role: str,
    ) -> dict[str, Any]:
        enriched = _json_safe(payload)
        if not isinstance(enriched, dict):
            enriched = dict(payload)
        ledger = self._runtime_assist_ledger_for_payload(enriched, payload_role=payload_role)
        enriched["assist_ledger"] = ledger
        for warning in ledger.get("warnings") or []:
            _append_warning(enriched, str(warning))
        return enriched

    def ensure(self) -> "RunArtifactStore":
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.route_health_dir.mkdir(parents=True, exist_ok=True)
        self.apollo_channel_health_dir.mkdir(parents=True, exist_ok=True)
        self.traffic_light_dir.mkdir(parents=True, exist_ok=True)
        if not self.events_path.exists():
            self.events_path.touch()
        return self

    def write_manifest(self, manifest: Mapping[str, Any]) -> None:
        _write_json(
            self.manifest_path,
            self._with_runtime_assist_ledger(manifest, payload_role="manifest"),
        )

    def update_manifest(self, updates: Mapping[str, Any]) -> dict:
        current: dict[str, Any] = {}
        if self.manifest_path.exists():
            try:
                current = json.loads(self.manifest_path.read_text() or "{}")
            except json.JSONDecodeError:
                current = {}
        current.update(_json_safe(updates))
        self.write_manifest(current)
        return current

    def write_resolved_config(self, config: Mapping[str, Any] | Any) -> None:
        self.resolved_config_path.parent.mkdir(parents=True, exist_ok=True)
        payload = _json_safe(config)
        text = yaml.safe_dump(payload, sort_keys=True, allow_unicode=False)
        _atomic_write_text(self.resolved_config_path, text)

    def write_summary(self, summary: Mapping[str, Any]) -> None:
        _write_json(
            self.summary_path,
            self._with_runtime_assist_ledger(summary, payload_role="summary"),
        )

    def open_events(self, *, append: bool = False) -> EventsWriter:
        return EventsWriter(self.events_path, append=append)

    def open_timeseries_jsonl(self, *, append: bool = False) -> TimeseriesJsonlWriter:
        return TimeseriesJsonlWriter(self.timeseries_jsonl_path, append=append)


def build_manifest(
    *,
    run_id: str,
    start_time_wall_s: float | None = None,
    config_path: str | Path | None = None,
    git_sha: str | None = None,
    carla_host: str | None = None,
    carla_port: int | None = None,
    carla_town: str | None = None,
    carla_world_identity: Mapping[str, Any] | None = None,
    scenario_name: str | None = None,
    backend_name: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "start_time_wall_s": time.time() if start_time_wall_s is None else float(start_time_wall_s),
        "end_time_wall_s": None,
        "config_path": None if config_path is None else str(config_path),
        "git_sha": git_sha,
        "carla": {
            "host": carla_host,
            "port": None if carla_port is None else int(carla_port),
            "town": carla_town,
        },
        "carla_world": None if carla_world_identity is None else dict(carla_world_identity),
        "scenario_name": scenario_name,
        "backend_name": backend_name,
        "route_health_schema_version": "route_health.v1",
        "route_curve_fields_schema_version": ROUTE_CURVE_FIELDS_SCHEMA_VERSION,
        "metadata": dict(metadata or {}),
    }


def build_summary(
    *,
    success: bool,
    exit_reason: str | None,
    frames: int,
    sim_duration_s: float | None,
    wall_duration_s: float | None,
    cleanup_errors_count: int = 0,
    metadata: Mapping[str, Any] | None = None,
) -> dict:
    return {
        "success": bool(success),
        "exit_reason": exit_reason,
        "frames": int(frames),
        "sim_duration_s": None if sim_duration_s is None else float(sim_duration_s),
        "wall_duration_s": None if wall_duration_s is None else float(wall_duration_s),
        "cleanup_errors_count": int(cleanup_errors_count),
        "route_curve_fields_schema_version": ROUTE_CURVE_FIELDS_SCHEMA_VERSION,
        "metadata": dict(metadata or {}),
    }
