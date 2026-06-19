"""Best-effort CARLA safety-event tracking for Phase 1 scenario playback.

The module is intentionally duck-typed and lazy about CARLA imports so offline
tests can validate the artifact contract without a CARLA runtime.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, Mapping


SAFETY_EVENT_SCHEMA_VERSION = "builtin_safety_event.v1"
SAFETY_TRACE_FILENAME = "safety_event_trace.jsonl"


class SafetyEventTracker:
    """Attach collision/lane-invasion sensors and expose comparable counters.

    Sensor creation is best-effort: if the runtime cannot create or listen to a
    sensor, the run can continue, but availability fields remain false so
    downstream comparison treats the safety surface as degraded evidence.
    """

    def __init__(self, *, world: Any, ego: Any, artifact_dir: str | Path) -> None:
        self.collision_count = 0
        self.lane_invasion_count = 0
        self.collision_sensor_available = False
        self.lane_invasion_sensor_available = False
        self.warnings: list[str] = []
        self._sensors: list[Any] = []
        self._trace_path = Path(artifact_dir) / SAFETY_TRACE_FILENAME
        self._trace_path.parent.mkdir(parents=True, exist_ok=True)
        if self._trace_path.exists():
            self._trace_path.unlink()

        self.collision_sensor_available = self._attach_sensor(
            world=world,
            ego=ego,
            blueprint_id="sensor.other.collision",
            event_name="collision",
            callback=self._on_collision,
        )
        self.lane_invasion_sensor_available = self._attach_sensor(
            world=world,
            ego=ego,
            blueprint_id="sensor.other.lane_invasion",
            event_name="lane_invasion",
            callback=self._on_lane_invasion,
        )

    @property
    def trace_path(self) -> Path:
        return self._trace_path

    def snapshot(self) -> dict[str, Any]:
        return {
            "collision_count": int(self.collision_count),
            "lane_invasion_count": int(self.lane_invasion_count),
            "collision_sensor_available": bool(self.collision_sensor_available),
            "lane_invasion_sensor_available": bool(self.lane_invasion_sensor_available),
            "warnings": list(self.warnings),
        }

    def destroy(self) -> list[str]:
        errors: list[str] = []
        for sensor in reversed(self._sensors):
            try:
                stop = getattr(sensor, "stop", None)
                if callable(stop):
                    stop()
            except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
                errors.append(f"safety_sensor_stop_failed:{type(exc).__name__}: {exc}")
            try:
                destroy = getattr(sensor, "destroy", None)
                if callable(destroy):
                    destroy()
            except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
                errors.append(f"safety_sensor_destroy_failed:{type(exc).__name__}: {exc}")
        self._sensors.clear()
        return errors

    def _attach_sensor(
        self,
        *,
        world: Any,
        ego: Any,
        blueprint_id: str,
        event_name: str,
        callback: Any,
    ) -> bool:
        blueprint = find_sensor_blueprint(world, blueprint_id)
        if blueprint is None:
            self.warnings.append(f"{event_name}_sensor_blueprint_missing:{blueprint_id}")
            return False

        sensor = spawn_attached_sensor(world=world, blueprint=blueprint, ego=ego)
        if sensor is None:
            self.warnings.append(f"{event_name}_sensor_spawn_failed:{blueprint_id}")
            return False

        listen = getattr(sensor, "listen", None)
        if not callable(listen):
            self.warnings.append(f"{event_name}_sensor_listen_missing:{blueprint_id}")
            destroy_sensor_quietly(sensor)
            return False

        try:
            listen(callback)
        except Exception as exc:
            self.warnings.append(f"{event_name}_sensor_listen_failed:{type(exc).__name__}")
            destroy_sensor_quietly(sensor)
            return False

        self._sensors.append(sensor)
        return True

    def _on_collision(self, event: Any) -> None:
        self.collision_count += 1
        other_actor = getattr(event, "other_actor", None)
        self._write_event(
            {
                "event_type": "collision",
                "collision_count": self.collision_count,
                "frame": getattr(event, "frame", None),
                "timestamp": getattr(event, "timestamp", None),
                "other_actor_id": getattr(other_actor, "id", None),
                "other_actor_type_id": getattr(other_actor, "type_id", None),
            }
        )

    def _on_lane_invasion(self, event: Any) -> None:
        self.lane_invasion_count += 1
        markings = getattr(event, "crossed_lane_markings", None)
        marking_types: list[str] = []
        if markings is not None:
            for marking in markings:
                marking_type = getattr(marking, "type", None)
                marking_types.append(str(marking_type) if marking_type is not None else "unknown")
        self._write_event(
            {
                "event_type": "lane_invasion",
                "lane_invasion_count": self.lane_invasion_count,
                "frame": getattr(event, "frame", None),
                "timestamp": getattr(event, "timestamp", None),
                "crossed_lane_marking_count": len(marking_types),
                "crossed_lane_marking_types": marking_types,
            }
        )

    def _write_event(self, payload: Mapping[str, Any]) -> None:
        row = {
            "schema_version": SAFETY_EVENT_SCHEMA_VERSION,
            **dict(payload),
        }
        with self._trace_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def empty_safety_snapshot() -> dict[str, Any]:
    return {
        "collision_count": 0,
        "lane_invasion_count": 0,
        "collision_sensor_available": False,
        "lane_invasion_sensor_available": False,
        "warnings": ["safety_event_tracker_not_initialized"],
    }


def find_sensor_blueprint(world: Any, blueprint_id: str) -> Any | None:
    try:
        return world.get_blueprint_library().find(blueprint_id)
    except Exception:
        return None


def spawn_attached_sensor(*, world: Any, blueprint: Any, ego: Any) -> Any | None:
    transform = sensor_transform()
    spawn_actor = getattr(world, "spawn_actor", None)
    if callable(spawn_actor):
        try:
            return spawn_actor(blueprint, transform, attach_to=ego)
        except TypeError:
            try:
                return spawn_actor(blueprint, transform)
            except Exception:
                pass
        except Exception:
            pass

    try_spawn_actor = getattr(world, "try_spawn_actor", None)
    if callable(try_spawn_actor):
        try:
            return try_spawn_actor(blueprint, transform, attach_to=ego)
        except TypeError:
            try:
                return try_spawn_actor(blueprint, transform)
            except Exception:
                pass
        except Exception:
            pass
    return None


def sensor_transform() -> Any:
    try:
        carla = importlib.import_module("carla")
        return carla.Transform()
    except Exception:
        return None


def destroy_sensor_quietly(sensor: Any) -> None:
    try:
        destroy = getattr(sensor, "destroy", None)
        if callable(destroy):
            destroy()
    except Exception:
        return
