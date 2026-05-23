from __future__ import annotations

from .specs import SensorSpec
from .synchronizer import SensorSynchronizer, SensorSyncResult, SensorSyncStatus


_LAZY_EXPORTS = {
    "CollisionEventSource": (".events", "CollisionEventSource"),
    "LaneInvasionEventSource": (".events", "LaneInvasionEventSource"),
    "SensorRig": (".rigs", "SensorRig"),
    "SensorRigStats": (".rigs", "SensorRigStats"),
}


def __getattr__(name: str):
    try:
        module_name, attr_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    import importlib

    module = importlib.import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


__all__ = [
    "CollisionEventSource",
    "LaneInvasionEventSource",
    "SensorRig",
    "SensorRigStats",
    "SensorSpec",
    "SensorSynchronizer",
    "SensorSyncResult",
    "SensorSyncStatus",
]
