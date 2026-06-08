from __future__ import annotations

from .applicator import ControlApplicator, ControlApplyResult, ControlLimits, apply_control_to_vehicle
from .base import Controller
from .dummy import DummyController, ManualSequenceController
from .simple_acc_route_follower import SimpleAccRouteFollowerConfig, SimpleAccRouteFollowerController


_LAZY_EXPORTS = {
    "CarlaLegacyControllerAdapter": (".legacy_controller", "CarlaLegacyControllerAdapter"),
    "LegacyControllerConfig": (".legacy_controller", "LegacyControllerConfig"),
    "LegacyFollowStopController": (".legacy_controller", "LegacyFollowStopController"),
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
    "CarlaLegacyControllerAdapter",
    "ControlApplicator",
    "ControlApplyResult",
    "ControlLimits",
    "Controller",
    "DummyController",
    "LegacyControllerConfig",
    "LegacyFollowStopController",
    "ManualSequenceController",
    "SimpleAccRouteFollowerConfig",
    "SimpleAccRouteFollowerController",
    "apply_control_to_vehicle",
]
