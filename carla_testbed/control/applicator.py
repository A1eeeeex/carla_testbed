from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(float(value), float(lower)), float(upper))


def _float_attr(obj: Any, name: str, default: float = 0.0) -> float:
    value = getattr(obj, name, default)
    if value is None:
        return float(default)
    return float(value)


def _bool_attr(obj: Any, name: str, default: bool = False) -> bool:
    value = getattr(obj, name, default)
    if value is None:
        return bool(default)
    return bool(value)


def _int_attr(obj: Any, name: str, default: int = 0) -> int:
    value = getattr(obj, name, default)
    if value is None:
        return int(default)
    return int(value)


def _metadata(command: Any) -> dict:
    meta = getattr(command, "metadata", None)
    if meta is None:
        meta = getattr(command, "meta", None)
    return dict(meta or {})


def _source(command: Any, metadata: dict) -> str:
    value = getattr(command, "source", None)
    if value:
        return str(value)
    return str(metadata.get("source") or metadata.get("name") or "unknown")


def _stamp_value(stamp: Any, name: str, default: Any = None) -> Any:
    if stamp is None:
        return default
    return getattr(stamp, name, default)


@dataclass(frozen=True)
class ControlLimits:
    min_throttle: float = 0.0
    max_throttle: float = 1.0
    min_brake: float = 0.0
    max_brake: float = 1.0
    min_steer: float = -1.0
    max_steer: float = 1.0
    brake_zero_throttle_threshold: float = 0.05


@dataclass(frozen=True)
class ControlApplyResult:
    requested_command: dict
    clamped_command: dict
    actor_id: int | str | None
    frame_id: int | None
    sim_time_s: float | None
    applied_ok: bool
    applied_control: dict = field(default_factory=dict)
    error: str | None = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "requested_command": dict(self.requested_command),
            "clamped_command": dict(self.clamped_command),
            "actor_id": self.actor_id,
            "frame_id": self.frame_id,
            "sim_time_s": self.sim_time_s,
            "applied_ok": bool(self.applied_ok),
            "applied_control": dict(self.applied_control),
            "error": self.error,
            "metadata": dict(self.metadata),
        }


class _FallbackVehicleControl:
    """Tiny stand-in used by unit tests when the CARLA Python module is absent."""

    def __init__(self, **kwargs: Any):
        self.throttle = kwargs.get("throttle", 0.0)
        self.brake = kwargs.get("brake", 0.0)
        self.steer = kwargs.get("steer", 0.0)
        self.reverse = kwargs.get("reverse", False)
        self.hand_brake = kwargs.get("hand_brake", False)
        self.manual_gear_shift = kwargs.get("manual_gear_shift", False)
        self.gear = kwargs.get("gear", 0)


def command_to_dict(command: Any, *, stamp: Any | None = None) -> dict:
    metadata = _metadata(command)
    command_stamp = stamp if stamp is not None else getattr(command, "stamp", None)
    frame_id = _stamp_value(command_stamp, "frame_id")
    sim_time_s = _stamp_value(command_stamp, "sim_time_s")
    return {
        "throttle": _float_attr(command, "throttle", 0.0),
        "brake": _float_attr(command, "brake", 0.0),
        "steer": _float_attr(command, "steer", 0.0),
        "reverse": _bool_attr(command, "reverse", False),
        "hand_brake": _bool_attr(command, "hand_brake", False),
        "manual_gear_shift": _bool_attr(command, "manual_gear_shift", False),
        "gear": _int_attr(command, "gear", 0),
        "source": _source(command, metadata),
        "frame_id": None if frame_id is None else int(frame_id),
        "sim_time_s": None if sim_time_s is None else float(sim_time_s),
        "metadata": metadata,
    }


def clamp_command(command: Any, *, limits: ControlLimits | None = None, stamp: Any | None = None) -> dict:
    limits = limits or ControlLimits()
    requested = command_to_dict(command, stamp=stamp)
    brake = _clamp(requested["brake"], limits.min_brake, limits.max_brake)
    throttle = _clamp(requested["throttle"], limits.min_throttle, limits.max_throttle)
    if brake > limits.brake_zero_throttle_threshold:
        throttle = 0.0
    return {
        **requested,
        "throttle": throttle,
        "brake": brake,
        "steer": _clamp(requested["steer"], limits.min_steer, limits.max_steer),
    }


def _make_vehicle_control(command: dict) -> Any:
    kwargs = {
        "throttle": float(command["throttle"]),
        "brake": float(command["brake"]),
        "steer": float(command["steer"]),
        "reverse": bool(command.get("reverse", False)),
        "hand_brake": bool(command.get("hand_brake", False)),
        "manual_gear_shift": bool(command.get("manual_gear_shift", False)),
        "gear": int(command.get("gear", 0)),
    }
    try:
        carla_mod = importlib.import_module("carla")
        vehicle_control_cls = getattr(carla_mod, "VehicleControl")
        return vehicle_control_cls(**kwargs)
    except Exception:
        return _FallbackVehicleControl(**kwargs)


def _control_snapshot(control: Any | None) -> dict:
    if control is None:
        return {}
    return {
        "throttle": _float_attr(control, "throttle", 0.0),
        "brake": _float_attr(control, "brake", 0.0),
        "steer": _float_attr(control, "steer", 0.0),
        "reverse": _bool_attr(control, "reverse", False),
        "hand_brake": _bool_attr(control, "hand_brake", False),
        "manual_gear_shift": _bool_attr(control, "manual_gear_shift", False),
        "gear": _int_attr(control, "gear", 0),
    }


def read_vehicle_control(vehicle: Any) -> dict:
    getter = getattr(vehicle, "get_control", None)
    if getter is None:
        return {}
    return _control_snapshot(getter())


def apply_control_to_vehicle(
    vehicle: Any,
    command: Any,
    *,
    stamp: Any | None = None,
    limits: ControlLimits | None = None,
) -> ControlApplyResult:
    """Clamp a platform control command and apply it to a CARLA-like vehicle.

    The function is deliberately CARLA-lazy: unit tests can use fake vehicles,
    while real runs receive a real ``carla.VehicleControl`` when CARLA is importable.
    """

    requested = command_to_dict(command, stamp=stamp)
    clamped = clamp_command(command, limits=limits, stamp=stamp)
    actor_id = getattr(vehicle, "id", None)
    frame_id = clamped.get("frame_id")
    sim_time_s = clamped.get("sim_time_s")
    vehicle_control = _make_vehicle_control(clamped)

    try:
        vehicle.apply_control(vehicle_control)
        snapshot_error = None
        try:
            applied_snapshot = read_vehicle_control(vehicle) or _control_snapshot(vehicle_control)
        except Exception as exc:
            snapshot_error = f"{type(exc).__name__}: {exc}"
            applied_snapshot = _control_snapshot(vehicle_control)
        metadata = {
            "brake_zeroed_throttle": bool(
                requested["throttle"] and clamped["throttle"] == 0.0 and clamped["brake"] > 0.0
            )
        }
        if snapshot_error:
            metadata["snapshot_error"] = snapshot_error
        return ControlApplyResult(
            requested_command=requested,
            clamped_command=clamped,
            actor_id=actor_id,
            frame_id=frame_id,
            sim_time_s=sim_time_s,
            applied_ok=True,
            applied_control=applied_snapshot,
            metadata=metadata,
        )
    except Exception as exc:
        return ControlApplyResult(
            requested_command=requested,
            clamped_command=clamped,
            actor_id=actor_id,
            frame_id=frame_id,
            sim_time_s=sim_time_s,
            applied_ok=False,
            applied_control={},
            error=str(exc),
            metadata={"error_type": type(exc).__name__},
        )


@dataclass
class ControlApplicator:
    limits: ControlLimits = field(default_factory=ControlLimits)

    def apply(self, vehicle: Any, command: Any, *, stamp: Any | None = None) -> ControlApplyResult:
        return apply_control_to_vehicle(vehicle, command, stamp=stamp, limits=self.limits)
