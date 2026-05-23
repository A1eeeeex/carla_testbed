from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from carla_testbed.contracts import ControlCommand, clamp


@dataclass(frozen=True)
class ApolloControlMappingConfig:
    steering_sign: float = 1.0
    steering_scale: float = 0.01
    throttle_scale: float = 0.01
    brake_scale: float = 0.01
    source: str = "apollo_mvp"
    metadata: dict[str, Any] = field(default_factory=dict)


def _first_present(raw: Mapping[str, Any], names: tuple[str, ...], default: Any = 0.0) -> tuple[str | None, Any]:
    for name in names:
        if name in raw and raw[name] is not None:
            return name, raw[name]
    return None, default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def map_apollo_control_dict_to_command(
    raw: Mapping[str, Any],
    cfg: ApolloControlMappingConfig | None = None,
) -> ControlCommand:
    """Map an Apollo-like raw control dict into the core ControlCommand.

    This function intentionally accepts dictionaries, not Apollo protobufs. The
    adapter implementation owns protobuf conversion before calling this mapper.
    Defaults assume Apollo percentage-like throttle/brake/steering fields.
    """

    cfg = cfg or ApolloControlMappingConfig()
    throttle_key, throttle_raw = _first_present(raw, ("throttle", "throttle_percentage", "acceleration_cmd"))
    brake_key, brake_raw = _first_present(raw, ("brake", "brake_percentage"))
    steering_key, steering_raw = _first_present(
        raw,
        ("steering_target", "steering_percentage", "steering_rate", "steer"),
    )

    throttle = clamp(_as_float(throttle_raw) * float(cfg.throttle_scale), 0.0, 1.0)
    brake = clamp(_as_float(brake_raw) * float(cfg.brake_scale), 0.0, 1.0)
    steer = clamp(
        _as_float(steering_raw) * float(cfg.steering_scale) * float(cfg.steering_sign),
        -1.0,
        1.0,
    )

    metadata = {
        "raw": dict(raw),
        "mapping": {
            "throttle_key": throttle_key,
            "brake_key": brake_key,
            "steering_key": steering_key,
            "steering_sign": float(cfg.steering_sign),
            "steering_scale": float(cfg.steering_scale),
            "throttle_scale": float(cfg.throttle_scale),
            "brake_scale": float(cfg.brake_scale),
        },
    }
    metadata.update(dict(cfg.metadata))

    command = ControlCommand(
        throttle=throttle,
        brake=brake,
        steer=steer,
        reverse=_as_bool(raw.get("reverse"), False),
        hand_brake=_as_bool(raw.get("hand_brake", raw.get("parking_brake")), False),
        source=cfg.source,
        metadata=metadata,
    )
    command.validate()
    return command
