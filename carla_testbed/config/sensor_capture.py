from __future__ import annotations

from typing import Any, Mapping


def sensor_capture_enabled_from_config(cfg: Mapping[str, Any]) -> tuple[bool, str]:
    """Resolve legacy sensor-capture intent from raw or normalized config.

    The config loader may preserve `record` at the top level or fold it into
    `recording.artifacts.legacy_record`. Runtime code must honor both forms so
    claim-grade online runs can disable camera/sensor capture without relying on
    CLI-specific object shape.
    """
    legacy_record = _mapping(cfg.get("record"))
    folded_record = _mapping(_mapping(_mapping(cfg.get("recording")).get("artifacts")).get("legacy_record"))
    if _mapping(legacy_record.get("sensors")).get("enable") is False:
        return False, "record.sensors.enable"
    if _mapping(folded_record.get("sensors")).get("enable") is False:
        return False, "recording.artifacts.legacy_record.sensors.enable"
    return True, "default_enabled"


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
