from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

TRAFFIC_LIGHT_GT_SCHEMA_VERSION = "apollo_traffic_light_gt.v1"
TRAFFIC_LIGHT_GT_MESSAGE_SCHEMA_VERSION = "apollo_traffic_light_gt_message.v1"
APOLLO_TRAFFIC_LIGHT_COLORS = {"RED", "YELLOW", "GREEN", "UNKNOWN"}


@dataclass(frozen=True)
class TrafficLightMapping:
    logical_id: str
    apollo_signal_id: str | None
    stop_line_id: str | None
    lane_ids: tuple[str, ...]
    carla_actor_id: str | int | None = None
    carla_landmark_id: str | int | None = None
    default_state: str = "UNKNOWN"
    supported_scenarios: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "TrafficLightMapping":
        lane_ids = payload.get("lane_ids") or ()
        if not isinstance(lane_ids, (list, tuple)):
            lane_ids = ()
        supported_scenarios = payload.get("supported_scenarios") or ()
        if not isinstance(supported_scenarios, (list, tuple)):
            supported_scenarios = ()
        return cls(
            logical_id=str(payload.get("logical_id") or ""),
            apollo_signal_id=_optional_str(payload.get("apollo_signal_id")),
            stop_line_id=_optional_str(payload.get("stop_line_id")),
            lane_ids=tuple(str(lane_id) for lane_id in lane_ids),
            carla_actor_id=payload.get("carla_actor_id"),
            carla_landmark_id=payload.get("carla_landmark_id"),
            default_state=normalize_traffic_light_state(payload.get("default_state")),
            supported_scenarios=tuple(str(item) for item in supported_scenarios),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "logical_id": self.logical_id,
            "carla_actor_id": self.carla_actor_id,
            "carla_landmark_id": self.carla_landmark_id,
            "apollo_signal_id": self.apollo_signal_id,
            "stop_line_id": self.stop_line_id,
            "lane_ids": list(self.lane_ids),
            "default_state": self.default_state,
            "supported_scenarios": list(self.supported_scenarios),
        }


@dataclass
class MockTrafficLightGTPublisher:
    """CI-safe traffic-light GT publisher that records dict messages only."""

    channel: str = "/apollo/perception/traffic_light"
    publish_count: int = 0
    messages: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def publish_state(
        self,
        state: Any,
        mapping: Mapping[str, Any] | TrafficLightMapping | None,
        *,
        timestamp_sec: float | None = None,
        frame_id: int | None = None,
        confidence: float = 1.0,
    ) -> dict[str, Any]:
        message = build_traffic_light_gt_message_dict(
            state,
            mapping,
            timestamp_sec=timestamp_sec,
            frame_id=frame_id,
            confidence=confidence,
        )
        self.publish_count += 1
        self.messages.append(message)
        self.warnings.extend(message.get("warnings") or [])
        return message


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def normalize_traffic_light_state(state: Any) -> str:
    if state is None:
        return "UNKNOWN"
    if hasattr(state, "name"):
        state = state.name
    normalized = str(state).strip().upper()
    aliases = {
        "R": "RED",
        "RED": "RED",
        "Y": "YELLOW",
        "YELLOW": "YELLOW",
        "AMBER": "YELLOW",
        "G": "GREEN",
        "GREEN": "GREEN",
        "UNKNOWN": "UNKNOWN",
        "OFF": "UNKNOWN",
        "NONE": "UNKNOWN",
    }
    return aliases.get(normalized, "UNKNOWN")


def load_traffic_light_mappings(path: str | Path) -> dict[str, Any]:
    config_path = Path(path).expanduser()
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"traffic light mapping config must be a mapping: {config_path}")
    payload["_source_path"] = str(config_path)
    return payload


def iter_traffic_light_mappings(config: Mapping[str, Any]) -> list[TrafficLightMapping]:
    entries = config.get("traffic_lights")
    if not isinstance(entries, list):
        return []
    return [TrafficLightMapping.from_mapping(entry) for entry in entries if isinstance(entry, Mapping)]


def find_traffic_light_mapping(
    config: Mapping[str, Any],
    *,
    logical_id: str | None = None,
    carla_actor_id: str | int | None = None,
    carla_landmark_id: str | int | None = None,
    apollo_signal_id: str | None = None,
) -> TrafficLightMapping | None:
    for mapping in iter_traffic_light_mappings(config):
        if logical_id is not None and mapping.logical_id == str(logical_id):
            return mapping
        if carla_actor_id is not None and str(mapping.carla_actor_id) == str(carla_actor_id):
            return mapping
        if carla_landmark_id is not None and str(mapping.carla_landmark_id) == str(carla_landmark_id):
            return mapping
        if apollo_signal_id is not None and mapping.apollo_signal_id == str(apollo_signal_id):
            return mapping
    return None


def build_traffic_light_gt_message_dict(
    state: Any,
    mapping: Mapping[str, Any] | TrafficLightMapping | None,
    *,
    timestamp_sec: float | None = None,
    frame_id: int | None = None,
    confidence: float = 1.0,
    traffic_light_policy: str = "carla_actual",
    color_source: str = "carla_actor_state",
    contain_lights: bool | None = None,
) -> dict[str, Any]:
    warnings: list[str] = []
    mapping_obj = _coerce_mapping(mapping)
    color = normalize_traffic_light_state(state)

    if mapping_obj is None:
        warnings.append("missing_signal_mapping")
        signal_id = None
        logical_id = None
        stop_line_id = None
        lane_ids: list[str] = []
    else:
        signal_id = mapping_obj.apollo_signal_id
        logical_id = mapping_obj.logical_id
        stop_line_id = mapping_obj.stop_line_id
        lane_ids = list(mapping_obj.lane_ids)
        if not signal_id:
            warnings.append("missing_apollo_signal_id")
        if not stop_line_id:
            warnings.append("missing_stop_line_id")
        if not lane_ids:
            warnings.append("missing_lane_ids")
        if signal_id and signal_id.startswith("placeholder:"):
            warnings.append("apollo_signal_id_placeholder_unverified")
        if stop_line_id and stop_line_id.startswith("placeholder:"):
            warnings.append("stop_line_id_placeholder_unverified")

    if color == "UNKNOWN":
        warnings.append("traffic_light_state_unknown")

    confidence_value = _clamp_confidence(confidence)
    policy_text = str(traffic_light_policy or "").strip()
    if policy_text == "force_green":
        warnings.append("traffic_light_policy_force_green_not_claim_grade")
    elif policy_text and policy_text != "carla_actual":
        warnings.append("traffic_light_policy_not_claim_grade")
    source_text = str(color_source or "").strip()
    if source_text not in {"carla_actor_state", "carla_landmark_state", "carla_traffic_light_actor_state"}:
        warnings.append("traffic_light_color_source_not_claim_grade")
    if confidence_value < 0.99:
        warnings.append("traffic_light_confidence_below_claim_grade")
    contains = bool(signal_id) if contain_lights is None else bool(contain_lights)
    return {
        "schema_version": TRAFFIC_LIGHT_GT_MESSAGE_SCHEMA_VERSION,
        "signal_id": signal_id,
        "logical_id": logical_id,
        "color": color,
        "confidence": confidence_value,
        "timestamp_sec": timestamp_sec,
        "frame_id": frame_id,
        "traffic_light_policy": policy_text or None,
        "color_source": source_text or None,
        "contain_lights": contains,
        "stop_line_id": stop_line_id,
        "lane_ids": lane_ids,
        "warnings": warnings,
        "status": "warn" if warnings else "pass",
    }


def _coerce_mapping(mapping: Mapping[str, Any] | TrafficLightMapping | None) -> TrafficLightMapping | None:
    if mapping is None:
        return None
    if isinstance(mapping, TrafficLightMapping):
        return mapping
    if isinstance(mapping, Mapping):
        return TrafficLightMapping.from_mapping(mapping)
    return None


def _clamp_confidence(confidence: float) -> float:
    try:
        value = float(confidence)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, value))
