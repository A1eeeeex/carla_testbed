from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .time_sync import ApolloTimeAdapter


@dataclass
class _BasePublisher:
    channel: str
    time_adapter: ApolloTimeAdapter = field(default_factory=ApolloTimeAdapter)
    writer: Any | None = None
    publish_count: int = 0
    last_message: dict[str, Any] | None = None

    def publish(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.last_message = dict(payload)
        self.publish_count += 1
        if self.writer is not None:
            self.writer.write(payload)
        return self.last_message


@dataclass
class LocalizationPublisher(_BasePublisher):
    def build_message(self, ego_state: Any, stamp: Any) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "header": self.time_adapter.to_apollo_time(stamp),
            "ego_state": _to_payload(ego_state),
        }


@dataclass
class ChassisPublisher(_BasePublisher):
    def build_message(self, chassis_state: Any, stamp: Any) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "header": self.time_adapter.to_apollo_time(stamp),
            "chassis_state": _to_payload(chassis_state),
        }


@dataclass
class GroundTruthObstaclePublisher(_BasePublisher):
    def build_message(self, scene_truth: Any, stamp: Any) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "header": self.time_adapter.to_apollo_time(stamp),
            "scene_truth": _to_payload(scene_truth),
        }


def _to_payload(value: Any) -> Any:
    if value is None:
        return None
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    if isinstance(value, dict):
        return dict(value)
    return value
