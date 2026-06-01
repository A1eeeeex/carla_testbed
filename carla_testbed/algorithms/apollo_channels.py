from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ApolloChannelNames:
    localization: str = "/apollo/localization/pose"
    chassis: str = "/apollo/canbus/chassis"
    perception_obstacles: str = "/apollo/perception/obstacles"
    routing_response: str = "/apollo/routing_response"
    planning: str = "/apollo/planning"
    control: str = "/apollo/control"

    def to_dict(self) -> dict[str, str]:
        return {
            "localization": self.localization,
            "chassis": self.chassis,
            "perception_obstacles": self.perception_obstacles,
            "routing_response": self.routing_response,
            "planning": self.planning,
            "control": self.control,
        }
