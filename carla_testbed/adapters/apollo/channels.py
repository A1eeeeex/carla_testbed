from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ApolloChannels:
    localization: str = "/apollo/localization/pose"
    chassis: str = "/apollo/canbus/chassis"
    obstacles: str = "/apollo/perception/obstacles"
    traffic_light: str = "/apollo/perception/traffic_light"
    routing_request: str = "/apollo/routing_request"
    routing_response: str = "/apollo/routing_response"
    planning: str = "/apollo/planning"
    control: str = "/apollo/control"

    def to_dict(self) -> dict[str, str]:
        return {
            "localization": self.localization,
            "chassis": self.chassis,
            "obstacles": self.obstacles,
            "traffic_light": self.traffic_light,
            "routing_request": self.routing_request,
            "routing_response": self.routing_response,
            "planning": self.planning,
            "control": self.control,
        }
