from __future__ import annotations

from dataclasses import dataclass, field

from .channels import ApolloChannels
from .control_mapping import ApolloControlMappingConfig


@dataclass(frozen=True)
class ApolloMVPConfig:
    channels: ApolloChannels = field(default_factory=ApolloChannels)
    publish_hz: float = 20.0
    control_timeout_s: float = 0.1
    use_ground_truth_obstacles: bool = True
    frame_id_map: dict[str, str] = field(default_factory=lambda: {"map": "map", "base_link": "base_link"})
    time_source: str = "sim_time"
    control_mapping: ApolloControlMappingConfig = field(default_factory=ApolloControlMappingConfig)

    def validate(self) -> None:
        if float(self.publish_hz) <= 0.0:
            raise ValueError(f"ApolloMVPConfig.publish_hz must be > 0, got {self.publish_hz}")
        if float(self.control_timeout_s) < 0.0:
            raise ValueError(
                f"ApolloMVPConfig.control_timeout_s must be >= 0, got {self.control_timeout_s}"
            )
        if self.time_source != "sim_time":
            raise ValueError(f"ApolloMVPConfig.time_source currently only supports sim_time, got {self.time_source}")

    def to_dict(self) -> dict:
        return {
            "channels": self.channels.to_dict(),
            "publish_hz": float(self.publish_hz),
            "control_timeout_s": float(self.control_timeout_s),
            "use_ground_truth_obstacles": bool(self.use_ground_truth_obstacles),
            "frame_id_map": dict(self.frame_id_map),
            "time_source": self.time_source,
            "control_mapping": {
                "steering_sign": float(self.control_mapping.steering_sign),
                "steering_scale": float(self.control_mapping.steering_scale),
                "throttle_scale": float(self.control_mapping.throttle_scale),
                "brake_scale": float(self.control_mapping.brake_scale),
                "source": self.control_mapping.source,
                "metadata": dict(self.control_mapping.metadata),
            },
        }
