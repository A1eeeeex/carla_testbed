from __future__ import annotations

from dataclasses import dataclass, field

from .frame import FrameStamp


def clamp(value: float, lower: float, upper: float) -> float:
    return min(max(float(value), float(lower)), float(upper))


@dataclass(frozen=True)
class ControlCommand:
    throttle: float = 0.0
    brake: float = 0.0
    steer: float = 0.0
    reverse: bool = False
    hand_brake: bool = False
    source: str = "unknown"
    stamp: FrameStamp | None = None
    metadata: dict = field(default_factory=dict)

    def validate(self) -> None:
        if not 0.0 <= float(self.throttle) <= 1.0:
            raise ValueError(f"ControlCommand.throttle must be in [0, 1], got {self.throttle}")
        if not 0.0 <= float(self.brake) <= 1.0:
            raise ValueError(f"ControlCommand.brake must be in [0, 1], got {self.brake}")
        if not -1.0 <= float(self.steer) <= 1.0:
            raise ValueError(f"ControlCommand.steer must be in [-1, 1], got {self.steer}")
        if self.stamp is not None:
            self.stamp.validate()

    def clamped(self) -> "ControlCommand":
        return ControlCommand(
            throttle=clamp(self.throttle, 0.0, 1.0),
            brake=clamp(self.brake, 0.0, 1.0),
            steer=clamp(self.steer, -1.0, 1.0),
            reverse=self.reverse,
            hand_brake=self.hand_brake,
            source=self.source,
            stamp=self.stamp,
            metadata=dict(self.metadata),
        )

    def to_dict(self) -> dict:
        return {
            "throttle": float(self.throttle),
            "brake": float(self.brake),
            "steer": float(self.steer),
            "reverse": bool(self.reverse),
            "hand_brake": bool(self.hand_brake),
            "source": self.source,
            "stamp": None if self.stamp is None else self.stamp.to_dict(),
            # metadata is for non-contract-critical debug fields only.
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class AppliedControl:
    command: ControlCommand
    applied_throttle: float = 0.0
    applied_brake: float = 0.0
    applied_steer: float = 0.0
    metadata: dict = field(default_factory=dict)

    def validate(self) -> None:
        self.command.validate()
        if not 0.0 <= float(self.applied_throttle) <= 1.0:
            raise ValueError(f"AppliedControl.applied_throttle must be in [0, 1], got {self.applied_throttle}")
        if not 0.0 <= float(self.applied_brake) <= 1.0:
            raise ValueError(f"AppliedControl.applied_brake must be in [0, 1], got {self.applied_brake}")
        if not -1.0 <= float(self.applied_steer) <= 1.0:
            raise ValueError(f"AppliedControl.applied_steer must be in [-1, 1], got {self.applied_steer}")

    def to_dict(self) -> dict:
        return {
            "command": self.command.to_dict(),
            "applied_throttle": float(self.applied_throttle),
            "applied_brake": float(self.applied_brake),
            "applied_steer": float(self.applied_steer),
            "metadata": dict(self.metadata),
        }
