from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from carla_testbed.contracts import ControlCommand, EgoState, FrameStamp, SceneTruth
from .base import Controller


@dataclass
class DummyController(Controller):
    """Contract-based fixed-output controller for smoke tests."""

    throttle: float = 0.0
    brake: float = 1.0
    steer: float = 0.0
    reverse: bool = False
    hand_brake: bool = False
    source: str = "dummy_controller"
    metadata: dict = field(default_factory=dict)
    name: str = "dummy_controller"

    def reset(self) -> None:
        pass

    def step(self, frame: FrameStamp, ego: EgoState, scene: SceneTruth) -> ControlCommand:
        command = ControlCommand(
            throttle=self.throttle,
            brake=self.brake,
            steer=self.steer,
            reverse=self.reverse,
            hand_brake=self.hand_brake,
            source=self.source,
            stamp=frame,
            metadata=dict(self.metadata),
        ).clamped()
        command.validate()
        return command


class ManualSequenceController(Controller):
    """Replay a finite control sequence, holding the final command after it ends."""

    name = "manual_sequence_controller"

    def __init__(self, commands: Sequence[ControlCommand]):
        if not commands:
            raise ValueError("ManualSequenceController requires at least one command")
        self._commands = tuple(command.clamped() for command in commands)
        self._index = 0

    def reset(self) -> None:
        self._index = 0

    def step(self, frame: FrameStamp, ego: EgoState, scene: SceneTruth) -> ControlCommand:
        idx = min(self._index, len(self._commands) - 1)
        raw = self._commands[idx]
        self._index += 1
        command = ControlCommand(
            throttle=raw.throttle,
            brake=raw.brake,
            steer=raw.steer,
            reverse=raw.reverse,
            hand_brake=raw.hand_brake,
            source=raw.source or self.name,
            stamp=frame,
            metadata=dict(raw.metadata),
        )
        command.validate()
        return command
