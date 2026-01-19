from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class AlgoInput:
    frame_id: int
    timestamp: float
    frame_packet: Optional[Any] = None
    truth_packet: Optional[Any] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ControlCommand:
    throttle: float = 0.0
    brake: float = 0.0
    steer: float = 0.0
    reverse: bool = False
    hand_brake: bool = False
    manual_gear_shift: bool = False
    gear: int = 0
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlgoOutput:
    control: ControlCommand
    debug: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
