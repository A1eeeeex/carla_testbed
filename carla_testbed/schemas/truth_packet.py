from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ObjectTruth:
    object_id: str
    pose: Dict[str, Any]  # {x,y,z,qw,qx,qy,qz}
    velocity: Optional[Dict[str, float]] = None
    acceleration: Optional[Dict[str, float]] = None
    bbox: Optional[Dict[str, float]] = None  # extent x/y/z
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Event:
    event_type: str
    frame_id: int
    timestamp: float
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GroundTruthPacket:
    frame_id: int
    timestamp: float
    ego: Optional[ObjectTruth] = None
    actors: List[ObjectTruth] = field(default_factory=list)
    events: List[Event] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
