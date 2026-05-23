from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class Vector3D:
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Quaternion:
    """Unit quaternion in x/y/z/w order."""

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    w: float = 1.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Pose3D:
    position: Vector3D = field(default_factory=Vector3D)
    orientation: Quaternion = field(default_factory=Quaternion)

    def to_dict(self) -> dict:
        return {
            "position": self.position.to_dict(),
            "orientation": self.orientation.to_dict(),
        }


@dataclass(frozen=True)
class Transform3D:
    translation: Vector3D = field(default_factory=Vector3D)
    rotation: Quaternion = field(default_factory=Quaternion)

    def to_dict(self) -> dict:
        return {
            "translation": self.translation.to_dict(),
            "rotation": self.rotation.to_dict(),
        }
