from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass
class ChaseCamConfig:
    distance: float = 8.0
    height: float = 3.0
    pitch_deg: float = -15.0


def chase_cam_pose(ego_transform) -> Tuple[float, float, float, float]:
    """Return a simple chase-cam pose relative to ego yaw."""
    yaw = getattr(getattr(ego_transform, "rotation", None), "yaw", 0.0)
    # In practice this would offset behind the ego; here we return yaw for overlays to log.
    return (0.0, 0.0, 0.0, yaw)
