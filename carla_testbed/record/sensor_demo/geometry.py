from __future__ import annotations

import math
import numpy as np


def transform_to_matrix(tr: dict) -> np.ndarray:
    roll = math.radians(tr.get("roll", 0.0) or 0.0)
    pitch = math.radians(tr.get("pitch", 0.0) or 0.0)
    yaw = math.radians(tr.get("yaw", 0.0) or 0.0)
    x, y, z = tr.get("x", 0.0) or 0.0, tr.get("y", 0.0) or 0.0, tr.get("z", 0.0) or 0.0
    cr, sr = math.cos(roll), math.sin(roll)
    cp, sp = math.cos(pitch), math.sin(pitch)
    cy, sy = math.cos(yaw), math.sin(yaw)
    R = np.array(
        [
            [cy * cp, cy * sp * sr - sy * cr, cy * sp * cr + sy * sr],
            [sy * cp, sy * sp * sr + cy * cr, sy * sp * cr - cy * sr],
            [-sp, cp * sr, cp * cr],
        ]
    )
    T = np.eye(4, dtype=float)
    T[:3, :3] = R
    T[:3, 3] = np.array([x, y, z], dtype=float)
    return T


def inverse_matrix(T: np.ndarray) -> np.ndarray:
    R = T[:3, :3]
    t = T[:3, 3]
    inv = np.eye(4, dtype=float)
    inv[:3, :3] = R.T
    inv[:3, 3] = -R.T @ t
    return inv
