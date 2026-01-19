from __future__ import annotations

import math

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None
import numpy as np


def draw_radar_sector(img, fov_deg: float = 30.0, range_m: float = 80.0, color=(255, 200, 0)):
    if cv2 is None:
        return img
    h, w = img.shape[:2]
    center = (int(w * 0.5), int(h * 0.8))
    radius = int(min(w, h) * 0.4)
    start_angle = -fov_deg / 2.0
    end_angle = fov_deg / 2.0
    cv2.ellipse(img, center, (radius, radius), 0, start_angle, end_angle, color, 2)
    cv2.putText(img, f"Radar FOV {fov_deg:.0f} deg", (20, h - 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 1, cv2.LINE_AA)
    return img
