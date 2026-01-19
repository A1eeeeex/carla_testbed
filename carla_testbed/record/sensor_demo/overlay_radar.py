from __future__ import annotations

import math

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None
import numpy as np

from carla_testbed.record.sensor_demo.geometry import inverse_matrix
from carla_testbed.record.sensor_demo.overlay_lidar import ue_to_optical


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


def detections_to_points(dets: np.ndarray) -> np.ndarray:
    """
    Convert radar detections (depth, azimuth, altitude, velocity) to xyz (UE frame).
    depth: m, azimuth/altitude: radians, velocity: m/s.
    """
    if dets.size == 0:
        return dets
    depth = dets[:, 0]
    az = dets[:, 1]
    alt = dets[:, 2]
    x = depth * np.cos(alt) * np.cos(az)
    y = depth * np.cos(alt) * np.sin(az)
    z = depth * np.sin(alt)
    return np.stack([x, y, z], axis=1)


def project_radar_to_image(
    img: np.ndarray,
    detections: np.ndarray,
    T_base_cam: np.ndarray,
    T_base_radar: np.ndarray,
    K: np.ndarray,
    color=(255, 0, 0),
):
    """Project radar detections into camera frame."""
    stats = {"n_det": 0, "n_inimg": 0}
    if cv2 is None or img is None:
        return img, stats
    if detections is None or detections.size == 0:
        return img, stats
    pts = detections_to_points(detections)
    stats["n_det"] = pts.shape[0]
    pts_h = np.concatenate([pts, np.ones((pts.shape[0], 1), dtype=np.float32)], axis=1).T
    T_radar_to_cam = T_base_cam @ inverse_matrix(T_base_radar)
    pts_cam = T_radar_to_cam @ pts_h
    pts_opt = ue_to_optical(pts_cam)
    zc = pts_opt[2, :]
    mask = zc > 0.1
    if not np.any(mask):
        return img, stats
    pts_opt = pts_opt[:, mask]
    proj = K @ pts_opt
    proj[:2, :] /= proj[2, :]
    u = proj[0, :].astype(np.int32)
    v = proj[1, :].astype(np.int32)
    h, w = img.shape[:2]
    inimg = (u >= 0) & (u < w) & (v >= 0) & (v < h)
    stats["n_inimg"] = int(np.count_nonzero(inimg))
    if stats["n_inimg"] == 0:
        return img, stats
    img[v[inimg], u[inimg]] = color
    return img, stats
