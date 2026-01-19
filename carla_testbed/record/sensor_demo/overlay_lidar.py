from __future__ import annotations

import numpy as np

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None

from carla_testbed.record.sensor_demo.geometry import inverse_matrix


def project_lidar_to_image(
    img: np.ndarray,
    points: np.ndarray,
    T_cam: np.ndarray,
    T_lidar: np.ndarray,
    K: np.ndarray,
    color=(0, 255, 0),
    max_points: int = 10000,
):
    if cv2 is None or img is None:
        return img
    pts = np.asarray(points, dtype=np.float32)
    if pts.ndim != 2 or pts.shape[1] < 3:
        return img
    if max_points and pts.shape[0] > max_points:
        idx = np.linspace(0, pts.shape[0] - 1, max_points).astype(int)
        pts = pts[idx]
    pts_h = np.concatenate([pts[:, :3], np.ones((pts.shape[0], 1), dtype=np.float32)], axis=1).T
    # Transform lidar->cam (both expressed relative to base_link)
    T_lidar_to_cam = T_cam @ inverse_matrix(T_lidar)
    pts_cam = T_lidar_to_cam @ pts_h
    zs = pts_cam[2, :]
    valid = zs > 0.1
    pts_cam = pts_cam[:, valid]
    if pts_cam.shape[1] == 0:
        return img
    proj = K @ pts_cam[:3, :]
    proj[:2, :] /= proj[2, :]
    uvs = proj[:2, :].T
    h, w = img.shape[:2]
    for uv in uvs:
        u, v = int(uv[0]), int(uv[1])
        if 0 <= u < w and 0 <= v < h:
            cv2.circle(img, (u, v), 1, color, thickness=-1)
    return img
