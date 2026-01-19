from __future__ import annotations

import numpy as np

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None

from carla_testbed.record.sensor_demo.geometry import inverse_matrix


def ue_to_optical(points_cam: np.ndarray) -> np.ndarray:
    """
    UE/vehicle frame (x forward, y right, z up) -> camera optical (x right, y down, z forward).
    """
    x = points_cam[1, :]
    y = -points_cam[2, :]
    z = points_cam[0, :]
    return np.vstack([x, y, z])


def project_lidar_to_image(
    img: np.ndarray,
    points: np.ndarray,
    T_base_cam: np.ndarray,
    T_base_lidar: np.ndarray,
    K: np.ndarray,
    color=(0, 255, 0),
    max_points: int = 8000,
    max_range: float = None,
    debug: bool = False,
):
    """Project lidar points (base->lidar) onto camera (base->cam) optical frame."""
    stats = {"n_in": 0, "n_sampled": 0, "n_front": 0, "n_inimg": 0}
    if cv2 is None or img is None:
        return img, stats
    pts = np.asarray(points, dtype=np.float32)
    if pts.ndim != 2 or pts.shape[1] < 3:
        return img, stats
    stats["n_in"] = pts.shape[0]
    if max_range:
        r = np.linalg.norm(pts[:, :3], axis=1)
        pts = pts[r <= max_range]
    if max_points and pts.shape[0] > max_points:
        idx = np.linspace(0, pts.shape[0] - 1, max_points).astype(int)
        pts = pts[idx]
    stats["n_sampled"] = pts.shape[0]
    if pts.size == 0:
        return img, stats

    pts_h = np.concatenate([pts[:, :3], np.ones((pts.shape[0], 1), dtype=np.float32)], axis=1).T
    # base->sensor transforms; p_cam = T_base_cam @ inv(T_base_lidar) @ p_lidar
    T_lidar_to_cam = T_base_cam @ inverse_matrix(T_base_lidar)
    pts_cam = T_lidar_to_cam @ pts_h
    pts_opt = ue_to_optical(pts_cam)
    zc = pts_opt[2, :]
    front_mask = zc > 0.1
    pts_opt = pts_opt[:, front_mask]
    if pts_opt.shape[1] == 0:
        if debug:
            print("[lidar] no points after Z>0.1 filter")
        return img, stats
    stats["n_front"] = pts_opt.shape[1]
    proj = K @ pts_opt
    proj[:2, :] /= proj[2, :]
    u = proj[0, :].astype(np.int32)
    v = proj[1, :].astype(np.int32)
    h, w = img.shape[:2]
    mask = (u >= 0) & (u < w) & (v >= 0) & (v < h)
    if not np.any(mask):
        if debug:
            print("[lidar] projected points all out of image")
        return img, stats
    u = u[mask]
    v = v[mask]
    stats["n_inimg"] = u.shape[0]
    img[v, u] = color
    return img, stats
