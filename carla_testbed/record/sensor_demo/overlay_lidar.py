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


def _project_uv(K: np.ndarray, pts_opt: np.ndarray):
    proj = K @ pts_opt
    proj[:2, :] /= proj[2, :]
    u = proj[0, :].astype(np.int32)
    v = proj[1, :].astype(np.int32)
    return u, v


def project_lidar_to_image(
    img: np.ndarray,
    points: np.ndarray,
    T_base_cam: np.ndarray,
    T_base_lidar: np.ndarray,
    K: np.ndarray,
    color=(0, 255, 0),
    max_points: int = 8000,
    max_range: float = None,
    max_depth: float = 60.0,
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
        r = np.sqrt(np.sum(np.square(pts[:, :3]), axis=1, dtype=np.float64))
        pts = pts[r <= max_range]
    if max_points and pts.shape[0] > max_points:
        idx = np.linspace(0, pts.shape[0] - 1, max_points).astype(int)
        pts = pts[idx]
    stats["n_sampled"] = pts.shape[0]
    if pts.size == 0:
        return img, stats

    pts_h = np.concatenate([pts[:, :3], np.ones((pts.shape[0], 1), dtype=np.float32)], axis=1).T
    # base->sensor transforms; point_cam = inv(T_base_cam) @ T_base_lidar @ point_lidar
    T_lidar_to_cam = inverse_matrix(T_base_cam) @ T_base_lidar
    pts_cam = T_lidar_to_cam @ pts_h
    candidates = [
        ("optical_xyz=[y,-z,x]", ue_to_optical),
        ("optical_fallback_xyz=[x,-y,z]", lambda pc: np.vstack([pc[0, :], -pc[1, :], pc[2, :]])),
        ("optical_xyz=[x,y,z]", lambda pc: np.vstack([pc[0, :], pc[1, :], pc[2, :]])),
    ]
    h, w = img.shape[:2]
    for name, fn in candidates:
        pts_opt = fn(pts_cam)
        zc = pts_opt[2, :]
        front_mask = zc > 0.1
        if max_depth is not None:
            front_mask &= zc < max_depth
        pts_front = pts_opt[:, front_mask]
        if pts_front.shape[1] == 0:
            continue
        stats["n_front"] = pts_front.shape[1]
        u, v = _project_uv(K, pts_front)
        mask = (u >= 0) & (u < w) & (v >= 0) & (v < h)
        if np.any(mask):
            u = u[mask]
            v = v[mask]
            stats["n_inimg"] = u.shape[0]
            stats["mapping"] = name
            # Thicker points for visibility
            for ui, vi in zip(u, v):
                cv2.circle(img, (int(ui), int(vi)), 2, color, thickness=-1, lineType=cv2.LINE_AA)
            return img, stats
    if debug:
        print("[lidar] projected points all out of image for all mappings")
    return img, stats
