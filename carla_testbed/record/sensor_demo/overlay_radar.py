from __future__ import annotations

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None
import numpy as np

from carla_testbed.record.sensor_demo.geometry import inverse_matrix
from carla_testbed.record.sensor_demo.overlay_lidar import ue_to_optical


def _vel_to_color(vel: float, vmax: float = 20.0):
    """Map radial velocity to BGR color (approaching=red, receding=blue)."""
    if vmax <= 0:
        vmax = 1.0
    v = max(-vmax, min(vmax, vel))
    # normalize to [0,1]
    t = 0.5 * (v / vmax + 1.0)
    # blue -> white -> red
    r = int(255 * t)
    b = int(255 * (1 - t))
    g = int(180 * (1 - abs(v) / vmax) + 50)
    return (b, g, r)


def _draw_marker(img, u, v, color, size=8, thickness=2, halo=True):
    if cv2 is None:
        return img
    c = (int(u), int(v))
    if halo:
        cv2.circle(img, c, size + 2, (0, 0, 0), -1, cv2.LINE_AA)
    cv2.rectangle(img, (c[0] - size, c[1] - size), (c[0] + size, c[1] + size), color, thickness, cv2.LINE_AA)
    cv2.line(img, (c[0] - size, c[1]), (c[0] + size, c[1]), color, thickness, cv2.LINE_AA)
    cv2.line(img, (c[0], c[1] - size), (c[0], c[1] + size), color, thickness, cv2.LINE_AA)


def draw_radar_sector(img, fov_deg: float = 30.0, color=(120, 120, 255)):
    """Draw light sector lines on the main image (optional)."""
    if cv2 is None:
        return img
    h, w = img.shape[:2]
    cx, cy = w // 2, int(h * 0.75)
    radius = int(min(w, h) * 0.45)
    angles = [-fov_deg / 2.0, fov_deg / 2.0]
    for ang in angles:
        rad = np.radians(ang)
        ex = int(cx + radius * np.sin(rad))
        ey = int(cy - radius * np.cos(rad))
        cv2.line(img, (cx, cy), (ex, ey), color, 1, cv2.LINE_AA)
    return img


def draw_radar_minimap(
    img: np.ndarray,
    depth: np.ndarray,
    az: np.ndarray,
    vel: np.ndarray,
    fov_deg: float,
    max_range_m: float,
    origin_px=(120, 120),
    radius_px: int = 90,
    rings_m=None,
    az_sign: int = 1,
    alt: np.ndarray = None,
    alt_limit: float = 0.2,
    v_deadband: float = 0.3,
):
    """Left-bottom mini-map showing radar FOV, range rings, and targets."""
    if cv2 is None or img is None:
        return img
    h, w = img.shape[:2]
    ox, oy = origin_px
    oy = h - oy  # flip to bottom
    radius_px = int(radius_px)
    if rings_m is None:
        if max_range_m <= 60:
            rings_m = [10, 20, 30]
        else:
            rings_m = [max_range_m * r for r in (0.2, 0.5, 0.8)]
    # background
    cv2.rectangle(
        img,
        (ox - radius_px - 8, oy - radius_px - 8),
        (ox + radius_px + 8, oy + radius_px + 8),
        (0, 0, 0),
        -1,
    )
    cv2.rectangle(
        img,
        (ox - radius_px - 8, oy - radius_px - 8),
        (ox + radius_px + 8, oy + radius_px + 8),
        (90, 90, 90),
        1,
    )
    # rings
    for r in rings_m:
        rp = int(radius_px * (r / max_range_m))
        if rp <= 0:
            continue
        cv2.circle(img, (ox, oy), rp, (160, 160, 160), 1, cv2.LINE_AA)
        cv2.putText(img, f"{r:.0f}m", (ox + rp - 24, oy - 6), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (200, 200, 200), 1, cv2.LINE_AA)
    # sector lines + forward axis
    cv2.line(img, (ox, oy), (ox, oy - radius_px), (80, 160, 255), 1, cv2.LINE_AA)
    for ang in [-fov_deg / 2.0, fov_deg / 2.0]:
        rad = np.radians(ang)
        ex = int(ox + radius_px * np.sin(rad))
        ey = int(oy - radius_px * np.cos(rad))
        cv2.line(img, (ox, oy), (ex, ey), (120, 120, 255), 1, cv2.LINE_AA)
    # ego marker
    cv2.circle(img, (ox, oy), 3, (255, 255, 255), -1, cv2.LINE_AA)
    cv2.line(img, (ox - 6, oy + 8), (ox, oy - 10), (255, 255, 255), 1, cv2.LINE_AA)
    cv2.line(img, (ox + 6, oy + 8), (ox, oy - 10), (255, 255, 255), 1, cv2.LINE_AA)
    # targets
    if depth is not None and az is not None and vel is not None:
        mask = (depth > 0) & (depth <= max_range_m)
        if alt is not None:
            mask &= np.abs(alt) <= alt_limit
        if az is not None:
            az_adj = az_sign * az
            mask &= np.abs(az_adj) <= np.radians(fov_deg / 2.0)
        else:
            az_adj = az
        if np.any(mask):
            d = depth[mask]
            a = az_adj[mask] if az_adj is not None else None
            v = vel[mask]
            if a is not None:
                x_fwd = d * np.cos(a)
                y_right = d * np.sin(a)
                px = (y_right / max_range_m * radius_px + ox).astype(int)
                py = (oy - x_fwd / max_range_m * radius_px).astype(int)
                for xi, yi, vi in zip(px, py, v):
                    color = (210, 210, 210) if abs(vi) < v_deadband else _vel_to_color(vi, vmax=15.0)
                    _draw_marker(img, int(xi), int(yi), color, size=5, thickness=2, halo=True)
    cv2.putText(img, "Radar", (ox - radius_px, oy + radius_px + 16), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (220, 220, 220), 1, cv2.LINE_AA)
    return img


def _parse_layout(dets: np.ndarray):
    """Heuristic mapping from raw detection columns to depth/az/alt/vel."""
    max_abs = [float(np.max(np.abs(dets[:, i]))) if dets.shape[0] > 0 else 0.0 for i in range(4)]
    # Default CARLA radar order is velocity, altitude, azimuth, depth
    if max_abs[3] > 5.0 and max_abs[0] < 5.0:
        return (3, 2, 1, 0), "vel,alt,az,depth(col3)"
    # Fallback: depth in col0, velocity in col3
    return (0, 1, 2, 3), "depth,az,alt(col0)"


def detections_to_points(dets: np.ndarray):
    """
    Convert radar detections to xyz (UE frame).
    Returns (points, layout_tag).
    """
    if dets.size == 0:
        return dets, "", np.array([], dtype=np.float32), np.array([], dtype=np.float32), np.array([], dtype=np.float32), np.array([], dtype=np.float32)
    (depth_idx, az_idx, alt_idx, vel_idx), layout_tag = _parse_layout(dets)
    depth = dets[:, depth_idx]
    az = dets[:, az_idx]
    alt = dets[:, alt_idx]
    vel = dets[:, vel_idx]
    # Heuristic: if angles look like degrees, convert
    if np.max(np.abs(az)) > 3.5:
        az = np.radians(az)
    if np.max(np.abs(alt)) > 3.5:
        alt = np.radians(alt)
    x = depth * np.cos(alt) * np.cos(az)
    y = depth * np.cos(alt) * np.sin(az)
    z = depth * np.sin(alt)
    return np.stack([x, y, z], axis=1), layout_tag, depth, az, alt, vel


def project_radar_to_image(
    img: np.ndarray,
    detections: np.ndarray,
    T_base_cam: np.ndarray,
    T_base_radar: np.ndarray,
    K: np.ndarray,
    color=(255, 0, 0),
    max_depth: float = 120.0,
    draw: bool = False,
    az_sign: int = 1,
    alt_limit: float = 0.2,
    v_deadband: float = 0.3,
):
    """Project radar detections into camera frame."""
    stats = {
        "n_det": 0,
        "n_front": 0,
        "n_inimg": 0,
        "uv": np.zeros((0, 2), dtype=np.int32),
        "mask_inimg": np.zeros((0,), dtype=bool),
        "depth": np.zeros((0,), dtype=np.float32),
        "vel": np.zeros((0,), dtype=np.float32),
        "az": np.zeros((0,), dtype=np.float32),
        "alt": np.zeros((0,), dtype=np.float32),
    }
    if cv2 is None or img is None:
        return img, stats
    if detections is None or detections.size == 0:
        return img, stats
    pts, layout_tag, depths, az, alt, vel = detections_to_points(detections)
    stats["layout"] = layout_tag
    stats["n_det"] = pts.shape[0]
    stats["depth"] = depths
    stats["vel"] = vel
    stats["az"] = az
    stats["alt"] = alt
    if pts.size == 0:
        return img, stats
    if depths.size > 0:
        stats["depth_min"] = float(np.min(depths))
        stats["depth_max"] = float(np.max(depths))
    if vel.size > 0:
        stats["vel_min"] = float(np.min(vel))
        stats["vel_max"] = float(np.max(vel))
    pts_h = np.concatenate([pts, np.ones((pts.shape[0], 1), dtype=np.float32)], axis=1).T
    # point_cam = inv(T_base_cam) @ T_base_radar @ point_radar
    T_radar_to_cam = inverse_matrix(T_base_cam) @ T_base_radar
    pts_cam = T_radar_to_cam @ pts_h
    pts_opt = ue_to_optical(pts_cam)
    zc = pts_opt[2, :]
    mask = zc > 0.1
    if max_depth is not None:
        mask &= zc < max_depth
    if alt is not None:
        mask &= np.abs(alt) <= alt_limit
    stats["n_front"] = int(np.count_nonzero(mask))
    if not np.any(mask):
        return img, stats
    pts_opt = pts_opt[:, mask]
    depths_masked = depths[mask]
    vel_masked = vel[mask]
    az_masked = az_sign * az[mask]
    alt_masked = alt[mask]
    proj = K @ pts_opt
    proj[:2, :] /= proj[2, :]
    u = proj[0, :].astype(np.int32)
    v = proj[1, :].astype(np.int32)
    h, w = img.shape[:2]
    inimg = (u >= 0) & (u < w) & (v >= 0) & (v < h)
    stats["n_inimg"] = int(np.count_nonzero(inimg))
    stats["uv"] = np.stack([u, v], axis=1)
    stats["mask_inimg"] = inimg
    stats["depth"] = depths_masked
    stats["vel"] = vel_masked
    stats["az"] = az_masked
    stats["alt"] = alt_masked
    if stats["n_inimg"] > 0:
        stats["u_min"], stats["u_max"] = int(u[inimg].min()), int(u[inimg].max())
        stats["v_min"], stats["v_max"] = int(v[inimg].min()), int(v[inimg].max())
    if draw and stats["n_inimg"] > 0:
        for ui, vi, vv in zip(u[inimg], v[inimg], vel_masked[inimg]):
            col = (220, 220, 220) if abs(vv) < v_deadband else _vel_to_color(float(vv))
            _draw_marker(img, int(ui), int(vi), col, size=6, thickness=2, halo=True)
    return img, stats


def draw_radar_targets_on_image(
    img: np.ndarray,
    radar_stats: dict,
    max_range_m: float,
    topk: int = 8,
    label_topk: int = 4,
    draw_arrow: bool = True,
    static_thresh: float = 0.3,
):
    if cv2 is None or img is None or not radar_stats:
        return img
    uv = radar_stats.get("uv")
    mask = radar_stats.get("mask_inimg")
    depth = radar_stats.get("depth")
    vel = radar_stats.get("vel")
    if uv is None or mask is None or depth is None or vel is None:
        return img
    idx = np.where(mask)[0]
    if idx.size == 0:
        return img
    order = np.argsort(depth[idx])
    idx = idx[order][:topk]
    for i, k in enumerate(idx):
        u, v = uv[k]
        d = float(depth[k])
        vv = float(vel[k]) if k < len(vel) else 0.0
        speed_abs = abs(vv)
        if speed_abs < static_thresh:
            color = (220, 220, 220)  # static
        else:
            color = _vel_to_color(vv, vmax=15.0)
        _draw_marker(img, int(u), int(v), color, size=10, thickness=2, halo=True)
        if draw_arrow and speed_abs >= static_thresh:
            length = int(6 + 24 * min(speed_abs / 15.0, 1.0))
            cv2.arrowedLine(
                img,
                (int(u), int(v)),
                (int(u), int(v - np.sign(vv) * length)),
                color,
                2,
                cv2.LINE_AA,
                tipLength=0.3,
            )
        if i < label_topk:
            cv2.putText(
                img,
                f"d={d:.1f} v={vv:+.1f}",
                (int(u) + 6, int(v) - 8 - 14 * i),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                color,
                2,
                cv2.LINE_AA,
            )
    return img
