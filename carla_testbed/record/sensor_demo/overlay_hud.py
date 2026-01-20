from __future__ import annotations

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None


def _fmt_vec(vec):
    if not isinstance(vec, dict):
        return ""
    return f"x={vec.get('x', 0):.2f} y={vec.get('y', 0):.2f} z={vec.get('z', 0):.2f}"


def draw_hud(img, frame_id=None, timestamp=None, imu=None, gnss=None, events=None, stats=None):
    """Lightweight HUD overlay: frame info + optional IMU/GNSS + events."""
    if cv2 is None:
        return img
    h, w = img.shape[:2]
    pad = 14
    y = pad + 10
    bar_h = 120
    cv2.rectangle(img, (pad, pad), (w - pad, pad + bar_h), (0, 0, 0), thickness=-1)
    cv2.rectangle(img, (pad, pad), (w - pad, pad + bar_h), (80, 80, 80), thickness=1)
    cv2.putText(img, "sensor_demo", (pad + 12, y), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2, cv2.LINE_AA)
    y += 26
    header = []
    if frame_id is not None:
        header.append(f"frame {frame_id}")
    if timestamp is not None:
        header.append(f"t={timestamp:.3f}s")
    if header:
        cv2.putText(img, " | ".join(header), (pad + 12, y), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (200, 200, 200), 1, cv2.LINE_AA)
        y += 22
    if imu:
        cv2.putText(
            img,
            f"IMU ang {_fmt_vec(imu.get('gyroscope', {}))} lin {_fmt_vec(imu.get('accelerometer', {}))}",
            (pad + 12, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.55,
            (200, 200, 255),
            1,
            cv2.LINE_AA,
        )
        y += 20
    if gnss:
        cv2.putText(
            img,
            f"GNSS lat {gnss.get('latitude', 0):.6f} lon {gnss.get('longitude', 0):.6f} alt {gnss.get('altitude', 0):.1f}",
            (pad + 12, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.55,
            (200, 255, 200),
            1,
            cv2.LINE_AA,
        )
        y += 20
    if stats:
        lidar_stat = stats.get("lidar")
        radar_stat = stats.get("radar")
        missing = stats.get("missing")
        if lidar_stat:
            cv2.putText(
                img,
                f"LiDAR {lidar_stat}",
                (pad + 12, y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.55,
                (0, 255, 0),
                1,
                cv2.LINE_AA,
            )
            y += 20
        if radar_stat:
            cv2.putText(
                img,
                f"Radar {radar_stat}",
                (pad + 12, y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.55,
                (255, 200, 0),
                1,
                cv2.LINE_AA,
            )
            y += 20
        radar_dbg = stats.get("radar_debug")
        if radar_dbg:
            cv2.putText(
                img,
                radar_dbg,
                (pad + 12, y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                (180, 180, 180),
                1,
                cv2.LINE_AA,
            )
            y += 18
        if missing:
            cv2.putText(
                img,
                f"Missing: {','.join(missing)}",
                (pad + 12, y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                (200, 150, 150),
                1,
                cv2.LINE_AA,
            )
            y += 18
    if events:
        for evt in events:
            cv2.putText(
                img,
                f"EVENT: {evt}",
                (pad + 12, y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (0, 0, 255),
                2,
                cv2.LINE_AA,
            )
            y += 22
    return img
