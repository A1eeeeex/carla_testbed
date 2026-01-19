from __future__ import annotations

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None


def draw_hud(img, imu=None, gnss=None, events=None):
    if cv2 is None:
        return img
    h, w = img.shape[:2]
    y = 30
    cv2.putText(img, "sensor_demo", (20, y), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 1, cv2.LINE_AA)
    y += 24
    if imu:
        cv2.putText(
            img,
            f"IMU ang: {imu.get('gyroscope', {})} lin: {imu.get('accelerometer', {})}",
            (20, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (200, 200, 255),
            1,
            cv2.LINE_AA,
        )
        y += 22
    if gnss:
        cv2.putText(
            img,
            f"GNSS lat {gnss.get('latitude', 0):.6f} lon {gnss.get('longitude', 0):.6f} alt {gnss.get('altitude', 0):.1f}",
            (20, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (200, 255, 200),
            1,
            cv2.LINE_AA,
        )
        y += 22
    if events:
        for evt in events:
            cv2.putText(
                img,
                f"EVENT: {evt}",
                (20, y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (0, 0, 255),
                2,
                cv2.LINE_AA,
            )
            y += 24
    return img
