from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import yaml


def _pick_first(rig: dict, prefix: str) -> Optional[Tuple[str, dict]]:
    for sensor in rig.get("sensors", []) or []:
        if not sensor.get("enabled", True):
            continue
        bp = sensor.get("blueprint", "")
        if bp.startswith(prefix):
            return sensor.get("id"), sensor
    return None


def generate_rviz_from_rig(
    rig_path: Path,
    ego_id: str,
    out_path: Path,
    camera_image_suffix: str = "image",
    lidar_cloud_suffix: str = "point_cloud",
) -> Path:
    rig = yaml.safe_load(Path(rig_path).read_text()) or {}
    cam = _pick_first(rig, "sensor.camera")
    lidar = _pick_first(rig, "sensor.lidar")

    cam_enabled = "true" if cam else "false"
    lidar_enabled = "true" if lidar else "false"
    cam_topic = f"/carla/{ego_id}/{cam[0]}/{camera_image_suffix}" if cam else ""
    lidar_topic = f"/carla/{ego_id}/{lidar[0]}/{lidar_cloud_suffix}" if lidar else ""

    template_path = Path(__file__).resolve().parent / "rviz" / "template_native.rviz"
    template = template_path.read_text()

    content = (
        template.replace("__FIXED_FRAME__", ego_id)
        .replace("__CAMERA_IMAGE_TOPIC__", cam_topic)
        .replace("__LIDAR_CLOUD_TOPIC__", lidar_topic)
        .replace("__CAMERA_ENABLED__", cam_enabled)
        .replace("__LIDAR_ENABLED__", lidar_enabled)
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content)
    return out_path
