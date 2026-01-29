from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
import math
from typing import Any, Dict, Optional

import yaml

from .base import Backend


class AutowareDirectBackend(Backend):
    """Backend for Mode-2: orchestrate Autoware container connecting directly to CARLA."""

    def __init__(self, profile: Dict[str, Any]):
        super().__init__(profile)
        self.compose_proc: Optional[subprocess.Popen] = None
        self._contract = self._load_contract()

    def _load_contract(self) -> Dict[str, Any]:
        contract_path = (
            Path(self.profile.get("contract", {}).get("canon_ros2", "io/contract/canon_ros2.yaml")).expanduser()
        )
        if not contract_path.is_absolute():
            contract_path = Path.cwd() / contract_path
        if contract_path.exists():
            with open(contract_path, "r") as f:
                return yaml.safe_load(f) or {}
        print(f"[WARN] contract file not found: {contract_path}")
        return {}

    def _load_rig(self) -> Dict[str, Any]:
        rig_cfg = self.profile.get("rig", {}) or {}
        rig_path = rig_cfg.get("rig_path")
        if not rig_path:
            return {}
        rig_path = Path(rig_path).expanduser()
        if not rig_path.is_absolute():
            rig_path = Path.cwd() / rig_path
        try:
            with open(rig_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception as exc:
            print(f"[WARN] failed to load rig {rig_path}: {exc}")
            return {}

    def _write_mapping_files(self, rig: Dict[str, Any]) -> None:
        cfg_dir = Path("algo/baselines/autoware/config/autoware_carla_interface")
        cfg_dir.mkdir(parents=True, exist_ok=True)
        mapping_path = cfg_dir / "sensor_mapping.yaml"
        calibration_path = cfg_dir / "sensor_kit_calibration.yaml"

        slots = self._contract.get("slots", {}) or {}
        mapping = {
            "front_camera": {
                "topic": slots.get("front_camera", {}).get("topic", "/carla/ego/front_camera/image"),
                "carla_sensor": "sensor.camera.rgb",
                "id": "front_camera",
            },
            "top_lidar": {
                "topic": slots.get("top_lidar", {}).get("topic", "/carla/ego/lidar_top/points"),
                "carla_sensor": "sensor.lidar.ray_cast",
                "id": "lidar_top",
            },
            "imu": {
                "topic": slots.get("imu", {}).get("topic", "/carla/ego/imu"),
                "carla_sensor": "sensor.other.imu",
                "id": "imu",
            },
            "gnss": {
                "topic": slots.get("gnss", {}).get("topic", "/carla/ego/gnss"),
                "carla_sensor": "sensor.other.gnss",
                "id": "gnss",
            },
        }
        mapping_path.write_text(yaml.safe_dump(mapping, sort_keys=False))

        sensors = rig.get("sensors", []) if isinstance(rig, dict) else []
        calibration_entries = {}
        for sensor in sensors:
            sid = sensor.get("id") or sensor.get("name")
            if not sid:
                continue
            pose = sensor.get("transform", sensor.get("pose", {})) or {}
            loc = pose if isinstance(pose, dict) else {}
            rot = pose if isinstance(pose, dict) else {}
            # fallback keys for nested location/rotation
            if not loc and isinstance(pose, dict):
                loc = pose.get("location", {}) or {}
                rot = pose.get("rotation", {}) or {}
            calibration_entries[sid] = {
                "translation": {
                    "x": float(loc.get("x", 0.0)),
                    "y": float(loc.get("y", 0.0)),
                    "z": float(loc.get("z", 0.0)),
                },
                "rotation": {
                    "roll": math.radians(float(rot.get("roll", 0.0))),
                    "pitch": math.radians(float(rot.get("pitch", 0.0))),
                    "yaw": math.radians(float(rot.get("yaw", 0.0))),
                },
            }
        if not calibration_entries:
            calibration_entries = {
                "front_camera": {"translation": {"x": 1.2, "y": 0.0, "z": 1.4}, "rotation": {"roll": 0.0, "pitch": 0.0, "yaw": 0.0}},
                "lidar_top": {"translation": {"x": 0.0, "y": 0.0, "z": 1.8}, "rotation": {"roll": 0.0, "pitch": 0.0, "yaw": 0.0}},
            }
        calibration_path.write_text(yaml.safe_dump(calibration_entries, sort_keys=False))
        print(f"[autoware] wrote mapping to {mapping_path} and calibration to {calibration_path}")

    def start(self) -> None:
        if self.profile.get("carla", {}).get("start_server", False):
            print("[TODO] starting CARLA server is not automated for autoware_direct; please start manually.")

        rig = self._load_rig()
        self._write_mapping_files(rig)

        compose_file = self.profile.get("autoware", {}).get("compose_file")
        if not compose_file:
            print("[WARN] compose_file missing in profile.autoware")
            return
        compose_path = Path(compose_file).resolve()
        if not compose_path.exists():
            print(f"[WARN] compose file not found: {compose_path}")
            return
        env = os.environ.copy()
        repo_root = Path(__file__).resolve().parents[2]
        env.setdefault("AUTOWARE_WORKSPACE", "/opt/Autoware")
        env["PYTHONPATH"] = f"{repo_root}:{env.get('PYTHONPATH','')}"
        cmd = ["docker", "compose", "-f", str(compose_path), "up", "-d"]
        print(f"[autoware] {' '.join(cmd)}")
        try:
            self.compose_proc = subprocess.Popen(cmd, env=env)
        except FileNotFoundError:
            print("[WARN] docker compose not available; skip container start")

    def health_check(self) -> bool:
        compose_file = self.profile.get("autoware", {}).get("compose_file")
        if not compose_file:
            return False
        compose_path = Path(compose_file).resolve()
        cmd = ["docker", "compose", "-f", str(compose_path), "ps"]
        try:
            out = subprocess.check_output(cmd, text=True)
            print(out)
            return True
        except Exception as exc:
            print(f"[WARN] docker compose ps failed: {exc}")
            return False

    def stop(self) -> None:
        compose_file = self.profile.get("autoware", {}).get("compose_file")
        if not compose_file:
            return
        compose_path = Path(compose_file).resolve()
        cmd = ["docker", "compose", "-f", str(compose_path), "down"]
        try:
            subprocess.check_call(cmd)
            print("[autoware] compose down")
        except Exception as exc:
            print(f"[WARN] failed to stop autoware compose: {exc}")
        self.compose_proc = None
