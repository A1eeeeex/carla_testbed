from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from carla_testbed.record.dual_cam import DualCamRecorder
from carla_testbed.record.hud import HudRenderer
from carla_testbed.record.sensor_demo.pipeline import SensorDemoRecorder


MODES = {
    "dual_cam": DualCamRecorder,
    "hud": HudRenderer,
    "sensor_demo": SensorDemoRecorder,
}


@dataclass
class RecordOptions:
    modes: List[str]
    output_dir: Optional[Path]
    fps: Optional[float]
    resolution: tuple[int, int]
    chase_distance: float
    chase_height: float
    chase_pitch: float
    max_lidar_points: int
    keep_frames: bool
    skip_lidar: bool
    skip_radar: bool
    skip_hud: bool


class RecordManager:
    """Dispatch recording/rendering modes."""

    def __init__(self, run_dir: Path, rig_resolved: dict, config_paths: Dict[str, Path], opts: RecordOptions):
        self.run_dir = Path(run_dir)
        self.rig_resolved = rig_resolved or {}
        self.config_paths = config_paths or {}
        self.opts = opts
        self.handlers = []

    def start(self, world=None, ego=None, client=None, dt: float = 0.05):
        self.handlers = []
        sensors_enabled = []
        for s in (self.rig_resolved.get("sensors") or []):
            if s.get("enabled", True):
                sensors_enabled.append(s.get("id"))
        print(f"[RecordManager] rig={self.rig_resolved.get('name')} sensors_enabled={sensors_enabled} modes={self.opts.modes}")
        for mode in self.opts.modes:
            cls = MODES.get(mode)
            if cls is None:
                warnings.warn(f"[RecordManager] Unknown record mode: {mode}")
                continue
            handler = cls(
                run_dir=self.run_dir,
                rig_resolved=self.rig_resolved,
                config_paths=self.config_paths,
                opts=self.opts,
                dt=dt,
            )
            try:
                handler.start(world=world, ego=ego, client=client)
                self.handlers.append(handler)
            except Exception as exc:
                warnings.warn(f"[RecordManager] start failed for mode {mode}: {exc}")

    def capture(self, frame_id: int, timestamp: float):
        for h in self.handlers:
            try:
                h.capture(frame_id, timestamp)
            except Exception:
                continue

    def stop(self):
        for h in self.handlers:
            try:
                h.stop()
            except Exception:
                continue

    def finalize(self, timeseries_path: Optional[Path], dt: float):
        for h in self.handlers:
            try:
                h.finalize(timeseries_path=timeseries_path, dt=dt)
            except Exception as exc:
                warnings.warn(f"[RecordManager] finalize failed for {h}: {exc}")
