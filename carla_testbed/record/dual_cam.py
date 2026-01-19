from __future__ import annotations

from pathlib import Path
from typing import Optional

import carla

from carla_testbed.record.video_recorder import DemoRecorder, ffmpeg_available, run_ffmpeg


class DualCamRecorder:
    """Capture dual camera streams and render mp4 (no HUD overlay here)."""

    def __init__(self, run_dir: Path, rig_resolved: dict, config_paths: dict, opts, dt: float):
        self.run_dir = Path(run_dir)
        self.opts = opts
        self.dt = dt
        self.out_dir = Path(opts.output_dir or (self.run_dir / "video")) / "dual_cam"
        self.recorder: Optional[DemoRecorder] = None

    def start(self, world: carla.World, ego: carla.Vehicle, client=None):
        w, h = self.opts.resolution
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.recorder = DemoRecorder(world=world, ego=ego, out_dir=self.out_dir, w=w, h=h, dt=self.dt)
        self.recorder.start()

    def capture(self, frame_id: int, timestamp: float):
        if self.recorder:
            self.recorder.capture(frame_id)

    def stop(self):
        if self.recorder:
            self.recorder.stop()

    def finalize(self, timeseries_path: Optional[Path], dt: float):
        if not self.recorder:
            return
        fps = self.opts.fps or (1.0 / dt if dt > 0 else 20.0)
        # Only raw mp4; HUD overlay handled by HudRenderer
        if ffmpeg_available():
            run_ffmpeg(self.recorder.raw_in, self.out_dir / "demo_in_car.mp4", fps=fps)
            run_ffmpeg(self.recorder.raw_tp, self.out_dir / "demo_third_person.mp4", fps=fps)
