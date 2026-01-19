from __future__ import annotations

from pathlib import Path
from typing import Optional

from carla_testbed.record.video_recorder import render_overlay


class HudRenderer:
    """Render HUD overlays on existing dual_cam frames."""

    def __init__(self, run_dir: Path, rig_resolved: dict, config_paths: dict, opts, dt: float):
        self.run_dir = Path(run_dir)
        self.opts = opts
        self.dt = dt
        base = opts.output_dir
        if base is None:
            base = self.run_dir / "video"
        else:
            base = Path(base)
            if not base.is_absolute():
                base = self.run_dir / base
        self.base_dir = base / "dual_cam"

    def start(self, world=None, ego=None, client=None):
        # No capture required
        return

    def capture(self, frame_id: int, timestamp: float):
        return

    def stop(self):
        return

    def finalize(self, timeseries_path: Optional[Path], dt: float):
        if self.opts.skip_hud:
            return
        if timeseries_path is None or not timeseries_path.exists():
            print("[HUD] timeseries.csv missing, skip HUD rendering")
            return
        fps = self.opts.fps or (1.0 / dt if dt > 0 else 20.0)
        raw_in = self.base_dir / "raw_in"
        raw_tp = self.base_dir / "raw_tp"
        if not raw_in.exists() or not raw_tp.exists():
            print("[HUD] dual_cam frames not found, skip HUD rendering")
            return
        overlay_in = self.base_dir / "overlay_in"
        overlay_tp = self.base_dir / "overlay_tp"
        overlay_in.mkdir(parents=True, exist_ok=True)
        overlay_tp.mkdir(parents=True, exist_ok=True)
        render_overlay(raw_in, timeseries_path, overlay_in, self.base_dir / "demo_in_car_hud.mp4", fps=fps, frame_dt=dt)
        render_overlay(raw_tp, timeseries_path, overlay_tp, self.base_dir / "demo_third_person_hud.mp4", fps=fps, frame_dt=dt)
