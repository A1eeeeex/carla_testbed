from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

import carla

from carla_testbed.record.video_recorder import DemoRecorder, ffmpeg_available, render_overlay, run_ffmpeg


class DualCamRecorder:
    """Capture dual-camera streams and export mp4, with optional HUD overlay."""

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
        self.out_dir = base / "dual_cam"
        self.recorder: Optional[DemoRecorder] = None

    def start(self, world: carla.World, ego: carla.Vehicle, client=None):
        w, h = self.opts.resolution
        self.out_dir.mkdir(parents=True, exist_ok=True)
        third_person_only = bool(getattr(self.opts, "dual_cam_third_person_only", False))
        self.recorder = DemoRecorder(
            world=world,
            ego=ego,
            out_dir=self.out_dir,
            w=w,
            h=h,
            dt=self.dt,
            enable_in_car=not third_person_only,
            enable_third_person=True,
        )
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
        make_hud = bool(not getattr(self.opts, "skip_hud", False) and timeseries_path is not None and timeseries_path.exists())
        third_person_only = bool(getattr(self.opts, "dual_cam_third_person_only", False))
        if make_hud:
            if not third_person_only:
                render_overlay(
                    self.recorder.raw_in,
                    timeseries_path,
                    self.recorder.overlay_in,
                    self.out_dir / "demo_in_car_hud.mp4",
                    fps=fps,
                    frame_dt=self.dt,
                    hud_mode=str(getattr(self.opts, "hud_mode", "driving") or "driving"),
                    hud_col_width=int(getattr(self.opts, "hud_col_width", 360) or 360),
                )
            render_overlay(
                self.recorder.raw_tp,
                timeseries_path,
                self.recorder.overlay_tp,
                self.out_dir / "demo_third_person_hud.mp4",
                fps=fps,
                frame_dt=self.dt,
                hud_mode=str(getattr(self.opts, "hud_mode", "driving") or "driving"),
                hud_col_width=int(getattr(self.opts, "hud_col_width", 360) or 360),
            )
        if ffmpeg_available():
            if not third_person_only:
                run_ffmpeg(self.recorder.raw_in, self.out_dir / "demo_in_car.mp4", fps=fps)
            run_ffmpeg(self.recorder.raw_tp, self.out_dir / "demo_third_person.mp4", fps=fps)
        if third_person_only and make_hud:
            hud_mp4 = self.out_dir / "demo_third_person_hud.mp4"
            raw_mp4 = self.out_dir / "demo_third_person.mp4"
            raw_backup_mp4 = self.out_dir / "demo_third_person_raw.mp4"
            if hud_mp4.exists():
                if raw_mp4.exists():
                    raw_mp4.replace(raw_backup_mp4)
                shutil.copyfile(hud_mp4, raw_mp4)
