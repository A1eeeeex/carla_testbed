from __future__ import annotations

import csv
from collections import deque
from pathlib import Path
from typing import List, Optional, Tuple

import carla

from carla_testbed.record.video_recorder import render_overlay, run_ffmpeg, ffmpeg_available


class FailFrameCapture:
    """
    保留最近窗口的相机帧；失败时导出“失败前 3s”带 HUD 的视频（同时输出 raw png 序列）。
    """

    def __init__(self, world: carla.World, ego: carla.Vehicle, out_dir: Path, cam_w: int = 1920, cam_h: int = 1080, fov: float = 90.0, window_sec: float = 3.0, dt: float = 0.05):
        self.world = world
        self.ego = ego
        self.out_dir = out_dir
        self.cam_w = cam_w
        self.cam_h = cam_h
        self.fov = fov
        self.window_sec = window_sec
        self.dt = dt
        self.cam: Optional[carla.Sensor] = None
        self.q = deque(maxlen=max(10, int(window_sec / dt) + 10))
        self._start()

    def _start(self):
        bp = self.world.get_blueprint_library().find("sensor.camera.rgb")
        bp.set_attribute("image_size_x", str(self.cam_w))
        bp.set_attribute("image_size_y", str(self.cam_h))
        bp.set_attribute("fov", str(self.fov))
        tr = carla.Transform(carla.Location(x=0.35, y=-0.35, z=1.20))
        self.cam = self.world.spawn_actor(bp, tr, attach_to=self.ego)

        def put(data: carla.Image):
            self.q.append(data)

        self.cam.listen(put)

    def capture(self):
        # listener already appends; nothing else needed per tick
        return

    def _dump_frames(self, frames: List[carla.Image], raw_dir: Path) -> None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        for idx, img in enumerate(frames):
            img.save_to_disk(str(raw_dir / f"{idx:06d}.png"))

    def _subset_timeseries(self, ts_csv_path: Path, frame_ids: List[int], out_csv: Path):
        if not ts_csv_path.exists():
            return
        keep = set(frame_ids)
        rows: List[dict] = []
        with ts_csv_path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                try:
                    f_id = int(r.get("frame", -1))
                except Exception:
                    continue
                if f_id in keep:
                    rows.append(r)
        if not rows:
            return
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        with out_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    def save(self, frame_id: Optional[int], timestamp: Optional[float], ts_csv_path: Optional[Path]) -> Optional[Path]:
        if not self.q:
            return None
        now_ts = timestamp if timestamp is not None else (self.q[-1].timestamp if hasattr(self.q[-1], "timestamp") else None)
        raw_dir = self.out_dir / "fail_window" / "raw"
        overlay_dir = self.out_dir / "fail_window" / "overlay"
        out_mp4 = self.out_dir / "fail_window_hud.mp4"
        out_raw_mp4 = self.out_dir / "fail_window_raw.mp4"

        # 选择窗口内的帧
        if now_ts is not None:
            frames = [img for img in self.q if hasattr(img, "timestamp") and img.timestamp >= now_ts - self.window_sec]
        else:
            max_frames = int(self.window_sec / self.dt) if self.dt > 0 else len(self.q)
            frames = list(self.q)[-max_frames:]
        if not frames:
            frames = [self.q[-1]]

        self._dump_frames(frames, raw_dir)
        frame_ids = [getattr(img, "frame", None) for img in frames if hasattr(img, "frame")]
        subset_csv = self.out_dir / "fail_window" / "timeseries_subset.csv"
        if ts_csv_path:
            self._subset_timeseries(ts_csv_path, [fid for fid in frame_ids if fid is not None], subset_csv)

        if ffmpeg_available():
            try:
                run_ffmpeg(raw_dir, out_raw_mp4, fps=1.0 / self.dt if self.dt > 0 else 20.0)
            except Exception as e:
                print(f"[WARN] fail_window raw video ffmpeg failed: {e}")
        if subset_csv.exists():
            try:
                render_overlay(raw_dir, subset_csv, overlay_dir, out_mp4, fps=1.0 / self.dt if self.dt > 0 else 20.0)
            except Exception as e:
                print(f"[WARN] fail_window HUD render failed: {e}")
        return out_mp4 if out_mp4.exists() else None

    def stop(self):
        if self.cam is None:
            return
        try:
            self.cam.stop()
        except Exception:
            pass
        try:
            self.cam.destroy()
        except Exception:
            pass
        self.cam = None
