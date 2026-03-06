from __future__ import annotations

import csv
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Tuple

import carla
try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None

from carla_testbed.record.sensor_demo.hud_renderer import HUDRenderer


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def run_ffmpeg(frames_dir: Path, out_mp4: Path, fps: float) -> None:
    frames = sorted(frames_dir.glob("*.png"))
    if not frames:
        raise RuntimeError(f"no png frames found in {frames_dir}")
    seq_dir = frames_dir.parent / f"._ffmpeg_seq_{frames_dir.name}"
    if seq_dir.exists():
        shutil.rmtree(seq_dir, ignore_errors=True)
    seq_dir.mkdir(parents=True, exist_ok=True)
    try:
        for idx, src in enumerate(frames):
            dst = seq_dir / f"{idx:06d}.png"
            try:
                os.link(src, dst)
            except Exception:
                shutil.copyfile(src, dst)
        cmd = [
            "ffmpeg",
            "-y",
            "-framerate",
            str(fps),
            "-start_number",
            "0",
            "-i",
            str(seq_dir / "%06d.png"),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(out_mp4),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            msg = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeError(f"ffmpeg encode failed ({proc.returncode}): {msg[:400]}")
    finally:
        shutil.rmtree(seq_dir, ignore_errors=True)


def make_cam_bp(bp_lib: carla.BlueprintLibrary, w: int, h: int, fov: float) -> carla.ActorBlueprint:
    cam_bp = bp_lib.find("sensor.camera.rgb")
    cam_bp.set_attribute("image_size_x", str(w))
    cam_bp.set_attribute("image_size_y", str(h))
    cam_bp.set_attribute("fov", str(fov))
    return cam_bp


def make_latest_queue(maxsize: int = 5):
    from queue import Queue

    q = Queue(maxsize=maxsize)

    def _put(data):
        try:
            q.put_nowait(data)
        except Exception:
            try:
                q.get_nowait()
            except Exception:
                pass
            try:
                q.put_nowait(data)
            except Exception:
                pass

    return q, _put


def render_overlay(
    frames_dir: Path,
    csv_path: Path,
    overlay_dir: Path,
    out_mp4: Path,
    fps: float,
    window_sec: int = 12,
    hud_bg_alpha: int = 110,
    frame_dt: Optional[float] = None,
    hud_mode: str = "driving",
    hud_col_width: int = 360,
) -> None:
    if cv2 is None:
        print("[WARN] OpenCV 未安装，跳过 HUD 生成。安装: pip install opencv-python")
        return
    rows = []
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    if not rows:
        print("[WARN] timeseries.csv 为空，跳过 HUD。")
        return

    overlay_dir.mkdir(parents=True, exist_ok=True)
    renderer = HUDRenderer(
        {
            "hud_mode": hud_mode,
            "col_w": hud_col_width,
        }
    )

    fps_val = float(fps) if fps and fps > 0 else 20.0
    frame_interval = frame_dt if frame_dt and frame_dt > 0 else (1.0 / fps_val)
    for idx, row in enumerate(rows):
        img_path = frames_dir / f"{idx:06d}.png"
        if not img_path.exists():
            continue
        frame = cv2.imread(str(img_path))
        if frame is None:
            continue
        metrics = dict(row)
        if metrics.get("timestamp") in (None, ""):
            metrics["timestamp"] = idx * frame_interval
        if metrics.get("fps") in (None, ""):
            metrics["fps"] = fps_val
        out = renderer.render(frame, metrics)
        cv2.imwrite(str(overlay_dir / f"{idx:06d}.png"), out)

    if ffmpeg_available():
        run_ffmpeg(overlay_dir, out_mp4, fps=fps)
    else:
        print("[WARN] ffmpeg 未安装，HUD overlay 仅输出 png 序列。")


class DemoRecorder:
    """Dual-camera recorder (in-car + third-person) with optional HUD overlay."""

    def __init__(
        self,
        world: carla.World,
        ego: carla.Vehicle,
        out_dir: Path,
        w: int = 1920,
        h: int = 1080,
        fov: float = 90.0,
        dt: float = 0.05,
        enable_in_car: bool = True,
        enable_third_person: bool = True,
    ):
        self.world = world
        self.ego = ego
        self.out_dir = out_dir
        self.w = w
        self.h = h
        self.fov = fov
        self.dt = dt
        self.enable_in_car = bool(enable_in_car)
        self.enable_third_person = bool(enable_third_person)
        self.cam_in = None
        self.cam_tp = None
        self.q_in = None
        self.put_in = None
        self.q_tp = None
        self.put_tp = None
        self.frames_written = 0
        self.raw_in = out_dir / "raw_in"
        self.raw_tp = out_dir / "raw_tp"
        self.overlay_in = out_dir / "overlay_in"
        self.overlay_tp = out_dir / "overlay_tp"

    def start(self):
        bp_lib = self.world.get_blueprint_library()
        cam_bp = make_cam_bp(bp_lib, self.w, self.h, self.fov)
        tr_in = carla.Transform(carla.Location(x=0.35, y=-0.35, z=1.20))
        tr_tp = carla.Transform(carla.Location(x=-7.5, y=0.0, z=3.0), carla.Rotation(pitch=-12.0, yaw=0.0, roll=0.0))
        if self.enable_in_car:
            self.cam_in = self.world.spawn_actor(cam_bp, tr_in, attach_to=self.ego)
            self.q_in, self.put_in = make_latest_queue()
            self.cam_in.listen(self.put_in)
            self.raw_in.mkdir(parents=True, exist_ok=True)
            self.overlay_in.mkdir(parents=True, exist_ok=True)
        if self.enable_third_person:
            self.cam_tp = self.world.spawn_actor(cam_bp, tr_tp, attach_to=self.ego)
            self.q_tp, self.put_tp = make_latest_queue()
            self.cam_tp.listen(self.put_tp)
            self.raw_tp.mkdir(parents=True, exist_ok=True)
            self.overlay_tp.mkdir(parents=True, exist_ok=True)

    def _fetch_latest(self, q) -> Optional[any]:
        if q is None:
            return None
        latest = None
        while not q.empty():
            latest = q.get()
        return latest

    def capture(self, frame_id: int):
        img_in = self._fetch_latest(self.q_in)
        img_tp = self._fetch_latest(self.q_tp)
        if img_in and self.enable_in_car:
            img_in.save_to_disk(str(self.raw_in / f"{self.frames_written:06d}.png"))
        if img_tp and self.enable_third_person:
            img_tp.save_to_disk(str(self.raw_tp / f"{self.frames_written:06d}.png"))
        self.frames_written += 1

    def finalize(self, csv_path: Optional[Path], make_hud: bool, fps: float):
        if make_hud and self.enable_in_car and csv_path is not None and csv_path.exists():
            render_overlay(self.raw_in, csv_path, self.overlay_in, self.out_dir / "demo_in_car_hud.mp4", fps=fps, frame_dt=self.dt)
        if make_hud and self.enable_third_person and csv_path is not None and csv_path.exists():
            render_overlay(self.raw_tp, csv_path, self.overlay_tp, self.out_dir / "demo_third_person_hud.mp4", fps=fps, frame_dt=self.dt)
        if ffmpeg_available():
            if self.enable_in_car:
                run_ffmpeg(self.raw_in, self.out_dir / "demo_in_car.mp4", fps=fps)
            if self.enable_third_person:
                run_ffmpeg(self.raw_tp, self.out_dir / "demo_third_person.mp4", fps=fps)
        else:
            print("[WARN] ffmpeg 未安装，raw 视频仅输出 png 序列。")

    def stop(self):
        for cam in [self.cam_in, self.cam_tp]:
            if cam is None:
                continue
            try:
                cam.stop()
            except Exception:
                pass
            try:
                cam.destroy()
            except Exception:
                pass
        self.cam_in = self.cam_tp = None
