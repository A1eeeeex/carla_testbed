from __future__ import annotations

import csv
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Tuple

import carla


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def run_ffmpeg(frames_dir: Path, out_mp4: Path, fps: float) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-framerate",
        str(fps),
        "-start_number",
        "0",
        "-i",
        str(frames_dir / "%06d.png"),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        str(out_mp4),
    ]
    subprocess.run(cmd, check=True)


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
) -> None:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        print("[WARN] pillow 未安装，跳过 HUD 生成。安装: pip install pillow")
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

    def load_font(size):
        for name in ["DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]:
            try:
                return ImageFont.truetype(name, size)
            except Exception:
                continue
        return ImageFont.load_default()

    font_big = load_font(58)
    font_med = load_font(30)
    font_small = load_font(22)

    def fmt(v):
        try:
            x = float(v)
            return f"{x:.1f}"
        except Exception:
            return str(v)

    total_frames = len(rows)

    for idx, row in enumerate(rows):
        img_path = frames_dir / f"{idx:06d}.png"
        if not img_path.exists():
            continue
        img = Image.open(img_path).convert("RGBA")
        draw = ImageDraw.Draw(img, "RGBA")
        w, h = img.size
        margin_x, margin_y = 24, 20
        bar_h = 150
        pad = 18
        rect = (margin_x, h - bar_h - margin_y, w - margin_x, h - margin_y)
        draw.rectangle(rect, fill=(0, 0, 0, hud_bg_alpha))
        draw.rectangle(rect, outline=(255, 255, 255, 80), width=2)

        text_y = rect[1] + pad
        speed = fmt(row.get("dbg_v", row.get("v_mps", 0.0)))
        draw.text((rect[0] + pad, text_y), f"Speed {speed} m/s", font=font_big, fill=(255, 255, 255, 255))
        text_y += 58
        gap = fmt(row.get("dbg_gap_euclid", row.get("gap_m", "inf")))
        draw.text((rect[0] + pad, text_y), f"Gap {gap} m", font=font_med, fill=(200, 200, 200, 255))
        text_y += 32
        phase = f"t {fmt(row.get('t'))}s | f {row.get('frame')}"
        draw.text((rect[0] + pad, text_y), phase, font=font_small, fill=(180, 180, 180, 255))

        # progress bar
        img.save(overlay_dir / f"{idx:06d}.png")

    if ffmpeg_available():
        run_ffmpeg(overlay_dir, out_mp4, fps=fps)
    else:
        print("[WARN] ffmpeg 未安装，HUD overlay 仅输出 png 序列。")


class DemoRecorder:
    """Dual-camera recorder (in-car + third-person) with optional HUD overlay."""

    def __init__(self, world: carla.World, ego: carla.Vehicle, out_dir: Path, w: int = 1920, h: int = 1080, fov: float = 90.0, dt: float = 0.05):
        self.world = world
        self.ego = ego
        self.out_dir = out_dir
        self.w = w
        self.h = h
        self.fov = fov
        self.dt = dt
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
        self.cam_in = self.world.spawn_actor(cam_bp, tr_in, attach_to=self.ego)
        self.cam_tp = self.world.spawn_actor(cam_bp, tr_tp, attach_to=self.ego)
        self.q_in, self.put_in = make_latest_queue()
        self.q_tp, self.put_tp = make_latest_queue()
        self.cam_in.listen(self.put_in)
        self.cam_tp.listen(self.put_tp)
        for p in [self.raw_in, self.raw_tp, self.overlay_in, self.overlay_tp]:
            p.mkdir(parents=True, exist_ok=True)

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
        if img_in:
            img_in.save_to_disk(str(self.raw_in / f"{self.frames_written:06d}.png"))
        if img_tp:
            img_tp.save_to_disk(str(self.raw_tp / f"{self.frames_written:06d}.png"))
        self.frames_written += 1

    def finalize(self, csv_path: Optional[Path], make_hud: bool, fps: float):
        if make_hud and csv_path is not None and csv_path.exists():
            render_overlay(self.raw_in, csv_path, self.overlay_in, self.out_dir / "demo_in_car_hud.mp4", fps=fps, frame_dt=self.dt)
            render_overlay(self.raw_tp, csv_path, self.overlay_tp, self.out_dir / "demo_third_person_hud.mp4", fps=fps, frame_dt=self.dt)
        if ffmpeg_available():
            run_ffmpeg(self.raw_in, self.out_dir / "demo_in_car.mp4", fps=fps)
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
