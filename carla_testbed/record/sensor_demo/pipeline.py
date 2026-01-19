from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

import numpy as np

try:
    import cv2
except ImportError:  # pragma: no cover
    cv2 = None

from carla_testbed.record.sensor_demo.geometry import inverse_matrix, transform_to_matrix
from carla_testbed.record.sensor_demo.index import write_index
from carla_testbed.record.sensor_demo.overlay_hud import draw_hud
from carla_testbed.record.sensor_demo.overlay_lidar import project_lidar_to_image
from carla_testbed.record.sensor_demo.overlay_radar import draw_radar_sector


class SensorDemoRecorder:
    """Two-phase sensor demo: record CARLA log + offline render overlays."""

    def __init__(self, run_dir: Path, rig_resolved: dict, config_paths: Dict[str, Path], opts, dt: float):
        self.run_dir = Path(run_dir)
        self.rig_resolved = rig_resolved or {}
        self.config_paths = config_paths or {}
        self.opts = opts
        self.dt = dt
        self.recorder_file = self.run_dir / "replay" / "recording.log"
        self.frames_index = self.run_dir / "frames.jsonl"
        self.sensors_dir = self.run_dir / "sensors"
        base = opts.output_dir
        if base is None:
            base = self.run_dir / "video"
        else:
            base = Path(base)
            if not base.is_absolute():
                base = self.run_dir / base
        self.video_dir = base
        self.out_mp4 = self.video_dir / "demo.mp4"
        self._client = None

    def start(self, world=None, ego=None, client=None):
        self._client = client
        if client is not None:
            self.recorder_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                client.start_recorder(str(self.recorder_file))
            except Exception as exc:
                print(f"[sensor_demo] start_recorder failed: {exc}")

    def capture(self, frame_id: int, timestamp: float):
        return

    def stop(self):
        if self._client is not None:
            try:
                self._client.stop_recorder()
            except Exception:
                pass

    def finalize(self, timeseries_path: Optional[Path], dt: float):
        # Build index
        try:
            self.frames_index = write_index(self.run_dir)
        except Exception as exc:
            print(f"[sensor_demo] index failed: {exc}")
        try:
            self.render_demo()
        except Exception as exc:
            print(f"[sensor_demo] render failed: {exc}")

    # --- rendering helpers ---
    def _load_json(self, path: Path):
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}

    def _load_calib(self):
        return self._load_json(self.config_paths.get("calibration", self.run_dir / "config" / "calibration.json"))

    def _load_sensors(self):
        raw = self._load_json(self.config_paths.get("sensors", self.run_dir / "config" / "sensors_expanded.json"))
        if isinstance(raw, dict) and "sensors" in raw:
            return raw.get("sensors", {})
        if isinstance(raw, list):
            return {s.get("id"): s for s in raw if isinstance(s, dict)}
        return raw or {}

    def _camera_intrinsics(self, sensors: Dict[str, dict], camera_id: str):
        cam = sensors.get(camera_id, {})
        intr = cam.get("camera", {}).get("intrinsics", {}) if cam else {}
        if not intr:
            w = float(cam.get("attributes", {}).get("image_size_x", 1920))
            h = float(cam.get("attributes", {}).get("image_size_y", 1080))
            fx = fy = w / 2.0
            cx, cy = w / 2.0, h / 2.0
            intr = {"fx": fx, "fy": fy, "cx": cx, "cy": cy}
        K = np.array([[intr.get("fx", 1.0), 0, intr.get("cx", 0)], [0, intr.get("fy", 1.0), intr.get("cy", 0)], [0, 0, 1]])
        return K

    def _frame_matrices(self, calib: dict):
        mats = {}
        for fr in calib.get("frames", []):
            fid = fr.get("id")
            tr = fr.get("transform", {}) or {}
            mats[fid] = transform_to_matrix(tr)
        return mats

    def _load_events(self):
        events_path = self.run_dir / "events.jsonl"
        events = {}
        if not events_path.exists():
            return events
        for line in events_path.read_text().splitlines():
            try:
                e = json.loads(line)
                fid = e.get("frame_id")
                events.setdefault(fid, []).append(e.get("event_type", "event"))
            except Exception:
                continue
        return events

    def render_demo(self):
        if cv2 is None:
            print("[sensor_demo] OpenCV not installed, skip rendering.")
            return
        frames = []
        if self.frames_index.exists():
            for line in self.frames_index.read_text().splitlines():
                try:
                    frames.append(json.loads(line))
                except Exception:
                    continue
        sensors_meta = self._load_sensors()
        calib = self._load_calib()
        mats = self._frame_matrices(calib)
        events_by_frame = self._load_events()
        cam_id = None
        # pick a camera as base
        for sid, meta in sensors_meta.items():
            if meta.get("type") == "camera":
                cam_id = sid
                break
        if cam_id is None:
            print("[sensor_demo] No camera found; skip rendering.")
            return
        K = self._camera_intrinsics(sensors_meta, cam_id)
        T_cam = mats.get(cam_id, np.eye(4))
        lidar_id = next((sid for sid, m in sensors_meta.items() if m.get("type") == "lidar"), None)
        T_lidar = mats.get(lidar_id, np.eye(4)) if lidar_id else np.eye(4)
        self.video_dir.mkdir(parents=True, exist_ok=True)
        fps = self.opts.fps or (1.0 / self.dt if self.dt > 0 else 20.0)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        w, h = self.opts.resolution
        writer = cv2.VideoWriter(str(self.out_mp4), fourcc, fps, (w, h))
        if not writer.isOpened():
            print("[sensor_demo] VideoWriter cannot open output.")
            return

        # Build frame lookup
        per_frame = {}
        for item in frames:
            fid = item.get("frame_id")
            per_frame.setdefault(fid, []).append(item)

        warned_zero = False
        for fid in sorted(per_frame.keys()):
            entries = per_frame[fid]
            img_path = None
            lidar_path = None
            imu_data = None
            gnss_data = None
            for e in entries:
                sid = e.get("sensor_id")
                path = Path(e.get("path"))
                stype = sensors_meta.get(sid, {}).get("type")
                if sid == cam_id and path.suffix.lower() == ".png":
                    img_path = path
                elif stype == "lidar":
                    lidar_path = path
                elif stype == "imu":
                    try:
                        imu_data = json.loads(path.read_text())
                    except Exception:
                        imu_data = None
                elif stype == "gnss":
                    try:
                        gnss_data = json.loads(path.read_text())
                    except Exception:
                        gnss_data = None
            stats = {"lidar": None, "radar": "sector_only", "missing": []}
            if img_path and img_path.exists():
                frame = cv2.imread(str(img_path))
            else:
                frame = np.zeros((h, w, 3), dtype=np.uint8)
            if lidar_path and lidar_path.exists() and not self.opts.skip_lidar:
                pts = None
                if lidar_path.suffix.lower() == ".ply":
                    # lightweight ply reader
                    try:
                        import open3d as o3d
                        pc = o3d.io.read_point_cloud(str(lidar_path))
                        pts = np.asarray(pc.points)
                        if hasattr(pc, "point") and "intensity" in pc.point:
                            inten = np.asarray(pc.point["intensity"]).reshape(-1, 1)
                            pts = np.hstack([pts, inten])
                    except Exception:
                        pts = None
                if pts is None:
                    raw = lidar_path.read_bytes()
                    n = len(raw) // 16
                    if n > 0:
                        pts = np.frombuffer(raw[: n * 16], dtype=np.float32).reshape((-1, 4))
                    else:
                        pts = None
                if pts is not None and pts.size > 0:
                    frame, st = project_lidar_to_image(
                        frame,
                        pts,
                        T_base_cam=T_cam,
                        T_base_lidar=T_lidar,
                        K=K,
                        max_points=self.opts.max_lidar_points,
                        max_range=80.0,
                        debug=False,
                    )
                    stats["lidar"] = f"{st.get('n_inimg',0)}/{st.get('n_sampled',0)}"
                    if st.get("n_inimg", 0) == 0 and not warned_zero:
                        print("[sensor_demo] LiDAR projected 0 points; check calibration/transform or range filter.")
                        warned_zero = True
                else:
                    stats["lidar"] = "no_points"
            else:
                stats["lidar"] = "skipped"
            if not self.opts.skip_radar:
                frame = draw_radar_sector(frame)
            else:
                stats["radar"] = "disabled"
            if not self.opts.skip_hud:
                evts = events_by_frame.get(fid, [])
                ts_val = None
                if imu_data and "timestamp" in imu_data:
                    ts_val = imu_data.get("timestamp")
                elif gnss_data and "timestamp" in gnss_data:
                    ts_val = gnss_data.get("timestamp")
                frame = draw_hud(frame, frame_id=fid, timestamp=ts_val, imu=imu_data, gnss=gnss_data, events=evts, stats=stats)
            if self.opts.keep_frames:
                frames_dir = self.video_dir / "frames"
                frames_dir.mkdir(parents=True, exist_ok=True)
                cv2.imwrite(str(frames_dir / f"{fid:06d}.png"), frame)
            writer.write(frame)
        writer.release()
        print(f"[sensor_demo] wrote {self.out_mp4}")
