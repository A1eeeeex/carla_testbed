from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

import carla

from carla_testbed.schemas import SensorSample
from carla_testbed.sensors.specs import SensorSpec


def _latest_queue(maxsize: int = 5):
    from queue import Queue

    q = Queue(maxsize=maxsize)

    def put(data):
        if q.full():
            try:
                q.get_nowait()
            except Exception:
                pass
        q.put_nowait(data)

    return q, put


@dataclass
class SensorRigStats:
    dropped: int = 0
    frames_saved: int = 0


class SensorRig:
    """Generic sensor rig driven by SensorSpec list."""

    def __init__(self, world: carla.World, ego: carla.Vehicle, specs: List[SensorSpec], out_dir: Path):
        self.world = world
        self.ego = ego
        self.specs = specs
        self.out_dir = out_dir
        self.entries = []  # list of dicts {spec, actor, queue, put, type}
        self.stats = SensorRigStats()

    def start(self):
        bp_lib = self.world.get_blueprint_library()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        meta: Dict[str, Dict] = {}
        for spec in self.specs:
            if not spec.enabled:
                continue
            bp = bp_lib.find(spec.blueprint)
            if spec.sensor_tick is not None:
                bp.set_attribute("sensor_tick", str(spec.sensor_tick))
            for k, v in spec.attributes.items():
                try:
                    bp.set_attribute(k, str(v))
                except Exception:
                    pass
            tr = carla.Transform(
                carla.Location(x=spec.transform.get("x", 0.0), y=spec.transform.get("y", 0.0), z=spec.transform.get("z", 0.0)),
                carla.Rotation(
                    roll=spec.transform.get("roll", 0.0),
                    pitch=spec.transform.get("pitch", 0.0),
                    yaw=spec.transform.get("yaw", 0.0),
                ),
            )
            actor = self.world.spawn_actor(bp, tr, attach_to=self.ego)
            q, put = _latest_queue()
            actor.listen(put)
            entry = {"spec": spec, "actor": actor, "queue": q, "put": put, "type": spec.sensor_type}
            self.entries.append(entry)
            meta[spec.sensor_id] = {
                "type": spec.sensor_type,
                "blueprint": spec.blueprint,
                "sensor_tick": spec.sensor_tick,
                "transform": spec.transform,
                "attributes": spec.attributes,
            }
        (self.out_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    def _fetch_latest(self, q):
        if q is None:
            return None
        data = None
        while not q.empty():
            cand = q.get()
            data = cand
        return data

    def _build_sample(self, spec: SensorSpec, data, frame_id: int, timestamp: Optional[float]) -> Optional[SensorSample]:
        ts = float(timestamp) if timestamp is not None else getattr(data, "timestamp", None)
        meta = {"carla_timestamp": getattr(data, "timestamp", None)}
        payload = None
        if spec.sensor_type == "camera":
            arr = np.frombuffer(bytes(data.raw_data), dtype=np.uint8).reshape((data.height, data.width, 4))
            payload = {"bgra": arr, "width": data.width, "height": data.height}
        elif spec.sensor_type == "lidar":
            pts = np.frombuffer(bytes(data.raw_data), dtype=np.float32).reshape((-1, 4))
            payload = {"points": pts}
        elif spec.sensor_type == "imu":
            def vec(v):
                return {"x": getattr(v, "x", 0.0), "y": getattr(v, "y", 0.0), "z": getattr(v, "z", 0.0)}
            payload = {
                "accelerometer": vec(getattr(data, "accelerometer", None)),
                "gyroscope": vec(getattr(data, "gyroscope", None)),
                "compass": getattr(data, "compass", None),
            }
        elif spec.sensor_type == "gnss":
            vel = getattr(data, "velocity", None)
            payload = {
                "latitude": getattr(data, "latitude", None),
                "longitude": getattr(data, "longitude", None),
                "altitude": getattr(data, "altitude", None),
                "velocity": {
                    "x": getattr(vel, "x", None),
                    "y": getattr(vel, "y", None),
                    "z": getattr(vel, "z", None),
                }
                if vel is not None
                else None,
            }
        elif spec.sensor_type == "radar":
            payload = {"raw_data": bytes(getattr(data, "raw_data", b""))}
            meta["note"] = "radar mapping pending"
        if payload is None:
            return None
        return SensorSample(
            sensor_id=spec.sensor_id,
            sensor_type=spec.sensor_type,
            frame_id=frame_id,
            timestamp=ts if ts is not None else 0.0,
            payload=payload,
            meta=meta,
        )

    def capture(self, frame_id: int, timestamp: Optional[float] = None, return_samples: bool = False):
        samples: Dict[str, SensorSample] = {}
        for entry in self.entries:
            spec: SensorSpec = entry["spec"]
            q = entry["queue"]
            data = self._fetch_latest(q)
            if data is None:
                self.stats.dropped += 1
                continue
            sensor_dir = self.out_dir / spec.sensor_id
            sensor_dir.mkdir(parents=True, exist_ok=True)
            fname = sensor_dir / f"{frame_id:06d}"
            if spec.sensor_type == "camera":
                data.save_to_disk(str(fname.with_suffix(".png")))
            elif spec.sensor_type == "lidar":
                try:
                    data.save_to_disk(str(fname.with_suffix(".ply")))
                except Exception:
                    fname.with_suffix(".bin").write_bytes(bytes(data.raw_data))
            elif spec.sensor_type == "radar":
                fname.with_suffix(".bin").write_bytes(bytes(data.raw_data))
            elif spec.sensor_type == "imu":
                def vec(v):
                    return {
                        "x": getattr(v, "x", None),
                        "y": getattr(v, "y", None),
                        "z": getattr(v, "z", None),
                    } if v is not None else {"x": None, "y": None, "z": None}
                imu_dict = {
                    "frame": frame_id,
                    "timestamp": getattr(data, "timestamp", None),
                    "accelerometer": vec(getattr(data, "accelerometer", None)),
                    "gyroscope": vec(getattr(data, "gyroscope", None)),
                    "compass": getattr(data, "compass", None),
                }
                fname.with_suffix(".json").write_text(json.dumps(imu_dict))
            elif spec.sensor_type == "gnss":
                vel = getattr(data, "velocity", None)
                vel_dict = {
                    "x": getattr(vel, "x", None),
                    "y": getattr(vel, "y", None),
                    "z": getattr(vel, "z", None),
                } if vel is not None else {"x": None, "y": None, "z": None}
                gnss_dict = {
                    "frame": frame_id,
                    "timestamp": getattr(data, "timestamp", None),
                    "latitude": getattr(data, "latitude", None),
                    "longitude": getattr(data, "longitude", None),
                    "altitude": getattr(data, "altitude", None),
                    "velocity": vel_dict,
                }
                fname.with_suffix(".json").write_text(json.dumps(gnss_dict))
            else:
                # generic binary dump
                fname.with_suffix(".bin").write_bytes(bytes(getattr(data, "raw_data", b"")))
            self.stats.frames_saved += 1
            if return_samples:
                sample = self._build_sample(spec, data, frame_id, timestamp)
                if sample:
                    samples[spec.sensor_id] = sample
        if return_samples:
            return samples

    def stop(self):
        for entry in self.entries:
            actor = entry.get("actor")
            if actor is None:
                continue
            try:
                actor.stop()
            except Exception:
                pass
            try:
                actor.destroy()
            except Exception:
                pass
        self.entries = []
