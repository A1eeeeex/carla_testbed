from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

import numpy as np

import carla

from carla_testbed.schemas import SensorSample
from carla_testbed.sensors.specs import SensorSpec
from carla_testbed.sensors.synchronizer import SensorSynchronizer, SensorSyncResult, SensorSyncStatus


def _latest_queue(maxsize: int = 5, on_overflow: Optional[Callable[[], None]] = None):
    from queue import Queue

    q = Queue(maxsize=maxsize)

    def put(data):
        if q.full():
            try:
                q.get_nowait()
                if on_overflow is not None:
                    on_overflow()
            except Exception:
                pass
        q.put_nowait(data)

    return q, put


@dataclass
class SensorRigStats:
    dropped: int = 0
    frames_saved: int = 0
    captured: int = 0
    missing: int = 0
    stale: int = 0
    future: int = 0
    exact: int = 0
    queue_overflow: int = 0
    per_sensor: Dict[str, Dict[str, int]] = field(default_factory=dict)
    frame_mismatches: List[Dict[str, int | str | None]] = field(default_factory=list)

    def _sensor_counts(self, sensor_id: str) -> Dict[str, int]:
        if sensor_id not in self.per_sensor:
            self.per_sensor[sensor_id] = {
                "captured": 0,
                "missing": 0,
                "stale": 0,
                "future": 0,
                "exact": 0,
                "queue_overflow": 0,
            }
        return self.per_sensor[sensor_id]

    def record_queue_overflow(self, sensor_id: str) -> None:
        self.queue_overflow += 1
        self.dropped += 1
        self._sensor_counts(sensor_id)["queue_overflow"] += 1

    def record_sync(self, sensor_id: str, result: SensorSyncResult) -> None:
        counts = self._sensor_counts(sensor_id)
        key = result.status.value
        if key == SensorSyncStatus.EXACT.value:
            self.exact += 1
            counts["exact"] += 1
            return
        if key == SensorSyncStatus.MISSING.value:
            self.missing += 1
            self.dropped += 1
            counts["missing"] += 1
        elif key == SensorSyncStatus.STALE.value:
            self.stale += 1
            counts["stale"] += 1
        elif key == SensorSyncStatus.FUTURE.value:
            self.future += 1
            counts["future"] += 1
        self.frame_mismatches.append(
            {
                "sensor_id": sensor_id,
                "status": key,
                "world_frame_id": result.world_frame_id,
                "sensor_frame_id": result.sensor_frame_id,
                "frame_delta": result.frame_delta,
            }
        )

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


class SensorRig:
    """Generic sensor rig driven by SensorSpec list."""

    def __init__(
        self,
        world: carla.World,
        ego: carla.Vehicle,
        specs: List[SensorSpec],
        out_dir: Path,
        enable_ros: bool = False,
        invert_tf: bool = True,
        ego_id: str = "hero",
    ):
        self.world = world
        self.ego = ego
        self.specs = specs
        self.out_dir = out_dir
        self.enable_ros = enable_ros
        self.invert_tf = invert_tf
        self.ego_id = ego_id
        self.entries = []  # list of dicts {spec, actor, queue, put, type}
        self.stats = SensorRigStats()
        self.synchronizer = SensorSynchronizer(tolerance_frames=0)

    def start(self):
        bp_lib = self.world.get_blueprint_library()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        meta: Dict[str, Dict] = {}
        for spec in self.specs:
            if not spec.enabled:
                continue
            bp_candidates = bp_lib.filter(spec.blueprint)
            bp = bp_candidates[0] if len(bp_candidates) > 0 else bp_lib.find(spec.blueprint)
            if spec.sensor_tick is not None:
                bp.set_attribute("sensor_tick", str(spec.sensor_tick))
            for k, v in spec.attributes.items():
                try:
                    bp.set_attribute(k, str(v))
                except Exception:
                    pass
            if self.enable_ros:
                for attr in ["ros_name", "role_name"]:
                    try:
                        bp.set_attribute(attr, spec.sensor_id)
                    except Exception:
                        pass
            raw_tf = spec.transform or {}
            # TODO(adapter/frame-conversion): invert_tf is a transport/frame convention
            # and should eventually move out of the core sensor rig.
            loc = carla.Location(
                x=raw_tf.get("x", 0.0),
                y=-(raw_tf.get("y", 0.0)) if self.invert_tf else raw_tf.get("y", 0.0),
                z=raw_tf.get("z", 0.0),
            )
            rot = carla.Rotation(
                roll=raw_tf.get("roll", 0.0),
                pitch=-(raw_tf.get("pitch", 0.0)) if self.invert_tf else raw_tf.get("pitch", 0.0),
                yaw=-(raw_tf.get("yaw", 0.0)) if self.invert_tf else raw_tf.get("yaw", 0.0),
            )
            tr = carla.Transform(loc, rot)
            if self.enable_ros:
                print(
                    f"[SensorRig] spawn {spec.sensor_id} enable_ros={self.enable_ros} "
                    f"transform=({loc.x:.2f},{loc.y:.2f},{loc.z:.2f}) "
                    f"rpy=({rot.roll:.2f},{rot.pitch:.2f},{rot.yaw:.2f})"
                )
            actor = self.world.spawn_actor(bp, tr, attach_to=self.ego)
            q, put = _latest_queue(
                on_overflow=lambda sensor_id=spec.sensor_id: self.stats.record_queue_overflow(sensor_id)
            )
            actor.listen(put)
            if self.enable_ros:
                try:
                    actor.enable_for_ros()
                except Exception:
                    pass
            entry = {"spec": spec, "actor": actor, "queue": q, "put": put, "type": spec.sensor_type}
            self.entries.append(entry)
            final_tf = {
                "x": tr.location.x,
                "y": tr.location.y,
                "z": tr.location.z,
                "roll": tr.rotation.roll,
                "pitch": tr.rotation.pitch,
                "yaw": tr.rotation.yaw,
            }
            meta[spec.sensor_id] = {
                "type": spec.sensor_type,
                "blueprint": spec.blueprint,
                "sensor_tick": spec.sensor_tick,
                "transform": final_tf,
                "transform_rig": spec.transform,
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

    def _record_sync_result(self, spec: SensorSpec, data, frame_id: int) -> SensorSyncResult:
        result = self.synchronizer.compare(world_frame_id=frame_id, sensor_sample=data)
        self.stats.record_sync(spec.sensor_id, result)
        return result

    def _build_sample(self, spec: SensorSpec, data, frame_id: int, timestamp: Optional[float]) -> Optional[SensorSample]:
        ts = float(timestamp) if timestamp is not None else getattr(data, "timestamp", None)
        sensor_frame = getattr(data, "frame", None)
        meta = {
            "carla_timestamp": getattr(data, "timestamp", None),
            "sensor_frame": sensor_frame,
            "world_frame": frame_id,
        }
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
                result = self.synchronizer.compare(world_frame_id=frame_id, sensor_sample=None)
                self.stats.record_sync(spec.sensor_id, result)
                continue
            sync_result = self._record_sync_result(spec, data, frame_id)
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
                    "sensor_frame": getattr(data, "frame", None),
                    "sync_status": sync_result.status.value,
                    "frame_delta": sync_result.frame_delta,
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
                    "sensor_frame": getattr(data, "frame", None),
                    "sync_status": sync_result.status.value,
                    "frame_delta": sync_result.frame_delta,
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
            self.stats.captured += 1
            self.stats._sensor_counts(spec.sensor_id)["captured"] += 1
            if return_samples:
                sample = self._build_sample(spec, data, frame_id, timestamp)
                if sample:
                    sample.meta.update(
                        {
                            "sync_status": sync_result.status.value,
                            "frame_delta": sync_result.frame_delta,
                            "sensor_frame": sync_result.sensor_frame_id,
                            "world_frame": sync_result.world_frame_id,
                        }
                    )
                    samples[spec.sensor_id] = sample
        if return_samples:
            return samples

    def stop(self):
        for entry in self.entries:
            spec = entry.get("spec")
            actor = entry.get("actor")
            if actor is None:
                continue
            try:
                actor.stop()
            except Exception as exc:
                print(f"[WARN] SensorRig stop failed sensor_id={getattr(spec, 'sensor_id', 'unknown')}: {exc}")
            try:
                actor.destroy()
            except Exception as exc:
                print(f"[WARN] SensorRig destroy failed sensor_id={getattr(spec, 'sensor_id', 'unknown')}: {exc}")
        self.entries = []
