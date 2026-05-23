from __future__ import annotations

from pathlib import Path
from queue import Queue

import pytest

pytest.importorskip("carla")

from carla_testbed.sensors.rigs import SensorRig, SensorRigStats, _latest_queue
from carla_testbed.sensors.specs import SensorSpec


class FakeSensorData:
    def __init__(self, *, frame: int, timestamp: float = 0.0, raw_data: bytes = b"abc"):
        self.frame = frame
        self.timestamp = timestamp
        self.raw_data = raw_data


def _rig_with_entry(tmp_path: Path, q: Queue) -> SensorRig:
    spec = SensorSpec(sensor_id="radar_front", sensor_type="radar", blueprint="sensor.other.radar")
    rig = SensorRig.__new__(SensorRig)
    rig.out_dir = tmp_path
    rig.entries = [{"spec": spec, "actor": None, "queue": q, "put": None, "type": spec.sensor_type}]
    rig.stats = SensorRigStats()
    from carla_testbed.sensors.synchronizer import SensorSynchronizer

    rig.synchronizer = SensorSynchronizer(tolerance_frames=0)
    return rig


def test_sensor_rig_capture_records_exact_stats(tmp_path: Path) -> None:
    q: Queue = Queue()
    q.put(FakeSensorData(frame=5, timestamp=1.25))
    rig = _rig_with_entry(tmp_path, q)

    samples = rig.capture(frame_id=5, timestamp=1.25, return_samples=True)

    assert rig.stats.captured == 1
    assert rig.stats.frames_saved == 1
    assert rig.stats.exact == 1
    assert rig.stats.missing == 0
    assert rig.stats.per_sensor["radar_front"]["captured"] == 1
    assert rig.stats.to_dict()["captured"] == 1
    assert samples["radar_front"].meta["sync_status"] == "exact"
    assert samples["radar_front"].meta["frame_delta"] == 0


def test_sensor_rig_capture_records_stale_future_and_missing(tmp_path: Path) -> None:
    stale_q: Queue = Queue()
    stale_q.put(FakeSensorData(frame=4))
    stale_rig = _rig_with_entry(tmp_path / "stale", stale_q)
    stale_rig.capture(frame_id=5)

    future_q: Queue = Queue()
    future_q.put(FakeSensorData(frame=7))
    future_rig = _rig_with_entry(tmp_path / "future", future_q)
    future_rig.capture(frame_id=5)

    missing_rig = _rig_with_entry(tmp_path / "missing", Queue())
    missing_rig.capture(frame_id=5)

    assert stale_rig.stats.stale == 1
    assert stale_rig.stats.frame_mismatches[0]["status"] == "stale"
    assert stale_rig.stats.frame_mismatches[0]["frame_delta"] == -1
    assert future_rig.stats.future == 1
    assert future_rig.stats.frame_mismatches[0]["status"] == "future"
    assert future_rig.stats.frame_mismatches[0]["frame_delta"] == 2
    assert missing_rig.stats.missing == 1
    assert missing_rig.stats.dropped == 1


def test_latest_queue_records_overflow() -> None:
    stats = SensorRigStats()
    q, put = _latest_queue(maxsize=1, on_overflow=lambda: stats.record_queue_overflow("cam_front"))

    put(FakeSensorData(frame=1))
    put(FakeSensorData(frame=2))

    assert stats.queue_overflow == 1
    assert stats.dropped == 1
    assert stats.per_sensor["cam_front"]["queue_overflow"] == 1
    latest = q.get_nowait()
    assert latest.frame == 2
