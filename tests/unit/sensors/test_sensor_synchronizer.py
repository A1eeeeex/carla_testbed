from __future__ import annotations

from carla_testbed.sensors.synchronizer import SensorSynchronizer, SensorSyncStatus


class FakeSample:
    def __init__(self, frame):
        self.frame = frame


def test_sensor_synchronizer_exact() -> None:
    result = SensorSynchronizer().compare(world_frame_id=10, sensor_sample=FakeSample(10))

    assert result.status == SensorSyncStatus.EXACT
    assert result.aligned
    assert result.frame_delta == 0


def test_sensor_synchronizer_stale() -> None:
    result = SensorSynchronizer().compare(world_frame_id=10, sensor_frame_id=8)

    assert result.status == SensorSyncStatus.STALE
    assert not result.aligned
    assert result.frame_delta == -2


def test_sensor_synchronizer_future() -> None:
    result = SensorSynchronizer().compare(world_frame_id=10, sensor_sample=FakeSample(12))

    assert result.status == SensorSyncStatus.FUTURE
    assert not result.aligned
    assert result.frame_delta == 2


def test_sensor_synchronizer_missing() -> None:
    result = SensorSynchronizer().compare(world_frame_id=10, sensor_sample=None)

    assert result.status == SensorSyncStatus.MISSING
    assert not result.aligned
    assert result.sensor_frame_id is None
    assert result.frame_delta is None


def test_sensor_synchronizer_tolerance_counts_as_exact() -> None:
    result = SensorSynchronizer(tolerance_frames=1).compare(world_frame_id=10, sensor_frame_id=9)

    assert result.status == SensorSyncStatus.EXACT
    assert result.frame_delta == -1
