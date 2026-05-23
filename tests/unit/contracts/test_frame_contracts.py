from __future__ import annotations

import pytest

from carla_testbed.contracts import FrameStamp, is_monotonic


def test_frame_stamp_validation_and_dict() -> None:
    stamp = FrameStamp(frame_id=2, sim_time_s=0.1, wall_time_s=10.0)

    stamp.validate()

    assert stamp.to_dict() == {"frame_id": 2, "sim_time_s": 0.1, "wall_time_s": 10.0}


def test_frame_stamp_rejects_negative_values() -> None:
    with pytest.raises(ValueError, match="frame_id"):
        FrameStamp(frame_id=-1, sim_time_s=0.0).validate()
    with pytest.raises(ValueError, match="sim_time_s"):
        FrameStamp(frame_id=1, sim_time_s=-0.1).validate()
    with pytest.raises(ValueError, match="wall_time_s"):
        FrameStamp(frame_id=1, sim_time_s=0.0, wall_time_s=-1.0).validate()


def test_frame_monotonic_helper() -> None:
    assert is_monotonic(
        [
            FrameStamp(frame_id=1, sim_time_s=0.05, wall_time_s=1.0),
            FrameStamp(frame_id=2, sim_time_s=0.10, wall_time_s=1.1),
        ]
    )
    assert not is_monotonic(
        [
            FrameStamp(frame_id=2, sim_time_s=0.10, wall_time_s=1.1),
            FrameStamp(frame_id=1, sim_time_s=0.05, wall_time_s=1.0),
        ]
    )
