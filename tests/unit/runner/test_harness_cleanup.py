from __future__ import annotations

import pytest

pytest.importorskip("carla")

from carla_testbed.core.lifecycle import LifecycleManager
from carla_testbed.runner.harness import _lane_invasion_artifact_event, _register_harness_cleanup_resources


class FakeResource:
    def __init__(self, name: str, calls: list[str], fail_method: str | None = None):
        self.name = name
        self.calls = calls
        self.fail_method = fail_method

    def _call(self, method: str) -> None:
        self.calls.append(f"{self.name}.{method}")
        if self.fail_method == method:
            raise RuntimeError(f"{self.name} {method} failed")

    def stop(self) -> None:
        self._call("stop")

    def close(self) -> None:
        self._call("close")

    def teardown(self) -> None:
        self._call("teardown")


def test_harness_cleanup_registration_preserves_legacy_order() -> None:
    calls: list[str] = []
    manager = LifecycleManager()

    _register_harness_cleanup_resources(
        manager,
        col_src=FakeResource("collision", calls),
        inv_src=FakeResource("lane", calls),
        rig=FakeResource("rig", calls),
        ros_native=FakeResource("ros_native", calls),
        gt_pub=FakeResource("gt_pub", calls),
        rviz_launcher=FakeResource("rviz", calls),
        bag_recorder=FakeResource("bag", calls),
        fail_cap=FakeResource("fail_cap", calls),
        record_mgr=FakeResource("record_mgr", calls),
        monitor=FakeResource("monitor", calls),
        ts_rec=FakeResource("timeseries", calls),
        persist_gt_stats=lambda: calls.append("gt_stats.persist"),
    )

    errors = manager.cleanup_all()

    assert errors == ()
    assert calls == [
        "collision.stop",
        "lane.stop",
        "rig.stop",
        "ros_native.teardown",
        "gt_stats.persist",
        "gt_pub.close",
        "rviz.stop",
        "bag.stop",
        "fail_cap.stop",
        "record_mgr.stop",
        "monitor.close",
        "monitor.stop",
        "timeseries.close",
    ]


def test_harness_cleanup_continues_after_resource_error() -> None:
    calls: list[str] = []
    manager = LifecycleManager()

    _register_harness_cleanup_resources(
        manager,
        col_src=FakeResource("collision", calls),
        bag_recorder=FakeResource("bag", calls, fail_method="stop"),
        record_mgr=FakeResource("record_mgr", calls),
        ts_rec=FakeResource("timeseries", calls),
    )

    errors = manager.cleanup_all()

    assert calls == ["collision.stop", "bag.stop", "record_mgr.stop", "timeseries.close"]
    assert len(errors) == 1
    assert errors[0].name == "bag_recorder"
    assert isinstance(errors[0].error, RuntimeError)


def test_lane_invasion_artifact_event_preserves_crossed_marking_types() -> None:
    event = type(
        "LaneEvent",
        (),
        {
            "crossed_lane_markings": [
                type("Marking", (), {"type": "Solid"})(),
                type("Marking", (), {"type": "Broken"})(),
            ]
        },
    )()

    row = _lane_invasion_artifact_event(
        invasion=event,
        frame_id=42,
        timestamp=12.5,
        step=7,
        lane_invasion_count=2,
    )

    assert row["event_type"] == "lane_invasion"
    assert row["frame"] == 42
    assert row["t"] == 12.5
    assert row["step"] == 7
    assert row["lane_invasion_count"] == 2
    assert row["crossed_lane_marking_count"] == 2
    assert row["crossed_lane_marking_types"] == ["Solid", "Broken"]
