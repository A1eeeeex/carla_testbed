from __future__ import annotations

import pytest

from carla_testbed.adapters.base import ADStackBackend
from carla_testbed.adapters.apollo import MockApolloBackend
from carla_testbed.contracts import ControlCommand


def test_mock_backend_matches_backend_protocol() -> None:
    backend = MockApolloBackend()

    assert isinstance(backend, ADStackBackend)
    assert backend.name == "apollo_mvp_mock"


def test_mock_backend_publish_poll_and_diagnostics() -> None:
    queued = ControlCommand(throttle=0.2, brake=0.0, steer=0.1, source="test")
    backend = MockApolloBackend(controls=[queued])
    frame_context = {"frame": 1, "speed_mps": 0.0}

    backend.prepare(context={"run_id": "unit"})
    backend.start()
    backend.publish_inputs(frame_context)

    command = backend.poll_control(timeout_s=0.01)
    assert command == queued
    assert backend.poll_control(timeout_s=0.01) is None
    assert backend.last_frame_context == frame_context

    diagnostics = backend.collect_diagnostics()
    payload = diagnostics.to_dict()
    assert payload["state"]["running"] is True
    assert payload["state"]["ready"] is True
    assert payload["state"]["metadata"]["queued_controls"] == 0
    assert payload["counters"]["publish_count"] == 1
    assert payload["counters"]["control_poll_count"] == 2
    assert payload["counters"]["control_return_count"] == 1

    backend.stop()
    assert backend.collect_diagnostics().state.running is False


def test_mock_backend_requires_start_before_publish() -> None:
    backend = MockApolloBackend()

    with pytest.raises(RuntimeError, match="before start"):
        backend.publish_inputs({"frame": 1})


def test_mock_backend_queue_control_validates_and_returns_command() -> None:
    backend = MockApolloBackend()
    command = ControlCommand(throttle=0.0, brake=1.0, steer=0.0)

    backend.start()
    backend.queue_control(command)

    assert backend.poll_control() == command
