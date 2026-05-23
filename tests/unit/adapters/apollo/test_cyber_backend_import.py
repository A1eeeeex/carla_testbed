from __future__ import annotations

import importlib

import pytest

from carla_testbed.adapters.apollo.cyber_backend import (
    ApolloCyberRTBackend,
    ApolloRuntimeUnavailableError,
)
from carla_testbed.adapters.apollo.time_sync import ApolloTimeAdapter
from carla_testbed.contracts import FrameStamp


def test_cyber_backend_import_does_not_require_cyber_runtime() -> None:
    module = importlib.import_module("carla_testbed.adapters.apollo.cyber_backend")

    assert module.ApolloCyberRTBackend is ApolloCyberRTBackend


def test_start_without_cyber_runtime_raises_clear_error(monkeypatch) -> None:
    real_import_module = importlib.import_module

    def fake_import_module(name: str, package: str | None = None):
        if name in {"cyber", "cyber_py.cyber"}:
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    backend = ApolloCyberRTBackend()
    with pytest.raises(ApolloRuntimeUnavailableError, match="Apollo CyberRT runtime is unavailable"):
        backend.start()


def test_time_adapter_outputs_sim_time_and_frame_id() -> None:
    adapter = ApolloTimeAdapter()

    payload = adapter.to_apollo_time(FrameStamp(frame_id=12, sim_time_s=3.4))

    assert payload == {"timestamp_sec": 3.4, "sequence_num": 12}
