from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from carla_testbed.platform.carla_session import (
    Phase1CarlaStartupError,
    start_phase1_carla_session,
)


def test_start_phase1_carla_session_stops_launcher_when_not_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    carla_root = tmp_path / "CARLA_0.9.16"
    carla_root.mkdir()

    class FakeLauncher:
        last_instance: "FakeLauncher | None" = None

        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.stopped = False
            FakeLauncher.last_instance = self

        def start(self) -> None:
            return None

        def wait_ready(self, *, timeout_s: float, poll_s: float) -> bool:
            return False

        def diagnostics_snapshot(self, *, probe_rpc: bool) -> dict[str, object]:
            return {
                "probe_rpc": probe_rpc,
                "rpc_handshake_ready": False,
                "process_alive": True,
            }

        def stop(self) -> None:
            self.stopped = True

    tbio_mod = types.ModuleType("tbio")
    carla_mod = types.ModuleType("tbio.carla")
    launcher_mod = types.ModuleType("tbio.carla.launcher")
    launcher_mod.CarlaLauncher = FakeLauncher
    monkeypatch.setitem(sys.modules, "tbio", tbio_mod)
    monkeypatch.setitem(sys.modules, "tbio.carla", carla_mod)
    monkeypatch.setitem(sys.modules, "tbio.carla.launcher", launcher_mod)

    with pytest.raises(Phase1CarlaStartupError):
        start_phase1_carla_session(
            out_dir=tmp_path / "session",
            carla_root=carla_root,
            town="Town01",
            timeout_s=0.01,
            poll_s=0.01,
        )

    assert FakeLauncher.last_instance is not None
    assert FakeLauncher.last_instance.stopped is True
    payload = json.loads((tmp_path / "session" / "phase1_carla_session.json").read_text(encoding="utf-8"))
    assert payload["status"] == "not_ready"
    assert payload["ready"] is False
    assert payload["stop"]["status"] == "stopped_after_startup_failure"
