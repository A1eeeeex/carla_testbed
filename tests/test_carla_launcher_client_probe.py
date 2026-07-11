from __future__ import annotations

import sys
from pathlib import Path

from tbio.carla import launcher


class _AliveProcess:
    returncode = None

    def poll(self):
        return None


def test_client_ok_falls_back_to_external_carla_python(monkeypatch):
    calls: list[tuple[str, str, int, float, bool]] = []

    def fail_in_process():
        raise ModuleNotFoundError("no in-process carla")

    def fake_external_probe(
        python_exec: str,
        *,
        host: str,
        port: int,
        timeout: float,
        require_world: bool,
    ) -> bool:
        calls.append((python_exec, host, port, timeout, require_world))
        return python_exec == "/opt/carla16/bin/python"

    monkeypatch.setattr(launcher, "_load_carla_module", fail_in_process)
    monkeypatch.setattr(
        launcher,
        "_external_carla_python_candidates",
        lambda: ["/bad/python", "/opt/carla16/bin/python"],
    )
    monkeypatch.setattr(launcher, "_external_client_ok_with_python", fake_external_probe)

    assert launcher._client_ok("127.0.0.1", 2000, timeout=0.25, require_world=True) is True
    assert calls == [
        ("/bad/python", "127.0.0.1", 2000, 0.25, True),
        ("/opt/carla16/bin/python", "127.0.0.1", 2000, 0.25, True),
    ]


def test_client_probe_details_records_in_process_import_failure(monkeypatch):
    def fail_in_process():
        raise ModuleNotFoundError("cp313 has no carla wheel")

    monkeypatch.setattr(launcher, "_load_carla_module", fail_in_process)
    monkeypatch.setattr(launcher, "_external_carla_python_candidates", lambda: ["/opt/carla16/bin/python"])
    monkeypatch.setattr(
        launcher,
        "_external_client_ok_with_python",
        lambda python_exec, **kwargs: python_exec == "/opt/carla16/bin/python",
    )

    details = launcher._client_probe_details("127.0.0.1", 2000, timeout=0.25)

    assert details["current_python"] == sys.executable
    assert details["current_python_tag"].startswith("cp")
    assert details["in_process_carla_available"] is False
    assert "cp313 has no carla wheel" in details["in_process_carla_import_error"]
    assert details["external_probe_candidates"] == ["/opt/carla16/bin/python"]
    assert details["external_probe_ready_python"] == "/opt/carla16/bin/python"


def test_wait_ready_does_not_treat_recovery_window_as_startup_timeout(
    tmp_path: Path,
    monkeypatch,
):
    instance = launcher.CarlaLauncher(
        carla_root=tmp_path,
        host="127.0.0.1",
        port=2000,
        town="straight_road_for_baguang",
        extra_args="-RenderOffScreen",
        foreground=False,
        run_dir=tmp_path / "run",
    )
    instance.proc = _AliveProcess()
    instance._launch_records = [{"command": ["-RenderOffScreen"]}]

    monkeypatch.setattr(instance, "_hung_startup_retry_after_sec", lambda timeout_s: 0.0)
    monkeypatch.setattr(launcher, "_is_port_open", lambda host, port: False)
    monkeypatch.setattr(launcher, "_client_ok", lambda *args, **kwargs: True)

    assert instance.wait_ready(timeout_s=0.1, poll_s=0.0) is True
    record = instance._launch_records[-1]
    assert record["listener_pending_after_recovery_window"] is True
    assert "early_fail_reason" not in record
