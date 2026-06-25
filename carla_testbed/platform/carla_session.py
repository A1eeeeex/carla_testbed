from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CARLA_SESSION_SCHEMA_VERSION = "phase1_carla_session.v1"


class Phase1CarlaStartupError(RuntimeError):
    pass


@dataclass
class Phase1CarlaSession:
    session_dir: Path
    status_path: Path
    launcher: Any
    payload: dict[str, Any]

    def stop(self) -> dict[str, Any]:
        try:
            self.launcher.stop()
            stop_status = "stopped"
            error = None
        except Exception as exc:  # pragma: no cover - defensive runtime path
            stop_status = "stop_failed"
            error = f"{exc.__class__.__name__}: {exc}"
        self.payload["stop"] = {
            "status": stop_status,
            "wall_time_s": time.time(),
            "error": error,
        }
        _write_json(self.status_path, self.payload)
        return dict(self.payload)


def dry_run_carla_session_payload(
    *,
    requested: bool,
    carla_root: str | Path | None = None,
    town: str | None = None,
    extra_args: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": CARLA_SESSION_SCHEMA_VERSION,
        "requested": bool(requested),
        "status": "dry_run_not_started" if requested else "not_requested",
        "carla_root": str(carla_root or ""),
        "town": str(town or ""),
        "extra_args": str(extra_args or ""),
        "claim_boundary": "CARLA startup evidence only; not backend behavior evidence.",
    }


def write_phase1_carla_session_payload(*, out_dir: str | Path, payload: dict[str, Any]) -> Path:
    path = Path(out_dir).expanduser() / "phase1_carla_session.json"
    _write_json(path, payload)
    return path


def start_phase1_carla_session(
    *,
    out_dir: str | Path,
    carla_root: str | Path | None = None,
    town: str = "Town01",
    host: str = "127.0.0.1",
    port: int = 2000,
    extra_args: str = "-RenderOffScreen",
    timeout_s: float = 90.0,
    poll_s: float = 2.0,
    stop_reused_on_exit: bool = False,
) -> Phase1CarlaSession:
    from tbio.carla.launcher import CarlaLauncher

    session_dir = Path(out_dir).expanduser()
    session_dir.mkdir(parents=True, exist_ok=True)
    status_path = session_dir / "phase1_carla_session.json"
    resolved_root = _resolve_carla_root(carla_root)
    payload: dict[str, Any] = {
        "schema_version": CARLA_SESSION_SCHEMA_VERSION,
        "requested": True,
        "status": "starting",
        "carla_root": str(resolved_root),
        "town": str(town),
        "host": host,
        "port": int(port),
        "extra_args": str(extra_args),
        "timeout_s": float(timeout_s),
        "poll_s": float(poll_s),
        "started_wall_time_s": time.time(),
        "claim_boundary": "CARLA startup evidence only; not backend behavior evidence.",
    }
    _write_json(status_path, payload)
    launcher = CarlaLauncher(
        carla_root=resolved_root,
        host=host,
        port=int(port),
        town=str(town),
        extra_args=str(extra_args),
        foreground=False,
        run_dir=session_dir,
        stop_reused_on_exit=bool(stop_reused_on_exit),
    )
    try:
        launcher.start()
        ready = bool(launcher.wait_ready(timeout_s=float(timeout_s), poll_s=float(poll_s)))
        diagnostics = launcher.diagnostics_snapshot(probe_rpc=True)
    except Exception as exc:
        stop_payload = _stop_launcher_for_failed_start(launcher)
        payload.update(
            {
                "status": "startup_failed",
                "ready": False,
                "error": f"{exc.__class__.__name__}: {exc}",
                "stop": stop_payload,
            }
        )
        try:
            payload["diagnostics"] = launcher.diagnostics_snapshot(probe_rpc=False)
        except Exception:
            pass
        _write_json(status_path, payload)
        raise Phase1CarlaStartupError(payload["error"]) from exc
    payload.update(
        {
            "status": "ready" if ready else "not_ready",
            "ready": ready,
            "diagnostics": diagnostics,
        }
    )
    _write_json(status_path, payload)
    if not ready:
        payload["stop"] = _stop_launcher_for_failed_start(launcher)
        _write_json(status_path, payload)
        raise Phase1CarlaStartupError(f"CARLA did not become ready within {timeout_s}s")
    return Phase1CarlaSession(
        session_dir=session_dir,
        status_path=status_path,
        launcher=launcher,
        payload=payload,
    )


def _resolve_carla_root(raw: str | Path | None) -> Path:
    candidates = [
        raw,
        os.environ.get("CARLA_ROOT"),
        "/home/ubuntu/CARLA_0.9.16",
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        path = Path(os.path.expanduser(os.path.expandvars(text))).resolve()
        if path.exists():
            return path
    raise Phase1CarlaStartupError("CARLA root not found; set --carla-root or CARLA_ROOT")


def _stop_launcher_for_failed_start(launcher: Any) -> dict[str, Any]:
    try:
        launcher.stop()
        return {
            "status": "stopped_after_startup_failure",
            "wall_time_s": time.time(),
            "error": None,
        }
    except Exception as exc:  # pragma: no cover - defensive runtime path
        return {
            "status": "stop_failed_after_startup_failure",
            "wall_time_s": time.time(),
            "error": f"{exc.__class__.__name__}: {exc}",
        }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
