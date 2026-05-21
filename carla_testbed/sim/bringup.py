from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from .carla_client import CarlaClientManager


AttemptCallback = Callable[[str, Dict[str, Any]], None]


@dataclass
class CarlaWorldBringupResult:
    client: Any
    world: Any
    current_town_before_load: str
    final_town: str
    session_state: Dict[str, Any] = field(default_factory=dict)
    startup_trace: list[Dict[str, Any]] = field(default_factory=list)


def _emit_attempt(callback: Optional[AttemptCallback], phase: str, payload: Dict[str, Any]) -> None:
    if callback is None:
        return
    callback(phase, dict(payload))


def create_client_with_retry(
    *,
    host: str,
    port: int,
    timeout_s: float,
    attempts: int = 3,
    delay_s: float = 2.0,
    root: Any = None,
    attempt_callback: Optional[AttemptCallback] = None,
) -> Any:
    last_exc: Optional[Exception] = None
    attempts = max(1, int(attempts))
    for attempt in range(1, attempts + 1):
        started = time.time()
        _emit_attempt(
            attempt_callback,
            "client_connect_attempt_start",
            {
                "attempt": attempt,
                "attempts": attempts,
                "timeout_sec": float(timeout_s),
            },
        )
        try:
            manager = CarlaClientManager(host=host, port=port, timeout=float(timeout_s), root=root)
            client = manager.create_client()
            _emit_attempt(
                attempt_callback,
                "client_connect_attempt_ok",
                {
                    "attempt": attempt,
                    "attempts": attempts,
                    "elapsed_sec": time.time() - started,
                },
            )
            return client
        except Exception as exc:
            last_exc = exc
            _emit_attempt(
                attempt_callback,
                "client_connect_attempt_failed",
                {
                    "attempt": attempt,
                    "attempts": attempts,
                    "elapsed_sec": time.time() - started,
                    "error": repr(exc),
                },
            )
            if attempt < attempts:
                time.sleep(float(delay_s))
    raise last_exc or RuntimeError("Failed to connect to CARLA client")


def get_world_with_retry(
    client: Any,
    *,
    attempts: int = 3,
    delay_s: float = 3.0,
    timeout_s: Optional[float] = None,
    attempt_callback: Optional[AttemptCallback] = None,
) -> Any:
    last_exc: Optional[Exception] = None
    attempts = max(1, int(attempts))
    for attempt in range(1, attempts + 1):
        started = time.time()
        _emit_attempt(
            attempt_callback,
            "get_world_attempt_start",
            {
                "attempt": attempt,
                "attempts": attempts,
                "delay_sec": float(delay_s),
                "timeout_sec": float(timeout_s) if timeout_s is not None else None,
            },
        )
        try:
            setter = getattr(client, "set_timeout", None)
            if callable(setter) and timeout_s is not None:
                setter(float(timeout_s))
            world = client.get_world()
            current_town = ""
            try:
                current_town = str(world.get_map().name or "").split("/")[-1]
            except Exception:
                current_town = ""
            _emit_attempt(
                attempt_callback,
                "get_world_attempt_ok",
                {
                    "attempt": attempt,
                    "attempts": attempts,
                    "elapsed_sec": time.time() - started,
                    "current_town": current_town,
                },
            )
            return world
        except Exception as exc:
            last_exc = exc
            _emit_attempt(
                attempt_callback,
                "get_world_attempt_failed",
                {
                    "attempt": attempt,
                    "attempts": attempts,
                    "elapsed_sec": time.time() - started,
                    "error": repr(exc),
                },
            )
            if attempt < attempts:
                time.sleep(float(delay_s))
    _emit_attempt(
        attempt_callback,
        "get_world_failed",
        {
            "attempts": attempts,
            "error": repr(last_exc) if last_exc is not None else "RuntimeError('get_world failed')",
        },
    )
    raise last_exc or RuntimeError("get_world failed")


def load_world_with_retry(
    client: Any,
    town: str,
    *,
    attempts: int = 3,
    delay_s: float = 3.0,
    timeout_s: float = 60.0,
    restore_timeout_s: Optional[float] = None,
    attempt_callback: Optional[AttemptCallback] = None,
) -> Any:
    last_exc: Optional[Exception] = None
    attempts = max(1, int(attempts))
    previous_timeout = float(restore_timeout_s) if restore_timeout_s is not None else None
    for attempt in range(1, attempts + 1):
        started = time.time()
        _emit_attempt(
            attempt_callback,
            "load_world_attempt_start",
            {
                "attempt": attempt,
                "attempts": attempts,
                "timeout_sec": float(timeout_s),
                "target_town": str(town),
            },
        )
        try:
            setter = getattr(client, "set_timeout", None)
            if callable(setter):
                setter(float(timeout_s))
            world = client.load_world(str(town))
            loaded_town = ""
            try:
                loaded_town = str(world.get_map().name or "").split("/")[-1]
            except Exception:
                loaded_town = ""
            _emit_attempt(
                attempt_callback,
                "load_world_attempt_ok",
                {
                    "attempt": attempt,
                    "attempts": attempts,
                    "elapsed_sec": time.time() - started,
                    "target_town": str(town),
                    "loaded_town": loaded_town,
                },
            )
            if callable(setter) and previous_timeout is not None:
                setter(previous_timeout)
            return world
        except Exception as exc:
            last_exc = exc
            _emit_attempt(
                attempt_callback,
                "load_world_attempt_failed",
                {
                    "attempt": attempt,
                    "attempts": attempts,
                    "elapsed_sec": time.time() - started,
                    "target_town": str(town),
                    "error": repr(exc),
                },
            )
            if attempt < attempts:
                time.sleep(float(delay_s))
    setter = getattr(client, "set_timeout", None)
    if callable(setter) and previous_timeout is not None:
        setter(previous_timeout)
    _emit_attempt(
        attempt_callback,
        "load_world_failed",
        {
            "attempts": attempts,
            "target_town": str(town),
            "error": repr(last_exc) if last_exc is not None else "RuntimeError('load_world failed')",
        },
    )
    raise last_exc or RuntimeError("load_world failed")


def connect_world_with_retry(
    *,
    host: str,
    port: int,
    target_town: str,
    client_timeout_s: float = 30.0,
    client_attempts: int = 3,
    client_delay_s: float = 2.0,
    get_world_attempts: int = 3,
    get_world_delay_s: float = 3.0,
    load_world_attempts: int = 3,
    load_world_delay_s: float = 3.0,
    load_world_timeout_s: float = 60.0,
    root: Any = None,
    attempt_callback: Optional[AttemptCallback] = None,
) -> CarlaWorldBringupResult:
    startup_trace: list[Dict[str, Any]] = []

    def _trace_attempt(phase: str, payload: Dict[str, Any]) -> None:
        row = {"phase": str(phase)}
        row.update(dict(payload))
        startup_trace.append(row)
        _emit_attempt(attempt_callback, phase, payload)

    client = create_client_with_retry(
        host=host,
        port=int(port),
        timeout_s=float(client_timeout_s),
        attempts=int(client_attempts),
        delay_s=float(client_delay_s),
        root=root,
        attempt_callback=_trace_attempt,
    )
    world = get_world_with_retry(
        client,
        attempts=int(get_world_attempts),
        delay_s=float(get_world_delay_s),
        attempt_callback=_trace_attempt,
    )
    current_town = ""
    try:
        current_town = str(world.get_map().name or "").split("/")[-1]
    except Exception:
        current_town = ""
    final_town = current_town
    if current_town != str(target_town):
        world = load_world_with_retry(
            client,
            str(target_town),
            attempts=int(load_world_attempts),
            delay_s=float(load_world_delay_s),
            timeout_s=float(load_world_timeout_s),
            restore_timeout_s=float(client_timeout_s),
            attempt_callback=_trace_attempt,
        )
        time.sleep(1.0)
        try:
            final_town = str(world.get_map().name or "").split("/")[-1]
        except Exception:
            final_town = str(target_town)
    return CarlaWorldBringupResult(
        client=client,
        world=world,
        current_town_before_load=current_town,
        final_town=final_town,
        session_state={
            "target_town": str(target_town),
            "client_timeout_s": float(client_timeout_s),
            "current_town_before_load": current_town,
            "final_town": final_town,
        },
        startup_trace=startup_trace,
    )
