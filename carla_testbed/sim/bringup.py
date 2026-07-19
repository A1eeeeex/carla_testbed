from __future__ import annotations

import hashlib
from pathlib import Path
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional


AttemptCallback = Callable[[str, Dict[str, Any]], None]

_OPENDRIVE_GENERATION_PARAMETER_KEYS = {
    "vertex_distance",
    "max_road_length",
    "wall_height",
    "additional_width",
    "smooth_junctions",
    "enable_mesh_visibility",
    "enable_pedestrian_navigation",
}


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
    from .carla_client import CarlaClientManager

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


def generate_opendrive_world_with_retry(
    client: Any,
    opendrive_path: str | Path,
    *,
    generation_parameters: Mapping[str, Any] | None = None,
    expected_map_name: str = "OpenDriveMap",
    attempts: int = 3,
    delay_s: float = 3.0,
    timeout_s: float = 60.0,
    restore_timeout_s: Optional[float] = None,
    attempt_callback: Optional[AttemptCallback] = None,
) -> Any:
    """Materialize a CARLA OpenDRIVE world with retry and source evidence."""

    source = Path(opendrive_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"OpenDRIVE source is missing: {source}")
    xodr = source.read_text(encoding="utf-8")
    source_sha256 = hashlib.sha256(xodr.encode("utf-8")).hexdigest()
    raw_parameters = dict(generation_parameters or {})
    unknown = sorted(set(raw_parameters) - _OPENDRIVE_GENERATION_PARAMETER_KEYS)
    if unknown:
        raise ValueError(f"unsupported OpenDRIVE generation parameters: {unknown}")

    import carla

    parameters = carla.OpendriveGenerationParameters(**raw_parameters)
    attempts = max(1, int(attempts))
    previous_timeout = float(restore_timeout_s) if restore_timeout_s is not None else None
    last_exc: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        started = time.time()
        common = {
            "attempt": attempt,
            "attempts": attempts,
            "timeout_sec": float(timeout_s),
            "opendrive_path": str(source),
            "opendrive_sha256": source_sha256,
            "opendrive_bytes": len(xodr.encode("utf-8")),
            "generation_parameters": raw_parameters,
            "expected_map_name": str(expected_map_name),
        }
        _emit_attempt(attempt_callback, "generate_opendrive_world_attempt_start", common)
        try:
            setter = getattr(client, "set_timeout", None)
            if callable(setter):
                setter(float(timeout_s))
            world = client.generate_opendrive_world(xodr, parameters)
            loaded_map_name = ""
            try:
                loaded_map_name = str(world.get_map().name or "").split("/")[-1]
            except Exception:
                loaded_map_name = ""
            if expected_map_name and loaded_map_name != str(expected_map_name):
                raise RuntimeError(
                    "generated OpenDRIVE map identity mismatch: "
                    f"loaded={loaded_map_name!r} expected={expected_map_name!r}"
                )
            _emit_attempt(
                attempt_callback,
                "generate_opendrive_world_attempt_ok",
                {
                    **common,
                    "elapsed_sec": time.time() - started,
                    "loaded_map_name": loaded_map_name,
                },
            )
            if callable(setter) and previous_timeout is not None:
                setter(previous_timeout)
            return world
        except Exception as exc:
            last_exc = exc
            _emit_attempt(
                attempt_callback,
                "generate_opendrive_world_attempt_failed",
                {
                    **common,
                    "elapsed_sec": time.time() - started,
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
        "generate_opendrive_world_failed",
        {
            "attempts": attempts,
            "opendrive_path": str(source),
            "opendrive_sha256": source_sha256,
            "error": repr(last_exc) if last_exc is not None else "RuntimeError('generate OpenDRIVE world failed')",
        },
    )
    raise last_exc or RuntimeError("generate OpenDRIVE world failed")


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
    opendrive_path: str | Path | None = None,
    opendrive_generation_parameters: Mapping[str, Any] | None = None,
    opendrive_expected_map_name: str = "OpenDriveMap",
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
    if opendrive_path is not None:
        world = generate_opendrive_world_with_retry(
            client,
            opendrive_path,
            generation_parameters=opendrive_generation_parameters,
            expected_map_name=str(opendrive_expected_map_name),
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
            final_town = str(opendrive_expected_map_name)
        expected_generated_town = str(opendrive_expected_map_name)
        if final_town != expected_generated_town:
            raise RuntimeError(
                "generated OpenDRIVE world does not match configured OpenDRIVE map identity: "
                f"loaded={final_town!r} expected={expected_generated_town!r}"
            )
    elif current_town != str(target_town):
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
            "world_source": "opendrive" if opendrive_path is not None else "packaged_map",
            "opendrive_path": str(Path(opendrive_path).expanduser().resolve()) if opendrive_path is not None else "",
            "opendrive_expected_map_name": (
                str(opendrive_expected_map_name) if opendrive_path is not None else ""
            ),
        },
        startup_trace=startup_trace,
    )
