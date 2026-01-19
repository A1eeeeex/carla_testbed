from __future__ import annotations

from typing import Optional, Tuple

import carla


def configure_synchronous_mode(world: carla.World, fixed_delta_seconds: float) -> carla.WorldSettings:
    """Enable synchronous mode and fixed delta; returns original settings for restoration."""
    original = world.get_settings()
    # Build a fresh settings object to avoid constructor type mismatch
    new_settings = carla.WorldSettings()
    new_settings.synchronous_mode = True
    new_settings.fixed_delta_seconds = fixed_delta_seconds
    # Preserve other flags from the original settings where possible
    for attr in [
        "no_rendering_mode",
        "substepping",
        "max_substep_delta_time",
        "max_substeps",
        "deterministic_ragdolls",
        "max_culling_distance",
        "actor_active_distance",
        "spectator_as_ego",
        "tile_stream_distance",
    ]:
        if hasattr(original, attr):
            setattr(new_settings, attr, getattr(original, attr))
    world.apply_settings(new_settings)
    return original


def restore_settings(world: carla.World, original: Optional[carla.WorldSettings]):
    if original is None:
        return
    world.apply_settings(original)


def tick_world(world: carla.World) -> Tuple[int, float, carla.WorldSnapshot]:
    snapshot = world.tick()
    # CARLA 0.9.16: world.tick() may return int frame_id; retrieve snapshot separately
    if isinstance(snapshot, int):
        frame_id = snapshot
        snapshot = world.get_snapshot()
        ts = snapshot.timestamp if snapshot is not None else None
    else:
        frame_id = snapshot.frame
        ts = snapshot.timestamp
    elapsed = ts.elapsed_seconds if ts is not None else 0.0
    return frame_id, elapsed, snapshot
