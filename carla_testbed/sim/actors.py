from __future__ import annotations

from typing import Iterable, Optional, Tuple

import carla


def spawn_with_retry(
    world: carla.World,
    blueprint: carla.ActorBlueprint,
    spawn_points: Iterable[carla.Transform],
    preferred_idx: int = 0,
    max_tries: int = 5,
) -> Tuple[Optional[carla.Actor], int]:
    """
    Try spawning with a preferred index first; then probe nearby spawn points.
    Returns (actor, used_index_or_-1_on_failure).
    """
    points = list(spawn_points)
    if not points:
        return None, -1

    # clamp preferred index into valid range
    preferred_idx = max(0, min(preferred_idx, len(points) - 1))
    total = len(points)
    # Try around preferred index first (preferred,+1,-1,+2,-2,...) to keep
    # fallback behavior spatially close to scenario intent.
    indices: list[int] = [preferred_idx]
    for step in range(1, total):
        right = preferred_idx + step
        left = preferred_idx - step
        if right < total:
            indices.append(right)
        if left >= 0:
            indices.append(left)
        if len(indices) >= total:
            break

    for tried, idx in enumerate(indices):
        if max_tries > 0 and tried >= max_tries:
            break
        try:
            actor = world.spawn_actor(blueprint, points[idx])
            if actor is not None:
                return actor, idx
        except RuntimeError:
            continue
        except Exception:
            continue
    return None, -1
