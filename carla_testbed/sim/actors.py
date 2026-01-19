from __future__ import annotations

import random
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
    Try spawning with a preferred index first; fall back to random choices.
    Returns (actor, used_index_or_-1_on_failure).
    """
    points = list(spawn_points)
    if not points:
        return None, -1

    # clamp preferred index into valid range
    preferred_idx = max(0, min(preferred_idx, len(points) - 1))
    indices = list(range(len(points)))

    tried = 0
    indices.insert(0, indices.pop(preferred_idx))

    for idx in indices:
        if tried >= max_tries:
            break
        tried += 1
        try:
            actor = world.spawn_actor(blueprint, points[idx])
            if actor is not None:
                return actor, idx
        except RuntimeError:
            continue
        except Exception:
            continue
    return None, -1
