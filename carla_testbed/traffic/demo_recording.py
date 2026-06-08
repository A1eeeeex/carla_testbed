from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping


def xy_from_transform(transform: Any) -> tuple[float, float]:
    location = getattr(transform, "location", transform)
    return (float(getattr(location, "x", 0.0) or 0.0), float(getattr(location, "y", 0.0) or 0.0))


def compute_displacements(
    start_positions: Mapping[int | str, tuple[float, float]],
    end_positions: Mapping[int | str, tuple[float, float]],
) -> dict[str, float]:
    displacements: dict[str, float] = {}
    for actor_id, start_xy in start_positions.items():
        if actor_id not in end_positions:
            continue
        end_xy = end_positions[actor_id]
        displacements[str(actor_id)] = math.hypot(end_xy[0] - start_xy[0], end_xy[1] - start_xy[1])
    return displacements


def movement_status(
    displacements_m: Mapping[str, float],
    *,
    min_moving_actors: int,
    min_displacement_m: float,
) -> dict[str, Any]:
    moving = {actor_id: distance for actor_id, distance in displacements_m.items() if distance >= min_displacement_m}
    status = "pass" if len(moving) >= min_moving_actors else "fail"
    blocking = [] if status == "pass" else ["traffic_actors_not_moving_enough_for_demo_recording"]
    return {
        "status": status,
        "moving_actor_count": len(moving),
        "min_moving_actors": int(min_moving_actors),
        "min_displacement_m": float(min_displacement_m),
        "max_displacement_m": max(displacements_m.values()) if displacements_m else 0.0,
        "moving_actor_ids": sorted(moving),
        "blocking_reasons": blocking,
    }


def write_json(path: str | Path, payload: Mapping[str, Any]) -> None:
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: str | Path, rows: list[Mapping[str, Any]]) -> None:
    Path(path).write_text(
        "\n".join(json.dumps(dict(row), sort_keys=True) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )
