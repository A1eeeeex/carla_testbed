from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Sequence


@dataclass(frozen=True)
class FrontAlignment:
    longitudinal_m: float | None
    lateral_m: float | None
    euclidean_m: float | None
    heading_diff_deg: float | None
    reasons: tuple[str, ...]

    @property
    def aligned(self) -> bool:
        return not self.reasons


@dataclass(frozen=True)
class FrontSpawnSelection:
    index: int
    found: bool
    requested_index: int
    requested_alignment: FrontAlignment | None
    selected_alignment: FrontAlignment | None
    candidate_count: int


@dataclass(frozen=True)
class WaypointAheadSelection:
    found: bool
    distance_m: float
    candidate_count: int
    selected_transform: Any | None
    selected_heading_diff_deg: float | None
    reason: str | None = None


def wrap_deg(delta_deg: float) -> float:
    return (float(delta_deg) + 180.0) % 360.0 - 180.0


def _get_attr_float(obj: Any, *names: str) -> float | None:
    cur = obj
    for name in names:
        cur = getattr(cur, name, None)
        if cur is None:
            return None
    try:
        return float(cur)
    except (TypeError, ValueError):
        return None


def relative_front_alignment(
    ego_tf: Any,
    front_tf: Any,
    *,
    min_ahead_m: float,
    max_ahead_m: float,
    max_lateral_m: float,
    max_heading_diff_deg: float,
) -> FrontAlignment:
    ex = _get_attr_float(ego_tf, "location", "x")
    ey = _get_attr_float(ego_tf, "location", "y")
    eyaw = _get_attr_float(ego_tf, "rotation", "yaw")
    fx = _get_attr_float(front_tf, "location", "x")
    fy = _get_attr_float(front_tf, "location", "y")
    fyaw = _get_attr_float(front_tf, "rotation", "yaw")
    if ex is None or ey is None or eyaw is None or fx is None or fy is None:
        return FrontAlignment(None, None, None, None, ("front_relative_pose_unavailable",))

    dx = fx - ex
    dy = fy - ey
    yaw = math.radians(eyaw)
    hx = math.cos(yaw)
    hy = math.sin(yaw)
    longitudinal = (dx * hx) + (dy * hy)
    lateral = (-dx * hy) + (dy * hx)
    heading_diff = abs(wrap_deg((fyaw if fyaw is not None else eyaw) - eyaw))
    reasons: list[str] = []
    if longitudinal < float(min_ahead_m):
        reasons.append("front_not_ahead_of_ego")
    if longitudinal > float(max_ahead_m):
        reasons.append("front_too_far_ahead")
    if abs(lateral) > float(max_lateral_m):
        reasons.append("front_lateral_misaligned")
    if heading_diff > float(max_heading_diff_deg):
        reasons.append("front_heading_misaligned")
    return FrontAlignment(
        longitudinal_m=longitudinal,
        lateral_m=lateral,
        euclidean_m=math.hypot(dx, dy),
        heading_diff_deg=heading_diff,
        reasons=tuple(reasons),
    )


def select_aligned_front_spawn_index(
    spawns: Sequence[Any],
    *,
    ego_idx: int,
    requested_front_idx: int,
    min_ahead_m: float,
    max_ahead_m: float,
    max_lateral_m: float,
    max_heading_diff_deg: float,
    target_ahead_m: float | None = None,
) -> FrontSpawnSelection:
    if not spawns:
        return FrontSpawnSelection(
            index=int(requested_front_idx),
            found=False,
            requested_index=int(requested_front_idx),
            requested_alignment=None,
            selected_alignment=None,
            candidate_count=0,
        )

    ego_idx = max(0, min(int(ego_idx), len(spawns) - 1))
    requested_front_idx = max(0, min(int(requested_front_idx), len(spawns) - 1))
    ego_tf = spawns[ego_idx]
    requested_alignment = relative_front_alignment(
        ego_tf,
        spawns[requested_front_idx],
        min_ahead_m=min_ahead_m,
        max_ahead_m=max_ahead_m,
        max_lateral_m=max_lateral_m,
        max_heading_diff_deg=max_heading_diff_deg,
    )

    best_idx = requested_front_idx
    best_alignment: FrontAlignment | None = None
    best_score = float("inf")
    candidate_count = 0
    for idx, tf in enumerate(spawns):
        if idx == ego_idx:
            continue
        alignment = relative_front_alignment(
            ego_tf,
            tf,
            min_ahead_m=min_ahead_m,
            max_ahead_m=max_ahead_m,
            max_lateral_m=max_lateral_m,
            max_heading_diff_deg=max_heading_diff_deg,
        )
        if not alignment.aligned:
            continue
        candidate_count += 1
        longitudinal = alignment.longitudinal_m or 0.0
        lateral = alignment.lateral_m or 0.0
        heading = alignment.heading_diff_deg or 0.0
        if target_ahead_m is None:
            # Prefer the nearest valid front spawn for legacy short follow-stop
            # smoke tests. Dedicated high-speed probes should set target_ahead_m.
            distance_score = longitudinal
        else:
            distance_score = abs(longitudinal - float(target_ahead_m))
        score = distance_score + (abs(lateral) * 5.0) + heading
        if score < best_score:
            best_score = score
            best_idx = idx
            best_alignment = alignment

    return FrontSpawnSelection(
        index=best_idx,
        found=best_alignment is not None,
        requested_index=requested_front_idx,
        requested_alignment=requested_alignment,
        selected_alignment=best_alignment,
        candidate_count=candidate_count,
    )


def select_waypoint_ahead_transform(start_wp: Any, distance_m: float) -> WaypointAheadSelection:
    """Select the waypoint ahead with the smallest heading discontinuity.

    CARLA maps may return multiple waypoints at junctions. Follow-stop probes
    should prefer lane continuity instead of arbitrary branch order.
    """

    if start_wp is None:
        return WaypointAheadSelection(
            found=False,
            distance_m=float(distance_m),
            candidate_count=0,
            selected_transform=None,
            selected_heading_diff_deg=None,
            reason="start_waypoint_missing",
        )
    try:
        candidates = list(start_wp.next(float(distance_m)))
    except Exception as exc:
        return WaypointAheadSelection(
            found=False,
            distance_m=float(distance_m),
            candidate_count=0,
            selected_transform=None,
            selected_heading_diff_deg=None,
            reason=f"waypoint_next_failed:{type(exc).__name__}",
        )
    if not candidates:
        return WaypointAheadSelection(
            found=False,
            distance_m=float(distance_m),
            candidate_count=0,
            selected_transform=None,
            selected_heading_diff_deg=None,
            reason="no_waypoint_ahead",
        )
    start_yaw = _get_attr_float(start_wp, "transform", "rotation", "yaw") or 0.0
    best = min(
        candidates,
        key=lambda wp: abs(
            wrap_deg((_get_attr_float(wp, "transform", "rotation", "yaw") or start_yaw) - start_yaw)
        ),
    )
    best_yaw = _get_attr_float(best, "transform", "rotation", "yaw")
    return WaypointAheadSelection(
        found=True,
        distance_m=float(distance_m),
        candidate_count=len(candidates),
        selected_transform=getattr(best, "transform", None),
        selected_heading_diff_deg=abs(wrap_deg((best_yaw if best_yaw is not None else start_yaw) - start_yaw)),
        reason=None,
    )
