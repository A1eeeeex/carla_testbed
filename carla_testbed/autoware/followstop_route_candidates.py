"""Candidate search for long follow-stop routes in CARLA maps.

The scanner is intentionally CARLA-runtime optional: pure geometry helpers can
be tested with fake transforms, while live scanning receives a loaded
``carla.World`` object from a thin CLI wrapper.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import json
import math
from pathlib import Path
from typing import Any, Iterable, Sequence


SCHEMA_VERSION = "autoware_followstop_route_candidates.v1"


@dataclass(frozen=True)
class Pose2D:
    x: float
    y: float
    z: float
    yaw_deg: float


@dataclass(frozen=True)
class RelativePose:
    longitudinal_m: float
    lateral_m: float
    euclidean_m: float
    heading_diff_deg: float


@dataclass(frozen=True)
class FollowStopRouteCandidate:
    ego_idx: int
    front_idx: int
    front_ahead_m: float
    goal_ahead_m: float
    front_relative: RelativePose
    front_target_error_m: float
    goal_pose: Pose2D
    front_pose: Pose2D
    ego_pose: Pose2D
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        return payload


def wrap_deg(delta_deg: float) -> float:
    return (float(delta_deg) + 180.0) % 360.0 - 180.0


def _float_attr(obj: Any, *attrs: str) -> float:
    cur = obj
    for attr in attrs:
        cur = getattr(cur, attr)
    return float(cur)


def pose_from_transform(transform: Any) -> Pose2D:
    return Pose2D(
        x=_float_attr(transform, "location", "x"),
        y=_float_attr(transform, "location", "y"),
        z=_float_attr(transform, "location", "z"),
        yaw_deg=_float_attr(transform, "rotation", "yaw"),
    )


def relative_pose(ego: Pose2D, target: Pose2D) -> RelativePose:
    dx = target.x - ego.x
    dy = target.y - ego.y
    yaw = math.radians(ego.yaw_deg)
    hx = math.cos(yaw)
    hy = math.sin(yaw)
    longitudinal = (dx * hx) + (dy * hy)
    lateral = (-dx * hy) + (dy * hx)
    return RelativePose(
        longitudinal_m=longitudinal,
        lateral_m=lateral,
        euclidean_m=math.hypot(dx, dy),
        heading_diff_deg=abs(wrap_deg(target.yaw_deg - ego.yaw_deg)),
    )


def _interpolate_pose(points: Sequence[tuple[float, Pose2D]], target_s: float) -> Pose2D | None:
    if not points:
        return None
    if target_s <= points[0][0]:
        return points[0][1]
    for (s0, p0), (s1, p1) in zip(points, points[1:]):
        if s1 < target_s:
            continue
        span = max(s1 - s0, 1e-6)
        t = max(0.0, min(1.0, (float(target_s) - s0) / span))
        yaw_delta = wrap_deg(p1.yaw_deg - p0.yaw_deg)
        return Pose2D(
            x=p0.x + ((p1.x - p0.x) * t),
            y=p0.y + ((p1.y - p0.y) * t),
            z=p0.z + ((p1.z - p0.z) * t),
            yaw_deg=p0.yaw_deg + (yaw_delta * t),
        )
    return None


def _pose_distance(a: Pose2D, b: Pose2D) -> float:
    return math.sqrt(((a.x - b.x) ** 2) + ((a.y - b.y) ** 2) + ((a.z - b.z) ** 2))


def find_front_spawn_for_path_target(
    spawns: Sequence[Any],
    *,
    ego_idx: int,
    target_pose: Pose2D,
    front_target_ahead_m: float,
    max_lateral_m: float,
    max_heading_diff_deg: float,
    min_ahead_m: float,
    max_ahead_m: float,
) -> tuple[int | None, RelativePose | None, float | None]:
    ego_pose = pose_from_transform(spawns[ego_idx])
    best: tuple[float, int, RelativePose, float] | None = None
    for idx, transform in enumerate(spawns):
        if idx == ego_idx:
            continue
        front_pose = pose_from_transform(transform)
        rel = relative_pose(ego_pose, front_pose)
        if rel.longitudinal_m < float(min_ahead_m) or rel.longitudinal_m > float(max_ahead_m):
            continue
        if abs(rel.lateral_m) > float(max_lateral_m):
            continue
        if rel.heading_diff_deg > float(max_heading_diff_deg):
            continue
        target_error = _pose_distance(front_pose, target_pose)
        score = (
            target_error
            + abs(rel.longitudinal_m - float(front_target_ahead_m))
            + abs(rel.lateral_m) * 5.0
            + rel.heading_diff_deg
        )
        if best is None or score < best[0]:
            best = (score, idx, rel, target_error)
    if best is None:
        return None, None, None
    return best[1], best[2], best[3]


def _trace_waypoint_path(start_wp: Any, *, step_m: float, max_distance_m: float) -> list[tuple[float, Pose2D]]:
    points: list[tuple[float, Pose2D]] = [(0.0, pose_from_transform(start_wp.transform))]
    current = start_wp
    travelled = 0.0
    while travelled + float(step_m) <= float(max_distance_m) + 1e-6:
        next_points = current.next(float(step_m))
        if not next_points:
            break
        # Prefer continuity over lane branching. If several choices exist,
        # choose the one with the smallest heading jump from the current lane.
        current_yaw = pose_from_transform(current.transform).yaw_deg
        current = min(
            next_points,
            key=lambda wp: abs(wrap_deg(pose_from_transform(wp.transform).yaw_deg - current_yaw)),
        )
        travelled += float(step_m)
        points.append((travelled, pose_from_transform(current.transform)))
    return points


def scan_followstop_route_candidates(
    carla_map: Any,
    spawns: Sequence[Any],
    *,
    front_target_ahead_m: float = 300.0,
    goal_beyond_front_m: float = 80.0,
    step_m: float = 5.0,
    min_ahead_m: float = 20.0,
    max_ahead_m: float = 360.0,
    max_lateral_m: float = 4.0,
    max_heading_diff_deg: float = 35.0,
    max_front_target_error_m: float = 30.0,
    limit: int = 20,
) -> list[FollowStopRouteCandidate]:
    goal_ahead_m = float(front_target_ahead_m) + float(goal_beyond_front_m)
    candidates: list[FollowStopRouteCandidate] = []
    for ego_idx, spawn in enumerate(spawns):
        try:
            start_wp = carla_map.get_waypoint(spawn.location, project_to_road=True)
        except TypeError:
            start_wp = carla_map.get_waypoint(spawn.location)
        except Exception:
            continue
        if start_wp is None:
            continue
        path = _trace_waypoint_path(start_wp, step_m=float(step_m), max_distance_m=goal_ahead_m)
        front_target = _interpolate_pose(path, float(front_target_ahead_m))
        goal_pose = _interpolate_pose(path, goal_ahead_m)
        if front_target is None or goal_pose is None:
            continue
        front_idx, front_rel, front_error = find_front_spawn_for_path_target(
            spawns,
            ego_idx=ego_idx,
            target_pose=front_target,
            front_target_ahead_m=float(front_target_ahead_m),
            max_lateral_m=float(max_lateral_m),
            max_heading_diff_deg=float(max_heading_diff_deg),
            min_ahead_m=float(min_ahead_m),
            max_ahead_m=float(max_ahead_m),
        )
        if front_idx is None or front_rel is None or front_error is None:
            continue
        warnings: list[str] = []
        if front_error > float(max_front_target_error_m):
            warnings.append("front_spawn_far_from_300m_path_target")
        candidates.append(
            FollowStopRouteCandidate(
                ego_idx=int(ego_idx),
                front_idx=int(front_idx),
                front_ahead_m=float(front_target_ahead_m),
                goal_ahead_m=float(goal_ahead_m),
                front_relative=front_rel,
                front_target_error_m=float(front_error),
                goal_pose=goal_pose,
                front_pose=pose_from_transform(spawns[front_idx]),
                ego_pose=pose_from_transform(spawn),
                warnings=tuple(warnings),
            )
        )
    candidates.sort(
        key=lambda c: (
            bool(c.warnings),
            c.front_target_error_m,
            abs(c.front_relative.longitudinal_m - float(front_target_ahead_m)),
            abs(c.front_relative.lateral_m),
        )
    )
    return candidates[: max(0, int(limit))]


def build_candidate_report(
    *,
    map_name: str,
    candidates: Iterable[FollowStopRouteCandidate],
    front_target_ahead_m: float,
    goal_beyond_front_m: float,
    source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    items = [candidate.to_dict() for candidate in candidates]
    status = "pass" if items else "insufficient_data"
    warnings: list[str] = []
    if not items:
        warnings.append("no_candidate_with_goal_beyond_front")
    return {
        "schema_version": SCHEMA_VERSION,
        "map_name": map_name,
        "front_target_ahead_m": float(front_target_ahead_m),
        "goal_beyond_front_m": float(goal_beyond_front_m),
        "candidate_count": len(items),
        "candidates": items,
        "status": status,
        "warnings": warnings,
        "source": source or {},
    }


def write_candidate_report(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "followstop_route_candidates.json"
    md_path = out / "followstop_route_candidates.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    lines = [
        "# Follow-stop Route Candidates",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- map_name: `{report.get('map_name')}`",
        f"- status: `{report.get('status')}`",
        f"- candidate_count: `{report.get('candidate_count')}`",
        f"- front_target_ahead_m: `{report.get('front_target_ahead_m')}`",
        f"- goal_beyond_front_m: `{report.get('goal_beyond_front_m')}`",
        "",
        "| rank | ego_idx | front_idx | front_longitudinal_m | lateral_m | goal_ahead_m | front_target_error_m | warnings |",
        "|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for rank, item in enumerate(report.get("candidates") or [], start=1):
        rel = item.get("front_relative") or {}
        warnings = ", ".join(item.get("warnings") or [])
        lines.append(
            "| {rank} | {ego} | {front} | {lon:.3f} | {lat:.3f} | {goal:.3f} | {err:.3f} | {warnings} |".format(
                rank=rank,
                ego=int(item.get("ego_idx")),
                front=int(item.get("front_idx")),
                lon=float(rel.get("longitudinal_m")),
                lat=float(rel.get("lateral_m")),
                goal=float(item.get("goal_ahead_m")),
                err=float(item.get("front_target_error_m")),
                warnings=warnings or "",
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "summary": str(md_path)}
