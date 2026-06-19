from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from carla_testbed.contracts import ControlCommand, EgoState, FrameStamp, ObstacleTruth, SceneTruth

from .base import Controller


@dataclass(frozen=True)
class SimpleAccRouteFollowerConfig:
    """Small diagnostic ego controller for scene playback checks."""

    target_speed_mps: float = 19.44
    max_throttle: float = 0.7
    max_brake: float = 1.0
    speed_kp: float = 0.16
    brake_kp: float = 0.24
    speed_deadband_mps: float = 0.2
    target_gap_m: float = 20.0
    min_gap_m: float = 8.0
    time_headway_s: float = 0.9
    gap_close_gain: float = 0.45
    emergency_gap_m: float = 6.0
    lead_lateral_gate_m: float = 4.0
    explicit_target_roles: tuple[str, ...] = ("lead_vehicle", "cutin_vehicle", "cutout_vehicle")
    max_explicit_target_distance_m: float = 400.0
    heading_gain: float = 0.7
    lateral_gain: float = 0.08
    max_steer: float = 0.45


@dataclass(frozen=True)
class LeadObservation:
    obstacle_id: str
    gap_m: float
    lateral_m: float
    speed_mps: float
    selection_mode: str


class SimpleAccRouteFollowerController(Controller):
    """CARLA-free route/ACC controller used only for diagnostic scene playback."""

    name = "simple_acc_route_follower"

    def __init__(self, config: SimpleAccRouteFollowerConfig | None = None) -> None:
        self.config = config or SimpleAccRouteFollowerConfig()

    def reset(self) -> None:
        pass

    def step(self, frame: FrameStamp, ego: EgoState, scene: SceneTruth) -> ControlCommand:
        ego_speed = _ego_speed_mps(ego)
        lead = _nearest_lead(
            ego,
            scene,
            lateral_gate_m=self.config.lead_lateral_gate_m,
            explicit_target_roles=self.config.explicit_target_roles,
            max_explicit_target_distance_m=self.config.max_explicit_target_distance_m,
        )
        desired_gap = None
        effective_target = float(self.config.target_speed_mps)
        if lead is not None:
            desired_gap = max(self.config.min_gap_m, self.config.target_gap_m + self.config.time_headway_s * ego_speed)
            if lead.gap_m < desired_gap:
                effective_target = min(
                    effective_target,
                    max(0.0, lead.speed_mps - self.config.gap_close_gain * (desired_gap - lead.gap_m)),
                )
        speed_error = effective_target - ego_speed
        throttle = 0.0
        brake = 0.0
        if speed_error > self.config.speed_deadband_mps:
            throttle = min(self.config.max_throttle, self.config.speed_kp * speed_error)
        elif speed_error < -self.config.speed_deadband_mps:
            brake = min(self.config.max_brake, self.config.brake_kp * abs(speed_error))
        if lead is not None and lead.gap_m <= self.config.emergency_gap_m:
            throttle = 0.0
            brake = max(brake, 0.8)
        steer = _steer_from_route_errors(ego.metadata, scene.metadata, self.config)
        command = ControlCommand(
            throttle=throttle,
            brake=brake,
            steer=steer,
            source="carla_testbed_builtin_controller",
            stamp=frame,
            metadata={
                "controller": self.name,
                "runtime_boundary": "diagnostic_builtin_ego_controller_not_autonomy_claim",
                "target_speed_mps": self.config.target_speed_mps,
                "effective_target_speed_mps": effective_target,
                "ego_speed_mps": ego_speed,
                "lead_obstacle_id": None if lead is None else lead.obstacle_id,
                "lead_gap_m": None if lead is None else lead.gap_m,
                "lead_lateral_m": None if lead is None else lead.lateral_m,
                "lead_speed_mps": None if lead is None else lead.speed_mps,
                "lead_selection_mode": None if lead is None else lead.selection_mode,
                "desired_gap_m": desired_gap,
                "steer_heading_error_rad": _steer_heading_error(ego.metadata, scene.metadata),
                "steer_cross_track_error_m": _metadata_float(ego.metadata, scene.metadata, "cross_track_error_m"),
            },
        ).clamped()
        command.validate()
        return command


def _ego_speed_mps(ego: EgoState) -> float:
    if ego.chassis.speed_mps:
        return float(ego.chassis.speed_mps)
    v = ego.linear_velocity
    return math.sqrt(v.x * v.x + v.y * v.y + v.z * v.z)


def _nearest_lead(
    ego: EgoState,
    scene: SceneTruth,
    *,
    lateral_gate_m: float,
    explicit_target_roles: tuple[str, ...] = (),
    max_explicit_target_distance_m: float = 150.0,
) -> LeadObservation | None:
    yaw = _yaw_from_quaternion(
        ego.pose.orientation.x,
        ego.pose.orientation.y,
        ego.pose.orientation.z,
        ego.pose.orientation.w,
    )
    ego_pos = ego.pose.position
    best: LeadObservation | None = None
    explicit_best: LeadObservation | None = None
    target_roles = {str(role).lower() for role in explicit_target_roles}
    for obstacle in scene.obstacles:
        if _is_ego_obstacle(obstacle):
            continue
        metadata = dict(obstacle.metadata or {})
        obs_pos = obstacle.pose.position
        dx = float(obs_pos.x) - float(ego_pos.x)
        dy = float(obs_pos.y) - float(ego_pos.y)
        gap = dx * math.cos(yaw) + dy * math.sin(yaw)
        lateral = -dx * math.sin(yaw) + dy * math.cos(yaw)
        speed = _obstacle_speed_mps(obstacle)
        if gap > 0.0 and abs(lateral) <= lateral_gate_m:
            candidate = LeadObservation(
                obstacle_id=str(obstacle.obstacle_id),
                gap_m=gap,
                lateral_m=lateral,
                speed_mps=speed,
                selection_mode="ego_frame_lateral_gate",
            )
            if best is None or candidate.gap_m < best.gap_m:
                best = candidate
            continue
        role = str(metadata.get("role") or "").lower()
        if role in target_roles:
            route_gap = _optional_metadata_float(metadata, "route_progress_gap_m")
            if route_gap is not None and 0.0 < route_gap <= max_explicit_target_distance_m:
                candidate = LeadObservation(
                    obstacle_id=str(obstacle.obstacle_id),
                    gap_m=route_gap,
                    lateral_m=lateral,
                    speed_mps=speed,
                    selection_mode="explicit_target_role_route_progress_gap",
                )
                if explicit_best is None or candidate.gap_m < explicit_best.gap_m:
                    explicit_best = candidate
                continue
            distance = math.hypot(dx, dy)
            if gap > 0.0 and 0.0 < distance <= max_explicit_target_distance_m:
                candidate = LeadObservation(
                    obstacle_id=str(obstacle.obstacle_id),
                    gap_m=distance,
                    lateral_m=lateral,
                    speed_mps=speed,
                    selection_mode="explicit_target_role_distance_fallback",
                )
                if explicit_best is None or candidate.gap_m < explicit_best.gap_m:
                    explicit_best = candidate
    return best or explicit_best


def _optional_metadata_float(metadata: dict[str, Any], key: str) -> float | None:
    value = metadata.get(key)
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _obstacle_speed_mps(obstacle: ObstacleTruth) -> float:
    return math.sqrt(
        obstacle.linear_velocity.x * obstacle.linear_velocity.x
        + obstacle.linear_velocity.y * obstacle.linear_velocity.y
        + obstacle.linear_velocity.z * obstacle.linear_velocity.z
    )


def _is_ego_obstacle(obstacle: ObstacleTruth) -> bool:
    metadata = dict(obstacle.metadata or {})
    return bool(metadata.get("is_ego")) or str(metadata.get("role") or "").lower() == "ego"


def _steer_from_route_errors(ego_metadata: dict[str, Any], scene_metadata: dict[str, Any], cfg: SimpleAccRouteFollowerConfig) -> float:
    heading_error = _steer_heading_error(ego_metadata, scene_metadata)
    lateral_error = _metadata_float(ego_metadata, scene_metadata, "cross_track_error_m")
    steer = -cfg.heading_gain * heading_error - cfg.lateral_gain * lateral_error
    return max(-cfg.max_steer, min(cfg.max_steer, steer))


def _steer_heading_error(ego_metadata: dict[str, Any], scene_metadata: dict[str, Any]) -> float:
    lookahead = _metadata_optional_float(ego_metadata, scene_metadata, "route_lookahead_heading_error_rad")
    if lookahead is not None:
        return lookahead
    return _metadata_float(ego_metadata, scene_metadata, "heading_error_rad")


def _metadata_optional_float(ego_metadata: dict[str, Any], scene_metadata: dict[str, Any], key: str) -> float | None:
    for source in (ego_metadata, scene_metadata):
        value = source.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _metadata_float(ego_metadata: dict[str, Any], scene_metadata: dict[str, Any], key: str) -> float:
    for source in (ego_metadata, scene_metadata):
        value = source.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _yaw_from_quaternion(x: float, y: float, z: float, w: float) -> float:
    siny_cosp = 2.0 * (w * z + x * y)
    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)
