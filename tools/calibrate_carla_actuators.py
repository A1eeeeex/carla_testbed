#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import carla
except Exception as exc:  # pragma: no cover
    raise RuntimeError("failed to import carla python api, please source the CARLA Python environment first") from exc

try:
    from tools.apollo10_cyber_bridge.actuator_mapping import build_inverse_table, clamp
except Exception:
    from apollo10_cyber_bridge.actuator_mapping import build_inverse_table, clamp


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
    except Exception:
        return float(default)
    if math.isnan(out) or math.isinf(out):
        return float(default)
    return out


def _parse_float_list(raw: str) -> List[float]:
    return [float(item.strip()) for item in str(raw).split(",") if item.strip()]


def _parse_axes(raw: str) -> Tuple[str, ...]:
    supported = ("steering", "throttle", "brake")
    requested = {item.strip().lower() for item in str(raw).split(",") if item.strip()}
    unknown = sorted(requested.difference(supported))
    if unknown:
        raise ValueError(f"unsupported calibration axes: {','.join(unknown)}")
    if not requested:
        raise ValueError("at least one calibration axis is required")
    return tuple(axis for axis in supported if axis in requested)


def _parse_speed_edges(raw: str) -> List[Tuple[float, float]]:
    values = sorted(set(max(0.0, float(item.strip())) for item in str(raw).split(",") if item.strip()))
    if len(values) < 2:
        raise ValueError("speed bin edges need at least 2 values")
    return list(zip(values[:-1], values[1:]))


def _median(values: Iterable[float], default: float = 0.0) -> float:
    buf = [float(item) for item in values]
    if not buf:
        return float(default)
    return float(statistics.median(buf))


def _percentile(values: Iterable[float], q: float, default: float = 0.0) -> float:
    buf = sorted(float(item) for item in values)
    if not buf:
        return float(default)
    if len(buf) == 1:
        return float(buf[0])
    q = min(1.0, max(0.0, float(q)))
    pos = q * (len(buf) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(buf[lo])
    frac = pos - lo
    return float(buf[lo] * (1.0 - frac) + buf[hi] * frac)


class CarlaProbe:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        ego_role_name: str,
        timeout_sec: float,
        actor_id: Optional[int] = None,
        ego_discovery_timeout_sec: float = 20.0,
        ego_discovery_poll_sec: float = 1.0,
    ) -> None:
        self.client = carla.Client(host, int(port))
        self.client.set_timeout(float(timeout_sec))
        self.world = self.client.get_world()
        self.ego_role_name = str(ego_role_name)
        self.actor_id = int(actor_id) if actor_id is not None else None
        self.ego_discovery_timeout_sec = max(0.0, float(ego_discovery_timeout_sec))
        self.ego_discovery_poll_sec = max(0.05, float(ego_discovery_poll_sec))
        self.ego: Optional[carla.Vehicle] = None
        self.reference_transform: Optional[carla.Transform] = None
        settings = self.world.get_settings()
        self.sync_mode = bool(getattr(settings, "synchronous_mode", False))
        fixed_dt = _safe_float(getattr(settings, "fixed_delta_seconds", 0.0), 0.0)
        self.step_sec = fixed_dt if fixed_dt > 1e-3 else 0.05

    def list_vehicles(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for actor in self.world.get_actors().filter("vehicle.*"):
            attrs = getattr(actor, "attributes", {}) or {}
            out.append(
                {
                    "actor_id": int(getattr(actor, "id", 0) or 0),
                    "role_name": str(attrs.get("role_name", "") or "").strip(),
                    "type_id": str(getattr(actor, "type_id", "") or ""),
                    "location": {
                        "x": float(getattr(getattr(actor.get_transform(), "location", None), "x", 0.0)),
                        "y": float(getattr(getattr(actor.get_transform(), "location", None), "y", 0.0)),
                        "z": float(getattr(getattr(actor.get_transform(), "location", None), "z", 0.0)),
                    },
                }
            )
        return out

    def _discover_ego_once(self) -> Optional[carla.Vehicle]:
        vehicles = list(self.world.get_actors().filter("vehicle.*"))
        if self.actor_id is not None:
            for actor in vehicles:
                if int(getattr(actor, "id", 0) or 0) == self.actor_id:
                    return actor
            return None
        for actor in vehicles:
            role = ((getattr(actor, "attributes", {}) or {}).get("role_name", "") or "").strip()
            if role == self.ego_role_name or role in {"hero", "ego", "tb_ego"}:
                return actor
        return None

    def _discover_ego(self) -> carla.Vehicle:
        deadline = time.monotonic() + self.ego_discovery_timeout_sec
        last_vehicles_payload: List[Dict[str, Any]] = []
        while True:
            try:
                self.world = self.client.get_world()
            except Exception:
                pass
            actor = self._discover_ego_once()
            if actor is not None:
                self.ego = actor
                if self.reference_transform is None:
                    try:
                        self.reference_transform = actor.get_transform()
                    except Exception:
                        self.reference_transform = None
                return actor
            try:
                last_vehicles_payload = self.list_vehicles()
            except Exception:
                last_vehicles_payload = []
            if time.monotonic() >= deadline:
                break
            time.sleep(self.ego_discovery_poll_sec)
        if self.actor_id is not None:
            available_ids = [int(item.get("actor_id", 0) or 0) for item in last_vehicles_payload]
            raise RuntimeError(
                "vehicle actor_id not found, "
                f"requested={self.actor_id}, "
                f"available_actor_ids={available_ids}, "
                f"discovery_timeout_sec={self.ego_discovery_timeout_sec}"
            )
        raise RuntimeError(
            "ego vehicle not found, "
            f"requested_role={self.ego_role_name}, "
            f"available_vehicles={json.dumps(last_vehicles_payload, ensure_ascii=True)}, "
            f"discovery_timeout_sec={self.ego_discovery_timeout_sec}"
        )

    def vehicle(self) -> carla.Vehicle:
        if self.ego is None:
            return self._discover_ego()
        return self.ego

    def reset_to_reference_pose(self, *, settle_sec: float = 0.8) -> None:
        actor = self.vehicle()
        if self.reference_transform is None:
            try:
                self.reference_transform = actor.get_transform()
            except Exception:
                self.reference_transform = None
        try:
            actor.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0, hand_brake=False))
        except Exception:
            pass
        try:
            if self.reference_transform is not None:
                actor.set_transform(self.reference_transform)
        except Exception:
            pass
        for setter in ["set_target_velocity", "set_target_angular_velocity"]:
            fn = getattr(actor, setter, None)
            if callable(fn):
                try:
                    fn(carla.Vector3D(0.0, 0.0, 0.0))
                except Exception:
                    pass
        self._step(max(0.2, settle_sec * 0.5))
        try:
            actor.apply_control(carla.VehicleControl(throttle=0.0, brake=0.0, steer=0.0, hand_brake=False))
        except Exception:
            pass
        self._step(max(0.1, settle_sec * 0.5))

    def _step(self, duration_sec: float) -> None:
        if self.sync_mode:
            step_count = max(0, int(math.ceil(max(0.0, float(duration_sec)) / self.step_sec)))
            for _ in range(step_count):
                self.world.tick()
            return
        deadline = time.monotonic() + max(0.0, float(duration_sec))
        while time.monotonic() < deadline:
            time.sleep(self.step_sec)

    def apply(self, *, throttle: float, brake: float, steer: float) -> None:
        ctrl = carla.VehicleControl(
            throttle=clamp(float(throttle), 0.0, 1.0),
            brake=clamp(float(brake), 0.0, 1.0),
            steer=clamp(float(steer), -1.0, 1.0),
        )
        self.vehicle().apply_control(ctrl)

    def settle(self, *, throttle: float, brake: float, steer: float, duration_sec: float) -> None:
        self.apply(throttle=throttle, brake=brake, steer=steer)
        self._step(duration_sec)

    def state(self) -> Dict[str, Any]:
        actor = self.vehicle()
        vel = actor.get_velocity()
        accel = actor.get_acceleration()
        ang = actor.get_angular_velocity()
        tr = actor.get_transform()
        ctrl = actor.get_control()
        speed = math.sqrt(float(vel.x) ** 2 + float(vel.y) ** 2 + float(vel.z) ** 2)
        fwd = tr.get_forward_vector()
        forward_speed = (
            float(vel.x) * float(fwd.x)
            + float(vel.y) * float(fwd.y)
            + float(vel.z) * float(fwd.z)
        )
        forward_accel = (
            float(accel.x) * float(fwd.x)
            + float(accel.y) * float(fwd.y)
            + float(accel.z) * float(fwd.z)
        )
        out = {
            "speed_mps": float(speed),
            "forward_speed_mps": float(forward_speed),
            "forward_accel_mps2": float(forward_accel),
            "velocity_x_mps": float(vel.x),
            "velocity_y_mps": float(vel.y),
            "velocity_z_mps": float(vel.z),
            "accel_x_mps2": float(accel.x),
            "accel_y_mps2": float(accel.y),
            "accel_z_mps2": float(accel.z),
            "yaw_rate_rps": math.radians(float(getattr(ang, "z", 0.0))),
            "location_x_m": float(tr.location.x),
            "location_y_m": float(tr.location.y),
            "location_z_m": float(tr.location.z),
            "roll_deg": float(tr.rotation.roll),
            "pitch_deg": float(tr.rotation.pitch),
            "yaw_deg": float(tr.rotation.yaw),
            "throttle_cmd": float(getattr(ctrl, "throttle", 0.0)),
            "brake_cmd": float(getattr(ctrl, "brake", 0.0)),
            "carla_steer_cmd": float(getattr(ctrl, "steer", 0.0)),
            "gear": int(getattr(ctrl, "gear", 0)),
            "manual_gear_shift": bool(getattr(ctrl, "manual_gear_shift", False)),
            "reverse": bool(getattr(ctrl, "reverse", False)),
            "hand_brake": bool(getattr(ctrl, "hand_brake", False)),
        }
        wheel_angle = self._wheel_steer_angle_deg(actor)
        if wheel_angle is not None:
            out["measured_steer_deg"] = float(wheel_angle)
        if speed > 0.3:
            out["curvature"] = out["yaw_rate_rps"] / max(speed, 1e-3)
        else:
            out["curvature"] = 0.0
        return out

    def _wheel_steer_angle_deg(self, actor: carla.Vehicle) -> Optional[float]:
        get_wheel_angle = getattr(actor, "get_wheel_steer_angle", None)
        wheel_loc_enum = getattr(carla, "VehicleWheelLocation", None)
        if not callable(get_wheel_angle) or wheel_loc_enum is None:
            return None
        try:
            fl = float(get_wheel_angle(getattr(wheel_loc_enum, "FL_Wheel")))
            fr = float(get_wheel_angle(getattr(wheel_loc_enum, "FR_Wheel")))
        except Exception:
            return None
        return 0.5 * (fl + fr)

    def hold_and_sample(
        self,
        *,
        throttle: float,
        brake: float,
        steer: float,
        settle_sec: float,
        sample_sec: float,
    ) -> List[Dict[str, Any]]:
        self.settle(throttle=throttle, brake=brake, steer=steer, duration_sec=settle_sec)
        samples: List[Dict[str, float]] = []
        if self.sync_mode:
            step_count = max(0, int(math.ceil(max(0.0, float(sample_sec)) / self.step_sec)))
            for index in range(step_count):
                self.apply(throttle=throttle, brake=brake, steer=steer)
                self.world.tick()
                item = self.state()
                item["sample_index"] = int(index)
                item["elapsed_sec"] = float((index + 1) * self.step_sec)
                samples.append(item)
            return samples
        deadline = time.monotonic() + max(0.0, float(sample_sec))
        sample_index = 0
        while time.monotonic() < deadline:
            self.apply(throttle=throttle, brake=brake, steer=steer)
            time.sleep(self.step_sec)
            item = self.state()
            item["sample_index"] = int(sample_index)
            item["elapsed_sec"] = float(sample_sec - max(0.0, deadline - time.monotonic()))
            samples.append(item)
            sample_index += 1
        return samples

    def brake_to_stop(self, *, hold_brake: float = 1.0, timeout_sec: float = 6.0) -> None:
        if self.sync_mode:
            step_count = max(0, int(math.ceil(max(0.0, float(timeout_sec)) / self.step_sec)))
            for _ in range(step_count):
                self.apply(throttle=0.0, brake=hold_brake, steer=0.0)
                self.world.tick()
                if self.state()["speed_mps"] <= 0.1:
                    break
            self.settle(throttle=0.0, brake=hold_brake, steer=0.0, duration_sec=0.5)
            self.settle(throttle=0.0, brake=0.0, steer=0.0, duration_sec=0.2)
            return
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        while time.monotonic() < deadline:
            self.apply(throttle=0.0, brake=hold_brake, steer=0.0)
            time.sleep(self.step_sec)
            if self.state()["speed_mps"] <= 0.1:
                break
        self.settle(throttle=0.0, brake=hold_brake, steer=0.0, duration_sec=0.5)
        self.settle(throttle=0.0, brake=0.0, steer=0.0, duration_sec=0.2)

    def accelerate_to_speed(
        self,
        *,
        target_speed_mps: float,
        throttle: float,
        timeout_sec: float = 8.0,
    ) -> Dict[str, float]:
        if self.sync_mode:
            step_count = max(0, int(math.ceil(max(0.0, float(timeout_sec)) / self.step_sec)))
            final_speed = 0.0
            for _ in range(step_count):
                self.apply(throttle=throttle, brake=0.0, steer=0.0)
                self.world.tick()
                final_speed = self.state()["speed_mps"]
                if final_speed >= float(target_speed_mps):
                    break
            return {
                "target_speed_mps": float(target_speed_mps),
                "final_speed_mps": float(final_speed),
                "reached": bool(final_speed >= float(target_speed_mps)),
            }
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        final_speed = 0.0
        while time.monotonic() < deadline:
            self.apply(throttle=throttle, brake=0.0, steer=0.0)
            time.sleep(self.step_sec)
            final_speed = self.state()["speed_mps"]
            if final_speed >= float(target_speed_mps):
                break
        return {
            "target_speed_mps": float(target_speed_mps),
            "final_speed_mps": float(final_speed),
            "reached": bool(final_speed >= float(target_speed_mps)),
        }

    def vehicle_characteristics(self) -> Dict[str, Any]:
        actor = self.vehicle()
        physics = actor.get_physics_control()
        wheels = list(getattr(physics, "wheels", []) or [])
        max_steer_candidates = [abs(float(getattr(wheel, "max_steer_angle", 0.0) or 0.0)) for wheel in wheels]
        bbox = actor.bounding_box
        return {
            "actor_id": int(getattr(actor, "id", 0) or 0),
            "type_id": str(getattr(actor, "type_id", "") or ""),
            "role_name": ((getattr(actor, "attributes", {}) or {}).get("role_name", "") or "").strip(),
            "length": 2.0 * float(getattr(getattr(bbox, "extent", None), "x", 0.0)),
            "width": 2.0 * float(getattr(getattr(bbox, "extent", None), "y", 0.0)),
            "height": 2.0 * float(getattr(getattr(bbox, "extent", None), "z", 0.0)),
            "mass_kg": float(getattr(physics, "mass", 0.0) or 0.0),
            "max_steer_angle_deg": max(max_steer_candidates) if max_steer_candidates else None,
        }


def _response_delay_sec(samples: Sequence[Dict[str, float]], target_key: str) -> Optional[float]:
    values = [_safe_float(item.get(target_key), 0.0) for item in samples]
    if not values:
        return None
    steady = _median(values[len(values) // 2 :], default=0.0)
    if abs(steady) <= 1e-3:
        return None
    threshold = 0.5 * steady
    for item in samples:
        current = _safe_float(item.get(target_key), 0.0)
        if steady >= 0.0 and current >= threshold:
            return _safe_float(item.get("elapsed_sec"), 0.0)
        if steady < 0.0 and current <= threshold:
            return _safe_float(item.get("elapsed_sec"), 0.0)
    return None


def calibrate_steering(probe: CarlaProbe, args: argparse.Namespace) -> Dict[str, Any]:
    probe.brake_to_stop()
    records: List[Dict[str, float]] = []
    raw_series: List[Dict[str, float]] = []
    for cmd in _parse_float_list(args.steering_commands):
        samples = probe.hold_and_sample(
            throttle=args.steering_probe_throttle,
            brake=0.0,
            steer=cmd,
            settle_sec=args.steering_settle_sec,
            sample_sec=args.steering_sample_sec,
        )
        if not samples:
            continue
        raw_series.extend(samples)
        record = {
            "carla_steer_cmd": float(cmd),
            "measured_steer_deg": _median((item.get("measured_steer_deg", 0.0) for item in samples), default=0.0),
            "yaw_rate_rps": _median((item.get("yaw_rate_rps", 0.0) for item in samples), default=0.0),
            "curvature": _median((item.get("curvature", 0.0) for item in samples), default=0.0),
            "speed_mps": _median((item.get("speed_mps", 0.0) for item in samples), default=0.0),
        }
        records.append(record)
    inverse_angle = build_inverse_table(
        records,
        measured_key="measured_steer_deg",
        command_key="carla_steer_cmd",
        target_key="target_front_wheel_angle_deg",
        absolute=True,
        positive_only=True,
    )
    inverse_curvature = build_inverse_table(
        records,
        measured_key="curvature",
        command_key="carla_steer_cmd",
        target_key="target_curvature",
        absolute=True,
        positive_only=True,
    )
    return {
        "probe": {
            "throttle_cmd": float(args.steering_probe_throttle),
            "settle_sec": float(args.steering_settle_sec),
            "sample_sec": float(args.steering_sample_sec),
        },
        "measurements": records,
        "inverse": {
            "target_front_wheel_angle_deg_to_carla_cmd": inverse_angle,
            "target_curvature_to_carla_cmd": inverse_curvature,
        },
        "raw_series": raw_series[: args.max_raw_series_per_axis],
    }


def _speed_bin_targets(speed_bins: Sequence[Tuple[float, float]]) -> List[Tuple[float, float, float]]:
    out: List[Tuple[float, float, float]] = []
    for speed_min, speed_max in speed_bins:
        midpoint = 0.5 * (float(speed_min) + float(speed_max))
        if float(speed_min) <= 0.0:
            midpoint = min(float(speed_max) * 0.35, 0.75)
        out.append((float(speed_min), float(speed_max), max(0.0, midpoint)))
    return out


def _within_speed_bin(speed: float, *, speed_min: float, speed_max: float, tolerance: float) -> bool:
    lower = max(0.0, float(speed_min) - float(tolerance))
    upper = float(speed_max) + float(tolerance)
    return lower <= float(speed) < upper


def _effective_brake_decel(
    samples: Sequence[Dict[str, Any]],
    *,
    start_sec: float = 0.2,
    window_sec: float = 0.4,
) -> float:
    if not samples:
        return 0.0
    start = max(0.0, float(start_sec))
    end = max(0.0, float(window_sec))
    if end <= start:
        return 0.0
    speed_key = (
        "forward_speed_mps"
        if any("forward_speed_mps" in item for item in samples)
        else "speed_mps"
    )
    start_speed = _value_at_elapsed(samples, start, speed_key, default=0.0)
    end_speed = _value_at_elapsed(samples, end, speed_key, default=start_speed)
    return max(0.0, (start_speed - end_speed) / (end - start))


def _effective_throttle_accel(samples: Sequence[Dict[str, float]], *, window_sec: float) -> float:
    active = []
    for item in samples:
        elapsed = _safe_float(item.get("elapsed_sec"), 0.0)
        if elapsed > float(window_sec):
            continue
        speed = _safe_float(item.get("speed_mps"), 0.0)
        accel = _safe_float(item.get("forward_accel_mps2"), 0.0)
        if speed >= 0.2:
            active.append(accel)
    if not active:
        active = [_safe_float(item.get("forward_accel_mps2"), 0.0) for item in samples]
    return _percentile(active, 0.7, default=0.0)


def _effective_throttle_speed(samples: Sequence[Dict[str, float]], *, window_sec: float) -> float:
    active = []
    for item in samples:
        elapsed = _safe_float(item.get("elapsed_sec"), 0.0)
        if elapsed <= float(window_sec):
            active.append(_safe_float(item.get("speed_mps"), 0.0))
    if not active:
        active = [_safe_float(item.get("speed_mps"), 0.0) for item in samples]
    return _percentile(active, 0.7, default=0.0)


def _effective_brake_speed(samples: Sequence[Dict[str, float]], *, window_sec: float) -> float:
    active = []
    for item in samples:
        elapsed = _safe_float(item.get("elapsed_sec"), 0.0)
        if elapsed <= float(window_sec):
            active.append(_safe_float(item.get("speed_mps"), 0.0))
    if not active:
        active = [_safe_float(item.get("speed_mps"), 0.0) for item in samples]
    return _percentile(active, 0.7, default=0.0)


def _hold_motion_speed_mps(item: Dict[str, Any]) -> float:
    if "forward_speed_mps" in item:
        return abs(_safe_float(item.get("forward_speed_mps"), 0.0))
    if "velocity_x_mps" in item or "velocity_y_mps" in item:
        return math.hypot(
            _safe_float(item.get("velocity_x_mps"), 0.0),
            _safe_float(item.get("velocity_y_mps"), 0.0),
        )
    return abs(_safe_float(item.get("speed_mps"), 0.0))


def _bucketize_samples(
    samples: Sequence[Dict[str, float]],
    *,
    speed_bins: Sequence[Tuple[float, float]],
    speed_key: str,
    command_key: str,
    value_key: str,
    response_delay_sec: Optional[float],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    grouped: Dict[Tuple[float, float], List[Dict[str, float]]] = {}
    for item in samples:
        speed = _safe_float(item.get(speed_key), float("nan"))
        if math.isnan(speed):
            continue
        for speed_min, speed_max in speed_bins:
            if speed_min <= speed < speed_max:
                grouped.setdefault((speed_min, speed_max), []).append(item)
                break
    for speed_min, speed_max in speed_bins:
        bucket = grouped.get((speed_min, speed_max), [])
        if not bucket:
            continue
        out.append(
            {
                "speed_min_mps": float(speed_min),
                "speed_max_mps": float(speed_max),
                command_key: _median((item.get(command_key, 0.0) for item in bucket), default=0.0),
                value_key: _median((item.get(value_key, 0.0) for item in bucket), default=0.0),
                "sample_count": len(bucket),
                "response_delay_sec": response_delay_sec,
            }
        )
    return out


def _aggregate_speed_bins(
    rows: Sequence[Dict[str, Any]],
    *,
    command_key: str,
    value_key: str,
    target_key: str,
) -> List[Dict[str, Any]]:
    bins: Dict[Tuple[float, float], List[Dict[str, Any]]] = {}
    for row in rows:
        key = (_safe_float(row.get("speed_min_mps"), 0.0), _safe_float(row.get("speed_max_mps"), 0.0))
        bins.setdefault(key, []).append(row)
    out: List[Dict[str, Any]] = []
    for (speed_min, speed_max), bucket in sorted(bins.items()):
        samples: List[Dict[str, float]] = []
        delays: List[float] = []
        raw_measurements: List[Dict[str, Any]] = []
        by_cmd: Dict[float, List[Dict[str, Any]]] = {}
        for item in bucket:
            cmd = _safe_float(item.get(command_key), 0.0)
            by_cmd.setdefault(cmd, []).append(item)
            raw_measurements.append(dict(item))
        for cmd in sorted(by_cmd):
            cmd_rows = by_cmd[cmd]
            value = _median((row.get(value_key, 0.0) for row in cmd_rows), default=0.0)
            delay = _median(
                [row.get("response_delay_sec", 0.0) for row in cmd_rows if row.get("response_delay_sec") is not None],
                default=0.0,
            )
            sample_count = int(sum(int(row.get("sample_count", 0) or 0) for row in cmd_rows))
            samples.append({command_key: float(cmd), value_key: float(value), "sample_count": sample_count})
            if delay > 0.0:
                delays.append(delay)
        inverse = build_inverse_table(
            samples,
            measured_key=value_key,
            command_key=command_key,
            target_key=target_key,
            absolute=False,
            positive_only=True,
        )
        out.append(
            {
                "speed_min_mps": float(speed_min),
                "speed_max_mps": float(speed_max),
                "measurements": samples,
                "inverse": {
                    f"{target_key}_to_{command_key}": inverse,
                },
                "median_response_delay_sec": _median(delays, default=0.0) if delays else None,
                "raw_measurements": raw_measurements[:200],
            }
        )
    return out


def _is_monotonic_non_decreasing(values: Sequence[float], *, tolerance: float = 0.15) -> bool:
    last = None
    for value in values:
        current = float(value)
        if last is not None and current + float(tolerance) < last:
            return False
        last = max(current, last if last is not None else current)
    return True


def _annotate_axis_reliability(
    speed_bins: List[Dict[str, Any]],
    *,
    axis: str,
    brake_min_incremental_signal_mps2: float = 0.2,
    brake_incremental_monotonic_tolerance_mps2: float = 0.1,
) -> None:
    for bucket in speed_bins:
        measurements = list(bucket.get("measurements", []) or [])
        values = []
        observed = []
        if axis == "throttle":
            values = [_safe_float(item.get("forward_accel_mps2"), 0.0) for item in measurements]
            observed = [_safe_float(item.get("observed_speed_mps"), 0.0) for item in bucket.get("raw_measurements", [])]
            max_value = max(values) if values else 0.0
            reliable = bool(
                len(measurements) >= 2
                and max_value >= 0.1
                and _is_monotonic_non_decreasing(values, tolerance=0.3)
            )
            reasons = []
            if len(measurements) < 2:
                reasons.append("insufficient_measurements")
            if max_value < 0.1:
                reasons.append("weak_positive_accel")
            if values and not _is_monotonic_non_decreasing(values, tolerance=0.3):
                reasons.append("non_monotonic")
        else:
            values = [_safe_float(item.get("target_decel_mps2"), 0.0) for item in measurements]
            incremental_values = [
                _safe_float(item.get("incremental_brake_decel_mps2"), 0.0)
                for item in measurements
            ]
            positive_measurements = [
                item for item in measurements if _safe_float(item.get("brake_cmd"), 0.0) > 1e-6
            ]
            observed = [_safe_float(item.get("observed_speed_mps"), 0.0) for item in bucket.get("raw_measurements", [])]
            max_value = max(values) if values else 0.0
            max_incremental = max(incremental_values) if incremental_values else 0.0
            coast_baseline = bucket.get("coast_baseline", {})
            coast_baseline_available = bool(
                isinstance(coast_baseline, dict) and coast_baseline.get("available")
            )
            reliable = bool(
                coast_baseline_available
                and len(positive_measurements) >= 3
                and max_incremental >= float(brake_min_incremental_signal_mps2)
                and max(observed, default=0.0) >= 1.0
                and _is_monotonic_non_decreasing(
                    incremental_values,
                    tolerance=float(brake_incremental_monotonic_tolerance_mps2),
                )
            )
            reasons = []
            if not coast_baseline_available:
                reasons.append("missing_coast_baseline")
            if len(positive_measurements) < 3:
                reasons.append("insufficient_measurements")
            if max_incremental < float(brake_min_incremental_signal_mps2):
                reasons.append("weak_incremental_brake_signal")
            if max(observed, default=0.0) < 1.0:
                reasons.append("low_observed_speed")
            if incremental_values and not _is_monotonic_non_decreasing(
                incremental_values,
                tolerance=float(brake_incremental_monotonic_tolerance_mps2),
            ):
                reasons.append("non_monotonic_incremental_brake_response")
        bucket["quality"] = {
            "reliable": reliable,
            "reasons": reasons,
            "max_value": float(max(values) if values else 0.0),
            "max_observed_speed_mps": float(max(observed, default=0.0)),
        }
        if axis == "brake":
            bucket["quality"].update(
                {
                    "coast_baseline_available": coast_baseline_available,
                    "max_incremental_brake_decel_mps2": float(max_incremental),
                    "incremental_monotonic_tolerance_mps2": float(
                        brake_incremental_monotonic_tolerance_mps2
                    ),
                    "minimum_incremental_signal_mps2": float(
                        brake_min_incremental_signal_mps2
                    ),
                }
            )


def _annotate_brake_coast_baseline(speed_bins: List[Dict[str, Any]]) -> None:
    for bucket in speed_bins:
        raw_measurements = list(bucket.get("raw_measurements", []) or [])
        baseline_rows = [
            item
            for item in raw_measurements
            if abs(_safe_float(item.get("brake_cmd"), 0.0)) <= 1e-6
        ]
        baseline = (
            _median((item.get("target_decel_mps2", 0.0) for item in baseline_rows), default=0.0)
            if baseline_rows
            else None
        )
        bucket["coast_baseline"] = {
            "available": baseline is not None,
            "brake_cmd": 0.0,
            "target_decel_mps2": float(baseline) if baseline is not None else None,
            "sample_count": int(
                sum(int(item.get("sample_count", 0) or 0) for item in baseline_rows)
            ),
            "response_metric": "forward_speed_delta_over_fixed_window",
        }
        measurements = list(bucket.get("measurements", []) or [])
        for item in measurements:
            total_decel = _safe_float(item.get("target_decel_mps2"), 0.0)
            item["coast_baseline_decel_mps2"] = (
                float(baseline) if baseline is not None else None
            )
            item["incremental_brake_decel_mps2"] = (
                max(0.0, total_decel - float(baseline))
                if baseline is not None
                else None
            )
        bucket.setdefault("inverse", {})[
            "target_incremental_brake_decel_mps2_to_brake_cmd"
        ] = build_inverse_table(
            measurements,
            measured_key="incremental_brake_decel_mps2",
            command_key="brake_cmd",
            target_key="target_incremental_brake_decel_mps2",
            absolute=False,
            positive_only=True,
        )


def _value_at_elapsed(samples: Sequence[Dict[str, float]], elapsed_sec: float, key: str, *, default: float = 0.0) -> float:
    if not samples:
        return float(default)
    target = float(elapsed_sec)
    for item in samples:
        if _safe_float(item.get("elapsed_sec"), 0.0) >= target:
            return _safe_float(item.get(key), default)
    return _safe_float(samples[-1].get(key), default)


def _time_to_speed(samples: Sequence[Dict[str, float]], speed_mps: float) -> Optional[float]:
    target = float(speed_mps)
    for item in samples:
        if _safe_float(item.get("speed_mps"), 0.0) >= target:
            return _safe_float(item.get("elapsed_sec"), 0.0)
    return None


def _time_to_stop(samples: Sequence[Dict[str, float]], *, stop_speed_mps: float = 0.1) -> Optional[float]:
    target = float(stop_speed_mps)
    for item in samples:
        if _safe_float(item.get("speed_mps"), 0.0) <= target:
            return _safe_float(item.get("elapsed_sec"), 0.0)
    return None


def _quality_for_measurements(
    measurements: Sequence[Dict[str, Any]],
    *,
    command_key: str,
    value_key: str,
    reliable_min_samples: int,
    monotonic_tolerance: float,
    min_max_value: float,
) -> Dict[str, Any]:
    rows = sorted(measurements, key=lambda item: _safe_float(item.get(command_key), 0.0))
    values = [_safe_float(item.get(value_key), 0.0) for item in rows]
    max_value = max(values) if values else 0.0
    reasons: List[str] = []
    if len(rows) < int(reliable_min_samples):
        reasons.append("insufficient_measurements")
    if max_value < float(min_max_value):
        reasons.append("weak_signal")
    if values and not _is_monotonic_non_decreasing(values, tolerance=monotonic_tolerance):
        reasons.append("non_monotonic")
    return {
        "reliable": not reasons,
        "reasons": reasons,
        "max_value": float(max_value),
        "sample_count": len(rows),
    }


def _monotonic_upper_envelope(
    measurements: Sequence[Dict[str, Any]],
    *,
    command_key: str,
    value_key: str,
    min_value: float = 0.0,
) -> List[Dict[str, Any]]:
    rows = sorted(
        (item for item in measurements if _safe_float(item.get(command_key), -1.0) >= 0.0),
        key=lambda item: _safe_float(item.get(command_key), 0.0),
    )
    out: List[Dict[str, Any]] = []
    best = float("-inf")
    threshold = float(min_value)
    for item in rows:
        value = _safe_float(item.get(value_key), float("-inf"))
        if value < threshold:
            continue
        if value <= best + 1e-6:
            continue
        out.append(dict(item))
        best = value
    return out


def calibrate_low_speed_throttle(probe: CarlaProbe, args: argparse.Namespace) -> Dict[str, Any]:
    commands = _parse_float_list(args.low_speed_throttle_commands)
    raw_series: List[Dict[str, Any]] = []

    launch_rows: List[Dict[str, Any]] = []
    for cmd in commands:
        probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
        samples = probe.hold_and_sample(
            throttle=cmd,
            brake=0.0,
            steer=0.0,
            settle_sec=0.0,
            sample_sec=args.low_speed_launch_sample_sec,
        )
        if not samples:
            continue
        eval_speed = _value_at_elapsed(samples, args.low_speed_launch_eval_sec, "speed_mps", default=0.0)
        eval_accel = _effective_throttle_accel(samples, window_sec=float(args.low_speed_launch_eval_sec))
        launch_rows.append(
            {
                "throttle_cmd": float(cmd),
                "speed_at_eval_mps": float(eval_speed),
                "launch_accel_mps2": float(eval_accel),
                "time_to_1mps_sec": _time_to_speed(samples, 1.0),
                "time_to_2mps_sec": _time_to_speed(samples, 2.0),
                "reached_1mps": _time_to_speed(samples, 1.0) is not None,
                "reached_2mps": _time_to_speed(samples, 2.0) is not None,
                "sample_count": len(samples),
            }
        )
        raw_series.extend({**item, "low_speed_mode": "launch"} for item in samples)
    launch_quality = _quality_for_measurements(
        launch_rows,
        command_key="throttle_cmd",
        value_key="speed_at_eval_mps",
        reliable_min_samples=4,
        monotonic_tolerance=0.2,
        min_max_value=1.0,
    )
    launch_boost_rows = _monotonic_upper_envelope(
        [
            item
            for item in launch_rows
            if bool(item.get("reached_1mps")) or _safe_float(item.get("speed_at_eval_mps"), 0.0) >= 1.0
        ],
        command_key="throttle_cmd",
        value_key="launch_accel_mps2",
        min_value=0.1,
    )
    launch_boost_quality = _quality_for_measurements(
        launch_boost_rows,
        command_key="throttle_cmd",
        value_key="launch_accel_mps2",
        reliable_min_samples=3,
        monotonic_tolerance=0.15,
        min_max_value=1.0,
    )

    crawl_rows: List[Dict[str, Any]] = []
    crawl_sections: List[Dict[str, Any]] = []
    crawl_boost_sections: List[Dict[str, Any]] = []
    for entry_speed in _parse_float_list(args.low_speed_crawl_entry_speeds):
        section_rows: List[Dict[str, Any]] = []
        for cmd in commands:
            probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
            prep = probe.accelerate_to_speed(
                target_speed_mps=float(entry_speed),
                throttle=max(float(cmd), float(args.longitudinal_prep_throttle)),
                timeout_sec=args.longitudinal_prep_timeout_sec,
            )
            samples = probe.hold_and_sample(
                throttle=cmd,
                brake=0.0,
                steer=0.0,
                settle_sec=0.0,
                sample_sec=args.low_speed_crawl_sample_sec,
            )
            if not samples:
                continue
            start_speed = _safe_float(samples[0].get("speed_mps"), 0.0)
            eval_speed = _value_at_elapsed(samples, args.low_speed_crawl_eval_sec, "speed_mps", default=start_speed)
            eval_sec = max(float(args.low_speed_crawl_eval_sec), 1e-6)
            row = {
                "entry_speed_mps": float(entry_speed),
                "throttle_cmd": float(cmd),
                "delta_speed_mps": float(eval_speed - start_speed),
                "effective_accel_mps2": float((eval_speed - start_speed) / eval_sec),
                "effective_accel_window_sec": eval_sec,
                "speed_retained_mps": float(eval_speed),
                "speed_at_eval_mps": float(eval_speed),
                "crawl_accel_mps2": float(_effective_throttle_accel(samples, window_sec=float(args.low_speed_crawl_eval_sec))),
                "entry_reached": bool(prep.get("reached")),
                "sample_count": len(samples),
                "prep": prep,
            }
            section_rows.append(row)
            crawl_rows.append(row)
            raw_series.extend({**item, "low_speed_mode": "crawl", "entry_speed_mps": float(entry_speed)} for item in samples)
        quality = _quality_for_measurements(
            section_rows,
            command_key="throttle_cmd",
            value_key="speed_retained_mps",
            reliable_min_samples=3,
            monotonic_tolerance=0.15,
            min_max_value=max(1.0, 0.5 * float(entry_speed)),
        )
        inverse = build_inverse_table(
            section_rows,
            measured_key="speed_retained_mps",
            command_key="throttle_cmd",
            target_key="target_speed_retained_mps",
            absolute=False,
            positive_only=True,
        )
        crawl_sections.append(
            {
                "entry_speed_mps": float(entry_speed),
                "probe": {
                    "sample_sec": float(args.low_speed_crawl_sample_sec),
                    "eval_sec": float(args.low_speed_crawl_eval_sec),
                },
                "measurements": section_rows,
                "inverse": {"target_speed_retained_mps_to_throttle_cmd": inverse},
                "quality": quality,
            }
        )
        boost_rows = _monotonic_upper_envelope(
            [
                item
                for item in section_rows
                if _safe_float(item.get("delta_speed_mps"), 0.0) > 0.5
                and _safe_float(item.get("crawl_accel_mps2"), 0.0) > 0.0
            ],
            command_key="throttle_cmd",
            value_key="crawl_accel_mps2",
            min_value=0.1,
        )
        boost_quality = _quality_for_measurements(
            boost_rows,
            command_key="throttle_cmd",
            value_key="crawl_accel_mps2",
            reliable_min_samples=2,
            monotonic_tolerance=0.2,
            min_max_value=0.5,
        )
        crawl_boost_sections.append(
            {
                "entry_speed_mps": float(entry_speed),
                "measurements": boost_rows,
                "inverse": {
                    "target_accel_mps2_to_throttle_cmd": build_inverse_table(
                        boost_rows,
                        measured_key="crawl_accel_mps2",
                        command_key="throttle_cmd",
                        target_key="target_accel_mps2",
                        absolute=False,
                        positive_only=True,
                    )
                },
                "quality": boost_quality,
            }
        )
    return {
        "launch": {
            "probe": {
                "sample_sec": float(args.low_speed_launch_sample_sec),
                "eval_sec": float(args.low_speed_launch_eval_sec),
            },
            "measurements": launch_rows,
            "inverse": {
                "target_speed_at_eval_mps_to_throttle_cmd": build_inverse_table(
                    launch_rows,
                    measured_key="speed_at_eval_mps",
                    command_key="throttle_cmd",
                    target_key="target_speed_at_eval_mps",
                    absolute=False,
                    positive_only=True,
                )
            },
            "quality": launch_quality,
        },
        "launch_boost": {
            "measurements": launch_boost_rows,
            "inverse": {
                "target_accel_mps2_to_throttle_cmd": build_inverse_table(
                    launch_boost_rows,
                    measured_key="launch_accel_mps2",
                    command_key="throttle_cmd",
                    target_key="target_accel_mps2",
                    absolute=False,
                    positive_only=True,
                )
            },
            "quality": launch_boost_quality,
        },
        "crawl": crawl_sections,
        "crawl_boost": crawl_boost_sections,
        "raw_series": raw_series[: args.max_raw_series_per_axis],
    }


def calibrate_low_speed_brake(probe: CarlaProbe, args: argparse.Namespace) -> Dict[str, Any]:
    commands = _parse_float_list(args.low_speed_brake_commands)
    raw_series: List[Dict[str, Any]] = []

    stop_rows: List[Dict[str, Any]] = []
    for cmd in commands:
        probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
        prep = probe.accelerate_to_speed(
            target_speed_mps=float(args.low_speed_hold_entry_speed_mps),
            throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
            timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
        )
        samples = probe.hold_and_sample(
            throttle=0.0,
            brake=cmd,
            steer=0.0,
            settle_sec=0.0,
            sample_sec=args.low_speed_hold_sample_sec,
        )
        if not samples:
            continue
        residual_speed = _value_at_elapsed(samples, args.low_speed_hold_eval_sec, "speed_mps", default=0.0)
        stop_rows.append(
            {
                "brake_cmd": float(cmd),
                "residual_speed_mps": float(residual_speed),
                "stop_time_sec": _time_to_stop(samples, stop_speed_mps=args.low_speed_hold_stop_speed_mps),
                "stops_vehicle": bool(residual_speed <= float(args.low_speed_hold_residual_speed_threshold_mps)),
                "entry_reached": bool(prep.get("reached")),
                "sample_count": len(samples),
                "prep": prep,
            }
        )
        raw_series.extend({**item, "low_speed_mode": "stop"} for item in samples)
    stop_quality_rows = [
        {"brake_cmd": row["brake_cmd"], "stop_strength": max(0.0, 1.0 - _safe_float(row.get("residual_speed_mps"), 0.0))}
        for row in stop_rows
    ]
    stop_quality = _quality_for_measurements(
        stop_quality_rows,
        command_key="brake_cmd",
        value_key="stop_strength",
        reliable_min_samples=4,
        monotonic_tolerance=0.12,
        min_max_value=0.8,
    )
    stop_cmds = [
        _safe_float(row.get("brake_cmd"), 0.0)
        for row in stop_rows
        if bool(row.get("entry_reached")) and bool(row.get("stops_vehicle"))
    ]

    hold_rows: List[Dict[str, Any]] = []
    for cmd in commands:
        probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
        start_state = probe.state()
        samples = probe.hold_and_sample(
            throttle=0.0,
            brake=cmd,
            steer=0.0,
            settle_sec=0.0,
            sample_sec=args.low_speed_hold_sample_sec,
        )
        if not samples:
            continue
        eval_rows = [
            item
            for item in samples
            if _safe_float(item.get("elapsed_sec"), 0.0)
            <= float(args.low_speed_hold_eval_sec) + 1e-9
        ]
        if not eval_rows:
            eval_rows = list(samples)
        max_speed = max(
            (_hold_motion_speed_mps(item) for item in eval_rows),
            default=0.0,
        )
        residual_row = next(
            (
                item
                for item in samples
                if _safe_float(item.get("elapsed_sec"), 0.0)
                >= float(args.low_speed_hold_eval_sec)
            ),
            samples[-1],
        )
        residual_speed = _hold_motion_speed_mps(residual_row)
        end_state = eval_rows[-1]
        displacement_m = math.hypot(
            _safe_float(end_state.get("location_x_m"), 0.0)
            - _safe_float(start_state.get("location_x_m"), 0.0),
            _safe_float(end_state.get("location_y_m"), 0.0)
            - _safe_float(start_state.get("location_y_m"), 0.0),
        )
        holds_vehicle = bool(
            max_speed <= float(args.low_speed_hold_residual_speed_threshold_mps)
        )
        hold_rows.append(
            {
                "brake_cmd": float(cmd),
                "initial_speed_mps": _hold_motion_speed_mps(start_state),
                "initial_speed_norm_mps": _safe_float(start_state.get("speed_mps"), 0.0),
                "max_speed_mps": float(max_speed),
                "residual_speed_mps": float(residual_speed),
                "displacement_m": float(displacement_m),
                "holds_vehicle": holds_vehicle,
                "sample_count": len(samples),
            }
        )
        raw_series.extend({**item, "low_speed_mode": "hold"} for item in samples)
    hold_quality_rows = [
        {
            "brake_cmd": row["brake_cmd"],
            "hold_strength": max(
                0.0,
                1.0 - _safe_float(row.get("max_speed_mps"), 0.0),
            ),
        }
        for row in hold_rows
    ]
    hold_quality = _quality_for_measurements(
        hold_quality_rows,
        command_key="brake_cmd",
        value_key="hold_strength",
        reliable_min_samples=4,
        monotonic_tolerance=0.12,
        min_max_value=0.8,
    )
    hold_cmds = [
        _safe_float(row.get("brake_cmd"), 0.0)
        for row in hold_rows
        if bool(row.get("holds_vehicle"))
    ]

    rolling_sections: List[Dict[str, Any]] = []
    for entry_speed in _parse_float_list(args.low_speed_rolling_brake_entry_speeds):
        section_rows: List[Dict[str, Any]] = []
        for cmd in commands:
            probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
            prep = probe.accelerate_to_speed(
                target_speed_mps=float(entry_speed),
                throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
                timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
            )
            samples = probe.hold_and_sample(
                throttle=0.0,
                brake=cmd,
                steer=0.0,
                settle_sec=0.0,
                sample_sec=args.low_speed_rolling_brake_sample_sec,
            )
            if not samples:
                continue
            start_speed = _safe_float(samples[0].get("speed_mps"), 0.0)
            eval_speed = _value_at_elapsed(samples, args.low_speed_rolling_brake_eval_sec, "speed_mps", default=start_speed)
            row = {
                "entry_speed_mps": float(entry_speed),
                "brake_cmd": float(cmd),
                "speed_drop_mps": float(max(0.0, start_speed - eval_speed)),
                "residual_speed_mps": float(eval_speed),
                "target_decel_mps2": float(
                    _effective_brake_decel(
                        samples,
                        start_sec=min(
                            float(args.brake_response_start_sec),
                            0.5 * float(args.low_speed_rolling_brake_eval_sec),
                        ),
                        window_sec=float(args.low_speed_rolling_brake_eval_sec),
                    )
                ),
                "stop_time_sec": _time_to_stop(samples, stop_speed_mps=args.low_speed_hold_stop_speed_mps),
                "entry_reached": bool(prep.get("reached")),
                "sample_count": len(samples),
                "prep": prep,
            }
            section_rows.append(row)
            raw_series.extend({**item, "low_speed_mode": "rolling_brake", "entry_speed_mps": float(entry_speed)} for item in samples)
        rolling_rows = [
            row
            for row in section_rows
            if bool(row.get("entry_reached"))
            and _safe_float(row.get("residual_speed_mps"), 0.0)
            > float(args.low_speed_hold_stop_speed_mps)
            and (
                row.get("stop_time_sec") is None
                or _safe_float(row.get("stop_time_sec"), 0.0)
                > float(args.low_speed_rolling_brake_eval_sec)
            )
        ]
        quality = _quality_for_measurements(
            rolling_rows,
            command_key="brake_cmd",
            value_key="speed_drop_mps",
            reliable_min_samples=4,
            monotonic_tolerance=0.2,
            min_max_value=0.5,
        )
        inverse = build_inverse_table(
            rolling_rows,
            measured_key="speed_drop_mps",
            command_key="brake_cmd",
            target_key="target_speed_drop_mps",
            absolute=False,
            positive_only=True,
        )
        rolling_sections.append(
            {
                "entry_speed_mps": float(entry_speed),
                "speed_min_mps": max(
                    0.0,
                    float(entry_speed)
                    - float(args.low_speed_rolling_speed_coverage_half_width_mps),
                ),
                "speed_max_mps": float(entry_speed)
                + float(args.low_speed_rolling_speed_coverage_half_width_mps),
                "probe": {
                    "sample_sec": float(args.low_speed_rolling_brake_sample_sec),
                    "eval_sec": float(args.low_speed_rolling_brake_eval_sec),
                },
                "measurements": section_rows,
                "rolling_measurements": rolling_rows,
                "inverse": {"target_speed_drop_mps_to_brake_cmd": inverse},
                "quality": {
                    **quality,
                    "state_transition_excluded_count": len(section_rows)
                    - len(rolling_rows),
                    "rolling_measurement_count": len(rolling_rows),
                },
            }
        )
    return {
        "stop": {
            "probe": {
                "entry_speed_mps": float(args.low_speed_hold_entry_speed_mps),
                "sample_sec": float(args.low_speed_hold_sample_sec),
                "eval_sec": float(args.low_speed_hold_eval_sec),
                "activation_max_speed_mps": float(args.low_speed_stop_activation_max_speed_mps),
                "request_decel_threshold_mps2": float(
                    args.low_speed_stop_request_decel_threshold_mps2
                ),
            },
            "measurements": stop_rows,
            "summary": {
                "stop_cmd": min(stop_cmds) if stop_cmds else None,
            },
            "quality": stop_quality,
        },
        "hold": {
            "probe": {
                "initial_state": "stationary",
                "sample_sec": float(args.low_speed_hold_sample_sec),
                "eval_sec": float(args.low_speed_hold_eval_sec),
                "activation_max_speed_mps": float(args.low_speed_hold_stop_speed_mps),
            },
            "measurements": hold_rows,
            "summary": {
                "hold_cmd": min(hold_cmds) if hold_cmds else None,
            },
            "quality": hold_quality,
        },
        "rolling": rolling_sections,
        "raw_series": raw_series[: args.max_raw_series_per_axis],
    }


def calibrate_throttle(probe: CarlaProbe, args: argparse.Namespace) -> Dict[str, Any]:
    speed_bins = _parse_speed_edges(args.speed_bins)
    target_bins = _speed_bin_targets(speed_bins)
    rows: List[Dict[str, Any]] = []
    signed_rows: List[Dict[str, Any]] = []
    boost_sections: List[Dict[str, Any]] = []
    raw_series: List[Dict[str, Any]] = []
    for speed_min, speed_max, target_speed in target_bins:
        boost_rows: List[Dict[str, Any]] = []
        for cmd in _parse_float_list(args.throttle_commands):
            probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
            prep = None
            if target_speed > 0.15:
                prep = probe.accelerate_to_speed(
                    target_speed_mps=target_speed,
                    throttle=max(float(cmd), float(args.longitudinal_prep_throttle)),
                    timeout_sec=args.longitudinal_prep_timeout_sec,
                )
            samples = probe.hold_and_sample(
                throttle=cmd,
                brake=0.0,
                steer=0.0,
                settle_sec=0.0,
                sample_sec=args.throttle_sample_sec,
            )
            if not samples:
                continue
            delay = _response_delay_sec(samples, "forward_accel_mps2")
            effective_speed = _effective_throttle_speed(
                samples,
                window_sec=float(args.throttle_effective_window_sec),
            )
            entry_reached = bool(
                prep is None
                or abs(float(prep.get("final_speed_mps", 0.0)) - float(target_speed))
                <= float(args.longitudinal_entry_speed_tolerance_mps)
                or float(prep.get("final_speed_mps", 0.0)) >= float(speed_min)
            )
            onset_accel = _effective_throttle_accel(
                samples,
                window_sec=float(args.throttle_effective_window_sec),
            )
            effective_window_sec = max(float(args.throttle_effective_window_sec), 1e-6)
            start_speed = _safe_float(samples[0].get("speed_mps"), 0.0)
            speed_at_effective_window = _value_at_elapsed(
                samples,
                effective_window_sec,
                "speed_mps",
                default=start_speed,
            )
            signed_effective_accel = (
                float(speed_at_effective_window) - float(start_speed)
            ) / effective_window_sec
            signed_entry_valid = bool(
                entry_reached
                and (
                    target_speed <= 0.15
                    or _within_speed_bin(
                        start_speed,
                        speed_min=speed_min,
                        speed_max=speed_max,
                        tolerance=float(args.longitudinal_entry_speed_tolerance_mps),
                    )
                )
            )
            boost_rows.append(
                {
                    "entry_speed_mps": float(target_speed),
                    "speed_min_mps": float(speed_min),
                    "speed_max_mps": float(speed_max),
                    "throttle_cmd": float(cmd),
                    "target_accel_mps2": float(onset_accel),
                    "observed_speed_mps": float(effective_speed),
                    "entry_reached": entry_reached,
                    "sample_count": len(samples),
                    "prep": prep,
                }
            )
            if signed_entry_valid:
                signed_rows.append(
                    {
                        "speed_min_mps": float(speed_min),
                        "speed_max_mps": float(speed_max),
                        "throttle_cmd": float(cmd),
                        "forward_accel_mps2": float(signed_effective_accel),
                        "sample_count": len(samples),
                        "response_delay_sec": delay,
                        "observed_speed_mps": float(start_speed),
                        "target_entry_speed_mps": float(target_speed),
                        "entry_reached": signed_entry_valid,
                        "effective_accel_window_sec": effective_window_sec,
                    }
                )
            if (not entry_reached) or (
                target_speed > 0.15
                and not _within_speed_bin(
                    effective_speed,
                    speed_min=speed_min,
                    speed_max=speed_max,
                    tolerance=float(args.longitudinal_entry_speed_tolerance_mps),
                )
            ):
                raw_series.extend(
                    {
                        **item,
                        "target_speed_bin": [speed_min, speed_max],
                        "dropped_reason": "entry_speed_mismatch",
                    }
                    for item in samples
                )
                continue
            rows.append(
                {
                    "speed_min_mps": float(speed_min),
                    "speed_max_mps": float(speed_max),
                    "throttle_cmd": float(cmd),
                    "forward_accel_mps2": float(onset_accel),
                    "sample_count": len(samples),
                    "response_delay_sec": delay,
                    "observed_speed_mps": float(effective_speed),
                    "target_entry_speed_mps": float(target_speed),
                    "entry_reached": entry_reached,
                    "prep": prep,
                }
            )
            raw_series.extend({**item, "target_speed_bin": [speed_min, speed_max]} for item in samples)
        boost_rows = [
            item
            for item in boost_rows
            if bool(item.get("entry_reached")) and _safe_float(item.get("target_accel_mps2"), 0.0) > 0.1
        ]
        boost_rows = _monotonic_upper_envelope(
            boost_rows,
            command_key="throttle_cmd",
            value_key="target_accel_mps2",
            min_value=0.1,
        )
        boost_quality = _quality_for_measurements(
            boost_rows,
            command_key="throttle_cmd",
            value_key="target_accel_mps2",
            reliable_min_samples=2,
            monotonic_tolerance=0.2,
            min_max_value=0.5,
        )
        boost_sections.append(
            {
                "entry_speed_mps": float(target_speed),
                "speed_min_mps": float(speed_min),
                "speed_max_mps": float(speed_max),
                "measurements": boost_rows,
                "inverse": {
                    "target_accel_mps2_to_throttle_cmd": build_inverse_table(
                        boost_rows,
                        measured_key="target_accel_mps2",
                        command_key="throttle_cmd",
                        target_key="target_accel_mps2",
                        absolute=False,
                        positive_only=True,
                    )
                },
                "quality": boost_quality,
            }
        )
    aggregated = _aggregate_speed_bins(
        rows,
        command_key="throttle_cmd",
        value_key="forward_accel_mps2",
        target_key="target_accel_mps2",
    )
    _annotate_axis_reliability(aggregated, axis="throttle")
    signed_aggregated = _aggregate_speed_bins(
        signed_rows,
        command_key="throttle_cmd",
        value_key="forward_accel_mps2",
        target_key="target_accel_mps2",
    )
    _annotate_axis_reliability(signed_aggregated, axis="throttle")
    for bucket in signed_aggregated:
        bucket["response_metric"] = "delta_speed_over_effective_window"
        bucket["effective_accel_window_sec"] = float(args.throttle_effective_window_sec)
    return {
        "probe": {
            "settle_sec": float(args.throttle_settle_sec),
            "sample_sec": float(args.throttle_sample_sec),
            "effective_window_sec": float(args.throttle_effective_window_sec),
            "prep_throttle": float(args.longitudinal_prep_throttle),
            "prep_timeout_sec": float(args.longitudinal_prep_timeout_sec),
            "entry_speed_tolerance_mps": float(args.longitudinal_entry_speed_tolerance_mps),
            "speed_bins": [{"min": float(lo), "max": float(hi)} for lo, hi in speed_bins],
        },
        "speed_bins": aggregated,
        "signed_speed_bins": signed_aggregated,
        "mid_speed_boost": boost_sections,
        "raw_series": raw_series[: args.max_raw_series_per_axis],
    }


def calibrate_brake(probe: CarlaProbe, args: argparse.Namespace) -> Dict[str, Any]:
    speed_bins = _parse_speed_edges(args.speed_bins)
    target_bins = _speed_bin_targets(speed_bins)
    rows: List[Dict[str, Any]] = []
    raw_series: List[Dict[str, Any]] = []
    unavailable_speed_bins: List[Dict[str, Any]] = []
    commands = _parse_float_list(args.brake_commands)
    for speed_min, speed_max, target_speed in target_bins:
        entry_target = max(
            float(target_speed),
            float(args.brake_entry_speed_mps_min),
        )
        if not _within_speed_bin(
            entry_target,
            speed_min=speed_min,
            speed_max=speed_max,
            tolerance=0.0,
        ):
            unavailable_speed_bins.append(
                {
                    "speed_min_mps": float(speed_min),
                    "speed_max_mps": float(speed_max),
                    "target_entry_speed_mps": float(target_speed),
                    "effective_entry_speed_mps": float(entry_target),
                    "reason": "entry_speed_floor_outside_bin",
                }
            )
            continue
        for cmd in commands:
            probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
            prep = probe.accelerate_to_speed(
                target_speed_mps=entry_target,
                throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
                timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
            )
            pre_brake_state = probe.state()
            # Brake identification should start from brake onset rather than after a
            # separate settle phase; otherwise strong brake commands get sampled only
            # after the vehicle has already bled off most of its speed.
            samples = probe.hold_and_sample(
                throttle=0.0,
                brake=cmd,
                steer=0.0,
                settle_sec=0.0,
                sample_sec=args.brake_sample_sec,
            )
            if not samples:
                continue
            for item in samples:
                item["target_decel_mps2"] = max(0.0, -_safe_float(item.get("forward_accel_mps2"), 0.0))
            delay = _response_delay_sec(
                [{**item, "neg_forward_accel_mps2": -_safe_float(item.get("forward_accel_mps2"), 0.0)} for item in samples],
                "neg_forward_accel_mps2",
            )
            median_speed = _median((item.get("speed_mps", 0.0) for item in samples), default=0.0)
            actual_entry_speed = float(prep.get("final_speed_mps", 0.0))
            entry_reached = bool(
                _within_speed_bin(
                    actual_entry_speed,
                    speed_min=speed_min,
                    speed_max=speed_max,
                    tolerance=0.0,
                )
            )
            if not entry_reached:
                raw_series.extend(
                    {
                        **item,
                        "target_speed_bin": [speed_min, speed_max],
                        "target_entry_speed_mps": float(entry_target),
                        "actual_entry_speed_mps": actual_entry_speed,
                        "dropped_reason": "entry_speed_outside_target_bin",
                    }
                    for item in samples
                )
                continue
            rows.append(
                {
                    "speed_min_mps": float(speed_min),
                    "speed_max_mps": float(speed_max),
                    "brake_cmd": float(cmd),
                    "target_decel_mps2": _effective_brake_decel(
                        samples,
                        start_sec=float(args.brake_response_start_sec),
                        window_sec=float(args.brake_effective_window_sec),
                    ),
                    "sample_count": len(samples),
                    "response_delay_sec": delay,
                    "observed_speed_mps": _effective_brake_speed(
                        samples,
                        window_sec=float(args.brake_effective_window_sec),
                    ),
                    "sample_median_speed_mps": float(median_speed),
                    "target_entry_speed_mps": float(target_speed),
                    "effective_entry_speed_mps": float(entry_target),
                    "actual_entry_speed_mps": actual_entry_speed,
                    "entry_reached": entry_reached,
                    "prep": prep,
                    "pre_brake_state": pre_brake_state,
                }
            )
            raw_series.extend(
                {
                    **item,
                    "target_speed_bin": [speed_min, speed_max],
                    "pre_brake_gear": pre_brake_state.get("gear"),
                    "pre_brake_speed_mps": pre_brake_state.get("speed_mps"),
                    "pre_brake_forward_accel_mps2": pre_brake_state.get(
                        "forward_accel_mps2"
                    ),
                }
                for item in samples
            )
    aggregated = _aggregate_speed_bins(
        rows,
        command_key="brake_cmd",
        value_key="target_decel_mps2",
        target_key="target_decel_mps2",
    )
    _annotate_brake_coast_baseline(aggregated)
    _annotate_axis_reliability(
        aggregated,
        axis="brake",
        brake_min_incremental_signal_mps2=float(
            args.brake_min_incremental_signal_mps2
        ),
        brake_incremental_monotonic_tolerance_mps2=float(
            args.brake_incremental_monotonic_tolerance_mps2
        ),
    )
    all_measurements = [item for bucket in aggregated for item in bucket.get("measurements", [])]
    deadzone_candidates = [
        _safe_float(item.get("brake_cmd"), 0.0)
        for item in all_measurements
        if _safe_float(item.get("incremental_brake_decel_mps2"), 0.0)
        >= float(args.brake_deadzone_threshold_mps2)
    ]
    low_speed_hold_candidates = [
        _safe_float(item.get("brake_cmd"), 0.0)
        for bucket in aggregated
        if _safe_float(bucket.get("speed_max_mps"), 0.0) <= float(args.low_speed_bin_upper_mps)
        for item in bucket.get("measurements", [])
        if _safe_float(item.get("target_decel_mps2"), 0.0) >= float(args.low_speed_hold_threshold_mps2)
    ]
    max_effective_cmd = 0.0
    if all_measurements:
        max_decel = max(
            _safe_float(item.get("incremental_brake_decel_mps2"), 0.0)
            for item in all_measurements
        )
        threshold = 0.95 * max_decel
        for item in sorted(all_measurements, key=lambda row: _safe_float(row.get("brake_cmd"), 0.0)):
            if _safe_float(item.get("incremental_brake_decel_mps2"), 0.0) >= threshold:
                max_effective_cmd = _safe_float(item.get("brake_cmd"), 0.0)
                break
    return {
        "probe": {
            "settle_sec": float(args.brake_settle_sec),
            "sample_sec": float(args.brake_sample_sec),
            "response_start_sec": float(args.brake_response_start_sec),
            "effective_window_sec": float(args.brake_effective_window_sec),
            "response_metric": "forward_speed_delta_over_fixed_window",
            "incremental_response_metric": "total_decel_minus_zero_brake_coast_baseline",
            "entry_speed_mps_min": float(args.brake_entry_speed_mps_min),
            "prep_throttle": float(args.brake_prep_throttle),
            "speed_bins": [{"min": float(lo), "max": float(hi)} for lo, hi in speed_bins],
        },
        "speed_bins": aggregated,
        "unavailable_speed_bins": unavailable_speed_bins,
        "summary": {
            "response_basis": "incremental_brake_decel_above_zero_brake_coast",
            "declared_speed_bin_count": len(speed_bins),
            "materialized_speed_bin_count": len(aggregated),
            "unavailable_speed_bin_count": len(unavailable_speed_bins),
            "full_declared_speed_range_covered": not unavailable_speed_bins,
            "deadzone_cmd": min(deadzone_candidates) if deadzone_candidates else None,
            "low_speed_hold_cmd": min(low_speed_hold_candidates) if low_speed_hold_candidates else None,
            "max_effective_brake_cmd": float(max_effective_cmd),
        },
        "raw_series": raw_series[: args.max_raw_series_per_axis],
    }


def _resolve_input_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def _load_base_calibration(raw: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    if not str(raw or "").strip():
        return {}, None
    path = _resolve_input_path(raw)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"base calibration must be a JSON object: {path}")
    metadata = {
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "calibration_id": str(payload.get("calibration_id") or ""),
    }
    return payload, metadata


def build_calibration_payload(
    probe: CarlaProbe,
    args: argparse.Namespace,
    *,
    axes: Sequence[str],
) -> Dict[str, Any]:
    payload, base_metadata = _load_base_calibration(args.base_calibration_file)
    payload = dict(payload)
    base_provenance = payload.pop("provenance", None)
    payload["schema_version"] = 1
    payload["generated_at_unix_sec"] = time.time()
    payload["calibration_id"] = str(args.calibration_id or "")
    payload["claim_boundary"] = {
        "evidence_owner": "diagnostic_carla_direct",
        "apollo_in_loop": False,
        "phase1_promotion_allowed": False,
        "mapping_promotion_allowed": False,
    }
    payload["recommendation_policy"] = {
        "automatic_promotion": False,
        "can_modify_mainline_config": False,
    }
    payload["generator"] = {
        "script": "tools/calibrate_carla_actuators.py",
        "carla_host": args.carla_host,
        "carla_port": int(args.carla_port),
        "ego_role_name": args.ego_role_name,
        "axes": list(axes),
        "synchronous_mode": bool(probe.sync_mode),
        "step_sec": float(probe.step_sec),
        "brake_low_speed_probes_only": bool(args.brake_low_speed_probes_only),
    }
    present_axes = [
        axis for axis in ("steering", "throttle", "brake") if axis in payload or axis in axes
    ]
    payload["scope"] = {
        "simulator": "CARLA",
        "actuator_axes": present_axes,
        "input_semantics": {
            "steering": "target_front_wheel_angle_deg",
            "throttle": "target_accel_mps2",
            "brake": "target_decel_mps2",
        },
        "output_semantics": {
            "steering": "carla_steer_cmd",
            "throttle": "throttle_cmd",
            "brake": "brake_cmd",
        },
    }
    payload["provenance"] = {
        "evidence_owner": "diagnostic_carla_direct",
        "base_calibration": base_metadata,
        "base_provenance": base_provenance,
    }
    payload["vehicle"] = probe.vehicle_characteristics()
    if "steering" in axes:
        payload["steering"] = calibrate_steering(probe, args)
    if "throttle" in axes:
        payload["throttle"] = calibrate_throttle(probe, args)
        if not args.skip_low_speed_probes:
            payload["throttle"]["low_speed"] = calibrate_low_speed_throttle(
                probe,
                args,
            )
    if "brake" in axes:
        if args.brake_low_speed_probes_only:
            base_brake = payload.get("brake")
            if not isinstance(base_brake, dict):
                raise ValueError(
                    "--brake-low-speed-probes-only requires a base calibration with brake data"
                )
            payload["brake"] = dict(base_brake)
            payload["brake"]["low_speed"] = calibrate_low_speed_brake(probe, args)
        else:
            payload["brake"] = calibrate_brake(probe, args)
            if not args.skip_low_speed_probes:
                payload["brake"]["low_speed"] = calibrate_low_speed_brake(probe, args)
    return payload


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Calibrate CARLA steering/throttle/brake into lookup tables")
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--ego-role-name", default="hero")
    ap.add_argument("--actor-id", type=int, default=None, help="bind a specific CARLA vehicle actor id")
    ap.add_argument("--timeout-sec", type=float, default=5.0)
    ap.add_argument("--ego-discovery-timeout-sec", type=float, default=20.0)
    ap.add_argument("--ego-discovery-poll-sec", type=float, default=1.0)
    ap.add_argument("--output", default="artifacts/carla_actuator_calibration.json")
    ap.add_argument("--list-vehicles", action="store_true", help="print current CARLA vehicles and exit")
    ap.add_argument(
        "--axes",
        default="steering,throttle,brake",
        help="comma-separated calibration axes; supported: steering,throttle,brake",
    )
    ap.add_argument(
        "--base-calibration-file",
        default="",
        help="optional JSON whose unrequested axes are preserved in the diagnostic output",
    )
    ap.add_argument("--calibration-id", default="")
    ap.add_argument(
        "--spawn-vehicle",
        action="store_true",
        help="spawn and later destroy an isolated diagnostic vehicle; refuses an occupied world",
    )
    ap.add_argument("--vehicle-blueprint", default="vehicle.lincoln.mkz_2020")
    ap.add_argument("--fixed-delta-seconds", type=float, default=0.05)
    ap.add_argument("--spawn-offset-m", type=float, default=5.0)
    ap.add_argument(
        "--skip-low-speed-probes",
        action="store_true",
        help="skip the separate launch/crawl/hold/rolling probe sections",
    )
    ap.add_argument(
        "--brake-low-speed-probes-only",
        action="store_true",
        help="preserve base brake speed bins and replace only brake.low_speed",
    )
    ap.add_argument("--speed-bins", default="0,2,5,10,15,25")
    ap.add_argument("--max-raw-series-per-axis", type=int, default=500)
    ap.add_argument("--steering-commands", default="-1.0,-0.8,-0.6,-0.4,-0.2,0.0,0.2,0.4,0.6,0.8,1.0")
    ap.add_argument("--steering-probe-throttle", type=float, default=0.25)
    ap.add_argument("--steering-settle-sec", type=float, default=1.0)
    ap.add_argument("--steering-sample-sec", type=float, default=1.5)
    ap.add_argument("--throttle-commands", default="0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7")
    ap.add_argument("--throttle-settle-sec", type=float, default=0.8)
    ap.add_argument("--throttle-sample-sec", type=float, default=2.5)
    ap.add_argument("--throttle-effective-window-sec", type=float, default=0.8)
    ap.add_argument("--low-speed-throttle-commands", default="0.0,0.05,0.08,0.1,0.12,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.6,0.7")
    ap.add_argument("--low-speed-launch-sample-sec", type=float, default=1.5)
    ap.add_argument("--low-speed-launch-eval-sec", type=float, default=0.8)
    ap.add_argument("--low-speed-crawl-entry-speeds", default="2.0,4.0,6.0")
    ap.add_argument("--low-speed-crawl-sample-sec", type=float, default=1.2)
    ap.add_argument("--low-speed-crawl-eval-sec", type=float, default=0.8)
    ap.add_argument("--longitudinal-prep-throttle", type=float, default=0.75)
    ap.add_argument("--longitudinal-prep-timeout-sec", type=float, default=18.0)
    ap.add_argument("--longitudinal-entry-speed-tolerance-mps", type=float, default=0.75)
    ap.add_argument("--longitudinal-reset-settle-sec", type=float, default=0.8)
    ap.add_argument("--brake-commands", default="0.0,0.05,0.1,0.2,0.3,0.4,0.6,0.8,1.0")
    ap.add_argument("--brake-entry-speed-mps-min", type=float, default=2.0)
    ap.add_argument("--brake-prep-throttle", type=float, default=0.45)
    ap.add_argument("--brake-prep-timeout-sec", type=float, default=8.0)
    ap.add_argument("--brake-settle-sec", type=float, default=0.0)
    ap.add_argument("--brake-sample-sec", type=float, default=3.5)
    ap.add_argument("--brake-response-start-sec", type=float, default=0.2)
    ap.add_argument("--brake-effective-window-sec", type=float, default=0.4)
    ap.add_argument("--brake-deadzone-threshold-mps2", type=float, default=0.4)
    ap.add_argument("--brake-min-incremental-signal-mps2", type=float, default=0.2)
    ap.add_argument(
        "--brake-incremental-monotonic-tolerance-mps2",
        type=float,
        default=0.1,
    )
    ap.add_argument("--low-speed-bin-upper-mps", type=float, default=1.5)
    ap.add_argument("--low-speed-hold-threshold-mps2", type=float, default=0.8)
    ap.add_argument("--low-speed-brake-commands", default="0.0,0.01,0.02,0.04,0.05,0.08,0.1,0.12,0.15,0.2,0.25,0.3,0.4")
    ap.add_argument("--low-speed-hold-entry-speed-mps", type=float, default=1.0)
    ap.add_argument("--low-speed-hold-sample-sec", type=float, default=1.2)
    ap.add_argument("--low-speed-hold-eval-sec", type=float, default=0.8)
    ap.add_argument("--low-speed-hold-stop-speed-mps", type=float, default=0.1)
    ap.add_argument("--low-speed-hold-residual-speed-threshold-mps", type=float, default=0.05)
    ap.add_argument("--low-speed-stop-activation-max-speed-mps", type=float, default=1.0)
    ap.add_argument("--low-speed-stop-request-decel-threshold-mps2", type=float, default=0.8)
    ap.add_argument("--low-speed-rolling-brake-entry-speeds", default="2.0,4.0,6.0")
    ap.add_argument("--low-speed-rolling-brake-sample-sec", type=float, default=1.2)
    ap.add_argument("--low-speed-rolling-brake-eval-sec", type=float, default=0.3)
    ap.add_argument("--low-speed-rolling-speed-coverage-half-width-mps", type=float, default=0.25)
    return ap.parse_args(argv)


def main() -> int:
    args = parse_args()
    axes = _parse_axes(args.axes)
    if "brake" in axes and not (
        0.0 <= float(args.brake_response_start_sec)
        < float(args.brake_effective_window_sec)
        <= float(args.brake_sample_sec)
    ):
        raise ValueError(
            "brake response window must satisfy "
            "0 <= start < effective_window <= sample_sec"
        )
    if args.spawn_vehicle and args.actor_id is not None:
        raise ValueError("--spawn-vehicle cannot be combined with --actor-id")
    if args.spawn_vehicle and float(args.fixed_delta_seconds) <= 0.0:
        raise ValueError("--fixed-delta-seconds must be positive")
    if args.brake_low_speed_probes_only:
        if axes != ("brake",):
            raise ValueError("--brake-low-speed-probes-only requires --axes brake")
        if args.skip_low_speed_probes:
            raise ValueError(
                "--brake-low-speed-probes-only cannot be combined with --skip-low-speed-probes"
            )
        if not str(args.base_calibration_file or "").strip():
            raise ValueError(
                "--brake-low-speed-probes-only requires --base-calibration-file"
            )

    actor: Optional[carla.Vehicle] = None
    spawned_world: Any = None
    original_settings: Any = None
    try:
        if args.spawn_vehicle:
            client = carla.Client(args.carla_host, int(args.carla_port))
            client.set_timeout(float(args.timeout_sec))
            spawned_world = client.get_world()
            existing = list(spawned_world.get_actors().filter("vehicle.*"))
            if existing:
                raise RuntimeError(
                    "refusing to spawn diagnostic calibration vehicle while CARLA vehicles exist: "
                    + ",".join(str(item.id) for item in existing)
                )
            original_settings = spawned_world.get_settings()
            settings = spawned_world.get_settings()
            settings.synchronous_mode = True
            settings.fixed_delta_seconds = float(args.fixed_delta_seconds)
            spawned_world.apply_settings(settings)

            blueprints = spawned_world.get_blueprint_library().filter(args.vehicle_blueprint)
            if not blueprints:
                raise RuntimeError(f"vehicle blueprint unavailable: {args.vehicle_blueprint}")
            blueprint = blueprints[0]
            role_name = "diagnostic_actuator_calibration"
            if blueprint.has_attribute("role_name"):
                blueprint.set_attribute("role_name", role_name)
            spawn_points = spawned_world.get_map().get_spawn_points()
            if not spawn_points:
                raise RuntimeError("current CARLA map has no spawn points")
            spawn_transform = spawn_points[0]
            spawn_forward = spawn_transform.get_forward_vector()
            spawn_transform.location.x += float(args.spawn_offset_m) * float(spawn_forward.x)
            spawn_transform.location.y += float(args.spawn_offset_m) * float(spawn_forward.y)
            spawn_transform.location.z += 0.1
            actor = spawned_world.try_spawn_actor(blueprint, spawn_transform)
            if actor is None:
                raise RuntimeError("failed to spawn diagnostic actuator-calibration vehicle")
            spawned_world.tick()
            probe = CarlaProbe(
                host=args.carla_host,
                port=args.carla_port,
                ego_role_name=role_name,
                timeout_sec=args.timeout_sec,
                actor_id=actor.id,
                ego_discovery_timeout_sec=2.0,
                ego_discovery_poll_sec=0.05,
            )
            probe.world = spawned_world
            probe.ego = actor
            probe.sync_mode = True
            probe.step_sec = float(args.fixed_delta_seconds)
            probe.reference_transform = actor.get_transform()
        else:
            probe = CarlaProbe(
                host=args.carla_host,
                port=args.carla_port,
                ego_role_name=args.ego_role_name,
                timeout_sec=args.timeout_sec,
                actor_id=args.actor_id,
                ego_discovery_timeout_sec=args.ego_discovery_timeout_sec,
                ego_discovery_poll_sec=args.ego_discovery_poll_sec,
            )
        if args.list_vehicles:
            print(json.dumps({"vehicles": probe.list_vehicles()}, indent=2))
            return 0
        output = _resolve_input_path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        payload = build_calibration_payload(probe, args, axes=axes)
        output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"[calibrate] wrote {output}")
        return 0
    finally:
        if actor is not None and spawned_world is not None:
            try:
                actor.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0))
                spawned_world.tick()
            except Exception:
                pass
            try:
                actor.destroy()
            except Exception:
                pass
        if spawned_world is not None and original_settings is not None:
            try:
                spawned_world.apply_settings(original_settings)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
