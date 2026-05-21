#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
        deadline = time.monotonic() + max(0.0, float(duration_sec))
        while time.monotonic() < deadline:
            if self.sync_mode:
                self.world.tick()
            else:
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

    def state(self) -> Dict[str, float]:
        actor = self.vehicle()
        vel = actor.get_velocity()
        accel = actor.get_acceleration()
        ang = actor.get_angular_velocity()
        tr = actor.get_transform()
        ctrl = actor.get_control()
        speed = math.sqrt(float(vel.x) ** 2 + float(vel.y) ** 2 + float(vel.z) ** 2)
        fwd = tr.get_forward_vector()
        forward_accel = (
            float(accel.x) * float(fwd.x)
            + float(accel.y) * float(fwd.y)
            + float(accel.z) * float(fwd.z)
        )
        out = {
            "speed_mps": float(speed),
            "forward_accel_mps2": float(forward_accel),
            "yaw_rate_rps": math.radians(float(getattr(ang, "z", 0.0))),
            "throttle_cmd": float(getattr(ctrl, "throttle", 0.0)),
            "brake_cmd": float(getattr(ctrl, "brake", 0.0)),
            "carla_steer_cmd": float(getattr(ctrl, "steer", 0.0)),
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
    ) -> List[Dict[str, float]]:
        self.settle(throttle=throttle, brake=brake, steer=steer, duration_sec=settle_sec)
        samples: List[Dict[str, float]] = []
        deadline = time.monotonic() + max(0.0, float(sample_sec))
        while time.monotonic() < deadline:
            self.apply(throttle=throttle, brake=brake, steer=steer)
            if self.sync_mode:
                self.world.tick()
            else:
                time.sleep(self.step_sec)
            item = self.state()
            item["elapsed_sec"] = float(sample_sec - max(0.0, deadline - time.monotonic()))
            samples.append(item)
        return samples

    def brake_to_stop(self, *, hold_brake: float = 1.0, timeout_sec: float = 6.0) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        while time.monotonic() < deadline:
            self.apply(throttle=0.0, brake=hold_brake, steer=0.0)
            if self.sync_mode:
                self.world.tick()
            else:
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
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        final_speed = 0.0
        while time.monotonic() < deadline:
            self.apply(throttle=throttle, brake=0.0, steer=0.0)
            if self.sync_mode:
                self.world.tick()
            else:
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


def _effective_brake_decel(samples: Sequence[Dict[str, float]]) -> float:
    active = []
    for item in samples:
        speed = _safe_float(item.get("speed_mps"), 0.0)
        elapsed = _safe_float(item.get("elapsed_sec"), 0.0)
        decel = max(0.0, -_safe_float(item.get("forward_accel_mps2"), 0.0))
        if speed >= 0.5 or elapsed <= 1.0:
            active.append(decel)
    if not active:
        active = [max(0.0, -_safe_float(item.get("forward_accel_mps2"), 0.0)) for item in samples]
    return _percentile(active, 0.85, default=0.0)


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


def _annotate_axis_reliability(speed_bins: List[Dict[str, Any]], *, axis: str) -> None:
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
            observed = [_safe_float(item.get("observed_speed_mps"), 0.0) for item in bucket.get("raw_measurements", [])]
            max_value = max(values) if values else 0.0
            reliable = bool(
                len(measurements) >= 3
                and max_value >= 2.0
                and max(observed, default=0.0) >= 1.0
                and _is_monotonic_non_decreasing(values, tolerance=0.6)
            )
            reasons = []
            if len(measurements) < 3:
                reasons.append("insufficient_measurements")
            if max_value < 2.0:
                reasons.append("weak_decel")
            if max(observed, default=0.0) < 1.0:
                reasons.append("low_observed_speed")
            if values and not _is_monotonic_non_decreasing(values, tolerance=0.6):
                reasons.append("non_monotonic")
        bucket["quality"] = {
            "reliable": reliable,
            "reasons": reasons,
            "max_value": float(max(values) if values else 0.0),
            "max_observed_speed_mps": float(max(observed, default=0.0)),
        }


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
            row = {
                "entry_speed_mps": float(entry_speed),
                "throttle_cmd": float(cmd),
                "delta_speed_mps": float(eval_speed - start_speed),
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

    hold_rows: List[Dict[str, Any]] = []
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
        hold_rows.append(
            {
                "brake_cmd": float(cmd),
                "residual_speed_mps": float(residual_speed),
                "stop_time_sec": _time_to_stop(samples, stop_speed_mps=args.low_speed_hold_stop_speed_mps),
                "holds_vehicle": bool(residual_speed <= float(args.low_speed_hold_residual_speed_threshold_mps)),
                "entry_reached": bool(prep.get("reached")),
                "sample_count": len(samples),
                "prep": prep,
            }
        )
        raw_series.extend({**item, "low_speed_mode": "hold"} for item in samples)
    hold_quality_rows = [
        {"brake_cmd": row["brake_cmd"], "hold_strength": max(0.0, 1.0 - _safe_float(row.get("residual_speed_mps"), 0.0))}
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
                "target_decel_mps2": float(_effective_brake_decel(samples)),
                "stop_time_sec": _time_to_stop(samples, stop_speed_mps=args.low_speed_hold_stop_speed_mps),
                "entry_reached": bool(prep.get("reached")),
                "sample_count": len(samples),
                "prep": prep,
            }
            section_rows.append(row)
            raw_series.extend({**item, "low_speed_mode": "rolling_brake", "entry_speed_mps": float(entry_speed)} for item in samples)
        quality = _quality_for_measurements(
            section_rows,
            command_key="brake_cmd",
            value_key="speed_drop_mps",
            reliable_min_samples=4,
            monotonic_tolerance=0.2,
            min_max_value=0.5,
        )
        inverse = build_inverse_table(
            section_rows,
            measured_key="speed_drop_mps",
            command_key="brake_cmd",
            target_key="target_speed_drop_mps",
            absolute=False,
            positive_only=True,
        )
        rolling_sections.append(
            {
                "entry_speed_mps": float(entry_speed),
                "measurements": section_rows,
                "inverse": {"target_speed_drop_mps_to_brake_cmd": inverse},
                "quality": quality,
            }
        )
    return {
        "hold": {
            "probe": {
                "entry_speed_mps": float(args.low_speed_hold_entry_speed_mps),
                "sample_sec": float(args.low_speed_hold_sample_sec),
                "eval_sec": float(args.low_speed_hold_eval_sec),
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
        "mid_speed_boost": boost_sections,
        "raw_series": raw_series[: args.max_raw_series_per_axis],
    }


def calibrate_brake(probe: CarlaProbe, args: argparse.Namespace) -> Dict[str, Any]:
    speed_bins = _parse_speed_edges(args.speed_bins)
    target_bins = _speed_bin_targets(speed_bins)
    rows: List[Dict[str, Any]] = []
    raw_series: List[Dict[str, Any]] = []
    commands = _parse_float_list(args.brake_commands)
    for speed_min, speed_max, target_speed in target_bins:
        for cmd in commands:
            probe.reset_to_reference_pose(settle_sec=args.longitudinal_reset_settle_sec)
            prep = probe.accelerate_to_speed(
                target_speed_mps=max(float(target_speed), float(args.brake_entry_speed_mps_min)),
                throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
                timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
            )
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
            entry_target = max(float(target_speed), float(args.brake_entry_speed_mps_min))
            entry_reached = bool(
                abs(float(prep.get("final_speed_mps", 0.0)) - entry_target)
                <= float(args.longitudinal_entry_speed_tolerance_mps)
                or float(prep.get("final_speed_mps", 0.0)) >= float(speed_min)
            )
            if not entry_reached:
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
                    "brake_cmd": float(cmd),
                    "target_decel_mps2": _effective_brake_decel(samples),
                    "sample_count": len(samples),
                    "response_delay_sec": delay,
                    "observed_speed_mps": _effective_brake_speed(
                        samples,
                        window_sec=float(args.brake_effective_window_sec),
                    ),
                    "sample_median_speed_mps": float(median_speed),
                    "target_entry_speed_mps": float(target_speed),
                    "entry_reached": entry_reached,
                    "prep": prep,
                }
            )
            raw_series.extend({**item, "target_speed_bin": [speed_min, speed_max]} for item in samples)
    aggregated = _aggregate_speed_bins(
        rows,
        command_key="brake_cmd",
        value_key="target_decel_mps2",
        target_key="target_decel_mps2",
    )
    _annotate_axis_reliability(aggregated, axis="brake")
    all_measurements = [item for bucket in aggregated for item in bucket.get("measurements", [])]
    deadzone_candidates = [
        _safe_float(item.get("brake_cmd"), 0.0)
        for item in all_measurements
        if _safe_float(item.get("target_decel_mps2"), 0.0) >= float(args.brake_deadzone_threshold_mps2)
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
        max_decel = max(_safe_float(item.get("target_decel_mps2"), 0.0) for item in all_measurements)
        threshold = 0.95 * max_decel
        for item in sorted(all_measurements, key=lambda row: _safe_float(row.get("brake_cmd"), 0.0)):
            if _safe_float(item.get("target_decel_mps2"), 0.0) >= threshold:
                max_effective_cmd = _safe_float(item.get("brake_cmd"), 0.0)
                break
    return {
        "probe": {
            "settle_sec": float(args.brake_settle_sec),
            "sample_sec": float(args.brake_sample_sec),
            "entry_speed_mps_min": float(args.brake_entry_speed_mps_min),
            "prep_throttle": float(args.brake_prep_throttle),
            "speed_bins": [{"min": float(lo), "max": float(hi)} for lo, hi in speed_bins],
        },
        "speed_bins": aggregated,
        "summary": {
            "deadzone_cmd": min(deadzone_candidates) if deadzone_candidates else None,
            "low_speed_hold_cmd": min(low_speed_hold_candidates) if low_speed_hold_candidates else None,
            "max_effective_brake_cmd": float(max_effective_cmd),
        },
        "raw_series": raw_series[: args.max_raw_series_per_axis],
    }


def parse_args() -> argparse.Namespace:
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
    ap.add_argument("--speed-bins", default="0,2,5,10,15,25")
    ap.add_argument("--max-raw-series-per-axis", type=int, default=500)
    ap.add_argument("--steering-commands", default="-1.0,-0.8,-0.6,-0.4,-0.2,0.0,0.2,0.4,0.6,0.8,1.0")
    ap.add_argument("--steering-probe-throttle", type=float, default=0.25)
    ap.add_argument("--steering-settle-sec", type=float, default=1.0)
    ap.add_argument("--steering-sample-sec", type=float, default=1.5)
    ap.add_argument("--throttle-commands", default="0.1,0.2,0.3,0.4,0.5,0.6,0.7")
    ap.add_argument("--throttle-settle-sec", type=float, default=0.8)
    ap.add_argument("--throttle-sample-sec", type=float, default=2.5)
    ap.add_argument("--throttle-effective-window-sec", type=float, default=0.8)
    ap.add_argument("--low-speed-throttle-commands", default="0.05,0.08,0.1,0.12,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.6,0.7")
    ap.add_argument("--low-speed-launch-sample-sec", type=float, default=1.5)
    ap.add_argument("--low-speed-launch-eval-sec", type=float, default=0.8)
    ap.add_argument("--low-speed-crawl-entry-speeds", default="2.0,4.0,6.0")
    ap.add_argument("--low-speed-crawl-sample-sec", type=float, default=1.2)
    ap.add_argument("--low-speed-crawl-eval-sec", type=float, default=0.8)
    ap.add_argument("--longitudinal-prep-throttle", type=float, default=0.75)
    ap.add_argument("--longitudinal-prep-timeout-sec", type=float, default=18.0)
    ap.add_argument("--longitudinal-entry-speed-tolerance-mps", type=float, default=0.75)
    ap.add_argument("--longitudinal-reset-settle-sec", type=float, default=0.8)
    ap.add_argument("--brake-commands", default="0.05,0.1,0.2,0.3,0.4,0.6,0.8,1.0")
    ap.add_argument("--brake-entry-speed-mps-min", type=float, default=2.0)
    ap.add_argument("--brake-prep-throttle", type=float, default=0.45)
    ap.add_argument("--brake-prep-timeout-sec", type=float, default=8.0)
    ap.add_argument("--brake-settle-sec", type=float, default=0.0)
    ap.add_argument("--brake-sample-sec", type=float, default=3.5)
    ap.add_argument("--brake-effective-window-sec", type=float, default=0.8)
    ap.add_argument("--brake-deadzone-threshold-mps2", type=float, default=0.4)
    ap.add_argument("--low-speed-bin-upper-mps", type=float, default=1.5)
    ap.add_argument("--low-speed-hold-threshold-mps2", type=float, default=0.8)
    ap.add_argument("--low-speed-brake-commands", default="0.02,0.04,0.05,0.08,0.1,0.12,0.15,0.2,0.25,0.3,0.4")
    ap.add_argument("--low-speed-hold-entry-speed-mps", type=float, default=1.0)
    ap.add_argument("--low-speed-hold-sample-sec", type=float, default=1.2)
    ap.add_argument("--low-speed-hold-eval-sec", type=float, default=0.8)
    ap.add_argument("--low-speed-hold-stop-speed-mps", type=float, default=0.1)
    ap.add_argument("--low-speed-hold-residual-speed-threshold-mps", type=float, default=0.05)
    ap.add_argument("--low-speed-rolling-brake-entry-speeds", default="2.0,4.0,6.0")
    ap.add_argument("--low-speed-rolling-brake-sample-sec", type=float, default=1.2)
    ap.add_argument("--low-speed-rolling-brake-eval-sec", type=float, default=0.3)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
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
    output = Path(args.output).expanduser()
    if not output.is_absolute():
        output = (Path.cwd() / output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "generated_at_unix_sec": time.time(),
        "generator": {
            "script": "tools/calibrate_carla_actuators.py",
            "carla_host": args.carla_host,
            "carla_port": int(args.carla_port),
            "ego_role_name": args.ego_role_name,
        },
        "vehicle": probe.vehicle_characteristics(),
        "steering": calibrate_steering(probe, args),
        "throttle": {
            **calibrate_throttle(probe, args),
            "low_speed": calibrate_low_speed_throttle(probe, args),
        },
        "brake": {
            **calibrate_brake(probe, args),
            "low_speed": calibrate_low_speed_brake(probe, args),
        },
    }
    output.write_text(json.dumps(payload, indent=2))
    print(f"[calibrate] wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
