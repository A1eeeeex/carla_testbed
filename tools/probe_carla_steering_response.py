#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

import carla

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.calibration.steering_response import (
    analyze_speed_dependent_steering_authority,
    analyze_steering_step_response,
    summarize_steering_step_trials,
)
from tools.apollo10_cyber_bridge.actuator_mapping import load_actuator_calibration
from tools.calibrate_carla_actuators import CarlaProbe


def _parse_float_list(raw: str) -> list[float]:
    return [float(item.strip()) for item in str(raw).split(",") if item.strip()]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _world_sample(probe: CarlaProbe, *, elapsed_sec: float, phase: str, trial_id: str) -> dict[str, Any]:
    state = probe.state()
    snapshot = probe.world.get_snapshot()
    return {
        "trial_id": trial_id,
        "phase": phase,
        "elapsed_sec": float(elapsed_sec),
        "frame_id": int(snapshot.frame),
        "sim_time_s": float(snapshot.timestamp.elapsed_seconds),
        **state,
    }


def _advance_and_sample(
    probe: CarlaProbe,
    *,
    throttle: float,
    steer: float,
    elapsed_sec: float,
    phase: str,
    trial_id: str,
) -> dict[str, Any]:
    probe.apply(throttle=throttle, brake=0.0, steer=steer)
    probe.world.tick()
    return _world_sample(probe, elapsed_sec=elapsed_sec, phase=phase, trial_id=trial_id)


def _run_trial(
    probe: CarlaProbe,
    *,
    trial_id: str,
    target_angle_deg: float,
    carla_steer_cmd: float,
    target_speed_mps: float,
    acceleration_throttle: float,
    hold_throttle: float,
    pre_step_sec: float,
    step_sec: float,
    speed_tolerance_mps: float,
) -> dict[str, Any]:
    probe.reset_to_reference_pose(settle_sec=0.5)
    prep = probe.accelerate_to_speed(
        target_speed_mps=target_speed_mps,
        throttle=acceleration_throttle,
        timeout_sec=14.0,
    )
    rows: list[dict[str, Any]] = []
    dt = float(probe.step_sec)
    baseline_steps = max(4, int(math.ceil(pre_step_sec / dt)))
    for index in range(baseline_steps):
        elapsed = -(baseline_steps - index) * dt
        rows.append(
            _advance_and_sample(
                probe,
                throttle=hold_throttle,
                steer=0.0,
                elapsed_sec=elapsed,
                phase="baseline",
                trial_id=trial_id,
            )
        )

    probe.apply(throttle=hold_throttle, brake=0.0, steer=carla_steer_cmd)
    rows.append(_world_sample(probe, elapsed_sec=0.0, phase="step", trial_id=trial_id))
    response_steps = max(4, int(math.ceil(step_sec / dt)))
    for index in range(1, response_steps + 1):
        rows.append(
            _advance_and_sample(
                probe,
                throttle=hold_throttle,
                steer=carla_steer_cmd,
                elapsed_sec=index * dt,
                phase="step",
                trial_id=trial_id,
            )
        )
    response = analyze_steering_step_response(rows)
    speed_at_step_mps = next(
        (float(row["speed_mps"]) for row in rows if float(row["elapsed_sec"]) == 0.0),
        None,
    )
    speed_error_mps = (
        None if speed_at_step_mps is None else abs(float(speed_at_step_mps) - float(target_speed_mps))
    )
    speed_gate = {
        "status": (
            "pass"
            if bool(prep.get("reached"))
            and speed_error_mps is not None
            and speed_error_mps <= float(speed_tolerance_mps)
            else "fail"
        ),
        "target_speed_mps": float(target_speed_mps),
        "observed_speed_mps": speed_at_step_mps,
        "error_mps": speed_error_mps,
        "tolerance_mps": float(speed_tolerance_mps),
    }
    if speed_gate["status"] != "pass":
        response["status"] = "insufficient_data"
        response["operating_point_status"] = "target_speed_not_reached"
        response["missing_fields"] = sorted(
            set(list(response.get("missing_fields") or []) + ["target_speed_operating_point"])
        )
    return {
        "trial_id": trial_id,
        "target_front_wheel_angle_deg": float(target_angle_deg),
        "carla_steer_cmd": float(carla_steer_cmd),
        "target_speed_mps": float(target_speed_mps),
        "prep": prep,
        "speed_at_step_mps": speed_at_step_mps,
        "speed_gate": speed_gate,
        "response": response,
        "rows": rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a diagnostic-only CARLA steering step-response probe"
    )
    parser.add_argument("--carla-host", default="127.0.0.1")
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--vehicle-blueprint", default="vehicle.lincoln.mkz_2020")
    parser.add_argument(
        "--calibration-file",
        default="configs/calibration/vehicles/vehicle.lincoln.mkz_2020/steering_front_wheel_v1.json",
    )
    parser.add_argument("--target-front-wheel-angles-deg", default="-6,-3,3,6")
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--target-speed-mps", type=float, default=18.9)
    parser.add_argument(
        "--target-speeds-mps",
        default="",
        help="comma-separated operating speeds; overrides --target-speed-mps when set",
    )
    parser.add_argument("--speed-tolerance-mps", type=float, default=1.0)
    parser.add_argument("--acceleration-throttle", type=float, default=1.0)
    parser.add_argument("--hold-throttle", type=float, default=0.25)
    parser.add_argument("--pre-step-sec", type=float, default=0.5)
    parser.add_argument("--step-sec", type=float, default=1.0)
    parser.add_argument("--fixed-delta-seconds", type=float, default=0.05)
    parser.add_argument("--spawn-offset-m", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    calibration_path = Path(args.calibration_file).expanduser()
    if not calibration_path.is_absolute():
        calibration_path = (REPO_ROOT / calibration_path).resolve()
    calibration = load_actuator_calibration(calibration_path)
    if not calibration.loaded:
        raise RuntimeError(f"steering calibration did not load: {calibration_path}")

    client = carla.Client(args.carla_host, int(args.carla_port))
    client.set_timeout(float(args.timeout_sec))
    world = client.get_world()
    existing_vehicles = list(world.get_actors().filter("vehicle.*"))
    if existing_vehicles:
        raise RuntimeError(
            "refusing to run diagnostic probe while CARLA vehicles already exist: "
            + ",".join(str(actor.id) for actor in existing_vehicles)
        )
    original_settings = world.get_settings()
    actor: carla.Vehicle | None = None
    trials: list[dict[str, Any]] = []
    raw_rows: list[dict[str, Any]] = []
    try:
        settings = world.get_settings()
        settings.synchronous_mode = True
        settings.fixed_delta_seconds = float(args.fixed_delta_seconds)
        world.apply_settings(settings)

        blueprints = world.get_blueprint_library().filter(args.vehicle_blueprint)
        if not blueprints:
            raise RuntimeError(f"vehicle blueprint unavailable: {args.vehicle_blueprint}")
        blueprint = blueprints[0]
        if blueprint.has_attribute("role_name"):
            blueprint.set_attribute("role_name", "diagnostic_steering_probe")
        spawn_points = world.get_map().get_spawn_points()
        if not spawn_points:
            raise RuntimeError("current CARLA map has no spawn points")
        spawn_transform = spawn_points[0]
        spawn_forward = spawn_transform.get_forward_vector()
        spawn_transform.location.x += float(args.spawn_offset_m) * float(spawn_forward.x)
        spawn_transform.location.y += float(args.spawn_offset_m) * float(spawn_forward.y)
        spawn_transform.location.z += 0.1
        actor = world.try_spawn_actor(blueprint, spawn_transform)
        if actor is None:
            raise RuntimeError("failed to spawn diagnostic steering probe vehicle")
        world.tick()
        probe = CarlaProbe(
            host=args.carla_host,
            port=args.carla_port,
            ego_role_name="diagnostic_steering_probe",
            timeout_sec=args.timeout_sec,
            actor_id=actor.id,
            ego_discovery_timeout_sec=2.0,
            ego_discovery_poll_sec=0.05,
        )
        probe.world = world
        probe.ego = actor
        probe.sync_mode = True
        probe.step_sec = float(args.fixed_delta_seconds)
        probe.reference_transform = actor.get_transform()

        angles = _parse_float_list(args.target_front_wheel_angles_deg)
        target_speeds = _parse_float_list(args.target_speeds_mps)
        if not target_speeds:
            target_speeds = [float(args.target_speed_mps)]
        if any(speed <= 0.0 for speed in target_speeds):
            raise ValueError("target steering-response speeds must be positive")
        for target_speed in target_speeds:
            for repeat in range(max(1, int(args.repeats))):
                for angle in angles:
                    command = calibration.steering_cmd_for_angle(angle)
                    if command is None:
                        raise RuntimeError(f"calibration has no command for target angle {angle}")
                    trial_id = (
                        f"speed_{target_speed:g}mps_steer_{angle:+g}deg_r{repeat + 1}"
                    )
                    print(f"[probe] {trial_id}: carla_steer_cmd={command:.6f}", flush=True)
                    trial = _run_trial(
                        probe,
                        trial_id=trial_id,
                        target_angle_deg=angle,
                        carla_steer_cmd=command,
                        target_speed_mps=target_speed,
                        acceleration_throttle=float(args.acceleration_throttle),
                        hold_throttle=float(args.hold_throttle),
                        pre_step_sec=float(args.pre_step_sec),
                        step_sec=float(args.step_sec),
                        speed_tolerance_mps=float(args.speed_tolerance_mps),
                    )
                    raw_rows.extend(trial.pop("rows"))
                    trials.append(trial)

        summary = summarize_steering_step_trials(trials)
        operating_points = []
        for target_speed in target_speeds:
            speed_trials = [
                trial
                for trial in trials
                if math.isclose(
                    float(trial.get("target_speed_mps", float("nan"))),
                    target_speed,
                    rel_tol=0.0,
                    abs_tol=1e-6,
                )
            ]
            operating_points.append(
                {
                    "target_speed_mps": target_speed,
                    **summarize_steering_step_trials(speed_trials),
                }
            )
        physics = actor.get_physics_control()
        steering_curve = [
            {
                "speed_axis_value": float(point.x),
                "max_steer_scale": float(point.y),
            }
            for point in list(getattr(physics, "steering_curve", []) or [])
        ]
        vehicle_characteristics = probe.vehicle_characteristics()
        vehicle_characteristics["steering_curve"] = steering_curve
        speed_dependent_authority = analyze_speed_dependent_steering_authority(
            trials,
            steering_curve=steering_curve,
            max_steer_angle_deg=float(
                vehicle_characteristics.get("max_steer_angle_deg") or 0.0
            ),
        )
        summary["operating_points"] = operating_points
        summary["speed_dependent_authority"] = speed_dependent_authority
        payload = {
            "schema_version": "carla_steering_response_probe.v1",
            "generated_at_unix_sec": time.time(),
            "claim_boundary": summary["claim_boundary"],
            "environment": {
                "map": world.get_map().name,
                "fixed_delta_seconds": float(args.fixed_delta_seconds),
                "carla_host": args.carla_host,
                "carla_port": int(args.carla_port),
            },
            "vehicle": vehicle_characteristics,
            "calibration": {
                "path": str(calibration_path),
                "sha256": hashlib.sha256(calibration_path.read_bytes()).hexdigest(),
                **calibration.status(),
            },
            "probe": {
                "target_speed_mps": target_speeds[0] if len(target_speeds) == 1 else None,
                "target_speeds_mps": target_speeds,
                "target_front_wheel_angles_deg": angles,
                "repeats": max(1, int(args.repeats)),
                "pre_step_sec": float(args.pre_step_sec),
                "step_sec": float(args.step_sec),
                "hold_throttle": float(args.hold_throttle),
                "speed_tolerance_mps": float(args.speed_tolerance_mps),
                "spawn_offset_m": float(args.spawn_offset_m),
            },
            "trials": trials,
            "summary": summary,
        }
        report_path = output_dir / "steering_response_probe.json"
        report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        _write_csv(output_dir / "steering_response_timeseries.csv", raw_rows)
        print(f"[probe] wrote {report_path}", flush=True)
        return 0 if summary["status"] == "pass" else 2
    finally:
        if actor is not None:
            try:
                actor.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0))
                world.tick()
            except Exception:
                pass
            try:
                actor.destroy()
            except Exception:
                pass
        try:
            world.apply_settings(original_settings)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
