#!/usr/bin/env python3
# OPERATIONAL HELPER: local Baguang follow-stop runner with CARLA runtime access.
# Do not add new platform logic here; move reusable code into carla_testbed.experiments.
# Migration target: carla_testbed.experiments follow-stop scenario runner.
"""Spawn a 300 m follow-stop scene on straight_road_for_baguang.

This is a thin CARLA API tool for validating the imported RoadRunner map before
connecting Apollo. It does not start CARLA or Apollo. Start packaged CARLA 0.9.16
first, then run this script with the carla16 Python environment.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time
from typing import Any


def _import_carla():
    try:
        import carla  # type: ignore
    except ImportError as exc:  # pragma: no cover - exercised in local runtime only.
        raise SystemExit(
            "CARLA Python API is not importable. Run with the carla16 environment, "
            "for example: /home/ubuntu/miniconda3/envs/carla16/bin/python3"
        ) from exc
    return carla


def _timestamp_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _pick_vehicle_blueprint(bp_lib: Any, blueprint_id: str | None) -> Any:
    patterns = [blueprint_id] if blueprint_id else []
    patterns.extend(["vehicle.lincoln.mkz_2020", "vehicle.lincoln.mkz_2017", "vehicle.tesla.model3"])
    for pattern in patterns:
        if not pattern:
            continue
        candidates = bp_lib.filter(pattern)
        if candidates:
            return candidates[0]
    raise RuntimeError("No suitable ego/front vehicle blueprint found")


def _set_role(bp: Any, role_name: str) -> None:
    for attr in ("role_name", "ros_name"):
        try:
            bp.set_attribute(attr, role_name)
        except Exception:
            pass


def _forward_metrics(ego_tf: Any, front_tf: Any) -> dict[str, float]:
    yaw = math.radians(float(ego_tf.rotation.yaw))
    hx = math.cos(yaw)
    hy = math.sin(yaw)
    dx = float(front_tf.location.x - ego_tf.location.x)
    dy = float(front_tf.location.y - ego_tf.location.y)
    longitudinal_m = (dx * hx) + (dy * hy)
    lateral_m = (-dx * hy) + (dy * hx)
    yaw_diff = (float(front_tf.rotation.yaw - ego_tf.rotation.yaw) + 180.0) % 360.0 - 180.0
    return {
        "longitudinal_m": longitudinal_m,
        "lateral_m": lateral_m,
        "euclidean_m": math.hypot(dx, dy),
        "yaw_diff_deg": yaw_diff,
    }


def _destroy_existing_dynamic_actors(world: Any) -> int:
    removed = 0
    for actor in list(world.get_actors()):
        type_id = getattr(actor, "type_id", "") or ""
        role = ""
        try:
            role = actor.attributes.get("role_name", "")
        except Exception:
            role = ""
        if type_id.startswith("vehicle.") or role in {"hero", "front"}:
            try:
                actor.destroy()
                removed += 1
            except Exception:
                pass
    return removed


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=2000)
    parser.add_argument("--map", dest="map_name", default="straight_road_for_baguang")
    parser.add_argument("--ego-spawn-index", type=int, default=0)
    parser.add_argument("--lead-distance-m", type=float, default=300.0)
    parser.add_argument("--duration-s", type=float, default=5.0)
    parser.add_argument("--fixed-dt-s", type=float, default=0.05)
    parser.add_argument("--vehicle-blueprint", default="")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--keep-actors", action="store_true")
    parser.add_argument("--no-clean-existing", action="store_true")
    parser.add_argument("--spectator", action="store_true", default=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    carla = _import_carla()

    run_dir = Path(args.run_dir or f"runs/baguang_followstop_300m_{_timestamp_id()}").resolve()
    client = carla.Client(args.host, args.port)
    client.set_timeout(60.0)

    available = client.get_available_maps()
    if args.map_name not in available and f"/{args.map_name}" not in " ".join(available):
        print(f"[followstop-300m][info] available custom maps: {[m for m in available if 'baguang' in m.lower() or 'straight' in m.lower()]}")

    world = client.load_world(args.map_name)
    settings = world.get_settings()
    old_synchronous_mode = bool(settings.synchronous_mode)
    old_fixed_delta_seconds = settings.fixed_delta_seconds
    settings.synchronous_mode = True
    settings.fixed_delta_seconds = float(args.fixed_dt_s)
    world.apply_settings(settings)

    ego = None
    front = None
    try:
        if not args.no_clean_existing:
            removed = _destroy_existing_dynamic_actors(world)
            print(f"[followstop-300m] removed_dynamic_actors={removed}")

        carla_map = world.get_map()
        spawns = carla_map.get_spawn_points()
        if not spawns:
            raise RuntimeError(f"Map {args.map_name} has no spawn points")
        if args.ego_spawn_index < 0 or args.ego_spawn_index >= len(spawns):
            raise RuntimeError(f"ego spawn index {args.ego_spawn_index} outside 0..{len(spawns)-1}")

        ego_tf = spawns[args.ego_spawn_index]
        ego_wp = carla_map.get_waypoint(
            ego_tf.location,
            project_to_road=True,
            lane_type=carla.LaneType.Driving,
        )
        if ego_wp is None:
            raise RuntimeError("Could not project ego spawn to a driving waypoint")
        lead_candidates = ego_wp.next(float(args.lead_distance_m))
        if not lead_candidates:
            raise RuntimeError(f"No waypoint {args.lead_distance_m:.1f} m ahead of ego")
        lead_wp = lead_candidates[0]
        front_tf = carla.Transform(
            lead_wp.transform.location + carla.Location(z=0.35),
            lead_wp.transform.rotation,
        )
        ego_tf = carla.Transform(ego_wp.transform.location + carla.Location(z=0.35), ego_wp.transform.rotation)

        bp_lib = world.get_blueprint_library()
        bp = _pick_vehicle_blueprint(bp_lib, args.vehicle_blueprint or None)

        _set_role(bp, "front")
        front = world.try_spawn_actor(bp, front_tf)
        if front is None:
            raise RuntimeError(f"Failed to spawn front vehicle at {front_tf}")
        front.set_simulate_physics(True)
        front.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0, hand_brake=True))

        _set_role(bp, "hero")
        ego = world.try_spawn_actor(bp, ego_tf)
        if ego is None:
            raise RuntimeError(f"Failed to spawn ego vehicle at {ego_tf}")
        ego.set_simulate_physics(True)
        ego.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0, hand_brake=False))

        world.tick()

        if args.spectator:
            spectator = world.get_spectator()
            spectator.set_transform(
                carla.Transform(
                    ego_tf.location + carla.Location(x=-80.0, z=90.0),
                    carla.Rotation(pitch=-58.0, yaw=ego_tf.rotation.yaw),
                )
            )

        ego_actual = ego.get_transform()
        front_actual = front.get_transform()
        metrics = _forward_metrics(ego_actual, front_actual)
        report = {
            "schema_version": "baguang_followstop_300m.v1",
            "run_id": run_dir.name,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "map": world.get_map().name,
            "requested": {
                "map_name": args.map_name,
                "ego_spawn_index": args.ego_spawn_index,
                "lead_distance_m": args.lead_distance_m,
            },
            "ego": {
                "actor_id": ego.id,
                "role_name": "hero",
                "transform": {
                    "x": ego_actual.location.x,
                    "y": ego_actual.location.y,
                    "z": ego_actual.location.z,
                    "yaw": ego_actual.rotation.yaw,
                },
                "road_id": ego_wp.road_id,
                "lane_id": ego_wp.lane_id,
                "s": ego_wp.s,
            },
            "front": {
                "actor_id": front.id,
                "role_name": "front",
                "transform": {
                    "x": front_actual.location.x,
                    "y": front_actual.location.y,
                    "z": front_actual.location.z,
                    "yaw": front_actual.rotation.yaw,
                },
                "road_id": lead_wp.road_id,
                "lane_id": lead_wp.lane_id,
                "s": lead_wp.s,
                "held_with_brake": True,
            },
            "relative": metrics,
            "validation": {
                "same_road": ego_wp.road_id == lead_wp.road_id,
                "same_lane": ego_wp.lane_id == lead_wp.lane_id,
                "lead_distance_ok": abs(metrics["longitudinal_m"] - float(args.lead_distance_m)) <= 2.0,
                "lateral_alignment_ok": abs(metrics["lateral_m"]) <= 0.5,
                "heading_alignment_ok": abs(metrics["yaw_diff_deg"]) <= 2.0,
            },
            "keep_actors": bool(args.keep_actors),
        }
        _write_json(run_dir / "followstop_300m_spawn_report.json", report)
        print(json.dumps(report["relative"], indent=2, sort_keys=True))
        print(f"[followstop-300m] report={run_dir / 'followstop_300m_spawn_report.json'}")

        ticks = max(1, int(round(float(args.duration_s) / float(args.fixed_dt_s))))
        for _ in range(ticks):
            front.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0, hand_brake=True))
            world.tick()
            time.sleep(0.0)

        return 0
    finally:
        if not args.keep_actors:
            for actor in (front, ego):
                if actor is not None:
                    try:
                        actor.destroy()
                    except Exception:
                        pass
        try:
            old_settings = world.get_settings()
            old_settings.synchronous_mode = old_synchronous_mode
            old_settings.fixed_delta_seconds = old_fixed_delta_seconds
            world.apply_settings(old_settings)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
