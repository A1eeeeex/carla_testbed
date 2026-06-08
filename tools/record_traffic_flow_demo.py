#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ctypes
import math
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.traffic.demo_recording import (  # noqa: E402
    compute_displacements,
    movement_status,
    write_json,
    write_jsonl,
    xy_from_transform,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Record a CARLA Traffic Manager random traffic demo.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=2000)
    parser.add_argument("--map", default="Town01")
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--vehicles", type=int, default=8)
    parser.add_argument("--walkers", type=int, default=0)
    parser.add_argument("--seed", type=int, default=60605)
    parser.add_argument("--tm-port", type=int, default=8000)
    parser.add_argument("--duration-s", type=float, default=30.0)
    parser.add_argument("--fps", type=int, default=20)
    parser.add_argument("--display", default=":0")
    parser.add_argument("--window-x", type=int, default=80)
    parser.add_argument("--window-y", type=int, default=97)
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=720)
    parser.add_argument("--prewarm-ticks", type=int, default=120)
    parser.add_argument("--min-moving-actors", type=int, default=2)
    parser.add_argument("--min-displacement-m", type=float, default=5.0)
    parser.add_argument("--max-spawn-retries", type=int, default=4)
    parser.add_argument("--ignore-lights-percentage", type=float, default=0.0)
    parser.add_argument(
        "--traffic-light-demo-cycle",
        action="store_true",
        help="Use a short visual-demo traffic-light cycle: red=5s, yellow=1.5s, green=6s.",
    )
    parser.add_argument("--traffic-light-red-time-s", type=float, default=None)
    parser.add_argument("--traffic-light-yellow-time-s", type=float, default=None)
    parser.add_argument("--traffic-light-green-time-s", type=float, default=None)
    parser.add_argument("--walker-cross-factor", type=float, default=0.0)
    parser.add_argument("--walker-speed-min-mps", type=float, default=1.0)
    parser.add_argument("--walker-speed-max-mps", type=float, default=1.8)
    parser.add_argument(
        "--walker-destination-radius-m",
        type=float,
        default=None,
        help="Prefer WalkerAIController destinations within this radius of the intersection center.",
    )
    parser.add_argument("--camera-mode", choices=("follow", "intersection", "intersection_chase"), default="follow")
    parser.add_argument("--intersection-center", default=None, help="Optional x,y center for intersection camera/spawn focus.")
    parser.add_argument("--intersection-radius-m", type=float, default=120.0)
    parser.add_argument("--camera-height-m", type=float, default=85.0)
    parser.add_argument("--camera-yaw-deg", type=float, default=35.0)
    parser.add_argument("--camera-pitch-deg", type=float, default=-78.0)
    parser.add_argument("--follow-distance-m", type=float, default=13.0)
    parser.add_argument("--follow-height-m", type=float, default=6.0)
    parser.add_argument("--follow-pitch-deg", type=float, default=-20.0)
    parser.add_argument(
        "--follow-target-index",
        type=int,
        default=0,
        help="Vehicle index to follow. Use -1 for fastest prewarm vehicle, -2 for nearest moving vehicle to --intersection-center.",
    )
    parser.add_argument(
        "--follow-lock-target",
        action="store_true",
        help="Keep the follow camera locked to one spawned vehicle instead of cycling between traffic actors.",
    )
    parser.add_argument(
        "--camera-distance-m",
        type=float,
        default=0.0,
        help="For intersection camera, place the camera this far back from the focused intersection center.",
    )
    parser.add_argument(
        "--camera-target-z-m",
        type=float,
        default=1.5,
        help="Target height used by --camera-auto-pitch for oblique intersection views.",
    )
    parser.add_argument(
        "--camera-auto-pitch",
        action="store_true",
        help="Aim the oblique intersection camera at the intersection center instead of using --camera-pitch-deg.",
    )
    parser.add_argument("--reuse-world", action="store_true", help="Do not reload the map if current world already matches.")
    parser.add_argument("--destroy-on-exit", action="store_true", help="Destroy demo vehicles after recording.")
    args = parser.parse_args()
    if args.traffic_light_demo_cycle:
        args.traffic_light_red_time_s = 5.0 if args.traffic_light_red_time_s is None else args.traffic_light_red_time_s
        args.traffic_light_yellow_time_s = (
            1.5 if args.traffic_light_yellow_time_s is None else args.traffic_light_yellow_time_s
        )
        args.traffic_light_green_time_s = 6.0 if args.traffic_light_green_time_s is None else args.traffic_light_green_time_s

    run_dir = args.out or Path("runs") / f"traffic_flow_recording_{time.strftime('%Y%m%d_%H%M%S')}_normal"
    video_dir = run_dir / "video"
    artifacts = run_dir / "artifacts"
    video_dir.mkdir(parents=True, exist_ok=True)
    artifacts.mkdir(parents=True, exist_ok=True)

    import carla  # noqa: PLC0415
    from carla import command  # noqa: PLC0415

    client = carla.Client(args.host, args.port)
    client.set_timeout(60.0)
    world = _prepare_world(client, args.map, reuse_world=args.reuse_world)
    original_settings = world.get_settings()
    settings = world.get_settings()
    settings.synchronous_mode = True
    settings.fixed_delta_seconds = 1.0 / float(args.fps)
    world.apply_settings(settings)

    tm = client.get_trafficmanager(args.tm_port)
    _configure_tm(tm, args)
    _destroy_demo_actors(client, world)
    traffic_light_timing_changes = _apply_traffic_light_timing(world, args)

    intersection_center = _resolve_intersection_center(world, args)
    actors: list[Any] = []
    walkers: list[Any] = []
    controllers: list[Any] = []
    responses: list[Any] = []
    rng = random.Random(args.seed)
    try:
        for attempt in range(args.max_spawn_retries):
            actors, responses = _spawn_traffic_batch(
                client,
                world,
                command,
                tm,
                args,
                seed=args.seed + attempt,
                rng=random.Random(args.seed + attempt),
                intersection_center=intersection_center,
            )
            if not actors:
                continue
            walkers, controllers, walker_events = _spawn_walkers(
                world,
                args,
                rng=random.Random(args.seed + attempt + 1000),
                intersection_center=intersection_center,
            )
            prewarm = _prewarm_and_measure(world, actors, ticks=args.prewarm_ticks)
            if walkers:
                walker_prewarm = _prewarm_and_measure(world, walkers, ticks=max(1, args.prewarm_ticks // 3))
                prewarm.update(walker_prewarm)
            movement = movement_status(
                prewarm,
                min_moving_actors=args.min_moving_actors,
                min_displacement_m=args.min_displacement_m,
            )
            if movement["status"] == "pass":
                break
            _destroy_controllers_and_walkers(controllers, walkers)
            controllers = []
            walkers = []
            _destroy_actors(client, actors)
            actors = []
        else:
            movement = movement_status({}, min_moving_actors=args.min_moving_actors, min_displacement_m=args.min_displacement_m)

        if not actors:
            status = {
                "schema_version": "traffic_flow_recording_status.v1",
                "status": "failed",
                "run_dir": str(run_dir),
                "blocking_reasons": movement["blocking_reasons"] or ["traffic_actors_not_spawned"],
            }
            write_json(artifacts / "traffic_flow_recording_status.json", status)
            print(status)
            return 2

        if args.camera_mode == "follow" and args.follow_lock_target:
            _select_follow_target(args, actors, prewarm if "prewarm" in locals() else {})
        _set_wide_spectator(world, args=args, intersection_center=intersection_center)
        window = _find_carla_window()
        if window is not None:
            _raise_window(window, x=args.window_x, y=args.window_y, width=args.width, height=args.height)
            time.sleep(1.0)

        manifest, events = _build_manifest(
            world,
            actors,
            walkers,
            controllers,
            args,
            responses,
            intersection_center=intersection_center,
            traffic_light_timing_changes=traffic_light_timing_changes,
        )
        events.extend(walker_events if "walker_events" in locals() else [])
        write_json(artifacts / "traffic_flow_manifest.json", manifest)
        write_jsonl(artifacts / "traffic_flow_events.jsonl", events)
        write_jsonl(artifacts / "traffic_spawn_candidates.jsonl", [])

        video_path = video_dir / "traffic_flow_demo_normal.mp4"
        recording = _record_video(world, [*actors, *walkers], video_path=video_path, args=args)
        status = {
            "schema_version": "traffic_flow_recording_status.v1",
            "status": "ready" if recording["video_ready"] and recording["recording_movement"]["status"] == "pass" else "failed",
            "run_dir": str(run_dir),
            "video_path": str(video_path),
            "window_id": None if window is None else hex(window),
            "spawned_vehicle_count": len(actors),
            "spawned_walker_count": len(walkers),
            "walker_controller_count": len(controllers),
            **recording,
        }
        write_json(artifacts / "traffic_flow_recording_status.json", status)
        print(status)
        return 0 if status["status"] == "ready" else 2
    finally:
        if args.destroy_on_exit and (walkers or controllers):
            _destroy_controllers_and_walkers(controllers, walkers)
        if args.destroy_on_exit and actors:
            _destroy_actors(client, actors)
        _restore_traffic_light_timing(world, traffic_light_timing_changes)
        restore = world.get_settings()
        restore.synchronous_mode = original_settings.synchronous_mode
        restore.fixed_delta_seconds = original_settings.fixed_delta_seconds
        world.apply_settings(restore)


def _prepare_world(client: Any, map_name: str, *, reuse_world: bool) -> Any:
    world = client.get_world()
    if reuse_world and world.get_map().name.endswith(map_name):
        return world
    world = client.load_world(map_name)
    time.sleep(2.0)
    return world


def _configure_tm(tm: Any, args: argparse.Namespace) -> None:
    if hasattr(tm, "set_synchronous_mode"):
        tm.set_synchronous_mode(True)
    if hasattr(tm, "set_random_device_seed"):
        tm.set_random_device_seed(args.seed)
    if hasattr(tm, "set_global_distance_to_leading_vehicle"):
        tm.set_global_distance_to_leading_vehicle(8.0)
    elif hasattr(tm, "global_distance_to_leading_vehicle"):
        tm.global_distance_to_leading_vehicle(8.0)
    if hasattr(tm, "global_percentage_speed_difference"):
        tm.global_percentage_speed_difference(-25.0)


def _apply_traffic_light_timing(world: Any, args: argparse.Namespace) -> list[dict[str, Any]]:
    requested = {
        "red_time_s": args.traffic_light_red_time_s,
        "yellow_time_s": args.traffic_light_yellow_time_s,
        "green_time_s": args.traffic_light_green_time_s,
    }
    if all(value is None for value in requested.values()):
        return []
    changes: list[dict[str, Any]] = []
    try:
        traffic_lights = list(world.get_actors().filter("traffic.traffic_light*"))
    except Exception:
        return [{"status": "failed", "reason": "traffic_light_actor_query_failed", "requested": requested}]
    for light in traffic_lights:
        change: dict[str, Any] = {"actor_id": int(light.id), "status": "applied", "requested": requested}
        try:
            old_red = light.get_red_time() if hasattr(light, "get_red_time") else None
            old_yellow = light.get_yellow_time() if hasattr(light, "get_yellow_time") else None
            old_green = light.get_green_time() if hasattr(light, "get_green_time") else None
            change["old"] = {"red_time_s": old_red, "yellow_time_s": old_yellow, "green_time_s": old_green}
            if requested["red_time_s"] is not None and hasattr(light, "set_red_time"):
                light.set_red_time(float(requested["red_time_s"]))
            if requested["yellow_time_s"] is not None and hasattr(light, "set_yellow_time"):
                light.set_yellow_time(float(requested["yellow_time_s"]))
            if requested["green_time_s"] is not None and hasattr(light, "set_green_time"):
                light.set_green_time(float(requested["green_time_s"]))
        except Exception as exc:
            change["status"] = "failed"
            change["reason"] = str(exc)
        changes.append(change)
    return changes


def _restore_traffic_light_timing(world: Any, changes: list[dict[str, Any]]) -> None:
    if not changes:
        return
    try:
        actors = world.get_actors()
    except Exception:
        return
    for change in changes:
        if change.get("status") != "applied":
            continue
        old = change.get("old") or {}
        try:
            light = actors.find(int(change["actor_id"]))
        except Exception:
            continue
        if light is None:
            continue
        try:
            if old.get("red_time_s") is not None and hasattr(light, "set_red_time"):
                light.set_red_time(float(old["red_time_s"]))
            if old.get("yellow_time_s") is not None and hasattr(light, "set_yellow_time"):
                light.set_yellow_time(float(old["yellow_time_s"]))
            if old.get("green_time_s") is not None and hasattr(light, "set_green_time"):
                light.set_green_time(float(old["green_time_s"]))
        except Exception:
            continue


def _spawn_traffic_batch(
    client: Any,
    world: Any,
    command: Any,
    tm: Any,
    args: argparse.Namespace,
    *,
    seed: int,
    rng: random.Random,
    intersection_center: tuple[float, float] | None,
) -> tuple[list[Any], list[Any]]:
    blueprints = _vehicle_blueprints(world, rng)
    spawns = _candidate_spawn_points(world, seed=seed, args=args, intersection_center=intersection_center)
    batch = []
    for index, spawn in enumerate(spawns[: args.vehicles]):
        blueprint = blueprints[index % len(blueprints)]
        if blueprint.has_attribute("role_name"):
            blueprint.set_attribute("role_name", f"tm_normal_demo_{index}")
        batch.append(command.SpawnActor(blueprint, spawn).then(command.SetAutopilot(command.FutureActor, True, args.tm_port)))
    responses = client.apply_batch_sync(batch, True)
    actor_ids = [response.actor_id for response in responses if not getattr(response, "error", None)]
    actors = [world.get_actor(actor_id) for actor_id in actor_ids]
    actors = [actor for actor in actors if actor is not None]
    for actor in actors:
        if hasattr(tm, "vehicle_percentage_speed_difference"):
            tm.vehicle_percentage_speed_difference(actor, rng.uniform(-35.0, -5.0))
        if hasattr(tm, "distance_to_leading_vehicle"):
            tm.distance_to_leading_vehicle(actor, rng.uniform(5.0, 10.0))
        if hasattr(tm, "auto_lane_change"):
            tm.auto_lane_change(actor, True)
        if args.ignore_lights_percentage and hasattr(tm, "ignore_lights_percentage"):
            tm.ignore_lights_percentage(actor, float(args.ignore_lights_percentage))
    return actors, responses


def _vehicle_blueprints(world: Any, rng: random.Random) -> list[Any]:
    library = world.get_blueprint_library()
    patterns = ["vehicle.tesla.model3", "vehicle.audi.tt", "vehicle.lincoln.mkz_2017", "vehicle.dodge.charger_2020"]
    blueprints: list[Any] = []
    for pattern in patterns:
        blueprints.extend(list(library.filter(pattern)))
    rng.shuffle(blueprints)
    if not blueprints:
        raise RuntimeError("no vehicle blueprints available")
    return blueprints


def _walker_blueprints(world: Any, rng: random.Random) -> list[Any]:
    library = world.get_blueprint_library()
    blueprints = list(library.filter("walker.pedestrian.*"))
    rng.shuffle(blueprints)
    return blueprints


def _spawn_walkers(
    world: Any,
    args: argparse.Namespace,
    *,
    rng: random.Random,
    intersection_center: tuple[float, float] | None,
) -> tuple[list[Any], list[Any], list[dict[str, Any]]]:
    if args.walkers <= 0:
        return [], [], []
    import carla  # noqa: PLC0415

    if hasattr(world, "set_pedestrians_seed"):
        world.set_pedestrians_seed(args.seed)
    if hasattr(world, "set_pedestrians_cross_factor"):
        world.set_pedestrians_cross_factor(float(args.walker_cross_factor))
    walker_bps = _walker_blueprints(world, rng)
    controller_bps = list(world.get_blueprint_library().filter("controller.ai.walker"))
    if not walker_bps or not controller_bps:
        return [], [], [{"event": "walker_setup_failed", "reason": "walker_or_controller_blueprint_missing"}]

    locations = _candidate_walker_locations(world, args, rng=rng, intersection_center=intersection_center)
    walkers: list[Any] = []
    controllers: list[Any] = []
    events: list[dict[str, Any]] = []
    for index, location in enumerate(locations[: args.walkers]):
        blueprint = walker_bps[index % len(walker_bps)]
        if blueprint.has_attribute("role_name"):
            blueprint.set_attribute("role_name", f"tm_normal_demo_walker_{index}")
        if blueprint.has_attribute("is_invincible"):
            blueprint.set_attribute("is_invincible", "false")
        walker = world.try_spawn_actor(blueprint, carla.Transform(location, carla.Rotation()))
        events.append({"event": "walker_spawn_attempt", "role_name": f"tm_normal_demo_walker_{index}", "status": "ok" if walker else "failed"})
        if walker is None:
            continue
        walkers.append(walker)
    world.tick()
    for index, walker in enumerate(walkers):
        try:
            controller = world.spawn_actor(controller_bps[0], carla.Transform(), walker)
        except Exception as exc:
            events.append({"event": "walker_controller_spawn_failed", "actor_id": walker.id, "error": str(exc)})
            continue
        controllers.append(controller)
    world.tick()
    for index, (walker, controller) in enumerate(zip(walkers, controllers)):
        destination = _random_nav_location_near_intersection(world, args, rng=rng, intersection_center=intersection_center)
        speed = rng.uniform(float(args.walker_speed_min_mps), float(args.walker_speed_max_mps))
        try:
            controller.start()
            if destination is not None:
                controller.go_to_location(destination)
            controller.set_max_speed(speed)
            events.append(
                {
                    "event": "walker_controller_started",
                    "actor_id": walker.id,
                    "controller_id": controller.id,
                    "role_name": walker.attributes.get("role_name", f"tm_normal_demo_walker_{index}"),
                    "max_speed_mps": speed,
                }
            )
        except Exception as exc:
            events.append({"event": "walker_controller_start_failed", "actor_id": walker.id, "error": str(exc)})
    return walkers, controllers, events


def _candidate_walker_locations(
    world: Any,
    args: argparse.Namespace,
    *,
    rng: random.Random,
    intersection_center: tuple[float, float] | None,
) -> list[Any]:
    attempts = max(args.walkers * 25, 300)
    near: list[Any] = []
    any_locations: list[Any] = []
    seen: set[tuple[float, float, float]] = set()
    for _ in range(attempts):
        location = _random_nav_location_near_intersection(world, args, rng=rng, intersection_center=None)
        if location is None:
            continue
        key = (round(location.x, 2), round(location.y, 2), round(location.z, 2))
        if key in seen:
            continue
        seen.add(key)
        any_locations.append(location)
        if intersection_center is None:
            near.append(location)
            continue
        distance = ((location.x - intersection_center[0]) ** 2 + (location.y - intersection_center[1]) ** 2) ** 0.5
        if distance <= float(args.intersection_radius_m):
            near.append(location)
    rng.shuffle(near)
    rng.shuffle(any_locations)
    return near if len(near) >= args.walkers else [*near, *any_locations]


def _random_nav_location_near_intersection(
    world: Any,
    args: argparse.Namespace,
    *,
    rng: random.Random,
    intersection_center: tuple[float, float] | None,
) -> Any | None:
    del rng
    if not hasattr(world, "get_random_location_from_navigation"):
        return None
    if intersection_center is not None:
        radius = float(args.walker_destination_radius_m or args.intersection_radius_m)
        fallback = None
        for _ in range(80):
            try:
                location = world.get_random_location_from_navigation()
            except Exception:
                return None
            if location is None:
                continue
            fallback = fallback or location
            distance = ((location.x - intersection_center[0]) ** 2 + (location.y - intersection_center[1]) ** 2) ** 0.5
            if distance <= radius:
                return location
        return fallback
    try:
        return world.get_random_location_from_navigation()
    except Exception:
        return None


def _candidate_spawn_points(
    world: Any,
    *,
    seed: int,
    args: argparse.Namespace,
    intersection_center: tuple[float, float] | None,
) -> list[Any]:
    rng = random.Random(seed)
    spawn_points = list(world.get_map().get_spawn_points())
    candidates = []
    for spawn in spawn_points:
        waypoint = world.get_map().get_waypoint(spawn.location)
        if intersection_center is not None:
            distance_to_center = ((spawn.location.x - intersection_center[0]) ** 2 + (spawn.location.y - intersection_center[1]) ** 2) ** 0.5
            if distance_to_center > float(args.intersection_radius_m):
                continue
        if getattr(waypoint, "is_junction", False):
            continue
        if not waypoint.next(20.0):
            continue
        candidates.append(spawn)
    rng.shuffle(candidates)
    if len(candidates) < args.vehicles and intersection_center is not None:
        near_spawns = sorted(
            spawn_points,
            key=lambda spawn: ((spawn.location.x - intersection_center[0]) ** 2 + (spawn.location.y - intersection_center[1]) ** 2),
        )
        for spawn in near_spawns:
            if spawn not in candidates:
                candidates.append(spawn)
            if len(candidates) >= args.vehicles:
                break
    return candidates or spawn_points


def _prewarm_and_measure(world: Any, actors: list[Any], *, ticks: int) -> dict[str, float]:
    start = {actor.id: xy_from_transform(actor.get_transform()) for actor in actors if actor.is_alive}
    for _ in range(max(1, ticks)):
        world.tick()
    end = {actor.id: xy_from_transform(actor.get_transform()) for actor in actors if actor.is_alive}
    return compute_displacements(start, end)


def _select_follow_target(args: argparse.Namespace, actors: list[Any], displacements: dict[str, float]) -> None:
    live_vehicles = [actor for actor in actors if actor.is_alive and str(getattr(actor, "type_id", "")).startswith("vehicle.")]
    if not live_vehicles:
        return
    requested_index = int(args.follow_target_index)
    index = max(0, min(requested_index, len(live_vehicles) - 1))
    if requested_index == -2 and getattr(args, "_resolved_intersection_center", None) is not None:
        center = getattr(args, "_resolved_intersection_center")
        moving = [
            actor
            for actor in live_vehicles
            if float(displacements.get(str(actor.id), 0.0)) >= float(getattr(args, "min_displacement_m", 0.0))
        ]
        candidates = moving or live_vehicles
        live_vehicles = sorted(
            candidates,
            key=lambda actor: (
                (actor.get_transform().location.x - center[0]) ** 2 + (actor.get_transform().location.y - center[1]) ** 2,
                -float(displacements.get(str(actor.id), 0.0)),
            ),
        )
        index = 0
    elif requested_index == -1:
        live_vehicles = sorted(live_vehicles, key=lambda actor: float(displacements.get(str(actor.id), 0.0)), reverse=True)
        index = 0
    target = live_vehicles[index]
    setattr(args, "_follow_actor_id", int(target.id))
    setattr(args, "_follow_actor_type_id", str(getattr(target, "type_id", "")))


def _record_video(world: Any, actors: list[Any], *, video_path: Path, args: argparse.Namespace) -> dict[str, Any]:
    capture = f"{args.display}.0+{args.window_x},{args.window_y}" if args.display == ":0" else f"{args.display}+{args.window_x},{args.window_y}"
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "x11grab",
        "-framerate",
        str(args.fps),
        "-video_size",
        f"{args.width}x{args.height}",
        "-i",
        capture,
        "-t",
        str(args.duration_s),
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-pix_fmt",
        "yuv420p",
        str(video_path),
    ]
    positions_start = {actor.id: xy_from_transform(actor.get_transform()) for actor in actors if actor.is_alive}
    proc = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    ticks = int(args.duration_s * args.fps)
    recording_started = time.monotonic()
    frame_period_s = 1.0 / float(args.fps)
    for tick in range(ticks):
        target_time = recording_started + tick * frame_period_s
        if args.camera_mode == "intersection":
            _set_wide_spectator(world, args=args, intersection_center=None)
        elif args.camera_mode == "intersection_chase":
            _set_intersection_chase_spectator(world, actors, args=args, tick=tick)
        else:
            _set_follow_spectator(world, actors, args=args, tick=tick)
        world.tick()
        sleep_s = target_time + frame_period_s - time.monotonic()
        if sleep_s > 0.0:
            time.sleep(sleep_s)
    _set_wide_spectator(world, args=args, intersection_center=None)
    try:
        out, err = proc.communicate(timeout=max(30.0, float(args.duration_s) + 30.0))
    except subprocess.TimeoutExpired:
        proc.terminate()
        out, err = proc.communicate(timeout=10)
        err = (err or "") + "\nffmpeg_terminated_after_finalize_timeout"
    del out
    positions_end = {actor.id: xy_from_transform(actor.get_transform()) for actor in actors if actor.is_alive}
    displacements = compute_displacements(positions_start, positions_end)
    return {
        "video_ready": proc.returncode == 0 and video_path.exists() and video_path.stat().st_size > 0,
        "ffmpeg_returncode": proc.returncode,
        "ffmpeg_cmd": ffmpeg_cmd,
        "duration_s": float(args.duration_s),
        "fps": int(args.fps),
        "wall_time_s": time.monotonic() - recording_started,
        "sim_ticks_recorded": ticks,
        "frame_period_s": frame_period_s,
        "recording_movement": movement_status(
            displacements,
            min_moving_actors=args.min_moving_actors,
            min_displacement_m=args.min_displacement_m,
        ),
        "displacements_m": displacements,
        "live_vehicle_count_after_recording": len(world.get_actors().filter("vehicle.*")),
        "live_walker_count_after_recording": len(world.get_actors().filter("walker.pedestrian.*")),
        "ffmpeg_stderr_tail": "\n".join(err.splitlines()[-20:]),
    }


def _set_wide_spectator(
    world: Any,
    *,
    args: argparse.Namespace | None = None,
    intersection_center: tuple[float, float] | None = None,
) -> None:
    import carla  # noqa: PLC0415

    center = intersection_center
    if center is None and args is not None:
        center = getattr(args, "_resolved_intersection_center", None)
    x, y = center if center is not None else (230.0, 190.0)
    height = float(getattr(args, "camera_height_m", 75.0) if args is not None else 75.0)
    pitch = float(getattr(args, "camera_pitch_deg", -78.0) if args is not None else -78.0)
    yaw = float(getattr(args, "camera_yaw_deg", 35.0) if args is not None else 35.0)
    distance = float(getattr(args, "camera_distance_m", 0.0) if args is not None else 0.0)
    if center is not None and distance > 0.0:
        yaw_rad = math.radians(yaw)
        x -= math.cos(yaw_rad) * distance
        y -= math.sin(yaw_rad) * distance
        if bool(getattr(args, "camera_auto_pitch", False)):
            target_z = float(getattr(args, "camera_target_z_m", 1.5))
            pitch = -math.degrees(math.atan2(max(height - target_z, 0.1), distance))
    world.get_spectator().set_transform(
        carla.Transform(carla.Location(float(x), float(y), height), carla.Rotation(pitch=pitch, yaw=yaw, roll=0.0))
    )


def _set_follow_spectator(world: Any, actors: list[Any], *, args: argparse.Namespace, tick: int) -> None:
    import carla  # noqa: PLC0415

    live = [actor for actor in actors if actor.is_alive]
    if not live:
        return
    vehicle_live = [actor for actor in live if str(getattr(actor, "type_id", "")).startswith("vehicle.")]
    camera_candidates = vehicle_live or live
    lead = None
    follow_actor_id = getattr(args, "_follow_actor_id", None)
    if follow_actor_id is not None:
        lead = next((actor for actor in camera_candidates if int(actor.id) == int(follow_actor_id)), None)
    if lead is None:
        lead = camera_candidates[(tick // 160) % len(camera_candidates)]
    try:
        transform = lead.get_transform()
    except RuntimeError:
        return
    forward = transform.get_forward_vector()
    distance = float(getattr(args, "follow_distance_m", 13.0))
    height = float(getattr(args, "follow_height_m", 6.0))
    pitch = float(getattr(args, "follow_pitch_deg", -20.0))
    world.get_spectator().set_transform(
        carla.Transform(
            carla.Location(
                transform.location.x - forward.x * distance,
                transform.location.y - forward.y * distance,
                transform.location.z + height,
            ),
            carla.Rotation(pitch=pitch, yaw=transform.rotation.yaw, roll=0.0),
        )
    )


def _set_intersection_chase_spectator(world: Any, actors: list[Any], *, args: argparse.Namespace, tick: int) -> None:
    import carla  # noqa: PLC0415

    center = getattr(args, "_resolved_intersection_center", None)
    if center is None:
        _set_follow_spectator(world, actors, args=args, tick=tick)
        return
    live = [actor for actor in actors if actor.is_alive]
    if not live:
        return
    ranked = sorted(
        live,
        key=lambda actor: (
            (actor.get_transform().location.x - center[0]) ** 2
            + (actor.get_transform().location.y - center[1]) ** 2
        ),
    )
    focus = ranked[(tick // 180) % min(8, len(ranked))]
    try:
        transform = focus.get_transform()
    except RuntimeError:
        return
    forward = transform.get_forward_vector()
    world.get_spectator().set_transform(
        carla.Transform(
            carla.Location(
                transform.location.x - forward.x * 17.0,
                transform.location.y - forward.y * 17.0,
                transform.location.z + 9.0,
            ),
            carla.Rotation(pitch=-25.0, yaw=transform.rotation.yaw, roll=0.0),
        )
    )


def _build_manifest(
    world: Any,
    actors: list[Any],
    walkers: list[Any],
    controllers: list[Any],
    args: argparse.Namespace,
    responses: list[Any],
    *,
    intersection_center: tuple[float, float] | None,
    traffic_light_timing_changes: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest_actors = []
    events = []
    for actor in actors:
        transform = actor.get_transform()
        velocity = actor.get_velocity()
        role = actor.attributes.get("role_name", "")
        manifest_actors.append(
            {
                "actor_id": actor.id,
                "role_name": role,
                "blueprint_id": actor.type_id,
                "provider": "carla_traffic_manager",
                "control_source": "carla_traffic_manager",
                "tm_port": args.tm_port,
                "spawn_transform": {
                    "x": transform.location.x,
                    "y": transform.location.y,
                    "z": transform.location.z,
                    "yaw": transform.rotation.yaw,
                },
                "speed_mps_at_manifest": (velocity.x**2 + velocity.y**2 + velocity.z**2) ** 0.5,
                "behavior": {
                    "normal_recording_demo": True,
                    "ignore_lights_percentage": float(args.ignore_lights_percentage),
                },
            }
        )
        events.append({"event": "autopilot_enabled", "actor_id": actor.id, "role_name": role, "tm_port": args.tm_port})
    manifest_walkers = []
    for walker in walkers:
        transform = walker.get_transform()
        velocity = walker.get_velocity()
        role = walker.attributes.get("role_name", "")
        manifest_walkers.append(
            {
                "actor_id": walker.id,
                "role_name": role,
                "blueprint_id": walker.type_id,
                "provider": "carla_walker_ai_controller",
                "control_source": "carla_walker_ai_controller",
                "spawn_transform": {
                    "location": {
                        "x": transform.location.x,
                        "y": transform.location.y,
                        "z": transform.location.z,
                    }
                },
                "speed_mps_at_manifest": (velocity.x**2 + velocity.y**2 + velocity.z**2) ** 0.5,
                "behavior": {
                    "normal_recording_demo": True,
                    "controller_started": True,
                },
            }
        )
    return (
        {
            "schema_version": "traffic_flow_manifest.v1",
            "enabled": True,
            "provider": "mixed_carla_flow" if walkers else "carla_traffic_manager",
            "seed": args.seed,
            "background_traffic_control_source": "carla_traffic_manager",
            "background_walker_control_source": "carla_walker_ai_controller" if walkers else "none",
            "requested_vehicle_count": args.vehicles,
            "spawned_vehicle_count": len(actors),
            "requested_walker_count": args.walkers,
            "spawned_walker_count": len(walkers),
            "controller_count": len(controllers),
            "controller_started_count": len(controllers),
            "world_pedestrians_seed": args.seed if walkers else None,
            "walker_cross_factor": float(args.walker_cross_factor),
            "traffic_light_timing_override": {
                "enabled": bool(traffic_light_timing_changes),
                "mode": "visual_demo_short_cycle" if bool(traffic_light_timing_changes) else "none",
                "red_time_s": args.traffic_light_red_time_s,
                "yellow_time_s": args.traffic_light_yellow_time_s,
                "green_time_s": args.traffic_light_green_time_s,
                "applied_count": sum(1 for change in traffic_light_timing_changes or [] if change.get("status") == "applied"),
                "failed_count": sum(1 for change in traffic_light_timing_changes or [] if change.get("status") == "failed"),
            },
            "tm_port": args.tm_port,
            "tm_synchronous_mode_requested": True,
            "tm_synchronous_mode_effective": True,
            "world_synchronous_mode": bool(world.get_settings().synchronous_mode),
            "destroy_on_teardown": bool(args.destroy_on_exit),
            "camera_mode": args.camera_mode,
            "follow_camera": {
                "locked": bool(getattr(args, "follow_lock_target", False)),
                "target_actor_id": getattr(args, "_follow_actor_id", None),
                "target_type_id": getattr(args, "_follow_actor_type_id", None),
                "target_index": int(getattr(args, "follow_target_index", 0)),
                "distance_m": float(getattr(args, "follow_distance_m", 13.0)),
                "height_m": float(getattr(args, "follow_height_m", 6.0)),
                "pitch_deg": float(getattr(args, "follow_pitch_deg", -20.0)),
            },
            "intersection_center": None if intersection_center is None else {"x": intersection_center[0], "y": intersection_center[1]},
            "intersection_radius_m": float(args.intersection_radius_m),
            "vehicles": manifest_actors,
            "walkers": manifest_walkers,
            "actors": manifest_actors,
            "warnings": _manifest_warnings(args),
            "errors": [str(getattr(response, "error", "")) for response in responses if getattr(response, "error", None)],
        },
        events,
    )


def _manifest_warnings(args: argparse.Namespace) -> list[str]:
    warnings = ["visual_demo_recording_not_natural_driving_evidence"]
    if float(args.ignore_lights_percentage):
        warnings.append("traffic_lights_ignored_for_visual_motion_demo")
    if bool(getattr(args, "traffic_light_demo_cycle", False)) or any(
        value is not None
        for value in (
            getattr(args, "traffic_light_red_time_s", None),
            getattr(args, "traffic_light_yellow_time_s", None),
            getattr(args, "traffic_light_green_time_s", None),
        )
    ):
        warnings.append("traffic_light_timing_shortened_for_visual_demo")
    if args.camera_mode.startswith("intersection"):
        warnings.append("intersection_visual_demo_not_scenario_validation")
    return warnings


def _resolve_intersection_center(world: Any, args: argparse.Namespace) -> tuple[float, float] | None:
    if args.intersection_center:
        parts = [float(part.strip()) for part in args.intersection_center.split(",", maxsplit=1)]
        if len(parts) != 2:
            raise ValueError("--intersection-center must be formatted as x,y")
        center = (parts[0], parts[1])
        setattr(args, "_resolved_intersection_center", center)
        return center
    if not args.camera_mode.startswith("intersection"):
        return None
    centers = _junction_centers(world, radius_m=float(args.intersection_radius_m))
    if not centers:
        return None
    center = centers[0]
    setattr(args, "_resolved_intersection_center", center)
    return center


def _junction_centers(world: Any, *, radius_m: float) -> list[tuple[float, float]]:
    try:
        waypoints = world.get_map().generate_waypoints(5.0)
    except Exception:
        return []
    groups: dict[Any, list[Any]] = {}
    for waypoint in waypoints:
        if not getattr(waypoint, "is_junction", False):
            continue
        key = _junction_key(waypoint)
        groups.setdefault(key, []).append(waypoint)
    spawn_points = list(world.get_map().get_spawn_points())
    scored: list[tuple[int, float, tuple[float, float]]] = []
    for group in groups.values():
        if not group:
            continue
        x = sum(float(wp.transform.location.x) for wp in group) / len(group)
        y = sum(float(wp.transform.location.y) for wp in group) / len(group)
        support = sum(
            1
            for spawn in spawn_points
            if ((spawn.location.x - x) ** 2 + (spawn.location.y - y) ** 2) ** 0.5 <= radius_m
        )
        scored.append((support, float(len(group)), (x, y)))
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [item[2] for item in scored]


def _junction_key(waypoint: Any) -> Any:
    try:
        junction = waypoint.get_junction()
        return getattr(junction, "id", None) or (round(waypoint.transform.location.x / 25.0), round(waypoint.transform.location.y / 25.0))
    except Exception:
        return (round(waypoint.transform.location.x / 25.0), round(waypoint.transform.location.y / 25.0))


def _destroy_demo_actors(client: Any, world: Any) -> None:
    actors = [
        actor
        for actor in world.get_actors().filter("vehicle.*")
        if actor.attributes.get("role_name", "").startswith(("tm_visible_demo_", "tm_recording_demo_", "tm_sync_demo_", "tm_normal_demo_"))
    ]
    _destroy_actors(client, actors)
    walkers = [
        actor
        for actor in world.get_actors().filter("walker.pedestrian.*")
        if actor.attributes.get("role_name", "").startswith("tm_normal_demo_walker_")
    ]
    controllers = list(world.get_actors().filter("controller.ai.walker"))
    _destroy_controllers_and_walkers(controllers, walkers)


def _destroy_controllers_and_walkers(controllers: list[Any], walkers: list[Any]) -> None:
    for controller in controllers:
        try:
            if hasattr(controller, "stop"):
                controller.stop()
        except Exception:
            pass
        try:
            controller.destroy()
        except Exception:
            pass
    for walker in walkers:
        try:
            walker.destroy()
        except Exception:
            pass


def _destroy_actors(client: Any, actors: list[Any]) -> None:
    from carla import command  # noqa: PLC0415

    if not actors:
        return
    client.apply_batch_sync([command.DestroyActor(actor) for actor in actors], True)


def _find_carla_window() -> int | None:
    try:
        result = subprocess.run(["xwininfo", "-root", "-tree"], text=True, capture_output=True, check=False)
    except FileNotFoundError:
        return None
    for line in result.stdout.splitlines():
        if "CarlaUE4" not in line and "CarlaUE4-Linux-Shipping" not in line:
            continue
        match = re.search(r"0x[0-9a-fA-F]+", line)
        if match:
            return int(match.group(0), 16)
    return None


def _raise_window(window_id: int, *, x: int, y: int, width: int, height: int) -> None:
    lib = ctypes.cdll.LoadLibrary("libX11.so.6")
    lib.XOpenDisplay.argtypes = [ctypes.c_char_p]
    lib.XOpenDisplay.restype = ctypes.c_void_p
    display = lib.XOpenDisplay(None)
    if not display:
        return
    parent_window = _top_level_parent(lib, display, window_id)
    lib.XMoveResizeWindow.argtypes = [
        ctypes.c_void_p,
        ctypes.c_ulong,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.c_uint,
    ]
    lib.XMapRaised.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    lib.XRaiseWindow.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    lib.XSetInputFocus.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
    lib.XFlush.argtypes = [ctypes.c_void_p]
    target = parent_window or window_id
    if parent_window and parent_window != window_id:
        # CARLA's drawable child is usually offset inside an unnamed frame window.
        lib.XMoveResizeWindow(display, target, max(0, x - 10), max(0, y - 45), width + 20, height + 55)
    else:
        lib.XMoveResizeWindow(display, target, x, y, width, height)
    lib.XMapRaised(display, target)
    lib.XRaiseWindow(display, target)
    lib.XMapRaised(display, window_id)
    lib.XRaiseWindow(display, window_id)
    _activate_window(lib, display, window_id)
    lib.XFlush(display)


def _top_level_parent(lib: Any, display: Any, window_id: int) -> int | None:
    root = ctypes.c_ulong()
    parent = ctypes.c_ulong()
    children = ctypes.POINTER(ctypes.c_ulong)()
    nchildren = ctypes.c_uint()
    lib.XQueryTree.argtypes = [
        ctypes.c_void_p,
        ctypes.c_ulong,
        ctypes.POINTER(ctypes.c_ulong),
        ctypes.POINTER(ctypes.c_ulong),
        ctypes.POINTER(ctypes.POINTER(ctypes.c_ulong)),
        ctypes.POINTER(ctypes.c_uint),
    ]
    lib.XQueryTree.restype = ctypes.c_int
    lib.XFree.argtypes = [ctypes.c_void_p]
    current = ctypes.c_ulong(window_id)
    top = int(window_id)
    for _ in range(16):
        ok = lib.XQueryTree(
            display,
            current,
            ctypes.byref(root),
            ctypes.byref(parent),
            ctypes.byref(children),
            ctypes.byref(nchildren),
        )
        if children:
            lib.XFree(children)
        if not ok or not parent.value or parent.value == root.value:
            return top
        top = int(parent.value)
        current = ctypes.c_ulong(parent.value)
    return top


def _activate_window(lib: Any, display: Any, window_id: int) -> None:
    try:
        lib.XDefaultRootWindow.argtypes = [ctypes.c_void_p]
        lib.XDefaultRootWindow.restype = ctypes.c_ulong
        lib.XInternAtom.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]
        lib.XInternAtom.restype = ctypes.c_ulong
        lib.XSendEvent.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_int, ctypes.c_long, ctypes.c_void_p]
        lib.XSendEvent.restype = ctypes.c_int
        root = lib.XDefaultRootWindow(display)
        atom = lib.XInternAtom(display, b"_NET_ACTIVE_WINDOW", False)

        class XClientMessageEvent(ctypes.Structure):
            _fields_ = [
                ("type", ctypes.c_int),
                ("serial", ctypes.c_ulong),
                ("send_event", ctypes.c_int),
                ("display", ctypes.c_void_p),
                ("window", ctypes.c_ulong),
                ("message_type", ctypes.c_ulong),
                ("format", ctypes.c_int),
                ("data", ctypes.c_long * 5),
            ]

        class XEvent(ctypes.Union):
            _fields_ = [("xclient", XClientMessageEvent), ("pad", ctypes.c_long * 24)]

        event = XEvent()
        event.xclient.type = 33
        event.xclient.display = display
        event.xclient.window = window_id
        event.xclient.message_type = atom
        event.xclient.format = 32
        event.xclient.data[0] = 2
        event.xclient.data[1] = 0
        event.xclient.data[2] = 0
        mask = (1 << 20) | (1 << 19)
        lib.XSendEvent(display, root, False, mask, ctypes.byref(event))
        lib.XSetInputFocus(display, window_id, 1, 0)
    except Exception:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
