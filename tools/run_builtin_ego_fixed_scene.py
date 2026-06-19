#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.scenario_player.builtin_ego_runner import run_builtin_ego_fixed_scene


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a CARLA fixed scene with the builtin diagnostic ego controller")
    parser.add_argument("--template", "--scenario", dest="template", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=2000)
    parser.add_argument("--town", default="Town01")
    parser.add_argument("--duration-s", type=float)
    parser.add_argument("--fixed-dt-s", type=float, default=0.05)
    parser.add_argument("--ego-spawn-index", type=int)
    parser.add_argument(
        "--ego-spawn-s-offset-m",
        type=float,
        help="Move ego spawn forward along the CARLA waypoint route before spawning; diagnostic RunConfig evidence only.",
    )
    parser.add_argument("--target-speed-mps", type=float)
    parser.add_argument("--follow-spectator", action="store_true")
    parser.add_argument("--spectator-distance", type=float, default=14.0)
    parser.add_argument("--spectator-height", type=float, default=5.0)
    parser.add_argument("--spectator-pitch", type=float, default=-18.0)
    parser.add_argument("--realtime", action="store_true", help="Sleep between sync ticks so wall-clock video matches sim time")
    args = parser.parse_args()

    result = run_builtin_ego_fixed_scene(
        template_path=args.template,
        run_dir=args.run_dir,
        host=args.host,
        port=args.port,
        town=args.town,
        duration_s=args.duration_s,
        fixed_dt_s=args.fixed_dt_s,
        ego_spawn_index=args.ego_spawn_index,
        ego_spawn_s_offset_m=args.ego_spawn_s_offset_m,
        target_speed_mps=args.target_speed_mps,
        follow_spectator=args.follow_spectator,
        spectator_distance=args.spectator_distance,
        spectator_height=args.spectator_height,
        spectator_pitch=args.spectator_pitch,
        realtime=args.realtime,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
