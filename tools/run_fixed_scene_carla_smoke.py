#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.scenario_player.carla_smoke import run_fixed_scene_carla_smoke


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local CARLA-only fixed-scene smoke")
    parser.add_argument("--template", required=True, help="fixed_scene_template YAML or scenario YAML with fixed_scene")
    parser.add_argument("--run-dir", required=True, help="Output run directory")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=2000)
    parser.add_argument("--town", default="Town01")
    parser.add_argument("--duration-s", type=float)
    parser.add_argument("--fixed-dt-s", type=float, default=0.05)
    parser.add_argument("--ego-spawn-index", type=int)
    parser.add_argument("--ego-throttle", type=float, default=0.35)
    parser.add_argument("--ego-brake-after-s", type=float)
    args = parser.parse_args()

    result = run_fixed_scene_carla_smoke(
        template_path=args.template,
        run_dir=args.run_dir,
        host=args.host,
        port=args.port,
        town=args.town,
        duration_s=args.duration_s,
        fixed_dt_s=args.fixed_dt_s,
        ego_spawn_index=args.ego_spawn_index,
        ego_throttle=args.ego_throttle,
        ego_brake_after_s=args.ego_brake_after_s,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
