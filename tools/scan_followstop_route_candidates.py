#!/usr/bin/env python3
"""Scan a loaded CARLA map for long same-lane follow-stop candidates."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.autoware.followstop_route_candidates import (  # noqa: E402
    build_candidate_report,
    scan_followstop_route_candidates,
    write_candidate_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2000)
    parser.add_argument("--town", default="Town01")
    parser.add_argument("--out", required=True)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--front-target-ahead-m", type=float, default=300.0)
    parser.add_argument("--goal-beyond-front-m", type=float, default=80.0)
    parser.add_argument("--max-ahead-m", type=float, default=380.0)
    parser.add_argument("--max-lateral-m", type=float, default=4.0)
    parser.add_argument("--max-heading-diff-deg", type=float, default=35.0)
    parser.add_argument("--max-front-target-error-m", type=float, default=30.0)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(argv)

    try:
        import carla  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover - exercised in runtime env
        print(f"CARLA Python API unavailable: {exc}", file=sys.stderr)
        return 2

    client = carla.Client(args.host, args.port)
    client.set_timeout(float(args.timeout))
    world = client.load_world(args.town)
    carla_map = world.get_map()
    spawns = carla_map.get_spawn_points()
    candidates = scan_followstop_route_candidates(
        carla_map,
        spawns,
        front_target_ahead_m=float(args.front_target_ahead_m),
        goal_beyond_front_m=float(args.goal_beyond_front_m),
        max_ahead_m=float(args.max_ahead_m),
        max_lateral_m=float(args.max_lateral_m),
        max_heading_diff_deg=float(args.max_heading_diff_deg),
        max_front_target_error_m=float(args.max_front_target_error_m),
        limit=int(args.limit),
    )
    report = build_candidate_report(
        map_name=str(carla_map.name or args.town),
        candidates=candidates,
        front_target_ahead_m=float(args.front_target_ahead_m),
        goal_beyond_front_m=float(args.goal_beyond_front_m),
        source={
            "host": args.host,
            "port": int(args.port),
            "town": args.town,
            "spawn_count": len(spawns),
        },
    )
    paths = write_candidate_report(report, args.out)
    print(paths["json"])
    print(paths["summary"])
    return 0 if report["status"] in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
