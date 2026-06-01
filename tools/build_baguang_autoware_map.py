#!/usr/bin/env python3
"""Generate a minimal Autoware Lanelet2/PCD map for straight_road_for_baguang."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.autoware.baguang_map_builder import (  # noqa: E402
    build_baguang_autoware_map,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--xodr",
        default=(
            "/home/ubuntu/CARLA_0.9.16/CarlaUE4/Content/Carla/Maps/"
            "straight_road_for_baguang/OpenDrive/straight_road_for_baguang.xodr"
        ),
    )
    parser.add_argument("--out-root", default="/home/ubuntu/autoware-contents/maps")
    parser.add_argument("--map", dest="map_name", default="straight_road_for_baguang")
    parser.add_argument("--speed-limit-kph", type=float, default=80.0)
    parser.add_argument("--sample-step-m", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_baguang_autoware_map(
        xodr_path=args.xodr,
        out_root=args.out_root,
        map_name=args.map_name,
        speed_limit_kph=args.speed_limit_kph,
        sample_step_m=args.sample_step_m,
    )
    print(
        "baguang_autoware_map_build "
        f"status={report['status']} lanelets={report['lanelet_count']} "
        f"pcd_points={report['pointcloud_point_count']} report={report['report_path']}"
    )
    for warning in report.get("warnings") or []:
        print(f"warning: {warning}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
