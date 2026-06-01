#!/usr/bin/env python3
"""Check Autoware Lanelet2 speed metadata without starting Autoware."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.autoware.lanelet_speed import (  # noqa: E402
    analyze_lanelet_speed_metadata,
    write_lanelet_speed_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--osm", required=True, help="Lanelet2 OSM path")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--map-name", default=None)
    parser.add_argument("--requested-max-vel-mps", type=float, default=None)
    args = parser.parse_args(argv)

    report = analyze_lanelet_speed_metadata(
        args.osm,
        requested_max_vel_mps=args.requested_max_vel_mps,
        map_name=args.map_name,
    )
    paths = write_lanelet_speed_report(report, args.out)
    print(paths["json"])
    return 0 if report["status"] in {"pass", "warn"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
