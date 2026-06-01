#!/usr/bin/env python3
"""Create a Lanelet2 OSM copy with explicit `speed_limit` tags."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.autoware.lanelet_speed import (  # noqa: E402
    analyze_lanelet_speed_metadata,
    write_lanelet_speed_limit_variant,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--osm", required=True, help="Input Lanelet2 OSM path")
    parser.add_argument("--out", required=True, help="Output OSM path")
    parser.add_argument(
        "--speed-limit",
        default="80",
        help="Lanelet2 `speed_limit` tag value to write, e.g. 80 or '22.22 m/s'",
    )
    parser.add_argument("--subtype", default="road", help="Lanelet subtype to patch")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing speed_limit tags")
    args = parser.parse_args(argv)

    patch_report = write_lanelet_speed_limit_variant(
        args.osm,
        args.out,
        speed_limit=args.speed_limit,
        subtype=args.subtype,
        overwrite=args.overwrite,
    )
    analysis_report = analyze_lanelet_speed_metadata(args.out)
    output = {"patch": patch_report, "analysis": analysis_report}
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if patch_report["status"] in {"pass", "warn"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
