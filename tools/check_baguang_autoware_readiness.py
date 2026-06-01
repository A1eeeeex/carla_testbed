#!/usr/bin/env python3
"""Check local readiness for the Baguang Autoware follow-stop demo."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.autoware.baguang_readiness import (  # noqa: E402
    BaguangAutowareReadinessConfig,
    check_baguang_autoware_readiness,
    write_baguang_autoware_readiness_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="configs/scenarios/baguang_autoware_followstop_suite.yaml")
    parser.add_argument("--carla-root", default="/home/ubuntu/CARLA_0.9.16")
    parser.add_argument("--autoware-map-root", default="/home/ubuntu/autoware-contents/maps")
    parser.add_argument("--map", dest="map_name", default="straight_road_for_baguang")
    parser.add_argument("--out", default="runs/readiness/baguang_autoware_followstop")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = BaguangAutowareReadinessConfig(
        suite_path=Path(args.suite),
        carla_root=Path(args.carla_root),
        autoware_map_root=Path(args.autoware_map_root),
        map_name=args.map_name,
        out_dir=Path(args.out),
    )
    report = check_baguang_autoware_readiness(cfg)
    outputs = write_baguang_autoware_readiness_report(report, cfg.out_dir)
    print(
        "baguang_autoware_readiness "
        f"status={report['status']} blockers={len(report['blockers'])} warnings={len(report['warnings'])} "
        f"out={outputs['readiness_json']}"
    )
    return 0 if report["status"] in {"ready", "warn"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
