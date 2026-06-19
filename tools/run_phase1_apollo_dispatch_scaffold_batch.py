#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.phase1_apollo_dispatch_scaffold_batch import (  # noqa: E402
    DEFAULT_DYNAMIC_APOLLO_FIXED_SCENE_SCENARIOS,
    write_apollo_dispatch_scaffold_batch,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write CI-safe Apollo fixed-scene dispatch scaffolds for Phase 1 ScenarioCases."
    )
    parser.add_argument(
        "--scenario",
        action="append",
        help="Scenario profile/name/path. Repeatable. Defaults to remaining dynamic Apollo fixed-scene cases.",
    )
    parser.add_argument("--out", required=True, help="Output directory for batch scaffold run dirs.")
    parser.add_argument(
        "--bridge-config",
        default="configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml",
    )
    parser.add_argument("--algorithm", default="apollo/apollo10_carla_gt")
    parser.add_argument("--recording", default="none")
    parser.add_argument("--gate", default="scenario_validation")
    parser.add_argument("--traffic", default="none")
    args = parser.parse_args(argv)

    report = write_apollo_dispatch_scaffold_batch(
        out_dir=args.out,
        scenarios=args.scenario or DEFAULT_DYNAMIC_APOLLO_FIXED_SCENE_SCENARIOS,
        repo_root=REPO_ROOT,
        bridge_config=args.bridge_config,
        algorithm=args.algorithm,
        recording=args.recording,
        gate=args.gate,
        traffic=args.traffic,
    )
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "outputs": report.get("outputs"),
                "row_count": len(report.get("rows") or []),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
