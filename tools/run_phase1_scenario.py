#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.phase1_scenario_scaffold import (  # noqa: E402
    default_algorithm_for_backend,
    write_phase1_scenario_scaffold,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create a Phase 1 ScenarioRun scaffold; unavailable runtime is invalid/backend_not_ready."
    )
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--backend", required=True, choices=("apollo_cyberrt", "carla_builtin", "autoware_ros2"))
    parser.add_argument("--algorithm", help="Algorithm profile. Defaults from --backend.")
    parser.add_argument("--record", "--recording", dest="recording", default="none")
    parser.add_argument("--gate", default="scenario_validation")
    parser.add_argument("--traffic", default="none")
    parser.add_argument("--bridge-config", help="Optional Apollo bridge config for fixed-scene readiness preflight.")
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)

    result = write_phase1_scenario_scaffold(
        scenario=args.scenario,
        backend=args.backend,
        algorithm=args.algorithm or default_algorithm_for_backend(args.backend),
        recording=args.recording,
        gate=args.gate,
        traffic=args.traffic,
        bridge_config=args.bridge_config,
        run_dir=args.run_dir,
        repo_root=REPO_ROOT,
    )
    exit_code = int(result.pop("exit_code"))
    print(json.dumps(result, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
