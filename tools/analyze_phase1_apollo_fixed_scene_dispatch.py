#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.backends.registry import default_backend_registry  # noqa: E402
from carla_testbed.experiments.phase1_apollo_fixed_scene_dispatch import (  # noqa: E402
    analyze_apollo_fixed_scene_dispatch,
    write_apollo_fixed_scene_dispatch_report,
)
from carla_testbed.platform.compiler import compile_run_plan  # noqa: E402
from carla_testbed.platform.registry import PlatformRegistry  # noqa: E402
from carla_testbed.scenario_player.manifest_contract import (  # noqa: E402
    fixed_scene_manifest_fields_from_template_path,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze Phase 1 Apollo fixed-scene dispatch contract.")
    parser.add_argument("--scenario", required=True, help="Scenario profile or YAML path.")
    parser.add_argument("--algorithm", default="apollo/apollo10_carla_gt")
    parser.add_argument("--recording", default="none")
    parser.add_argument("--traffic", default="none")
    parser.add_argument("--gate", default="scenario_validation")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    plan = compile_run_plan(
        {
            "include": {
                "platform": "apollo_cyberrt",
                "algorithm": args.algorithm,
                "scenario": args.scenario,
                "recording": args.recording,
                "traffic": args.traffic,
                "gate": args.gate,
            }
        },
        registry=PlatformRegistry(repo_root=REPO_ROOT),
    )
    backend = default_backend_registry().for_plan(plan)
    launch = backend.build_launch_plan(plan).to_dict()
    scenario_path = plan.source_profiles.get("scenario")
    target_contract = {}
    if scenario_path:
        target_contract = fixed_scene_manifest_fields_from_template_path(scenario_path).get("target_actor_contract") or {}
    report = analyze_apollo_fixed_scene_dispatch(
        backend="apollo_cyberrt",
        backend_type="apollo_reference_backend",
        scenario_id=plan.scenario.scenario_id,
        scenario_class=plan.scenario.scenario_class,
        target_actor_contract=target_contract,
        launch_plan=launch,
    )
    paths = write_apollo_fixed_scene_dispatch_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "outputs": paths}, indent=2, sort_keys=True))
    return 0 if report.get("status") in {"pass", "warn", "not_applicable"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
