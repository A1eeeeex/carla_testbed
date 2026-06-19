#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.phase1_apollo_fixed_scene_readiness import (  # noqa: E402
    analyze_apollo_fixed_scene_readiness,
    write_apollo_fixed_scene_readiness_report,
)
from carla_testbed.scenario_player.manifest_contract import (  # noqa: E402
    fixed_scene_manifest_fields_from_template_path,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze Apollo fixed-scene target obstacle-row readiness.")
    parser.add_argument("--scenario", required=True, help="Scenario YAML with fixed_scene/target_actor contract.")
    parser.add_argument("--bridge-config", help="Apollo bridge YAML used or planned for the online run.")
    parser.add_argument("--backend", default="apollo_cyberrt")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    scenario = Path(args.scenario).expanduser()
    if not scenario.is_absolute():
        scenario = REPO_ROOT / scenario
    fields = fixed_scene_manifest_fields_from_template_path(scenario)
    report = analyze_apollo_fixed_scene_readiness(
        backend=args.backend,
        target_actor_contract=fields.get("target_actor_contract"),
        bridge_config_path=args.bridge_config,
        repo_root=REPO_ROOT,
    )
    paths = write_apollo_fixed_scene_readiness_report(report, args.out)
    print(json.dumps({"status": report.get("status"), "outputs": paths}, indent=2, sort_keys=True))
    return 0 if report.get("status") in {"pass", "warn", "not_applicable"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
