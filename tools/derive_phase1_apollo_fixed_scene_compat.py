#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.phase1_apollo_fixed_scene_compat import (  # noqa: E402
    derive_apollo_fixed_scene_artifacts,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Derive Phase 1 fixed-scene artifacts from Apollo obstacle GT row evidence."
    )
    parser.add_argument("--run-dir", required=True, help="Run directory containing manifest and obstacle GT rows.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing derived fixed-scene artifacts.")
    args = parser.parse_args(argv)

    report = derive_apollo_fixed_scene_artifacts(args.run_dir, overwrite=args.overwrite)
    print(json.dumps({"status": report.get("status"), "report": report}, indent=2, sort_keys=True))
    return 0 if report.get("status") in {"pass", "warn", "not_applicable"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
