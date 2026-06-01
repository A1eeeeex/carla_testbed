#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.baguang_assist_reduction import (  # noqa: E402
    BaguangAssistReductionConfig,
    DEFAULT_CONFIG_PATH,
    execute_assist_reduction,
    parse_filter,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate or execute a Baguang Apollo/Autoware assist-reduction run matrix."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Write matrix only; do not start CARLA/Apollo/Autoware.")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--stacks", default=None, help="Comma-separated stack filter, e.g. apollo,autoware.")
    parser.add_argument("--profiles", default=None, help="Comma-separated profile_id filter.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = BaguangAssistReductionConfig(
        config_path=args.config,
        out_dir=args.out,
        dry_run=bool(args.dry_run),
        continue_on_failure=bool(args.continue_on_failure),
        selected_stacks=parse_filter(args.stacks),
        selected_profiles=parse_filter(args.profiles),
    )
    matrix, manifest = execute_assist_reduction(config)
    status_counts = manifest.get("status_counts") if isinstance(manifest.get("status_counts"), dict) else {}
    payload = {
        "dry_run": config.dry_run,
        "out": manifest["out_dir"],
        "matrix_rows": len(matrix),
        "status_counts": status_counts,
        "assist_reduction_manifest": str(Path(manifest["out_dir"]) / "assist_reduction_manifest.json"),
        "assist_reduction_matrix": str(Path(manifest["out_dir"]) / "assist_reduction_matrix.csv"),
        "analysis_command": manifest["analysis_commands"]["comparison_after_runs"],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if not config.continue_on_failure and int(status_counts.get("failed", 0)) > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
