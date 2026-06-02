#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.followstop_combo_matrix import (  # noqa: E402
    FollowStopComboConfig,
    estimate_followstop_combo_counts,
    load_followstop_combo_plan,
    write_followstop_combo_outputs,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the Follow-Stop Apollo/Autoware combination matrix. "
            "This is CI-safe and does not start CARLA, Apollo, or Autoware."
        )
    )
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path("configs/experiments/followstop_combo_matrix.yaml"),
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--include-transport-ab", action="store_true")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--python", "--python-exec", dest="python_exec", default=sys.executable)
    parser.add_argument("--runner-script", default="examples/run_followstop.py")
    parser.add_argument(
        "--start-carla",
        action="store_true",
        help="Include --start-carla in generated online run commands.",
    )
    parser.add_argument(
        "--carla-root",
        default=None,
        help="Explicit CARLA package/source root to pass to run_followstop.py.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = FollowStopComboConfig(
        plan_path=args.plan,
        out_dir=args.out,
        dry_run=True,
        include_transport_ab=bool(args.include_transport_ab),
        python_exec=str(args.python_exec),
        runner_script=str(args.runner_script),
        start_carla=bool(args.start_carla),
        carla_root=str(args.carla_root) if args.carla_root else None,
    )
    matrix, manifest = write_followstop_combo_outputs(config)
    plan = load_followstop_combo_plan(config.plan_path)
    payload = {
        "dry_run": True,
        "out": str(config.out_dir),
        "include_transport_ab": config.include_transport_ab,
        "start_carla": config.start_carla,
        "carla_root": config.carla_root,
        "matrix_rows": len(matrix),
        "expected_counts": estimate_followstop_combo_counts(
            plan,
            include_transport_ab=config.include_transport_ab,
        ),
        "manifest": str(config.out_dir / "followstop_combo_manifest.json"),
        "matrix": str(config.out_dir / "followstop_combo_matrix.csv"),
        "validation": manifest["validation"],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
