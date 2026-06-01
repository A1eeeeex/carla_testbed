#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.experiments.natural_driving_readiness import (  # noqa: E402
    DEFAULT_APOLLO_CONTAINER,
    DEFAULT_APOLLO_CORE_DIR,
    DEFAULT_CARLA16_PYTHON,
    DEFAULT_CARLA_ROOT,
    DEFAULT_MAX_APOLLO_CORE_DUMP_GB,
    DEFAULT_MIN_DISK_FREE_GB,
    NaturalDrivingReadinessConfig,
    build_town01_natural_driving_readiness,
    write_readiness_plan_preview,
    write_readiness_report,
)
from carla_testbed.experiments.natural_driving_runner import (  # noqa: E402
    NaturalDrivingRunnerConfig,
    build_run_matrix,
    build_suite_manifest,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Check local readiness for the Town01 truth-input natural-driving suite. "
            "This does not start CARLA or Apollo."
        )
    )
    parser.add_argument(
        "--suite",
        type=Path,
        default=Path("configs/scenarios/town01_natural_driving_suite.yaml"),
        help="Town01 natural-driving suite YAML.",
    )
    parser.add_argument("--out", type=Path, required=True, help="Output directory for readiness artifacts.")
    parser.add_argument(
        "--strict-runtime",
        action="store_true",
        help="Treat missing/stopped local runtime prerequisites as blockers instead of warnings.",
    )
    parser.add_argument(
        "--python",
        "--python-exec",
        dest="python_exec",
        type=Path,
        default=DEFAULT_CARLA16_PYTHON if DEFAULT_CARLA16_PYTHON.exists() else Path(sys.executable),
    )
    parser.add_argument("--carla-root", type=Path, default=DEFAULT_CARLA_ROOT)
    parser.add_argument("--apollo-container", default=DEFAULT_APOLLO_CONTAINER)
    parser.add_argument("--apollo-core-dir", type=Path, default=DEFAULT_APOLLO_CORE_DIR)
    parser.add_argument("--min-disk-free-gb", type=float, default=DEFAULT_MIN_DISK_FREE_GB)
    parser.add_argument("--max-apollo-core-dump-gb", type=float, default=DEFAULT_MAX_APOLLO_CORE_DUMP_GB)
    parser.add_argument("--fixed-delta-seconds", type=float, default=0.05)
    parser.add_argument(
        "--config",
        default="configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
        help="Online route-health config that the suite runner will pass to run_town01_capability_online_chain.py.",
    )
    parser.add_argument(
        "--no-plan-preview",
        action="store_true",
        help="Do not write run_matrix.preview.csv and suite_manifest.preview.json.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = NaturalDrivingReadinessConfig(
        suite_path=args.suite,
        out_dir=args.out,
        strict_runtime=bool(args.strict_runtime),
        python_exec=args.python_exec,
        carla_root=args.carla_root,
        apollo_container=str(args.apollo_container),
        apollo_core_dir=args.apollo_core_dir,
        min_disk_free_gb=float(args.min_disk_free_gb),
        max_apollo_core_dump_gb=float(args.max_apollo_core_dump_gb),
        fixed_delta_seconds=float(args.fixed_delta_seconds),
        route_health_config=str(args.config),
    )
    report = build_town01_natural_driving_readiness(config)
    outputs = write_readiness_report(report, config.out_dir)
    if not args.no_plan_preview:
        runner_config = NaturalDrivingRunnerConfig(
            suite_path=config.suite_path,
            out_dir=config.out_dir / "planned_suite",
            dry_run=True,
            continue_on_failure=True,
            python_exec=str(config.python_exec),
            fixed_delta_seconds=float(config.fixed_delta_seconds),
            route_health_config=str(config.route_health_config),
        )
        matrix = build_run_matrix(runner_config)
        manifest = build_suite_manifest(runner_config, matrix)
        outputs.update(write_readiness_plan_preview(config, matrix, manifest))
    payload = {
        "status": report.get("status"),
        "blockers": report.get("blockers") or [],
        "warnings": report.get("warnings") or [],
        "outputs": outputs,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 1 if report.get("status") == "not_ready" else 0


if __name__ == "__main__":
    raise SystemExit(main())
