#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_CARLA16_PYTHON = Path("/home/ubuntu/miniconda3/envs/carla16/bin/python3")

from carla_testbed.experiments.natural_driving_runner import (  # noqa: E402
    NaturalDrivingRunnerConfig,
    STRICT_POSTPROCESS_FAIL_STATUSES,
    audit_suite_goal_outputs,
    execute_suite,
    goal_audit_command,
    parse_class_filter,
    parse_scenario_filter,
    postprocess_and_audit_command,
    postprocess_suite_outputs,
    read_run_matrix_csv,
    summarize_suite_coverage,
)


def _default_online_python() -> str:
    return str(DEFAULT_CARLA16_PYTHON if DEFAULT_CARLA16_PYTHON.exists() else Path(sys.executable))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run or dry-run the Town01 natural-driving suite matrix.")
    parser.add_argument(
        "--suite",
        type=Path,
        default=Path("configs/scenarios/town01_natural_driving_suite.yaml"),
        help="Natural-driving suite YAML.",
    )
    parser.add_argument("--out", type=Path, required=True, help="Output root for suite_manifest.json and run_matrix.csv.")
    parser.add_argument("--dry-run", action="store_true", help="Do not start CARLA/Apollo; write matrix only.")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--classes", help="Comma-separated scenario_class filter.")
    parser.add_argument(
        "--scenarios",
        help="Comma-separated scenario_id filter for targeted canaries.",
    )
    parser.add_argument(
        "--config",
        default="configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
        help="Route-health config forwarded to run_town01_capability_online_chain.py.",
    )
    parser.add_argument("--startup-profile", default="render_offscreen_no_ros2")
    parser.add_argument(
        "--python",
        "--python-exec",
        dest="python_exec",
        default=_default_online_python(),
        help=(
            "Python executable used for generated online commands. Defaults to the local "
            "conda carla16 Python when present."
        ),
    )
    parser.add_argument("--fixed-delta-seconds", type=float, default=0.05)
    parser.add_argument("--post-fail-steps", type=int, default=120)
    parser.add_argument("--carla-world-ready-timeout-sec", type=float, default=180.0)
    parser.add_argument("--carla-launch-attempts", type=int, default=1)
    parser.add_argument("--progress-update-sec", type=float, default=5.0)
    parser.add_argument("--carla-ignore-memory-preflight", action="store_true")
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        help=(
            "Additional run_town01_capability_online_chain.py --override entries forwarded "
            "to every scenario. Use for explicit probe-only runtime overrides."
        ),
    )
    parser.add_argument(
        "--postprocess-after-run",
        action="store_true",
        help="Run offline evidence postprocess after the suite commands finish.",
    )
    parser.add_argument(
        "--postprocess-existing",
        action="store_true",
        help="Do not run commands; postprocess an existing --out suite root.",
    )
    parser.add_argument("--refresh-postprocess", action="store_true")
    parser.add_argument(
        "--fail-on-postprocess-status",
        default="",
        help=(
            "Comma-separated postprocess natural-driving statuses that should make this runner "
            "return non-zero when --postprocess-after-run or --postprocess-existing is used. "
            "Example: fail,insufficient_data."
        ),
    )
    parser.add_argument(
        "--require-full-target-coverage",
        action="store_true",
        help=(
            "When postprocessing, return non-zero unless every target natural-driving "
            "scenario class has at least one passing run."
        ),
    )
    parser.add_argument(
        "--audit-after-postprocess",
        action="store_true",
        help="After postprocess, write analysis/goal_audit/town01_goal_audit.json/md.",
    )
    parser.add_argument("--ab-root", type=Path, default=Path("runs/ab"))
    parser.add_argument("--calibration-root", type=Path, default=Path("runs"))
    parser.add_argument("--demo-root", type=Path, default=Path("runs"))
    parser.add_argument("--cadence-ratio-min", type=float, default=0.8)
    parser.add_argument(
        "--no-refresh-ab-from-manifest",
        action="store_true",
        help="Use stored A/B report JSON as-is when building the final goal audit.",
    )
    parser.add_argument(
        "--fail-on-audit-status",
        default="",
        help="Comma-separated goal-audit statuses that should make this runner return non-zero.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = NaturalDrivingRunnerConfig(
        suite_path=args.suite,
        out_dir=args.out,
        dry_run=bool(args.dry_run),
        continue_on_failure=bool(args.continue_on_failure),
        scenario_classes=parse_class_filter(args.classes),
        scenario_ids=parse_scenario_filter(args.scenarios),
        python_exec=str(args.python_exec),
        route_health_config=str(args.config),
        startup_profile=str(args.startup_profile),
        fixed_delta_seconds=float(args.fixed_delta_seconds),
        post_fail_steps=int(args.post_fail_steps),
        carla_world_ready_timeout_sec=float(args.carla_world_ready_timeout_sec),
        carla_launch_attempts=int(args.carla_launch_attempts),
        progress_update_sec=float(args.progress_update_sec),
        carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
        runner_overrides=tuple(str(item) for item in (args.override or [])),
    )
    postprocess_report = None
    if args.postprocess_existing:
        postprocess_report = postprocess_suite_outputs(
            config,
            require_full_target_coverage=True if args.require_full_target_coverage else None,
            refresh=bool(args.refresh_postprocess),
        )
        matrix = read_run_matrix_csv(config.out_dir / "run_matrix.csv")
        manifest = {
            "analysis_commands": {
                "natural_driving": (
                    f"{config.python_exec} tools/analyze_town01_natural_driving.py "
                    f"--suite-root {config.out_dir} --out {config.out_dir / 'analysis'}"
                ),
                "postprocess": (
                    f"{config.python_exec} tools/postprocess_town01_natural_driving.py "
                    f"--suite-root {config.out_dir} --out {config.out_dir / 'analysis' / 'natural_driving'}"
                ),
                "postprocess_strict": (
                    f"{config.python_exec} tools/postprocess_town01_natural_driving.py "
                    f"--suite-root {config.out_dir} --out {config.out_dir / 'analysis' / 'natural_driving'} "
                    f"--require-full-target-coverage --fail-on-status {STRICT_POSTPROCESS_FAIL_STATUSES}"
                ),
                "goal_audit_strict": goal_audit_command(config),
                "postprocess_and_audit_strict": postprocess_and_audit_command(config),
            }
        }
    else:
        matrix, manifest = execute_suite(config)
        if args.postprocess_after_run and not config.dry_run:
            postprocess_report = postprocess_suite_outputs(
                config,
                require_full_target_coverage=True if args.require_full_target_coverage else None,
                refresh=bool(args.refresh_postprocess),
            )
    if args.audit_after_postprocess and postprocess_report is None:
        postprocess_report = postprocess_suite_outputs(
            config,
            require_full_target_coverage=True if args.require_full_target_coverage else None,
            refresh=bool(args.refresh_postprocess),
        )
    goal_audit_report = None
    if args.audit_after_postprocess:
        goal_audit_report = audit_suite_goal_outputs(
            config,
            ab_root=args.ab_root,
            calibration_root=args.calibration_root,
            demo_root=args.demo_root,
            cadence_ratio_min=float(args.cadence_ratio_min),
            refresh_ab_from_manifest=not args.no_refresh_ab_from_manifest,
        )
    payload = {
        "dry_run": config.dry_run,
        "out": str(config.out_dir),
        "matrix_rows": len(matrix),
        "coverage": manifest.get("coverage") or summarize_suite_coverage(matrix),
        "suite_manifest": str(config.out_dir / "suite_manifest.json"),
        "run_matrix": str(config.out_dir / "run_matrix.csv"),
        "analysis_command": manifest["analysis_commands"]["natural_driving"],
        "postprocess_command": manifest["analysis_commands"]["postprocess"],
        "postprocess_strict_command": manifest["analysis_commands"]["postprocess_strict"],
        "goal_audit_strict_command": manifest["analysis_commands"]["goal_audit_strict"],
        "postprocess_and_audit_strict_command": manifest["analysis_commands"]["postprocess_and_audit_strict"],
    }
    exit_code = 0
    if postprocess_report is not None:
        natural = postprocess_report.get("natural_driving") if isinstance(postprocess_report.get("natural_driving"), dict) else {}
        verdict = natural.get("verdict") if isinstance(natural.get("verdict"), dict) else {}
        postprocess_status = natural.get("status")
        payload["postprocess_status"] = postprocess_status
        payload["postprocess_outputs"] = postprocess_report.get("outputs")
        payload["artifact_index_pre_refresh"] = postprocess_report.get("artifact_index_pre_refresh")
        payload["artifact_index_refresh"] = postprocess_report.get("artifact_index_refresh")
        payload["suite_manifest_postprocess_update"] = postprocess_report.get(
            "suite_manifest_postprocess_update"
        )
        payload["postprocess_summary"] = natural.get("summary") or {}
        coverage = natural.get("capability_coverage") if isinstance(natural.get("capability_coverage"), dict) else {}
        coverage_passed = bool(
            verdict.get("can_claim_full_natural_driving") is True
            and coverage.get("can_claim_full_natural_driving") is True
        )
        payload["coverage_check"] = {
            "required": bool(args.require_full_target_coverage),
            "passed": coverage_passed,
            "suite_plan_missing": verdict.get("suite_plan_missing") or [],
            "missing_required_scenario_classes": coverage.get("missing_required_scenario_classes") or [],
            "unproven_required_scenario_classes": coverage.get("unproven_required_scenario_classes") or [],
            "missing_required_scenario_ids": coverage.get("missing_required_scenario_ids") or [],
            "unproven_required_scenario_ids": coverage.get("unproven_required_scenario_ids") or [],
            "scenario_identity_mismatches": coverage.get("scenario_identity_mismatches") or [],
        }
        payload["problem_run_count"] = int(natural.get("problem_run_count") or 0)
        payload["problem_runs"] = natural.get("problem_runs") or []
        payload["failed_runs"] = verdict.get("failed_runs") or []
        payload["warning_runs"] = verdict.get("warning_runs") or []
        payload["insufficient_data_runs"] = verdict.get("insufficient_data_runs") or []
        fail_statuses = {
            item.strip()
            for item in str(args.fail_on_postprocess_status).split(",")
            if item.strip()
        }
        if postprocess_status in fail_statuses:
            exit_code = 2
        if args.require_full_target_coverage and not coverage_passed:
            exit_code = 2
    if goal_audit_report is not None:
        payload["goal_audit_status"] = goal_audit_report.get("status")
        payload["goal_audit_outputs"] = goal_audit_report.get("outputs")
        audit = goal_audit_report.get("audit") if isinstance(goal_audit_report.get("audit"), dict) else {}
        payload["goal_audit_missing_evidence"] = audit.get("missing_evidence") or []
        payload["goal_audit_next_actions"] = audit.get("next_actions") or []
        audit_fail_statuses = {
            item.strip()
            for item in str(args.fail_on_audit_status).split(",")
            if item.strip()
        }
        if goal_audit_report.get("status") in audit_fail_statuses:
            exit_code = 3
    print(json.dumps(payload, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
