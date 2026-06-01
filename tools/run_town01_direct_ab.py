#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.transport_ab import analyze_ab_manifest, check_ab_report_requirements, write_ab_report
from carla_testbed.experiments.ab_runner import (
    ABRunnerConfig,
    execute_matrix,
    load_experiment_config,
    parse_durations,
    parse_route_filter,
    write_dry_run_outputs,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run or dry-run Town01 long-window baseline/direct A/B matrix.")
    parser.add_argument("--experiment-config", type=Path, default=Path("configs/experiments/town01_direct_ab.yaml"))
    parser.add_argument("--route-config", type=Path, default=Path("configs/routes/town01/canonical_five.yaml"))
    parser.add_argument("--durations", default=None, help="Comma-separated seconds, e.g. 30,60,120.")
    parser.add_argument("--baseline", default=None)
    parser.add_argument("--candidate", default=None)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-diagnostic-curves", action="store_true")
    parser.add_argument(
        "--include-informational-routes",
        action="store_true",
        help="Include routes marked gate_role=informational, e.g. random regression pool routes.",
    )
    parser.add_argument("--carla-ignore-memory-preflight", action="store_true")
    parser.add_argument("--routes", default=None, help="Optional comma-separated route selectors, e.g. 097,217,031.")
    parser.add_argument("--analyze-after-run", action="store_true", help="Write ab_report.* after matrix execution.")
    parser.add_argument(
        "--require-hard-gate-pass",
        action="store_true",
        help="With --analyze-after-run, return non-zero unless lane097/lane217/junction031 all pass.",
    )
    parser.add_argument(
        "--require-steering-normalization-mode",
        help="With --analyze-after-run, return non-zero unless every run reports exactly this mode.",
    )
    parser.add_argument(
        "--require-direct-control-apply-mode",
        help="With --analyze-after-run, require this mode for every carla_direct run.",
    )
    parser.add_argument(
        "--require-direct-stale-world-frame-policy",
        help="With --analyze-after-run, require this stale-frame policy for every carla_direct run.",
    )
    parser.add_argument(
        "--require-direct-transport-contract-aligned",
        action="store_true",
        help="With --analyze-after-run, require every carla_direct run to have observed transport contract status aligned.",
    )
    parser.add_argument(
        "--require-direct-bridge-cadence-ratio-min",
        type=float,
        help="With --analyze-after-run, require direct bridge loc/chassis cadence ratio to be at least this value.",
    )
    parser.add_argument(
        "--require-route-curve-p1-complete",
        action="store_true",
        help="With --analyze-after-run, require curve diagnostic runs to have complete per-frame P1 route-curve evidence.",
    )
    return parser


def _build_config(args: argparse.Namespace) -> ABRunnerConfig:
    exp = load_experiment_config(args.experiment_config) if args.experiment_config.exists() else {}
    durations = parse_durations(args.durations or exp.get("durations_s") or "30,60,120")
    output_root = Path(args.out or exp.get("output_root") or "runs/ab/town01_direct_ab_dryrun")
    return ABRunnerConfig(
        route_config=Path(args.route_config or exp.get("route_config") or "configs/routes/town01/canonical_five.yaml"),
        durations_s=durations,
        baseline_backend=str(args.baseline or exp.get("baseline_backend") or "ros2_gt"),
        candidate_backend=str(args.candidate or exp.get("candidate_backend") or "carla_direct"),
        out_dir=output_root,
        fixed_delta_seconds=float(exp.get("fixed_delta_seconds") or 0.05),
        include_diagnostic_curves=bool(args.include_diagnostic_curves),
        include_informational_routes=bool(args.include_informational_routes),
        selected_routes=parse_route_filter(args.routes),
        continue_on_failure=bool(args.continue_on_failure),
        dry_run=bool(args.dry_run),
        carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
        required_steering_normalization_mode=str(
            exp.get("required_steering_normalization_mode") or "legacy_double_percent"
        ),
        required_direct_control_apply_mode=str(
            exp.get("required_direct_control_apply_mode") or "frame_flush_only"
        ),
        required_direct_stale_world_frame_policy=str(
            exp.get("required_direct_stale_world_frame_policy") or "always_republish"
        ),
        required_direct_bridge_cadence_ratio_min=float(
            exp.get("required_direct_bridge_cadence_ratio_min") or 0.8
        ),
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _build_config(args)
    if config.dry_run:
        matrix, manifest = write_dry_run_outputs(config)
    else:
        matrix, manifest = execute_matrix(config)
    report_payload = None
    required_direct_control_apply_mode = (
        args.require_direct_control_apply_mode if args.require_direct_control_apply_mode is not None else None
    )
    required_direct_stale_world_frame_policy = (
        args.require_direct_stale_world_frame_policy
        if args.require_direct_stale_world_frame_policy is not None
        else None
    )
    required_direct_bridge_cadence_ratio_min = args.require_direct_bridge_cadence_ratio_min
    if (
        args.analyze_after_run
        or args.require_hard_gate_pass
        or args.require_steering_normalization_mode
        or required_direct_control_apply_mode
        or required_direct_stale_world_frame_policy
        or args.require_direct_transport_contract_aligned
        or required_direct_bridge_cadence_ratio_min is not None
        or args.require_route_curve_p1_complete
    ):
        report = analyze_ab_manifest(config.out_dir / "ab_manifest.json", batch_root=config.out_dir)
        outputs = write_ab_report(config.out_dir / "analysis", report)
        requirement_check = check_ab_report_requirements(
            report,
            require_hard_gate_pass=bool(args.require_hard_gate_pass),
            required_steering_mode=args.require_steering_normalization_mode,
            required_direct_control_apply_mode=required_direct_control_apply_mode,
            required_direct_stale_world_frame_policy=required_direct_stale_world_frame_policy,
            require_direct_transport_contract_aligned=bool(args.require_direct_transport_contract_aligned),
            required_direct_bridge_cadence_ratio_min=required_direct_bridge_cadence_ratio_min,
            require_route_curve_p1_complete=bool(args.require_route_curve_p1_complete),
        )
        report_payload = {
            "outputs": outputs,
            "verdict": report.get("verdict"),
            "requirement_check": requirement_check,
        }
    payload = {
        "dry_run": config.dry_run,
        "out": str(config.out_dir),
        "matrix_rows": len(matrix),
        "manifest_status": manifest.consistency_status,
        "ab_manifest": str(config.out_dir / "ab_manifest.json"),
        "ab_matrix": str(config.out_dir / "ab_matrix.csv"),
        "analysis_command": manifest.analysis_commands.get("standard", ""),
        "strict_gate_command": manifest.analysis_commands.get("strict_hard_gate_steering_norm", ""),
    }
    if report_payload is not None:
        payload["analysis"] = report_payload
    print(json.dumps(payload, indent=2, sort_keys=True))
    if report_payload is not None and not report_payload["requirement_check"]["passed"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
