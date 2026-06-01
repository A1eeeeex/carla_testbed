#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.natural_driving_postprocess import (  # noqa: E402
    DEFAULT_CHANNEL_HEALTH_CONFIG,
    DEFAULT_TOWN01_APOLLO_CONTRACT,
    DEFAULT_TRAFFIC_LIGHT_MAPPING,
    postprocess_natural_driving_runs,
)
from carla_testbed.experiments.natural_driving_runner import (  # noqa: E402
    persist_postprocess_evidence_status,
    refresh_artifact_index_for_root,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline postprocess for Town01 truth-input natural-driving run artifacts."
    )
    parser.add_argument("--suite-root", required=True, type=Path, help="Run dir or suite root containing run dirs.")
    parser.add_argument("--out", type=Path, help="Output directory for postprocess and natural-driving reports.")
    parser.add_argument(
        "--channel-config",
        type=Path,
        default=DEFAULT_CHANNEL_HEALTH_CONFIG,
        help="Apollo natural-driving channel health config.",
    )
    parser.add_argument(
        "--town01-apollo-contract",
        type=Path,
        default=DEFAULT_TOWN01_APOLLO_CONTRACT,
        help="Town01 Apollo route/signal contract config used for traffic-light contract reports.",
    )
    parser.add_argument(
        "--traffic-light-mapping",
        type=Path,
        default=DEFAULT_TRAFFIC_LIGHT_MAPPING,
        help="CARLA traffic-light to Apollo signal mapping config.",
    )
    parser.add_argument("--refresh", action="store_true", help="Regenerate existing derived reports.")
    parser.add_argument(
        "--fail-on-status",
        default="",
        help=(
            "Comma-separated natural-driving statuses that should return non-zero. "
            "Example: fail,warn,insufficient_data. By default the tool only reports status."
        ),
    )
    parser.add_argument(
        "--require-full-target-coverage",
        action="store_true",
        help=(
            "Return non-zero unless the natural-driving report proves every target "
            "capability class has at least one passing run."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifact_index_pre_refresh = refresh_artifact_index_for_root(args.suite_root)
    report = postprocess_natural_driving_runs(
        args.suite_root,
        out_dir=args.out,
        channel_config=args.channel_config,
        town01_contract_config=args.town01_apollo_contract,
        traffic_light_mapping_config=args.traffic_light_mapping,
        require_full_target_coverage=True if args.require_full_target_coverage else None,
        refresh=args.refresh,
    )
    artifact_index_refresh = refresh_artifact_index_for_root(args.suite_root)
    suite_manifest_postprocess_update = persist_postprocess_evidence_status(
        args.suite_root,
        report,
        require_full_target_coverage=True if args.require_full_target_coverage else None,
    )
    natural = report.get("natural_driving") if isinstance(report.get("natural_driving"), dict) else {}
    verdict = natural.get("verdict") if isinstance(natural.get("verdict"), dict) else {}
    summary = natural.get("summary") if isinstance(natural.get("summary"), dict) else {}
    coverage = natural.get("capability_coverage") if isinstance(natural.get("capability_coverage"), dict) else {}
    problem_runs = [item for item in natural.get("problem_runs") or [] if isinstance(item, dict)]
    coverage_passed = bool(
        verdict.get("can_claim_full_natural_driving") is True
        and coverage.get("can_claim_full_natural_driving") is True
    )
    payload = {
        "status": natural.get("status"),
        "run_count": report.get("run_count"),
        "summary": summary,
        "verdict": verdict,
        "coverage_check": {
            "required": bool(args.require_full_target_coverage),
            "passed": coverage_passed,
            "suite_plan_missing": verdict.get("suite_plan_missing") or [],
            "missing_required_scenario_classes": coverage.get("missing_required_scenario_classes") or [],
            "unproven_required_scenario_classes": coverage.get("unproven_required_scenario_classes") or [],
            "missing_required_scenario_ids": coverage.get("missing_required_scenario_ids") or [],
            "unproven_required_scenario_ids": coverage.get("unproven_required_scenario_ids") or [],
            "scenario_identity_mismatches": coverage.get("scenario_identity_mismatches") or [],
        },
        "outputs": report.get("outputs"),
        "artifact_index_pre_refresh": artifact_index_pre_refresh,
        "artifact_index_refresh": artifact_index_refresh,
        "suite_manifest_postprocess_update": suite_manifest_postprocess_update,
        "problem_run_count": len(problem_runs),
        "problem_runs": problem_runs,
        "failed_runs": verdict.get("failed_runs") or [],
        "warning_runs": verdict.get("warning_runs") or [],
        "insufficient_data_runs": [
            run.get("run_id")
            for run in problem_runs
            if run.get("verdict") == "insufficient_data"
        ],
        "artifact_incomplete_runs": [
            run.get("run_id")
            for run in report.get("runs") or []
            if (run.get("artifact_completeness") or {}).get("status") == "insufficient_data"
        ],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.require_full_target_coverage and not coverage_passed:
        return 3
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    if payload["status"] in fail_statuses:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
