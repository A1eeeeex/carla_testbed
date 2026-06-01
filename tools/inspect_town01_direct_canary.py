#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.transport_ab import analyze_ab_manifest, check_ab_report_requirements, write_ab_report

DEFAULT_MARKER = Path("/tmp/town01_direct_stale_republish_097_canary_root.txt")


def _read_marker(path: Path) -> Path | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    return Path(text).expanduser() if text else None


def _filter_report_routes(report: Mapping[str, Any], route_filter: set[str]) -> dict[str, Any]:
    if not route_filter:
        return dict(report)
    comparisons = [
        comparison
        for comparison in (report.get("comparisons") or [])
        if str(comparison.get("route_id")) in route_filter
    ]
    run_ids = {
        str(value)
        for comparison in comparisons
        for value in (comparison.get("baseline_run_id"), comparison.get("candidate_run_id"))
        if value
    }
    run_results = [
        row
        for row in (report.get("run_results") or [])
        if str(row.get("run_id")) in run_ids or str(row.get("route_id")) in route_filter
    ]
    payload = dict(report)
    payload["routes"] = sorted(route_filter)
    payload["run_results"] = run_results
    payload["comparisons"] = comparisons
    return payload


def _key_rows(report: Mapping[str, Any]) -> dict[str, Any]:
    direct_rows = [
        {
            "run_id": row.get("run_id"),
            "route_id": row.get("route_id"),
            "run_status": row.get("run_status"),
            "return_code": row.get("return_code"),
            "actual_run_dir": row.get("actual_run_dir"),
            "artifact_complete": row.get("artifact_complete"),
            "direct_transport_contract_status": row.get("direct_transport_contract_status"),
            "direct_transport_contract_reasons": row.get("direct_transport_contract_reasons"),
            "direct_control_apply_mode": row.get("direct_control_apply_mode"),
            "direct_stale_world_frame_policy": row.get("direct_stale_world_frame_policy"),
            "direct_stale_world_frame_policy_source": row.get("direct_stale_world_frame_policy_source"),
            "bridge_loc_hz": row.get("bridge_loc_hz"),
            "bridge_chassis_hz": row.get("bridge_chassis_hz"),
            "failure_reason": row.get("failure_reason"),
            "missing_fields": row.get("missing_fields"),
        }
        for row in (report.get("run_results") or [])
        if row.get("backend") == "carla_direct"
    ]
    comparisons = [
        {
            "route_id": item.get("route_id"),
            "duration_s": item.get("duration_s"),
            "status": item.get("status"),
            "reasons": item.get("reasons"),
            "cadence_comparison": item.get("cadence_comparison"),
        }
        for item in (report.get("comparisons") or [])
    ]
    return {
        "direct_rows": direct_rows,
        "comparisons": comparisons,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect the latest Town01 direct canary A/B result.")
    parser.add_argument("--run-root", type=Path, help="Explicit A/B run root. Defaults to marker file.")
    parser.add_argument("--marker", type=Path, default=DEFAULT_MARKER, help="Marker file containing the run root.")
    parser.add_argument("--out", type=Path, help="Analysis output directory. Defaults to <run-root>/analysis.")
    parser.add_argument("--routes", default="", help="Optional comma-separated route ids to evaluate, e.g. lane097.")
    parser.add_argument("--require-hard-gate-pass", action="store_true")
    parser.add_argument("--require-steering-normalization-mode", default="legacy_double_percent")
    parser.add_argument("--require-direct-control-apply-mode", default="frame_flush_only")
    parser.add_argument("--require-direct-stale-world-frame-policy", default="always_republish")
    parser.add_argument("--require-direct-transport-contract-aligned", action="store_true", default=True)
    parser.add_argument("--require-direct-bridge-cadence-ratio-min", type=float, default=0.8)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_root = args.run_root or _read_marker(args.marker)
    if run_root is None:
        print(
            json.dumps(
                {
                    "status": "missing_run_root",
                    "marker": str(args.marker),
                    "message": "run root was not provided and marker file is missing or empty",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    run_root = run_root.expanduser()
    manifest = run_root / "ab_manifest.json"
    if not manifest.exists():
        print(
            json.dumps(
                {
                    "status": "missing_manifest",
                    "run_root": str(run_root),
                    "manifest": str(manifest),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    report = analyze_ab_manifest(manifest, batch_root=run_root)
    route_filter = {item.strip() for item in args.routes.split(",") if item.strip()}
    requirement_report = _filter_report_routes(report, route_filter)
    out_dir = args.out or (run_root / "analysis")
    outputs = write_ab_report(out_dir, report)
    requirement_check = check_ab_report_requirements(
        requirement_report,
        require_hard_gate_pass=bool(args.require_hard_gate_pass),
        required_steering_mode=args.require_steering_normalization_mode,
        required_direct_control_apply_mode=args.require_direct_control_apply_mode,
        required_direct_stale_world_frame_policy=args.require_direct_stale_world_frame_policy,
        require_direct_transport_contract_aligned=bool(args.require_direct_transport_contract_aligned),
        required_direct_bridge_cadence_ratio_min=args.require_direct_bridge_cadence_ratio_min,
    )
    payload = {
        "status": "passed" if requirement_check["passed"] else "failed",
        "run_root": str(run_root),
        "manifest": str(manifest),
        "outputs": outputs,
        "verdict": report.get("verdict"),
        "route_filter": sorted(route_filter),
        "requirement_check": requirement_check,
        **_key_rows(requirement_report),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if requirement_check["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
