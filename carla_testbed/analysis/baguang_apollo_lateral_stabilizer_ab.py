from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from .baguang_apollo_lateral_blocker import (
    DEFAULT_NO_LATERAL_RUN,
    analyze_baguang_apollo_lateral_blocker,
    write_baguang_apollo_lateral_blocker_report,
)

BAGUANG_APOLLO_LATERAL_STABILIZER_AB_SCHEMA_VERSION = "baguang_apollo_lateral_stabilizer_ab.v1"
DEFAULT_STABILIZER_RUN = Path(
    "runs/assist_reduction/baguang_online_20260531_apollo_no_terminal_hold/apollo/"
    "apollo_no_terminal_stop_hold/"
    "baguang_assist_reduction_20260531_130752__apollo_no_terminal_stop_hold__02"
)


def analyze_baguang_apollo_lateral_stabilizer_ab(
    *,
    no_lateral_run: str | Path = DEFAULT_NO_LATERAL_RUN,
    stabilizer_run: str | Path = DEFAULT_STABILIZER_RUN,
) -> dict[str, Any]:
    no_lateral = analyze_baguang_apollo_lateral_blocker(no_lateral_run)
    stabilizer = analyze_baguang_apollo_lateral_blocker(stabilizer_run)
    contrast = _contrast(no_lateral, stabilizer)
    conclusion = _conclusion(contrast)
    return {
        "schema_version": BAGUANG_APOLLO_LATERAL_STABILIZER_AB_SCHEMA_VERSION,
        "status": conclusion["status"],
        "reason": conclusion["reason"],
        "no_lateral_run": str(Path(no_lateral_run).expanduser()),
        "stabilizer_run": str(Path(stabilizer_run).expanduser()),
        "no_lateral": _compact_run(no_lateral),
        "stabilizer_enabled": _compact_run(stabilizer),
        "contrast": contrast,
        "findings": conclusion["findings"],
        "claim_boundary": {
            "proves_unassisted_apollo_lateral_capability": False,
            "proves_algorithm_limitation_without_map_reference_audit": False,
            "reason": (
                "The paired report shows stabilizer dependence and steering suppression, but it does not "
                "separate Apollo algorithm capability from map/reference-line/input-contract issues."
            ),
        },
    }


def write_baguang_apollo_lateral_stabilizer_ab_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "baguang_apollo_lateral_stabilizer_ab_report.json"
    md_path = out / "baguang_apollo_lateral_stabilizer_ab_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {"report": str(json_path), "summary": str(md_path)}


def _compact_run(report: Mapping[str, Any]) -> dict[str, Any]:
    lateral = _as_mapping(report.get("lateral_divergence"))
    control = _as_mapping(report.get("control_semantics"))
    runtime = _as_mapping(report.get("runtime_chain"))
    planning = _as_mapping(report.get("planning_semantics"))
    return {
        "run_dir": report.get("run_dir"),
        "status": report.get("status"),
        "reason": report.get("reason"),
        "summary_success": runtime.get("summary_success"),
        "exit_reason": runtime.get("exit_reason"),
        "collision_count": runtime.get("collision_count"),
        "routing_success_count": runtime.get("routing_success_count"),
        "planning_nonzero_messages": runtime.get("planning_nonzero_messages"),
        "control_tx_count": runtime.get("control_tx_count"),
        "direct_control_apply_count": runtime.get("direct_control_apply_count"),
        "stabilizer_enabled": _as_mapping(control.get("straight_lane_lateral_stabilizer")).get("enabled"),
        "stabilizer_apply_count": _as_mapping(control.get("straight_lane_lateral_stabilizer")).get("apply_count"),
        "max_abs_cross_track_error_m": _as_mapping(lateral.get("cross_track_error")).get("max_abs"),
        "max_abs_heading_error_rad": _as_mapping(lateral.get("heading_error")).get("max_abs"),
        "bridge_decode_raw_steer_max_abs": _as_mapping(control.get("bridge_decode_raw_steer")).get("max_abs"),
        "bridge_decode_commanded_steer_max_abs": _as_mapping(control.get("bridge_decode_commanded_steer")).get(
            "max_abs"
        ),
        "direct_apply_source_steer_max_abs": _as_mapping(control.get("direct_apply_source_steer")).get("max_abs"),
        "direct_apply_steer_max_abs": _as_mapping(control.get("direct_apply_steer")).get("max_abs"),
        "p0_timeseries_raw_steer_mismatch": control.get("p0_timeseries_raw_steer_mismatch"),
        "planning_status_top": planning.get("trajectory_header_status_top"),
    }


def _contrast(no_lateral_report: Mapping[str, Any], stabilizer_report: Mapping[str, Any]) -> dict[str, Any]:
    no_lat = _compact_run(no_lateral_report)
    with_stab = _compact_run(stabilizer_report)
    no_lat_cte = _num(no_lat.get("max_abs_cross_track_error_m"))
    stab_cte = _num(with_stab.get("max_abs_cross_track_error_m"))
    no_lat_applied = _num(no_lat.get("direct_apply_steer_max_abs"))
    stab_source = _num(with_stab.get("direct_apply_source_steer_max_abs"))
    stab_applied = _num(with_stab.get("direct_apply_steer_max_abs"))
    return {
        "cross_track_error_ratio_no_lateral_over_stabilized": _safe_ratio(no_lat_cte, stab_cte),
        "no_lateral_direct_source_to_applied_ratio": _safe_ratio(no_lat_applied, no_lat_applied),
        "stabilizer_source_to_applied_suppression_ratio": _safe_ratio(stab_source, stab_applied),
        "stabilizer_reduced_applied_steer": bool(stab_source > 0.01 and stab_applied < stab_source * 0.1),
        "no_lateral_applied_raw_directly": bool(no_lat_applied > 0.05),
        "stabilizer_kept_route": bool(stab_cte < 0.1),
        "no_lateral_diverged": bool(no_lat_cte > 5.0),
        "both_runs_had_nonempty_planning": bool(
            _num(no_lat.get("planning_nonzero_messages")) > 0 and _num(with_stab.get("planning_nonzero_messages")) > 0
        ),
        "both_runs_had_control_apply": bool(
            _num(no_lat.get("direct_control_apply_count")) > 0 and _num(with_stab.get("direct_control_apply_count")) > 0
        ),
    }


def _conclusion(contrast: Mapping[str, Any]) -> dict[str, Any]:
    findings: list[str] = []
    if contrast.get("both_runs_had_nonempty_planning") and contrast.get("both_runs_had_control_apply"):
        findings.append("paired_runs_materialized_planning_control_and_carla_apply")
    if contrast.get("no_lateral_diverged"):
        findings.append("no_lateral_run_diverged")
    if contrast.get("stabilizer_kept_route"):
        findings.append("stabilizer_enabled_run_stayed_on_route")
    if contrast.get("stabilizer_reduced_applied_steer"):
        findings.append("stabilizer_suppressed_direct_applied_steer")
    if contrast.get("no_lateral_applied_raw_directly"):
        findings.append("no_lateral_run_applied_large_direct_steer")
    status = "warn"
    reason = "stabilizer_dependence_observed"
    if (
        contrast.get("no_lateral_diverged")
        and contrast.get("stabilizer_kept_route")
        and contrast.get("stabilizer_reduced_applied_steer")
    ):
        reason = "stabilizer_suppresses_harmful_direct_steer_on_straight_route"
    elif not contrast.get("both_runs_had_nonempty_planning") or not contrast.get("both_runs_had_control_apply"):
        status = "insufficient_data"
        reason = "paired_runs_missing_planning_or_control_apply"
    return {"status": status, "reason": reason, "findings": findings}


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return numerator / denominator


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _summary_markdown(report: Mapping[str, Any]) -> str:
    contrast = _as_mapping(report.get("contrast"))
    no_lat = _as_mapping(report.get("no_lateral"))
    with_stab = _as_mapping(report.get("stabilizer_enabled"))
    lines = [
        "# Baguang Apollo Lateral Stabilizer A/B",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        f"- no_lateral_run: `{report.get('no_lateral_run')}`",
        f"- stabilizer_run: `{report.get('stabilizer_run')}`",
        f"- no_lateral_max_abs_cte_m: `{no_lat.get('max_abs_cross_track_error_m')}`",
        f"- stabilizer_max_abs_cte_m: `{with_stab.get('max_abs_cross_track_error_m')}`",
        f"- no_lateral_direct_apply_steer_max_abs: `{no_lat.get('direct_apply_steer_max_abs')}`",
        f"- stabilizer_source_steer_max_abs: `{with_stab.get('direct_apply_source_steer_max_abs')}`",
        f"- stabilizer_applied_steer_max_abs: `{with_stab.get('direct_apply_steer_max_abs')}`",
        f"- stabilizer_source_to_applied_suppression_ratio: `{contrast.get('stabilizer_source_to_applied_suppression_ratio')}`",
        "",
        "## Findings",
    ]
    lines.extend(f"- `{item}`" for item in report.get("findings") or [])
    lines.extend(
        [
            "",
            "## Claim Boundary",
            "",
            "This report shows dependence on the straight-lane stabilizer and steering suppression. It is not proof of unassisted Apollo lateral capability or a standalone Apollo algorithm limitation.",
            "",
        ]
    )
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare Apollo Baguang no-lateral and stabilizer-enabled runs.")
    parser.add_argument("--no-lateral-run", default=str(DEFAULT_NO_LATERAL_RUN))
    parser.add_argument("--stabilizer-run", default=str(DEFAULT_STABILIZER_RUN))
    parser.add_argument("--out", required=True)
    parser.add_argument("--write-child-reports", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = analyze_baguang_apollo_lateral_stabilizer_ab(
        no_lateral_run=args.no_lateral_run,
        stabilizer_run=args.stabilizer_run,
    )
    out = Path(args.out).expanduser()
    outputs = write_baguang_apollo_lateral_stabilizer_ab_report(report, out)
    if args.write_child_reports:
        child = out / "child_reports"
        write_baguang_apollo_lateral_blocker_report(
            analyze_baguang_apollo_lateral_blocker(args.no_lateral_run),
            child / "no_lateral",
        )
        write_baguang_apollo_lateral_blocker_report(
            analyze_baguang_apollo_lateral_blocker(args.stabilizer_run),
            child / "stabilizer_enabled",
        )
    print(json.dumps({"status": report.get("status"), "reason": report.get("reason"), "outputs": outputs}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
