from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.record.route_curve_fields import ROUTE_CURVE_P0_FIELDS, ROUTE_CURVE_P1_FIELDS

ROUTE_CURVE_ARTIFACT_GAP_SCHEMA_VERSION = "route_curve_artifact_gap.v1"

P1_REQUIRED_SEMANTIC_FIELDS = (
    "apollo_trajectory_heading",
    "matched_point_distance",
    "target_point_distance",
)

P1_FIELD_ALIASES = {
    "apollo_trajectory_heading": (
        "apollo_trajectory_heading",
        "apollo_debug_simple_lat_target_point_theta_rad",
        "debug_simple_lat_target_point_theta",
        "debug_simple_lat_current_target_point_theta",
    ),
    "matched_point_distance": (
        "matched_point_distance",
        "apollo_matched_point_distance",
        "apollo_debug_simple_lon_matched_point_distance",
        "debug_simple_lon_matched_point_distance",
        "apollo_debug_simple_mpc_matched_point_distance",
        "debug_simple_mpc_matched_point_distance",
    ),
    "target_point_distance": (
        "target_point_distance",
        "apollo_target_point_distance",
        "apollo_debug_simple_lat_target_point_distance",
        "debug_simple_lat_target_point_distance",
    ),
    "apollo_trajectory_curvature": (
        "apollo_trajectory_curvature",
        "apollo_debug_simple_lat_target_point_kappa",
        "debug_simple_lat_target_point_kappa",
        "debug_simple_lat_current_target_point_kappa",
    ),
    "apollo_matched_point_index": ("apollo_matched_point_index", "matched_point_index"),
    "apollo_target_point_index": ("apollo_target_point_index", "target_point_index"),
}

P1_SUPPORTING_FIELD_ALIASES = {
    "apollo_target_point_s": (
        "apollo_target_point_s",
        "apollo_debug_simple_lat_target_point_s",
        "debug_simple_lat_target_point_s",
        "debug_simple_lat_current_target_point_s",
    ),
    "apollo_matched_point_s": (
        "apollo_matched_point_s",
        "apollo_debug_simple_lon_matched_point_s",
        "debug_simple_lon_matched_point_s",
        "debug_simple_lon_current_matched_point_s",
        "apollo_debug_simple_mpc_matched_point_s",
        "debug_simple_mpc_matched_point_s",
        "debug_simple_mpc_current_matched_point_s",
    ),
    "apollo_target_point_relative_time_sec": (
        "apollo_target_point_relative_time_sec",
        "apollo_debug_simple_lat_target_point_relative_time_sec",
        "debug_simple_lat_target_point_relative_time_sec",
        "debug_simple_lat_current_target_point_relative_time",
    ),
}

SUMMARY_SEMANTIC_KEYS = (
    "semantic_window_anchor_kind",
    "semantic_window_anchor_seq",
    "semantic_window_anchor_at",
    "first_high_steer_seq",
    "first_high_steer_at",
    "first_matched_point_too_large_seq",
    "first_matched_point_too_large_at",
    "high_steer_before_first_matched_point_too_large",
    "first_path_fallback_trigger_reason_family",
    "first_path_fallback_trigger_lon_diff",
    "first_relapse_after_recovery_reason_family",
    "first_relapse_after_recovery_lon_diff",
    "apollo_simple_lat_lateral_error_abs_p95",
    "apollo_simple_lat_heading_error_abs_p95",
    "apollo_simple_lat_target_point_kappa_abs_p95",
    "simple_lat_lateral_error_abs_p95_before_anchor",
    "simple_lat_heading_error_abs_p95_before_anchor",
    "target_point_kappa_abs_p95_before_anchor",
)


def analyze_route_curve_artifact_gap(
    timeseries_csv: str | Path | None,
    *,
    summary_json: str | Path | None = None,
) -> dict[str, Any]:
    """Check whether a run has the per-frame P1 fields needed for curve attribution.

    This is intentionally an artifact checker, not a behavior verdict. Summary-level
    semantics can guide the next probe, but they do not replace per-frame P1 fields.
    """
    rows, timeseries_error = _read_csv_rows(timeseries_csv)
    summary_payload = _read_json(summary_json)
    summary_semantics = _summary_semantics(summary_payload)
    p0_missing_columns = _missing_columns(rows, ROUTE_CURVE_P0_FIELDS)
    p1_presence = {
        field: _presence_for_aliases(rows, P1_FIELD_ALIASES.get(field, (field,)))
        for field in sorted(set(ROUTE_CURVE_P1_FIELDS) | set(P1_REQUIRED_SEMANTIC_FIELDS))
    }
    supporting_presence = {
        field: _presence_for_aliases(rows, aliases)
        for field, aliases in sorted(P1_SUPPORTING_FIELD_ALIASES.items())
    }
    missing_required_p1 = [
        field for field in P1_REQUIRED_SEMANTIC_FIELDS if not p1_presence[field]["has_value"]
    ]

    missing_inputs: list[str] = []
    warnings: list[str] = []
    if timeseries_csv in (None, ""):
        missing_inputs.append("timeseries")
    elif timeseries_error:
        missing_inputs.append("timeseries")
        warnings.append(timeseries_error)
    if summary_json not in (None, "") and not summary_payload:
        warnings.append("summary_json_unavailable_or_unparseable")
    if p0_missing_columns:
        warnings.append("route_curve_p0_columns_missing")
    if missing_required_p1 and summary_semantics:
        warnings.append("summary_semantics_available_but_per_frame_p1_missing")

    if missing_inputs:
        status = "insufficient_data"
        failure_reason = "missing_timeseries"
    elif missing_required_p1 and summary_semantics:
        status = "insufficient_data"
        failure_reason = "per_frame_p1_missing_summary_semantics_available"
    elif missing_required_p1:
        status = "insufficient_data"
        failure_reason = "per_frame_p1_missing"
    else:
        status = "pass"
        failure_reason = None

    return {
        "schema_version": ROUTE_CURVE_ARTIFACT_GAP_SCHEMA_VERSION,
        "status": status,
        "failure_reason": failure_reason,
        "source": {
            "timeseries_csv": None if timeseries_csv in (None, "") else str(Path(str(timeseries_csv))),
            "summary_json": None if summary_json in (None, "") else str(Path(str(summary_json))),
        },
        "artifact_scope": "route_curve_lateral_semantics",
        "row_count": len(rows),
        "run_id": _first_present(rows, "run_id"),
        "route_id": _first_present(rows, "route_id"),
        "per_frame_p1_complete": not missing_required_p1 and not missing_inputs,
        "required_p1_fields": list(P1_REQUIRED_SEMANTIC_FIELDS),
        "missing_p1_fields": missing_required_p1,
        "p1_field_presence": p1_presence,
        "p1_supporting_field_presence": supporting_presence,
        "p0_missing_columns": p0_missing_columns,
        "summary_semantics_available": bool(summary_semantics),
        "summary_semantics": summary_semantics,
        "missing_inputs": missing_inputs,
        "warnings": warnings,
        "required_next_fields": missing_required_p1,
        "required_next_artifacts": _required_next_artifacts(missing_inputs, missing_required_p1),
        "claim_supported": _claim_supported(status, missing_required_p1, summary_semantics),
        "claim_not_supported": (
            "This report does not prove curve health, does not promote carla_direct, "
            "and does not replace route_health or closed-loop A/B evidence."
        ),
    }


def write_route_curve_artifact_gap_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "route_curve_artifact_gap_report.json"
    summary_path = output_dir / "summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_route_curve_artifact_gap_markdown(report), encoding="utf-8")
    return {
        "route_curve_artifact_gap_report": str(report_path),
        "summary": str(summary_path),
    }


def render_route_curve_artifact_gap_markdown(report: Mapping[str, Any]) -> str:
    semantics = report.get("summary_semantics") if isinstance(report.get("summary_semantics"), Mapping) else {}
    return "\n".join(
        [
            "# Route-Curve Artifact Gap Summary",
            "",
            f"- status: `{report.get('status')}`",
            f"- failure_reason: `{report.get('failure_reason')}`",
            f"- route_id: `{report.get('route_id')}`",
            f"- row_count: `{report.get('row_count')}`",
            f"- per_frame_p1_complete: `{report.get('per_frame_p1_complete')}`",
            f"- missing_p1_fields: `{', '.join(report.get('missing_p1_fields') or []) or 'none'}`",
            f"- p0_missing_columns: `{', '.join(report.get('p0_missing_columns') or []) or 'none'}`",
            f"- summary_semantics_available: `{report.get('summary_semantics_available')}`",
            f"- summary_anchor: `{semantics.get('semantic_window_anchor_kind')}`",
            f"- first_matched_point_too_large_seq: `{semantics.get('first_matched_point_too_large_seq')}`",
            "",
            "Summary-level Apollo semantics can guide the next probe, but closed-loop curve attribution "
            "still requires per-frame P1 matched/target/trajectory fields.",
            "",
        ]
    )


def _read_csv_rows(path: str | Path | None) -> tuple[list[dict[str, Any]], str | None]:
    if path in (None, ""):
        return [], "timeseries_path_missing"
    csv_path = Path(str(path)).expanduser()
    if not csv_path.exists():
        return [], f"timeseries_not_found:{csv_path}"
    try:
        with csv_path.open(encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)], None
    except OSError as exc:
        return [], f"timeseries_read_error:{exc}"


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if path in (None, ""):
        return {}
    json_path = Path(str(path)).expanduser()
    if not json_path.exists():
        return {}
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _first_mapping_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload.get(key)
    for value in payload.values():
        if isinstance(value, Mapping) and key in value:
            return value.get(key)
    return None


def _summary_semantics(summary: Mapping[str, Any]) -> dict[str, Any]:
    if not summary:
        return {}
    result = {
        key: _first_mapping_value(summary, key)
        for key in SUMMARY_SEMANTIC_KEYS
        if _first_mapping_value(summary, key) not in (None, "")
    }
    if result:
        result["evidence_level"] = "summary_derived_not_per_frame_p1"
        result["claim_not_supported"] = (
            "summary-derived semantics do not replace per-frame matched/target/"
            "trajectory fields for closed-loop curve attribution"
        )
    return result


def _presence_for_aliases(rows: Sequence[Mapping[str, Any]], aliases: Sequence[str]) -> dict[str, Any]:
    observed_columns = sorted({alias for alias in aliases if any(alias in row for row in rows)})
    value_count = sum(
        1
        for row in rows
        if any(row.get(alias) not in (None, "") for alias in aliases)
    )
    return {
        "aliases": list(aliases),
        "observed_columns": observed_columns,
        "has_column": bool(observed_columns),
        "has_value": value_count > 0,
        "value_count": value_count,
    }


def _missing_columns(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> list[str]:
    if not rows:
        return list(fields)
    columns = set().union(*(set(row.keys()) for row in rows))
    return [field for field in fields if field not in columns]


def _first_present(rows: Sequence[Mapping[str, Any]], field: str) -> str | None:
    for row in rows:
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def _required_next_artifacts(missing_inputs: Sequence[str], missing_p1_fields: Sequence[str]) -> list[str]:
    artifacts: list[str] = []
    if "timeseries" in missing_inputs:
        artifacts.append("timeseries.csv or timeseries.jsonl")
    if missing_p1_fields:
        artifacts.append(
            "per-frame P1 lateral semantics in timeseries: "
            + ", ".join(missing_p1_fields)
        )
    return artifacts


def _claim_supported(
    status: str,
    missing_p1_fields: Sequence[str],
    summary_semantics: Mapping[str, Any],
) -> str:
    if status == "pass":
        return "per-frame P1 lateral semantics fields are present for downstream route/curve attribution"
    if missing_p1_fields and summary_semantics:
        return (
            "summary-level Apollo lateral semantics are available, but per-frame P1 "
            "fields are still missing; this supports an artifact-gap conclusion only"
        )
    if missing_p1_fields:
        return "per-frame P1 lateral semantics are missing; closed-loop curve attribution remains insufficient"
    return "artifact inputs are incomplete"
