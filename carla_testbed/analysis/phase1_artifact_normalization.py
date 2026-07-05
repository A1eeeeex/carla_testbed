from __future__ import annotations

import csv
import json
import math
import shutil
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.artifact_completeness import (
    check_run_artifact_completeness,
    write_run_artifact_completeness_report,
)


PHASE1_ARTIFACT_NORMALIZATION_SCHEMA_VERSION = "phase1_artifact_normalization.v1"


def normalize_phase1_artifacts(run_dir: str | Path) -> dict[str, Any]:
    """Expose legacy nested artifacts on the Phase 1 canonical run surface.

    Some compatibility backends still write a legacy run directory inside the
    declared Phase 1 run directory. Phase 1 analyzers intentionally consume the
    declared run root, so a unique nested `timeseries.*` can be promoted there.
    This is evidence-surface normalization only; it does not rewrite behavior
    status or turn a timeout/failure into success.
    """

    root = Path(run_dir).expanduser()
    warnings: list[str] = []
    promoted_artifacts: list[dict[str, str]] = []
    status = "missing"
    try:
        if root.exists() and not root.is_dir():
            status = "error"
            source_artifacts = []
            warnings.append("run_dir_is_not_directory")
        else:
            root.mkdir(parents=True, exist_ok=True)
            timeseries = _promote_unique_timeseries(root)
            status = str(timeseries["status"])
            source_artifacts = list(timeseries["source_artifacts"])
            promoted_artifacts.extend(timeseries["promoted_artifacts"])
            warnings.extend(timeseries["warnings"])
            for spec in _OPTIONAL_PROMOTION_SPECS:
                optional = _promote_unique_relative_file(root, spec)
                promoted_artifacts.extend(optional["promoted_artifacts"])
                warnings.extend(optional["warnings"])
            control_overlay = _overlay_control_trace_fields(root)
            promoted_artifacts.extend(control_overlay["promoted_artifacts"])
            warnings.extend(control_overlay["warnings"])
    except Exception as exc:  # pragma: no cover - defensive artifact path
        status = "error"
        source_artifacts = []
        warnings.append(f"normalization_error:{type(exc).__name__}:{exc}")

    report = {
        "schema_version": PHASE1_ARTIFACT_NORMALIZATION_SCHEMA_VERSION,
        "run_dir": str(root),
        "status": status,
        "canonical_surface": "timeseries.*",
        "accepted_formats": ["timeseries.csv", "timeseries.jsonl"],
        "source_artifacts": source_artifacts,
        "promoted_artifacts": promoted_artifacts,
        "warnings": warnings,
        "claim_boundary": (
            "phase1_artifact_normalization_only_exposes_existing_legacy_outputs; "
            "it_does_not_change_runtime_behavior_or_mark_failures_successful"
        ),
    }
    if root.is_dir():
        try:
            _write_report(root, report)
        except Exception as exc:  # pragma: no cover - report write is secondary
            report["warnings"] = [*list(report.get("warnings") or []), f"report_write_error:{type(exc).__name__}:{exc}"]
    return report


def ensure_phase1_comparison_artifacts(run_dir: str | Path) -> dict[str, Any]:
    """Write minimal Phase 1 comparison artifacts after runtime attempts.

    This helper fills backend-neutral report surfaces that can be derived from
    existing run evidence. It never marks behavior successful; it only makes
    explicit whether the declared run root has enough Phase 1 artifacts for
    ScenarioComparison.
    """

    root = Path(run_dir).expanduser()
    warnings: list[str] = []
    outputs: dict[str, str] = {}
    if not root.is_dir():
        return {
            "schema_version": "phase1_comparison_artifact_surface.v1",
            "run_dir": str(root),
            "status": "error",
            "warnings": ["run_dir_is_not_directory"],
            "outputs": {},
        }

    route_v_t_gap = _ensure_route_only_v_t_gap(root)
    if route_v_t_gap.get("written"):
        outputs["v_t_gap_report"] = str(root / "analysis" / "v_t_gap" / "v_t_gap_report.json")
    warnings.extend(route_v_t_gap.get("warnings") or [])

    completeness = check_run_artifact_completeness(root, profile="phase1")
    canonical_outputs = write_run_artifact_completeness_report(
        completeness,
        root / "analysis" / "artifact_completeness",
    )
    outputs.update(canonical_outputs)
    legacy_path = root / "analysis" / "phase1_status" / "artifact_completeness.json"
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_payload = {
        "schema_version": "phase1_artifact_completeness.v1",
        "status": completeness.get("status"),
        "artifact_complete": completeness.get("artifact_complete"),
        "missing_artifacts": list(completeness.get("missing_artifacts") or []),
        "warnings": list(completeness.get("warnings") or []),
        "source_profile": "run_artifact_completeness_phase1",
        "source_artifact": canonical_outputs.get("artifact_completeness_report"),
        "claim_boundary": (
            "phase1 artifact completeness supports ScenarioComparison only; "
            "it does not prove backend behavior success or natural driving."
        ),
    }
    legacy_path.write_text(json.dumps(legacy_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    outputs["phase1_status_artifact_completeness"] = str(legacy_path)

    return {
        "schema_version": "phase1_comparison_artifact_surface.v1",
        "run_dir": str(root),
        "status": completeness.get("status"),
        "artifact_complete": completeness.get("artifact_complete"),
        "v_t_gap_status": route_v_t_gap.get("status"),
        "missing_artifacts": list(completeness.get("missing_artifacts") or []),
        "warnings": warnings,
        "outputs": outputs,
        "claim_boundary": (
            "derived comparison artifacts only expose existing run evidence; "
            "they do not change runtime behavior or mark failures successful."
        ),
    }


_OPTIONAL_PROMOTION_SPECS = [
    {
        "name": "events",
        "root_rel": Path("events.jsonl"),
        "patterns": ("**/events.jsonl",),
    },
    {
        "name": "config_resolved",
        "root_rel": Path("config.resolved.yaml"),
        "patterns": ("**/config.resolved.yaml",),
    },
    {
        "name": "route_json",
        "root_rel": Path("route.json"),
        "patterns": ("**/route.json",),
    },
    {
        "name": "control_apply_trace",
        "root_rel": Path("artifacts/control_apply_trace.jsonl"),
        "patterns": ("**/artifacts/control_apply_trace.jsonl",),
    },
    {
        "name": "bridge_health_summary",
        "root_rel": Path("artifacts/bridge_health_summary.json"),
        "patterns": ("**/artifacts/bridge_health_summary.json",),
    },
    {
        "name": "bridge_health_summary_finalized",
        "root_rel": Path("artifacts/bridge_health_summary.finalized.json"),
        "patterns": ("**/artifacts/bridge_health_summary.finalized.json",),
    },
    {
        "name": "cyber_bridge_stats",
        "root_rel": Path("artifacts/cyber_bridge_stats.json"),
        "patterns": ("**/artifacts/cyber_bridge_stats.json",),
    },
    {
        "name": "control_handoff_summary",
        "root_rel": Path("artifacts/control_handoff_summary.json"),
        "patterns": ("**/artifacts/control_handoff_summary.json",),
    },
    {
        "name": "command_materialization_summary",
        "root_rel": Path("artifacts/command_materialization_summary.json"),
        "patterns": ("**/artifacts/command_materialization_summary.json",),
    },
    {
        "name": "startup_geometry_summary",
        "root_rel": Path("artifacts/startup_geometry_summary.json"),
        "patterns": ("**/artifacts/startup_geometry_summary.json",),
    },
    {
        "name": "publish_gap_trace",
        "root_rel": Path("artifacts/publish_gap_trace.jsonl"),
        "patterns": ("**/artifacts/publish_gap_trace.jsonl",),
    },
    {
        "name": "topic_publish_stats",
        "root_rel": Path("artifacts/topic_publish_stats.jsonl"),
        "patterns": ("**/artifacts/topic_publish_stats.jsonl",),
    },
    {
        "name": "carla_tick_health",
        "root_rel": Path("artifacts/carla_tick_health.jsonl"),
        "patterns": ("**/artifacts/carla_tick_health.jsonl",),
    },
    {
        "name": "carla_tick_health_summary",
        "root_rel": Path("artifacts/carla_tick_health_summary.json"),
        "patterns": ("**/artifacts/carla_tick_health_summary.json",),
    },
    {
        "name": "apollo_control_raw",
        "root_rel": Path("artifacts/apollo_control_raw.jsonl"),
        "patterns": ("**/artifacts/apollo_control_raw.jsonl",),
    },
    {
        "name": "apollo_reference_line_contract_raw",
        "root_rel": Path("artifacts/apollo_reference_line_contract.jsonl"),
        "patterns": ("**/artifacts/apollo_reference_line_contract.jsonl",),
    },
    {
        "name": "planning_topic_debug",
        "root_rel": Path("artifacts/planning_topic_debug.jsonl"),
        "patterns": ("**/artifacts/planning_topic_debug.jsonl",),
    },
    {
        "name": "planning_topic_debug_summary",
        "root_rel": Path("artifacts/planning_topic_debug_summary.json"),
        "patterns": ("**/artifacts/planning_topic_debug_summary.json",),
    },
    {
        "name": "planning_route_segment_debug",
        "root_rel": Path("artifacts/planning_route_segment_debug.jsonl"),
        "patterns": ("**/artifacts/planning_route_segment_debug.jsonl",),
    },
    {
        "name": "apollo_route_segment_debug",
        "root_rel": Path("artifacts/apollo_route_segment_debug.jsonl"),
        "patterns": ("**/artifacts/apollo_route_segment_debug.jsonl",),
    },
    {
        "name": "apollo_planning_info_log",
        "root_rel": Path("artifacts/apollo_planning.INFO"),
        "patterns": ("**/artifacts/apollo_planning.INFO",),
    },
    {
        "name": "control_trajectory_consume_debug",
        "root_rel": Path("artifacts/control_trajectory_consume_debug.jsonl"),
        "patterns": ("**/artifacts/control_trajectory_consume_debug.jsonl",),
    },
    {
        "name": "planning_materialization_report",
        "root_rel": Path("analysis/planning_materialization/planning_materialization_report.json"),
        "patterns": ("**/analysis/planning_materialization/planning_materialization_report.json",),
    },
    {
        "name": "route_health_json",
        "root_rel": Path("analysis/route_health/route_health.json"),
        "patterns": ("**/analysis/route_health/route_health.json",),
    },
    {
        "name": "route_health_csv",
        "root_rel": Path("analysis/route_health/route_health.csv"),
        "patterns": ("**/analysis/route_health/route_health.csv",),
    },
    {
        "name": "route_health_curve_segments",
        "root_rel": Path("analysis/route_health/curve_segments.csv"),
        "patterns": ("**/analysis/route_health/curve_segments.csv",),
    },
    {
        "name": "route_health_summary",
        "root_rel": Path("analysis/route_health/route_health_summary.md"),
        "patterns": ("**/analysis/route_health/route_health_summary.md",),
    },
    {
        "name": "route_start_alignment_json",
        "root_rel": Path("analysis/route_start_alignment/route_start_alignment_report.json"),
        "patterns": ("**/analysis/route_start_alignment/route_start_alignment_report.json",),
    },
    {
        "name": "route_start_alignment_summary",
        "root_rel": Path("analysis/route_start_alignment/route_start_alignment_summary.md"),
        "patterns": ("**/analysis/route_start_alignment/route_start_alignment_summary.md",),
    },
    {
        "name": "apollo_reference_line_contract_report",
        "root_rel": Path("analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"),
        "patterns": ("**/analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",),
    },
    {
        "name": "apollo_reference_line_contract_summary",
        "root_rel": Path("analysis/apollo_reference_line_contract/apollo_reference_line_contract_summary.md"),
        "patterns": ("**/analysis/apollo_reference_line_contract/apollo_reference_line_contract_summary.md",),
    },
    {
        "name": "apollo_hdmap_projection_raw",
        "root_rel": Path("artifacts/apollo_hdmap_projection.jsonl"),
        "patterns": ("**/artifacts/apollo_hdmap_projection.jsonl",),
    },
    {
        "name": "apollo_hdmap_projection_report",
        "root_rel": Path("analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json"),
        "patterns": ("**/analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",),
    },
    {
        "name": "apollo_hdmap_projection_summary",
        "root_rel": Path("analysis/apollo_hdmap_projection/apollo_hdmap_projection_summary.md"),
        "patterns": ("**/analysis/apollo_hdmap_projection/apollo_hdmap_projection_summary.md",),
    },
    {
        "name": "ego_control_trace",
        "root_rel": Path("artifacts/ego_control_trace.jsonl"),
        "patterns": ("**/artifacts/ego_control_trace.jsonl",),
    },
]


def _promote_unique_timeseries(root: Path) -> dict[str, Any]:
    existing = _root_timeseries(root)
    if existing is not None:
        return {
            "status": "already_present",
            "source_artifacts": [str(existing)],
            "promoted_artifacts": [],
            "warnings": [],
        }
    candidates = _nested_timeseries_candidates(root)
    if len(candidates) == 1:
        source = candidates[0]
        destination = root / source.name
        shutil.copy2(source, destination)
        return {
            "status": "promoted",
            "source_artifacts": [str(source)],
            "promoted_artifacts": [{"name": "timeseries", "source": str(source), "destination": str(destination)}],
            "warnings": [],
        }
    if len(candidates) > 1:
        return {
            "status": "ambiguous",
            "source_artifacts": [str(path) for path in candidates],
            "promoted_artifacts": [],
            "warnings": ["multiple_nested_timeseries_candidates"],
        }
    return {
        "status": "missing",
        "source_artifacts": [],
        "promoted_artifacts": [],
        "warnings": ["timeseries_missing_from_root_and_nested_legacy_runs"],
    }


def _promote_unique_relative_file(root: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    destination = root / Path(spec["root_rel"])
    if destination.exists():
        return {"promoted_artifacts": [], "warnings": []}
    candidates = _nested_candidates_for_patterns(root, [str(pattern) for pattern in spec["patterns"]])
    if len(candidates) == 1:
        source = candidates[0]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return {
            "promoted_artifacts": [
                {"name": str(spec["name"]), "source": str(source), "destination": str(destination)}
            ],
            "warnings": [],
        }
    if len(candidates) > 1 and spec.get("name") == "config_resolved":
        preferred = _select_non_hidden_candidate(candidates)
        if preferred is not None:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(preferred, destination)
            return {
                "promoted_artifacts": [
                    {
                        "name": str(spec["name"]),
                        "source": str(preferred),
                        "destination": str(destination),
                    }
                ],
                "warnings": ["multiple_nested_config_resolved_candidates_selected_non_hidden"],
            }
    if len(candidates) > 1:
        return {"promoted_artifacts": [], "warnings": [f"multiple_nested_{spec['name']}_candidates"]}
    return {"promoted_artifacts": [], "warnings": []}


def _select_non_hidden_candidate(candidates: list[Path]) -> Path | None:
    visible = [path for path in candidates if not any(part.startswith(".") for part in path.parts)]
    return visible[0] if len(visible) == 1 else None


_CONTROL_TRACE_OVERLAY_FIELDS = {
    "apollo_steer_raw": ("apollo_steer_raw", "apollo_raw.steer"),
    "bridge_steer_mapped": (
        "bridge_mapped.mapped_carla_steer_cmd",
        "mapped_carla_steer_cmd",
        "bridge_steer_mapped",
        "bridge_mapped.steer",
    ),
    "bridge_steer_pre_policy": ("bridge_steer_pre_policy", "bridge_mapped.steer_pre_policy"),
    "carla_steer_applied": ("carla_steer_applied", "carla_applied.steer"),
    "throttle_raw": ("throttle_raw", "apollo_raw.throttle"),
    "throttle_mapped": (
        "bridge_mapped.mapped_throttle_cmd",
        "mapped_throttle_cmd",
        "throttle_mapped",
        "bridge_mapped.throttle",
    ),
    "throttle_applied": ("throttle_applied", "carla_applied.throttle"),
    "brake_raw": ("brake_raw", "apollo_raw.brake"),
    "brake_mapped": (
        "bridge_mapped.mapped_brake_cmd",
        "mapped_brake_cmd",
        "brake_mapped",
        "bridge_mapped.brake",
    ),
    "brake_applied": ("brake_applied", "carla_applied.brake"),
    "control_latency_ms": ("control_latency_ms",),
}


def _overlay_control_trace_fields(root: Path) -> dict[str, Any]:
    """Fill sparse root timeseries control fields from canonical control trace.

    Compatibility Apollo paths can expose a promoted root `timeseries.csv` whose
    control columns are blank or all-zero placeholders, while
    `artifacts/control_apply_trace.jsonl` contains the actual raw/mapped/applied
    control rows. This overlay only repairs the evidence surface. It does not
    change behavior status, summary success, or acceptance verdicts.
    """

    timeseries_path = root / "timeseries.csv"
    control_trace_path = root / "artifacts" / "control_apply_trace.jsonl"
    if not timeseries_path.exists() or not control_trace_path.exists():
        return {"promoted_artifacts": [], "warnings": []}

    rows = _read_csv_rows(timeseries_path)
    trace_rows = _read_jsonl_rows(control_trace_path)
    if not rows or not trace_rows:
        return {"promoted_artifacts": [], "warnings": []}

    time_key = "sim_time" if "sim_time" in rows[0] else "timestamp" if "timestamp" in rows[0] else None
    if time_key is None:
        return {"promoted_artifacts": [], "warnings": ["control_trace_overlay_skipped:no_timeseries_time_field"]}

    trace_points = _control_trace_points(trace_rows)
    if not trace_points:
        return {"promoted_artifacts": [], "warnings": ["control_trace_overlay_skipped:no_usable_control_rows"]}

    target_fields = [
        field
        for field in _CONTROL_TRACE_OVERLAY_FIELDS
        if _timeseries_field_needs_overlay(rows, field, trace_points)
    ]
    if not target_fields:
        return {"promoted_artifacts": [], "warnings": []}

    times = [_optional_float(row.get(time_key)) for row in rows]
    indexed_times = [(index, value) for index, value in enumerate(times) if value is not None]
    if not indexed_times:
        return {"promoted_artifacts": [], "warnings": ["control_trace_overlay_skipped:no_numeric_timeseries_time"]}

    tolerance_s = _time_join_tolerance_s([value for _, value in indexed_times])
    updates = 0
    updated_fields: set[str] = set()
    for point in trace_points:
        trace_time = point.get("time")
        if trace_time is None:
            continue
        nearest = min(indexed_times, key=lambda item: abs(item[1] - trace_time))
        row_index, row_time = nearest
        if abs(row_time - trace_time) > tolerance_s:
            continue
        row = rows[row_index]
        for field in target_fields:
            value = point.get(field)
            if value is None:
                continue
            previous = row.get(field)
            row[field] = _format_overlay_value(value)
            if row[field] != previous:
                updates += 1
                updated_fields.add(field)

    if updates <= 0:
        return {"promoted_artifacts": [], "warnings": ["control_trace_overlay_skipped:no_matching_timeseries_rows"]}

    fieldnames = list(rows[0].keys())
    for field in target_fields:
        if field not in fieldnames:
            fieldnames.append(field)
    with timeseries_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return {
        "promoted_artifacts": [
            {
                "name": "timeseries_control_trace_overlay",
                "source": str(control_trace_path),
                "destination": str(timeseries_path),
                "fields": ",".join(sorted(updated_fields)),
                "updated_cells": str(updates),
            }
        ],
        "warnings": ["root_timeseries_control_fields_overlaid_from_control_apply_trace"],
    }


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                if isinstance(payload, Mapping):
                    rows.append(dict(payload))
    except Exception:
        return []
    return rows


def _control_trace_points(rows: list[dict[str, Any]]) -> list[dict[str, float | None]]:
    points: list[dict[str, float | None]] = []
    for row in rows:
        point: dict[str, float | None] = {
            "time": _first_number(row, ("sim_time", "timestamp", "gt_state.sim_time_sec"))
        }
        for field, aliases in _CONTROL_TRACE_OVERLAY_FIELDS.items():
            point[field] = _first_number(row, aliases)
        if any(point.get(field) is not None for field in _CONTROL_TRACE_OVERLAY_FIELDS):
            points.append(point)
    return points


def _timeseries_field_needs_overlay(
    rows: list[dict[str, str]],
    field: str,
    trace_points: list[dict[str, float | None]],
) -> bool:
    trace_values = [point[field] for point in trace_points if point.get(field) is not None]
    if not trace_values:
        return False
    if not any(abs(float(value)) > 1e-12 for value in trace_values):
        return False
    if field not in rows[0]:
        return True
    values = [_optional_float(row.get(field)) for row in rows]
    present = [value for value in values if value is not None]
    if not present:
        return True
    return all(abs(value) <= 1e-12 for value in present)


def _time_join_tolerance_s(times: list[float]) -> float:
    ordered = sorted(set(times))
    deltas = [b - a for a, b in zip(ordered, ordered[1:]) if b > a]
    if not deltas:
        return 0.075
    median_delta = sorted(deltas)[len(deltas) // 2]
    return max(0.075, min(0.5, median_delta * 1.5))


def _first_number(row: Mapping[str, Any], aliases: tuple[str, ...]) -> float | None:
    for alias in aliases:
        value = _nested_get(row, alias)
        parsed = _optional_float(value)
        if parsed is not None:
            return parsed
    return None


def _nested_get(row: Mapping[str, Any], dotted: str) -> Any:
    current: Any = row
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _format_overlay_value(value: float) -> str:
    return f"{value:.12g}"


def _root_timeseries(root: Path) -> Path | None:
    for name in ("timeseries.csv", "timeseries.jsonl"):
        path = root / name
        if path.exists():
            return path
    return None


def _nested_timeseries_candidates(root: Path) -> list[Path]:
    return _nested_candidates_for_patterns(root, ["**/timeseries.csv", "**/timeseries.jsonl"])


def _nested_candidates_for_patterns(root: Path, patterns: list[str]) -> list[Path]:
    candidates: dict[Path, Path] = {}
    for pattern in patterns:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            if path.parent == root or path == root / path.name:
                continue
            if _inside_normalization_report(path):
                continue
            if "analysis" in path.parts and "phase1_artifact_normalization" in path.parts:
                continue
            try:
                resolved = path.resolve()
            except OSError:
                resolved = path
            candidates.setdefault(resolved, path)
    return sorted(candidates.values(), key=lambda item: (item.name, str(item)))


def _ensure_route_only_v_t_gap(root: Path) -> dict[str, Any]:
    path = root / "analysis" / "v_t_gap" / "v_t_gap_report.json"
    if path.exists():
        return {"status": "already_present", "written": False, "warnings": []}
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    phase1_status = _read_json(root / "analysis" / "phase1_status" / "phase1_status.json")
    if _phase1_target_required(manifest=manifest, summary=summary, phase1_status=phase1_status):
        return {"status": "target_required", "written": False, "warnings": []}
    scenario_id = (
        manifest.get("scenario_case")
        or manifest.get("scenario_id")
        or phase1_status.get("scenario_case")
        or phase1_status.get("scenario_id")
        or summary.get("scenario_id")
    )
    report = {
        "schema_version": "v_t_gap.v1",
        "status": "not_applicable",
        "run_id": manifest.get("run_id") or phase1_status.get("run_id") or root.name,
        "scenario_id": scenario_id,
        "scenario_case": scenario_id,
        "target_actor_required": False,
        "target_actor_contract": {
            "status": "not_required",
            "required": False,
            "source": "phase1_route_only_artifact_surface",
        },
        "rows": [],
        "missing_fields": [],
        "warnings": ["route_only_scenario_has_no_target_gap_metric"],
        "claim_boundary": "v_t_gap_not_applicable_for_route_only_phase1_run",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"status": "not_applicable", "written": True, "warnings": []}


def _phase1_target_required(
    *,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    phase1_status: Mapping[str, Any],
) -> bool:
    target_contract = phase1_status.get("target_actor_contract")
    if not isinstance(target_contract, Mapping):
        target_contract = manifest.get("target_actor_contract")
    if isinstance(target_contract, Mapping):
        if target_contract.get("required") is False:
            return False
        if target_contract.get("status") == "not_required":
            return False
        if target_contract.get("status") == "resolved":
            return True
    scenario = str(
        manifest.get("scenario_case")
        or manifest.get("scenario_class")
        or manifest.get("scenario_id")
        or phase1_status.get("scenario_case")
        or phase1_status.get("scenario_class")
        or phase1_status.get("scenario_id")
        or summary.get("scenario_class")
        or summary.get("scenario_id")
        or ""
    )
    if any(token in scenario for token in ("follow_stop", "lead_", "cut_in", "cut_out", "hard_brake")):
        return True
    return False


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _inside_normalization_report(path: Path) -> bool:
    return "phase1_artifact_normalization" in path.parts


def _write_report(root: Path, report: dict[str, Any]) -> None:
    out_dir = root / "analysis" / "phase1_artifact_normalization"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "phase1_artifact_normalization_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Phase 1 Artifact Normalization",
        "",
        f"Status: `{report.get('status')}`",
        "",
        f"Canonical surface: `{report.get('canonical_surface')}`",
        "",
        f"Claim boundary: `{report.get('claim_boundary')}`",
    ]
    promoted = report.get("promoted_artifacts")
    if isinstance(promoted, list) and promoted:
        lines.extend(["", "## Promoted Artifacts", ""])
        for item in promoted:
            if not isinstance(item, dict):
                continue
            lines.append(f"- `{item.get('source')}` -> `{item.get('destination')}`")
    warnings = report.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.extend(["", "## Warnings", ""])
        for warning in warnings:
            lines.append(f"- `{warning}`")
    (out_dir / "phase1_artifact_normalization_summary.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
