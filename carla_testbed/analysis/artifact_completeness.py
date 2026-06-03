from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.algorithms.variant import AlgorithmVariantError, load_algorithm_variant
from carla_testbed.record.route_curve_fields import ROUTE_CURVE_P0_FIELDS

RUN_ARTIFACT_COMPLETENESS_SCHEMA_VERSION = "run_artifact_completeness.v1"
TRUTH_INPUT_CLOSED_LOOP_VARIANT_TYPES = {"ported_carla_gt", "tuned_town01"}

TRAFFIC_LIGHT_SCENARIO_CLASSES = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
CURVE_DIAGNOSTIC_SCENARIO_CLASSES = {
    "curve_diagnostic",
}
TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS = {
    "traffic_light_red_stop": "red_stop",
    "traffic_light_green_go": "green_go",
    "traffic_light_red_to_green_release": "red_to_green_release",
}

CONTROL_TRACE_REQUIRED_FIELDS = [
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "throttle_raw",
    "throttle_mapped",
    "throttle_applied",
    "brake_raw",
    "brake_mapped",
    "brake_applied",
    "lateral_guard_applied",
    "trajectory_contract_guard_applied",
]

CONTROL_TRACE_P0_FIELDS = [
    field for field in CONTROL_TRACE_REQUIRED_FIELDS if field in ROUTE_CURVE_P0_FIELDS
]
REQUIRED_MANIFEST_FIELDS = [
    "scenario_id",
    "scenario_class",
    "route_id",
    "algorithm_variant_id",
    "algorithm_variant_manifest_path",
    "online_config_path",
    "online_config_profile_name",
    "map",
    "transport_mode",
    "transport_mode_source",
    "backend",
    "truth_input",
    "duration_s",
    "fixed_delta_seconds",
    "ticks",
]


def check_run_artifact_completeness(
    run_dir: str | Path,
    *,
    scenario_class: str | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    inferred_scenario_class = scenario_class or _scenario_class(summary, manifest, root.name)
    run_id = _text_field(summary, manifest, "run_id") or root.name
    scenario_id = _text_field(summary, manifest, "scenario_id")
    route_id = _route_id(summary, manifest)

    config_path = _find_first(root, ["config.resolved.yaml", "effective_config.yaml"])
    timeseries_path = _find_first(root, ["timeseries.csv", "timeseries.jsonl"])
    route_health_path = _find_first(
        root,
        [
            "analysis/route_health/route_health.json",
            "route_health.json",
        ],
    )
    route_health_csv_path = _find_first(
        root,
        [
            "analysis/route_health/route_health.csv",
            "route_health.csv",
        ],
    )
    curve_segments_path = _find_first(
        root,
        [
            "analysis/route_health/curve_segments.csv",
            "curve_segments.csv",
        ],
    )
    route_health_summary_path = _find_first(
        root,
        [
            "analysis/route_health/route_health_summary.md",
            "route_health_summary.md",
        ],
    )
    channel_health_path = _find_first(
        root,
        [
            "analysis/apollo_channel_health/apollo_channel_health_report.json",
            "apollo_channel_health_report.json",
        ],
    )
    localization_contract_path = _find_first(
        root,
        [
            "analysis/localization_contract/localization_contract_report.json",
            "localization_contract_report.json",
        ],
    )
    apollo_reference_line_contract_path = _find_first(
        root,
        [
            "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
            "apollo_reference_line_contract_report.json",
        ],
    )
    control_health_path = _find_first(
        root,
        [
            "analysis/control_health/control_health_report.json",
            "control_health_report.json",
        ],
    )
    failure_timeline_path = _find_first(
        root,
        [
            "analysis/failure_timeline/failure_timeline_report.json",
            "failure_timeline_report.json",
        ],
    )
    route_start_alignment_path = _find_first(
        root,
        [
            "analysis/route_start_alignment/route_start_alignment_report.json",
            "route_start_alignment_report.json",
        ],
    )
    route_curve_artifact_gap_path = _find_first(
        root,
        [
            "analysis/route_curve_artifact_gap/route_curve_artifact_gap_report.json",
            "route_curve_artifact_gap_report.json",
        ],
    )
    traffic_light_contract_path = _find_first(
        root,
        [
            "analysis/traffic_light/traffic_light_contract_report.json",
            "traffic_light_contract_report.json",
        ],
    )
    traffic_light_behavior_path = _find_first(
        root,
        [
            "analysis/traffic_light/traffic_light_behavior_report.json",
            "traffic_light_behavior_report.json",
        ],
    )

    artifacts = {
        "manifest": str(root / "manifest.json") if (root / "manifest.json").exists() else None,
        "summary": str(root / "summary.json") if (root / "summary.json").exists() else None,
        "config_resolved": str(config_path) if config_path else None,
        "events": str(root / "events.jsonl") if (root / "events.jsonl").exists() else None,
        "timeseries": str(timeseries_path) if timeseries_path else None,
        "route_health": str(route_health_path) if route_health_path else None,
        "route_health_csv": str(route_health_csv_path) if route_health_csv_path else None,
        "curve_segments": str(curve_segments_path) if curve_segments_path else None,
        "route_health_summary": str(route_health_summary_path) if route_health_summary_path else None,
        "apollo_channel_health": str(channel_health_path) if channel_health_path else None,
        "localization_contract": str(localization_contract_path) if localization_contract_path else None,
        "apollo_reference_line_contract": (
            str(apollo_reference_line_contract_path) if apollo_reference_line_contract_path else None
        ),
        "control_health": str(control_health_path) if control_health_path else None,
        "failure_timeline": str(failure_timeline_path) if failure_timeline_path else None,
        "route_start_alignment": str(route_start_alignment_path) if route_start_alignment_path else None,
        "route_curve_artifact_gap": str(route_curve_artifact_gap_path)
        if route_curve_artifact_gap_path
        else None,
        "traffic_light_contract": str(traffic_light_contract_path) if traffic_light_contract_path else None,
        "traffic_light_behavior": str(traffic_light_behavior_path) if traffic_light_behavior_path else None,
    }
    missing_artifacts = _missing_artifacts(
        artifacts,
        scenario_class=inferred_scenario_class,
    )
    timeseries_rows = _read_timeseries_rows(timeseries_path)
    missing_control_trace = _missing_control_trace_fields(timeseries_rows)
    missing_manifest_fields = _missing_manifest_fields(
        manifest,
        scenario_class=inferred_scenario_class,
    )
    invalid_manifest_source_fields = _invalid_manifest_source_fields(manifest, run_dir=root)
    invalid_report_source_fields = _invalid_report_source_fields(
        run_dir=root,
        scenario_class=inferred_scenario_class,
        route_id=route_id,
        route_health_path=route_health_path,
        channel_health_path=channel_health_path,
        localization_contract_path=localization_contract_path,
        apollo_reference_line_contract_path=apollo_reference_line_contract_path,
        control_health_path=control_health_path,
        failure_timeline_path=failure_timeline_path,
        route_start_alignment_path=route_start_alignment_path,
        traffic_light_contract_path=traffic_light_contract_path,
        traffic_light_behavior_path=traffic_light_behavior_path,
        route_curve_artifact_gap_path=route_curve_artifact_gap_path,
    )
    status = "pass"
    if (
        missing_artifacts
        or missing_control_trace
        or missing_manifest_fields
        or invalid_manifest_source_fields
        or invalid_report_source_fields
    ):
        status = "insufficient_data"
    return {
        "schema_version": RUN_ARTIFACT_COMPLETENESS_SCHEMA_VERSION,
        "run_dir": str(root),
        "run_id": run_id,
        "scenario_id": scenario_id,
        "route_id": route_id,
        "scenario_class": inferred_scenario_class,
        "status": status,
        "artifact_complete": not missing_artifacts
        and not missing_control_trace
        and not missing_manifest_fields
        and not invalid_manifest_source_fields
        and not invalid_report_source_fields,
        "artifacts": artifacts,
        "missing_artifacts": missing_artifacts,
        "missing_manifest_fields": missing_manifest_fields,
        "invalid_manifest_source_fields": invalid_manifest_source_fields,
        "invalid_report_source_fields": invalid_report_source_fields,
        "control_trace_available": not missing_control_trace,
        "missing_control_trace_fields": missing_control_trace,
        "warnings": [],
    }


def write_run_artifact_completeness_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "artifact_completeness_report.json"
    md_path = output / "artifact_completeness_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_artifact_completeness_markdown(report), encoding="utf-8")
    return {
        "artifact_completeness_report": str(json_path),
        "artifact_completeness_summary": str(md_path),
    }


def _artifact_completeness_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Run Artifact Completeness",
        "",
        f"- run_id: `{report.get('run_id')}`",
        f"- scenario_id: `{report.get('scenario_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- status: `{report.get('status')}`",
        f"- scenario_class: `{report.get('scenario_class')}`",
        f"- artifact_complete: `{report.get('artifact_complete')}`",
        f"- control_trace_available: `{report.get('control_trace_available')}`",
        "",
        "## Missing Artifacts",
        "",
    ]
    missing_artifacts = [str(item) for item in report.get("missing_artifacts") or []]
    if missing_artifacts:
        lines.extend(f"- `{item}`" for item in missing_artifacts)
    else:
        lines.append("- none")
    lines.extend(["", "## Missing Manifest Fields", ""])
    missing_manifest = [str(item) for item in report.get("missing_manifest_fields") or []]
    if missing_manifest:
        lines.extend(f"- `{item}`" for item in missing_manifest)
    else:
        lines.append("- none")
    lines.extend(["", "## Invalid Manifest Source Fields", ""])
    invalid_manifest_sources = [str(item) for item in report.get("invalid_manifest_source_fields") or []]
    if invalid_manifest_sources:
        lines.extend(f"- `{item}`" for item in invalid_manifest_sources)
    else:
        lines.append("- none")
    lines.extend(["", "## Missing Control Trace Fields", ""])
    missing_control = [str(item) for item in report.get("missing_control_trace_fields") or []]
    if missing_control:
        lines.extend(f"- `{item}`" for item in missing_control)
    else:
        lines.append("- none")
    lines.extend(["", "## Invalid Report Source Fields", ""])
    invalid_sources = [str(item) for item in report.get("invalid_report_source_fields") or []]
    if invalid_sources:
        lines.extend(f"- `{item}`" for item in invalid_sources)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "This report checks artifact presence, required diagnostic fields, and report source "
            "traceability only. It does not prove Apollo behavior correctness.",
            "",
        ]
    )
    return "\n".join(lines)


def _missing_artifacts(artifacts: Mapping[str, Any], *, scenario_class: str | None) -> list[str]:
    missing: list[str] = []
    required = [
        ("manifest", "manifest.json"),
        ("summary", "summary.json"),
        ("config_resolved", "config.resolved.yaml"),
        ("events", "events.jsonl"),
        ("timeseries", "timeseries.csv"),
        ("route_health", "route_health.json"),
        ("route_health_csv", "route_health.csv"),
        ("curve_segments", "curve_segments.csv"),
        ("route_health_summary", "route_health_summary.md"),
        ("apollo_channel_health", "apollo_channel_health_report.json"),
        ("localization_contract", "localization_contract_report.json"),
        ("apollo_reference_line_contract", "apollo_reference_line_contract_report.json"),
        ("control_health", "control_health_report.json"),
        ("failure_timeline", "failure_timeline_report.json"),
        ("route_start_alignment", "route_start_alignment_report.json"),
    ]
    for key, name in required:
        if not artifacts.get(key):
            missing.append(name)
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and not artifacts.get("traffic_light_contract"):
        missing.append("traffic_light_contract_report.json")
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and not artifacts.get("traffic_light_behavior"):
        missing.append("traffic_light_behavior_report.json")
    if scenario_class in CURVE_DIAGNOSTIC_SCENARIO_CLASSES and not artifacts.get("route_curve_artifact_gap"):
        missing.append("route_curve_artifact_gap_report.json")
    return missing


def _invalid_report_source_fields(
    *,
    run_dir: Path,
    scenario_class: str | None,
    route_id: str,
    route_health_path: Path | None,
    channel_health_path: Path | None,
    localization_contract_path: Path | None,
    apollo_reference_line_contract_path: Path | None,
    control_health_path: Path | None,
    failure_timeline_path: Path | None,
    route_start_alignment_path: Path | None,
    traffic_light_contract_path: Path | None,
    traffic_light_behavior_path: Path | None,
    route_curve_artifact_gap_path: Path | None,
) -> list[str]:
    invalid: list[str] = []
    invalid.extend(
        _invalid_route_health_source_fields(
            route_health_path,
            run_dir=run_dir,
            route_id=route_id,
        )
    )
    invalid.extend(
        _invalid_report_context_fields(
            channel_health_path,
            report_name="apollo_channel_health",
            scenario_class=scenario_class,
        )
    )
    invalid.extend(
        _invalid_source_fields(
            channel_health_path,
            run_dir=run_dir,
            report_name="apollo_channel_health",
            required_source_fields=("config_path", "stats_path"),
        )
    )
    invalid.extend(
        _invalid_source_fields(
            localization_contract_path,
            run_dir=run_dir,
            report_name="localization_contract",
            required_source_fields=("timeseries_path", "channel_stats_path", "route_health_path"),
        )
    )
    invalid.extend(
        _invalid_source_fields(
            apollo_reference_line_contract_path,
            run_dir=run_dir,
            report_name="apollo_reference_line_contract",
            required_source_fields=("run_dir",),
        )
    )
    invalid.extend(
        _invalid_report_context_fields(
            control_health_path,
            report_name="control_health",
            scenario_class=scenario_class,
            route_id=route_id,
        )
    )
    invalid.extend(
        _invalid_source_fields(
            control_health_path,
            run_dir=run_dir,
            report_name="control_health",
            required_source_fields=("summary_path", "manifest_path", "timeseries_path"),
        )
    )
    invalid.extend(
        _invalid_report_context_fields(
            failure_timeline_path,
            report_name="failure_timeline",
            scenario_class=scenario_class,
            route_id=route_id,
        )
    )
    invalid.extend(
        _invalid_source_fields(
            failure_timeline_path,
            run_dir=run_dir,
            report_name="failure_timeline",
            required_source_fields=(
                "manifest_path",
                "summary_path",
                "events_path",
                "timeseries_path",
                "route_health_path",
                "control_health_path",
            ),
        )
    )
    invalid.extend(
        _invalid_report_context_fields(
            route_start_alignment_path,
            report_name="route_start_alignment",
            scenario_class=scenario_class,
            route_id=route_id,
        )
    )
    invalid.extend(
        _invalid_source_fields(
            route_start_alignment_path,
            run_dir=run_dir,
            report_name="route_start_alignment",
            required_source_fields=(
                "manifest_path",
                "summary_path",
                "timeseries_path",
                "failure_timeline_path",
            ),
        )
    )
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        invalid.extend(
            _invalid_report_context_fields(
                traffic_light_contract_path,
                report_name="traffic_light_contract",
                scenario_class=scenario_class,
            )
        )
        invalid.extend(
            _invalid_source_fields(
                traffic_light_contract_path,
                run_dir=run_dir,
                report_name="traffic_light_contract",
                required_source_fields=(
                    "town01_contract_path",
                    "traffic_light_mapping_path",
                ),
            )
        )
        invalid.extend(
            _invalid_report_context_fields(
                traffic_light_behavior_path,
                report_name="traffic_light_behavior",
                scenario_class=scenario_class,
                route_id=route_id,
            )
        )
        invalid.extend(
            _invalid_source_fields(
                traffic_light_behavior_path,
                run_dir=run_dir,
                report_name="traffic_light_behavior",
                required_source_fields=(
                    "summary_path",
                    "manifest_path",
                    "timeseries_path",
                    "events_path",
                    "traffic_light_contract_path",
                ),
            )
        )
    if scenario_class in CURVE_DIAGNOSTIC_SCENARIO_CLASSES:
        invalid.extend(
            _invalid_source_fields(
                route_curve_artifact_gap_path,
                run_dir=run_dir,
                report_name="route_curve_artifact_gap",
                required_source_fields=("timeseries_csv", "summary_json"),
            )
        )
    return sorted(set(invalid))


def _invalid_route_health_source_fields(
    path: Path | None,
    *,
    run_dir: Path,
    route_id: str,
) -> list[str]:
    report_name = "route_health"
    report = _read_json(path) if path is not None else {}
    invalid: list[str] = []
    actual_route_id = str(report.get("route_id") or "").strip()
    if path is not None and route_id:
        if not actual_route_id or actual_route_id != route_id:
            invalid.append(f"{report_name}.route_id")
    source = report.get("source")
    if not isinstance(source, Mapping):
        if path is not None:
            invalid.append(f"{report_name}.source")
        return invalid
    for field in ("manifest_path", "summary_path", "timeseries_path"):
        invalid.extend(
            _source_field_issue(
                source,
                field,
                run_dir=run_dir,
                report_path=path,
                report_name=report_name,
            )
        )
    route_path = source.get("route_path")
    reconstructed = "route reconstructed from timeseries P0 route_curve fields" in {
        str(item) for item in (report.get("warnings") or [])
    }
    if route_path in {None, ""}:
        if not reconstructed:
            invalid.append(f"{report_name}.source.route_path")
    else:
        invalid.extend(
            _source_field_issue(
                source,
                "route_path",
                run_dir=run_dir,
                report_path=path,
                report_name=report_name,
            )
        )
    return invalid


def _invalid_report_context_fields(
    path: Path | None,
    *,
    report_name: str,
    scenario_class: str | None = None,
    route_id: str = "",
) -> list[str]:
    if path is None:
        return []
    report = _read_json(path)
    invalid: list[str] = []
    expected_scenario = str(scenario_class or "").strip()
    if expected_scenario:
        actual_scenario = str(report.get("scenario_class") or "").strip()
        if not actual_scenario or actual_scenario != expected_scenario:
            invalid.append(f"{report_name}.scenario_class")
    expected_route = str(route_id or "").strip()
    if expected_route:
        actual_route = str(report.get("route_id") or "").strip()
        if not actual_route or actual_route != expected_route:
            invalid.append(f"{report_name}.route_id")
    return invalid


def _invalid_source_fields(
    path: Path | None,
    *,
    run_dir: Path,
    report_name: str,
    required_source_fields: Sequence[str],
) -> list[str]:
    if path is None:
        return []
    report = _read_json(path)
    source = report.get("source")
    if not isinstance(source, Mapping):
        return [f"{report_name}.source"]
    invalid: list[str] = []
    for field in required_source_fields:
        invalid.extend(
            _source_field_issue(
                source,
                field,
                run_dir=run_dir,
                report_path=path,
                report_name=report_name,
            )
        )
    return invalid


def _source_field_issue(
    source: Mapping[str, Any],
    field: str,
    *,
    run_dir: Path,
    report_path: Path | None,
    report_name: str,
) -> list[str]:
    raw = source.get(field)
    path_name = f"{report_name}.source.{field}"
    if raw in {None, ""}:
        return [path_name]
    if not _source_path_exists(str(raw), run_dir=run_dir, report_path=report_path):
        return [path_name]
    return []


def _source_path_exists(raw: str, *, run_dir: Path, report_path: Path | None) -> bool:
    path = Path(raw).expanduser()
    candidates = [path]
    if not path.is_absolute():
        candidates.extend(
            [
                run_dir / path,
                Path.cwd() / path,
            ]
        )
        if report_path is not None:
            candidates.append(report_path.parent / path)
    return any(candidate.exists() for candidate in candidates)


def _resolve_source_path(raw: str, *, run_dir: Path, report_path: Path | None = None) -> Path | None:
    path = Path(raw).expanduser()
    candidates = [path]
    if not path.is_absolute():
        candidates.extend([run_dir / path, Path.cwd() / path])
        if report_path is not None:
            candidates.append(report_path.parent / path)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _invalid_manifest_source_fields(manifest: Mapping[str, Any], *, run_dir: Path) -> list[str]:
    invalid: list[str] = []
    raw_online_config_path = manifest.get("online_config_path")
    if raw_online_config_path not in {None, ""}:
        online_config_path = _resolve_source_path(str(raw_online_config_path), run_dir=run_dir)
        if online_config_path is None:
            invalid.append("online_config_path")

    raw_variant_path = manifest.get("algorithm_variant_manifest_path")
    if raw_variant_path in {None, ""}:
        return invalid
    variant_path = _resolve_source_path(str(raw_variant_path), run_dir=run_dir)
    if variant_path is None:
        return [*invalid, "algorithm_variant_manifest_path"]
    try:
        variant = load_algorithm_variant(variant_path)
    except (AlgorithmVariantError, OSError, UnicodeDecodeError, ValueError):
        return [*invalid, "algorithm_variant_manifest_path"]
    manifest_variant_id = manifest.get("algorithm_variant_id") or manifest.get("variant_id")
    if manifest_variant_id not in {None, ""} and variant.get("variant_id") != manifest_variant_id:
        invalid.append("algorithm_variant_manifest_path.variant_id_mismatch")
        return invalid
    if (
        _truth_input_declared(manifest)
        and variant.get("variant_type") not in TRUTH_INPUT_CLOSED_LOOP_VARIANT_TYPES
    ):
        invalid.append("algorithm_variant_manifest_path.variant_type_not_truth_input_closed_loop")
    return invalid


def _missing_manifest_fields(
    manifest: Mapping[str, Any],
    *,
    scenario_class: str | None,
) -> list[str]:
    missing: list[str] = []
    for field in REQUIRED_MANIFEST_FIELDS:
        if field == "algorithm_variant_id":
            if manifest.get("algorithm_variant_id") in {None, ""} and manifest.get("variant_id") in {None, ""}:
                missing.append(field)
            continue
        if field == "map":
            if manifest.get("map") in {None, ""} and manifest.get("town_map") in {None, ""}:
                missing.append(field)
            continue
        if field == "truth_input":
            if not _truth_input_declared(manifest):
                missing.append(field)
            continue
        if manifest.get(field) in {None, ""}:
            missing.append(field)
    missing.extend(_missing_carla_world_identity_fields(manifest))
    expected_behavior = TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS.get(str(scenario_class or ""))
    if expected_behavior is not None:
        expectation = manifest.get("traffic_light_expectation")
        if not isinstance(expectation, Mapping):
            missing.append("traffic_light_expectation.expected_behavior")
            missing.append("traffic_light_expectation.stimulus_mode")
        elif expectation.get("expected_behavior") != expected_behavior:
            missing.append("traffic_light_expectation.expected_behavior")
        elif expectation.get("stimulus_mode") in {None, ""}:
            missing.append("traffic_light_expectation.stimulus_mode")
        else:
            missing.extend(_missing_traffic_light_control_fields(manifest, expectation))
    return missing


def _missing_carla_world_identity_fields(manifest: Mapping[str, Any]) -> list[str]:
    carla_world = manifest.get("carla_world")
    if not isinstance(carla_world, Mapping):
        return ["carla_world.loaded_map_name", "carla_world.matches_configured_town"]
    missing: list[str] = []
    if carla_world.get("loaded_map_name") in {None, ""}:
        missing.append("carla_world.loaded_map_name")
    if carla_world.get("matches_configured_town") is not True:
        missing.append("carla_world.matches_configured_town")
    return missing


def _missing_traffic_light_control_fields(
    manifest: Mapping[str, Any],
    expectation: Mapping[str, Any],
) -> list[str]:
    if not _traffic_light_claim_grade(expectation):
        return []
    control = manifest.get("traffic_light_control")
    missing: list[str] = []
    if not isinstance(control, Mapping):
        return ["traffic_light_control"]
    stimulus_mode = str(control.get("stimulus_mode") or control.get("mode") or "").strip()
    if stimulus_mode != "deterministic_gt_control":
        missing.append("traffic_light_control.stimulus_mode")
    affected_count = control.get("initial_affected_count")
    if affected_count in {None, ""}:
        affected_count = control.get("last_affected_count")
    if affected_count in {None, ""}:
        missing.append("traffic_light_control.initial_affected_count")
    expected_release = str(expectation.get("expected_release_state") or "").strip().upper()
    if expected_release and not _traffic_light_release_observed(control):
        missing.append("traffic_light_control.release_frame_id")
    return missing


def _traffic_light_claim_grade(expectation: Mapping[str, Any]) -> bool:
    if expectation.get("claim_grade") is False:
        return False
    stimulus_mode = str(expectation.get("stimulus_mode") or "").strip()
    return expectation.get("claim_grade") is True or stimulus_mode == "deterministic_gt_control"


def _traffic_light_release_observed(control: Mapping[str, Any]) -> bool:
    if control.get("release_frame_id") not in {None, ""}:
        return True
    for event in control.get("events") or []:
        if isinstance(event, Mapping) and event.get("phase") == "release":
            return True
    return False


def _truth_input_declared(manifest: Mapping[str, Any]) -> bool:
    value = manifest.get("truth_input")
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "truth_input"}:
        return True
    input_mode = str(manifest.get("input_mode") or manifest.get("gt_source") or "").strip().lower()
    return input_mode in {"truth_input", "carla_gt", "ground_truth", "gt"}


def _missing_control_trace_fields(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return list(CONTROL_TRACE_P0_FIELDS)
    observed_fields: set[str] = set()
    for row in rows:
        observed_fields.update(str(field) for field in row)
    return [field for field in CONTROL_TRACE_P0_FIELDS if field not in observed_fields]


def _read_timeseries_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
        return rows
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relative_paths}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _scenario_class(summary: Mapping[str, Any], manifest: Mapping[str, Any], name: str) -> str | None:
    for source in (summary, manifest):
        value = source.get("scenario_class")
        if value:
            return str(value)
        metadata = source.get("metadata")
        if isinstance(metadata, Mapping) and metadata.get("scenario_class"):
            return str(metadata.get("scenario_class"))
    scenario_name = str(manifest.get("scenario_name") or name)
    if "traffic_light_red_to_green" in scenario_name:
        return "traffic_light_red_to_green_release"
    if "traffic_light_red" in scenario_name or "red_stop" in scenario_name:
        return "traffic_light_red_stop"
    if "traffic_light_green" in scenario_name or "green_go" in scenario_name:
        return "traffic_light_green_go"
    if "junction" in scenario_name:
        return "junction_turn"
    if "curve" in scenario_name:
        return "curve_diagnostic"
    if "lane" in scenario_name:
        return "lane_keep"
    return None


def _route_id(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> str:
    for source in (summary, manifest):
        value = source.get("route_id")
        if value not in {None, ""}:
            return str(value)
    return ""


def _text_field(summary: Mapping[str, Any], manifest: Mapping[str, Any], field: str) -> str | None:
    for source in (summary, manifest):
        value = source.get(field)
        if value not in {None, ""}:
            return str(value)
    return None
