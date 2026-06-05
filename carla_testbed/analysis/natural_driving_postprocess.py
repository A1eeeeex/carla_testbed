from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.apollo_channel_health import (
    CHANNEL_HEALTH_REPORT_SCHEMA_VERSION,
    analyze_apollo_channel_health_files,
    write_apollo_channel_health_report,
)
from carla_testbed.analysis.apollo_control_handoff import (
    ensure_apollo_control_handoff_report,
)
from carla_testbed.analysis.apollo_hdmap_projection import (
    analyze_apollo_hdmap_projection_file,
    write_apollo_hdmap_projection_report,
)
from carla_testbed.analysis.apollo_reference_line_contract import (
    ensure_apollo_reference_line_contract_report,
)
from carla_testbed.analysis.channel_stats_normalizer import normalize_channel_stats_for_run
from carla_testbed.analysis.artifact_completeness import check_run_artifact_completeness
from carla_testbed.analysis.artifact_completeness import write_run_artifact_completeness_report
from carla_testbed.analysis.control_health import (
    analyze_control_health_run_dir,
    write_control_health_report,
)
from carla_testbed.analysis.failure_timeline import (
    analyze_failure_timeline_run_dir,
    write_failure_timeline_report,
)
from carla_testbed.analysis.localization_contract import ensure_localization_contract_report
from carla_testbed.analysis.natural_driving import (
    TRAFFIC_LIGHT_SCENARIO_CLASSES,
    analyze_natural_driving_suite,
    problem_run_details,
    write_natural_driving_report,
)
from carla_testbed.analysis.obstacle_gt_contract import (
    analyze_obstacle_gt_contract_run_dir,
    write_obstacle_gt_contract_report,
)
from carla_testbed.analysis.route_health_report import analyze_route_health_run_dir
from carla_testbed.analysis.route_curve_artifact_gap import (
    analyze_route_curve_artifact_gap,
    write_route_curve_artifact_gap_report,
)
from carla_testbed.analysis.route_start_alignment import (
    analyze_route_start_alignment_run_dir,
    write_route_start_alignment_report,
)
from carla_testbed.analysis.run_artifact_standardizer import standardize_legacy_run_artifacts
from carla_testbed.analysis.traffic_light_behavior import (
    analyze_traffic_light_behavior_run_dir,
    write_traffic_light_behavior_report,
)
from carla_testbed.analysis.traffic_light_contract import (
    build_insufficient_traffic_light_contract_report,
    build_traffic_light_contract_report_files,
    write_traffic_light_contract_report,
)
from carla_testbed.experiments.natural_driving_probe_plan import (
    build_route_start_probe_plan,
    write_route_start_probe_plan,
)

NATURAL_DRIVING_POSTPROCESS_SCHEMA_VERSION = "town01_natural_driving_postprocess.v1"
DEFAULT_CHANNEL_HEALTH_CONFIG = Path("configs/algorithms/apollo_natural_driving_channels.yaml")
DEFAULT_TOWN01_APOLLO_CONTRACT = Path("configs/town01/apollo_contract.example.yaml")
DEFAULT_TRAFFIC_LIGHT_MAPPING = Path("configs/town01/traffic_lights.example.yaml")


def postprocess_natural_driving_runs(
    suite_root: str | Path,
    *,
    out_dir: str | Path | None = None,
    channel_config: str | Path = DEFAULT_CHANNEL_HEALTH_CONFIG,
    town01_contract_config: str | Path = DEFAULT_TOWN01_APOLLO_CONTRACT,
    traffic_light_mapping_config: str | Path = DEFAULT_TRAFFIC_LIGHT_MAPPING,
    require_full_target_coverage: bool | None = None,
    refresh: bool = False,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser()
    output_dir = Path(out_dir).expanduser() if out_dir is not None else root / "analysis" / "natural_driving"
    output_dir.mkdir(parents=True, exist_ok=True)
    run_dirs = _discover_run_dirs(root)
    run_results = [
        _postprocess_run_dir(
            run_dir,
            channel_config=Path(channel_config),
            town01_contract_config=Path(town01_contract_config),
            traffic_light_mapping_config=Path(traffic_light_mapping_config),
            refresh=refresh,
        )
        for run_dir in run_dirs
    ]
    natural_report = analyze_natural_driving_suite(
        root,
        require_full_target_coverage=require_full_target_coverage,
    )
    natural_outputs = write_natural_driving_report(natural_report, output_dir)
    natural_problem_runs = problem_run_details(natural_report.get("run_results") or [])
    suite_manifest = _read_json(root / "suite_manifest.json")
    route_start_probe_plan = build_route_start_probe_plan(
        natural_report,
        source_report_path=natural_outputs["natural_driving_report"],
        suite_path=suite_manifest.get("suite_path") or "configs/scenarios/town01_natural_driving_suite.yaml",
        output_root=root.parent / f"{root.name}_route_start_alignment_probe",
    )
    route_start_probe_outputs = write_route_start_probe_plan(
        route_start_probe_plan,
        output_dir / "route_start_probe_plan",
    )
    report = {
        "schema_version": NATURAL_DRIVING_POSTPROCESS_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "suite_root": str(root),
        "run_count": len(run_results),
        "runs": run_results,
        "natural_driving": {
            "status": natural_report.get("verdict", {}).get("status"),
            "outputs": natural_outputs,
            "summary": natural_report.get("summary") or {},
            "verdict": natural_report.get("verdict") or {},
            "capability_coverage": natural_report.get("capability_coverage") or {},
            "problem_run_count": len(natural_problem_runs),
            "problem_runs": natural_problem_runs,
        },
        "route_start_probe_plan": {
            "status": route_start_probe_plan.get("status"),
            "probe_count": route_start_probe_plan.get("probe_count"),
            "outputs": route_start_probe_outputs,
        },
        "outputs": {
            "natural_driving_postprocess_json": str(output_dir / "natural_driving_postprocess.json"),
            **natural_outputs,
            **route_start_probe_outputs,
        },
        "interpretation_boundary": (
            "Postprocess only generates or checks offline artifacts. It does not start CARLA/Apollo "
            "and does not turn insufficient_data artifacts into behavior success."
        ),
    }
    _write_json(output_dir / "natural_driving_postprocess.json", report)
    _write_markdown(output_dir / "natural_driving_postprocess.md", report)
    return report


def _postprocess_run_dir(
    run_dir: Path,
    *,
    channel_config: Path,
    town01_contract_config: Path,
    traffic_light_mapping_config: Path,
    refresh: bool,
) -> dict[str, Any]:
    standardization = standardize_legacy_run_artifacts(run_dir, refresh=refresh)
    manifest = _read_json(run_dir / "manifest.json")
    summary = _read_json(run_dir / "summary.json")
    scenario_class = _scenario_class(summary, manifest, run_dir.name)
    route_health_result = _ensure_route_health(run_dir, refresh=refresh)
    route_curve_artifact_gap_result = _ensure_route_curve_artifact_gap(run_dir, refresh=refresh)
    apollo_control_handoff_result = _ensure_apollo_control_handoff(run_dir, refresh=refresh)
    apollo_hdmap_projection_result = _ensure_apollo_hdmap_projection(run_dir, refresh=refresh)
    localization_contract_result = _ensure_localization_contract(run_dir, refresh=refresh)
    apollo_reference_line_contract_result = _ensure_apollo_reference_line_contract(
        run_dir,
        refresh=refresh,
    )
    control_health_result = _ensure_control_health(
        run_dir,
        scenario_class=scenario_class,
        refresh=refresh,
    )
    failure_timeline_result = _ensure_failure_timeline(
        run_dir,
        scenario_class=scenario_class,
        refresh=refresh,
    )
    route_start_alignment_result = _ensure_route_start_alignment(
        run_dir,
        scenario_class=scenario_class,
        refresh=refresh,
    )
    channel_health_result = _ensure_channel_health(
        run_dir,
        channel_config=channel_config,
        scenario_class=scenario_class,
        refresh=refresh,
    )
    traffic_light_result = _ensure_traffic_light_contract_report(
        run_dir,
        scenario_class=scenario_class,
        town01_contract_config=town01_contract_config,
        traffic_light_mapping_config=traffic_light_mapping_config,
        refresh=refresh,
    )
    traffic_light_behavior_result = _ensure_traffic_light_behavior_report(
        run_dir,
        scenario_class=scenario_class,
        refresh=refresh,
    )
    obstacle_gt_contract_result = _ensure_obstacle_gt_contract_report(
        run_dir,
        scenario_class=scenario_class,
        refresh=refresh,
    )
    completeness = check_run_artifact_completeness(run_dir, scenario_class=scenario_class)
    completeness_outputs = write_run_artifact_completeness_report(
        completeness,
        run_dir / "analysis" / "artifact_completeness",
    )
    return {
        "run_dir": str(run_dir),
        "run_id": str(summary.get("run_id") or manifest.get("run_id") or run_dir.name),
        "scenario_class": scenario_class,
        "route_health": route_health_result,
        "route_curve_artifact_gap": route_curve_artifact_gap_result,
        "standardization": standardization,
        "apollo_control_handoff": apollo_control_handoff_result,
        "apollo_hdmap_projection": apollo_hdmap_projection_result,
        "localization_contract": localization_contract_result,
        "apollo_reference_line_contract": apollo_reference_line_contract_result,
        "control_health": control_health_result,
        "failure_timeline": failure_timeline_result,
        "route_start_alignment": route_start_alignment_result,
        "apollo_channel_health": channel_health_result,
        "traffic_light_contract": traffic_light_result,
        "traffic_light_behavior": traffic_light_behavior_result,
        "obstacle_gt_contract": obstacle_gt_contract_result,
        "artifact_completeness": {
            "status": completeness["status"],
            "artifact_complete": completeness["artifact_complete"],
            "path": completeness_outputs["artifact_completeness_report"],
            "summary_path": completeness_outputs["artifact_completeness_summary"],
            "missing_artifacts": completeness["missing_artifacts"],
            "missing_manifest_fields": completeness.get("missing_manifest_fields") or [],
            "invalid_manifest_source_fields": completeness.get("invalid_manifest_source_fields") or [],
            "missing_control_trace_fields": completeness["missing_control_trace_fields"],
            "invalid_report_source_fields": completeness.get("invalid_report_source_fields") or [],
        },
    }


def _ensure_apollo_control_handoff(run_dir: Path, *, refresh: bool) -> dict[str, Any]:
    return ensure_apollo_control_handoff_report(run_dir, refresh=refresh)


def _ensure_apollo_hdmap_projection(run_dir: Path, *, refresh: bool) -> dict[str, Any]:
    existing = _find_first(
        run_dir,
        [
            "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
            "apollo_hdmap_projection_report.json",
        ],
    )
    if existing is not None and not refresh:
        report = _read_json(existing)
        return {
            "status": "existing",
            "path": str(existing),
            "summary_path": str(existing.with_name("apollo_hdmap_projection_summary.md")),
            "report_status": report.get("status"),
            "claim_grade": bool(report.get("claim_grade")),
            "artifact_path": report.get("artifact_path"),
        }
    projection = _find_first(
        run_dir,
        [
            "artifacts/apollo_hdmap_projection.jsonl",
            "apollo_hdmap_projection.jsonl",
            "analysis/apollo_hdmap_projection/apollo_hdmap_projection.json",
        ],
    )
    expected_projection = projection or (run_dir / "artifacts" / "apollo_hdmap_projection.jsonl")
    report = analyze_apollo_hdmap_projection_file(expected_projection)
    outputs = write_apollo_hdmap_projection_report(
        report,
        run_dir / "analysis" / "apollo_hdmap_projection",
    )
    return {
        "status": "generated",
        "path": outputs["apollo_hdmap_projection_report"],
        "summary_path": outputs["apollo_hdmap_projection_summary"],
        "report_status": report.get("status"),
        "claim_grade": bool(report.get("claim_grade")),
        "artifact_path": report.get("artifact_path"),
    }


def _ensure_localization_contract(run_dir: Path, *, refresh: bool) -> dict[str, Any]:
    return ensure_localization_contract_report(run_dir, refresh=refresh)


def _ensure_apollo_reference_line_contract(run_dir: Path, *, refresh: bool) -> dict[str, Any]:
    return ensure_apollo_reference_line_contract_report(run_dir, refresh=refresh)


def _ensure_route_health(run_dir: Path, *, refresh: bool) -> dict[str, Any]:
    path = run_dir / "analysis" / "route_health" / "route_health.json"
    existing_path = _find_first(
        run_dir,
        [
            "analysis/route_health/route_health.json",
            "route_health.json",
        ],
    )
    if existing_path is not None and not refresh and _route_health_report_reusable(
        existing_path,
        run_dir=run_dir,
    ):
        return {"status": "existing", "path": str(existing_path)}
    if (
        existing_path is not None
        and not _run_has_route_health_regeneration_inputs(run_dir)
        and _route_health_report_reusable(existing_path, run_dir=run_dir)
    ):
        _copy_existing_route_health_artifacts(existing_path, path.parent)
        report = _read_json(path)
        return {
            "status": "existing_report_copied",
            "path": str(path),
            "report_status": (report.get("verdict") or {}).get("status") or report.get("status"),
            "source_report": str(existing_path),
        }
    result = analyze_route_health_run_dir(run_dir)
    report_status = (result.get("report") or {}).get("verdict", {}).get("status")
    return {
        "status": "generated",
        "path": result["outputs"]["route_health_json"],
        "report_status": report_status,
    }


def _route_health_report_reusable(path: Path, *, run_dir: Path) -> bool:
    report = _read_json(path)
    expected_route_id = _run_route_id(run_dir)
    actual_route_id = str(report.get("route_id") or "").strip()
    if expected_route_id and actual_route_id != expected_route_id:
        return False
    source = report.get("source")
    if not isinstance(source, Mapping):
        return False
    required = ("manifest_path", "summary_path", "timeseries_path")
    if not _report_source_paths_resolve(path, run_dir=run_dir, required_source_fields=required):
        return False
    route_path = source.get("route_path")
    reconstructed = "route reconstructed from timeseries P0 route_curve fields" in {
        str(item) for item in (report.get("warnings") or [])
    }
    if route_path in {None, ""}:
        return reconstructed
    return _source_path_exists(str(route_path), run_dir=run_dir, report_path=path)


def _run_route_id(run_dir: Path) -> str:
    summary = _read_json(run_dir / "summary.json")
    manifest = _read_json(run_dir / "manifest.json")
    for source in (summary, manifest):
        value = source.get("route_id")
        if value not in {None, ""}:
            return str(value)
    return ""


def _copy_existing_route_health_artifacts(existing_json: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    source_dir = existing_json.parent
    for filename in (
        "route_health.json",
        "route_health.csv",
        "curve_segments.csv",
        "route_health_summary.md",
    ):
        source = source_dir / filename
        if source.exists():
            destination = output_dir / filename
            if source.resolve() != destination.resolve():
                shutil.copy2(source, destination)


def _run_has_route_health_regeneration_inputs(run_dir: Path) -> bool:
    for relative in (
        "route.json",
        "artifacts/route.json",
        "config/route.json",
    ):
        if (run_dir / relative).exists():
            return True
    timeseries = _find_first(run_dir, ["timeseries.csv", "timeseries.jsonl"])
    return _timeseries_has_route_context(timeseries)


def _timeseries_has_route_context(path: Path | None) -> bool:
    if path is None or not path.exists():
        return False
    if path.suffix.lower() == ".csv":
        try:
            with path.open(encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                fields = set(reader.fieldnames or [])
                if not {"route_x", "route_y"}.issubset(fields):
                    return False
                first = next(reader, None)
                return bool(first and first.get("route_x") not in {None, ""} and first.get("route_y") not in {None, ""})
        except OSError:
            return False
    if path.suffix.lower() == ".jsonl":
        try:
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    if not isinstance(payload, Mapping):
                        return False
                    return payload.get("route_x") not in {None, ""} and payload.get("route_y") not in {None, ""}
        except (OSError, json.JSONDecodeError):
            return False
    return False


def _ensure_control_health(
    run_dir: Path,
    *,
    scenario_class: str | None,
    refresh: bool,
) -> dict[str, Any]:
    existing = _find_first(
        run_dir,
        [
            "analysis/control_health/control_health_report.json",
            "control_health_report.json",
        ],
    )
    if (
        existing is not None
        and not refresh
        and _control_health_report_reusable(existing, run_dir=run_dir, scenario_class=scenario_class)
    ):
        return {"status": "existing", "path": str(existing)}
    report = analyze_control_health_run_dir(run_dir)
    outputs = write_control_health_report(report, run_dir / "analysis" / "control_health")
    return {
        "status": "generated",
        "path": outputs["control_health_report"],
        "report_status": report.get("status"),
        "failure_reason": report.get("failure_reason"),
    }


def _control_health_report_reusable(
    path: Path,
    *,
    run_dir: Path,
    scenario_class: str | None,
) -> bool:
    report = _read_json(path)
    if str(report.get("scenario_class") or "") != str(scenario_class or ""):
        return False
    expected_route_id = _run_route_id(run_dir)
    actual_route_id = str(report.get("route_id") or "").strip()
    if expected_route_id and actual_route_id != expected_route_id:
        return False
    return _report_source_paths_resolve(
        path,
        run_dir=run_dir,
        required_source_fields=("summary_path", "manifest_path", "timeseries_path"),
    )


def _ensure_failure_timeline(
    run_dir: Path,
    *,
    scenario_class: str | None,
    refresh: bool,
) -> dict[str, Any]:
    existing = _find_first(
        run_dir,
        [
            "analysis/failure_timeline/failure_timeline_report.json",
            "failure_timeline_report.json",
        ],
    )
    if (
        existing is not None
        and not refresh
        and _failure_timeline_report_reusable(
            existing,
            run_dir=run_dir,
            scenario_class=scenario_class,
        )
    ):
        return {"status": "existing", "path": str(existing)}
    report = analyze_failure_timeline_run_dir(run_dir)
    outputs = write_failure_timeline_report(report, run_dir / "analysis" / "failure_timeline")
    primary = report.get("primary_failure") if isinstance(report.get("primary_failure"), Mapping) else {}
    anchor = report.get("anchor_event") if isinstance(report.get("anchor_event"), Mapping) else {}
    return {
        "status": "generated",
        "path": outputs["failure_timeline_report"],
        "summary_path": outputs["failure_timeline_summary"],
        "report_status": report.get("status"),
        "primary_failure": primary.get("failure_reason"),
        "anchor_event_type": anchor.get("event_type"),
        "ordering_findings": report.get("ordering_findings") or [],
    }


def _failure_timeline_report_reusable(
    path: Path,
    *,
    run_dir: Path,
    scenario_class: str | None,
) -> bool:
    report = _read_json(path)
    if str(report.get("scenario_class") or "") != str(scenario_class or ""):
        return False
    expected_route_id = _run_route_id(run_dir)
    actual_route_id = str(report.get("route_id") or "").strip()
    if expected_route_id and actual_route_id != expected_route_id:
        return False
    return _report_source_paths_resolve(
        path,
        run_dir=run_dir,
        required_source_fields=(
            "manifest_path",
            "summary_path",
            "events_path",
            "timeseries_path",
            "route_health_path",
            "control_health_path",
        ),
    )


def _ensure_route_start_alignment(
    run_dir: Path,
    *,
    scenario_class: str | None,
    refresh: bool,
) -> dict[str, Any]:
    existing = _find_first(
        run_dir,
        [
            "analysis/route_start_alignment/route_start_alignment_report.json",
            "route_start_alignment_report.json",
        ],
    )
    if (
        existing is not None
        and not refresh
        and _route_start_alignment_report_reusable(
            existing,
            run_dir=run_dir,
            scenario_class=scenario_class,
        )
    ):
        return {"status": "existing", "path": str(existing)}
    report = analyze_route_start_alignment_run_dir(run_dir)
    outputs = write_route_start_alignment_report(report, run_dir / "analysis" / "route_start_alignment")
    return {
        "status": "generated",
        "path": outputs["route_start_alignment_report"],
        "summary_path": outputs["route_start_alignment_summary"],
        "report_status": report.get("status"),
        "reason": report.get("reason"),
        "hypotheses": report.get("hypotheses") or [],
    }


def _route_start_alignment_report_reusable(
    path: Path,
    *,
    run_dir: Path,
    scenario_class: str | None,
) -> bool:
    report = _read_json(path)
    if str(report.get("scenario_class") or "") != str(scenario_class or ""):
        return False
    expected_route_id = _run_route_id(run_dir)
    actual_route_id = str(report.get("route_id") or "").strip()
    if expected_route_id and actual_route_id != expected_route_id:
        return False
    return _report_source_paths_resolve(
        path,
        run_dir=run_dir,
        required_source_fields=(
            "manifest_path",
            "summary_path",
            "timeseries_path",
            "failure_timeline_path",
        ),
    )


def _ensure_route_curve_artifact_gap(run_dir: Path, *, refresh: bool) -> dict[str, Any]:
    existing = _find_first(
        run_dir,
        [
            "analysis/route_curve_artifact_gap/route_curve_artifact_gap_report.json",
            "route_curve_artifact_gap_report.json",
        ],
    )
    if existing is not None and not refresh and _report_source_paths_resolve(
        existing,
        run_dir=run_dir,
        required_source_fields=("timeseries_csv", "summary_json"),
    ):
        return {"status": "existing", "path": str(existing)}
    timeseries = _find_first(run_dir, ["timeseries.csv", "timeseries.jsonl"])
    summary = _find_first(run_dir, ["summary.json"])
    report = analyze_route_curve_artifact_gap(timeseries, summary_json=summary)
    outputs = write_route_curve_artifact_gap_report(
        report,
        run_dir / "analysis" / "route_curve_artifact_gap",
    )
    return {
        "status": "generated",
        "path": outputs["route_curve_artifact_gap_report"],
        "report_status": report.get("status"),
        "failure_reason": report.get("failure_reason"),
        "missing_p1_fields": report.get("missing_p1_fields") or [],
    }


def _ensure_channel_health(
    run_dir: Path,
    *,
    channel_config: Path,
    scenario_class: str | None,
    refresh: bool,
) -> dict[str, Any]:
    report_path = run_dir / "analysis" / "apollo_channel_health" / "apollo_channel_health_report.json"
    existing = _find_first(
        run_dir,
        [
            "analysis/apollo_channel_health/apollo_channel_health_report.json",
            "apollo_channel_health_report.json",
        ],
    )
    if (
        existing is not None
        and not refresh
        and _channel_health_report_reusable(existing, run_dir=run_dir, scenario_class=scenario_class)
    ):
        return {"status": "existing", "path": str(existing)}
    stats_path = _find_first(run_dir, ["channel_stats.json", "artifacts/channel_stats.json"])
    generated_channel_stats = None
    if refresh and _channel_stats_refreshable(stats_path):
        regenerated = normalize_channel_stats_for_run(run_dir)
        if regenerated is not None:
            generated_channel_stats = regenerated
            output_path = regenerated.get("_output_path")
            stats_path = Path(str(output_path)) if output_path else run_dir / "channel_stats.json"
    if stats_path is None:
        generated_channel_stats = normalize_channel_stats_for_run(run_dir)
        if generated_channel_stats is not None:
            output_path = generated_channel_stats.get("_output_path")
            stats_path = Path(str(output_path)) if output_path else run_dir / "channel_stats.json"
    if stats_path is None:
        reusable_report = _existing_channel_health_report(run_dir)
        if reusable_report is not None and _channel_health_report_reusable(
            reusable_report,
            run_dir=run_dir,
            scenario_class=scenario_class,
        ):
            report = _read_json(reusable_report)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            if reusable_report.resolve() != report_path.resolve():
                shutil.copy2(reusable_report, report_path)
            return {
                "status": "existing_report_copied",
                "path": str(report_path),
                "report_status": report.get("status"),
                "stats_source": "existing_report_without_raw_stats",
                "source_report": str(reusable_report),
            }
        report = _insufficient_channel_health_report(
            run_dir=run_dir,
            channel_config=channel_config,
            scenario_class=scenario_class,
        )
        _write_json(report_path, report)
        return {
            "status": "insufficient_data",
            "path": str(report_path),
            "missing_inputs": ["channel_stats"],
        }
    report = analyze_apollo_channel_health_files(
        channel_config,
        stats_path,
        scenario_class=scenario_class,
    )
    outputs = write_apollo_channel_health_report(report, report_path.parent)
    return {
        "status": "generated",
        "path": outputs["apollo_channel_health_report"],
        "report_status": report.get("status"),
        "stats_path": str(stats_path),
        "stats_source": _channel_stats_source_label(stats_path, generated_channel_stats),
    }


def _existing_channel_health_report(run_dir: Path) -> Path | None:
    for relative in (
        "apollo_channel_health_report.json",
        "analysis/apollo_channel_health/apollo_channel_health_report.json",
    ):
        path = run_dir / relative
        if path.exists():
            return path
    return None


def _channel_stats_refreshable(path: Path | None) -> bool:
    if path is None:
        return True
    stats = _read_json(path)
    source = stats.get("source")
    if not isinstance(source, Mapping):
        return False
    return source.get("type") in {
        "derived_from_bridge_counters",
        "mixed_bridge_and_row_level_artifacts",
    }


def _channel_stats_source_label(path: Path, generated_stats: Mapping[str, Any] | None) -> str:
    if generated_stats is not None:
        source = generated_stats.get("source")
        if isinstance(source, Mapping) and source.get("type"):
            return str(source["type"])
        return "generated_channel_stats"
    stats = _read_json(path)
    source = stats.get("source")
    if isinstance(source, Mapping) and source.get("type"):
        return str(source["type"])
    return "channel_stats"


def _channel_health_report_reusable(
    path: Path,
    *,
    run_dir: Path,
    scenario_class: str | None,
) -> bool:
    report = _read_json(path)
    if str(report.get("scenario_class") or "") != str(scenario_class or ""):
        return False
    return _report_source_paths_resolve(
        path,
        run_dir=run_dir,
        required_source_fields=("config_path", "stats_path"),
    )


def _report_source_paths_resolve(
    report_path: Path,
    *,
    run_dir: Path,
    required_source_fields: Sequence[str],
) -> bool:
    report = _read_json(report_path)
    source = report.get("source")
    if not isinstance(source, Mapping):
        return False
    for field in required_source_fields:
        raw = source.get(field)
        if raw in {None, ""}:
            return False
        if not _source_path_exists(str(raw), run_dir=run_dir, report_path=report_path):
            return False
    return True


def _source_path_exists(raw: str, *, run_dir: Path, report_path: Path) -> bool:
    path = Path(raw).expanduser()
    candidates = [path]
    if not path.is_absolute():
        candidates.extend(
            [
                run_dir / path,
                Path.cwd() / path,
                report_path.parent / path,
            ]
        )
    return any(candidate.exists() for candidate in candidates)


def _ensure_obstacle_gt_contract_report(
    run_dir: Path,
    *,
    scenario_class: str | None,
    refresh: bool,
) -> dict[str, Any]:
    report_path = run_dir / "analysis" / "obstacle_gt_contract" / "obstacle_gt_contract_report.json"
    existing = _find_first(
        run_dir,
        [
            "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
            "obstacle_gt_contract_report.json",
        ],
    )
    if existing is not None and not refresh:
        return {
            "status": "existing",
            "path": str(existing),
            "report_status": _json_report_status(existing),
        }
    report = analyze_obstacle_gt_contract_run_dir(run_dir, scenario_class=scenario_class)
    outputs = write_obstacle_gt_contract_report(report, report_path.parent)
    return {
        "status": "generated",
        "path": outputs["obstacle_gt_contract_report"],
        "report_status": report.get("status"),
        "missing_fields": report.get("missing_fields") or [],
        "warnings": report.get("warnings") or [],
        "errors": report.get("errors") or [],
    }


def _ensure_traffic_light_contract_report(
    run_dir: Path,
    *,
    scenario_class: str | None,
    town01_contract_config: Path,
    traffic_light_mapping_config: Path,
    refresh: bool,
) -> dict[str, Any] | None:
    if scenario_class not in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        return None
    report_path = run_dir / "analysis" / "traffic_light_contract" / "traffic_light_contract_report.json"
    legacy_report_path = run_dir / "analysis" / "traffic_light" / "traffic_light_contract_report.json"
    existing = _find_first(
        run_dir,
        [
            "analysis/traffic_light_contract/traffic_light_contract_report.json",
            "analysis/traffic_light/traffic_light_contract_report.json",
            "traffic_light_contract_report.json",
        ],
    )
    if (
        existing is not None
        and not refresh
        and _traffic_light_contract_report_reusable(
            existing,
            run_dir=run_dir,
            scenario_class=scenario_class,
        )
    ):
        return {"status": "existing", "path": str(existing)}
    if (
        existing is not None
        and _json_report_status(existing) == "pass"
        and _traffic_light_contract_report_reusable(
            existing,
            run_dir=run_dir,
            scenario_class=scenario_class,
        )
    ):
        report = _read_json(existing)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        if existing.resolve() != report_path.resolve():
            shutil.copy2(existing, report_path)
        _mirror_legacy_traffic_light_contract_report(report_path, legacy_report_path)
        return {
            "status": "existing_report_copied",
            "path": str(legacy_report_path),
            "report_status": report.get("status"),
            "source_report": str(existing),
        }

    missing_inputs: list[str] = []
    if not town01_contract_config.exists():
        missing_inputs.append("town01_apollo_contract_config")
    if not traffic_light_mapping_config.exists():
        missing_inputs.append("traffic_light_mapping_config")

    if missing_inputs:
        report = build_insufficient_traffic_light_contract_report(
            scenario_class=scenario_class,
            missing_inputs=missing_inputs,
        )
        _write_json(report_path, report)
        _mirror_legacy_traffic_light_contract_report(report_path, legacy_report_path)
        return {
            "status": "insufficient_data",
            "path": str(legacy_report_path),
            "missing_inputs": report["missing_inputs"],
        }

    report = build_traffic_light_contract_report_files(
        town01_contract_path=town01_contract_config,
        traffic_light_mapping_path=traffic_light_mapping_config,
        scenario_class=scenario_class,
    )
    outputs = write_traffic_light_contract_report(report, report_path.parent)
    _mirror_legacy_traffic_light_contract_report(Path(outputs["traffic_light_contract_report"]), legacy_report_path)
    return {
        "status": "generated",
        "path": str(legacy_report_path),
        "report_status": report.get("status"),
        "town01_contract_config": str(town01_contract_config),
        "traffic_light_mapping_config": str(traffic_light_mapping_config),
    }


def _mirror_legacy_traffic_light_contract_report(source: Path, legacy_path: Path) -> None:
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() != legacy_path.resolve():
        shutil.copy2(source, legacy_path)


def _traffic_light_contract_report_reusable(
    path: Path,
    *,
    run_dir: Path,
    scenario_class: str | None,
) -> bool:
    report = _read_json(path)
    if str(report.get("scenario_class") or "") != str(scenario_class or ""):
        return False
    return _report_source_paths_resolve(
        path,
        run_dir=run_dir,
        required_source_fields=(
            "town01_contract_path",
            "traffic_light_mapping_path",
        ),
    )


def _json_report_status(path: Path) -> str | None:
    payload = _read_json(path)
    value = payload.get("status")
    return str(value) if value not in {None, ""} else None


def _ensure_traffic_light_behavior_report(
    run_dir: Path,
    *,
    scenario_class: str | None,
    refresh: bool,
) -> dict[str, Any] | None:
    if scenario_class not in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        return None
    existing = _find_first(
        run_dir,
        [
            "analysis/traffic_light_behavior/traffic_light_behavior_report.json",
            "analysis/traffic_light/traffic_light_behavior_report.json",
            "traffic_light_behavior_report.json",
        ],
    )
    if (
        existing is not None
        and not refresh
        and _traffic_light_behavior_report_reusable(
            existing,
            run_dir=run_dir,
            scenario_class=scenario_class,
        )
    ):
        return {"status": "existing", "path": str(existing)}
    report = analyze_traffic_light_behavior_run_dir(run_dir, scenario_class=scenario_class)
    outputs = write_traffic_light_behavior_report(report, run_dir / "analysis" / "traffic_light")
    return {
        "status": "generated",
        "path": outputs["traffic_light_behavior_report"],
        "report_status": report.get("status"),
        "failure_reason": report.get("failure_reason"),
    }


def _traffic_light_behavior_report_reusable(
    path: Path,
    *,
    run_dir: Path,
    scenario_class: str | None,
) -> bool:
    report = _read_json(path)
    if str(report.get("scenario_class") or "") != str(scenario_class or ""):
        return False
    expected_route_id = _run_route_id(run_dir)
    actual_route_id = str(report.get("route_id") or "").strip()
    if expected_route_id and actual_route_id != expected_route_id:
        return False
    return _report_source_paths_resolve(
        path,
        run_dir=run_dir,
        required_source_fields=(
            "summary_path",
            "manifest_path",
            "timeseries_path",
            "events_path",
            "traffic_light_contract_path",
        ),
    )


def _insufficient_channel_health_report(
    *,
    run_dir: Path,
    channel_config: Path,
    scenario_class: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": CHANNEL_HEALTH_REPORT_SCHEMA_VERSION,
        "status": "insufficient_data",
        "scenario_class": scenario_class,
        "channel_results": {},
        "missing_required_channels": [],
        "missing_optional_channels": [],
        "low_rate_channels": [],
        "gap_failures": [],
        "timestamp_failures": [],
        "sequence_failures": [],
        "stale_channels": [],
        "warnings": ["channel_stats_missing"],
        "missing_inputs": ["channel_stats"],
        "source": {
            "config_path": str(channel_config),
            "run_dir": str(run_dir),
            "stats_path": None,
        },
        "interpretation_boundary": (
            "Channel-health postprocess could not find channel_stats.json. "
            "This is insufficient_data, not a transport pass."
        ),
    }


def _discover_run_dirs(root: Path) -> list[Path]:
    if (root / "summary.json").exists():
        return [root]
    if not root.exists():
        return []
    summary_dirs = {path.parent for path in root.rglob("summary.json") if path.is_file()}
    matrix_dirs = set(_run_dirs_from_matrix(root, summary_dirs))
    return sorted(summary_dirs | matrix_dirs, key=lambda path: str(path))


def _run_dirs_from_matrix(root: Path, summary_dirs: set[Path]) -> list[Path]:
    matrix_path = root / "run_matrix.csv"
    if not matrix_path.exists():
        return []
    summary_dirs_resolved = {path.expanduser().resolve() for path in summary_dirs}
    run_dirs: list[Path] = []
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            raw = str(row.get("actual_run_dir") or row.get("run_dir") or "").strip()
            if not raw:
                continue
            path = _resolve_matrix_run_dir(root, raw)
            resolved = path.resolve()
            if any(_is_relative_to(summary_dir, resolved) for summary_dir in summary_dirs_resolved):
                continue
            run_dirs.append(resolved)
    return run_dirs


def _resolve_matrix_run_dir(root: Path, raw: str) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    if path.exists():
        return path
    cwd_path = Path.cwd() / path
    if cwd_path.exists():
        return cwd_path
    return root / path


def _scenario_class(summary: Mapping[str, Any], manifest: Mapping[str, Any], name: str) -> str | None:
    for source in (summary, manifest):
        value = source.get("scenario_class")
        if value:
            return str(value)
        metadata = source.get("metadata")
        if isinstance(metadata, Mapping) and metadata.get("scenario_class"):
            return str(metadata.get("scenario_class"))
    if "traffic_light_red_to_green" in name:
        return "traffic_light_red_to_green_release"
    if "traffic_light_red" in name or "red_stop" in name:
        return "traffic_light_red_stop"
    if "traffic_light_green" in name or "green_go" in name:
        return "traffic_light_green_go"
    if "junction" in name:
        return "junction_turn"
    if "curve" in name:
        return "curve_diagnostic"
    if "lane" in name:
        return "lane_keep"
    return None


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


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    natural = report.get("natural_driving") if isinstance(report.get("natural_driving"), Mapping) else {}
    probe_plan = report.get("route_start_probe_plan") if isinstance(report.get("route_start_probe_plan"), Mapping) else {}
    lines = [
        "# Town01 Natural Driving Postprocess",
        "",
        f"- status: `{natural.get('status')}`",
        f"- run_count: `{report.get('run_count')}`",
        f"- problem_run_count: `{natural.get('problem_run_count')}`",
        f"- route_start_probe_plan_status: `{probe_plan.get('status')}`",
        f"- route_start_probe_count: `{probe_plan.get('probe_count')}`",
        "",
        "| run_id | scenario_class | artifact_status | route_health | route_curve_gap | apollo_hdmap_projection | apollo_reference_line | failure_timeline | route_start_alignment | channel_health | traffic_light_contract | traffic_light_behavior |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in report.get("runs") or []:
        completeness = run.get("artifact_completeness") or {}
        route_health = run.get("route_health") or {}
        route_curve = run.get("route_curve_artifact_gap") or {}
        hdmap_projection = run.get("apollo_hdmap_projection") or {}
        reference_line = run.get("apollo_reference_line_contract") or {}
        timeline = run.get("failure_timeline") or {}
        route_start = run.get("route_start_alignment") or {}
        channel_health = run.get("apollo_channel_health") or {}
        traffic = run.get("traffic_light_contract") or {}
        traffic_behavior = run.get("traffic_light_behavior") or {}
        lines.append(
            f"| {run.get('run_id')} | {run.get('scenario_class')} | "
            f"{completeness.get('status')} | {route_health.get('status')} | "
            f"{route_curve.get('status')} | "
            f"{hdmap_projection.get('report_status') or hdmap_projection.get('status')} | "
            f"{reference_line.get('status')} | "
            f"{timeline.get('status')} | "
            f"{route_start.get('status')} | "
            f"{channel_health.get('status')} | {traffic.get('status') if traffic else 'not_applicable'} | "
            f"{traffic_behavior.get('status') if traffic_behavior else 'not_applicable'} |"
        )
    problem_runs = [item for item in natural.get("problem_runs") or [] if isinstance(item, Mapping)]
    if problem_runs:
        lines.extend(
            [
                "",
                "## Natural Driving Problem Runs",
                "",
                "| run_id | scenario_class | verdict | failure_reason | missing_artifacts | missing_fields |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in problem_runs:
            lines.append(
                "| {} | {} | {} | {} | {} | {} |".format(
                    item.get("run_id"),
                    item.get("scenario_class"),
                    item.get("verdict"),
                    item.get("failure_reason"),
                    ", ".join(item.get("missing_artifacts") or []),
                    ", ".join(item.get("missing_fields") or []),
                )
            )
    lines.extend(
        [
            "",
            "## Route-Start Probe Plan",
            "",
            f"- status: `{probe_plan.get('status')}`",
            f"- probe_count: `{probe_plan.get('probe_count')}`",
            f"- plan: `{(probe_plan.get('outputs') or {}).get('route_start_probe_plan')}`",
            f"- script: `{(probe_plan.get('outputs') or {}).get('route_start_probe_script')}`",
            "",
            "This postprocess is offline-only. It does not start CARLA/Apollo and does not turn "
            "`insufficient_data` into behavior success.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
