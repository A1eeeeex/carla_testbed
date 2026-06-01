from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROUTE_START_OFFSET_SWEEP_SCHEMA_VERSION = "route_start_offset_sweep.v1"

OFFSET_OVERRIDE_KEY = "scenario.route_health.ego_offset_y_m"
SAFETY_FAILURE_REASONS = {
    "collision",
    "lane_invasion",
    "off_route",
    "high_lateral_error",
    "heading_divergence",
    "route_start_lane_invasion",
}

CSV_FIELDS = [
    "scenario_id",
    "offset_y_m",
    "status",
    "verdict",
    "failure_reason",
    "route_completion",
    "route_completion_delta",
    "lateral_error_p95",
    "lateral_error_p95_delta",
    "heading_error_p95",
    "heading_error_p95_delta",
    "control_latency_p95_ms",
    "lane_invasion_count",
    "collision_count",
    "report_path",
]


@dataclass(frozen=True)
class ProbeInput:
    report_path: Path
    suite_root: Path | None = None
    offset_y_m: float | None = None


def analyze_route_start_offset_sweep(
    source_report_path: str | Path,
    probe_reports: Sequence[str | Path | ProbeInput],
    *,
    scenario_id: str | None = None,
    min_completion_delta_for_candidate: float = 0.02,
) -> dict[str, Any]:
    source_path = Path(source_report_path)
    source_report = _read_json(source_path)
    source_run = _select_run(source_report, scenario_id=scenario_id)
    selected_scenario_id = str(scenario_id or (source_run or {}).get("scenario_id") or "").strip() or None
    missing_inputs: list[str] = []
    if source_run is None:
        missing_inputs.append("source_run")

    probes = [
        _probe_summary(
            _coerce_probe_input(item),
            source_run=source_run,
            scenario_id=selected_scenario_id,
            min_completion_delta_for_candidate=min_completion_delta_for_candidate,
        )
        for item in probe_reports
    ]
    if not probes:
        missing_inputs.append("probe_reports")

    status, reason = _sweep_verdict(probes, missing_inputs=missing_inputs)
    return {
        "schema_version": ROUTE_START_OFFSET_SWEEP_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "reason": reason,
        "scenario_id": selected_scenario_id,
        "source_report_path": str(source_path),
        "source_run": dict(source_run or {}),
        "probes": probes,
        "summary": _sweep_summary(probes),
        "missing_inputs": missing_inputs,
        "claim_boundary": {
            "probe_only": True,
            "can_claim_lane_keep_fix": status == "pass_candidate",
            "can_claim_curve_health": False,
            "can_change_default_config": False,
            "can_change_steer_scale": False,
            "can_enable_physical_mapping": False,
        },
        "required_next_actions": _required_next_actions(status, reason, source_run, probes),
    }


def write_route_start_offset_sweep_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "route_start_offset_sweep_report.json"
    csv_path = output_dir / "route_start_offset_sweep_report.csv"
    md_path = output_dir / "route_start_offset_sweep_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(csv_path, report.get("probes") or [])
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "route_start_offset_sweep_report": str(json_path),
        "route_start_offset_sweep_csv": str(csv_path),
        "route_start_offset_sweep_summary": str(md_path),
    }


def infer_offset_from_suite_root(suite_root: str | Path, *, scenario_id: str | None = None) -> float | None:
    root = Path(suite_root)
    matrix_path = root / "run_matrix.csv"
    if not matrix_path.exists():
        return None
    with matrix_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if scenario_id and str(row.get("scenario_id") or "") != str(scenario_id):
                continue
            parsed = _offset_from_overrides_json(row.get("runner_overrides_json"))
            if parsed is not None:
                return parsed
            parsed = _offset_from_command(row.get("command"))
            if parsed is not None:
                return parsed
    return None


def _probe_summary(
    probe: ProbeInput,
    *,
    source_run: Mapping[str, Any] | None,
    scenario_id: str | None,
    min_completion_delta_for_candidate: float,
) -> dict[str, Any]:
    report = _read_json(probe.report_path)
    run = _select_run(report, scenario_id=scenario_id)
    offset = probe.offset_y_m
    if offset is None and probe.suite_root is not None:
        offset = infer_offset_from_suite_root(probe.suite_root, scenario_id=scenario_id)
    if offset is None:
        offset = _num((run or {}).get("applied_ego_offset_y_m"))
    comparison = _comparison(source_run, run)
    status = _probe_status(
        run,
        comparison=comparison,
        min_completion_delta_for_candidate=min_completion_delta_for_candidate,
    )
    return {
        "report_path": str(probe.report_path),
        "suite_root": str(probe.suite_root) if probe.suite_root else None,
        "scenario_id": None if run is None else run.get("scenario_id"),
        "offset_y_m": offset,
        "status": status,
        "verdict": None if run is None else run.get("verdict"),
        "failure_reason": None if run is None else run.get("failure_reason"),
        "route_completion": None if run is None else _num(run.get("route_completion")),
        "lateral_error_p95": None if run is None else _num(run.get("lateral_error_p95")),
        "heading_error_p95": None if run is None else _num(run.get("heading_error_p95")),
        "control_latency_p95_ms": None if run is None else _num(run.get("control_latency_p95_ms")),
        "lane_invasion_count": None if run is None else _num(run.get("lane_invasion_count")),
        "collision_count": None if run is None else _num(run.get("collision_count")),
        "missing_fields": [] if run is None else list(run.get("missing_fields") or []),
        "missing_artifacts": [] if run is None else list(run.get("missing_artifacts") or []),
        "comparison": comparison,
    }


def _probe_status(
    run: Mapping[str, Any] | None,
    *,
    comparison: Mapping[str, Any],
    min_completion_delta_for_candidate: float,
) -> str:
    if run is None:
        return "missing_probe_run"
    if run.get("missing_artifacts"):
        return "insufficient_data"
    failure_reason = str(run.get("failure_reason") or "")
    lane_invasions = _num(run.get("lane_invasion_count")) or 0.0
    collisions = _num(run.get("collision_count")) or 0.0
    if failure_reason in SAFETY_FAILURE_REASONS or lane_invasions > 0 or collisions > 0:
        return "safety_regressed"
    completion_delta = _num(comparison.get("route_completion_delta"))
    if completion_delta is None:
        return "insufficient_data"
    if completion_delta >= float(min_completion_delta_for_candidate):
        return "candidate_positive"
    return "no_clear_gain"


def _sweep_verdict(probes: Sequence[Mapping[str, Any]], *, missing_inputs: Sequence[str]) -> tuple[str, str]:
    if missing_inputs:
        return "insufficient_data", "missing_sweep_inputs"
    candidate_offsets = {
        _offset_key(probe.get("offset_y_m"))
        for probe in probes
        if probe.get("status") == "candidate_positive"
    }
    safety_offsets = {
        _offset_key(probe.get("offset_y_m"))
        for probe in probes
        if probe.get("status") == "safety_regressed"
    }
    conflicts = candidate_offsets.intersection(safety_offsets)
    if conflicts:
        return "warn", "offset_repeatability_conflict"
    if candidate_offsets:
        return "pass_candidate", "safe_offset_candidate_found"
    if safety_offsets and len(safety_offsets) == len(probes):
        return "fail", "all_tested_offsets_safety_regressed"
    return "insufficient_data", "no_safe_positive_offset_evidence"


def _sweep_summary(probes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    offsets = sorted({_offset_key(probe.get("offset_y_m")) for probe in probes})
    candidate = [probe for probe in probes if probe.get("status") == "candidate_positive"]
    safety = [probe for probe in probes if probe.get("status") == "safety_regressed"]
    by_offset: dict[str, dict[str, Any]] = {}
    for probe in probes:
        key = _offset_key(probe.get("offset_y_m"))
        bucket = by_offset.setdefault(
            key,
            {
                "offset_y_m": probe.get("offset_y_m"),
                "run_count": 0,
                "candidate_positive_count": 0,
                "safety_regressed_count": 0,
                "best_route_completion": None,
                "has_repeatability_conflict": False,
            },
        )
        bucket["run_count"] += 1
        if probe.get("status") == "candidate_positive":
            bucket["candidate_positive_count"] += 1
        if probe.get("status") == "safety_regressed":
            bucket["safety_regressed_count"] += 1
        completion = _num(probe.get("route_completion"))
        if completion is not None:
            current = _num(bucket.get("best_route_completion"))
            bucket["best_route_completion"] = completion if current is None else max(current, completion)
    for bucket in by_offset.values():
        bucket["has_repeatability_conflict"] = bool(
            bucket["candidate_positive_count"] > 0 and bucket["safety_regressed_count"] > 0
        )
    best_safe = _best_candidate(candidate)
    return {
        "tested_offset_count": len(offsets),
        "probe_count": len(probes),
        "candidate_positive_count": len(candidate),
        "safety_regressed_count": len(safety),
        "offsets": offsets,
        "by_offset": [by_offset[key] for key in sorted(by_offset)],
        "best_safe_probe": best_safe,
        "repeatability_conflict_offsets": [
            item["offset_y_m"]
            for item in by_offset.values()
            if item["has_repeatability_conflict"]
        ],
    }


def _best_candidate(candidate: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not candidate:
        return None
    return dict(
        max(
            candidate,
            key=lambda probe: (
                _num((probe.get("comparison") or {}).get("route_completion_delta")) or -math.inf,
                _num(probe.get("route_completion")) or -math.inf,
            ),
        )
    )


def _comparison(source_run: Mapping[str, Any] | None, probe_run: Mapping[str, Any] | None) -> dict[str, Any]:
    if source_run is None or probe_run is None:
        return {}
    fields = {
        "route_completion": ("route_completion",),
        "lateral_error_p95": ("lateral_error_p95",),
        "heading_error_p95": ("heading_error_p95",),
    }
    comparison: dict[str, Any] = {}
    for name, (field,) in fields.items():
        source = _num(source_run.get(field))
        probe = _num(probe_run.get(field))
        comparison[f"source_{name}"] = source
        comparison[f"probe_{name}"] = probe
        comparison[f"{name}_delta"] = None if source is None or probe is None else probe - source
    comparison["source_failure_reason"] = source_run.get("failure_reason")
    comparison["probe_failure_reason"] = probe_run.get("failure_reason")
    return comparison


def _select_run(report: Mapping[str, Any], *, scenario_id: str | None) -> Mapping[str, Any] | None:
    runs = [run for run in (report.get("run_results") or []) if isinstance(run, Mapping)]
    if scenario_id:
        for run in runs:
            if str(run.get("scenario_id") or "") == str(scenario_id):
                return run
    return runs[0] if len(runs) == 1 else None


def _coerce_probe_input(item: str | Path | ProbeInput) -> ProbeInput:
    if isinstance(item, ProbeInput):
        return item
    path = Path(item)
    suite_root = None
    if path.name == "natural_driving_report.json" and len(path.parents) >= 3:
        suite_root = path.parents[2]
    return ProbeInput(report_path=path, suite_root=suite_root)


def _offset_from_overrides_json(text: Any) -> float | None:
    if text in {None, ""}:
        return None
    try:
        payload = json.loads(str(text))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, list):
        return None
    for item in payload:
        value = _offset_from_override(str(item))
        if value is not None:
            return value
    return None


def _offset_from_command(text: Any) -> float | None:
    if text in {None, ""}:
        return None
    match = re.search(rf"{re.escape(OFFSET_OVERRIDE_KEY)}=([-+0-9.eE]+)", str(text))
    if not match:
        return None
    return _num(match.group(1))


def _offset_from_override(text: str) -> float | None:
    if not text.startswith(f"{OFFSET_OVERRIDE_KEY}="):
        return None
    return _num(text.split("=", 1)[1])


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _write_csv(path: Path, probes: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for probe in probes:
            comparison = probe.get("comparison") if isinstance(probe.get("comparison"), Mapping) else {}
            writer.writerow(
                {
                    "scenario_id": probe.get("scenario_id"),
                    "offset_y_m": _csv_value(probe.get("offset_y_m")),
                    "status": probe.get("status"),
                    "verdict": probe.get("verdict"),
                    "failure_reason": probe.get("failure_reason"),
                    "route_completion": _csv_value(probe.get("route_completion")),
                    "route_completion_delta": _csv_value(comparison.get("route_completion_delta")),
                    "lateral_error_p95": _csv_value(probe.get("lateral_error_p95")),
                    "lateral_error_p95_delta": _csv_value(comparison.get("lateral_error_p95_delta")),
                    "heading_error_p95": _csv_value(probe.get("heading_error_p95")),
                    "heading_error_p95_delta": _csv_value(comparison.get("heading_error_p95_delta")),
                    "control_latency_p95_ms": _csv_value(probe.get("control_latency_p95_ms")),
                    "lane_invasion_count": _csv_value(probe.get("lane_invasion_count")),
                    "collision_count": _csv_value(probe.get("collision_count")),
                    "report_path": probe.get("report_path"),
                }
            )


def _markdown(report: Mapping[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    lines = [
        "# Route Start Offset Sweep",
        "",
        f"- status: `{report.get('status')}`",
        f"- reason: `{report.get('reason')}`",
        f"- scenario_id: `{report.get('scenario_id')}`",
        f"- tested_offset_count: `{summary.get('tested_offset_count')}`",
        f"- probe_count: `{summary.get('probe_count')}`",
        f"- candidate_positive_count: `{summary.get('candidate_positive_count')}`",
        f"- safety_regressed_count: `{summary.get('safety_regressed_count')}`",
        f"- repeatability_conflict_offsets: `{summary.get('repeatability_conflict_offsets')}`",
        "",
        "| offset_y_m | status | verdict | failure_reason | route_completion | route_completion_delta | lane_invasion_count | control_latency_p95_ms |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for probe in report.get("probes") or []:
        if not isinstance(probe, Mapping):
            continue
        comparison = probe.get("comparison") if isinstance(probe.get("comparison"), Mapping) else {}
        lines.append(
            "| {offset} | {status} | {verdict} | {failure} | {completion} | {delta} | {lane} | {latency} |".format(
                offset=_csv_value(probe.get("offset_y_m")),
                status=probe.get("status"),
                verdict=probe.get("verdict"),
                failure=probe.get("failure_reason"),
                completion=_csv_value(probe.get("route_completion")),
                delta=_csv_value(comparison.get("route_completion_delta")),
                lane=_csv_value(probe.get("lane_invasion_count")),
                latency=_csv_value(probe.get("control_latency_p95_ms")),
            )
        )
    lines.extend(
        [
            "",
            "This report compares route-start offset probes only. It does not prove curve health, "
            "does not justify changing `steer_scale`, and does not enable physical mapping.",
            "",
        ]
    )
    return "\n".join(lines)


def _required_next_actions(
    status: str,
    reason: str,
    source_run: Mapping[str, Any] | None,
    probes: Sequence[Mapping[str, Any]],
) -> list[str]:
    if status == "warn" and reason == "offset_repeatability_conflict":
        tested = {_offset_key(probe.get("offset_y_m")) for probe in probes}
        base_delta = _num((source_run or {}).get("recommended_ego_offset_y_delta_m"))
        suggested: list[float] = []
        if base_delta is not None:
            for fraction in (0.25, 0.5, 0.75):
                value = base_delta * fraction
                if _offset_key(value) not in tested:
                    suggested.append(value)
        return [
            "Do not promote the tested route-start offset; repeatability is conflicting.",
            "Run a smaller-offset sweep before changing defaults.",
            f"Suggested next offsets: {suggested}",
        ]
    if status == "pass_candidate":
        return [
            "Repeat the best safe offset at least once.",
            "Run lane_keep_217 and junction_031 no-regression before any promotion.",
            "Do not apply this offset to curve diagnostics without separate route-health evidence.",
        ]
    if status == "fail":
        return [
            "Do not use the tested offsets.",
            "Inspect route-start contract, spawn pose, lane boundaries, and route projection before another online run.",
        ]
    return [
        "Collect more complete probe reports with natural_driving_report.json and run_matrix.csv.",
    ]


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _offset_key(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "unknown"
    return f"{number:.6f}"


def _csv_value(value: Any) -> Any:
    number = _num(value)
    if number is not None:
        return f"{number:.12g}"
    return "" if value in {None, ""} else value
