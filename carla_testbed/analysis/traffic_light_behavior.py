from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

TRAFFIC_LIGHT_BEHAVIOR_REPORT_SCHEMA_VERSION = "traffic_light_behavior_report.v1"
TRAFFIC_LIGHT_SCENARIO_CLASSES = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
EXPECTED_BEHAVIOR_BY_SCENARIO_CLASS = {
    "traffic_light_red_stop": "red_stop",
    "traffic_light_green_go": "green_go",
    "traffic_light_red_to_green_release": "red_to_green_release",
}
DEFAULT_THRESHOLDS = {
    "max_red_stop_distance_m": 8.0,
    "stop_speed_mps": 0.2,
}


def analyze_traffic_light_behavior_run_dir(
    run_dir: str | Path,
    *,
    scenario_class: str | None = None,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})

    summary_path = root / "summary.json"
    manifest_path = root / "manifest.json"
    timeseries_path = _find_first(root, ["timeseries.csv", "timeseries.jsonl"])
    events_path = _find_first(root, ["events.jsonl"])
    contract_path = _find_first(
        root,
        [
            "analysis/traffic_light_contract/traffic_light_contract_report.json",
            "analysis/traffic_light/traffic_light_contract_report.json",
            "traffic_light_contract_report.json",
        ],
    )

    summary = _read_json(summary_path)
    manifest = _read_json(manifest_path)
    contract = _read_json(contract_path)
    rows = _read_rows(timeseries_path)
    events = _read_events(events_path)
    metrics = _summary_metrics(summary)
    scenario = scenario_class or _first_text(
        summary,
        "scenario_class",
        manifest,
        "scenario_class",
        default=_infer_scenario_class(root.name),
    )
    run_id = _first_text(summary, "run_id", manifest, "run_id", default=root.name)
    route_id = _first_text(summary, "route_id", manifest, "route_id")
    traffic_light_expectation = _traffic_light_expectation(manifest)
    traffic_light_control = _traffic_light_control(summary, manifest)

    missing_inputs: list[str] = []
    if not summary_path.exists():
        missing_inputs.append("summary")
    if timeseries_path is None:
        missing_inputs.append("timeseries")
    if events_path is None:
        missing_inputs.append("events")
    if contract_path is None:
        missing_inputs.append("traffic_light_contract_report")

    behavior_metrics = {
        "red_stop_distance_m": _first_number(
            metrics.get("red_stop_distance_m"),
            _event_metric(events, "red_stop_distance_m", mode="min"),
            _row_metric(rows, "red_stop_distance_m", mode="min"),
        ),
        "distance_to_stop_line_m": _first_number(
            metrics.get("distance_to_stop_line_m"),
            metrics.get("red_stop_distance_m"),
            _event_metric(events, "distance_to_stop_line_m", mode="min"),
            _row_metric(rows, "distance_to_stop_line_m", mode="min"),
            _row_metric(rows, "red_stop_distance_m", mode="min"),
        ),
        "ego_s": _first_number(
            metrics.get("ego_s"),
            _event_metric(events, "ego_s", mode="max"),
            _row_metric(rows, "ego_s", mode="max"),
        ),
        "ego_l": _first_number(
            metrics.get("ego_l"),
            _event_metric(events, "ego_l", mode="min"),
            _row_metric(rows, "ego_l", mode="min"),
        ),
        "light_color_timeline_available": _first_bool(
            metrics.get("light_color_timeline_available"),
            _event_bool(events, "light_color_timeline_available"),
            _row_has_any(rows, ("traffic_light_state", "traffic_light_color", "light_state", "signal_color")),
        ),
        "apollo_stop_decision_available": _first_bool(
            metrics.get("apollo_stop_decision_available"),
            _event_bool(events, "apollo_stop_decision_available"),
            _row_bool(rows, "apollo_stop_decision_available"),
        ),
        "control_full_stop_evidence": _first_bool(
            metrics.get("control_full_stop_evidence"),
            _event_bool(events, "control_full_stop_evidence"),
            _row_bool(rows, "control_full_stop_evidence"),
        ),
        "stopped_at_red": _first_bool(
            metrics.get("stopped_at_red"),
            _event_bool(events, "stopped_at_red"),
            _row_bool(rows, "stopped_at_red"),
            _derive_stopped_at_red(rows, active_thresholds),
        ),
        "green_pass_time_s": _first_number(
            metrics.get("green_pass_time_s"),
            _event_metric(events, "green_pass_time_s", mode="min"),
            _row_metric(rows, "green_pass_time_s", mode="min"),
        ),
        "red_to_green_release_time_s": _first_number(
            metrics.get("red_to_green_release_time_s"),
            _event_metric(events, "red_to_green_release_time_s", mode="min"),
            _row_metric(rows, "red_to_green_release_time_s", mode="min"),
        ),
        "traffic_light_contract_status": contract.get("status"),
    }

    status, failure_reason, missing_fields, warnings = _verdict(
        scenario,
        behavior_metrics,
        contract=contract,
        missing_inputs=missing_inputs,
        thresholds=active_thresholds,
        traffic_light_expectation=traffic_light_expectation,
        traffic_light_control=traffic_light_control,
    )
    return {
        "schema_version": TRAFFIC_LIGHT_BEHAVIOR_REPORT_SCHEMA_VERSION,
        "run_id": run_id,
        "route_id": route_id,
        "scenario_class": scenario,
        "status": status,
        "failure_reason": failure_reason,
        "metrics": behavior_metrics,
        "traffic_light_expectation": traffic_light_expectation,
        "traffic_light_control": traffic_light_control or None,
        "missing_fields": missing_fields,
        "missing_inputs": sorted(set(missing_inputs)),
        "warnings": sorted(set(warnings)),
        "source": {
            "run_dir": str(root),
            "summary_path": str(summary_path) if summary_path.exists() else None,
            "manifest_path": str(manifest_path) if manifest_path.exists() else None,
            "timeseries_path": str(timeseries_path) if timeseries_path else None,
            "events_path": str(events_path) if events_path else None,
            "traffic_light_contract_path": str(contract_path) if contract_path else None,
        },
        "interpretation_boundary": (
            "Traffic-light behavior report aggregates offline run artifacts only. It does not prove "
            "full Apollo perception, and warn-level contract evidence cannot become a pass-level "
            "traffic-light interaction claim."
        ),
    }


def write_traffic_light_behavior_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "traffic_light_behavior_report.json"
    md_path = output_dir / "traffic_light_behavior_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "traffic_light_behavior_report": str(json_path),
        "traffic_light_behavior_summary": str(md_path),
    }


def _verdict(
    scenario_class: str | None,
    metrics: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    missing_inputs: Sequence[str],
    thresholds: Mapping[str, float],
    traffic_light_expectation: Mapping[str, Any] | None,
    traffic_light_control: Mapping[str, Any],
) -> tuple[str, str | None, list[str], list[str]]:
    missing_fields: list[str] = []
    warnings: list[str] = []
    scenario = str(scenario_class or "")
    if scenario not in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        return "insufficient_data", "not_traffic_light_scenario", [], ["scenario_class_not_traffic_light"]

    expected_behavior = EXPECTED_BEHAVIOR_BY_SCENARIO_CLASS[scenario]
    if traffic_light_expectation is None:
        warnings.append("traffic_light_expectation_missing")
    elif traffic_light_expectation.get("expected_behavior") != expected_behavior:
        missing_fields.append("traffic_light_expectation.expected_behavior")
        return "fail", "traffic_light_expectation_mismatch", missing_fields, warnings

    contract_status = contract.get("status")
    if contract_status == "fail":
        return "fail", "traffic_light_contract_failed", missing_fields, warnings
    if contract_status not in {"pass", "warn"}:
        return "insufficient_data", "traffic_light_contract_missing_status", missing_fields, warnings
    if contract_status == "warn":
        warnings.append("traffic_light_contract_warn")

    if missing_inputs:
        warnings.extend(f"missing_input:{item}" for item in missing_inputs)

    if _traffic_light_claim_grade(traffic_light_expectation):
        control_status, control_reason, control_missing = _traffic_light_control_evidence_verdict(
            traffic_light_expectation,
            traffic_light_control,
        )
        missing_fields.extend(control_missing)
        if control_status != "pass":
            return control_status, control_reason, missing_fields, warnings

    if scenario == "traffic_light_red_stop":
        stopped = metrics.get("stopped_at_red")
        distance = _num(metrics.get("red_stop_distance_m"))
        if stopped is False:
            return "fail", "red_light_not_stopped", missing_fields, warnings
        if distance is None:
            missing_fields.append("red_stop_distance_m")
            return "insufficient_data", "missing_red_stop_distance", missing_fields, warnings
        if stopped is None:
            missing_fields.append("stopped_at_red")
            return "insufficient_data", "missing_stopped_at_red", missing_fields, warnings
        if distance < 0:
            return "fail", "red_light_stop_line_violation", missing_fields, warnings
        if distance > float(thresholds["max_red_stop_distance_m"]):
            return "warn", "red_stop_too_far_from_line", missing_fields, warnings
        claim_status, claim_reason, claim_missing = _red_stop_claim_grade_evidence_verdict(metrics)
        if claim_status != "pass":
            missing_fields.extend(claim_missing)
            return claim_status, claim_reason, missing_fields, warnings
    elif scenario == "traffic_light_green_go":
        if _num(metrics.get("green_pass_time_s")) is None:
            missing_fields.append("green_pass_time_s")
            return "insufficient_data", "missing_green_pass_time", missing_fields, warnings
    elif scenario == "traffic_light_red_to_green_release":
        if _num(metrics.get("red_to_green_release_time_s")) is None:
            missing_fields.append("red_to_green_release_time_s")
            return "insufficient_data", "missing_red_to_green_release_time", missing_fields, warnings

    if missing_inputs:
        missing_fields.extend(f"source.{item}" for item in missing_inputs)
        return "insufficient_data", "missing_behavior_inputs", missing_fields, warnings

    if contract_status == "warn":
        return "warn", "traffic_light_contract_warn", missing_fields, warnings
    return "pass", None, missing_fields, warnings


def _summary_metrics(summary: Mapping[str, Any]) -> Mapping[str, Any]:
    metrics = summary.get("metrics")
    return metrics if isinstance(metrics, Mapping) else summary


def _event_metric(events: Sequence[Mapping[str, Any]], field: str, *, mode: str) -> float | None:
    return _aggregate_values([_num(event.get(field)) for event in events], mode=mode)


def _row_metric(rows: Sequence[Mapping[str, Any]], field: str, *, mode: str) -> float | None:
    return _aggregate_values([_num(row.get(field)) for row in rows], mode=mode)


def _event_bool(events: Sequence[Mapping[str, Any]], field: str) -> bool | None:
    for event in events:
        value = _bool_metric(event.get(field))
        if value is not None:
            return value
    return None


def _row_bool(rows: Sequence[Mapping[str, Any]], field: str) -> bool | None:
    for row in rows:
        value = _bool_metric(row.get(field))
        if value is not None:
            return value
    return None


def _aggregate_values(values: Sequence[float | None], *, mode: str) -> float | None:
    cleaned = [value for value in values if value is not None and math.isfinite(value)]
    if not cleaned:
        return None
    if mode == "min":
        return min(cleaned)
    return max(cleaned)


def _first_number(*values: Any) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _first_bool(*values: Any) -> bool | None:
    for value in values:
        parsed = _bool_metric(value)
        if parsed is not None:
            return parsed
    return None


def _derive_stopped_at_red(rows: Sequence[Mapping[str, Any]], thresholds: Mapping[str, float]) -> bool | None:
    """Infer red-stop evidence only when distance and ego speed are both present."""
    if not rows:
        return None

    max_distance = float(thresholds["max_red_stop_distance_m"])
    stop_speed = float(thresholds["stop_speed_mps"])
    observed_near_stop_line = False

    for row in rows:
        distance = _first_number(row.get("red_stop_distance_m"), row.get("red_stop_distance"))
        speed = _first_number(row.get("ego_speed"), row.get("ego_speed_mps"), row.get("speed"), row.get("speed_mps"))
        if distance is None or speed is None:
            continue
        if not _row_is_red_or_unspecified(row):
            continue
        if 0.0 <= distance <= max_distance:
            observed_near_stop_line = True
            if abs(speed) <= stop_speed:
                return True

    return False if observed_near_stop_line else None


def _row_is_red_or_unspecified(row: Mapping[str, Any]) -> bool:
    for field in ("traffic_light_state", "traffic_light_color", "light_state", "signal_color"):
        value = row.get(field)
        if value not in {None, ""}:
            return str(value).strip().upper() == "RED"
    return True


def _bool_metric(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_events(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _read_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix == ".jsonl":
        return _read_events(path)
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


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


def _traffic_light_expectation(manifest: Mapping[str, Any]) -> dict[str, Any] | None:
    expectation = manifest.get("traffic_light_expectation")
    return dict(expectation) if isinstance(expectation, Mapping) else None


def _traffic_light_control(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    for source in (summary, manifest):
        value = source.get("traffic_light_control")
        if isinstance(value, Mapping):
            return dict(value)
        metadata = source.get("metadata")
        if isinstance(metadata, Mapping):
            scenario_metadata = metadata.get("scenario_metadata")
            if isinstance(scenario_metadata, Mapping):
                value = scenario_metadata.get("traffic_light_control")
                if isinstance(value, Mapping):
                    return dict(value)
    return {}


def _traffic_light_claim_grade(expectation: Mapping[str, Any] | None) -> bool:
    if not isinstance(expectation, Mapping):
        return False
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


def _traffic_light_control_evidence_verdict(
    expectation: Mapping[str, Any] | None,
    control: Mapping[str, Any],
) -> tuple[str, str | None, list[str]]:
    if not isinstance(expectation, Mapping):
        return "insufficient_data", "traffic_light_expectation_missing", ["traffic_light_expectation"]
    if not isinstance(control, Mapping) or not control:
        return "insufficient_data", "traffic_light_control_missing", ["traffic_light_control"]
    stimulus_mode = str(control.get("stimulus_mode") or control.get("mode") or "").strip()
    if stimulus_mode == "force_green" or str(control.get("traffic_light_policy") or control.get("policy") or "").strip() == "force_green":
        return (
            "insufficient_data",
            "traffic_light_force_green_not_claim_grade",
            ["traffic_light_control.traffic_light_policy"],
        )
    if stimulus_mode not in {"deterministic_gt_control", "carla_actual"}:
        return (
            "insufficient_data",
            "traffic_light_control_not_deterministic",
            ["traffic_light_control.stimulus_mode"],
        )
    affected_count = _num(control.get("initial_affected_count"))
    if affected_count is None:
        affected_count = _num(control.get("last_affected_count"))
    if affected_count is None:
        return (
            "insufficient_data",
            "traffic_light_control_missing_affected_count",
            ["traffic_light_control.initial_affected_count"],
        )
    if affected_count <= 0:
        return "fail", "traffic_light_control_no_actor_affected", []
    policy = str(control.get("traffic_light_policy") or control.get("policy") or "").strip()
    if policy != "carla_actual":
        return (
            "insufficient_data",
            "traffic_light_policy_not_carla_actual",
            ["traffic_light_control.traffic_light_policy"],
        )
    color_source = str(control.get("color_source") or "").strip()
    if color_source not in {"carla_actor_state", "carla_landmark_state", "carla_traffic_light_actor_state"}:
        return (
            "insufficient_data",
            "traffic_light_color_source_not_claim_grade",
            ["traffic_light_control.color_source"],
        )
    confidence = _num(control.get("confidence"))
    if confidence is None or confidence < 0.99:
        return (
            "insufficient_data",
            "traffic_light_confidence_below_claim_grade",
            ["traffic_light_control.confidence"],
        )
    contain_lights = _bool_metric(control.get("contain_lights"))
    if contain_lights is not True:
        return (
            "insufficient_data",
            "traffic_light_contain_lights_not_verified",
            ["traffic_light_control.contain_lights"],
        )

    expected_initial = str(expectation.get("expected_initial_state") or "").strip().upper()
    observed_initial = str(control.get("initial_state") or "").strip().upper()
    if expected_initial and observed_initial and observed_initial != expected_initial:
        return (
            "fail",
            "traffic_light_control_initial_state_mismatch",
            ["traffic_light_control.initial_state"],
        )

    expected_release = str(expectation.get("expected_release_state") or "").strip().upper()
    if expected_release:
        observed_release = str(control.get("release_state") or "").strip().upper()
        if observed_release and observed_release != expected_release:
            return (
                "fail",
                "traffic_light_control_release_state_mismatch",
                ["traffic_light_control.release_state"],
            )
        if not _traffic_light_release_observed(control):
            return (
                "insufficient_data",
                "traffic_light_control_release_not_observed",
                ["traffic_light_control.release_frame_id"],
            )
    return "pass", None, []


def _red_stop_claim_grade_evidence_verdict(metrics: Mapping[str, Any]) -> tuple[str, str | None, list[str]]:
    missing: list[str] = []
    if _num(metrics.get("distance_to_stop_line_m")) is None:
        missing.append("distance_to_stop_line_m")
    if _num(metrics.get("ego_s")) is None:
        missing.append("ego_s")
    if _num(metrics.get("ego_l")) is None:
        missing.append("ego_l")
    if _bool_metric(metrics.get("light_color_timeline_available")) is not True:
        missing.append("light_color_timeline_available")
    stop_decision = _bool_metric(metrics.get("apollo_stop_decision_available"))
    full_stop = _bool_metric(metrics.get("control_full_stop_evidence"))
    if stop_decision is not True and full_stop is not True:
        missing.append("apollo_stop_decision_or_control_full_stop_evidence")
    if missing:
        return "insufficient_data", "missing_red_stop_claim_grade_evidence", missing
    return "pass", None, []


def _row_has_any(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bool | None:
    if not rows:
        return None
    for row in rows:
        if any(row.get(field) not in {None, ""} for field in fields):
            return True
    return None


def _first_text(*args: Any, default: str | None = None) -> str | None:
    pairs = list(zip(args[0::2], args[1::2]))
    for mapping, key in pairs:
        if isinstance(mapping, Mapping):
            value = mapping.get(str(key))
            if value not in {None, ""}:
                return str(value)
    return default


def _infer_scenario_class(name: str) -> str | None:
    if "traffic_light_red_to_green" in name:
        return "traffic_light_red_to_green_release"
    if "traffic_light_red" in name or "red_stop" in name:
        return "traffic_light_red_stop"
    if "traffic_light_green" in name or "green_go" in name:
        return "traffic_light_green_go"
    return None


def _markdown(report: Mapping[str, Any]) -> str:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    expectation = (
        report.get("traffic_light_expectation")
        if isinstance(report.get("traffic_light_expectation"), Mapping)
        else {}
    )
    lines = [
        "# Traffic-Light Behavior Summary",
        "",
        f"- status: `{report.get('status')}`",
        f"- failure_reason: `{report.get('failure_reason')}`",
        f"- scenario_class: `{report.get('scenario_class')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- expected_behavior: `{expectation.get('expected_behavior')}`",
        f"- traffic_light_control_available: `{bool(report.get('traffic_light_control'))}`",
        f"- red_stop_distance_m: `{metrics.get('red_stop_distance_m')}`",
        f"- green_pass_time_s: `{metrics.get('green_pass_time_s')}`",
        f"- red_to_green_release_time_s: `{metrics.get('red_to_green_release_time_s')}`",
        "",
        "This report is artifact-derived and does not prove full Apollo perception.",
        "",
    ]
    return "\n".join(lines)
