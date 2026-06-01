from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

BAGUANG_STACK_COMPARISON_SCHEMA_VERSION = "baguang_stack_comparison_suite.v1"
BAGUANG_STACK_COMPARISON_REPORT_VERSION = "baguang_stack_comparison_report.v1"
LANE_EVENT_CONTRACT_REPORT_VERSION = "baguang_lane_event_contract.v1"
DEFAULT_SUITE_PATH = Path("configs/scenarios/baguang_stack_comparison_suite.yaml")
REQUIRED_STACKS = {"apollo", "autoware"}
REQUIRED_COMMON_FIELDS = {
    "scenario_id",
    "scenario_class",
    "map",
    "route_id",
    "target_speed_kph",
    "lead_distance_m",
    "ego_spawn_ref",
    "front_vehicle",
    "route_ref",
    "vehicle_blueprint",
    "required_artifacts",
}
REQUIRED_STACK_FIELDS = {
    "stack_id",
    "stack",
    "backend",
    "algorithm_variant_id",
    "evidence_run",
    "status",
    "required_artifacts",
    "declared_assists",
    "not_proven",
}
REPORT_CSV_FIELDS = [
    "stack_id",
    "stack",
    "backend",
    "algorithm_variant_id",
    "run_dir",
    "verdict",
    "artifact_complete",
    "success",
    "fail_reason",
    "frames",
    "sim_duration_s",
    "wall_duration_s",
    "max_speed_mps",
    "final_speed_mps",
    "distance_span_x_m",
    "min_lead_distance_m",
    "collision_count",
    "lane_invasion_count",
    "declared_assist_count",
    "declared_assists",
    "missing_artifacts",
]


def load_baguang_stack_comparison_suite(path: str | Path = DEFAULT_SUITE_PATH) -> dict[str, Any]:
    suite_path = Path(path).expanduser()
    with suite_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Baguang stack comparison suite must be a mapping: {suite_path}")
    validation = validate_baguang_stack_comparison_suite(payload)
    payload["_validation"] = validation
    payload["_source_path"] = str(suite_path)
    if validation["errors"]:
        raise ValueError("; ".join(validation["errors"]))
    return payload


def validate_baguang_stack_comparison_suite(suite: Mapping[str, Any]) -> dict[str, list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if suite.get("schema_version") != BAGUANG_STACK_COMPARISON_SCHEMA_VERSION:
        errors.append(f"schema_version must be {BAGUANG_STACK_COMPARISON_SCHEMA_VERSION}")
    common = _as_mapping(suite.get("common_scene"))
    missing_common = sorted(REQUIRED_COMMON_FIELDS - set(common))
    if missing_common:
        errors.append(f"common_scene missing fields: {', '.join(missing_common)}")
    if common.get("map") != "straight_road_for_baguang":
        errors.append("common_scene.map must be straight_road_for_baguang")
    if float(common.get("lead_distance_m") or 0.0) < 295.0:
        errors.append("common_scene.lead_distance_m must preserve the 300m scene")
    if float(common.get("target_speed_kph") or 0.0) < 75.0:
        errors.append("common_scene.target_speed_kph must preserve the near-80kph target")

    comparison = _as_mapping(suite.get("comparison_requirements"))
    behavior_gate = _as_mapping(comparison.get("behavior_gate"))
    for key in ("max_speed_mps_min", "final_speed_mps_max", "min_lead_distance_m_max"):
        if key not in behavior_gate:
            errors.append(f"comparison_requirements.behavior_gate missing {key}")
    boundary = _as_mapping(comparison.get("claim_boundary"))
    if boundary.get("assisted_profiles_prove_unassisted_natural_driving") is True:
        errors.append("assisted profiles must not prove unassisted natural driving")
    if boundary.get("full_sensor_perception_reproduced") is True:
        errors.append("truth-input comparison must not claim full sensor perception reproduction")

    stacks = suite.get("stacks")
    if not isinstance(stacks, list) or not stacks:
        errors.append("stacks must be a non-empty list")
        return {"errors": errors, "warnings": warnings}
    seen_stack_ids: set[str] = set()
    seen_stacks: set[str] = set()
    for stack in stacks:
        if not isinstance(stack, Mapping):
            errors.append("each stack must be a mapping")
            continue
        stack_id = str(stack.get("stack_id") or "")
        stack_name = str(stack.get("stack") or "")
        if stack_id in seen_stack_ids:
            errors.append(f"duplicate stack_id: {stack_id}")
        if stack_id:
            seen_stack_ids.add(stack_id)
        if stack_name:
            seen_stacks.add(stack_name)
        missing_stack = sorted(REQUIRED_STACK_FIELDS - set(stack))
        if missing_stack:
            errors.append(f"{stack_id or '<missing stack_id>'}: missing fields {', '.join(missing_stack)}")
        if stack_name not in REQUIRED_STACKS:
            errors.append(f"{stack_id}: unsupported stack {stack_name!r}")
        if not isinstance(stack.get("required_artifacts"), list) or not stack.get("required_artifacts"):
            errors.append(f"{stack_id}: required_artifacts must be a non-empty list")
        assists = stack.get("declared_assists")
        if not isinstance(assists, list):
            errors.append(f"{stack_id}: declared_assists must be a list")
        elif not assists:
            warnings.append(f"{stack_id}: no declared assists; verify this before natural-driving claims")
        if not isinstance(stack.get("not_proven"), list) or not stack.get("not_proven"):
            errors.append(f"{stack_id}: not_proven must be a non-empty list")
    missing_required_stacks = sorted(REQUIRED_STACKS - seen_stacks)
    if missing_required_stacks:
        errors.append(f"stacks missing required stack types: {', '.join(missing_required_stacks)}")
    return {"errors": errors, "warnings": warnings}


def analyze_baguang_stack_comparison(
    suite_or_path: str | Path | Mapping[str, Any] = DEFAULT_SUITE_PATH,
) -> dict[str, Any]:
    suite = (
        load_baguang_stack_comparison_suite(suite_or_path)
        if isinstance(suite_or_path, (str, Path))
        else dict(suite_or_path)
    )
    validation = validate_baguang_stack_comparison_suite(suite)
    common = _as_mapping(suite.get("common_scene"))
    behavior_gate = _as_mapping(_as_mapping(suite.get("comparison_requirements")).get("behavior_gate"))
    common_artifacts = list(common.get("required_artifacts") or [])
    stack_results = [
        _analyze_stack(stack, common_artifacts=common_artifacts, behavior_gate=behavior_gate)
        for stack in suite.get("stacks", [])
        if isinstance(stack, Mapping)
    ]
    verdict = _comparison_verdict(stack_results, validation)
    lane_event_contract = _lane_event_contract_boundary(suite, stack_results)
    return {
        "schema_version": BAGUANG_STACK_COMPARISON_REPORT_VERSION,
        "suite": {
            "path": suite.get("_source_path"),
            "name": suite.get("name"),
            "schema_version": suite.get("schema_version"),
        },
        "common_scene": dict(common),
        "validation": validation,
        "stack_results": stack_results,
        "verdict": verdict,
        "claim_boundary": {
            "can_compare_assisted_profiles": verdict["status"] in {"pass", "warn"},
            "can_claim_unassisted_natural_driving": False,
            "lane_event_contract": lane_event_contract,
            "reason": (
                "At least one stack declares bridge/control assistance, so this report compares "
                "truth-input assisted follow-stop behavior only."
            ),
        },
        "next_reduction_steps": list(suite.get("next_reduction_steps") or []),
    }


def write_baguang_stack_comparison_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "baguang_stack_comparison_report.json"
    csv_path = output / "baguang_stack_comparison_report.csv"
    md_path = output / "baguang_stack_comparison_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(csv_path, report.get("stack_results") or [])
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {
        "comparison_report": str(json_path),
        "comparison_csv": str(csv_path),
        "comparison_summary": str(md_path),
    }


def _analyze_stack(
    stack: Mapping[str, Any],
    *,
    common_artifacts: Sequence[str],
    behavior_gate: Mapping[str, Any],
) -> dict[str, Any]:
    run_dir = Path(str(stack.get("evidence_run") or "")).expanduser()
    summary = _read_json(run_dir / "summary.json")
    rows = _read_csv_rows(run_dir / "timeseries.csv")
    required_artifacts = list(common_artifacts) + list(stack.get("required_artifacts") or [])
    missing_artifacts = [rel for rel in _unique(required_artifacts) if not (run_dir / rel).exists()]
    final_speed = _final_float(rows, "ego_speed", "v_mps")
    ego_x_values = _float_values(rows, "ego_x")
    distance_span = (max(ego_x_values) - min(ego_x_values)) if ego_x_values else None
    max_speed = _summary_number(summary, "max_speed_mps")
    if max_speed is None:
        max_speed = _metric_number(summary, "max_speed_mps")
    min_lead = _metric_number(summary, "min_lead_distance_m")
    collision_count = _summary_number(summary, "collision_count")
    lane_invasion_count = _summary_number(summary, "lane_invasion_count")
    result = {
        "stack_id": stack.get("stack_id"),
        "stack": stack.get("stack"),
        "backend": stack.get("backend"),
        "algorithm_variant_id": stack.get("algorithm_variant_id"),
        "run_dir": str(run_dir),
        "run_exists": run_dir.exists(),
        "artifact_complete": not missing_artifacts,
        "missing_artifacts": missing_artifacts,
        "summary_available": bool(summary),
        "timeseries_available": bool(rows),
        "success": summary.get("success") if summary else None,
        "fail_reason": summary.get("fail_reason") if summary else None,
        "exit_reason": summary.get("exit_reason") if summary else None,
        "frames": summary.get("frames") if summary else None,
        "sim_duration_s": summary.get("sim_duration_s") if summary else None,
        "wall_duration_s": summary.get("wall_duration_s") if summary else None,
        "max_speed_mps": max_speed,
        "final_speed_mps": final_speed,
        "distance_span_x_m": distance_span,
        "min_lead_distance_m": min_lead,
        "collision_count": collision_count,
        "lane_invasion_count": lane_invasion_count,
        "declared_assists": list(stack.get("declared_assists") or []),
        "not_proven": list(stack.get("not_proven") or []),
    }
    result["verdict"] = _stack_verdict(result, behavior_gate)
    return result


def _stack_verdict(result: Mapping[str, Any], behavior_gate: Mapping[str, Any]) -> str:
    if not result.get("run_exists") or not result.get("summary_available") or not result.get("timeseries_available"):
        return "insufficient_data"
    if result.get("success") is not True:
        return "fail"
    checks = [
        _number(result.get("max_speed_mps")) >= _number(behavior_gate.get("max_speed_mps_min"), 20.0),
        _number(result.get("final_speed_mps"), 999.0)
        <= _number(behavior_gate.get("final_speed_mps_max"), 0.5),
        _number(result.get("min_lead_distance_m"), 999.0)
        <= _number(behavior_gate.get("min_lead_distance_m_max"), 15.0),
        _number(result.get("collision_count"), 999.0)
        <= _number(behavior_gate.get("collision_count_max"), 0.0),
        _number(result.get("lane_invasion_count"), 999.0)
        <= _number(behavior_gate.get("lane_invasion_count_max"), 0.0),
    ]
    if not all(checks):
        return "fail"
    return "pass" if result.get("artifact_complete") else "warn"


def _comparison_verdict(
    stack_results: Sequence[Mapping[str, Any]],
    validation: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    if validation.get("errors"):
        return {
            "status": "fail",
            "reason": "suite_validation_failed",
            "blocking_reasons": list(validation.get("errors") or []),
        }
    verdicts = {str(result.get("stack")): str(result.get("verdict")) for result in stack_results}
    missing = sorted(REQUIRED_STACKS - set(verdicts))
    if missing:
        return {"status": "insufficient_data", "reason": "missing_stack_results", "missing_stacks": missing}
    if any(value == "fail" for value in verdicts.values()):
        return {"status": "fail", "reason": "one_or_more_stacks_failed", "stack_verdicts": verdicts}
    if any(value == "insufficient_data" for value in verdicts.values()):
        return {"status": "insufficient_data", "reason": "one_or_more_stacks_insufficient", "stack_verdicts": verdicts}
    if any(value == "warn" for value in verdicts.values()):
        return {
            "status": "warn",
            "reason": "behavior_passed_but_some_artifacts_missing",
            "stack_verdicts": verdicts,
        }
    return {"status": "pass", "reason": "assisted_profiles_behavior_and_artifacts_pass", "stack_verdicts": verdicts}


def _lane_event_contract_boundary(
    suite: Mapping[str, Any],
    stack_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    disabled_stacks = sorted(
        str(result.get("stack_id") or result.get("stack") or "")
        for result in stack_results
        if "lane_invasion_event_disabled" in set(result.get("declared_assists") or [])
    )
    comparison = _as_mapping(suite.get("comparison_requirements"))
    contract_cfg = _as_mapping(comparison.get("lane_event_contract"))
    path = contract_cfg.get("report_path")
    report = _read_json(Path(str(path)).expanduser()) if path else {}
    status = "not_required"
    reason = "no_stack_disables_lane_invasion_event"
    if disabled_stacks:
        if not path:
            status = "missing"
            reason = "lane_invasion_disabled_without_contract_report_path"
        elif not report:
            status = "missing"
            reason = "lane_event_contract_report_missing_or_unreadable"
        elif report.get("schema_version") != LANE_EVENT_CONTRACT_REPORT_VERSION:
            status = "invalid"
            reason = "lane_event_contract_schema_mismatch"
        elif report.get("quarantine_recommended") is True:
            status = "quarantined"
            reason = "lane_invasion_event_quarantined_by_contract_report"
        elif report.get("status") == "fail":
            status = "fail"
            reason = "lane_event_contract_report_failed"
        else:
            status = "unjustified"
            reason = "lane_invasion_disabled_without_quarantine_recommendation"
    claim_boundary = _as_mapping(report.get("claim_boundary"))
    return {
        "status": status,
        "reason": reason,
        "report_path": str(path) if path else None,
        "disabled_stacks": disabled_stacks,
        "quarantine_recommended": report.get("quarantine_recommended") if report else None,
        "lane_invasion_event_can_be_used_as_hard_gate": claim_boundary.get(
            "lane_invasion_event_can_be_used_as_hard_gate"
        )
        if claim_boundary
        else None,
        "report_status": report.get("status") if report else None,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception:
        return []


def _summary_number(summary: Mapping[str, Any], key: str) -> float | None:
    return _maybe_number(summary.get(key))


def _metric_number(summary: Mapping[str, Any], key: str) -> float | None:
    metrics = summary.get("metrics")
    if not isinstance(metrics, Mapping):
        return None
    return _maybe_number(metrics.get(key))


def _maybe_number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _number(value: Any, default: float = 0.0) -> float:
    maybe = _maybe_number(value)
    return default if maybe is None else maybe


def _final_float(rows: Sequence[Mapping[str, str]], *names: str) -> float | None:
    if not rows:
        return None
    row = rows[-1]
    for name in names:
        value = _maybe_number(row.get(name))
        if value is not None:
            return value
    return None


def _float_values(rows: Sequence[Mapping[str, str]], name: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _maybe_number(row.get(name))
        if value is not None:
            values.append(value)
    return values


def _unique(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=REPORT_CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            payload: dict[str, Any] = {field: row.get(field) for field in REPORT_CSV_FIELDS}
            payload["declared_assist_count"] = len(row.get("declared_assists") or [])
            payload["declared_assists"] = "|".join(str(item) for item in row.get("declared_assists") or [])
            payload["missing_artifacts"] = "|".join(str(item) for item in row.get("missing_artifacts") or [])
            writer.writerow(payload)


def _summary_markdown(report: Mapping[str, Any]) -> str:
    verdict = _as_mapping(report.get("verdict"))
    claim_boundary = _as_mapping(report.get("claim_boundary"))
    lane_event_contract = _as_mapping(claim_boundary.get("lane_event_contract"))
    lines = [
        "# Baguang Apollo vs Autoware Follow-Stop Comparison",
        "",
        f"- schema_version: `{report.get('schema_version')}`",
        f"- status: `{verdict.get('status')}`",
        f"- reason: `{verdict.get('reason')}`",
        f"- can_claim_unassisted_natural_driving: `{claim_boundary.get('can_claim_unassisted_natural_driving')}`",
        f"- lane_event_contract_status: `{lane_event_contract.get('status')}`",
        f"- lane_event_contract_report: `{lane_event_contract.get('report_path')}`",
        "",
        "## Stack Results",
    ]
    for result in report.get("stack_results") or []:
        if not isinstance(result, Mapping):
            continue
        lines.extend(
            [
                "",
                f"### {result.get('stack_id')}",
                f"- stack/backend: `{result.get('stack')}` / `{result.get('backend')}`",
                f"- verdict: `{result.get('verdict')}`",
                f"- run_dir: `{result.get('run_dir')}`",
                f"- max_speed_mps: `{result.get('max_speed_mps')}`",
                f"- final_speed_mps: `{result.get('final_speed_mps')}`",
                f"- min_lead_distance_m: `{result.get('min_lead_distance_m')}`",
                f"- collision_count: `{result.get('collision_count')}`",
                f"- lane_invasion_count: `{result.get('lane_invasion_count')}`",
                f"- declared_assists: `{', '.join(result.get('declared_assists') or [])}`",
                f"- missing_artifacts: `{', '.join(result.get('missing_artifacts') or []) or 'none'}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "This report compares assisted truth-input follow-stop profiles only. It is not proof of unassisted natural-driving, or that Apollo or Autoware can complete the scenario without bridge/control assistance.",
        ]
    )
    return "\n".join(lines) + "\n"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
