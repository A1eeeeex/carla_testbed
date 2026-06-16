from __future__ import annotations

import json
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROUTE_START_PROBE_PLAN_SCHEMA_VERSION = "town01_route_start_probe_plan.v1"
ROUTE_START_PROBE_RESULT_SCHEMA_VERSION = "town01_route_start_probe_result.v1"
DEFAULT_SUITE_PATH = "configs/scenarios/town01_natural_driving_suite.yaml"
DEFAULT_ROUTE_HEALTH_CONFIG = "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
DEFAULT_FAIL_ON_STATUS = "fail,warn,insufficient_data"
NON_BLOCKING_PROBE_MISSING_FIELDS = {"control_latency_p95_ms"}


def build_route_start_probe_plan(
    natural_driving_report: Mapping[str, Any],
    *,
    source_report_path: str | Path | None = None,
    suite_path: str | Path = DEFAULT_SUITE_PATH,
    output_root: str | Path = "runs/route_start_alignment_probes",
    python_exec: str = sys.executable,
    route_health_config: str | Path = DEFAULT_ROUTE_HEALTH_CONFIG,
    fail_on_postprocess_status: str = DEFAULT_FAIL_ON_STATUS,
) -> dict[str, Any]:
    """Create online probe commands from route-start alignment evidence.

    The returned commands are intentionally probe-only: they pass an explicit
    `scenario.route_health.ego_offset_y_m` override and never change default
    configs, steer scale, or actuator mapping.
    """

    probes: list[dict[str, Any]] = []
    root = Path(output_root)
    for run in natural_driving_report.get("run_results") or []:
        if not isinstance(run, Mapping):
            continue
        candidate_reason = _route_start_probe_candidate_reason(run)
        if candidate_reason is None:
            continue
        delta = _num(run.get("recommended_ego_offset_y_delta_m"))
        source_scenario_id = str(run.get("scenario_id") or "").strip()
        if delta is None or not source_scenario_id:
            continue
        runner_scenario_id = _runner_scenario_id_for_run(run, suite_path) or source_scenario_id
        out_dir = root / f"{source_scenario_id}_route_start_alignment_probe"
        override = f"scenario.route_health.ego_offset_y_m={delta:.16g}"
        command_parts = [
            str(python_exec),
            "tools/run_town01_natural_driving_suite.py",
            "--suite",
            str(suite_path),
            "--out",
            str(out_dir),
            "--scenarios",
            runner_scenario_id,
            "--config",
            str(route_health_config),
            "--continue-on-failure",
            "--carla-ignore-memory-preflight",
            "--postprocess-after-run",
            "--refresh-postprocess",
            "--fail-on-postprocess-status",
            str(fail_on_postprocess_status),
            "--override",
            override,
        ]
        probe_report_path = out_dir / "analysis" / "natural_driving" / "natural_driving_report.json"
        result_out_dir = out_dir / "analysis" / "route_start_probe_result"
        result_command = None
        if source_report_path is not None:
            result_command_parts = [
                str(python_exec),
                "tools/analyze_route_start_probe_result.py",
                "--source-report",
                str(source_report_path),
                "--probe-report",
                str(probe_report_path),
                "--out",
                str(result_out_dir),
                "--scenario-id",
                source_scenario_id,
                "--fail-on-status",
                "negative,insufficient_data",
            ]
            result_command = " ".join(shlex.quote(part) for part in result_command_parts)
        probes.append(
            {
                "probe_id": f"{source_scenario_id}_route_start_alignment_probe",
                "source_run_id": run.get("run_id"),
                "scenario_id": source_scenario_id,
                "runner_scenario_id": runner_scenario_id,
                "scenario_class": run.get("scenario_class"),
                "route_id": run.get("route_id"),
                "failure_reason": run.get("failure_reason"),
                "route_start_alignment_reason": run.get("route_start_alignment_reason"),
                "candidate_reason": candidate_reason,
                "failure_route_s": run.get("failure_route_s"),
                "static_spawn_lateral_offset_m": run.get("static_spawn_lateral_offset_m"),
                "recommended_config_field": "scenario.route_health.ego_offset_y_m",
                "recommended_ego_offset_y_delta_m": delta,
                "override": override,
                "out_dir": str(out_dir),
                "command": " ".join(shlex.quote(part) for part in command_parts),
                "probe_report_path": str(probe_report_path),
                "result_out_dir": str(result_out_dir),
                "result_command": result_command,
                "expected_followup_artifacts": [
                    "natural_driving_report.json",
                    "analysis/route_start_probe_result/route_start_probe_result.json",
                    "analysis/route_start_alignment/route_start_alignment_report.json",
                    "analysis/failure_timeline/failure_timeline_report.json",
                    "analysis/route_health/route_health.json",
                    "analysis/control_health/control_health_report.json",
                ],
                "success_interpretation": (
                    "A successful probe may support spawn/route-start alignment as a lane097 factor. "
                    "It does not prove curve tracking or justify mainline promotion by itself."
                ),
            }
        )

    status = "ready" if probes else "no_probe_candidates"
    return {
        "schema_version": ROUTE_START_PROBE_PLAN_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "source_report_path": str(source_report_path) if source_report_path is not None else None,
        "suite_path": str(suite_path),
        "route_health_config": str(route_health_config),
        "output_root": str(root),
        "probe_count": len(probes),
        "probes": probes,
        "claim_boundary": {
            "probe_only": True,
            "does_not_change_default_config": True,
            "does_not_change_steer_scale": True,
            "does_not_enable_physical_mapping": True,
            "does_not_prove_curve_health": True,
            "needs_local_carla": True,
            "needs_local_apollo": True,
        },
    }


def build_route_start_probe_plan_from_file(
    report_path: str | Path,
    **kwargs: Any,
) -> dict[str, Any]:
    path = Path(report_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"natural driving report must be a JSON object: {path}")
    return build_route_start_probe_plan(payload, source_report_path=path, **kwargs)


def _runner_scenario_id_for_run(run: Mapping[str, Any], suite_path: str | Path) -> str | None:
    route_id = str(run.get("route_id") or "").strip()
    if not route_id:
        return None
    try:
        import yaml
    except Exception:
        return None
    path = Path(suite_path)
    if not path.exists():
        return None
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None
    scenarios = payload.get("scenarios") if isinstance(payload, Mapping) else None
    if not isinstance(scenarios, Sequence) or isinstance(scenarios, (str, bytes)):
        return None
    for item in scenarios:
        if not isinstance(item, Mapping):
            continue
        scenario_id = str(item.get("scenario_id") or "").strip()
        if not scenario_id:
            continue
        candidates = {
            str(item.get("route_id") or "").strip(),
            str(item.get("stable_id") or "").strip(),
            str(item.get("route_ref") or "").strip(),
        }
        if route_id in candidates:
            return scenario_id
    return None


def write_route_start_probe_plan(plan: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "route_start_probe_plan.json"
    md_path = output_dir / "route_start_probe_plan.md"
    sh_path = output_dir / "run_route_start_probes.sh"
    json_path.write_text(json.dumps(dict(plan), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(plan), encoding="utf-8")
    sh_path.write_text(_shell_script(plan), encoding="utf-8")
    sh_path.chmod(0o755)
    return {
        "route_start_probe_plan": str(json_path),
        "route_start_probe_plan_summary": str(md_path),
        "route_start_probe_script": str(sh_path),
    }


def evaluate_route_start_probe_result(
    source_report: Mapping[str, Any],
    probe_report: Mapping[str, Any],
    *,
    source_report_path: str | Path | None = None,
    probe_report_path: str | Path | None = None,
    scenario_id: str | None = None,
    min_completion_delta_for_positive: float = 0.02,
) -> dict[str, Any]:
    source_run = _select_source_probe_candidate(source_report, scenario_id=scenario_id)
    missing_inputs: list[str] = []
    if source_run is None:
        missing_inputs.append("source_route_start_lane_invasion_run")
    selected_scenario_id = str(scenario_id or (source_run or {}).get("scenario_id") or "").strip() or None
    probe_run = _select_probe_run(probe_report, scenario_id=selected_scenario_id)
    if probe_run is None:
        missing_inputs.append("probe_run")
    if missing_inputs:
        return _probe_result_payload(
            status="insufficient_data",
            reason="missing_probe_inputs",
            source_run=source_run,
            probe_run=probe_run,
            source_report_path=source_report_path,
            probe_report_path=probe_report_path,
            scenario_id=selected_scenario_id,
            comparison={},
            missing_inputs=missing_inputs,
        )

    assert source_run is not None
    assert probe_run is not None
    comparison = _probe_comparison(source_run, probe_run)
    status, reason = _probe_result_status(
        source_run,
        probe_run,
        comparison=comparison,
        min_completion_delta_for_positive=min_completion_delta_for_positive,
    )
    return _probe_result_payload(
        status=status,
        reason=reason,
        source_run=source_run,
        probe_run=probe_run,
        source_report_path=source_report_path,
        probe_report_path=probe_report_path,
        scenario_id=selected_scenario_id,
        comparison=comparison,
        missing_inputs=[],
    )


def evaluate_route_start_probe_result_from_files(
    source_report_path: str | Path,
    probe_report_path: str | Path,
    **kwargs: Any,
) -> dict[str, Any]:
    source_path = Path(source_report_path)
    probe_path = Path(probe_report_path)
    source = json.loads(source_path.read_text(encoding="utf-8"))
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    if not isinstance(source, Mapping):
        raise ValueError(f"source report must be a JSON object: {source_path}")
    if not isinstance(probe, Mapping):
        raise ValueError(f"probe report must be a JSON object: {probe_path}")
    return evaluate_route_start_probe_result(
        source,
        probe,
        source_report_path=source_path,
        probe_report_path=probe_path,
        **kwargs,
    )


def write_route_start_probe_result(result: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "route_start_probe_result.json"
    md_path = output_dir / "route_start_probe_result.md"
    json_path.write_text(json.dumps(dict(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_result_markdown(result), encoding="utf-8")
    return {
        "route_start_probe_result": str(json_path),
        "route_start_probe_result_summary": str(md_path),
    }


def _markdown(plan: Mapping[str, Any]) -> str:
    lines = [
        "# Route Start Probe Plan",
        "",
        f"- status: `{plan.get('status')}`",
        f"- probe_count: `{plan.get('probe_count')}`",
        f"- suite_path: `{plan.get('suite_path')}`",
        "",
        "This plan is generated from artifacts. It is probe-only and does not change default configs.",
        "",
    ]
    for probe in plan.get("probes") or []:
        if not isinstance(probe, Mapping):
            continue
        lines.extend(
            [
                f"## {probe.get('probe_id')}",
                "",
                f"- source_run_id: `{probe.get('source_run_id')}`",
                f"- failure_reason: `{probe.get('failure_reason')}`",
                f"- failure_route_s: `{probe.get('failure_route_s')}`",
                f"- override: `{probe.get('override')}`",
                "",
                "```bash",
                str(probe.get("command") or ""),
                "```",
                "",
                "### Result Comparison",
                "",
                "```bash",
                str(probe.get("result_command") or "# source_report_path unavailable; compare manually after probe."),
                "```",
                "",
            ]
        )
    boundary = plan.get("claim_boundary") if isinstance(plan.get("claim_boundary"), Mapping) else {}
    lines.extend(
        [
            "## Boundary",
            "",
            f"- probe_only: `{boundary.get('probe_only')}`",
            f"- does_not_change_steer_scale: `{boundary.get('does_not_change_steer_scale')}`",
            f"- does_not_enable_physical_mapping: `{boundary.get('does_not_enable_physical_mapping')}`",
            f"- does_not_prove_curve_health: `{boundary.get('does_not_prove_curve_health')}`",
            "",
        ]
    )
    return "\n".join(lines)


def _result_markdown(result: Mapping[str, Any]) -> str:
    comparison = result.get("comparison") if isinstance(result.get("comparison"), Mapping) else {}
    source = result.get("source_run") if isinstance(result.get("source_run"), Mapping) else {}
    probe = result.get("probe_run") if isinstance(result.get("probe_run"), Mapping) else {}
    lines = [
        "# Route Start Probe Result",
        "",
        f"- status: `{result.get('status')}`",
        f"- reason: `{result.get('reason')}`",
        f"- scenario_id: `{result.get('scenario_id')}`",
        f"- evidence_warnings: `{result.get('evidence_warnings')}`",
        "",
        "## Source",
        "",
        f"- run_id: `{source.get('run_id')}`",
        f"- failure_reason: `{source.get('failure_reason')}`",
        f"- route_completion: `{source.get('route_completion')}`",
        f"- lane_invasion_count: `{source.get('lane_invasion_count')}`",
        "",
        "## Probe",
        "",
        f"- run_id: `{probe.get('run_id')}`",
        f"- verdict: `{probe.get('verdict')}`",
        f"- failure_reason: `{probe.get('failure_reason')}`",
        f"- route_completion: `{probe.get('route_completion')}`",
        f"- lane_invasion_count: `{probe.get('lane_invasion_count')}`",
        "",
        "## Comparison",
        "",
        f"- lane_invasion_resolved: `{comparison.get('lane_invasion_resolved')}`",
        f"- route_completion_delta: `{comparison.get('route_completion_delta')}`",
        f"- lateral_error_p95_delta: `{comparison.get('lateral_error_p95_delta')}`",
        f"- heading_error_p95_delta: `{comparison.get('heading_error_p95_delta')}`",
        "",
        "This result evaluates one route-start alignment probe only. It does not prove curve health "
        "or justify changing steer scale, physical mapping, or default backend.",
        "",
    ]
    return "\n".join(lines)


def _shell_script(plan: Mapping[str, Any]) -> str:
    probes = [probe for probe in plan.get("probes") or [] if isinstance(probe, Mapping)]
    lines = [
        "#!/usr/bin/env bash",
        "set -uo pipefail",
        "",
        "# Generated from route-start alignment artifacts.",
        "# Probe-only: does not change defaults, steer_scale, or physical mapping.",
        "overall_rc=0",
    ]
    if not any(probe.get("command") for probe in probes):
        lines.append("echo 'No route-start probe candidates.'")
    else:
        for probe in probes:
            command = probe.get("command")
            probe_report_path = probe.get("probe_report_path")
            result_command = probe.get("result_command")
            if command:
                lines.append("")
                lines.append(f"echo '--- running {probe.get('probe_id') or 'route-start probe'} ---'")
                lines.append("probe_rc=0")
                lines.append(str(command))
                lines.append("probe_rc=$?")
                lines.append("if [ \"$probe_rc\" -ne 0 ]; then")
                lines.append("  overall_rc=\"$probe_rc\"")
                lines.append("fi")
            if result_command and probe_report_path:
                lines.append(f"if [ -f {shlex.quote(str(probe_report_path))} ]; then")
                lines.append("  result_rc=0")
                lines.append(f"  {result_command}")
                lines.append("  result_rc=$?")
                lines.append("  if [ \"$result_rc\" -ne 0 ]; then")
                lines.append("    overall_rc=\"$result_rc\"")
                lines.append("  fi")
                lines.append("else")
                lines.append(
                    f"  echo 'Missing probe natural_driving_report.json: {shlex.quote(str(probe_report_path))}' >&2"
                )
                lines.append("  if [ \"$overall_rc\" -eq 0 ]; then")
                lines.append("    overall_rc=1")
                lines.append("  fi")
                lines.append("fi")
        lines.append("")
        lines.append("exit \"$overall_rc\"")
    return "\n".join(lines) + "\n"


def _select_source_probe_candidate(
    report: Mapping[str, Any],
    *,
    scenario_id: str | None,
) -> Mapping[str, Any] | None:
    for run in report.get("run_results") or []:
        if not isinstance(run, Mapping):
            continue
        if scenario_id and str(run.get("scenario_id") or "") != str(scenario_id):
            continue
        if _route_start_probe_candidate_reason(run) is not None:
            return run
    return None


def _select_probe_run(report: Mapping[str, Any], *, scenario_id: str | None) -> Mapping[str, Any] | None:
    runs = [run for run in (report.get("run_results") or []) if isinstance(run, Mapping)]
    if scenario_id:
        for run in runs:
            if str(run.get("scenario_id") or "") == str(scenario_id):
                return run
    return runs[0] if len(runs) == 1 else None


def _probe_comparison(source: Mapping[str, Any], probe: Mapping[str, Any]) -> dict[str, Any]:
    source_completion = _num(source.get("route_completion"))
    probe_completion = _num(probe.get("route_completion"))
    source_lateral = _num(source.get("lateral_error_p95"))
    probe_lateral = _num(probe.get("lateral_error_p95"))
    source_heading = _num(source.get("heading_error_p95"))
    probe_heading = _num(probe.get("heading_error_p95"))
    source_lane = _num(source.get("lane_invasion_count")) or 0.0
    probe_lane = _num(probe.get("lane_invasion_count")) or 0.0
    return {
        "lane_invasion_resolved": source_lane > 0 and probe_lane <= 0,
        "route_start_lane_invasion_persisted": str(probe.get("failure_reason") or "") == "route_start_lane_invasion",
        "source_probe_candidate_reason": _route_start_probe_candidate_reason(source),
        "source_failure_reason": source.get("failure_reason"),
        "probe_failure_reason": probe.get("failure_reason"),
        "source_verdict": source.get("verdict"),
        "probe_verdict": probe.get("verdict"),
        "source_route_completion": source_completion,
        "probe_route_completion": probe_completion,
        "route_completion_delta": None
        if source_completion is None or probe_completion is None
        else probe_completion - source_completion,
        "source_lateral_error_p95": source_lateral,
        "probe_lateral_error_p95": probe_lateral,
        "lateral_error_p95_delta": None if source_lateral is None or probe_lateral is None else probe_lateral - source_lateral,
        "source_heading_error_p95": source_heading,
        "probe_heading_error_p95": probe_heading,
        "heading_error_p95_delta": None if source_heading is None or probe_heading is None else probe_heading - source_heading,
    }


def _probe_result_status(
    source: Mapping[str, Any],
    probe: Mapping[str, Any],
    *,
    comparison: Mapping[str, Any],
    min_completion_delta_for_positive: float,
) -> tuple[str, str]:
    blocking_missing_fields = _blocking_probe_missing_fields(probe)
    if probe.get("missing_artifacts") or blocking_missing_fields:
        return "insufficient_data", "probe_missing_required_evidence"
    if str(probe.get("failure_reason") or "") == "route_start_lane_invasion":
        return "negative", "route_start_lane_invasion_persisted"
    if str(probe.get("failure_reason") or "") in {"routing_missing", "planning_missing", "control_handoff_not_consuming"}:
        return "inconclusive", "probe_link_health_failure"
    if str(probe.get("failure_reason") or "") in {"runtime_contract_not_aligned", "missing_required_artifacts"}:
        return "insufficient_data", "probe_invalid_artifacts_or_runtime"
    completion_delta = _num(comparison.get("route_completion_delta"))
    if completion_delta is None:
        return "inconclusive", "missing_route_completion_comparison"
    source_candidate_reason = str(comparison.get("source_probe_candidate_reason") or "")
    if source_candidate_reason == "route_start_lane_invasion":
        if comparison.get("lane_invasion_resolved") is not True:
            return "inconclusive", "lane_invasion_not_resolved_or_unobserved"
    elif source_candidate_reason.startswith("route_start_alignment:"):
        if str(probe.get("failure_reason") or "") in {
            "collision",
            "lane_invasion",
            "off_route",
            "high_lateral_error",
            "heading_divergence",
        }:
            return "negative", "route_start_alignment_probe_safety_or_tracking_regressed"
    else:
        return "inconclusive", "source_not_route_start_probe_candidate"
    if completion_delta >= float(min_completion_delta_for_positive):
        return "positive", "route_start_alignment_probe_supported"
    if _num(probe.get("route_completion")) is not None and _num(source.get("route_completion")) is not None:
        return "inconclusive", "lane_invasion_resolved_without_completion_gain"
    return "inconclusive", "probe_result_unclear"


def _probe_result_payload(
    *,
    status: str,
    reason: str,
    source_run: Mapping[str, Any] | None,
    probe_run: Mapping[str, Any] | None,
    source_report_path: str | Path | None,
    probe_report_path: str | Path | None,
    scenario_id: str | None,
    comparison: Mapping[str, Any],
    missing_inputs: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": ROUTE_START_PROBE_RESULT_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "reason": reason,
        "scenario_id": scenario_id,
        "source_report_path": str(source_report_path) if source_report_path is not None else None,
        "probe_report_path": str(probe_report_path) if probe_report_path is not None else None,
        "source_run": dict(source_run or {}),
        "probe_run": dict(probe_run or {}),
        "comparison": dict(comparison),
        "missing_inputs": list(missing_inputs),
        "evidence_warnings": _probe_evidence_warnings(probe_run),
        "claim_boundary": {
            "probe_only": True,
            "can_claim_lane097_route_start_alignment_factor": status == "positive",
            "can_claim_curve_health": False,
            "can_change_default_config": False,
            "can_change_steer_scale": False,
            "can_enable_physical_mapping": False,
        },
        "required_next_actions": _required_next_actions(status, reason),
    }


def _required_next_actions(status: str, reason: str) -> list[str]:
    if status == "positive":
        return [
            "Repeat lane_keep_097 with the same override to check repeatability.",
            "Run lane_keep_217 and junction_031 no-regression before considering any mainline config candidate.",
            "Do not apply the override to curve diagnostics without separate route-health evidence.",
        ]
    if status == "negative":
        return [
            "Do not promote the spawn lateral alignment override.",
            "Inspect route_start_alignment, failure_timeline, and control_health artifacts for the probe run.",
        ]
    if status == "insufficient_data":
        return [
            f"Regenerate or inspect missing evidence for reason={reason}.",
            "Do not use this probe as behavior evidence.",
        ]
    return [
        "Classify the probe failure layer before rerunning.",
        "Do not treat this as support or rejection of the alignment hypothesis yet.",
    ]


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _blocking_probe_missing_fields(probe: Mapping[str, Any]) -> list[str]:
    return [
        str(field)
        for field in (probe.get("missing_fields") or [])
        if str(field) not in NON_BLOCKING_PROBE_MISSING_FIELDS
    ]


def _probe_evidence_warnings(probe: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(probe, Mapping):
        return []
    warnings: list[str] = []
    non_blocking = [
        str(field)
        for field in (probe.get("missing_fields") or [])
        if str(field) in NON_BLOCKING_PROBE_MISSING_FIELDS
    ]
    for field in non_blocking:
        warnings.append(f"probe_missing_non_blocking_field:{field}")
    verdict = str(probe.get("verdict") or "")
    if verdict == "insufficient_data" and non_blocking:
        warnings.append("probe_natural_driving_verdict_insufficient_data_due_non_blocking_fields")
    elif verdict == "warn" and non_blocking:
        warnings.append("probe_natural_driving_verdict_warn_due_non_blocking_fields")
    return sorted(set(warnings))


def _route_start_probe_candidate_reason(run: Mapping[str, Any]) -> str | None:
    if _num(run.get("recommended_ego_offset_y_delta_m")) is None:
        return None
    if not str(run.get("scenario_id") or "").strip():
        return None
    failure_reason = str(run.get("failure_reason") or "")
    if failure_reason == "route_start_lane_invasion":
        return "route_start_lane_invasion"
    route_start_reason = str(run.get("route_start_alignment_reason") or "")
    if route_start_reason in {
        "failure_before_route_start",
        "failure_near_route_start",
        "spawn_lateral_offset_high",
    } and failure_reason in {
        "collision",
        "route_completion_too_low",
        "lane_invasion",
        "lateral_error_too_high",
        "heading_error_too_high",
    }:
        return f"route_start_alignment:{route_start_reason}"
    return None
