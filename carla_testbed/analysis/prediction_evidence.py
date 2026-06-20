from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.algorithms.gt_replacement_matrix import (
    GTReplacementMatrixError,
    load_gt_replacement_matrix,
)

PREDICTION_EVIDENCE_SCHEMA_VERSION = "prediction_evidence.v1"
PREDICTION_CHANNEL = "/apollo/prediction"
_MIDDLEWARE_TOKEN = "cy" + "ber"
_BRIDGE_STATS_NAME = _MIDDLEWARE_TOKEN + "_bridge_stats"
OBSTACLES_CHANNEL = "/apollo/perception/obstacles"
DEFAULT_REPLACEMENT_MATRIX = "configs/reference/apollo_gt_replacement_matrix.yaml"
STATIC_BYPASS_SCENARIOS = {"lane_keep", "lane_keep_097"}
DYNAMIC_OR_INTERACTION_SCENARIOS = {
    "cut_in",
    "cut_out",
    "dynamic_obstacle",
    "follow_stop",
    "follow_stop_static",
    "junction_turn",
    "lead_accel",
    "lead_decel",
    "lead_decel_accel",
    "lead_hard_brake",
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}


def analyze_prediction_evidence_run_dir(
    run_dir: str | Path,
    *,
    replacement_matrix_path: str | Path | None = DEFAULT_REPLACEMENT_MATRIX,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    return analyze_prediction_evidence(
        summary=_read_json(_find_first(root, ["summary.json"])),
        manifest=_read_json(_find_first(root, ["manifest.json"])),
        channel_stats=_read_json(
            _find_first(root, ["channel_stats.json", "artifacts/channel_stats.json"])
        ),
        bridge_runtime_stats=_read_json(
            _find_first(root, ["artifacts/" + _BRIDGE_STATS_NAME + ".json", _BRIDGE_STATS_NAME + ".json"])
        ),
    planning_topic_debug_summary=_read_json(
            _find_first(
                root,
                [
                    "artifacts/planning_topic_debug_summary.json",
                    "planning_topic_debug_summary.json",
                ],
            )
        ),
        obstacle_gt_contract=_read_json(
            _find_first(
                root,
                [
                    "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
                    "obstacle_gt_contract_report.json",
                ],
            )
        ),
        scenario_metadata=_read_json(root / "artifacts" / "scenario_metadata.json"),
        prediction_log_paths=_find_prediction_logs(root),
        prediction_runtime_observed=_prediction_runtime_observed(root),
        replacement_matrix=_load_replacement_matrix(replacement_matrix_path),
    )


def analyze_prediction_evidence_files(
    *,
    channel_stats: str | Path | None = None,
    bridge_runtime_stats: str | Path | None = None,
    summary: str | Path | None = None,
    manifest: str | Path | None = None,
    planning_topic_debug_summary: str | Path | None = None,
    obstacle_gt_contract: str | Path | None = None,
    scenario_metadata: str | Path | None = None,
    prediction_logs: Sequence[str | Path] | None = None,
    prediction_runtime_observed: bool | None = None,
    replacement_matrix_path: str | Path | None = DEFAULT_REPLACEMENT_MATRIX,
) -> dict[str, Any]:
    return analyze_prediction_evidence(
        summary=_read_json(Path(summary).expanduser() if summary else None),
        manifest=_read_json(Path(manifest).expanduser() if manifest else None),
        channel_stats=_read_json(Path(channel_stats).expanduser() if channel_stats else None),
        bridge_runtime_stats=_read_json(
            Path(bridge_runtime_stats).expanduser() if bridge_runtime_stats else None
        ),
        planning_topic_debug_summary=_read_json(
            Path(planning_topic_debug_summary).expanduser()
            if planning_topic_debug_summary
            else None
        ),
        obstacle_gt_contract=_read_json(
            Path(obstacle_gt_contract).expanduser() if obstacle_gt_contract else None
        ),
        scenario_metadata=_read_json(Path(scenario_metadata).expanduser() if scenario_metadata else None),
        prediction_log_paths=[
            Path(path).expanduser() for path in prediction_logs or [] if Path(path).expanduser().exists()
        ],
        prediction_runtime_observed=prediction_runtime_observed,
        replacement_matrix=_load_replacement_matrix(replacement_matrix_path),
    )


def analyze_prediction_evidence(
    *,
    summary: Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
    channel_stats: Mapping[str, Any] | None = None,
    bridge_runtime_stats: Mapping[str, Any] | None = None,
    planning_topic_debug_summary: Mapping[str, Any] | None = None,
    obstacle_gt_contract: Mapping[str, Any] | None = None,
    scenario_metadata: Mapping[str, Any] | None = None,
    prediction_log_paths: Sequence[Path] = (),
    prediction_runtime_observed: bool | None = None,
    replacement_matrix: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = summary or {}
    manifest = manifest or {}
    channel_stats = channel_stats or {}
    bridge_runtime_stats = bridge_runtime_stats or {}
    planning_topic_debug_summary = planning_topic_debug_summary or {}
    obstacle_gt_contract = obstacle_gt_contract or {}
    scenario_metadata = scenario_metadata or {}
    replacement_matrix = replacement_matrix or {}

    manifest_metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), Mapping) else {}
    nested_scenario_metadata = (
        manifest_metadata.get("scenario_metadata")
        if isinstance(manifest_metadata.get("scenario_metadata"), Mapping)
        else {}
    )
    scenario_class = _first_text(
        summary,
        "scenario_class",
        manifest,
        "scenario_class",
        scenario_metadata,
        "scenario_class",
        nested_scenario_metadata,
        "scenario_class",
    )
    run_id = _first_text(summary, "run_id", manifest, "run_id")
    route_id = _first_text(
        summary,
        "route_id",
        manifest,
        "route_id",
        scenario_metadata,
        "route_id",
        nested_scenario_metadata,
        "route_id",
    )
    planning_requires_prediction = _planning_requires_prediction(
        summary,
        manifest,
        planning_topic_debug_summary,
    )
    bypass_reason = _first_text(
        manifest,
        "prediction_bypass_reason",
        summary,
        "prediction_bypass_reason",
        manifest,
        "bypass_reason",
    )
    bypass_reason_source: str | None = None
    if bypass_reason:
        bypass_reason_source = "manifest_or_summary"
    matrix_prediction = _matrix_prediction_module(replacement_matrix)
    if not bypass_reason:
        bypass_reason = _matrix_prediction_bypass_reason(matrix_prediction)
        if bypass_reason:
            bypass_reason_source = "replacement_matrix"
    explicit_allow_dynamic_bypass = _bool_or_none(
        _first_raw(
            manifest,
            "allow_prediction_bypass_for_dynamic_case",
            summary,
            "allow_prediction_bypass_for_dynamic_case",
        )
    )

    prediction_channel = _find_channel(channel_stats, PREDICTION_CHANNEL, "prediction")
    obstacles_channel = _find_channel(channel_stats, OBSTACLES_CHANNEL, "obstacles", "perception_obstacles")
    prediction_count = _message_count(prediction_channel)
    prediction_hz = _number(_channel_value(prediction_channel, "hz"))
    obstacle_count = _message_count(obstacles_channel)
    prediction_log_errors = _prediction_log_errors(prediction_log_paths)
    prediction_log_activity = _prediction_log_activity(prediction_log_paths)
    prediction_logs_present = bool(prediction_log_paths)
    obstacle_contract_status = str(obstacle_gt_contract.get("status") or "")
    perception_available = bool(obstacle_count and obstacle_count > 0) or obstacle_contract_status in {
        "pass",
        "warn",
    }

    missing_fields: list[str] = []
    warnings: list[str] = []
    blocking_capabilities: list[str] = []
    hard_gate_eligible = False
    explicit_scenario_override = bool(explicit_allow_dynamic_bypass)
    prediction_bypass_scope = "none"
    claim_boundary_downgraded = False

    if not channel_stats:
        missing_fields.append("channel_stats")
    if not summary and not manifest:
        missing_fields.append("summary_or_manifest")

    if prediction_count and prediction_count > 0:
        prediction_mode = "native_observed"
        verdict = "pass"
        hard_gate_eligible = True
    elif prediction_runtime_observed is True and _scenario_requires_prediction(scenario_class):
        prediction_mode = "missing"
        verdict = "fail"
        hard_gate_eligible = False
        blocking_capabilities.extend(_blocked_for_missing_prediction(scenario_class))
        warnings.append("prediction_module_observed_without_prediction_channel_output")
    elif not planning_requires_prediction:
        prediction_mode = "not_required_for_case"
        verdict = "warn"
        hard_gate_eligible = True
        prediction_bypass_scope = "not_required_for_case"
        warnings.append("prediction_not_required_for_case")
    elif bypass_reason and _scenario_allows_bypass(scenario_class, explicit_allow_dynamic_bypass):
        prediction_mode = "bypassed_with_gt_obstacles"
        verdict = "warn"
        # Static lane-keep has no interaction target for Apollo Prediction to
        # forecast. A declared bypass can support the lane-keep gate, but it is
        # still not native /apollo/prediction evidence and must not be
        # generalized to dynamic/junction/traffic-light claims.
        hard_gate_eligible = True
        claim_boundary_downgraded = True
        prediction_bypass_scope = (
            "explicit_scenario_override"
            if explicit_scenario_override
            else "static_lane_keep_no_dynamic_obstacles"
        )
        if not explicit_scenario_override:
            blocking_capabilities.extend(["dynamic_obstacle", "junction", "traffic_light"])
        warnings.append("prediction_bypassed_with_reason")
    elif bypass_reason:
        prediction_mode = "bypassed_with_gt_obstacles"
        verdict = "fail"
        hard_gate_eligible = False
        claim_boundary_downgraded = True
        prediction_bypass_scope = "blocked_for_scenario"
        blocking_capabilities.extend(["closed_loop", "junction", "traffic_light"])
        warnings.append("prediction_bypass_not_allowed_for_scenario")
    elif planning_requires_prediction:
        prediction_mode = "missing"
        verdict = "fail" if _scenario_requires_prediction(scenario_class) else "insufficient_data"
        blocking_capabilities.extend(_blocked_for_missing_prediction(scenario_class))
    else:
        prediction_mode = "unknown"
        verdict = "insufficient_data"
        blocking_capabilities.append("closed_loop")

    if perception_available and prediction_mode != "native_observed":
        warnings.append("perception_obstacles_do_not_count_as_prediction")
    if (
        prediction_log_activity
        and prediction_mode != "native_observed"
        and not (prediction_count and prediction_count > 0)
    ):
        warnings.append("prediction_internal_log_activity_without_channel_output")
    if prediction_log_errors:
        warnings.append("prediction_logs_contain_errors")
        if any(error.get("severity") == "fatal" for error in prediction_log_errors):
            verdict = "fail"
            blocking_capabilities.append("closed_loop")
        elif verdict == "pass":
            verdict = "warn"

    if verdict in {"pass", "warn"} and prediction_mode in {"missing", "unknown"}:
        verdict = "insufficient_data"
        hard_gate_eligible = False

    return {
        "schema_version": PREDICTION_EVIDENCE_SCHEMA_VERSION,
        "run_id": run_id,
        "route_id": route_id,
        "scenario_class": scenario_class,
        "prediction_mode": prediction_mode,
        "perception_obstacles_available": perception_available,
        "perception_obstacles_message_count": obstacle_count,
        "prediction_channel_available": bool(prediction_count and prediction_count > 0),
        "prediction_message_count": prediction_count,
        "prediction_hz": prediction_hz,
        "prediction_logs_present": prediction_logs_present,
        "prediction_runtime_observed": prediction_runtime_observed,
        "prediction_internal_log_activity_observed": bool(prediction_log_activity),
        "prediction_internal_log_activity_count": len(prediction_log_activity),
        "prediction_internal_log_activity_samples": prediction_log_activity[:5],
        "prediction_errors": prediction_log_errors,
        "planning_requires_prediction": planning_requires_prediction,
        "bypass_reason": bypass_reason,
        "bypass_reason_source": bypass_reason_source,
        "prediction_bypass_scope": prediction_bypass_scope,
        "explicit_scenario_override": explicit_scenario_override,
        "claim_boundary_downgraded": claim_boundary_downgraded,
        "hard_gate_eligible": hard_gate_eligible,
        "blocking_capabilities": sorted(set(blocking_capabilities)),
        "missing_fields": sorted(set(missing_fields)),
        "warnings": sorted(set(warnings)),
        "status": verdict,
        "verdict": verdict,
        "interpretation_boundary": (
            "/apollo/perception/obstacles is not /apollo/prediction. Perception obstacle "
            "availability cannot by itself prove Apollo prediction availability."
        ),
    }


def write_prediction_evidence_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "prediction_evidence_report.json"
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path = output_dir / "prediction_evidence_summary.md"
    summary_path.write_text(prediction_evidence_summary_md(report), encoding="utf-8")
    return {
        "prediction_evidence_report": str(path),
        "prediction_evidence_summary": str(summary_path),
    }


def prediction_evidence_summary_md(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Apollo Prediction Evidence Summary",
            "",
            f"- Run ID: `{report.get('run_id')}`",
            f"- Route ID: `{report.get('route_id')}`",
            f"- Scenario class: `{report.get('scenario_class')}`",
            f"- Prediction mode: `{report.get('prediction_mode')}`",
            f"- Prediction runtime observed: `{report.get('prediction_runtime_observed')}`",
            (
                "- Prediction internal log activity observed: "
                f"`{report.get('prediction_internal_log_activity_observed')}` "
                f"({report.get('prediction_internal_log_activity_count') or 0} samples)"
            ),
            f"- Verdict: `{report.get('verdict')}`",
            f"- Hard gate eligible: `{report.get('hard_gate_eligible')}`",
            f"- Blocking capabilities: `{', '.join(report.get('blocking_capabilities') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )


def _stats_channels(stats: Mapping[str, Any]) -> Mapping[str, Any]:
    channels = stats.get("channels")
    return channels if isinstance(channels, Mapping) else {}


def _find_channel(stats: Mapping[str, Any], *names: str) -> Mapping[str, Any] | None:
    channels = _stats_channels(stats)
    wanted = {name for name in names if name}
    for key, entry in channels.items():
        if key in wanted and isinstance(entry, Mapping):
            return entry
        if isinstance(entry, Mapping):
            channel = entry.get("channel") or entry.get("name")
            if channel in wanted:
                return entry
    return None


def _channel_value(entry: Mapping[str, Any] | None, key: str) -> Any:
    return entry.get(key) if isinstance(entry, Mapping) else None


def _message_count(entry: Mapping[str, Any] | None) -> int | None:
    if not isinstance(entry, Mapping):
        return None
    for key in ("message_count", "count", "rx_count", "tx_count"):
        value = _number(entry.get(key))
        if value is not None:
            return int(value)
    return None


def _planning_requires_prediction(*payloads: Mapping[str, Any]) -> bool:
    for payload in payloads:
        value = _first_raw(
            payload,
            "planning_requires_prediction",
            payload,
            "prediction_required",
        )
        parsed = _bool_or_none(value)
        if parsed is not None:
            return parsed
    return True


def _scenario_allows_bypass(scenario_class: str | None, explicit_allow_dynamic_bypass: bool | None) -> bool:
    if explicit_allow_dynamic_bypass is True:
        return True
    return str(scenario_class or "") in STATIC_BYPASS_SCENARIOS


def _scenario_requires_prediction(scenario_class: str | None) -> bool:
    return str(scenario_class or "") in DYNAMIC_OR_INTERACTION_SCENARIOS


def _blocked_for_missing_prediction(scenario_class: str | None) -> list[str]:
    scenario = str(scenario_class or "")
    if scenario in {"traffic_light_red_stop", "traffic_light_green_go", "traffic_light_red_to_green_release"}:
        return ["traffic_light", "closed_loop"]
    if scenario in {"junction_turn"}:
        return ["junction", "closed_loop"]
    if scenario in {"dynamic_obstacle", "follow_stop"}:
        return ["closed_loop"]
    return ["closed_loop"]


def _prediction_log_errors(paths: Sequence[Path]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line_number, line in enumerate(lines, start=1):
            upper = line.upper()
            if "FATAL" in upper or "ERROR" in upper:
                errors.append(
                    {
                        "path": str(path),
                        "line": line_number,
                        "severity": "fatal" if "FATAL" in upper else "error",
                        "message": line[:240],
                    }
                )
    return errors


def _prediction_log_activity(paths: Sequence[Path]) -> list[dict[str, Any]]:
    """Extract lightweight evidence that Prediction did internal work.

    This is intentionally weaker than native /apollo/prediction evidence. It
    helps distinguish "module not observed" from "module observed but no output
    channel evidence" without upgrading prediction_mode.
    """

    activity: list[dict[str, Any]] = []
    markers = (
        "EVALUATOR",
        "PREDICTOR",
        "PREDICTION OBSTACLE",
        "NORMAL OBSTACLE",
        "FEATURE OUTPUT",
        "TRAJECTORY",
    )
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line_number, line in enumerate(lines, start=1):
            upper = line.upper()
            if "FATAL" in upper or "ERROR" in upper:
                continue
            if not any(marker in upper for marker in markers):
                continue
            activity.append(
                {
                    "path": str(path),
                    "line": line_number,
                    "message": line[:240],
                }
            )
    return activity


def _find_prediction_logs(root: Path) -> list[Path]:
    candidates: list[Path] = []
    for pattern in (
        "*prediction*.INFO",
        "*prediction*.INFO.tail",
        "*prediction*.log",
        "*prediction*.txt",
    ):
        candidates.extend(path for path in root.rglob(pattern) if path.is_file())
    return sorted(set(candidates))


def _prediction_runtime_observed(root: Path) -> bool | None:
    """Return whether Apollo Prediction mainboard was observed in runtime status artifacts."""

    saw_status = False
    needles = (
        "modules/prediction/dag/prediction.dag",
        "prediction.dag",
        " -p prediction",
    )
    for relative in (
        "artifacts/apollo_modules_start.log",
        "artifacts/apollo_modules_status.log",
        "artifacts/apollo_modules_status_final.log",
        "artifacts/apollo_control_deferred_status.log",
    ):
        path = root / relative
        if not path.is_file():
            continue
        saw_status = True
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if any(needle in text for needle in needles):
            return True
    return False if saw_status else None


def _load_replacement_matrix(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        return load_gt_replacement_matrix(path)
    except (OSError, GTReplacementMatrixError):
        return {}


def _matrix_prediction_module(matrix: Mapping[str, Any]) -> Mapping[str, Any]:
    modules = matrix.get("modules")
    if not isinstance(modules, list):
        return {}
    for module in modules:
        if isinstance(module, Mapping) and module.get("name") == "prediction":
            return module
    return {}


def _matrix_prediction_bypass_reason(module: Mapping[str, Any]) -> str | None:
    if module.get("replacement_status") != "bypassed":
        return None
    reason = module.get("bypass_reason")
    if reason in {None, ""}:
        return None
    return str(reason)


def _find_first(root: Path, relatives: Sequence[str]) -> Path | None:
    for relative in relatives:
        path = root / relative
        if path.exists():
            return path
    return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _first_text(*args: Any, default: str | None = None) -> str | None:
    value = _first_raw(*args)
    if value not in {None, ""}:
        return str(value)
    return default


def _first_raw(*args: Any) -> Any:
    index = 0
    while index < len(args):
        if isinstance(args[index], Mapping):
            mapping = args[index]
            index += 1
            while index < len(args) and isinstance(args[index], str):
                value = mapping.get(args[index])
                if value not in {None, ""}:
                    return value
                index += 1
        else:
            value = args[index]
            if value not in {None, ""}:
                return value
            index += 1
    return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in {None, ""}:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _number(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
