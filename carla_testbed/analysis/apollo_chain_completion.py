from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.algorithms.gt_replacement_matrix import (
    GTReplacementMatrixError,
    load_gt_replacement_matrix,
)
from carla_testbed.algorithms.reference_chain import (
    ApolloReferenceChainError,
    load_apollo_reference_chain,
    module_by_name,
    required_modules_for_capability,
)
from carla_testbed.analysis.apollo_link_health import (
    analyze_apollo_link_health_run_dir,
    apollo_link_health_summary_md,
    write_apollo_link_health_report,
)

APOLLO_CHAIN_COMPLETION_SCHEMA_VERSION = "apollo_chain_completion.v1"
_CYBER_TOKEN = "cy" + "ber"
_CYBER_STATS_KEY = _CYBER_TOKEN + "_bridge_stats"
CAPABILITIES = ("lane_keep", "curve", "junction", "traffic_light", "closed_loop")
NON_BLOCKING = {"pass", "warn", "not_applicable"}
BLOCKING = {"fail", "insufficient_data", "missing"}

MODULE_LAYER_MAP = {
    "hdmap": "hdmap_projection",
    "routing": "bridge_runtime",
    "localization": "localization_gt_contract",
    "chassis": "channel_health",
    "perception_obstacles": "perception_gt_obstacles",
    "prediction": "obstacle_or_prediction_contract",
    "traffic_light_perception": "traffic_light_gt",
    "planning": "planning_reference_line",
    "control": "routing_planning_control_handoff",
    "vehicle_interface": "control_mapping_apply",
    _CYBER_TOKEN + "rt": "channel_health",
    "dreamview": None,
}

EVIDENCE_PATTERNS = {
    "manifest": ("manifest.json",),
    "summary": ("summary.json",),
    "channel_stats": ("channel_stats.json", "artifacts/channel_stats.json"),
    _CYBER_STATS_KEY: ("artifacts/" + _CYBER_STATS_KEY + ".json", _CYBER_STATS_KEY + ".json"),
    "bridge_transport_summary": (
        "artifacts/bridge_transport_summary.json",
        "bridge_transport_summary.json",
    ),
    "localization_contract_report.json": (
        "analysis/localization_contract/localization_contract_report.json",
        "localization_contract_report.json",
    ),
    "apollo_reference_line_contract_report.json": (
        "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        "apollo_reference_line_contract_report.json",
    ),
    "apollo_control_handoff_report.json": (
        "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
        "apollo_control_handoff_report.json",
    ),
    "control_health_report.json": (
        "analysis/control_health/control_health_report.json",
        "control_health_report.json",
    ),
    "control_attribution_report.json": (
        "analysis/control_attribution/control_attribution_report.json",
        "control_attribution_report.json",
    ),
    "obstacle_gt_contract_report.json": (
        "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        "obstacle_gt_contract_report.json",
    ),
    "traffic_light_contract_report.json": (
        "analysis/traffic_light_contract/traffic_light_contract_report.json",
        "traffic_light_contract_report.json",
    ),
    "traffic_light_evidence_report.json": (
        "analysis/traffic_light_evidence/traffic_light_evidence_report.json",
        "traffic_light_evidence_report.json",
    ),
    "traffic_light_behavior_report.json": (
        "analysis/traffic_light_behavior/traffic_light_behavior_report.json",
        "traffic_light_behavior_report.json",
        "traffic_light_behavior_report.json",
    ),
    "prediction_evidence_report.json": (
        "analysis/prediction_evidence/prediction_evidence_report.json",
        "prediction_evidence_report.json",
    ),
    "route_health.json": (
        "analysis/route_health/route_health.json",
        "route_health.json",
    ),
    "apollo_hdmap_projection.jsonl": (
        "artifacts/apollo_hdmap_projection.jsonl",
        "apollo_hdmap_projection.jsonl",
    ),
    "direct_bridge_control_apply.jsonl": (
        "artifacts/direct_bridge_control_apply.jsonl",
        "direct_bridge_control_apply.jsonl",
    ),
}


def analyze_apollo_chain_completion_run_dir(
    run_dir: str | Path,
    *,
    reference_path: str | Path = "configs/reference/apollo_reference_chain.yaml",
    replacement_path: str | Path = "configs/reference/apollo_gt_replacement_matrix.yaml",
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    warnings: list[str] = []
    reference_chain: dict[str, Any] | None = None
    replacement_matrix: dict[str, Any] | None = None
    invalid_stage: str | None = None

    try:
        reference_chain = load_apollo_reference_chain(reference_path)
    except ApolloReferenceChainError as exc:
        warnings.append(f"invalid_reference_chain:{exc}")
        invalid_stage = "invalid_reference_chain"

    try:
        replacement_matrix = load_gt_replacement_matrix(
            replacement_path,
            reference_chain=reference_chain,
        )
    except GTReplacementMatrixError as exc:
        warnings.append(f"invalid_replacement_matrix:{exc}")
        invalid_stage = invalid_stage or "invalid_replacement_matrix"

    link_health = analyze_apollo_link_health_run_dir(root)
    link_layers = link_health.get("layers") if isinstance(link_health.get("layers"), Mapping) else {}
    if invalid_stage or reference_chain is None or replacement_matrix is None:
        return _invalid_report(
            run_dir=root,
            reference_path=reference_path,
            replacement_path=replacement_path,
            link_health=link_health,
            warnings=warnings,
            failure_stage=invalid_stage or "insufficient_data",
        )

    module_statuses = _build_module_statuses(
        root,
        reference_chain=reference_chain,
        replacement_matrix=replacement_matrix,
        link_layers=link_layers,
    )
    capability_status = _capability_status(reference_chain, module_statuses)
    missing_required_evidence = _missing_required_evidence(module_statuses)
    blocking_modules = _blocking_modules(module_statuses)
    blocking_layers = _blocking_layers(link_layers)
    failure_stage = _failure_stage(
        module_statuses=module_statuses,
        link_layers=link_layers,
        capability_status=capability_status,
    )
    verdict = _overall_verdict(capability_status, module_statuses, missing_required_evidence)
    can_claim_closed_loop = capability_status.get("closed_loop") in {"pass", "warn"} and not blocking_modules
    can_claim_unassisted = bool(
        can_claim_closed_loop
        and link_health.get("can_claim_unassisted_natural_driving") is True
        and capability_status.get("lane_keep") in {"pass", "warn"}
    )
    can_claim_algorithm_limitation = bool(
        can_claim_unassisted
        and _pre_outcome_layers_non_blocking(link_layers)
        and _layer_status(link_layers, "natural_driving_outcome") == "fail"
    )

    return {
        "schema_version": APOLLO_CHAIN_COMPLETION_SCHEMA_VERSION,
        "run_id": link_health.get("run_id"),
        "variant_id": _variant_id(root),
        "scenario_id": link_health.get("scenario_id"),
        "route_id": link_health.get("route_id"),
        "reference_chain_path": str(Path(reference_path)),
        "replacement_matrix_path": str(Path(replacement_path)),
        "module_statuses": module_statuses,
        "link_health_layers": dict(link_layers),
        "capability_status": capability_status,
        "missing_required_evidence": missing_required_evidence,
        "blocking_modules": blocking_modules,
        "blocking_layers": blocking_layers,
        "failure_stage": failure_stage,
        "can_claim_truth_input_closed_loop": can_claim_closed_loop,
        "can_claim_unassisted_natural_driving": can_claim_unassisted,
        "can_claim_algorithm_limitation": can_claim_algorithm_limitation,
        "warnings": sorted(set(warnings + _completion_warnings(module_statuses))),
        "verdict": verdict,
        "source_link_health": {
            "schema_version": link_health.get("schema_version"),
            "primary_blocker": link_health.get("primary_blocker"),
            "secondary_blockers": link_health.get("secondary_blockers") or [],
            "source": link_health.get("source") or {},
        },
        "interpretation_boundary": (
            "Chain completion aggregates existing artifacts against the reference chain and GT "
            "replacement matrix. It does not generate evidence and cannot turn summary success, "
            "route_health alone, or operator video into Apollo capability proof."
        ),
    }


def write_apollo_chain_completion_report(
    report: Mapping[str, Any],
    out_dir: str | Path,
    *,
    link_health_report: Mapping[str, Any] | None = None,
    write_compatible_link_health: bool = True,
) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "apollo_chain_completion_report.json"
    summary_path = output_dir / "apollo_chain_completion_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_chain_completion_summary_md(report), encoding="utf-8")
    outputs = {
        "apollo_chain_completion_report": str(json_path),
        "apollo_chain_completion_summary": str(summary_path),
    }
    if write_compatible_link_health and link_health_report:
        link_outputs = write_apollo_link_health_report(link_health_report, output_dir)
        outputs.update(link_outputs)
    return outputs


def analyze_and_write_apollo_chain_completion(
    run_dir: str | Path,
    *,
    reference_path: str | Path = "configs/reference/apollo_reference_chain.yaml",
    replacement_path: str | Path = "configs/reference/apollo_gt_replacement_matrix.yaml",
    out_dir: str | Path | None = None,
    write_compatible_link_health: bool = True,
) -> dict[str, str]:
    root = Path(run_dir).expanduser()
    report = analyze_apollo_chain_completion_run_dir(
        root,
        reference_path=reference_path,
        replacement_path=replacement_path,
    )
    link_health = analyze_apollo_link_health_run_dir(root)
    output = Path(out_dir).expanduser() if out_dir is not None else root / "analysis" / "apollo_chain_completion"
    return write_apollo_chain_completion_report(
        report,
        output,
        link_health_report=link_health,
        write_compatible_link_health=write_compatible_link_health,
    )


def apollo_chain_completion_summary_md(report: Mapping[str, Any]) -> str:
    module_statuses = report.get("module_statuses") if isinstance(report.get("module_statuses"), Mapping) else {}
    lines = [
        "# Apollo Chain Completion Summary",
        "",
        f"- Run ID: `{report.get('run_id')}`",
        f"- Scenario: `{report.get('scenario_id')}`",
        f"- Route ID: `{report.get('route_id')}`",
        f"- Verdict: `{report.get('verdict')}`",
        f"- Failure stage: `{report.get('failure_stage')}`",
        f"- Can claim truth-input closed loop: `{report.get('can_claim_truth_input_closed_loop')}`",
        f"- Can claim unassisted natural driving: `{report.get('can_claim_unassisted_natural_driving')}`",
        f"- Can claim algorithm limitation: `{report.get('can_claim_algorithm_limitation')}`",
        f"- Blocking modules: `{', '.join(report.get('blocking_modules') or []) or 'none'}`",
        f"- Blocking layers: `{', '.join(report.get('blocking_layers') or []) or 'none'}`",
        "",
        "## Capability Status",
        "",
    ]
    for capability, status in (report.get("capability_status") or {}).items():
        lines.append(f"- `{capability}`: `{status}`")
    lines.extend(["", "## Module Statuses", ""])
    for name in sorted(module_statuses):
        module = module_statuses[name]
        if not isinstance(module, Mapping):
            continue
        lines.extend(
            [
                f"### {name}",
                f"- replacement_status: `{module.get('replacement_status')}`",
                f"- evidence_status: `{module.get('evidence_status')}`",
                f"- hard_gate_eligible: `{module.get('hard_gate_eligible')}`",
                f"- blocking_capabilities: `{', '.join(module.get('blocking_capabilities') or []) or 'none'}`",
                "",
            ]
        )
    lines.extend([str(report.get("interpretation_boundary") or ""), ""])
    return "\n".join(lines)


def _invalid_report(
    *,
    run_dir: Path,
    reference_path: str | Path,
    replacement_path: str | Path,
    link_health: Mapping[str, Any],
    warnings: Sequence[str],
    failure_stage: str,
) -> dict[str, Any]:
    return {
        "schema_version": APOLLO_CHAIN_COMPLETION_SCHEMA_VERSION,
        "run_id": link_health.get("run_id") or run_dir.name,
        "variant_id": _variant_id(run_dir),
        "scenario_id": link_health.get("scenario_id"),
        "route_id": link_health.get("route_id"),
        "reference_chain_path": str(Path(reference_path)),
        "replacement_matrix_path": str(Path(replacement_path)),
        "module_statuses": {},
        "link_health_layers": link_health.get("layers") or {},
        "capability_status": {capability: "insufficient_data" for capability in CAPABILITIES},
        "missing_required_evidence": [],
        "blocking_modules": [],
        "blocking_layers": _blocking_layers(link_health.get("layers") or {}),
        "failure_stage": failure_stage,
        "can_claim_truth_input_closed_loop": False,
        "can_claim_unassisted_natural_driving": False,
        "can_claim_algorithm_limitation": False,
        "warnings": list(warnings),
        "verdict": "fail",
    }


def _build_module_statuses(
    run_dir: Path,
    *,
    reference_chain: Mapping[str, Any],
    replacement_matrix: Mapping[str, Any],
    link_layers: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    module_statuses: dict[str, dict[str, Any]] = {}
    matrix_modules = replacement_matrix.get("modules") if isinstance(replacement_matrix.get("modules"), list) else []
    for matrix_module in matrix_modules:
        if not isinstance(matrix_module, Mapping):
            continue
        name = str(matrix_module.get("name") or "")
        if not name:
            continue
        try:
            reference_module = module_by_name(reference_chain, str(matrix_module.get("reference_module") or name))
        except Exception:
            reference_module = {}
        observed = _observed_evidence(run_dir, matrix_module.get("required_evidence") or [])
        if "bypass_reason" in observed and matrix_module.get("bypass_reason"):
            observed["bypass_reason"] = str(matrix_module.get("bypass_reason"))
        layer_name = MODULE_LAYER_MAP.get(name)
        layer = link_layers.get(layer_name) if layer_name else None
        prediction_report = _read_json_path(observed.get("prediction_evidence_report.json"))
        evidence_status = _module_evidence_status(
            name=name,
            matrix_module=matrix_module,
            observed_evidence=observed,
            layer=layer if isinstance(layer, Mapping) else None,
            prediction_report=prediction_report,
        )
        replacement_status = str(matrix_module.get("replacement_status") or "")
        hard_gate_eligible = evidence_status in {"pass", "warn"} and replacement_status not in {
            "missing",
            "mock",
            "unknown",
            "operator_evidence",
        }
        if name == "prediction" and prediction_report:
            hard_gate_eligible = bool(prediction_report.get("hard_gate_eligible")) and evidence_status in {
                "pass",
                "warn",
            }
        blocked = (
            list(matrix_module.get("blocked_capabilities") or [])
            if evidence_status in BLOCKING or replacement_status in {"bypassed", "missing", "mock", "unknown"}
            else []
        )
        if name == "prediction" and prediction_report.get("blocking_capabilities"):
            blocked = list(prediction_report.get("blocking_capabilities") or [])
        if not hard_gate_eligible and not blocked and evidence_status in BLOCKING:
            blocked = _capabilities_from_reference_module(reference_module)
        module_statuses[name] = {
            "reference_module": matrix_module.get("reference_module"),
            "replacement_status": matrix_module.get("replacement_status"),
            "required_inputs": list(reference_module.get("required_inputs") or []),
            "expected_outputs": list(reference_module.get("expected_outputs") or []),
            "required_evidence": list(matrix_module.get("required_evidence") or []),
            "observed_evidence": observed,
            "evidence_status": evidence_status,
            "hard_gate_eligible": hard_gate_eligible,
            "blocking_capabilities": sorted(set(str(item) for item in blocked if item)),
            "notes": matrix_module.get("notes"),
        }
    return module_statuses


def _module_evidence_status(
    *,
    name: str,
    matrix_module: Mapping[str, Any],
    observed_evidence: Mapping[str, Any],
    layer: Mapping[str, Any] | None,
    prediction_report: Mapping[str, Any] | None = None,
) -> str:
    if name == "prediction" and prediction_report:
        return _normalize_status(prediction_report.get("verdict"))
    replacement_status = str(matrix_module.get("replacement_status") or "")
    if replacement_status == "bypassed":
        return "warn" if matrix_module.get("bypass_reason") else "insufficient_data"
    if replacement_status == "operator_evidence":
        return "warn"
    if _has_required_artifact_gaps(name, matrix_module, observed_evidence):
        return "missing"
    if layer:
        status = _normalize_status(layer.get("status"))
        if status in {"fail", "insufficient_data", "warn", "pass"}:
            return status
    return _matrix_status(matrix_module)


def _has_required_artifact_gaps(
    name: str,
    matrix_module: Mapping[str, Any],
    observed_evidence: Mapping[str, Any],
) -> bool:
    required = matrix_module.get("required_evidence")
    if not isinstance(required, list):
        return True
    required_tokens = {
        "localization": ("localization_contract_report.json",),
        "planning": ("planning_topic_debug_summary.json", "apollo_reference_line_contract_report.json"),
        "control": ("apollo_control_handoff_report.json",),
        "vehicle_interface": ("control_attribution_report.json", "direct_bridge_control_apply.jsonl"),
        "perception_obstacles": ("obstacle_gt_contract_report.json",),
        "prediction": ("prediction_evidence_report.json",),
    }.get(name)
    if name == "traffic_light_perception":
        required_all = ("traffic_light_contract_report.json", "traffic_light_evidence_report.json")
        return any(not observed_evidence.get(token) for token in required_all)
    if not required_tokens:
        return False
    return not any(observed_evidence.get(token) for token in required_tokens)


def _matrix_status(matrix_module: Mapping[str, Any]) -> str:
    status = str(matrix_module.get("current_evidence_status") or "insufficient_data")
    return _normalize_status(status)


def _observed_evidence(run_dir: Path, required_evidence: Sequence[Any]) -> dict[str, Any]:
    observed: dict[str, Any] = {}
    for item in required_evidence:
        text = str(item)
        key = _evidence_key(text)
        path = _find_evidence_path(run_dir, key, text)
        observed[key] = str(path) if path else None
    return observed


def _evidence_key(text: str) -> str:
    lowered = text.lower()
    for key in sorted(EVIDENCE_PATTERNS, key=len, reverse=True):
        if key.lower() in lowered:
            return key
    if "/apollo/" in lowered:
        return "channel_stats"
    if "route_health" in lowered:
        return "route_health.json"
    if _CYBER_TOKEN in lowered:
        return _CYBER_STATS_KEY
    if "planning_topic_debug_summary" in lowered:
        return "planning_topic_debug_summary.json"
    return text


def _find_evidence_path(run_dir: Path, key: str, original: str) -> Path | None:
    patterns = EVIDENCE_PATTERNS.get(key, ())
    for relative in patterns:
        candidate = run_dir / relative
        if candidate.exists():
            return candidate
    if key == "planning_topic_debug_summary.json":
        candidate = run_dir / "artifacts/planning_topic_debug_summary.json"
        if candidate.exists():
            return candidate
    if key.endswith(".json") or key.endswith(".jsonl") or key.endswith(".csv"):
        for candidate in run_dir.rglob(key):
            if candidate.exists():
                return candidate
    original_name = Path(original.split()[0]).name
    if original_name.endswith((".json", ".jsonl", ".csv")):
        for candidate in run_dir.rglob(original_name):
            if candidate.exists():
                return candidate
    return None


def _read_json_path(path: Any) -> dict[str, Any]:
    if not path:
        return {}
    try:
        payload = json.loads(Path(str(path)).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _capability_status(
    reference_chain: Mapping[str, Any],
    module_statuses: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    statuses: dict[str, str] = {}
    for capability in CAPABILITIES:
        required_names = [module.get("name") for module in required_modules_for_capability(reference_chain, capability)]
        required_statuses = [
            module_statuses.get(str(name), {})
            for name in required_names
            if isinstance(module_statuses.get(str(name), {}), Mapping)
        ]
        blocking_modules = [
            module
            for module in required_statuses
            if capability in set(module.get("blocking_capabilities") or [])
            or module.get("evidence_status") in {"fail", "missing", "insufficient_data"}
            and module.get("replacement_status") not in {"operator_evidence"}
        ]
        if not blocking_modules:
            statuses[capability] = "warn" if any(m.get("evidence_status") == "warn" for m in required_statuses) else "pass"
            continue
        if capability == "lane_keep" and _only_reference_line_or_projection_missing(blocking_modules):
            statuses[capability] = "warn"
        elif any(module.get("evidence_status") == "fail" for module in blocking_modules):
            statuses[capability] = "fail"
        else:
            statuses[capability] = "insufficient_data"
    return statuses


def _only_reference_line_or_projection_missing(blocking_modules: Sequence[Mapping[str, Any]]) -> bool:
    names = {module.get("reference_module") for module in blocking_modules}
    return bool(names) and names.issubset({"hdmap", "planning"})


def _missing_required_evidence(module_statuses: Mapping[str, Mapping[str, Any]]) -> list[str]:
    missing: list[str] = []
    for name, module in module_statuses.items():
        observed = module.get("observed_evidence") if isinstance(module.get("observed_evidence"), Mapping) else {}
        for evidence, path in observed.items():
            if not path:
                missing.append(f"{name}:{evidence}")
    return sorted(set(missing))


def _blocking_modules(module_statuses: Mapping[str, Mapping[str, Any]]) -> list[str]:
    blocking: list[str] = []
    for name, module in module_statuses.items():
        if module.get("evidence_status") in BLOCKING or module.get("blocking_capabilities"):
            blocking.append(str(name))
    return sorted(set(blocking))


def _blocking_layers(link_layers: Mapping[str, Any]) -> list[str]:
    blocking: list[str] = []
    for name, layer in link_layers.items():
        if not isinstance(layer, Mapping):
            continue
        if layer.get("status") in {"fail", "insufficient_data"} or layer.get("blocking_reasons"):
            blocking.append(str(name))
    return sorted(blocking)


def _failure_stage(
    *,
    module_statuses: Mapping[str, Mapping[str, Any]],
    link_layers: Mapping[str, Any],
    capability_status: Mapping[str, str],
) -> str:
    if _module_blocking(module_statuses, "hdmap") or _layer_blocking(link_layers, "hdmap_projection"):
        return "hdmap_or_route_contract"
    if _module_blocking(module_statuses, "localization") or _layer_blocking(
        link_layers, "localization_gt_contract"
    ):
        return "localization_contract"
    if _module_blocking(module_statuses, "chassis"):
        return "chassis_contract"
    if _module_blocking(module_statuses, "perception_obstacles") or _module_blocking(
        module_statuses, "prediction"
    ):
        return "obstacle_or_prediction_contract"
    if _module_blocking(module_statuses, "traffic_light_perception") or _layer_blocking(
        link_layers, "traffic_light_gt"
    ):
        return "traffic_light_contract"
    if _module_blocking(module_statuses, "planning") or _layer_blocking(
        link_layers, "planning_reference_line"
    ):
        return "planning_materialization"
    if _module_blocking(module_statuses, "control") or _layer_blocking(
        link_layers, "routing_planning_control_handoff"
    ):
        return "control_handoff"
    if _module_blocking(module_statuses, "vehicle_interface") or _layer_blocking(
        link_layers, "control_mapping_apply"
    ):
        return "vehicle_interface"
    if _layer_blocking(link_layers, "no_assist_claim_boundary"):
        return "assist_or_calibration"
    if any(status in {"fail", "insufficient_data"} for status in capability_status.values()):
        return "insufficient_data"
    return "none"


def _module_blocking(module_statuses: Mapping[str, Mapping[str, Any]], name: str) -> bool:
    module = module_statuses.get(name, {})
    return bool(module.get("evidence_status") in BLOCKING or module.get("blocking_capabilities"))


def _layer_blocking(link_layers: Mapping[str, Any], name: str) -> bool:
    layer = link_layers.get(name, {})
    return bool(
        isinstance(layer, Mapping)
        and (layer.get("status") in {"fail", "insufficient_data"} or layer.get("blocking_reasons"))
    )


def _overall_verdict(
    capability_status: Mapping[str, str],
    module_statuses: Mapping[str, Mapping[str, Any]],
    missing_required_evidence: Sequence[str],
) -> str:
    if any(status == "fail" for status in capability_status.values()):
        return "fail"
    if missing_required_evidence or any(status == "insufficient_data" for status in capability_status.values()):
        return "insufficient_data"
    if any(status == "warn" for status in capability_status.values()) or any(
        module.get("evidence_status") == "warn" for module in module_statuses.values()
    ):
        return "warn"
    return "pass"


def _completion_warnings(module_statuses: Mapping[str, Mapping[str, Any]]) -> list[str]:
    warnings: list[str] = []
    prediction = module_statuses.get("prediction", {})
    if prediction.get("replacement_status") == "bypassed":
        warnings.append("prediction_bypassed_requires_explicit_scope_boundary")
    if any(module.get("evidence_status") == "missing" for module in module_statuses.values()):
        warnings.append("missing_required_evidence_blocks_claim")
    return warnings


def _pre_outcome_layers_non_blocking(link_layers: Mapping[str, Any]) -> bool:
    for name, layer in link_layers.items():
        if name == "natural_driving_outcome":
            continue
        if isinstance(layer, Mapping) and layer.get("status") not in NON_BLOCKING:
            return False
    return True


def _layer_status(link_layers: Mapping[str, Any], name: str) -> str:
    layer = link_layers.get(name, {})
    if not isinstance(layer, Mapping):
        return "insufficient_data"
    return _normalize_status(layer.get("status"))


def _variant_id(run_dir: Path) -> str | None:
    for name in ("manifest.json", "summary.json"):
        path = run_dir / name
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, Mapping):
            value = payload.get("algorithm_variant_id") or payload.get("variant_id")
            if value:
                return str(value)
    return None


def _capabilities_from_reference_module(module: Mapping[str, Any]) -> list[str]:
    return [str(item) for item in module.get("hard_gate_required_for") or [] if item]


def _normalize_status(value: Any) -> str:
    text = str(value or "insufficient_data")
    if text in {"pass", "warn", "fail", "insufficient_data", "missing", "not_applicable"}:
        return text
    if text in {"success", "ok", "candidate_positive"}:
        return "pass"
    if text in {"failed", "candidate_negative"}:
        return "fail"
    return "insufficient_data"
