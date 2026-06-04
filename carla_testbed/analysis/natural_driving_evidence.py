from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.assist_ledger import read_assist_ledger_from_run_dir

NATURAL_DRIVING_EVIDENCE_SCHEMA_VERSION = "natural_driving_evidence.v1"

TRAFFIC_LIGHT_SCENARIO_CLASSES = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}


@dataclass
class NaturalDrivingEvidence:
    schema_version: str = NATURAL_DRIVING_EVIDENCE_SCHEMA_VERSION
    scenario_id: str | None = None
    route_id: str | None = None
    scenario_class: str | None = None
    gate_role: str | None = None
    route_health_status: str = "insufficient_data"
    route_hard_gate_eligible: bool | None = None
    channel_health_status: str = "insufficient_data"
    localization_contract_status: str = "insufficient_data"
    localization_blocking_reasons: list[str] = field(default_factory=list)
    localization_report_path: str | None = None
    apollo_reference_line_contract_status: str = "insufficient_data"
    apollo_reference_line_blocking_reasons: list[str] = field(default_factory=list)
    apollo_reference_line_report_path: str | None = None
    apollo_control_handoff_status: str = "insufficient_data"
    apollo_control_handoff_failure_stage: str | None = None
    apollo_control_handoff_report_path: str | None = None
    control_attribution_status: str = "insufficient_data"
    traffic_light_evidence_status: str = "not_required"
    assist_ledger_status: str = "insufficient_data"
    artifact_completeness_status: str = "insufficient_data"
    active_assists: list[str] = field(default_factory=list)
    blocking_assists: list[str] = field(default_factory=list)
    non_blocking_assists: list[str] = field(default_factory=list)
    assist_confidence: str | None = None
    can_claim_unassisted_natural_driving: bool = False
    can_claim_truth_input_natural_driving: bool = False
    why_not_claimable: list[str] = field(default_factory=list)
    missing_artifacts: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    evidence_warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_natural_driving_evidence_from_run_dir(run_dir: str | Path) -> NaturalDrivingEvidence:
    root = Path(run_dir).expanduser()
    summary = _read_json(root / "summary.json")
    manifest = _read_json(root / "manifest.json")
    route_health_path = _find_first(root, ["analysis/route_health/route_health.json", "route_health.json"])
    channel_health_path = _find_first(
        root,
        [
            "analysis/apollo_channel_health/apollo_channel_health_report.json",
            "apollo_channel_health_report.json",
            "analysis/channel_health/channel_health_report.json",
            "channel_health_report.json",
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
    control_attribution_path = _find_first(
        root,
        [
            "analysis/control_attribution/control_attribution_report.json",
            "control_attribution_report.json",
        ],
    )
    apollo_control_handoff_path = _find_first(
        root,
        [
            "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
            "apollo_control_handoff_report.json",
        ],
    )
    traffic_light_path = _find_first(root, _traffic_light_evidence_paths())
    assist_path = _find_first(
        root,
        [
            "assist_ledger.json",
            "artifacts/assist_ledger.json",
            "analysis/assist_ledger/assist_ledger.json",
        ],
    )
    assist_ledger = read_assist_ledger_from_run_dir(root)
    return build_natural_driving_evidence(
        run_dir=root,
        summary=summary,
        manifest=manifest,
        route_health=_read_json(route_health_path),
        channel_health=_read_json(channel_health_path),
        localization_contract=_read_json(localization_contract_path),
        apollo_reference_line_contract=_read_json(apollo_reference_line_contract_path),
        control_attribution=_read_json(control_attribution_path),
        apollo_control_handoff=_read_json(apollo_control_handoff_path),
        assist_ledger=assist_ledger,
        route_health_path=route_health_path,
        channel_health_path=channel_health_path,
        localization_contract_path=localization_contract_path,
        apollo_reference_line_contract_path=apollo_reference_line_contract_path,
        control_attribution_path=control_attribution_path,
        apollo_control_handoff_path=apollo_control_handoff_path,
        traffic_light_evidence_path=traffic_light_path,
        assist_ledger_path=assist_path,
    )


def build_natural_driving_evidence(
    *,
    run_dir: str | Path,
    summary: Mapping[str, Any] | None = None,
    manifest: Mapping[str, Any] | None = None,
    route_health: Mapping[str, Any] | None = None,
    channel_health: Mapping[str, Any] | None = None,
    localization_contract: Mapping[str, Any] | None = None,
    apollo_reference_line_contract: Mapping[str, Any] | None = None,
    control_attribution: Mapping[str, Any] | None = None,
    apollo_control_handoff: Mapping[str, Any] | None = None,
    assist_ledger: Mapping[str, Any] | None = None,
    route_health_path: str | Path | None = None,
    channel_health_path: str | Path | None = None,
    localization_contract_path: str | Path | None = None,
    apollo_reference_line_contract_path: str | Path | None = None,
    control_attribution_path: str | Path | None = None,
    apollo_control_handoff_path: str | Path | None = None,
    traffic_light_evidence_path: str | Path | None = None,
    assist_ledger_path: str | Path | None = None,
) -> NaturalDrivingEvidence:
    root = Path(run_dir).expanduser()
    summary = _as_mapping(summary)
    manifest = _as_mapping(manifest)
    route_health = _as_mapping(route_health)
    channel_health = _as_mapping(channel_health)
    localization_contract = _as_mapping(localization_contract)
    apollo_reference_line_contract = _as_mapping(apollo_reference_line_contract)
    control_attribution = _as_mapping(control_attribution)
    apollo_control_handoff = _as_mapping(apollo_control_handoff)
    assist_ledger = _as_mapping(assist_ledger)

    scenario_id = _first_text(summary, "scenario_id", manifest, "scenario_id", default=root.name)
    route_id = _first_text(summary, "route_id", manifest, "route_id", route_health, "route_id")
    scenario_class = _first_text(summary, "scenario_class", manifest, "scenario_class")
    gate_role = _first_text(summary, "gate_role", manifest, "gate_role")

    missing_artifacts = _missing_artifacts(
        root,
        scenario_class=scenario_class,
        route_health_path=Path(route_health_path) if route_health_path else None,
        channel_health_path=Path(channel_health_path) if channel_health_path else None,
        localization_contract_path=Path(localization_contract_path) if localization_contract_path else None,
        apollo_reference_line_contract_path=(
            Path(apollo_reference_line_contract_path) if apollo_reference_line_contract_path else None
        ),
        control_attribution_path=Path(control_attribution_path) if control_attribution_path else None,
        apollo_control_handoff_path=Path(apollo_control_handoff_path) if apollo_control_handoff_path else None,
        traffic_light_evidence_path=Path(traffic_light_evidence_path) if traffic_light_evidence_path else None,
        assist_ledger_path=Path(assist_ledger_path) if assist_ledger_path else None,
        summary=summary,
        manifest=manifest,
    )
    missing_fields: list[str] = []
    warnings: list[str] = []

    route_status = _report_status(route_health)
    route_hard_gate_eligible = _optional_bool(route_health.get("hard_gate_eligible"))
    if route_health_path is None:
        route_status = "insufficient_data"
    if route_hard_gate_eligible is None and route_health_path is not None:
        missing_fields.append("route_health.hard_gate_eligible")

    channel_status = _report_status(channel_health)
    if channel_health_path is None:
        channel_status = "insufficient_data"

    localization_status = _report_status(localization_contract)
    localization_blocking = _localization_blocking_reasons(localization_contract)
    if localization_contract_path is None:
        localization_status = "insufficient_data"
    if localization_blocking:
        warnings.append("localization contract has blocking reasons")

    apollo_reference_status = _report_status(apollo_reference_line_contract)
    apollo_reference_blocking = _blocking_reasons(apollo_reference_line_contract)
    if apollo_reference_line_contract_path is None:
        apollo_reference_status = "insufficient_data"
    if apollo_reference_blocking:
        warnings.append("apollo reference-line contract has blocking reasons")

    handoff_status = _report_status(apollo_control_handoff)
    handoff_stage = str(apollo_control_handoff.get("failure_stage") or "").strip() or None
    if apollo_control_handoff_path is None:
        handoff_status = "insufficient_data"
    if handoff_status == "fail" or (handoff_stage not in {None, "none"}):
        warnings.append("apollo control handoff has blocking evidence")

    control_attribution_status = _report_status(control_attribution)
    if control_attribution_path is None:
        control_attribution_status = "insufficient_data"

    traffic_light_status = "not_required"
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES:
        traffic_light_status = "insufficient_data"
        if traffic_light_evidence_path is not None:
            traffic_light_payload = _read_json(Path(traffic_light_evidence_path))
            traffic_light_status = _report_status(traffic_light_payload)

    active_assists = [str(item) for item in (assist_ledger.get("active_assists") or []) if item]
    blocking_assists = [str(item) for item in (assist_ledger.get("blocking_assists") or []) if item]
    non_blocking_assists = [
        str(item) for item in (assist_ledger.get("non_blocking_assists") or []) if item
    ]
    assist_confidence = str(assist_ledger.get("assist_confidence") or "").strip()
    why_not_claimable: list[str] = []
    assist_status = "pass"
    if not _has_assist_ledger_artifact(summary=summary, manifest=manifest, assist_ledger_path=assist_ledger_path):
        assist_status = "insufficient_data"
        missing_fields.append("assist_ledger")
        warnings.append("assist ledger is inferred or missing; unassisted claims remain blocked")
        why_not_claimable.append("assist_ledger_missing_or_unknown")
    elif blocking_assists:
        assist_status = "warn"
        why_not_claimable.append("blocking_assists_present")
    if assist_confidence == "unknown":
        warnings.append("assist confidence is unknown")
        why_not_claimable.append("assist_ledger_missing_or_unknown")

    artifact_status = "pass" if not missing_artifacts else "insufficient_data"
    can_claim_unassisted = bool(assist_ledger.get("can_claim_unassisted_natural_driving")) and not blocking_assists
    if assist_status == "insufficient_data":
        can_claim_unassisted = False
    if active_assists:
        can_claim_unassisted = False
        why_not_claimable.append("active_assists_present")

    can_claim_truth_input = bool(
        artifact_status == "pass"
        and route_status in {"pass", "warn", "diagnostic_ready"}
        and channel_status in {"pass", "warn"}
        and localization_status in {"pass", "warn"}
        and not localization_blocking
        and apollo_reference_status in {"pass", "warn"}
        and not apollo_reference_blocking
        and handoff_status in {"pass", "warn"}
        and handoff_stage in {None, "none"}
        and control_attribution_status in {"pass", "warn"}
        and traffic_light_status in {"not_required", "pass", "warn"}
        and can_claim_unassisted
    )

    return NaturalDrivingEvidence(
        scenario_id=scenario_id,
        route_id=route_id,
        scenario_class=scenario_class,
        gate_role=gate_role,
        route_health_status=route_status,
        route_hard_gate_eligible=route_hard_gate_eligible,
        channel_health_status=channel_status,
        localization_contract_status=localization_status,
        localization_blocking_reasons=localization_blocking,
        localization_report_path=str(localization_contract_path) if localization_contract_path else None,
        apollo_reference_line_contract_status=apollo_reference_status,
        apollo_reference_line_blocking_reasons=apollo_reference_blocking,
        apollo_reference_line_report_path=(
            str(apollo_reference_line_contract_path) if apollo_reference_line_contract_path else None
        ),
        apollo_control_handoff_status=handoff_status,
        apollo_control_handoff_failure_stage=handoff_stage,
        apollo_control_handoff_report_path=(
            str(apollo_control_handoff_path) if apollo_control_handoff_path else None
        ),
        control_attribution_status=control_attribution_status,
        traffic_light_evidence_status=traffic_light_status,
        assist_ledger_status=assist_status,
        artifact_completeness_status=artifact_status,
        active_assists=active_assists,
        blocking_assists=blocking_assists,
        non_blocking_assists=non_blocking_assists,
        assist_confidence=assist_confidence or None,
        can_claim_unassisted_natural_driving=can_claim_unassisted,
        can_claim_truth_input_natural_driving=can_claim_truth_input,
        why_not_claimable=sorted(set(why_not_claimable)),
        missing_artifacts=sorted(set(missing_artifacts)),
        missing_fields=sorted(set(missing_fields)),
        evidence_warnings=sorted(set(warnings)),
    )


def _missing_artifacts(
    root: Path,
    *,
    scenario_class: str | None,
    route_health_path: Path | None,
    channel_health_path: Path | None,
    localization_contract_path: Path | None,
    apollo_reference_line_contract_path: Path | None,
    control_attribution_path: Path | None,
    apollo_control_handoff_path: Path | None,
    traffic_light_evidence_path: Path | None,
    assist_ledger_path: Path | None,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> list[str]:
    missing: list[str] = []
    if not (root / "summary.json").exists():
        missing.append("summary.json")
    if not (root / "manifest.json").exists():
        missing.append("manifest.json")
    if _find_first(root, ["timeseries.csv", "timeseries.jsonl"]) is None:
        missing.append("timeseries.csv/jsonl")
    if route_health_path is None:
        missing.append("route_health.json")
    if channel_health_path is None:
        missing.append("channel_health_report.json or apollo_channel_health_report.json")
    if localization_contract_path is None:
        missing.append("localization_contract_report.json")
    if apollo_reference_line_contract_path is None:
        missing.append("apollo_reference_line_contract_report.json")
    if apollo_control_handoff_path is None:
        missing.append("apollo_control_handoff_report.json")
    if control_attribution_path is None:
        missing.append("control_attribution_report.json")
    if not _has_assist_ledger_artifact(
        summary=summary,
        manifest=manifest,
        assist_ledger_path=assist_ledger_path,
    ):
        missing.append("assist_ledger")
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and traffic_light_evidence_path is None:
        missing.append("traffic_light_evidence_report.json")
    return missing


def _traffic_light_evidence_paths() -> list[str]:
    return [
        "analysis/traffic_light/traffic_light_evidence_report.json",
        "traffic_light_evidence_report.json",
        "analysis/traffic_light_behavior/traffic_light_behavior_report.json",
        "analysis/traffic_light/traffic_light_behavior_report.json",
        "traffic_light_behavior_report.json",
        "analysis/traffic_light_contract/traffic_light_contract_report.json",
        "analysis/traffic_light/traffic_light_contract_report.json",
        "traffic_light_contract_report.json",
    ]


def _has_assist_ledger_artifact(
    *,
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    assist_ledger_path: Path | None,
) -> bool:
    if assist_ledger_path is not None:
        return True
    return isinstance(summary.get("assist_ledger"), Mapping) or isinstance(manifest.get("assist_ledger"), Mapping)


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
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


def _as_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_text(*items: Any, default: str | None = None) -> str | None:
    for index in range(0, len(items), 2):
        source = items[index] if index < len(items) else None
        key = items[index + 1] if index + 1 < len(items) else None
        if not isinstance(source, Mapping) or key is None:
            continue
        value = source.get(key)
        if value not in {None, ""}:
            return str(value)
    return default


def _report_status(payload: Mapping[str, Any]) -> str:
    status = payload.get("status")
    if _scalar_status_value(status):
        return str(status)
    verdict = payload.get("verdict")
    if isinstance(verdict, Mapping):
        value = verdict.get("status")
        if _scalar_status_value(value):
            return str(value)
    if _scalar_status_value(verdict):
        return str(verdict)
    if payload.get("artifact_complete") is True:
        return "pass"
    return "insufficient_data"


def _scalar_status_value(value: Any) -> bool:
    return isinstance(value, str | int | float | bool) and value not in {None, ""}


def _localization_blocking_reasons(report: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = _blocking_reasons(report)
    reference = report.get("reference_point")
    if isinstance(reference, Mapping) and reference.get("position_uses_vrp") is False:
        reasons.append("position_uses_vrp_false")
    frame_transform = report.get("frame_transform")
    if isinstance(frame_transform, Mapping):
        if frame_transform.get("uses_configured_transform") is False:
            reasons.append("frame_transform_missing")
        if frame_transform.get("y_flip_or_axis_mapping_declared") is False:
            reasons.append("axis_mapping_missing")
    channel = report.get("channel")
    if isinstance(channel, Mapping):
        if channel.get("timestamp_monotonic") is False:
            reasons.append("timestamp_non_monotonic")
        if channel.get("sequence_monotonic") is False:
            reasons.append("sequence_non_monotonic")
    return sorted(set(reasons))


def _blocking_reasons(report: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for key in ("blocking_reasons",):
        reasons.extend(str(item) for item in (report.get(key) or []) if item)
    verdict = report.get("verdict")
    if isinstance(verdict, Mapping):
        reasons.extend(str(item) for item in (verdict.get("blocking_reasons") or []) if item)
    return sorted(set(reasons))


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in {None, ""}:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None
