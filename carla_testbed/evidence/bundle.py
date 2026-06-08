from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.artifact_completeness import check_run_artifact_completeness
from carla_testbed.platform.plan import RunPlan

EVIDENCE_BUNDLE_SCHEMA_VERSION = "evidence_bundle.v1"


@dataclass(frozen=True)
class EvidenceArtifact:
    name: str
    path: str | None
    status: str
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "status": self.status,
            "summary": dict(self.summary),
        }


REPORT_CANDIDATES = {
    "manifest": ("manifest.json",),
    "summary": ("summary.json",),
    "route_health": (
        "analysis/route_health/route_health.json",
        "route_health.json",
    ),
    "channel_health": (
        "analysis/apollo_channel_health/apollo_channel_health_report.json",
        "apollo_channel_health_report.json",
    ),
    "localization_contract": (
        "analysis/localization_contract/localization_contract_report.json",
        "localization_contract_report.json",
    ),
    "chassis_gt_contract": (
        "analysis/chassis_gt_contract/chassis_gt_contract_report.json",
        "chassis_gt_contract_report.json",
    ),
    "apollo_hdmap_projection": (
        "analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json",
        "apollo_hdmap_projection_report.json",
    ),
    "apollo_reference_line_contract": (
        "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        "apollo_reference_line_contract_report.json",
    ),
    "planning_materialization": (
        "analysis/planning_materialization/planning_materialization_report.json",
        "planning_materialization_report.json",
    ),
    "apollo_control_handoff": (
        "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
        "apollo_control_handoff_report.json",
    ),
    "control_health": (
        "analysis/control_health/control_health_report.json",
        "control_health_report.json",
    ),
    "control_attribution": (
        "analysis/control_attribution/control_attribution_report.json",
        "control_attribution_report.json",
    ),
    "prediction_evidence": (
        "analysis/prediction_evidence/prediction_evidence_report.json",
        "prediction_evidence_report.json",
    ),
    "traffic_light_contract": (
        "analysis/traffic_light_contract/traffic_light_contract_report.json",
        "traffic_light_contract_report.json",
    ),
    "traffic_light_behavior": (
        "analysis/traffic_light_behavior/traffic_light_behavior_report.json",
        "traffic_light_behavior_report.json",
    ),
    "obstacle_gt_contract": (
        "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        "obstacle_gt_contract_report.json",
    ),
    "fixed_scene_contract": (
        "analysis/fixed_scene_contract/fixed_scene_contract_report.json",
        "fixed_scene_contract_report.json",
    ),
    "scenario_actor_contract": (
        "analysis/scenario_actor_contract/scenario_actor_contract_report.json",
        "scenario_actor_contract_report.json",
    ),
    "traffic_flow_contract": (
        "analysis/traffic_flow_contract/traffic_flow_contract_report.json",
        "traffic_flow_contract_report.json",
    ),
    "pedestrian_flow_contract": (
        "analysis/pedestrian_flow_contract/pedestrian_flow_contract_report.json",
        "pedestrian_flow_contract_report.json",
    ),
    "assist_ledger": (
        "analysis/assist_ledger/assist_ledger.json",
        "assist_ledger.json",
    ),
    "apollo_link_health": (
        "analysis/apollo_link_health/apollo_link_health_report.json",
        "apollo_link_health_report.json",
    ),
    "natural_driving": (
        "analysis/natural_driving/natural_driving_report.json",
        "natural_driving_report.json",
    ),
    "autoware_evidence": (
        "analysis/autoware_evidence/autoware_evidence_report.json",
        "autoware_evidence_report.json",
    ),
    "autoware_tf_health": (
        "analysis/autoware_tf_health/autoware_tf_health_report.json",
        "autoware_tf_health_report.json",
    ),
    "autoware_route_state": (
        "analysis/autoware_route_state/autoware_route_state_report.json",
        "autoware_route_state_report.json",
    ),
    "autoware_engage_state": (
        "analysis/autoware_engage_state/autoware_engage_state_report.json",
        "autoware_engage_state_report.json",
    ),
    "autoware_planning_trajectory_health": (
        "analysis/autoware_planning_trajectory_health/autoware_planning_trajectory_health_report.json",
        "autoware_planning_trajectory_health_report.json",
    ),
    "autoware_control_command_health": (
        "analysis/autoware_control_command_health/autoware_control_command_health_report.json",
        "autoware_control_command_health_report.json",
    ),
    "autoware_lanelet_map_contract": (
        "analysis/autoware_lanelet_map_contract/autoware_lanelet_map_contract_report.json",
        "autoware_lanelet_map_contract_report.json",
    ),
}


def build_evidence_bundle(
    run_dir: str | Path,
    *,
    plan: RunPlan | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    plan_payload = plan.to_dict() if isinstance(plan, RunPlan) else dict(plan or {})
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    artifact_completeness = check_run_artifact_completeness(root)
    artifacts = [
        EvidenceArtifact(
            name="artifact_completeness",
            path=None,
            status=str(artifact_completeness.get("status") or "insufficient_data"),
            summary={
                "schema_version": artifact_completeness.get("schema_version"),
                "status": artifact_completeness.get("status"),
            },
        )
    ]
    artifacts.extend(_artifact_summary(root, name, candidates) for name, candidates in REPORT_CANDIDATES.items())
    missing = [artifact.name for artifact in artifacts if artifact.status == "missing"]
    present = [artifact.name for artifact in artifacts if artifact.status != "missing"]
    required = _required_evidence_names(plan_payload)
    not_applicable = _not_applicable_evidence_names(plan_payload)
    missing_required = [name for name in required if name not in present]
    status = "pass"
    if missing_required:
        status = "insufficient_data"
    elif artifact_completeness.get("status") not in {"pass", "warn", None}:
        status = str(artifact_completeness.get("status"))
    return {
        "schema_version": EVIDENCE_BUNDLE_SCHEMA_VERSION,
        "run_dir": str(root),
        "run_id": _first(summary, "run_id") or _first(manifest, "run_id") or root.name,
        "scenario_id": _first(summary, "scenario_id") or _first(manifest, "scenario_id"),
        "scenario_class": _first(summary, "scenario_class") or _first(manifest, "scenario_class"),
        "route_id": _first(summary, "route_id") or _first(manifest, "route_id"),
        "backend": _first(summary, "backend") or _first(manifest, "backend") or _first(manifest, "backend_name"),
        "ego_control_source": _control_source(
            plan_payload,
            manifest,
            "ego_control_source",
            default=_default_ego_control_source(plan_payload, manifest, summary),
        ),
        "scenario_actor_control_source": _control_source(
            plan_payload,
            manifest,
            "scenario_actor_control_source",
            default=_default_scenario_actor_control_source(plan_payload),
        ),
        "background_traffic_control_source": _control_source(
            plan_payload,
            manifest,
            "background_traffic_control_source",
            default=_default_background_traffic_source(plan_payload),
        ),
        "background_walker_control_source": _control_source(
            plan_payload,
            manifest,
            "background_walker_control_source",
            default=_default_background_walker_source(plan_payload),
        ),
        "plan": plan_payload or None,
        "status": status,
        "required_evidence": required,
        "not_applicable_evidence": not_applicable,
        "present_evidence": present,
        "missing_evidence": missing,
        "missing_required_evidence": missing_required,
        "artifacts": {artifact.name: artifact.to_dict() for artifact in artifacts},
        "artifact_completeness": artifact_completeness,
        "claim_boundary": (
            "Evidence bundle is an index. It cannot turn operator video, RViz, Dreamview, "
            "or rosbag presence into a natural-driving pass."
        ),
    }


def write_evidence_bundle(bundle: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    path = output / "evidence_bundle.json"
    path.write_text(json.dumps(dict(bundle), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"evidence_bundle": str(path)}


def build_and_write_evidence_bundle(
    run_dir: str | Path,
    *,
    out_dir: str | Path | None = None,
    plan: RunPlan | Mapping[str, Any] | None = None,
) -> dict[str, str]:
    root = Path(run_dir).expanduser()
    bundle = build_evidence_bundle(root, plan=plan)
    output = Path(out_dir).expanduser() if out_dir is not None else root / "analysis" / "evidence_bundle"
    return write_evidence_bundle(bundle, output)


def _artifact_summary(root: Path, name: str, candidates: tuple[str, ...]) -> EvidenceArtifact:
    path = _find_first(root, candidates)
    if path is None:
        return EvidenceArtifact(name=name, path=None, status="missing")
    payload = _read_json(path)
    summary = {}
    if payload:
        summary = {
            "schema_version": payload.get("schema_version"),
            "status": _status_from_report(payload),
            "verdict": payload.get("verdict"),
        }
    return EvidenceArtifact(
        name=name,
        path=str(path),
        status=str(summary.get("status") or "present"),
        summary=summary,
    )


def _status_from_report(payload: Mapping[str, Any]) -> str | None:
    verdict = payload.get("verdict")
    if isinstance(verdict, Mapping):
        status = verdict.get("status")
        if status is not None:
            return str(status)
    for key in ("status", "verdict"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    return None


def _required_evidence_names(plan_payload: Mapping[str, Any]) -> list[str]:
    evidence = plan_payload.get("evidence")
    if isinstance(evidence, Mapping):
        analyzers = evidence.get("required_analyzers") or []
        return [str(item) for item in analyzers]
    return []


def _not_applicable_evidence_names(plan_payload: Mapping[str, Any]) -> list[str]:
    evidence = plan_payload.get("evidence")
    if isinstance(evidence, Mapping):
        analyzers = evidence.get("not_applicable_analyzers") or []
        return [str(item) for item in analyzers]
    return []


def _find_first(root: Path, candidates: tuple[str, ...]) -> Path | None:
    for candidate in candidates:
        path = root / candidate
        if path.exists():
            return path
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.is_dir():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in {None, ""}:
            return value
    return None


def _control_source(
    plan_payload: Mapping[str, Any],
    manifest: Mapping[str, Any],
    key: str,
    *,
    default: str,
) -> str:
    value = manifest.get(key)
    if value not in {None, ""}:
        return str(value)
    compatibility = plan_payload.get("compatibility")
    if isinstance(compatibility, Mapping) and compatibility.get(key) not in {None, ""}:
        return str(compatibility[key])
    return default


def _default_ego_control_source(
    plan_payload: Mapping[str, Any],
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    algorithm = plan_payload.get("algorithm") if isinstance(plan_payload.get("algorithm"), Mapping) else {}
    platform = plan_payload.get("platform") if isinstance(plan_payload.get("platform"), Mapping) else {}
    stack = str(algorithm.get("stack") or "")
    if stack in {"apollo", "autoware", "builtin"}:
        return "carla_builtin" if stack == "builtin" else stack
    backend = str(manifest.get("backend") or summary.get("backend") or platform.get("name") or "unknown")
    return "carla_builtin" if backend == "carla_builtin" else backend


def _default_background_traffic_source(plan_payload: Mapping[str, Any]) -> str:
    traffic = plan_payload.get("traffic_flow") if isinstance(plan_payload.get("traffic_flow"), Mapping) else {}
    vehicles = traffic.get("vehicles") if isinstance(traffic.get("vehicles"), Mapping) else {}
    provider = str(traffic.get("provider") or "none")
    if not traffic.get("enabled"):
        return "none"
    if bool(vehicles.get("enabled", provider in {"carla_traffic_manager", "mixed_carla_flow"})):
        return "carla_traffic_manager" if provider in {"carla_traffic_manager", "mixed_carla_flow"} else provider
    return "none"


def _default_background_walker_source(plan_payload: Mapping[str, Any]) -> str:
    traffic = plan_payload.get("traffic_flow") if isinstance(plan_payload.get("traffic_flow"), Mapping) else {}
    walkers = traffic.get("walkers") if isinstance(traffic.get("walkers"), Mapping) else {}
    provider = str(traffic.get("provider") or "none")
    if not traffic.get("enabled"):
        return "none"
    if bool(walkers.get("enabled", provider in {"carla_walker_ai_controller", "mixed_carla_flow"})):
        if provider in {"carla_walker_ai_controller", "mixed_carla_flow"}:
            return "carla_walker_ai_controller"
        return provider
    return "none"


def _default_scenario_actor_control_source(plan_payload: Mapping[str, Any]) -> str:
    if _has_fixed_scene(plan_payload):
        return "fixed_scene_player"
    if _has_scenario_actors(plan_payload):
        return "scripted_template"
    return "none"


def _has_scenario_actors(plan_payload: Mapping[str, Any]) -> bool:
    scenario = plan_payload.get("scenario") if isinstance(plan_payload.get("scenario"), Mapping) else {}
    if _has_fixed_scene(plan_payload):
        return True
    actors = scenario.get("actors")
    if not isinstance(actors, Mapping):
        return bool(actors)
    for key, value in actors.items():
        if str(key) in {"ego", "traffic_lights"}:
            continue
        if isinstance(value, list) and not value:
            continue
        if isinstance(value, Mapping) and not value:
            continue
        if value:
            return True
    return False


def _has_fixed_scene(plan_payload: Mapping[str, Any]) -> bool:
    scenario = plan_payload.get("scenario") if isinstance(plan_payload.get("scenario"), Mapping) else {}
    fixed_scene = scenario.get("fixed_scene") if isinstance(scenario.get("fixed_scene"), Mapping) else {}
    return bool(fixed_scene and fixed_scene.get("enabled", True))
