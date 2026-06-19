from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from carla_testbed.analysis.apollo_control_handoff import analyze_and_write_apollo_control_handoff
from carla_testbed.analysis.apollo_hdmap_projection import (
    analyze_apollo_hdmap_projection_file,
    write_apollo_hdmap_projection_report,
)
from carla_testbed.analysis.apollo_link_health import analyze_and_write_apollo_link_health
from carla_testbed.analysis.apollo_reference_line_contract import (
    analyze_apollo_reference_line_contract_run_dir,
    write_apollo_reference_line_contract_report,
)
from carla_testbed.analysis.baguang_lane_event_contract import (
    analyze_baguang_lane_event_contract,
    write_baguang_lane_event_contract_report,
)
from carla_testbed.analysis.control_health import analyze_control_health_run_dir, write_control_health_report
from carla_testbed.analysis.obstacle_gt_contract import (
    analyze_obstacle_gt_contract_run_dir,
    write_obstacle_gt_contract_report,
)
from carla_testbed.analysis.phase1_status import classify_phase1_run, write_phase1_status
from carla_testbed.analysis.v_t_gap import extract_v_t_gap, write_v_t_gap_report
from carla_testbed.experiments.phase1_apollo_fixed_scene_compat import derive_apollo_fixed_scene_artifacts
from carla_testbed.experiments.phase1_apollo_fixed_scene_readiness import (
    analyze_apollo_fixed_scene_readiness,
    write_apollo_fixed_scene_readiness_report,
)
from carla_testbed.experiments.phase1_scenario_binding import bind_phase1_scenario_to_run
from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract

PHASE1_POSTPROCESS_SCHEMA_VERSION = "phase1_postprocess.v1"


def run_phase1_postprocess(run_dir: str | Path) -> dict[str, Any]:
    """Write Phase 1 run-local analysis artifacts.

    This is intentionally backend-neutral. It consumes already-written run
    artifacts and does not change runtime behavior.
    """

    root = Path(run_dir).expanduser()
    analysis = root / "analysis"
    apollo_fixed_scene_compat_report: dict[str, Any] | None = None
    apollo_fixed_scene_readiness_report: dict[str, Any] | None = None
    apollo_fixed_scene_readiness_paths: dict[str, str] | None = None
    apollo_control_handoff_report: dict[str, Any] | None = None
    apollo_control_handoff_paths: dict[str, str] | None = None
    apollo_hdmap_projection_report: dict[str, Any] | None = None
    apollo_hdmap_projection_paths: dict[str, str] | None = None
    apollo_reference_line_contract_report: dict[str, Any] | None = None
    apollo_reference_line_contract_paths: dict[str, str] | None = None
    control_health_report: dict[str, Any] | None = None
    control_health_paths: dict[str, str] | None = None
    apollo_link_health_report: dict[str, Any] | None = None
    apollo_link_health_paths: dict[str, str] | None = None
    phase1_scenario_binding_report = _ensure_phase1_scenario_binding(root)
    if _is_apollo_fixed_scene_target_run(root):
        apollo_fixed_scene_readiness_report, apollo_fixed_scene_readiness_paths = _write_apollo_readiness(root)
        apollo_fixed_scene_compat_report = derive_apollo_fixed_scene_artifacts(root)
        _write_obstacle_gt_contract(root)
        apollo_control_handoff_report, apollo_control_handoff_paths = _write_apollo_control_handoff(root)
        apollo_hdmap_projection_report, apollo_hdmap_projection_paths = _write_apollo_hdmap_projection(root)
        apollo_reference_line_contract_report, apollo_reference_line_contract_paths = _write_apollo_reference_line_contract(root)
        control_health_report, control_health_paths = _write_control_health(root)
        apollo_link_health_report, apollo_link_health_paths = _write_apollo_link_health(root)
    v_t_gap_report = extract_v_t_gap(run_dir=root)
    v_t_gap_paths = write_v_t_gap_report(v_t_gap_report, analysis / "v_t_gap")
    baguang_lane_event_report, baguang_lane_event_paths = _write_baguang_lane_event_contract(root)
    phase1_report = classify_phase1_run(root)
    phase1_paths = write_phase1_status(phase1_report, analysis / "phase1_status")
    completeness = build_phase1_artifact_completeness(root)
    completeness_path = analysis / "phase1_status" / "artifact_completeness.json"
    completeness_path.parent.mkdir(parents=True, exist_ok=True)
    completeness_path.write_text(json.dumps(completeness, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "schema_version": PHASE1_POSTPROCESS_SCHEMA_VERSION,
        "run_dir": str(root),
        "v_t_gap_status": v_t_gap_report.get("status"),
        "phase1_status": phase1_report.get("status"),
        "phase1_failure_reason": phase1_report.get("failure_reason"),
        "apollo_fixed_scene_compat_status": (
            apollo_fixed_scene_compat_report.get("status") if apollo_fixed_scene_compat_report else None
        ),
        "apollo_fixed_scene_readiness_status": (
            apollo_fixed_scene_readiness_report.get("status") if apollo_fixed_scene_readiness_report else None
        ),
        "phase1_scenario_binding_status": (
            phase1_scenario_binding_report.get("status") if phase1_scenario_binding_report else None
        ),
        "baguang_lane_event_contract_status": (
            baguang_lane_event_report.get("status") if baguang_lane_event_report else None
        ),
        "apollo_control_handoff_status": (
            apollo_control_handoff_report.get("status") if apollo_control_handoff_report else None
        ),
        "control_health_status": (control_health_report.get("status") if control_health_report else None),
        "apollo_hdmap_projection_status": (
            apollo_hdmap_projection_report.get("status") if apollo_hdmap_projection_report else None
        ),
        "apollo_reference_line_contract_status": (
            apollo_reference_line_contract_report.get("status") if apollo_reference_line_contract_report else None
        ),
        "apollo_link_health_primary_blocker": (
            apollo_link_health_report.get("primary_blocker") if apollo_link_health_report else None
        ),
        "artifact_completeness_status": completeness.get("status"),
        "outputs": {
            "v_t_gap": v_t_gap_paths,
            "phase1_status": phase1_paths,
            "artifact_completeness": str(completeness_path),
            **({"baguang_lane_event_contract": baguang_lane_event_paths} if baguang_lane_event_paths else {}),
            **(
                {
                    "obstacle_gt_contract": str(
                        root / "analysis" / "obstacle_gt_contract" / "obstacle_gt_contract_report.json"
                    )
                }
                if _obstacle_gt_contract_raw_exists(root)
                else {}
            ),
            **(
                {
                    "apollo_fixed_scene_compat": str(
                        root
                        / "analysis"
                        / "phase1_apollo_fixed_scene_compat"
                        / "phase1_apollo_fixed_scene_compat_report.json"
                    )
                }
                if apollo_fixed_scene_compat_report
                else {}
            ),
            **({"apollo_fixed_scene_readiness": apollo_fixed_scene_readiness_paths} if apollo_fixed_scene_readiness_paths else {}),
            **({"apollo_control_handoff": apollo_control_handoff_paths} if apollo_control_handoff_paths else {}),
            **({"control_health": control_health_paths} if control_health_paths else {}),
            **({"apollo_hdmap_projection": apollo_hdmap_projection_paths} if apollo_hdmap_projection_paths else {}),
            **(
                {"apollo_reference_line_contract": apollo_reference_line_contract_paths}
                if apollo_reference_line_contract_paths
                else {}
            ),
            **({"apollo_link_health": apollo_link_health_paths} if apollo_link_health_paths else {}),
            **(
                {
                    "phase1_scenario_binding": str(
                        root
                        / "analysis"
                        / "phase1_scenario_binding"
                        / "phase1_scenario_binding_report.json"
                    )
                }
                if phase1_scenario_binding_report
                else {}
            ),
        },
    }


def _write_apollo_control_handoff(root: Path) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    if not _apollo_control_handoff_raw_exists(root):
        return None, None
    paths = analyze_and_write_apollo_control_handoff(
        run_dir=root,
        out_dir=root / "analysis" / "apollo_control_handoff",
    )
    report = _read_json(Path(paths["apollo_control_handoff_report"]))
    return report, paths


def _apollo_control_handoff_raw_exists(root: Path) -> bool:
    return any(
        path.exists()
        for path in (
            root / "artifacts" / "cyber_bridge_stats.json",
            root / "artifacts" / "apollo_control_raw.jsonl",
            root / "artifacts" / "bridge_control_decode.jsonl",
            root / "artifacts" / "control_decode_debug.jsonl",
            root / "artifacts" / "control_apply_trace.jsonl",
        )
    )


def _write_control_health(root: Path) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    if not _control_health_raw_exists(root):
        return None, None
    report = analyze_control_health_run_dir(root)
    paths = write_control_health_report(report, root / "analysis" / "control_health")
    return report, paths


def _write_apollo_link_health(root: Path) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    if not (root / "manifest.json").exists() and not (root / "summary.json").exists():
        return None, None
    paths = analyze_and_write_apollo_link_health(root, out_dir=root / "analysis" / "apollo_link_health")
    report = _read_json(Path(paths["apollo_link_health_report"]))
    return report, paths


def _write_apollo_hdmap_projection(root: Path) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    projection_path = root / "artifacts" / "apollo_hdmap_projection.jsonl"
    if not projection_path.exists():
        return None, None
    report = analyze_apollo_hdmap_projection_file(projection_path)
    paths = write_apollo_hdmap_projection_report(report, root / "analysis" / "apollo_hdmap_projection")
    return report, paths


def _write_apollo_reference_line_contract(root: Path) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    if not _apollo_reference_line_contract_raw_exists(root):
        return None, None
    report = analyze_apollo_reference_line_contract_run_dir(root)
    paths = write_apollo_reference_line_contract_report(report, root / "analysis" / "apollo_reference_line_contract")
    return report, paths


def _apollo_reference_line_contract_raw_exists(root: Path) -> bool:
    if any(
        path.exists()
        for path in (
            root / "artifacts" / "apollo_reference_line_contract.jsonl",
            root / "apollo_reference_line_contract.jsonl",
            root / "artifacts" / "planning_topic_debug.jsonl",
            root / "planning_topic_debug.jsonl",
            root / "artifacts" / "planning_route_segment_debug.jsonl",
            root / "planning_route_segment_debug.jsonl",
            root / "artifacts" / "control_decode_debug.jsonl",
            root / "artifacts" / "bridge_control_decode.jsonl",
            root / "control_decode_debug.jsonl",
            root / "bridge_control_decode.jsonl",
        )
    ):
        return True
    return any(
        _csv_has_reference_line_fields(path)
        for path in (
            root / "artifacts" / "debug_timeseries.csv",
            root / "debug_timeseries.csv",
            root / "timeseries.csv",
        )
    )


def _csv_has_reference_line_fields(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        header = path.read_text(encoding="utf-8", errors="replace").splitlines()[0]
    except Exception:
        return False
    fields = {field.strip() for field in header.split(",") if field.strip()}
    return any(
        field in fields
        for field in (
            "planning_ref_heading_error_rad",
            "control_ref_heading_error_rad",
            "reference_line_heading",
            "reference_line_count",
            "route_segment_count",
            "apollo_reference_heading",
        )
    )


def _control_health_raw_exists(root: Path) -> bool:
    return any(
        path.exists()
        for path in (
            root / "artifacts" / "control_apply_trace.jsonl",
            root / "artifacts" / "bridge_control_decode.jsonl",
            root / "artifacts" / "control_decode_debug.jsonl",
            root / "timeseries.csv",
            root / "timeseries.jsonl",
        )
    )


def _write_obstacle_gt_contract(root: Path) -> dict[str, Any] | None:
    if not _obstacle_gt_contract_raw_exists(root):
        return None
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    scenario_class = str(
        manifest.get("scenario_class")
        or summary.get("scenario_class")
        or manifest.get("fixed_scene_case")
        or ""
    )
    report = analyze_obstacle_gt_contract_run_dir(root, scenario_class=scenario_class)
    write_obstacle_gt_contract_report(report, root / "analysis" / "obstacle_gt_contract")
    return report


def _obstacle_gt_contract_raw_exists(root: Path) -> bool:
    return any(
        path.exists()
        for path in (
            root / "artifacts" / "obstacle_gt_contract.jsonl",
            root / "artifacts" / "obstacle_gt_contract.json",
            root / "obstacle_gt_contract.jsonl",
            root / "obstacle_gt_contract.json",
        )
    )


def _write_baguang_lane_event_contract(root: Path) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    map_name = str(manifest.get("map") or _nested_value(summary, ("carla", "town")) or "")
    if map_name != "straight_road_for_baguang":
        return None, None
    if not (root / "timeseries.csv").exists():
        return None, None
    report = analyze_baguang_lane_event_contract(run_dirs=[root])
    paths = write_baguang_lane_event_contract_report(report, root / "analysis" / "baguang_lane_event_contract")
    return report, paths


def _write_apollo_readiness(root: Path) -> tuple[dict[str, Any], dict[str, str]]:
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    bridge_config_path = _bridge_config_path(root, manifest)
    runtime_bridge_config = None if bridge_config_path else _bridge_config_from_cyber_stats(root)
    report = analyze_apollo_fixed_scene_readiness(
        backend=str(manifest.get("backend") or manifest.get("backend_name") or summary.get("backend") or ""),
        backend_type=str(manifest.get("backend_type") or summary.get("backend_type") or ""),
        target_actor_contract=_target_actor_contract(root, manifest),
        bridge_config_path=bridge_config_path,
        bridge_config=runtime_bridge_config,
        bridge_config_source="runtime_cyber_bridge_stats" if runtime_bridge_config else None,
        repo_root=_repo_root(),
    )
    paths = write_apollo_fixed_scene_readiness_report(
        report,
        root / "analysis" / "phase1_apollo_fixed_scene_readiness",
    )
    return report, paths


def _ensure_phase1_scenario_binding(root: Path) -> dict[str, Any] | None:
    manifest = _read_json(root / "manifest.json")
    if _target_actor_contract(root, manifest).get("status") == "resolved" and (
        root / "artifacts" / "fixed_scene_resolved.json"
    ).exists():
        return None
    scenario_path = _phase1_scenario_path(root, manifest)
    if not scenario_path:
        return None
    role_aliases = _phase1_role_aliases(root, manifest)
    return bind_phase1_scenario_to_run(root, scenario_path, role_aliases=role_aliases)


def _phase1_scenario_path(root: Path, manifest: Mapping[str, Any]) -> Path | None:
    for value in (
        manifest.get("phase1_scenario_path"),
        manifest.get("scenario_path"),
        _nested_value(manifest, ("metadata", "phase1_scenario_path")),
        _nested_value(manifest, ("metadata", "recording", "artifacts", "phase1_scenario_path")),
    ):
        path = _existing_path(root, value)
        if path:
            return path
    for rel in ("config.resolved.yaml", "typed_runtime.effective_legacy.yaml"):
        payload = _read_yaml(root / rel)
        for value in _walk_values_for_key(payload, "phase1_scenario_path"):
            path = _existing_path(root, value)
            if path:
                return path
    return None


def _phase1_role_aliases(root: Path, manifest: Mapping[str, Any]) -> dict[str, str]:
    front_role = _nested_value(manifest, ("metadata", "scenario_metadata", "front_role"))
    if front_role in {None, "", "lead_vehicle"}:
        summary = _read_json(root / "summary.json")
        front_role = _nested_value(summary, ("metadata", "scenario_metadata", "front_role"))
    if front_role in {None, "", "lead_vehicle"}:
        return {}
    return {"lead_vehicle": str(front_role)}


def _existing_path(root: Path, value: Any) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    path = Path(value).expanduser()
    if path.is_absolute():
        return path if path.exists() else None
    for base in (root, _repo_root()):
        candidate = base / path
        if candidate.exists():
            return candidate
    return None


def _target_actor_contract(root: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if isinstance(manifest.get("target_actor_contract"), Mapping):
        merged.update(dict(manifest["target_actor_contract"]))
    resolved = _read_json(root / "artifacts" / "fixed_scene_resolved.json")
    contract = resolved.get("target_actor_contract")
    if isinstance(contract, Mapping):
        merged.update(dict(contract))
    if not merged and manifest:
        merged.update(resolve_target_actor_contract(manifest))
    return merged


def _bridge_config_path(root: Path, manifest: Mapping[str, Any]) -> str | Path | None:
    for key in (
        "bridge_config_path",
        "apollo_bridge_config_path",
        "resolved_bridge_config_path",
        "bridge_config",
    ):
        value = manifest.get(key)
        if isinstance(value, str) and value:
            return _resolve_run_path(root, value)
    launch_plan = manifest.get("launch_plan")
    if isinstance(launch_plan, Mapping):
        for key in ("bridge_config_path", "backend_config_path", "config_path"):
            value = launch_plan.get(key)
            if isinstance(value, str) and value:
                return _resolve_run_path(root, value)
    return None


def _bridge_config_from_cyber_stats(root: Path) -> dict[str, Any] | None:
    stats = _read_json(root / "artifacts" / "cyber_bridge_stats.json")
    front = stats.get("front_obstacle_behavior")
    if not isinstance(front, Mapping):
        return None
    return {"bridge": {"front_obstacle_behavior": dict(front)}}


def _resolve_run_path(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    candidate = root / path
    return candidate if candidate.exists() else _repo_root() / path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _nested_value(payload: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _walk_values_for_key(payload: Any, key: str):
    if isinstance(payload, Mapping):
        for name, value in payload.items():
            if name == key:
                yield value
            yield from _walk_values_for_key(value, key)
    elif isinstance(payload, list):
        for item in payload:
            yield from _walk_values_for_key(item, key)


def _is_apollo_fixed_scene_target_run(root: Path) -> bool:
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    backend = str(manifest.get("backend") or manifest.get("backend_name") or summary.get("backend") or "")
    backend_type = str(manifest.get("backend_type") or summary.get("backend_type") or "")
    if backend != "apollo_cyberrt" and backend_type != "apollo_reference_backend":
        return False
    target_contract = _target_actor_contract(root, manifest)
    if target_contract.get("status") != "resolved" or not target_contract.get("target_actor_role"):
        return False
    return bool(manifest.get("fixed_scene_enabled") or manifest.get("fixed_scene_case") or (root / "artifacts" / "fixed_scene_resolved.json").exists())


def build_phase1_artifact_completeness(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    backend = str(manifest.get("backend") or manifest.get("backend_name") or summary.get("backend") or "")
    backend_type = str(manifest.get("backend_type") or summary.get("backend_type") or "")
    ego_control_artifact = _phase1_control_trace_path(root, backend=backend, backend_type=backend_type)
    required = {
        "manifest": root / "manifest.json",
        "summary": root / "summary.json",
        "events": root / "events.jsonl",
        "timeseries": _first_existing(root / "timeseries.csv", root / "timeseries.jsonl"),
        "fixed_scene_resolved": root / "artifacts" / "fixed_scene_resolved.json",
        "fixed_scene_runtime_state": root / "artifacts" / "fixed_scene_runtime_state.json",
        "scenario_actor_trace": root / "artifacts" / "scenario_actor_trace.jsonl",
        "scenario_phase_events": root / "artifacts" / "scenario_phase_events.jsonl",
        "ego_control_trace": ego_control_artifact,
        "v_t_gap_report": root / "analysis" / "v_t_gap" / "v_t_gap_report.json",
        "phase1_status": root / "analysis" / "phase1_status" / "phase1_status.json",
    }
    artifacts: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for name, path in required.items():
        exists = bool(path and Path(path).exists())
        artifacts[name] = {"status": "present" if exists else "missing", "path": str(path) if path else None}
        if not exists:
            missing.append(name)
    return {
        "schema_version": "phase1_artifact_completeness.v1",
        "run_dir": str(root),
        "backend": backend or None,
        "backend_type": backend_type or None,
        "status": "pass" if not missing else "invalid",
        "missing_artifacts": missing,
        "artifacts": artifacts,
        "claim_boundary": "artifact completeness supports Phase 1 run evaluability only, not natural-driving capability.",
    }


def _first_existing(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _phase1_control_trace_path(root: Path, *, backend: str, backend_type: str) -> Path:
    if backend == "apollo_cyberrt" or backend_type == "apollo_reference_backend":
        return _first_existing(
            root / "artifacts" / "control_apply_trace.jsonl",
            root / "artifacts" / "bridge_control_decode.jsonl",
            root / "artifacts" / "apollo_control_raw.jsonl",
        ) or root / "artifacts" / "control_apply_trace.jsonl"
    return root / "artifacts" / "ego_control_trace.jsonl"
