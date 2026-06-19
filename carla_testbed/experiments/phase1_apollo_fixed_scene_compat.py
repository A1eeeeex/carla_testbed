from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract

PHASE1_APOLLO_FIXED_SCENE_COMPAT_SCHEMA_VERSION = "phase1_apollo_fixed_scene_compat.v1"


def derive_apollo_fixed_scene_artifacts(run_dir: str | Path, *, overwrite: bool = False) -> dict[str, Any]:
    """Derive Phase 1 fixed-scene artifacts from Apollo row-level evidence.

    This adapter is deliberately conservative: it only writes runtime-state and
    scenario-actor trace files when raw Apollo obstacle GT contract rows expose
    a target actor role/id. It never invents an obstacle or backend behavior.
    """

    root = Path(run_dir).expanduser()
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    target_contract = _target_contract(root, manifest)
    report: dict[str, Any] = {
        "schema_version": PHASE1_APOLLO_FIXED_SCENE_COMPAT_SCHEMA_VERSION,
        "run_dir": str(root),
        "backend": manifest.get("backend") or manifest.get("backend_name") or summary.get("backend"),
        "backend_type": _backend_type(manifest, summary),
        "scenario_case": manifest.get("scenario_case") or manifest.get("fixed_scene_case"),
        "target_actor_contract": target_contract,
        "status": "not_applicable",
        "derived_artifacts": {},
        "preserved_artifacts": {},
        "artifact_sources": {},
        "source_artifacts": {},
        "row_count": 0,
        "derived_trace_rows": 0,
        "actor_roles": {},
        "missing_fields": [],
        "warnings": [],
        "claim_boundary": (
            "Derived fixed-scene compatibility artifacts expose observed Apollo "
            "row-level obstacle evidence only. They do not prove Apollo behavior success."
        ),
    }
    if report["backend_type"] != "apollo_reference_backend":
        report["warnings"].append("not_apollo_reference_backend")
        return _finalize_report(report, root)
    if target_contract.get("status") != "resolved" or not target_contract.get("target_actor_role"):
        report["status"] = "not_applicable"
        report["warnings"].append("target_actor_not_required_or_unresolved")
        return _finalize_report(report, root)

    raw_path = _first_existing(
        root / "artifacts" / "obstacle_gt_contract.jsonl",
        root / "artifacts" / "obstacle_gt_contract.json",
        root / "obstacle_gt_contract.jsonl",
        root / "obstacle_gt_contract.json",
    )
    if raw_path is None:
        report["status"] = "insufficient_data"
        report["missing_fields"].append("artifacts/obstacle_gt_contract.jsonl")
        return _finalize_report(report, root)
    report["source_artifacts"]["obstacle_gt_contract"] = str(raw_path)
    rows = _read_rows(raw_path)
    report["row_count"] = len(rows)
    actor_rows = [_actor_trace_row(row, target_contract, _template(root)) for row in rows]
    actor_rows = [row for row in actor_rows if row]
    if not actor_rows:
        report["status"] = "insufficient_data"
        report["missing_fields"].extend(
            ["obstacle_gt_contract.front_obstacle_actor_role", "obstacle_gt_contract.carla_actor_id"]
        )
        return _finalize_report(report, root)

    actor_roles = {
        str(row["actor_role"]): str(row["actor_id"])
        for row in actor_rows
        if row.get("actor_role") and row.get("actor_id")
    }
    report["actor_roles"] = actor_roles
    report["derived_trace_rows"] = len(actor_rows)
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    runtime_path = artifacts / "fixed_scene_runtime_state.json"
    trace_path = artifacts / "scenario_actor_trace.jsonl"
    phase_events_path = artifacts / "scenario_phase_events.jsonl"
    if runtime_path.exists() and not overwrite:
        report["warnings"].append("fixed_scene_runtime_state_exists_preserved")
        report["preserved_artifacts"]["fixed_scene_runtime_state"] = str(runtime_path)
        report["derived_artifacts"]["fixed_scene_runtime_state"] = str(runtime_path)
        report["artifact_sources"]["fixed_scene_runtime_state"] = "preserved_existing"
    else:
        runtime_payload = {
            "schema_version": "fixed_scene_runtime_state.v1",
            "status": "derived_from_apollo_obstacle_gt_contract",
            "actor_roles": actor_roles,
            "actor_role_source": "obstacle_gt_contract_record_roles",
            "source_artifacts": dict(report["source_artifacts"]),
            "claim_boundary": report["claim_boundary"],
        }
        runtime_path.write_text(json.dumps(runtime_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report["derived_artifacts"]["fixed_scene_runtime_state"] = str(runtime_path)
        report["artifact_sources"]["fixed_scene_runtime_state"] = "derived_from_apollo_obstacle_gt_contract"
    if trace_path.exists() and not overwrite:
        report["warnings"].append("scenario_actor_trace_exists_preserved")
        report["preserved_artifacts"]["scenario_actor_trace"] = str(trace_path)
        report["derived_artifacts"]["scenario_actor_trace"] = str(trace_path)
        report["artifact_sources"]["scenario_actor_trace"] = "preserved_existing"
    else:
        trace_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in actor_rows), encoding="utf-8")
        report["derived_artifacts"]["scenario_actor_trace"] = str(trace_path)
        report["artifact_sources"]["scenario_actor_trace"] = "derived_from_apollo_obstacle_gt_contract"
    if phase_events_path.exists() and not overwrite:
        report["warnings"].append("scenario_phase_events_exists_preserved")
        report["preserved_artifacts"]["scenario_phase_events"] = str(phase_events_path)
        report["derived_artifacts"]["scenario_phase_events"] = str(phase_events_path)
        report["artifact_sources"]["scenario_phase_events"] = "preserved_existing"
    else:
        phase_events = _phase_events(root, actor_rows)
        phase_events_path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in phase_events),
            encoding="utf-8",
        )
        report["derived_artifacts"]["scenario_phase_events"] = str(phase_events_path)
        report["artifact_sources"]["scenario_phase_events"] = "derived_from_apollo_obstacle_gt_contract"
    required_outputs = {"fixed_scene_runtime_state", "scenario_actor_trace", "scenario_phase_events"}
    missing_outputs = sorted(required_outputs - set(report["derived_artifacts"]))
    report["status"] = "pass" if not missing_outputs else "warn"
    if missing_outputs:
        report["missing_fields"].extend([f"derived_artifacts.{item}" for item in missing_outputs])
    return _finalize_report(report, root)


def _actor_trace_row(row: Mapping[str, Any], target_contract: Mapping[str, Any], template: str | None) -> dict[str, Any] | None:
    if _bool(row.get("is_ego")):
        return None
    target_role = str(target_contract.get("target_actor_role") or "")
    accepted_roles = _accepted_target_roles(target_contract)
    role = _str_first(row, "front_obstacle_actor_role", "target_actor_role", "actor_role", "role_name", "role")
    if not role:
        role = target_role if _str_first(row, "front_obstacle_actor_id", "carla_actor_id", "actor_id") else None
    if role not in accepted_roles:
        return None
    actor_id = _str_first(row, "carla_actor_id", "front_obstacle_actor_id", "actor_id", "apollo_perception_id")
    if not actor_id:
        return None
    timestamp = _float_first(row, "timestamp", "sim_time", "sim_time_s", "ts_sec")
    speed = _float_first(row, "front_obstacle_actor_speed_mps", "actual_speed_mps", "speed_mps")
    if speed is None and isinstance(row.get("velocity"), Mapping):
        speed = _to_float(row["velocity"].get("x"))
    yaw_deg = _float_first(row, "front_obstacle_actor_yaw_deg", "yaw_deg")
    phase = _str_first(row, "phase", "front_obstacle_phase")
    if not phase and template == "static_lead_stop":
        phase = "lead_hold_stop"
    trace = {
        "schema_version": "scenario_actor_trace.v1",
        "sim_time_sec": timestamp,
        "actor_role": target_role,
        "actor_id": actor_id,
        "source_actor_role": role if role != target_role else None,
        "actual_speed_mps": speed,
        "x": _float_first(row, "front_obstacle_actor_x", "x"),
        "y": _float_first(row, "front_obstacle_actor_y", "y"),
        "yaw_rad": math.radians(yaw_deg) if yaw_deg is not None else _float_first(row, "yaw_rad", "yaw"),
        "length_m": _float_first(row, "length", "length_m", "vehicle_length_m"),
        "width_m": _float_first(row, "width", "width_m", "vehicle_width_m"),
        "height_m": _float_first(row, "height", "height_m"),
        "longitudinal_to_ego_m": _float_first(row, "front_obstacle_gap_lon_m", "longitudinal_to_ego_m"),
        "lateral_to_ego_m": _float_first(row, "front_obstacle_gap_lat_m", "lateral_to_ego_m"),
        "distance_to_ego_m": _float_first(row, "front_obstacle_gap_distance_m", "distance_to_ego_m"),
        "phase": phase,
        "source": "derived_from_apollo_obstacle_gt_contract",
    }
    return {key: value for key, value in trace.items() if value is not None}


def _accepted_target_roles(target_contract: Mapping[str, Any]) -> set[str]:
    target_role = str(target_contract.get("target_actor_role") or "")
    accepted = {target_role} if target_role else set()
    aliases = target_contract.get("role_aliases") if isinstance(target_contract.get("role_aliases"), Mapping) else {}
    for key, value in aliases.items():
        if key:
            accepted.add(str(key))
        if value:
            accepted.add(str(value))
    return {item for item in accepted if item}


def _target_contract(root: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
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


def _template(root: Path) -> str | None:
    resolved = _read_json(root / "artifacts" / "fixed_scene_resolved.json")
    return str(resolved.get("template")) if resolved.get("template") else None


def _phase_events(root: Path, actor_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not actor_rows:
        return []
    resolved = _read_json(root / "artifacts" / "fixed_scene_resolved.json")
    scene_id = str(resolved.get("scene_id") or root.name)
    first_row = actor_rows[0]
    phase = str(first_row.get("phase") or _first_storyboard_phase(resolved) or "phase_observed")
    return [
        {
            "schema_version": "scenario_phase_event.v1",
            "scene_id": scene_id,
            "phase": phase,
            "event": "phase_started",
            "sim_time_sec": first_row.get("sim_time_sec"),
            "world_frame": first_row.get("world_frame"),
            "source": "derived_from_apollo_obstacle_gt_contract",
            "reason": (
                "phase1_apollo_fixed_scene_compatibility_observed_target_actor; "
                "not fixed_scene_player playback proof"
            ),
        }
    ]


def _first_storyboard_phase(resolved: Mapping[str, Any]) -> str | None:
    storyboard = resolved.get("storyboard")
    phases = storyboard.get("phases") if isinstance(storyboard, Mapping) else None
    if not isinstance(phases, list):
        return None
    for phase in phases:
        if isinstance(phase, Mapping) and phase.get("id"):
            return str(phase["id"])
    return None


def _backend_type(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str | None:
    explicit = manifest.get("backend_type") or summary.get("backend_type")
    if explicit:
        return str(explicit)
    backend = str(manifest.get("backend") or manifest.get("backend_name") or summary.get("backend") or "")
    return "apollo_reference_backend" if backend == "apollo_cyberrt" else None


def _write_report(report: Mapping[str, Any], root: Path) -> None:
    output = root / "analysis" / "phase1_apollo_fixed_scene_compat"
    output.mkdir(parents=True, exist_ok=True)
    (output / "phase1_apollo_fixed_scene_compat_report.json").write_text(
        json.dumps(dict(report), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _finalize_report(report: dict[str, Any], root: Path) -> dict[str, Any]:
    _write_report(report, root)
    return report


def _read_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, Mapping)]
        if isinstance(payload, Mapping):
            rows = payload.get("rows") or payload.get("records")
            if isinstance(rows, list):
                return [dict(item) for item in rows if isinstance(item, Mapping)]
            return [dict(payload)]
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, Mapping):
                rows.append(dict(payload))
    return rows


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _first_existing(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _str_first(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value not in {None, ""}:
            return str(value)
    return None


def _float_first(row: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _to_float(row.get(key))
        if value is not None:
            return value
    return None


def _to_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)
