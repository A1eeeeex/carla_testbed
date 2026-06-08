from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

PEDESTRIAN_FLOW_CONTRACT_SCHEMA_VERSION = "pedestrian_flow_contract.v1"

VALID_PROVIDERS = {"carla_walker_ai_controller", "mixed_carla_flow"}


def analyze_pedestrian_flow_contract_files(
    *,
    run_dir: str | Path | None = None,
    manifest_path: str | Path | None = None,
    events_path: str | Path | None = None,
    candidates_path: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser() if run_dir else None
    if root is not None:
        manifest_path = manifest_path or _find_first(root, ("artifacts/traffic_flow_manifest.json", "traffic_flow_manifest.json"))
        events_path = events_path or _find_first(root, ("artifacts/traffic_flow_events.jsonl", "traffic_flow_events.jsonl"))
        candidates_path = candidates_path or _find_first(
            root,
            ("artifacts/walker_spawn_candidates.jsonl", "walker_spawn_candidates.jsonl"),
        )
    return analyze_pedestrian_flow_contract(
        manifest=_read_json(Path(manifest_path).expanduser() if manifest_path else None),
        events=_read_jsonl(Path(events_path).expanduser() if events_path else None),
        candidates=_read_jsonl(Path(candidates_path).expanduser() if candidates_path else None),
        source={
            "run_dir": str(root) if root is not None else None,
            "manifest_path": str(manifest_path) if manifest_path else None,
            "events_path": str(events_path) if events_path else None,
            "candidates_path": str(candidates_path) if candidates_path else None,
        },
    )


def analyze_pedestrian_flow_contract(
    *,
    manifest: Mapping[str, Any] | None = None,
    events: Sequence[Mapping[str, Any]] = (),
    candidates: Sequence[Mapping[str, Any]] = (),
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = manifest or {}
    source = source or {}
    warnings: list[str] = []
    blocking: list[str] = []
    missing: list[str] = []
    if not manifest:
        return {
            "schema_version": PEDESTRIAN_FLOW_CONTRACT_SCHEMA_VERSION,
            "status": "insufficient_data",
            "provider": None,
            "enabled": None,
            "seed": None,
            "metrics": {},
            "walkers": [],
            "blocking_reasons": ["traffic_flow_manifest_missing"],
            "warnings": [],
            "missing_fields": ["traffic_flow_manifest"],
            "source": dict(source),
            "claim_boundary": _boundary(),
        }

    provider = _text(manifest.get("provider"))
    enabled = bool(manifest.get("enabled", False))
    requested = _int(manifest.get("requested_walker_count")) or 0
    walkers = [dict(item) for item in manifest.get("walkers") or [] if isinstance(item, Mapping)]
    if not walkers:
        walkers = [
            dict(item)
            for item in manifest.get("actors") or []
            if isinstance(item, Mapping)
            and str(item.get("control_source") or item.get("provider") or "") == "carla_walker_ai_controller"
        ]
    spawned = _int(manifest.get("spawned_walker_count")) or len(walkers)
    controller_count_value = _int(manifest.get("controller_count"))
    controller_started_value = _int(manifest.get("controller_started_count"))
    controller_count = controller_count_value if controller_count_value is not None else _count_controllers(walkers)
    controller_started = controller_started_value if controller_started_value is not None else _count_started(walkers)
    cross_factor = _number(manifest.get("walker_cross_factor"))

    if not enabled or requested <= 0:
        status = "not_applicable"
    else:
        if provider not in VALID_PROVIDERS:
            blocking.append("pedestrian_flow_provider_invalid")
        if manifest.get("seed") is None:
            missing.append("seed")
            blocking.append("pedestrian_flow_seed_missing")
        if manifest.get("world_pedestrians_seed") is None:
            missing.append("world_pedestrians_seed")
            warnings.append("world_pedestrians_seed_missing")
        if spawned < requested:
            blocking.append("spawned_walker_count_below_requested")
        if spawned == 0:
            blocking.append("pedestrian_flow_no_walkers_spawned")
        if controller_count < spawned:
            blocking.append("walker_ai_controller_missing")
        if controller_started < spawned:
            blocking.append("walker_ai_controller_not_started")
        if cross_factor is None:
            missing.append("walker_cross_factor")
            warnings.append("walker_cross_factor_missing")
        elif cross_factor > 0.0:
            warnings.append("walker_cross_factor_nonzero_builtin_crossing")

    role_names = [str(walker.get("role_name") or "") for walker in walkers]
    if len(set(role_names)) != len(role_names):
        blocking.append("walker_role_name_not_unique")
    ego_registered = any(role in {"ego", "hero"} for role in role_names)
    scenario_registered = any(role.startswith(("lead_vehicle", "cut_in_vehicle", "scenario_")) for role in role_names)
    if ego_registered:
        blocking.append("ego_registered_as_walker")
    if scenario_registered:
        blocking.append("scenario_actor_registered_as_walker")
    if _event_count(events, "walker_initial_overlap"):
        blocking.append("walker_initial_overlap")
    stuck_count = _event_count(events, "walker_stuck_detected")
    if stuck_count:
        warnings.append("pedestrian_flow_stuck_actor_detected")
    if manifest.get("destroyed_cleanly") is False:
        warnings.append("walkers_not_destroyed_cleanly")

    selected_candidates = sum(1 for item in candidates if bool(item.get("selected")))
    if requested > 0 and candidates and selected_candidates < min(requested, spawned):
        warnings.append("walker_spawn_candidates_selected_below_spawned")

    status = "pass" if enabled and requested > 0 else "not_applicable"
    if blocking:
        status = "fail"
    elif missing and enabled and requested > 0:
        status = "insufficient_data"
    elif warnings and enabled and requested > 0:
        status = "warn"

    metrics = {
        "requested_walker_count": requested,
        "spawned_walker_count": spawned,
        "spawn_success_ratio": (float(spawned) / float(requested)) if requested else None,
        "controller_count": controller_count,
        "controller_started_count": controller_started,
        "world_pedestrians_seed_recorded": manifest.get("world_pedestrians_seed") is not None,
        "walker_cross_factor": cross_factor,
        "role_name_unique": len(set(role_names)) == len(role_names),
        "ego_registered_as_walker": ego_registered,
        "scenario_actor_registered_as_walker": scenario_registered,
        "stuck_actor_count": stuck_count,
        "selected_spawn_candidate_count": selected_candidates,
        "destroyed_cleanly": manifest.get("destroyed_cleanly"),
    }
    return {
        "schema_version": PEDESTRIAN_FLOW_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "provider": provider,
        "enabled": enabled,
        "seed": manifest.get("seed"),
        "metrics": metrics,
        "walkers": walkers,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "missing_fields": sorted(set(missing)),
        "source": dict(source),
        "claim_boundary": _boundary(),
    }


def write_pedestrian_flow_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "pedestrian_flow_contract_report.json"
    summary_path = output / "pedestrian_flow_contract_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(pedestrian_flow_contract_summary_md(report), encoding="utf-8")
    return {
        "pedestrian_flow_contract_report": str(report_path),
        "pedestrian_flow_contract_summary": str(summary_path),
    }


def pedestrian_flow_contract_summary_md(report: Mapping[str, Any]) -> str:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    return "\n".join(
        [
            "# Pedestrian Flow Contract",
            "",
            f"- Status: {report.get('status')}",
            f"- Provider: {report.get('provider')}",
            f"- Enabled: {report.get('enabled')}",
            f"- Requested walkers: {metrics.get('requested_walker_count')}",
            f"- Spawned walkers: {metrics.get('spawned_walker_count')}",
            f"- Controllers started: {metrics.get('controller_started_count')}",
            f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
            f"- Warnings: {', '.join(report.get('warnings') or []) or 'none'}",
        ]
    ) + "\n"


def _boundary() -> str:
    return (
        "pedestrian_flow_contract only proves reproducible CARLA WalkerAIController setup. "
        "It is not Apollo/Autoware pedestrian perception, prediction, planning, or natural-driving evidence."
    )


def _count_controllers(walkers: Sequence[Mapping[str, Any]]) -> int:
    return sum(1 for walker in walkers if (walker.get("behavior") or {}).get("controller_id") not in {None, -1})


def _count_started(walkers: Sequence[Mapping[str, Any]]) -> int:
    return sum(1 for walker in walkers if bool((walker.get("behavior") or {}).get("controller_started")))


def _event_count(events: Sequence[Mapping[str, Any]], name: str) -> int:
    return sum(1 for event in events if str(event.get("event") or "") == name)


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _find_first(root: Path, rels: Sequence[str]) -> Path | None:
    for rel in rels:
        candidate = root / rel
        if candidate.is_file():
            return candidate
    return None


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
