from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

REPORT_SCHEMA_VERSION = "traffic_flow_contract.v1"


def analyze_traffic_flow_contract_files(
    *,
    run_dir: str | Path | None = None,
    manifest_path: str | Path | None = None,
    events_path: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser() if run_dir else None
    if root is not None:
        manifest_path = manifest_path or _find_first(
            root,
            ("artifacts/traffic_flow_manifest.json", "traffic_flow_manifest.json"),
        )
        events_path = events_path or _find_first(
            root,
            ("artifacts/traffic_flow_events.jsonl", "traffic_flow_events.jsonl"),
        )
    return analyze_traffic_flow_contract(
        manifest=_read_json(Path(manifest_path).expanduser() if manifest_path else None),
        events=_read_jsonl(Path(events_path).expanduser() if events_path else None),
        source={
            "run_dir": str(root) if root is not None else None,
            "manifest_path": str(manifest_path) if manifest_path else None,
            "events_path": str(events_path) if events_path else None,
        },
    )


def analyze_traffic_flow_contract(
    *,
    manifest: Mapping[str, Any] | None = None,
    events: Sequence[Mapping[str, Any]] = (),
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = manifest or {}
    source = source or {}
    warnings: list[str] = []
    blocking: list[str] = []
    missing: list[str] = []
    if not manifest:
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "status": "insufficient_data",
            "blocking_reasons": ["traffic_flow_manifest_missing"],
            "warnings": [],
            "provider": None,
            "enabled": None,
            "seed": None,
            "metrics": {},
            "actors": [],
            "missing_fields": ["traffic_flow_manifest"],
            "source": dict(source),
            "claim_boundary": _boundary(),
        }

    enabled = bool(manifest.get("enabled", False))
    provider = _text(manifest.get("provider"))
    actors = [dict(item) for item in manifest.get("actors") or [] if isinstance(item, Mapping)]
    requested = _int(manifest.get("requested_vehicle_count")) or 0
    spawned = _int(manifest.get("spawned_vehicle_count")) or len(actors)
    autopilot_enabled = _event_count(events, "autopilot_enabled")
    if enabled and requested > 0 and provider not in {"carla_traffic_manager", "mixed_carla_flow"}:
        blocking.append("traffic_flow_provider_invalid")
    if not enabled:
        status = "not_applicable"
    else:
        if manifest.get("seed") is None:
            missing.append("seed")
            blocking.append("traffic_flow_seed_missing")
        if requested <= 0:
            blocking.append("requested_vehicle_count_missing")
        if spawned < requested:
            warnings.append("spawned_vehicle_count_below_requested")
        if spawned == 0 and requested > 0:
            blocking.append("traffic_flow_no_vehicles_spawned")
        if autopilot_enabled < spawned:
            blocking.append("background_vehicle_autopilot_missing")
        if manifest.get("tm_synchronous_mode_effective") != manifest.get("world_synchronous_mode"):
            blocking.append("tm_sync_mismatch")
    role_names = [str(actor.get("role_name") or "") for actor in actors]
    if len(set(role_names)) != len(role_names):
        blocking.append("background_role_name_not_unique")
    ego_registered = any(role in {"ego", "hero"} for role in role_names)
    scenario_registered = any(
        role.startswith(("lead_vehicle", "cut_in_vehicle", "scenario_")) for role in role_names
    )
    background_all_tm = all(
        not str(actor.get("role_name") or "").startswith("background_vehicle")
        or str(actor.get("control_source") or actor.get("provider") or "") == "carla_traffic_manager"
        for actor in actors
    )
    if ego_registered:
        blocking.append("ego_registered_to_tm")
    if scenario_registered:
        blocking.append("scenario_actor_registered_to_tm")
    if not background_all_tm:
        blocking.append("background_vehicle_not_registered_to_tm")
    stuck_count = _event_count(events, "stuck_detected")
    collision_count = _event_count(events, "collision")
    if collision_count:
        blocking.append("traffic_flow_collision")
    if stuck_count:
        warnings.append("traffic_flow_stuck_actor_detected")

    status = "pass" if enabled else "not_applicable"
    if blocking:
        status = "fail"
    elif warnings and enabled:
        status = "warn"
    metrics = {
        "requested_vehicle_count": requested,
        "spawned_vehicle_count": spawned,
        "spawn_success_ratio": (float(spawned) / float(requested)) if requested else None,
        "autopilot_enabled_count": autopilot_enabled,
        "tm_sync_matches_world": manifest.get("tm_synchronous_mode_effective") == manifest.get("world_synchronous_mode"),
        "seed_recorded": manifest.get("seed") is not None,
        "role_name_unique": len(set(role_names)) == len(role_names),
        "ego_registered_to_tm": ego_registered,
        "scenario_actor_registered_to_tm": scenario_registered,
        "background_vehicle_all_tm_controlled": background_all_tm,
        "stuck_actor_count": stuck_count,
        "collision_count": collision_count,
        "min_initial_distance_to_ego_m": _min_initial_distance(actors),
    }
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "blocking_reasons": sorted(set(blocking)),
        "warnings": sorted(set(warnings)),
        "provider": provider,
        "enabled": enabled,
        "seed": manifest.get("seed"),
        "metrics": metrics,
        "actors": actors,
        "missing_fields": sorted(set(missing)),
        "source": dict(source),
        "claim_boundary": _boundary(),
    }


def write_traffic_flow_contract_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "traffic_flow_contract_report.json"
    summary_path = output / "traffic_flow_contract_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(traffic_flow_contract_summary_md(report), encoding="utf-8")
    return {
        "traffic_flow_contract_report": str(report_path),
        "traffic_flow_contract_summary": str(summary_path),
    }


def traffic_flow_contract_summary_md(report: Mapping[str, Any]) -> str:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    return "\n".join(
        [
            "# Traffic Flow Contract",
            "",
            f"- Status: {report.get('status')}",
            f"- Provider: {report.get('provider')}",
            f"- Enabled: {report.get('enabled')}",
            f"- Requested vehicles: {metrics.get('requested_vehicle_count')}",
            f"- Spawned vehicles: {metrics.get('spawned_vehicle_count')}",
            f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
            f"- Warnings: {', '.join(report.get('warnings') or []) or 'none'}",
        ]
    ) + "\n"


def _event_count(events: Sequence[Mapping[str, Any]], name: str) -> int:
    return sum(1 for event in events if str(event.get("event") or "") == name)


def _min_initial_distance(actors: Sequence[Mapping[str, Any]]) -> float | None:
    distances = [
        _number(actor.get("initial_distance_to_ego_m") or actor.get("distance_to_ego_m"))
        for actor in actors
    ]
    values = [value for value in distances if value is not None]
    return min(values) if values else None


def _boundary() -> str:
    return (
        "traffic_flow_contract only proves reproducible background Traffic Manager setup. "
        "It is not an Apollo/Autoware natural-driving pass."
    )


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
