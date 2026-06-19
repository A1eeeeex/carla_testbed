from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml


PHASE1_APOLLO_FIXED_SCENE_READINESS_SCHEMA_VERSION = "phase1_apollo_fixed_scene_readiness.v1"


def analyze_apollo_fixed_scene_readiness(
    *,
    backend: str | None = None,
    backend_type: str | None = None,
    target_actor_contract: Mapping[str, Any] | None = None,
    bridge_config_path: str | Path | None = None,
    bridge_config: Mapping[str, Any] | None = None,
    bridge_config_source: str | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    """Check whether Apollo bridge config can expose fixed-scene target obstacle rows.

    This is a preflight/readiness report only. It does not execute CARLA,
    CyberRT, or Apollo, and it does not prove backend behavior.
    """

    target_contract = dict(target_actor_contract or {})
    effective_backend_type = backend_type or ("apollo_reference_backend" if backend == "apollo_cyberrt" else None)
    report: dict[str, Any] = {
        "schema_version": PHASE1_APOLLO_FIXED_SCENE_READINESS_SCHEMA_VERSION,
        "backend": backend,
        "backend_type": effective_backend_type,
        "status": "not_applicable",
        "target_actor_contract": target_contract,
        "target_actor_role": target_contract.get("target_actor_role") or target_contract.get("role"),
        "target_actor_aliases": _target_aliases(target_contract),
        "accepted_target_roles": [],
        "covered_bridge_roles": [],
        "bridge_config_path": None,
        "bridge_config_source": "missing",
        "front_obstacle_behavior": {},
        "actor_probe_enabled_effective": None,
        "actor_probe_enabled_source": None,
        "target_role_covered_by_bridge_roles": None,
        "expected_obstacle_row_fields": [
            "front_obstacle_actor_probe_enabled",
            "front_obstacle_actor_role",
            "front_obstacle_actor_id",
            "carla_actor_id",
            "ego_actor_id",
        ],
        "blocking_reasons": [],
        "warnings": [],
        "missing_fields": [],
        "next_action": [],
        "claim_boundary": (
            "Phase 1 Apollo fixed-scene readiness checks whether bridge config "
            "can materialize row-level target-obstacle evidence. It is not "
            "online behavior evidence."
        ),
    }

    if effective_backend_type != "apollo_reference_backend":
        report["warnings"].append("not_apollo_reference_backend")
        return report
    if target_contract.get("required") is False or target_contract.get("status") == "not_required":
        report["warnings"].append("target_actor_not_required")
        return report
    target_role = report["target_actor_role"]
    if not target_role:
        report["status"] = "insufficient_data"
        report["missing_fields"].append("target_actor_contract.target_actor_role")
        report["next_action"].append("declare target_actor.role in the ScenarioCase or fixed-scene contract")
        return report

    config, config_path, config_source = _load_bridge_config(
        bridge_config=bridge_config,
        bridge_config_source=bridge_config_source,
        bridge_config_path=bridge_config_path,
        repo_root=repo_root,
    )
    report["bridge_config_path"] = str(config_path) if config_path else None
    report["bridge_config_source"] = config_source
    if config is None:
        report["status"] = "insufficient_data"
        report["missing_fields"].append("bridge.front_obstacle_behavior")
        report["next_action"].append("provide the Apollo bridge config used by the online run")
        return report

    front_cfg, front_path = _find_front_obstacle_behavior(config)
    if front_cfg is None:
        report["status"] = "insufficient_data"
        report["missing_fields"].append("bridge.front_obstacle_behavior")
        report["next_action"].append("add bridge.front_obstacle_behavior to the Apollo bridge config")
        return report

    mode = str(front_cfg.get("mode", "normal") or "normal").strip().lower()
    role_names = [str(item) for item in (front_cfg.get("role_names") or ["front"]) if str(item)]
    if "actor_probe_enabled" in front_cfg:
        actor_probe_enabled = bool(front_cfg.get("actor_probe_enabled"))
        actor_probe_source = "explicit"
    else:
        actor_probe_enabled = mode != "normal"
        actor_probe_source = "mode_default"
    bridge_roles = set(role_names)
    accepted_roles = {str(target_role), *report["target_actor_aliases"]}
    accepted_roles = {role for role in accepted_roles if role}
    covered_bridge_roles = sorted(bridge_roles.intersection(accepted_roles))
    target_role_covered = bool(bridge_roles.intersection(accepted_roles))
    sample_stride = _int_or_none(front_cfg.get("claim_evidence_artifact_sample_stride"))
    if sample_stride is None:
        bridge_cfg = _bridge_block(config)
        sample_stride = _int_or_none(bridge_cfg.get("claim_evidence_artifact_sample_stride"))

    report["front_obstacle_behavior"] = {
        "config_path": front_path,
        "mode": mode,
        "role_names": role_names,
        "actor_probe_enabled": actor_probe_enabled,
        "actor_probe_enabled_source": actor_probe_source,
        "claim_evidence_artifact_sample_stride": sample_stride,
    }
    report["actor_probe_enabled_effective"] = actor_probe_enabled
    report["actor_probe_enabled_source"] = actor_probe_source
    report["accepted_target_roles"] = sorted(accepted_roles)
    report["covered_bridge_roles"] = covered_bridge_roles
    report["target_role_covered_by_bridge_roles"] = target_role_covered

    if not actor_probe_enabled:
        report["blocking_reasons"].append("front_obstacle_actor_probe_disabled")
        report["next_action"].append("set bridge.front_obstacle_behavior.actor_probe_enabled=true for fixed-scene target evidence")
    if not target_role_covered:
        report["blocking_reasons"].append("target_role_not_in_front_obstacle_role_names")
        report["next_action"].append(
            f"include {target_role!s} in bridge.front_obstacle_behavior.role_names for this ScenarioCase"
        )
    if actor_probe_source == "mode_default":
        report["warnings"].append("actor_probe_enabled_inferred_from_front_obstacle_mode")
    if target_role_covered and str(target_role) not in role_names:
        report["warnings"].append("target_role_covered_by_operator_declared_alias")
    if sample_stride and sample_stride > 1:
        report["warnings"].append("claim_evidence_artifact_sample_stride_above_one")
    if config_source == "default_bridge_template":
        report["warnings"].append("readiness_based_on_default_bridge_template_not_resolved_run_config")

    if report["blocking_reasons"]:
        report["status"] = "fail"
    elif report["warnings"]:
        report["status"] = "warn"
    else:
        report["status"] = "pass"
    return report


def write_apollo_fixed_scene_readiness_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "phase1_apollo_fixed_scene_readiness_report.json"
    summary_path = output / "phase1_apollo_fixed_scene_readiness_summary.md"
    payload = dict(report)
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_summary_markdown(payload), encoding="utf-8")
    return {
        "phase1_apollo_fixed_scene_readiness_report": str(report_path),
        "phase1_apollo_fixed_scene_readiness_summary": str(summary_path),
    }


def write_apollo_fixed_scene_readiness_for_plan(
    plan: Any,
    run_dir: str | Path,
    *,
    repo_root: str | Path,
    target_actor_contract: Mapping[str, Any] | None = None,
    bridge_config_path: str | Path | None = None,
) -> dict[str, Any]:
    backend = getattr(getattr(plan, "platform", None), "name", None)
    report = analyze_apollo_fixed_scene_readiness(
        backend=str(backend) if backend else None,
        target_actor_contract=target_actor_contract,
        bridge_config_path=bridge_config_path,
        repo_root=repo_root,
    )
    paths = write_apollo_fixed_scene_readiness_report(
        report,
        Path(run_dir).expanduser() / "analysis" / "phase1_apollo_fixed_scene_readiness",
    )
    return {"status": report.get("status"), "paths": paths, "report": report}


def _load_bridge_config(
    *,
    bridge_config: Mapping[str, Any] | None,
    bridge_config_source: str | None,
    bridge_config_path: str | Path | None,
    repo_root: str | Path | None,
) -> tuple[dict[str, Any] | None, Path | None, str]:
    if isinstance(bridge_config, Mapping):
        return dict(bridge_config), None, bridge_config_source or "provided_mapping"
    if bridge_config_path:
        path = Path(bridge_config_path).expanduser()
        if not path.is_absolute() and repo_root:
            path = Path(repo_root).expanduser() / path
        return _read_yaml(path), path, "explicit_path"
    if repo_root:
        default_path = Path(repo_root).expanduser() / "tools" / "apollo10_cyber_bridge" / "config_example.yaml"
        if default_path.exists():
            return _read_yaml(default_path), default_path, "default_bridge_template"
    return None, None, "missing"


def _read_yaml(path: Path) -> dict[str, Any] | None:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _find_front_obstacle_behavior(config: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    bridge = _bridge_block(config)
    front = bridge.get("front_obstacle_behavior")
    if isinstance(front, Mapping):
        return dict(front), "bridge.front_obstacle_behavior"
    direct = config.get("front_obstacle_behavior")
    if isinstance(direct, Mapping):
        return dict(direct), "front_obstacle_behavior"
    found = _find_nested_mapping(config, "front_obstacle_behavior")
    return found


def _bridge_block(config: Mapping[str, Any]) -> dict[str, Any]:
    bridge = config.get("bridge")
    return dict(bridge) if isinstance(bridge, Mapping) else {}


def _find_nested_mapping(config: Mapping[str, Any], key: str, prefix: str = "") -> tuple[dict[str, Any] | None, str | None]:
    for name, value in config.items():
        path = f"{prefix}.{name}" if prefix else str(name)
        if name == key and isinstance(value, Mapping):
            return dict(value), path
        if isinstance(value, Mapping):
            found, found_path = _find_nested_mapping(value, key, path)
            if found is not None:
                return found, found_path
    return None, None


def _target_aliases(contract: Mapping[str, Any]) -> list[str]:
    aliases: set[str] = set()
    raw = contract.get("role_aliases")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if key:
                aliases.add(str(key))
            if value:
                aliases.add(str(value))
    elif isinstance(raw, list):
        aliases.update(str(item) for item in raw if item)
    return sorted(aliases)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Apollo Fixed-Scene Readiness",
        "",
        f"- Status: {report.get('status')}",
        f"- Backend: {report.get('backend') or report.get('backend_type')}",
        f"- Target actor role: {report.get('target_actor_role')}",
        f"- Bridge config source: {report.get('bridge_config_source')}",
        f"- Bridge config path: {report.get('bridge_config_path')}",
        f"- Actor probe enabled: {report.get('actor_probe_enabled_effective')}",
        f"- Target role covered by bridge roles: {report.get('target_role_covered_by_bridge_roles')}",
        f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
        f"- Warnings: {', '.join(report.get('warnings') or []) or 'none'}",
        "",
        "This report is readiness evidence only. It does not prove Apollo fixed-scene behavior.",
        "",
    ]
    return "\n".join(lines)
