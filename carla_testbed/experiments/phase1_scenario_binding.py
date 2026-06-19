from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.manifest_contract import PHASE1_ARTIFACT_CONTRACT_VERSION
from carla_testbed.scenario_player.schema import load_fixed_scene_template

PHASE1_SCENARIO_BINDING_SCHEMA_VERSION = "phase1_scenario_binding.v1"


def bind_phase1_scenario_to_run(
    run_dir: str | Path,
    scenario_path: str | Path,
    *,
    role_aliases: Mapping[str, str] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Attach an explicit Phase 1 ScenarioCase contract to an existing run.

    This is an operator-declared compatibility step for legacy online runs. It
    writes identity and target-role evidence only; it does not prove fixed-scene
    playback or backend behavior.
    """

    root = Path(run_dir).expanduser()
    scenario = Path(scenario_path).expanduser()
    storyboard = compile_fixed_scene_template(load_fixed_scene_template(scenario))
    aliases = {str(k): str(v) for k, v in (role_aliases or {}).items() if str(k) and str(v)}
    target_contract = dict(storyboard.get("target_actor_contract") or {})
    if aliases:
        merged_aliases = dict(target_contract.get("role_aliases") or {})
        merged_aliases.update(aliases)
        target_contract["role_aliases"] = merged_aliases
        warnings = list(target_contract.get("warnings") or [])
        warnings.append("phase1_scenario_binding_role_aliases_operator_declared")
        target_contract["warnings"] = warnings
    storyboard["target_actor_contract"] = target_contract

    artifacts_dir = root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    fixed_scene_path = artifacts_dir / "fixed_scene_resolved.json"
    fixed_scene_written = False
    fixed_scene_preserved = False
    if fixed_scene_path.exists() and not overwrite:
        fixed_scene_preserved = True
    else:
        fixed_scene_path.write_text(json.dumps(storyboard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        fixed_scene_written = True

    manifest_path = root / "manifest.json"
    manifest = _read_json(manifest_path)
    if not manifest:
        manifest = {"run_id": root.name}
    manifest.update(
        {
            "run_id": manifest.get("run_id") or root.name,
            "scenario_id": storyboard.get("scene_id"),
            "scenario_case": storyboard.get("scene_id"),
            "scenario_class": storyboard.get("scenario_class"),
            "map": storyboard.get("map"),
            "route_id": storyboard.get("route_id"),
            "fixed_scene_enabled": True,
            "fixed_scene_case": storyboard.get("scene_id"),
            "target_actor_contract": target_contract,
            "artifact_contract_version": PHASE1_ARTIFACT_CONTRACT_VERSION,
            "phase1_scenario_binding": {
                "schema_version": PHASE1_SCENARIO_BINDING_SCHEMA_VERSION,
                "scenario_path": str(scenario),
                "role_aliases": aliases,
                "source": "explicit_operator_binding",
                "claim_boundary": (
                    "Scenario binding identifies the requested Phase 1 ScenarioCase. "
                    "It does not verify fixed-scene playback or backend behavior."
                ),
            },
        }
    )
    if _backend_type(manifest):
        manifest["backend_type"] = _backend_type(manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = {
        "schema_version": PHASE1_SCENARIO_BINDING_SCHEMA_VERSION,
        "run_dir": str(root),
        "scenario_path": str(scenario),
        "scenario_case": storyboard.get("scene_id"),
        "scenario_class": storyboard.get("scenario_class"),
        "target_actor_contract": target_contract,
        "role_aliases": aliases,
        "fixed_scene_resolved_written": fixed_scene_written,
        "fixed_scene_resolved_preserved": fixed_scene_preserved,
        "manifest_path": str(manifest_path),
        "status": "pass",
        "warnings": (
            ["fixed_scene_resolved_exists_preserved"] if fixed_scene_preserved else []
        )
        + (
            ["role_aliases_are_operator_declared_not_runtime_verified"] if aliases else []
        ),
        "claim_boundary": (
            "Binding is setup evidence only. Phase 1 status, v-t-gap, obstacle GT, "
            "and comparison reports decide run evaluability."
        ),
    }
    out_dir = root / "analysis" / "phase1_scenario_binding"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "phase1_scenario_binding_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def _backend_type(manifest: Mapping[str, Any]) -> str | None:
    explicit = manifest.get("backend_type")
    if explicit:
        return str(explicit)
    backend = str(manifest.get("backend") or manifest.get("backend_name") or "")
    if backend == "apollo_cyberrt":
        return "apollo_reference_backend"
    if backend == "carla_builtin":
        return "planning_control_backend"
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}
