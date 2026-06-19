from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


PHASE1_ARTIFACT_CONTRACT_VERSION = "phase1_scenario_run_artifacts.v1"


def fixed_scene_manifest_fields_from_storyboard(storyboard: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return Phase 1 manifest fields that identify the fixed-scene target contract."""

    if not isinstance(storyboard, Mapping):
        return {
            "fixed_scene_case": None,
            "target_actor_contract": None,
            "artifact_contract_version": PHASE1_ARTIFACT_CONTRACT_VERSION,
        }
    target_contract = storyboard.get("target_actor_contract")
    return {
        "fixed_scene_case": storyboard.get("scene_id"),
        "target_actor_contract": dict(target_contract) if isinstance(target_contract, Mapping) else None,
        "artifact_contract_version": PHASE1_ARTIFACT_CONTRACT_VERSION,
    }


def fixed_scene_manifest_fields_from_template_path(path: str | Path | None) -> dict[str, Any]:
    """Compile a fixed-scene template only far enough to expose target actor evidence."""

    if not path:
        return fixed_scene_manifest_fields_from_storyboard(None)
    source = Path(path).expanduser()
    if not source.exists():
        return fixed_scene_manifest_fields_from_storyboard(None)
    try:
        storyboard = compile_fixed_scene_template(load_fixed_scene_template(source))
    except ValueError:
        return fixed_scene_manifest_fields_from_storyboard(None)
    return fixed_scene_manifest_fields_from_storyboard(storyboard)
