from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from carla_testbed.analysis.fixed_scene_contract import (
    analyze_fixed_scene_contract_run_dir,
    write_fixed_scene_contract_report,
)
from carla_testbed.analysis.scenario_actor_contract import (
    analyze_scenario_actor_contract_run_dir,
    write_scenario_actor_contract_report,
)
from carla_testbed.experiments.phase1_apollo_fixed_scene_readiness import (
    write_apollo_fixed_scene_readiness_for_plan,
)
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def write_offline_fixed_scene_artifacts(
    plan: Any,
    run_dir: str | Path,
    *,
    repo_root: str | Path,
    bridge_config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Write static fixed-scene scaffold artifacts without faking runtime evidence."""

    root = Path(run_dir).expanduser()
    scenario_path = plan.source_profiles.get("scenario")
    if not scenario_path:
        return {}
    source = Path(scenario_path).expanduser()
    if not source.is_absolute():
        source = Path(repo_root).expanduser() / source
    try:
        storyboard = compile_fixed_scene_template(load_fixed_scene_template(source))
    except Exception as exc:  # noqa: BLE001 - report scaffold artifact failure without aborting preflight
        return {
            "status": "failed",
            "source": str(source),
            "error": f"{type(exc).__name__}: {exc}",
            "claim_boundary": "offline fixed-scene compile failure; no runtime behavior executed",
        }
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    storyboard_path = artifacts / "fixed_scene_resolved.json"
    storyboard_path.write_text(json.dumps(storyboard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    analysis = root / "analysis"
    fixed_report = analyze_fixed_scene_contract_run_dir(root)
    actor_report = analyze_scenario_actor_contract_run_dir(root)
    fixed_paths = write_fixed_scene_contract_report(fixed_report, analysis / "fixed_scene_contract")
    actor_paths = write_scenario_actor_contract_report(actor_report, analysis / "scenario_actor_contract")
    apollo_readiness = None
    if getattr(getattr(plan, "platform", None), "name", None) == "apollo_cyberrt":
        target_contract = (
            storyboard.get("target_actor_contract") if isinstance(storyboard.get("target_actor_contract"), dict) else None
        )
        apollo_readiness = write_apollo_fixed_scene_readiness_for_plan(
            plan,
            root,
            repo_root=repo_root,
            target_actor_contract=target_contract,
            bridge_config_path=bridge_config_path,
        )
    return {
        "status": "static_only",
        "fixed_scene_resolved": str(storyboard_path),
        "fixed_scene_contract": fixed_paths,
        "scenario_actor_contract": actor_paths,
        **(
            {
                "apollo_fixed_scene_readiness": {
                    "status": apollo_readiness.get("status"),
                    "paths": apollo_readiness.get("paths"),
                }
            }
            if apollo_readiness
            else {}
        ),
        "fixed_scene_contract_status": fixed_report.get("status"),
        "scenario_actor_contract_status": actor_report.get("status"),
        "claim_boundary": (
            "These are offline fixed-scene compile/contract artifacts only. "
            "They do not prove runtime actor playback or backend behavior."
        ),
    }


def existing_offline_fixed_scene_artifacts(run_dir: str | Path) -> dict[str, str]:
    root = Path(run_dir).expanduser()
    paths = {
        "fixed_scene_resolved": root / "artifacts" / "fixed_scene_resolved.json",
        "fixed_scene_contract": root / "analysis" / "fixed_scene_contract" / "fixed_scene_contract_report.json",
        "scenario_actor_contract": root
        / "analysis"
        / "scenario_actor_contract"
        / "scenario_actor_contract_report.json",
    }
    return {name: str(path) for name, path in paths.items() if path.exists()}
