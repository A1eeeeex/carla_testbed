from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.phase1_status import PHASE1_STATUS_SCHEMA_VERSION
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
from carla_testbed.experiments.phase1_apollo_fixed_scene_dispatch import (
    write_apollo_fixed_scene_dispatch_for_plan,
)
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.manifest_contract import fixed_scene_manifest_fields_from_template_path
from carla_testbed.scenario_player.schema import load_fixed_scene_template


APOLLO_FIXED_SCENE_PREFLIGHT_SCHEMA_VERSION = "apollo_fixed_scene_preflight.v1"


def write_offline_fixed_scene_artifacts(
    plan: Any,
    run_dir: str | Path,
    *,
    repo_root: str | Path,
    launch_plan: Mapping[str, Any] | None = None,
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
    apollo_dispatch = None
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
        if launch_plan is not None:
            apollo_dispatch = write_apollo_fixed_scene_dispatch_for_plan(
                plan,
                launch_plan,
                root,
                target_actor_contract=target_contract,
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
        **(
            {
                "apollo_fixed_scene_dispatch": {
                    "status": apollo_dispatch.get("status"),
                    "paths": apollo_dispatch.get("paths"),
                }
            }
            if apollo_dispatch
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


def phase1_preflight_status(
    *,
    backend: str,
    fixed_scene_enabled: bool,
    backend_preflight: Mapping[str, Any],
    launch_plan: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if backend == "apollo_cyberrt" and fixed_scene_enabled and not bool(launch_plan.get("starts_runtime")):
        reasons.append("apollo_fixed_scene_runtime_not_migrated")
    if backend_preflight.get("status") not in {"ready", "pass"}:
        reasons.extend(str(item) for item in backend_preflight.get("missing_requirements") or [])
    if not launch_plan.get("starts_runtime"):
        reasons.append("phase1_scaffold_does_not_start_runtime")
    reasons = sorted(set(item for item in reasons if item))
    return ("ready", []) if not reasons else ("backend_not_ready", reasons)


def build_phase1_scaffold_manifest(
    *,
    plan: Any,
    contract: Mapping[str, Any],
    launch_plan: Mapping[str, Any],
    run_dir: str | Path,
    preflight_status: str,
    start_time_wall_s: float,
    bridge_config_path: str | None = None,
) -> dict[str, Any]:
    fixed_scene_fields = fixed_scene_manifest_fields_from_template_path(plan.source_profiles.get("scenario"))
    return {
        "schema_version": "phase1_scenario_run_manifest.v1",
        "run_id": plan.identity.run_id,
        "scenario_id": plan.scenario.scenario_id,
        "scenario_class": plan.scenario.scenario_class,
        "scenario_case": plan.scenario.scenario_id,
        "map": plan.scenario.map,
        "backend": contract.get("backend"),
        "backend_name": contract.get("backend_name") or contract.get("backend"),
        "backend_type": contract.get("backend_type"),
        "backend_ready": preflight_status == "ready",
        "starts_runtime": False,
        "starts_carla": contract.get("starts_carla"),
        "starts_apollo": contract.get("starts_apollo"),
        "starts_autoware": contract.get("starts_autoware"),
        "input_contract": contract.get("input_contract"),
        "adapter_path": contract.get("adapter_path"),
        "available_truth_fields": contract.get("available_truth_fields") or [],
        "output_control_mode": contract.get("output_control_mode"),
        "transport_mode": contract.get("transport_mode"),
        "bridge_config_path": bridge_config_path,
        "fixed_scene_enabled": bool(plan.scenario.fixed_scene),
        **fixed_scene_fields,
        "expected_artifacts": launch_plan.get("expected_artifacts") or [],
        "launch_plan": launch_plan,
        "source_profiles": dict(plan.source_profiles),
        "start_time_wall_s": start_time_wall_s,
        "run_dir": str(run_dir),
        "claim_boundary": "phase1_preflight_manifest_not_behavior_evidence",
    }


def build_phase1_scaffold_status(
    plan: Any,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    run_dir: str | Path,
    reasons: list[str],
) -> dict[str, Any]:
    root = Path(run_dir)
    return {
        "schema_version": PHASE1_STATUS_SCHEMA_VERSION,
        "run_dir": str(root),
        "run_id": plan.identity.run_id,
        "scenario_id": plan.scenario.scenario_id,
        "scenario_case": plan.scenario.scenario_id,
        "backend": manifest.get("backend"),
        "backend_name": manifest.get("backend_name"),
        "backend_type": manifest.get("backend_type"),
        "status": "invalid",
        "failure_reason": "backend_not_ready",
        "invalid_reasons": ["backend_not_ready"],
        "degraded_reasons": [],
        "failed_reasons": [],
        "original_failure_reason": summary.get("fail_reason"),
        "evaluable": False,
        "target_actor_contract": manifest.get("target_actor_contract"),
        "artifact_contract_version": manifest.get("artifact_contract_version"),
        "evidence_files": [
            str(root / "manifest.json"),
            str(root / "preflight.json"),
            str(root / "summary.json"),
        ],
        "missing_artifacts": [],
        "expected_artifacts": manifest.get("expected_artifacts") or [],
        "missing_expected_artifacts": missing_expected_artifacts(
            root, manifest.get("expected_artifacts") or [], assume_scaffold_written=True
        ),
        "required_artifacts": {
            "manifest": "present",
            "summary": "present",
            "preflight": "present",
            "timeseries": "missing",
            "v_t_gap_report": "missing",
        },
        "preflight_status": "backend_not_ready",
        "preflight_reasons": reasons,
        "offline_fixed_scene_artifacts": existing_offline_fixed_scene_artifacts(root),
        "v_t_gap_status": None,
        "summary_status": summary.get("status"),
        "summary_success": summary.get("success"),
        "notes": [
            "invalid_run_is_setup_or_evidence_failure_not_backend_loss",
            "apollo_fixed_scene_scaffold_does_not_execute_runtime",
        ],
        "claim_boundary": "phase1_status_is_scenario_evaluation_not_natural_driving_claim",
    }


def write_phase1_scaffold_events(path: str | Path, plan: Any, status: str, reasons: list[str]) -> None:
    rows = [
        {
            "event_type": "phase1_preflight_start",
            "run_id": plan.identity.run_id,
            "backend": plan.platform.name,
            "scenario_id": plan.scenario.scenario_id,
        },
        {
            "event_type": "phase1_preflight_end",
            "run_id": plan.identity.run_id,
            "status": status,
            "reasons": reasons,
        },
    ]
    Path(path).write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def write_json(path: str | Path, payload: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def missing_expected_artifacts(
    run_dir: str | Path,
    expected_artifacts: list[str],
    *,
    assume_scaffold_written: bool = False,
) -> list[str]:
    root = Path(run_dir)
    scaffold = {"manifest.json", "summary.json", "events.jsonl", "config.resolved.yaml", "run_plan.resolved.yaml"}
    missing: list[str] = []
    for rel in expected_artifacts:
        if not rel:
            continue
        if assume_scaffold_written and rel in scaffold:
            continue
        if not (root / rel).exists():
            missing.append(rel)
    return missing
