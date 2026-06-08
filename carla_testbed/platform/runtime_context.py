from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.platform.compiler import write_run_plan
from carla_testbed.platform.plan import RunPlan


@dataclass(frozen=True)
class RuntimeContext:
    plan: RunPlan
    run_dir: Path
    dry_run: bool = True
    legacy_dispatch: bool = False

    @classmethod
    def from_plan(
        cls,
        plan: RunPlan,
        *,
        run_dir: str | Path | None = None,
        dry_run: bool = True,
        legacy_dispatch: bool = False,
    ) -> "RuntimeContext":
        output_root = Path(plan.compatibility.get("output_root") or "runs")
        resolved_run_dir = Path(run_dir).expanduser() if run_dir is not None else output_root / plan.identity.run_id
        return cls(
            plan=plan,
            run_dir=resolved_run_dir,
            dry_run=dry_run,
            legacy_dispatch=legacy_dispatch,
        )


def write_runtime_context_artifacts(
    context: RuntimeContext,
    *,
    launch_plan: Mapping[str, Any],
    status: str,
    compatibility_source: str | None,
    summary: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    run_dir = context.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    plan_path = write_run_plan(context.plan, run_dir / "plan.resolved.yaml")
    launch_path = run_dir / "launch_plan.json"
    launch_path.write_text(json.dumps(dict(launch_plan), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest_path = run_dir / "manifest.json"
    traffic_flow = _traffic_flow_manifest_payload(context.plan, spawned_count=None)
    manifest = {
        "schema_version": "run_manifest.platform.v1",
        "run_id": context.plan.identity.run_id,
        "suite_id": context.plan.identity.suite_id,
        "scenario_id": context.plan.scenario.scenario_id,
        "scenario_class": context.plan.scenario.scenario_class,
        "route_id": context.plan.scenario.route_ref,
        "map": context.plan.scenario.map,
        "backend": context.plan.platform.name,
        "platform": context.plan.to_dict().get("platform"),
        "algorithm_variant_id": context.plan.algorithm.variant_id,
        "recording_profile": context.plan.recording.profile,
        "traffic_flow": traffic_flow,
        "gate_profile": context.plan.gate.profile,
        "ego_control_source": _ego_control_source(context.plan),
        "scenario_actor_control_source": "scripted_template" if _has_scripted_scenario_actors(context.plan) else "none",
        "background_traffic_control_source": (
            _background_vehicle_control_source(context.plan)
        ),
        "background_walker_control_source": _background_walker_control_source(context.plan),
        "compatibility_source": compatibility_source,
        "runtime_dispatch_status": status,
        "dry_run": context.dry_run,
        "legacy_dispatch": context.legacy_dispatch,
        "plan_path": str(plan_path),
        "launch_plan_path": str(launch_path),
        "created_wall_time_s": time.time(),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path = run_dir / "summary.json"
    summary_payload = {
        "schema_version": "run_summary.platform.v1",
        "run_id": context.plan.identity.run_id,
        "scenario_id": context.plan.scenario.scenario_id,
        "scenario_class": context.plan.scenario.scenario_class,
        "route_id": context.plan.scenario.route_ref,
        "backend": context.plan.platform.name,
        "traffic_flow": traffic_flow,
        "success": status in {"dry_run", "completed"},
        "exit_reason": f"platform_{status}",
        "runtime_dispatched": status == "completed",
        "dry_run": context.dry_run,
        "legacy_dispatch": context.legacy_dispatch,
        **dict(summary or {}),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "plan": str(plan_path),
        "launch_plan": str(launch_path),
        "manifest": str(manifest_path),
        "summary": str(summary_path),
    }


def _ego_control_source(plan: RunPlan) -> str:
    if plan.algorithm.stack == "apollo":
        return "apollo"
    if plan.algorithm.stack == "autoware":
        return "autoware"
    if plan.platform.name == "carla_builtin" or plan.algorithm.stack == "builtin":
        return "carla_builtin"
    return plan.platform.name


def _traffic_flow_manifest_payload(plan: RunPlan, *, spawned_count: int | None) -> dict[str, Any]:
    payload = dict(plan.to_dict().get("traffic_flow") or {})
    vehicles = payload.get("vehicles") if isinstance(payload.get("vehicles"), Mapping) else {}
    walkers = payload.get("walkers") if isinstance(payload.get("walkers"), Mapping) else {}
    payload["requested_count"] = int(vehicles.get("count", 0) or 0) if payload.get("enabled") else 0
    payload["requested_walker_count"] = int(walkers.get("count", 0) or 0) if payload.get("enabled") else 0
    payload["spawned_count"] = spawned_count
    return payload


def _background_vehicle_control_source(plan: RunPlan) -> str:
    if not plan.traffic_flow.enabled:
        return "none"
    vehicles = plan.traffic_flow.vehicles or {}
    if bool(vehicles.get("enabled", plan.traffic_flow.provider in {"carla_traffic_manager", "mixed_carla_flow"})):
        if plan.traffic_flow.provider in {"carla_traffic_manager", "mixed_carla_flow"}:
            return "carla_traffic_manager"
    return "none"


def _background_walker_control_source(plan: RunPlan) -> str:
    if not plan.traffic_flow.enabled:
        return "none"
    walkers = plan.traffic_flow.walkers or {}
    if bool(walkers.get("enabled", plan.traffic_flow.provider in {"carla_walker_ai_controller", "mixed_carla_flow"})):
        if plan.traffic_flow.provider in {"carla_walker_ai_controller", "mixed_carla_flow"}:
            return "carla_walker_ai_controller"
    return "none"


def _has_scripted_scenario_actors(plan: RunPlan) -> bool:
    for key, value in plan.scenario.actors.items():
        if str(key) in {"ego", "traffic_lights"}:
            continue
        if isinstance(value, list) and not value:
            continue
        if isinstance(value, Mapping) and not value:
            continue
        if value:
            return True
    return False
