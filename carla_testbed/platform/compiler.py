from __future__ import annotations

import itertools
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from .evidence_resolver import resolve_evidence_for_plan
from .plan import (
    AlgorithmPlan,
    EvidencePlan,
    GatePlan,
    IdentityPlan,
    PlatformPlan,
    RecordingPlan,
    RunPlan,
    ScenarioPlan,
    TruthInputPlan,
    TrafficFlowPlan,
    WorldPlan,
)
from .registry import PlatformRegistry, RegistryEntry, default_platform_registry


class PlatformCompileError(ValueError):
    """Raised when a RunPlan cannot be compiled."""


def compile_run_plan(
    request: str | Path | Mapping[str, Any] | None = None,
    *,
    platform: str | Path | None = None,
    algorithm: str | Path | None = None,
    scenario: str | Path | None = None,
    recording: str | Path | None = None,
    traffic: str | Path | None = None,
    gate: str | Path | None = None,
    registry: PlatformRegistry | None = None,
) -> RunPlan:
    registry = registry or default_platform_registry()
    request_payload = _read_request(request)
    include = dict(request_payload.get("include") or {})
    run_request = dict(request_payload.get("run") or {})
    world_request = dict(request_payload.get("world") or request_payload.get("sim") or {})

    platform_entry = _resolve_profile(registry, "platform", platform or include.get("platform"))
    algorithm_entry = _resolve_profile(registry, "algorithm", algorithm or include.get("algorithm"))
    scenario_entry = _resolve_profile(registry, "scenario", scenario or include.get("scenario"))
    recording_entry = _resolve_profile(
        registry,
        "recording",
        recording or include.get("recording") or include.get("record") or "none",
    )
    traffic_entry = _resolve_profile(
        registry,
        "traffic",
        traffic or include.get("traffic") or include.get("traffic_flow") or "none",
    )
    gate_entry = _resolve_profile(registry, "gate", gate or include.get("gate") or "smoke")

    scenario_plan = _scenario_plan(scenario_entry.payload)
    platform_plan = _platform_plan(platform_entry.payload)
    algorithm_plan = _algorithm_plan(algorithm_entry.payload)
    recording_plan = _recording_plan(recording_entry.payload, platform_name=platform_plan.name)
    traffic_plan = _traffic_flow_plan(traffic_entry.payload)
    evidence_plan = _evidence_plan(gate_entry.payload)
    gate_plan = _gate_plan(gate_entry.payload)
    world_plan = _world_plan(
        run=run_request,
        world=world_request,
        scenario=scenario_plan,
        platform=platform_entry.payload,
    )
    identity = _identity_plan(
        run=run_request,
        scenario=scenario_plan,
        platform=platform_plan,
        algorithm=algorithm_plan,
        recording=recording_plan,
        traffic=traffic_plan,
        gate=gate_plan,
    )
    truth_input = _truth_input_plan(algorithm_entry.payload, scenario=scenario_plan)

    plan = RunPlan(
        identity=identity,
        world=world_plan,
        scenario=scenario_plan,
        platform=platform_plan,
        algorithm=algorithm_plan,
        truth_input=truth_input,
        recording=recording_plan,
        traffic_flow=traffic_plan,
        evidence=evidence_plan,
        gate=gate_plan,
        source_profiles={
            "request": str(Path(request).expanduser()) if isinstance(request, (str, Path)) else None,
            "platform": str(platform_entry.path),
            "algorithm": str(algorithm_entry.path),
            "scenario": str(scenario_entry.path),
            "recording": str(recording_entry.path),
            "traffic": str(traffic_entry.path),
            "gate": str(gate_entry.path),
        },
        compatibility={
            "runtime_dispatched": False,
            "legacy_entrypoints_preserved": True,
            "output_root": str(run_request.get("output_root") or "runs"),
            "notes": (
                "RunPlan compilation is offline. Runtime backends may wrap legacy "
                "tools/configs until they are migrated."
            ),
        },
    )
    return _resolve_plan_evidence(plan)


def compile_suite_matrix(
    suite: str | Path | Mapping[str, Any],
    *,
    registry: PlatformRegistry | None = None,
) -> list[RunPlan]:
    registry = registry or default_platform_registry()
    payload = _read_request(suite)
    matrix = payload.get("matrix")
    if not isinstance(matrix, Mapping):
        raise PlatformCompileError("suite matrix must be a mapping")
    scenarios = _as_list(matrix.get("scenarios"))
    platforms = _as_list(matrix.get("platforms"))
    recordings = _as_list(matrix.get("recording") or matrix.get("recordings") or "none")
    traffics = _as_list(matrix.get("traffic") or matrix.get("traffics") or "none")
    gates = _as_list(matrix.get("gate") or matrix.get("gates") or "smoke")
    if not scenarios or not platforms:
        raise PlatformCompileError("suite matrix requires scenarios and platforms")

    plans: list[RunPlan] = []
    suite_id = str(payload.get("suite_id") or payload.get("name") or "suite")
    for scenario, platform in itertools.product(scenarios, platforms):
        algorithm_values = _algorithms_for_platform(matrix.get("algorithms"), platform)
        for algorithm, recording, traffic, gate in itertools.product(algorithm_values, recordings, traffics, gates):
            request = {
                "run": {
                    "id": _run_id_from_parts(suite_id, platform, algorithm, scenario, traffic, recording, gate),
                    "seed": payload.get("seed", 1),
                    "max_ticks": payload.get("max_ticks"),
                    "output_root": payload.get("output_root", "runs"),
                },
                "include": {
                    "platform": platform,
                    "algorithm": algorithm,
                    "scenario": scenario,
                    "recording": recording,
                    "traffic": traffic,
                    "gate": gate,
                },
            }
            plan = compile_run_plan(request, registry=registry)
            plan = RunPlan.from_dict(
                {
                    **plan.to_dict(),
                    "identity": {
                        **plan.to_dict()["identity"],
                        "suite_id": suite_id,
                    },
                    "source_profiles": {
                        **plan.source_profiles,
                        "suite": str(Path(suite).expanduser()) if isinstance(suite, (str, Path)) else None,
                    },
                }
            )
            plans.append(plan)
    return plans


def write_run_plan(plan: RunPlan, path: str | Path) -> Path:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(plan.to_dict(), sort_keys=False), encoding="utf-8")
    return output_path


def plan_to_yaml(plan: RunPlan) -> str:
    return yaml.safe_dump(plan.to_dict(), sort_keys=False)


def _read_request(request: str | Path | Mapping[str, Any] | None) -> dict[str, Any]:
    if request is None:
        return {}
    if isinstance(request, Mapping):
        return dict(request)
    path = Path(request).expanduser()
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise PlatformCompileError(f"request file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise PlatformCompileError(f"failed to parse YAML in {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise PlatformCompileError(f"{path}: root must be a mapping")
    return dict(payload)


def _resolve_profile(
    registry: PlatformRegistry,
    kind: str,
    value: str | Path | None,
) -> RegistryEntry:
    if value in {None, ""}:
        raise PlatformCompileError(f"missing required {kind} profile")
    try:
        return registry.get(kind, str(value))
    except Exception as exc:
        raise PlatformCompileError(str(exc)) from exc


def _identity_plan(
    *,
    run: Mapping[str, Any],
    scenario: ScenarioPlan,
    platform: PlatformPlan,
    algorithm: AlgorithmPlan,
    recording: RecordingPlan,
    traffic: TrafficFlowPlan,
    gate: GatePlan,
) -> IdentityPlan:
    run_id = str(
        run.get("id")
        or _run_id_from_parts(
            platform.name,
            algorithm.variant_id,
            scenario.scenario_id,
            traffic.profile,
            recording.profile,
            gate.profile,
        )
    )
    return IdentityPlan(
        run_id=run_id,
        seed=int(run.get("seed", 1) or 1),
    )


def _world_plan(
    *,
    run: Mapping[str, Any],
    world: Mapping[str, Any],
    scenario: ScenarioPlan,
    platform: Mapping[str, Any],
) -> WorldPlan:
    return WorldPlan(
        simulator=str(world.get("simulator") or "carla"),
        town=str(world.get("town") or scenario.map or "Town01"),
        fixed_dt_s=float(world.get("fixed_dt_s") or run.get("fixed_dt_s") or 0.05),
        synchronous=bool(world.get("synchronous", True)),
        host=str(world.get("host") or platform.get("host") or "localhost"),
        port=int(world.get("port") or platform.get("port") or 2000),
        max_ticks=int(run["max_ticks"]) if run.get("max_ticks") is not None else None,
        timeout_s=float(world["timeout_s"]) if world.get("timeout_s") is not None else None,
    )


def _scenario_plan(payload: Mapping[str, Any]) -> ScenarioPlan:
    route = payload.get("route") if isinstance(payload.get("route"), Mapping) else {}
    fixed_scene = payload.get("fixed_scene") if isinstance(payload.get("fixed_scene"), Mapping) else {}
    return ScenarioPlan(
        scenario_id=str(payload.get("scenario_id") or payload.get("name") or "unknown"),
        scenario_class=str(payload.get("scenario_class") or payload.get("class") or "unknown"),
        map=str(payload.get("map") or payload.get("town") or "Town01"),
        route_id=_str_or_none(payload.get("route_id") or route.get("route_id")),
        spawn_ref=_str_or_none(route.get("spawn_ref") or payload.get("spawn_ref")),
        goal_ref=_str_or_none(route.get("goal_ref") or payload.get("goal_ref")),
        route_ref=_str_or_none(route.get("route_ref") or payload.get("route_ref")),
        actors=dict(payload.get("actors") or {}),
        requirements=dict(payload.get("requirements") or {}),
        success_intent=dict(payload.get("success_intent") or payload.get("success_criteria") or {}),
        fixed_scene=dict(fixed_scene),
        tags=[str(item) for item in payload.get("tags") or []],
    )


def _platform_plan(payload: Mapping[str, Any]) -> PlatformPlan:
    return PlatformPlan(
        name=str(payload.get("name") or "unknown"),
        adapter=str(payload.get("adapter") or payload.get("name") or "unknown"),
        runtime_mode=_str_or_none(payload.get("runtime_mode")),
        middleware=_str_or_none(payload.get("middleware")),
        launch_profile=_str_or_none(payload.get("launch_profile")),
        expected_outputs=[str(item) for item in payload.get("expected_outputs") or []],
        supported_recorders=[str(item) for item in payload.get("supported_recorders") or []],
        params=dict(payload.get("params") or {}),
    )


def _algorithm_plan(payload: Mapping[str, Any]) -> AlgorithmPlan:
    return AlgorithmPlan(
        stack=str(payload.get("stack") or payload.get("name") or "unknown"),
        variant_id=str(payload.get("variant_id") or payload.get("name") or "unknown"),
        modules=dict(payload.get("modules") or {}),
        forbidden_assists=[str(item) for item in payload.get("forbidden_assists") or []],
        claim_boundary=dict(payload.get("claim_boundary") or {}),
        params=dict(payload.get("params") or {}),
    )


def _truth_input_plan(payload: Mapping[str, Any], *, scenario: ScenarioPlan) -> TruthInputPlan:
    truth = payload.get("truth_input") if isinstance(payload.get("truth_input"), Mapping) else {}
    modules = payload.get("modules") if isinstance(payload.get("modules"), Mapping) else {}
    return TruthInputPlan(
        localization=_truth_value(truth, modules, "localization"),
        chassis=_truth_value(truth, modules, "chassis"),
        obstacles=_truth_value(truth, modules, "obstacles", "perception_obstacles"),
        traffic_light=_truth_value(truth, modules, "traffic_light"),
        prediction=_truth_value(truth, modules, "prediction"),
        claim_boundary=str(_claim_label(payload, scenario)),
        replacements=dict(truth.get("replacements") or {}),
    )


def _recording_plan(payload: Mapping[str, Any], *, platform_name: str) -> RecordingPlan:
    base_recorders = [str(item) for item in payload.get("recorders") or []]
    overrides = dict(payload.get("platform_overrides") or {})
    platform_override = overrides.get(platform_name)
    if isinstance(platform_override, Mapping):
        for item in platform_override.get("add") or []:
            if str(item) not in base_recorders:
                base_recorders.append(str(item))
    return RecordingPlan(
        profile=str(payload.get("name") or payload.get("profile") or "none"),
        recorders=base_recorders,
        neutral=dict(payload.get("neutral") or {}),
        platform_overrides=overrides,
        claim_boundary=dict(payload.get("claim_boundary") or {}),
    )


def _traffic_flow_plan(payload: Mapping[str, Any]) -> TrafficFlowPlan:
    traffic = payload.get("traffic_flow") if isinstance(payload.get("traffic_flow"), Mapping) else {}
    return TrafficFlowPlan(
        profile=str(payload.get("name") or payload.get("profile") or "none"),
        enabled=bool(traffic.get("enabled", False)),
        provider=str(traffic.get("provider") or "none"),
        seed=int(traffic["seed"]) if traffic.get("seed") is not None else None,
        traffic_manager=dict(traffic.get("traffic_manager") or {}),
        vehicles=dict(traffic.get("vehicles") or {}),
        walkers=dict(traffic.get("walkers") or {}),
        lifecycle=dict(traffic.get("lifecycle") or {}),
        raw=dict(traffic),
    )


def _evidence_plan(payload: Mapping[str, Any]) -> EvidencePlan:
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), Mapping) else payload
    return EvidencePlan(
        profile=str(payload.get("name") or payload.get("profile") or "smoke"),
        required_analyzers=[str(item) for item in evidence.get("required_analyzers") or []],
        optional_analyzers=[str(item) for item in evidence.get("optional_analyzers") or []],
        not_applicable_analyzers=[str(item) for item in evidence.get("not_applicable_analyzers") or []],
        expected_artifacts=[str(item) for item in evidence.get("expected_artifacts") or []],
    )


def _gate_plan(payload: Mapping[str, Any]) -> GatePlan:
    gate = payload.get("gate") if isinstance(payload.get("gate"), Mapping) else payload
    return GatePlan(
        profile=str(payload.get("name") or payload.get("profile") or "smoke"),
        can_claim_natural_driving=bool(gate.get("can_claim_natural_driving", False)),
        claim_requires=[str(item) for item in gate.get("claim_requires") or []],
        fail_on_status=[str(item) for item in gate.get("fail_on_status") or []],
        rules=[dict(item) for item in gate.get("rules") or [] if isinstance(item, Mapping)],
    )


def _resolve_plan_evidence(plan: RunPlan) -> RunPlan:
    resolution = resolve_evidence_for_plan(plan)
    payload = plan.to_dict()
    payload["evidence"] = {
        **payload.get("evidence", {}),
        "required_analyzers": resolution.required_analyzers,
        "optional_analyzers": resolution.optional_analyzers,
        "not_applicable_analyzers": resolution.not_applicable_analyzers,
    }
    payload["gate"] = {
        **payload.get("gate", {}),
        "rules": resolution.rules,
    }
    return RunPlan.from_dict(payload)


def _truth_value(
    truth: Mapping[str, Any],
    modules: Mapping[str, Any],
    *names: str,
) -> str | None:
    for name in names:
        value = truth.get(name)
        if value is not None:
            return str(value)
        module = modules.get(name)
        if isinstance(module, Mapping):
            source = module.get("source")
            if source is not None:
                return str(source)
        elif module is not None:
            return str(module)
    return None


def _claim_label(payload: Mapping[str, Any], scenario: ScenarioPlan) -> str:
    boundary = payload.get("claim_boundary")
    if isinstance(boundary, Mapping) and boundary.get("label"):
        return str(boundary["label"])
    if scenario.requirements.get("traffic_light_required"):
        return "Truth-input stack behavior with explicit traffic-light evidence required"
    return "Truth-input stack behavior; not full sensor/perception reproduction"


def _algorithms_for_platform(algorithms: Any, platform: str) -> list[str]:
    if isinstance(algorithms, Mapping):
        values = algorithms.get(platform) or algorithms.get(Path(str(platform)).stem) or algorithms.get("default")
    else:
        values = algorithms
    result = _as_list(values)
    if not result:
        raise PlatformCompileError(f"no algorithms configured for platform {platform}")
    return result


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _str_or_none(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _run_id_from_parts(*parts: Any) -> str:
    text = "__".join(str(part) for part in parts if part not in {None, ""})
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text)
    return safe.strip("_") or "run"
