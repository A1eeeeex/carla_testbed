from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Mapping

RUN_PLAN_SCHEMA_VERSION = "run_plan.v1"


@dataclass(frozen=True)
class IdentityPlan:
    run_id: str
    suite_id: str | None = None
    seed: int = 1
    created_by: str = "carla_testbed.platform.compiler"


@dataclass(frozen=True)
class WorldPlan:
    simulator: str = "carla"
    town: str = "Town01"
    fixed_dt_s: float = 0.05
    synchronous: bool = True
    host: str = "localhost"
    port: int = 2000
    max_ticks: int | None = None
    timeout_s: float | None = None


@dataclass(frozen=True)
class ScenarioPlan:
    scenario_id: str
    scenario_class: str
    map: str = "Town01"
    route_id: str | None = None
    spawn_ref: str | None = None
    goal_ref: str | None = None
    route_ref: str | None = None
    actors: dict[str, Any] = field(default_factory=dict)
    requirements: dict[str, Any] = field(default_factory=dict)
    success_intent: dict[str, Any] = field(default_factory=dict)
    fixed_scene: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PlatformPlan:
    name: str
    adapter: str
    runtime_mode: str | None = None
    middleware: str | None = None
    launch_profile: str | None = None
    expected_outputs: list[str] = field(default_factory=list)
    supported_recorders: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AlgorithmPlan:
    stack: str
    variant_id: str
    modules: dict[str, Any] = field(default_factory=dict)
    forbidden_assists: list[str] = field(default_factory=list)
    claim_boundary: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TruthInputPlan:
    localization: str | None = None
    chassis: str | None = None
    obstacles: str | None = None
    traffic_light: str | None = None
    prediction: str | None = None
    claim_boundary: str | None = None
    replacements: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecordingPlan:
    profile: str
    recorders: list[str] = field(default_factory=list)
    neutral: dict[str, Any] = field(default_factory=dict)
    platform_overrides: dict[str, Any] = field(default_factory=dict)
    claim_boundary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TrafficFlowPlan:
    profile: str = "none"
    enabled: bool = False
    provider: str = "none"
    seed: int | None = None
    traffic_manager: dict[str, Any] = field(default_factory=dict)
    vehicles: dict[str, Any] = field(default_factory=dict)
    walkers: dict[str, Any] = field(default_factory=dict)
    lifecycle: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidencePlan:
    profile: str
    required_analyzers: list[str] = field(default_factory=list)
    optional_analyzers: list[str] = field(default_factory=list)
    not_applicable_analyzers: list[str] = field(default_factory=list)
    expected_artifacts: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GatePlan:
    profile: str
    can_claim_natural_driving: bool = False
    claim_requires: list[str] = field(default_factory=list)
    fail_on_status: list[str] = field(default_factory=list)
    rules: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class RunPlan:
    schema_version: str = RUN_PLAN_SCHEMA_VERSION
    identity: IdentityPlan = field(default_factory=lambda: IdentityPlan(run_id="default"))
    world: WorldPlan = field(default_factory=WorldPlan)
    scenario: ScenarioPlan = field(
        default_factory=lambda: ScenarioPlan(scenario_id="unknown", scenario_class="unknown")
    )
    platform: PlatformPlan = field(
        default_factory=lambda: PlatformPlan(name="dummy", adapter="dummy")
    )
    algorithm: AlgorithmPlan = field(
        default_factory=lambda: AlgorithmPlan(stack="dummy", variant_id="dummy")
    )
    truth_input: TruthInputPlan = field(default_factory=TruthInputPlan)
    recording: RecordingPlan = field(default_factory=lambda: RecordingPlan(profile="none"))
    traffic_flow: TrafficFlowPlan = field(default_factory=TrafficFlowPlan)
    evidence: EvidencePlan = field(default_factory=lambda: EvidencePlan(profile="smoke"))
    gate: GatePlan = field(default_factory=lambda: GatePlan(profile="smoke"))
    source_profiles: dict[str, str | None] = field(default_factory=dict)
    compatibility: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _drop_none(dataclasses.asdict(self))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "RunPlan":
        return cls(
            schema_version=str(payload.get("schema_version") or RUN_PLAN_SCHEMA_VERSION),
            identity=IdentityPlan(**dict(payload.get("identity") or {})),
            world=WorldPlan(**dict(payload.get("world") or {})),
            scenario=ScenarioPlan(**dict(payload.get("scenario") or {})),
            platform=PlatformPlan(**dict(payload.get("platform") or {})),
            algorithm=AlgorithmPlan(**dict(payload.get("algorithm") or {})),
            truth_input=TruthInputPlan(**dict(payload.get("truth_input") or {})),
            recording=RecordingPlan(**dict(payload.get("recording") or {})),
            traffic_flow=TrafficFlowPlan(**dict(payload.get("traffic_flow") or {})),
            evidence=EvidencePlan(**dict(payload.get("evidence") or {})),
            gate=GatePlan(**dict(payload.get("gate") or {})),
            source_profiles=dict(payload.get("source_profiles") or {}),
            compatibility=dict(payload.get("compatibility") or {}),
        )


def _drop_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _drop_none(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_drop_none(item) for item in value]
    return value
