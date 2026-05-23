from __future__ import annotations

from typing import Any, Mapping

from .base import ScenarioSpec


FOLLOW_STOP_SPEC = ScenarioSpec(
    name="follow_stop",
    town="Town01",
    ego_spawn={"spawn_index": 208, "role_name": "hero"},
    actors={
        "front": {
            "spawn_index": 210,
            "role_name": "front",
            "behavior": "static_brake_hold",
        }
    },
    params={
        "stop_brake": 1.0,
        "auto_align_front_spawn": True,
        "front_min_ahead_m": 20.0,
        "front_max_ahead_m": 80.0,
    },
    metadata={
        "scenario_family": "baseline",
        "platform_role": "legacy_demo_baseline",
        "controller_owned_by": "runner_or_demo_entry",
        "notes": "Scenario intent only; follow-stop controller logic is not part of this spec.",
    },
)


def follow_stop_spec(overrides: Mapping[str, Any] | None = None) -> ScenarioSpec:
    if not overrides:
        return FOLLOW_STOP_SPEC
    params = dict(FOLLOW_STOP_SPEC.params)
    params.update(dict(overrides))
    return ScenarioSpec(
        name=FOLLOW_STOP_SPEC.name,
        town=FOLLOW_STOP_SPEC.town,
        ego_spawn=FOLLOW_STOP_SPEC.ego_spawn,
        actors=FOLLOW_STOP_SPEC.actors,
        duration_s=FOLLOW_STOP_SPEC.duration_s,
        max_ticks=FOLLOW_STOP_SPEC.max_ticks,
        params=params,
        metadata=FOLLOW_STOP_SPEC.metadata,
    )


def build_follow_stop_scenario(config: Mapping[str, Any] | None = None):
    """Build the legacy CARLA follow-stop scenario lazily.

    Importing this module is CARLA-free. Constructing the legacy scenario still
    requires the existing CARLA-backed implementation in ``followstop.py``.
    """
    from .followstop import FollowStopConfig, FollowStopScenario

    payload = dict(config or {})
    return FollowStopScenario(FollowStopConfig(**payload))
