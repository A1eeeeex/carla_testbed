"""CI-safe fixed scene playback primitives.

The scenario player controls scripted non-ego actors only. Ego control remains
owned by the configured backend, and random traffic remains owned by a traffic
flow provider.
"""

from __future__ import annotations

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime, CarlaFixedSceneRuntimeState
from carla_testbed.scenario_player.player import FixedSceneFrameContext, FixedScenePlayer
from carla_testbed.scenario_player.schema import (
    FIXED_SCENE_STORYBOARD_SCHEMA_VERSION,
    FIXED_SCENE_TEMPLATE_SCHEMA_VERSION,
    load_fixed_scene_storyboard,
    load_fixed_scene_template,
    validate_fixed_scene_storyboard,
    validate_fixed_scene_template,
)

__all__ = [
    "FIXED_SCENE_STORYBOARD_SCHEMA_VERSION",
    "FIXED_SCENE_TEMPLATE_SCHEMA_VERSION",
    "CarlaFixedSceneRuntime",
    "CarlaFixedSceneRuntimeState",
    "FixedSceneFrameContext",
    "FixedScenePlayer",
    "compile_fixed_scene_template",
    "load_fixed_scene_storyboard",
    "load_fixed_scene_template",
    "validate_fixed_scene_storyboard",
    "validate_fixed_scene_template",
]
