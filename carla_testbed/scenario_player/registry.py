from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Any

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template


@dataclass(frozen=True)
class FixedSceneTemplateRegistry:
    compilers: Mapping[str, Callable[[Mapping[str, Any]], dict[str, Any]]]

    def names(self) -> list[str]:
        return sorted(self.compilers)

    def compile(self, template: Mapping[str, Any]) -> dict[str, Any]:
        name = str(template.get("template"))
        compiler = self.compilers.get(name)
        if compiler is None:
            raise KeyError(f"unknown fixed scene template: {name}")
        return compiler(template)


def default_fixed_scene_template_registry() -> FixedSceneTemplateRegistry:
    return FixedSceneTemplateRegistry(
        compilers={
            "follow_stop": compile_fixed_scene_template,
            "lead_vehicle_accel_decel": compile_fixed_scene_template,
            "cut_in": compile_fixed_scene_template,
            "cut_out": compile_fixed_scene_template,
        }
    )
