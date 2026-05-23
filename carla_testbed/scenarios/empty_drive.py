from __future__ import annotations

from typing import Any, Mapping

from .base import ScenarioSpec, ScenarioState


EMPTY_DRIVE_SPEC = ScenarioSpec(
    name="empty_drive",
    town="Town01",
    ego_spawn={"role_name": "hero"},
    actors={},
    metadata={
        "scenario_family": "smoke",
        "platform_role": "baseline_empty_world_drive",
        "notes": "Minimal single-ego scenario contract for harness smoke runs.",
    },
)


class EmptyDriveScenarioRunner:
    def __init__(self, config: Mapping[str, Any] | None = None):
        self.config = dict(config or {})
        self.state = ScenarioState(status="created")

    def setup(self, world: Any, context: Any, config: Any) -> ScenarioState:
        self.state.status = "setup"
        self.state.metadata["config"] = dict(self.config)
        return self.state

    def tick(self, frame_context: Any) -> ScenarioState:
        self.state.status = "running"
        return self.state

    def teardown(self) -> None:
        self.state.status = "teardown_complete"


def build_empty_drive_scenario(config: Mapping[str, Any] | None = None) -> EmptyDriveScenarioRunner:
    return EmptyDriveScenarioRunner(config)
