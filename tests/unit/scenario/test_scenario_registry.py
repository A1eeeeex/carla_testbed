from __future__ import annotations

from carla_testbed.scenarios import ScenarioSpec, get_scenario_builder, get_scenario_entry
from carla_testbed.scenarios.empty_drive import EmptyDriveScenarioRunner
from carla_testbed.scenarios.registry import ScenarioEntry, ScenarioRegistry


def test_default_registry_returns_follow_stop_spec_without_carla() -> None:
    entry = get_scenario_entry("follow_stop")

    assert entry.name == "follow_stop"
    assert entry.spec.name == "follow_stop"
    assert entry.spec.town == "Town01"
    assert entry.spec.metadata["scenario_family"] == "baseline"
    assert entry.spec.metadata["platform_role"] == "legacy_demo_baseline"
    assert callable(get_scenario_builder("follow-stop"))


def test_default_registry_supports_empty_drive_alias() -> None:
    entry = get_scenario_entry("smoke_empty")
    runner = entry.builder({"example": True})

    assert entry.name == "empty_drive"
    assert isinstance(runner, EmptyDriveScenarioRunner)
    assert runner.setup(world=None, context={}, config={}).status == "setup"
    assert runner.tick(frame_context={}).status == "running"
    runner.teardown()
    assert runner.state.status == "teardown_complete"


def test_custom_registry_can_register_fake_builder() -> None:
    calls: list[dict] = []

    def fake_builder(config=None):
        calls.append(dict(config or {}))
        return {"built": True}

    registry = ScenarioRegistry()
    registry.register(
        ScenarioEntry(
            name="fake_scene",
            spec=ScenarioSpec(name="fake_scene", town="Town01"),
            builder=fake_builder,
            aliases=("fake-scene",),
        )
    )

    assert registry.get("fake-scene").spec.name == "fake_scene"
    assert registry.build("fake_scene", {"seed": 7}) == {"built": True}
    assert calls == [{"seed": 7}]
