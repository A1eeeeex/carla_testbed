from __future__ import annotations

from carla_testbed.platform.registry import PlatformRegistry
from carla_testbed.traffic.registry import default_traffic_flow_registry
from carla_testbed.traffic.schema import load_traffic_flow_profile


def test_random_walkers_profiles_load() -> None:
    profile = load_traffic_flow_profile("configs/traffic/town01/random_walkers2.yaml")

    assert profile["traffic_flow"]["provider"] == "carla_walker_ai_controller"
    assert profile["traffic_flow"]["seed"] == 52
    assert profile["traffic_flow"]["walkers"]["count"] == 2


def test_random_mixed_profile_loads() -> None:
    profile = load_traffic_flow_profile("configs/traffic/town01/random_tm2_walkers2.yaml")

    assert profile["traffic_flow"]["provider"] == "mixed_carla_flow"
    assert profile["traffic_flow"]["vehicles"]["count"] == 2
    assert profile["traffic_flow"]["walkers"]["count"] == 2


def test_platform_registry_lists_walker_profiles() -> None:
    registry = PlatformRegistry(repo_root=".")

    names = registry.names("traffic")

    assert "town01_random_walkers1" in names
    assert "town01_random_walkers2" in names
    assert "town01_random_tm2_walkers2" in names
    assert registry.get("traffic", "town01/random_walkers2").name == "town01_random_walkers2"


def test_traffic_provider_registry_lists_walkers_and_mixed() -> None:
    registry = default_traffic_flow_registry()

    assert "carla_walker_ai_controller" in registry.names()
    assert "mixed_carla_flow" in registry.names()
