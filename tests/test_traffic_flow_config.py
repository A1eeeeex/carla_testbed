from __future__ import annotations

from carla_testbed.platform.registry import PlatformRegistry
from carla_testbed.traffic.registry import default_traffic_flow_registry
from carla_testbed.traffic.schema import load_traffic_flow_profile


def test_random_tm_2_profile_loads() -> None:
    profile = load_traffic_flow_profile("configs/traffic/town01/random_tm_2.yaml")

    assert profile["traffic_flow"]["provider"] == "carla_traffic_manager"
    assert profile["traffic_flow"]["seed"] == 42
    assert profile["traffic_flow"]["vehicles"]["count"] == 2


def test_platform_registry_lists_traffic_profiles() -> None:
    registry = PlatformRegistry(repo_root=".")

    names = registry.names("traffic")

    assert "town01_random_tm_2" in names
    assert registry.get("traffic", "town01/random_tm_2").name == "town01_random_tm_2"


def test_traffic_provider_registry_lists_tm() -> None:
    registry = default_traffic_flow_registry()

    assert "none" in registry.names()
    assert "carla_traffic_manager" in registry.names()
