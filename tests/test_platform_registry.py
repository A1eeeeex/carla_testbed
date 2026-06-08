from __future__ import annotations

import subprocess
import sys

import pytest

from carla_testbed.platform.registry import PlatformRegistry, PlatformRegistryError


def test_platform_registry_lists_core_profile_kinds_without_runtime_imports() -> None:
    registry = PlatformRegistry(repo_root=".")

    assert "apollo_cyberrt" in registry.names("platforms")
    assert "autoware_ros2" in registry.names("platforms")
    assert "carla_builtin" in registry.names("platforms")
    assert "town01_lane_keep_097" in registry.names("scenarios")
    assert "baguang_follow_stop_static_300m" in registry.names("scenarios")
    assert "demo" in registry.names("recording")
    assert "claim_natural_driving" in registry.names("gates")


def test_registry_resolves_nested_algorithm_alias() -> None:
    registry = PlatformRegistry(repo_root=".")

    entry = registry.get("algorithm", "apollo/apollo10_carla_gt")

    assert entry.name == "apollo10_carla_gt"
    assert entry.payload["stack"] == "apollo"


def test_registry_unknown_profile_has_clear_error() -> None:
    registry = PlatformRegistry(repo_root=".")

    with pytest.raises(PlatformRegistryError, match="Unknown platform profile"):
        registry.get("platform", "not_a_platform")


def test_cli_list_platforms() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "carla_testbed", "list", "platforms"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "apollo_cyberrt" in result.stdout
    assert "autoware_ros2" in result.stdout
