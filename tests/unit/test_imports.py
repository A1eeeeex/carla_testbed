from __future__ import annotations

import importlib
import importlib.util

import pytest


MODULES_TO_SMOKE = [
    "carla_testbed",
    "carla_testbed.cli",
    "carla_testbed.runner.harness",
    "carla_testbed.sim",
    "carla_testbed.sensors",
    "carla_testbed.scenarios",
    "carla_testbed.control",
    "carla_testbed.control.base",
    "carla_testbed.control.dummy",
    "carla_testbed.config",
    "carla_testbed.adapters",
    "carla_testbed.adapters.base",
    "carla_testbed.adapters.apollo",
    "carla_testbed.adapters.apollo.control_mapping",
    "carla_testbed.adapters.apollo.cyber_backend",
    "carla_testbed.adapters.apollo.mock_backend",
    "tbio",
    "algo",
]

OPTIONAL_RUNTIME_DEPS = {
    "autoware_control_msgs",
    "carla",
    "cyber",
    "cyber_py",
    "geometry_msgs",
    "rclpy",
    "rosgraph_msgs",
    "sensor_msgs",
    "std_msgs",
    "tf2_msgs",
}


def _missing_optional_runtime_dependency(exc: ModuleNotFoundError) -> str | None:
    missing_name = (exc.name or "").split(".", 1)[0]
    if missing_name in OPTIONAL_RUNTIME_DEPS:
        return missing_name
    return None


@pytest.mark.parametrize("module_name", MODULES_TO_SMOKE)
def test_package_import_smoke(module_name: str) -> None:
    try:
        spec = importlib.util.find_spec(module_name)
    except ModuleNotFoundError as exc:
        missing_dependency = _missing_optional_runtime_dependency(exc)
        if missing_dependency is not None:
            pytest.skip(f"{module_name} requires optional runtime dependency {missing_dependency}")
        raise

    if spec is None:
        pytest.skip(f"{module_name} is not present in this checkout")

    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing_dependency = _missing_optional_runtime_dependency(exc)
        if missing_dependency is not None:
            pytest.skip(f"{module_name} requires optional runtime dependency {missing_dependency}")
        raise
