from __future__ import annotations

import subprocess
import sys

from carla_testbed.backends.registry import default_backend_registry
from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.registry import PlatformRegistry


def test_backend_contracts_are_metadata_only() -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="claim",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    backend = default_backend_registry().for_plan(plan)
    contract = backend.contract(plan)
    preflight = backend.preflight(plan)

    assert contract.backend == "apollo_cyberrt"
    assert contract.needs_local_carla is True
    assert contract.needs_local_apollo is True
    assert "/apollo/localization/pose" in contract.required_inputs
    assert preflight.starts_runtime is False
    assert preflight.status == "local_required"


def test_cli_lists_backends() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "carla_testbed", "list", "backends"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "apollo_cyberrt" in result.stdout
    assert "autoware_ros2" in result.stdout
