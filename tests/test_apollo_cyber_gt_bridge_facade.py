from __future__ import annotations

from pathlib import Path

from carla_testbed.adapters.apollo.cyber_gt_bridge import default_apollo_cyber_gt_bridge_entrypoint


def test_apollo_cyber_gt_bridge_facade_points_to_current_legacy_runtime() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    entrypoint = default_apollo_cyber_gt_bridge_entrypoint(repo_root)

    assert entrypoint.bridge_script == repo_root / "tools" / "apollo10_cyber_bridge" / "bridge.py"
    assert entrypoint.bridge_package_dir == repo_root / "tools" / "apollo10_cyber_bridge"
    assert entrypoint.bridge_script.exists()
    assert entrypoint.pb_root.name == "pb"
