from __future__ import annotations

from pathlib import Path

import yaml

from algo.adapters.apollo import ApolloAdapter


def test_apollo_adapter_preserves_steering_percent_normalization_in_bridge_config(tmp_path: Path) -> None:
    profile = {
        "run": {"ego_id": "hero"},
        "runtime": {"carla": {"host": "127.0.0.1", "port": 2000}},
        "sim": {"map": "Town01"},
        "algo": {
            "apollo": {
                "apollo_root": "",
                "docker": {"enabled": False},
                "bridge": {"map_file": "configs/io/maps/Town01/base_map.txt"},
                "transport_mode": "carla_direct",
                "direct_bridge": {
                    "control_apply_mode": "frame_flush_only",
                    "stale_world_frame_policy": "always_republish",
                },
                "control_mapping": {
                    "steer_scale": 0.25,
                    "steering_percent_normalization": "legacy_double_percent",
                },
            }
        },
    }

    ApolloAdapter().prepare(profile, tmp_path)

    bridge_cfg_path = tmp_path / "artifacts" / "apollo_bridge_effective.yaml"
    bridge_cfg = yaml.safe_load(bridge_cfg_path.read_text(encoding="utf-8"))
    control_mapping = bridge_cfg["bridge"]["control_mapping"]
    assert control_mapping["steer_scale"] == 0.25
    assert control_mapping["steering_percent_normalization"] == "legacy_double_percent"
    direct_bridge = bridge_cfg["algo"]["apollo"]["direct_bridge"]
    assert direct_bridge["control_apply_mode"] == "frame_flush_only"
    assert direct_bridge["stale_world_frame_policy"] == "always_republish"
