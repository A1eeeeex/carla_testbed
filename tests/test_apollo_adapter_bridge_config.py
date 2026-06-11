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
                "bridge": {
                    "map_file": "configs/io/maps/Town01/base_map.txt",
                    "artifact_async_write_enabled": True,
                    "artifact_async_queue_max_rows": 8192,
                    "artifact_async_queue_soft_limit_rows": 4096,
                    "artifact_flush_interval_s": 0.5,
                    "artifact_flush_max_pending_rows": 200,
                    "artifact_stats_flush_interval_s": 1.0,
                    "claim_grade": {
                        "enabled": True,
                        "stale_world_frame_policy": "skip",
                        "localization_publish_policy": "once_per_new_sim_frame",
                        "chassis_publish_policy": "once_per_new_sim_frame",
                    },
                },
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
    bridge = bridge_cfg["bridge"]
    control_mapping = bridge_cfg["bridge"]["control_mapping"]
    claim_grade = bridge_cfg["bridge"]["claim_grade"]
    assert bridge["artifact_async_write_enabled"] is True
    assert bridge["artifact_async_queue_max_rows"] == 8192
    assert bridge["artifact_async_queue_soft_limit_rows"] == 4096
    assert bridge["artifact_flush_interval_s"] == 0.5
    assert bridge["artifact_flush_max_pending_rows"] == 200
    assert bridge["artifact_stats_flush_interval_s"] == 1.0
    assert control_mapping["steer_scale"] == 0.25
    assert control_mapping["steering_percent_normalization"] == "legacy_double_percent"
    assert claim_grade["enabled"] is True
    assert claim_grade["stale_world_frame_policy"] == "skip"
    direct_bridge = bridge_cfg["algo"]["apollo"]["direct_bridge"]
    assert direct_bridge["control_apply_mode"] == "frame_flush_only"
    assert direct_bridge["stale_world_frame_policy"] == "always_republish"
