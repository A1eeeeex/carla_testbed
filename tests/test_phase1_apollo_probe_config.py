from __future__ import annotations

from pathlib import Path

import yaml

from carla_testbed.config.loader import load_config
from carla_testbed.config.sensor_capture import sensor_capture_enabled_from_config


def test_phase1_baguang_apollo_overlay_probe_uses_tf_odom_without_clock() -> None:
    config_path = Path(
        "configs/io/examples/"
        "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_compat.yaml"
    )
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    probe = ((payload.get("record") or {}).get("probe") or {})

    assert probe.get("enable") is True
    assert probe.get("require_clock") is False
    assert probe.get("topics") == [
        "/tf",
        "/tb/ego/control_cmd",
        "/carla/hero/odom",
    ]
    assert probe.get("max_msgs") == 5


def test_phase1_baguang_apollo_low_capture_probe_disables_legacy_sensor_capture() -> None:
    config_path = Path(
        "configs/io/examples/"
        "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_compat.yaml"
    )

    cfg = load_config(config_path)
    resolved = {
        "recording": {
            "artifacts": cfg.recording.artifacts,
        }
    }
    enabled, source = sensor_capture_enabled_from_config(resolved)
    legacy_record = cfg.recording.artifacts["legacy_record"]

    assert cfg.run.id == "phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_compat"
    assert enabled is False
    assert source == "recording.artifacts.legacy_record.sensors.enable"
    assert legacy_record["sensors"]["enable"] is False
    assert legacy_record["probe"]["topics"] == [
        "/tf",
        "/tb/ego/control_cmd",
        "/carla/hero/odom",
    ]
