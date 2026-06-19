from __future__ import annotations

from pathlib import Path

import yaml


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
