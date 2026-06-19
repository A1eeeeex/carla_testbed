from __future__ import annotations

import json
import subprocess
import sys

from carla_testbed.experiments.phase1_apollo_fixed_scene_readiness import (
    analyze_apollo_fixed_scene_readiness,
)


def test_default_bridge_template_blocks_baguang_fixed_scene_target_role() -> None:
    report = analyze_apollo_fixed_scene_readiness(
        backend="apollo_cyberrt",
        target_actor_contract={"status": "resolved", "target_actor_role": "lead_vehicle"},
        repo_root=".",
    )

    assert report["status"] == "fail"
    assert report["bridge_config_source"] == "default_bridge_template"
    assert "front_obstacle_actor_probe_disabled" in report["blocking_reasons"]
    assert "target_role_not_in_front_obstacle_role_names" in report["blocking_reasons"]
    assert report["target_role_covered_by_bridge_roles"] is False


def test_explicit_bridge_config_with_target_role_and_actor_probe_passes() -> None:
    report = analyze_apollo_fixed_scene_readiness(
        backend="apollo_cyberrt",
        target_actor_contract={"status": "resolved", "target_actor_role": "lead_vehicle"},
        bridge_config={
            "bridge": {
                "claim_evidence_artifact_sample_stride": 1,
                "front_obstacle_behavior": {
                    "mode": "cruise_then_stop",
                    "actor_probe_enabled": True,
                    "role_names": ["lead_vehicle"],
                },
            }
        },
    )

    assert report["status"] == "pass"
    assert report["actor_probe_enabled_effective"] is True
    assert report["target_role_covered_by_bridge_roles"] is True
    assert report["blocking_reasons"] == []


def test_operator_declared_role_alias_covers_legacy_front_role() -> None:
    report = analyze_apollo_fixed_scene_readiness(
        backend="apollo_cyberrt",
        target_actor_contract={
            "status": "resolved",
            "target_actor_role": "lead_vehicle",
            "role_aliases": {"lead_vehicle": "front"},
        },
        bridge_config={
            "bridge": {
                "front_obstacle_behavior": {
                    "mode": "cruise_then_stop",
                    "actor_probe_enabled": True,
                    "role_names": ["front"],
                },
            }
        },
    )

    assert report["status"] == "warn"
    assert report["target_role_covered_by_bridge_roles"] is True
    assert report["accepted_target_roles"] == ["front", "lead_vehicle"]
    assert report["covered_bridge_roles"] == ["front"]
    assert "target_role_not_in_front_obstacle_role_names" not in report["blocking_reasons"]
    assert "target_role_covered_by_operator_declared_alias" in report["warnings"]


def test_mode_default_actor_probe_is_warn_but_not_blocking() -> None:
    report = analyze_apollo_fixed_scene_readiness(
        backend="apollo_cyberrt",
        target_actor_contract={"status": "resolved", "target_actor_role": "lead_vehicle"},
        bridge_config={
            "bridge": {
                "front_obstacle_behavior": {
                    "mode": "cruise_then_stop",
                    "role_names": ["lead_vehicle"],
                },
            }
        },
    )

    assert report["status"] == "warn"
    assert report["actor_probe_enabled_effective"] is True
    assert report["actor_probe_enabled_source"] == "mode_default"
    assert "actor_probe_enabled_inferred_from_front_obstacle_mode" in report["warnings"]
    assert report["blocking_reasons"] == []


def test_non_apollo_backend_is_not_applicable() -> None:
    report = analyze_apollo_fixed_scene_readiness(
        backend="carla_builtin",
        target_actor_contract={"status": "resolved", "target_actor_role": "lead_vehicle"},
        bridge_config={"bridge": {"front_obstacle_behavior": {"role_names": ["lead_vehicle"]}}},
    )

    assert report["status"] == "not_applicable"
    assert "not_apollo_reference_backend" in report["warnings"]


def test_readiness_cli_uses_explicit_bridge_config(tmp_path) -> None:
    bridge_config = tmp_path / "bridge.yaml"
    bridge_config.write_text(
        """
bridge:
  front_obstacle_behavior:
    mode: cruise_then_stop
    actor_probe_enabled: true
    role_names:
      - lead_vehicle
""",
        encoding="utf-8",
    )
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_phase1_apollo_fixed_scene_readiness.py",
            "--scenario",
            "configs/scenarios/baguang/follow_stop_static_300m.yaml",
            "--bridge-config",
            str(bridge_config),
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads((out / "phase1_apollo_fixed_scene_readiness_report.json").read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert payload["bridge_config_source"] == "explicit_path"
    assert payload["target_actor_role"] == "lead_vehicle"
