from __future__ import annotations

from pathlib import Path

import yaml


DIRECT_CANDIDATE_CONFIG = Path(
    "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
)


def test_direct_candidate_keeps_heavy_control_raw_dump_disabled() -> None:
    cfg = yaml.safe_load(DIRECT_CANDIDATE_CONFIG.read_text(encoding="utf-8"))

    assert cfg["algo"]["apollo"]["bridge"]["debug_dump_control_raw"] is False


def test_direct_candidate_disables_raw_sensor_frame_capture_by_default() -> None:
    cfg = yaml.safe_load(DIRECT_CANDIDATE_CONFIG.read_text(encoding="utf-8"))

    assert cfg["record"]["sensors"]["enable"] is False


def test_direct_candidate_apollo_warmup_fits_short_light_artifact_canary() -> None:
    cfg = yaml.safe_load(DIRECT_CANDIDATE_CONFIG.read_text(encoding="utf-8"))

    warmup_s = cfg["algo"]["apollo"]["routing"]["startup_apollo_warmup_sec"]
    assert warmup_s == 0.0


def test_direct_candidate_keeps_town01_control_sign_consistent_with_baseline() -> None:
    cfg = yaml.safe_load(DIRECT_CANDIDATE_CONFIG.read_text(encoding="utf-8"))

    assert cfg["algo"]["apollo"]["control_mapping"]["steer_sign"] == -1.0


def test_direct_candidate_keeps_planning_stitcher_semantics_consistent_with_baseline() -> None:
    cfg = yaml.safe_load(DIRECT_CANDIDATE_CONFIG.read_text(encoding="utf-8"))
    planning = cfg["algo"]["apollo"]["planning"]

    assert planning["enable_reference_line_stitching"] is False
    assert planning["enable_trajectory_stitcher"] is False
