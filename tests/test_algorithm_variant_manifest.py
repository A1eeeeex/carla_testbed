from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from carla_testbed.algorithms.variant import (
    AlgorithmVariantError,
    load_algorithm_variant,
    validate_algorithm_variant,
)

UPSTREAM_PATH = Path("configs/algorithms/apollo_variant.upstream.example.yaml")
CARLA_GT_PATH = Path("configs/algorithms/apollo_variant.carla_gt.example.yaml")


def _read_yaml(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_upstream_example_passes_without_patches() -> None:
    manifest = load_algorithm_variant(UPSTREAM_PATH)

    assert manifest["variant_id"] == "apollo_upstream_10_0_reference"
    assert manifest["variant_type"] == "upstream"
    assert manifest["apollo"]["source_patch_path"] is None
    assert manifest["apollo"]["config_patch_path"] is None
    assert manifest["_validation"]["ok"] is True


def test_carla_gt_example_passes_and_preserves_variant_type() -> None:
    manifest = load_algorithm_variant(CARLA_GT_PATH)

    assert manifest["variant_id"] == "apollo_10_0_carla_gt_town01_reference"
    assert manifest["variant_type"] == "ported_carla_gt"
    assert manifest["bridge"]["backend"] == "carla_direct"
    assert "planning_algorithm_code" in manifest["forbidden_changes"]
    assert "control_algorithm_code" in manifest["forbidden_changes"]
    assert manifest["_validation"]["ok"] is True


def test_upstream_with_patch_fails() -> None:
    manifest = _read_yaml(UPSTREAM_PATH)
    manifest["apollo"]["source_patch_path"] = "patches/apollo/source.patch"
    manifest["apollo"]["source_patch_sha256"] = "sha256-placeholder"

    validation = validate_algorithm_variant(manifest)

    assert not validation.ok
    assert any("upstream variant must not set source_patch" in error for error in validation.errors)


def test_tuned_town01_missing_tuning_reason_fails() -> None:
    manifest = _read_yaml(CARLA_GT_PATH)
    manifest["variant_id"] = "apollo_tuned_town01_example"
    manifest["variant_type"] = "tuned_town01"
    manifest["apollo"]["config_patch_path"] = "patches/apollo/town01_config.patch"
    manifest["apollo"]["config_patch_sha256"] = "sha256-placeholder"

    validation = validate_algorithm_variant(manifest)

    assert not validation.ok
    assert any("tuning_reason" in error for error in validation.errors)


def test_missing_bridge_config_hash_warns_but_does_not_fail() -> None:
    manifest = _read_yaml(CARLA_GT_PATH)
    manifest["bridge"]["bridge_config_sha256"] = None

    validation = validate_algorithm_variant(manifest)

    assert validation.ok
    assert any("bridge.bridge_config_sha256" in warning for warning in validation.warnings)


def test_ported_carla_gt_requires_planning_and_control_forbidden_changes() -> None:
    manifest = _read_yaml(CARLA_GT_PATH)
    manifest["forbidden_changes"] = [
        item
        for item in manifest["forbidden_changes"]
        if item not in {"planning_algorithm_code", "control_algorithm_code"}
    ]

    validation = validate_algorithm_variant(manifest)

    assert not validation.ok
    assert any("planning_algorithm_code" in error for error in validation.errors)
    assert any("control_algorithm_code" in error for error in validation.errors)


def test_loader_raises_on_invalid_manifest(tmp_path: Path) -> None:
    manifest = _read_yaml(UPSTREAM_PATH)
    invalid = deepcopy(manifest)
    invalid["variant_id"] = ""
    path = tmp_path / "invalid_variant.yaml"
    path.write_text(yaml.safe_dump(invalid), encoding="utf-8")

    validation = validate_algorithm_variant(invalid)

    assert not validation.ok
    with pytest.raises(AlgorithmVariantError):
        load_algorithm_variant(path)
