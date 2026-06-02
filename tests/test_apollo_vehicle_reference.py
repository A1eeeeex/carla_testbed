from __future__ import annotations

from pathlib import Path

import pytest

from carla_testbed.adapters.apollo.frame_transform import ApolloFrameTransform, Vector3
from carla_testbed.adapters.apollo.vehicle_reference import (
    VehicleReferenceConfig,
    carla_actor_origin_to_vrp_carla,
    carla_vrp_to_apollo_position,
    load_vehicle_reference,
)

VEHICLE_REFERENCE_EXAMPLE = Path("configs/vehicles/ego_vehicle_reference.example.yaml")
VEHICLE_REFERENCE_MODULE = Path("carla_testbed/adapters/apollo/vehicle_reference.py")


def test_load_vehicle_reference_example_is_assumed_not_hard_gate() -> None:
    cfg = load_vehicle_reference(VEHICLE_REFERENCE_EXAMPLE)

    assert cfg.vehicle_blueprint == "vehicle.lincoln.mkz_2020"
    assert cfg.apollo_reference_point == "rear_axle_center"
    assert cfg.origin_to_vrp_carla.x == pytest.approx(-1.35)
    assert cfg.confidence == "assumed"
    assert cfg.hard_gate_eligible is False


def test_origin_to_vrp_x_moves_back_along_actor_forward() -> None:
    cfg = _reference_config(Vector3(-1.35, 0.0, 0.0))

    vrp = carla_actor_origin_to_vrp_carla(
        Vector3(10.0, 20.0, 0.0),
        actor_forward=Vector3(1.0, 0.0, 0.0),
        actor_right=Vector3(0.0, 1.0, 0.0),
        actor_up=Vector3(0.0, 0.0, 1.0),
        vehicle_reference=cfg,
    )

    assert vrp == Vector3(8.65, 20.0, 0.0)


def test_right_and_up_offsets_use_actor_basis_vectors() -> None:
    cfg = _reference_config(Vector3(-1.0, 2.0, 0.5))

    vrp = carla_actor_origin_to_vrp_carla(
        Vector3(10.0, 20.0, 3.0),
        actor_forward=Vector3(0.0, 1.0, 0.0),
        actor_right=Vector3(1.0, 0.0, 0.0),
        actor_up=Vector3(0.0, 0.0, 1.0),
        vehicle_reference=cfg,
    )

    assert vrp == Vector3(12.0, 19.0, 3.5)


def test_vrp_position_is_transformed_to_apollo_map() -> None:
    cfg = _reference_config(Vector3(-1.35, 0.0, 0.0), confidence="verified")
    transform = ApolloFrameTransform(tx=100.0, ty=200.0, tz=0.0, scale=1.0, y_flip=True)

    result = carla_vrp_to_apollo_position(
        Vector3(10.0, 20.0, 0.0),
        {
            "forward": Vector3(1.0, 0.0, 0.0),
            "right": Vector3(0.0, 1.0, 0.0),
            "up": Vector3(0.0, 0.0, 1.0),
        },
        cfg,
        transform,
    )

    assert result["vrp_carla"] == {"x": 8.65, "y": 20.0, "z": 0.0}
    assert result["vrp_apollo"] == {"x": 108.65, "y": 180.0, "z": 0.0}
    assert result["reference_confidence"] == "verified"
    assert result["hard_gate_eligible"] is True


def test_missing_origin_to_vrp_carla_has_clear_error() -> None:
    payload = {
        "schema_version": "vehicle_reference.v1",
        "vehicle_blueprint": "vehicle.lincoln.mkz_2020",
        "apollo_reference_point": "rear_axle_center",
        "carla_actor_origin_definition": "measured",
        "source": "manual",
        "confidence": "verified",
    }

    with pytest.raises(ValueError, match="origin_to_vrp_carla is required"):
        VehicleReferenceConfig.from_mapping(payload)


def test_assumed_confidence_blocks_hard_gate_unless_explicitly_allowed() -> None:
    cfg = _reference_config(Vector3(-1.35, 0.0, 0.0), confidence="assumed")
    transform = ApolloFrameTransform()
    basis = {
        "forward": Vector3(1.0, 0.0, 0.0),
        "right": Vector3(0.0, 1.0, 0.0),
        "up": Vector3(0.0, 0.0, 1.0),
    }

    result = carla_vrp_to_apollo_position(Vector3(0.0, 0.0, 0.0), basis, cfg, transform)
    allowed = carla_vrp_to_apollo_position(
        Vector3(0.0, 0.0, 0.0),
        basis,
        cfg,
        transform,
        allow_assumed=True,
    )

    assert result["hard_gate_eligible"] is False
    assert allowed["hard_gate_eligible"] is True


def test_missing_basis_vector_has_clear_error() -> None:
    cfg = _reference_config(Vector3(-1.35, 0.0, 0.0))

    with pytest.raises(ValueError, match="basis_vectors\\.right is required"):
        carla_vrp_to_apollo_position(
            Vector3(0.0, 0.0, 0.0),
            {"forward": Vector3(1.0, 0.0, 0.0), "up": Vector3(0.0, 0.0, 1.0)},
            cfg,
            ApolloFrameTransform(),
        )


def test_vehicle_reference_module_has_no_runtime_imports() -> None:
    text = VEHICLE_REFERENCE_MODULE.read_text(encoding="utf-8").lower()

    assert "import carla" not in text
    assert "import cyber" not in text
    assert "pb2" not in text


def _reference_config(offset: Vector3, *, confidence: str = "verified") -> VehicleReferenceConfig:
    return VehicleReferenceConfig(
        vehicle_blueprint="vehicle.lincoln.mkz_2020",
        apollo_reference_point="rear_axle_center",
        carla_actor_origin_definition="measured_actor_origin",
        origin_to_vrp_carla=offset,
        source="unit_test",
        confidence=confidence,
    )
