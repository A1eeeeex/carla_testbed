from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .frame_transform import ApolloFrameTransform, Vector3, carla_point_to_apollo

VEHICLE_REFERENCE_SCHEMA_VERSION = "vehicle_reference.v1"
CLAIM_GRADE_CONFIDENCE = {"verified"}
CONFIDENCE_VALUES = {"assumed", "measured", "verified"}


@dataclass(frozen=True)
class VehicleReferenceConfig:
    vehicle_blueprint: str
    apollo_reference_point: str
    carla_actor_origin_definition: str
    origin_to_vrp_carla: Vector3
    source: str
    confidence: str
    notes: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "VehicleReferenceConfig":
        _require_value(payload, "schema_version", expected=VEHICLE_REFERENCE_SCHEMA_VERSION)
        origin_to_vrp = _require_mapping(payload, "origin_to_vrp_carla")
        confidence = _require_confidence(payload)
        return cls(
            vehicle_blueprint=_require_text(payload, "vehicle_blueprint"),
            apollo_reference_point=_require_value(
                payload,
                "apollo_reference_point",
                expected="rear_axle_center",
            ),
            carla_actor_origin_definition=_require_text(payload, "carla_actor_origin_definition"),
            origin_to_vrp_carla=Vector3(
                x=_require_number(origin_to_vrp, "x", prefix="origin_to_vrp_carla"),
                y=_require_number(origin_to_vrp, "y", prefix="origin_to_vrp_carla"),
                z=_require_number(origin_to_vrp, "z", prefix="origin_to_vrp_carla"),
            ),
            source=_require_text(payload, "source"),
            confidence=confidence,
            notes=_optional_str(payload.get("notes")),
        )

    @property
    def hard_gate_eligible(self) -> bool:
        if self.confidence not in CLAIM_GRADE_CONFIDENCE:
            return False
        return self.carla_actor_origin_definition.strip().lower() != "unknown"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["origin_to_vrp_carla"] = self.origin_to_vrp_carla.to_dict()
        payload["schema_version"] = VEHICLE_REFERENCE_SCHEMA_VERSION
        payload["hard_gate_eligible"] = self.hard_gate_eligible
        return payload


def load_vehicle_reference(path: str | Path) -> VehicleReferenceConfig:
    payload = yaml.safe_load(Path(path).expanduser().read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"{path} must contain a YAML mapping")
    return VehicleReferenceConfig.from_mapping(payload)


def carla_actor_origin_to_vrp_carla(
    actor_location: Any,
    actor_forward: Any,
    actor_right: Any,
    actor_up: Any,
    vehicle_reference: VehicleReferenceConfig,
) -> Vector3:
    actor_location = Vector3.from_any(actor_location)
    actor_forward = Vector3.from_any(actor_forward)
    actor_right = Vector3.from_any(actor_right)
    actor_up = Vector3.from_any(actor_up)
    offset = vehicle_reference.origin_to_vrp_carla
    return Vector3(
        x=actor_location.x + actor_forward.x * offset.x + actor_right.x * offset.y + actor_up.x * offset.z,
        y=actor_location.y + actor_forward.y * offset.x + actor_right.y * offset.y + actor_up.y * offset.z,
        z=actor_location.z + actor_forward.z * offset.x + actor_right.z * offset.y + actor_up.z * offset.z,
    )


def carla_vrp_to_apollo_position(
    actor_location: Any,
    basis_vectors: Mapping[str, Any],
    vehicle_reference: VehicleReferenceConfig,
    frame_transform: ApolloFrameTransform,
    *,
    allow_assumed: bool = False,
) -> dict[str, Any]:
    vrp_carla = carla_actor_origin_to_vrp_carla(
        actor_location,
        _basis_vector(basis_vectors, "forward"),
        _basis_vector(basis_vectors, "right"),
        _basis_vector(basis_vectors, "up"),
        vehicle_reference,
    )
    vrp_apollo = carla_point_to_apollo(vrp_carla, frame_transform)
    hard_gate_eligible = vehicle_reference.hard_gate_eligible or (
        allow_assumed and vehicle_reference.confidence == "assumed"
    )
    return {
        "schema_version": "apollo_vrp_position.v1",
        "vrp_carla": vrp_carla.to_dict(),
        "vrp_apollo": vrp_apollo.to_dict(),
        "reference_confidence": vehicle_reference.confidence,
        "hard_gate_eligible": hard_gate_eligible,
        "apollo_reference_point": vehicle_reference.apollo_reference_point,
        "vehicle_blueprint": vehicle_reference.vehicle_blueprint,
    }


def _basis_vector(basis_vectors: Mapping[str, Any], name: str) -> Any:
    if name not in basis_vectors:
        raise ValueError(f"basis_vectors.{name} is required")
    return basis_vectors[name]


def _require_mapping(cfg: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    value = cfg.get(field)
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} is required and must be a mapping")
    return value


def _require_value(cfg: Mapping[str, Any], field: str, *, expected: str) -> str:
    value = cfg.get(field)
    if value in {None, ""}:
        raise ValueError(f"{field} is required")
    text = str(value)
    if text != expected:
        raise ValueError(f"{field} must be {expected!r}, got {text!r}")
    return text


def _require_text(cfg: Mapping[str, Any], field: str) -> str:
    value = cfg.get(field)
    if value in {None, ""}:
        raise ValueError(f"{field} is required")
    return str(value)


def _require_number(cfg: Mapping[str, Any], field: str, *, prefix: str) -> float:
    value = cfg.get(field)
    path = f"{prefix}.{field}"
    if value in {None, ""}:
        raise ValueError(f"{path} is required")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be numeric, got {value!r}") from exc


def _require_confidence(cfg: Mapping[str, Any]) -> str:
    confidence = _require_text(cfg, "confidence")
    if confidence not in CONFIDENCE_VALUES:
        raise ValueError(f"confidence must be one of {sorted(CONFIDENCE_VALUES)}, got {confidence!r}")
    return confidence


def _optional_str(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)
