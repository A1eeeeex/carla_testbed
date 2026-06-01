from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "calibration_profile.v1"
STATUS_VALUES = {"draft", "measured", "validated", "rejected"}
ACTUATOR_MAPPING_MODES = {"legacy", "physical", "custom"}
REQUIRED_GATE_ROUTE_IDS = {"097", "217", "031"}


class CalibrationProfileError(ValueError):
    pass


@dataclass(frozen=True)
class ControlMappingProfile:
    actuator_mapping_mode: str
    steer_scale: float
    steering_sign: int
    throttle_mapping: dict[str, Any]
    brake_mapping: dict[str, Any]

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ControlMappingProfile":
        return cls(
            actuator_mapping_mode=str(payload.get("actuator_mapping_mode") or ""),
            steer_scale=float(payload.get("steer_scale")) if payload.get("steer_scale") is not None else 0.0,
            steering_sign=int(payload.get("steering_sign", 1)),
            throttle_mapping=dict(payload.get("throttle_mapping") or {}),
            brake_mapping=dict(payload.get("brake_mapping") or {}),
        )


@dataclass(frozen=True)
class LatencyProfile:
    steer: float | None
    throttle: float | None
    brake: float | None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LatencyProfile":
        return cls(
            steer=_optional_float(payload.get("steer")),
            throttle=_optional_float(payload.get("throttle")),
            brake=_optional_float(payload.get("brake")),
        )


@dataclass(frozen=True)
class NoRegressionGate:
    route_id: str
    purpose: str
    status: str
    required: bool = True

    @classmethod
    def from_mapping(cls, route_id: str, payload: Mapping[str, Any]) -> "NoRegressionGate":
        return cls(
            route_id=str(payload.get("route_id") or route_id),
            purpose=str(payload.get("purpose") or payload.get("description") or ""),
            status=str(payload.get("status") or "pending"),
            required=bool(payload.get("required", True)),
        )


@dataclass(frozen=True)
class CalibrationProfile:
    schema_version: str
    profile_id: str
    created_at: str
    status: str
    environment: dict[str, Any]
    vehicle: dict[str, Any]
    control_mapping: ControlMappingProfile
    latency_ms: LatencyProfile
    validation_routes: tuple[str, ...]
    no_regression_gates: tuple[NoRegressionGate, ...]
    recommendation_policy: dict[str, Any]
    notes: str
    source_path: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any], *, source_path: str | None = None) -> "CalibrationProfile":
        gates_payload = payload.get("no_regression_gates") or {}
        gates: list[NoRegressionGate] = []
        if isinstance(gates_payload, Mapping):
            for route_id, gate_payload in gates_payload.items():
                if isinstance(gate_payload, Mapping):
                    gates.append(NoRegressionGate.from_mapping(str(route_id), gate_payload))
        elif isinstance(gates_payload, list):
            for gate_payload in gates_payload:
                if isinstance(gate_payload, Mapping):
                    gates.append(NoRegressionGate.from_mapping(str(gate_payload.get("route_id") or ""), gate_payload))

        return cls(
            schema_version=str(payload.get("schema_version") or ""),
            profile_id=str(payload.get("profile_id") or ""),
            created_at=str(payload.get("created_at") or ""),
            status=str(payload.get("status") or ""),
            environment=dict(payload.get("environment") or {}),
            vehicle=dict(payload.get("vehicle") or {}),
            control_mapping=ControlMappingProfile.from_mapping(payload.get("control_mapping") or {}),
            latency_ms=LatencyProfile.from_mapping(payload.get("latency_ms") or {}),
            validation_routes=tuple(str(item) for item in payload.get("validation_routes") or ()),
            no_regression_gates=tuple(gates),
            recommendation_policy=dict(payload.get("recommendation_policy") or {}),
            notes=str(payload.get("notes") or ""),
            source_path=source_path,
        )


@dataclass(frozen=True)
class CalibrationProfileValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": list(self.errors), "warnings": list(self.warnings)}


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _missing_required_routes(profile: CalibrationProfile) -> set[str]:
    routes = set(profile.validation_routes)
    gate_routes = {gate.route_id for gate in profile.no_regression_gates}
    return REQUIRED_GATE_ROUTE_IDS - routes | (REQUIRED_GATE_ROUTE_IDS - gate_routes)


def _required_gates(profile: CalibrationProfile) -> dict[str, NoRegressionGate]:
    return {gate.route_id: gate for gate in profile.no_regression_gates if gate.required}


def _all_required_gates_pass(profile: CalibrationProfile) -> bool:
    gates = _required_gates(profile)
    return all(gates.get(route_id) is not None and gates[route_id].status == "pass" for route_id in REQUIRED_GATE_ROUTE_IDS)


def validate_calibration_profile(
    profile: CalibrationProfile,
    *,
    allow_missing_gates: bool = False,
) -> CalibrationProfileValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if profile.schema_version != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if not profile.profile_id:
        errors.append("profile_id is required")
    if profile.status not in STATUS_VALUES:
        errors.append(f"status must be one of {sorted(STATUS_VALUES)}")
    if profile.control_mapping.actuator_mapping_mode not in ACTUATOR_MAPPING_MODES:
        errors.append(
            f"actuator_mapping_mode must be one of {sorted(ACTUATOR_MAPPING_MODES)}"
        )
    if profile.control_mapping.steer_scale <= 0:
        errors.append("steer_scale must be > 0")
    if profile.control_mapping.steering_sign not in {-1, 1}:
        errors.append("steering_sign must be -1 or 1")

    missing_routes = sorted(_missing_required_routes(profile))
    if missing_routes:
        message = f"validation_routes/no_regression_gates missing required routes: {', '.join(missing_routes)}"
        if allow_missing_gates and profile.status == "draft":
            warnings.append(message)
        else:
            errors.append(message)

    if profile.status == "validated" and not _all_required_gates_pass(profile):
        errors.append("validated profile requires 097/217/031 no_regression_gates with status=pass")

    if profile.control_mapping.actuator_mapping_mode == "physical" and not _all_required_gates_pass(profile):
        errors.append("physical actuator mapping requires all 097/217/031 no_regression_gates to pass")

    policy = profile.recommendation_policy
    if policy.get("can_modify_mainline_config") is not False:
        errors.append("recommendation_policy.can_modify_mainline_config must be false")
    if policy.get("promotion_requires_all_no_regression_gates_pass") is not True:
        errors.append("recommendation_policy.promotion_requires_all_no_regression_gates_pass must be true")
    if policy.get("automatic_promotion") is not False:
        errors.append("recommendation_policy.automatic_promotion must be false")

    if profile.status == "draft" and any(
        getattr(profile.latency_ms, field) is None for field in ("steer", "throttle", "brake")
    ):
        warnings.append("draft profile has null latency_ms fields")

    return CalibrationProfileValidation(tuple(errors), tuple(warnings))


def load_calibration_profile(
    path: str | Path,
    *,
    allow_missing_gates: bool = False,
) -> CalibrationProfile:
    profile_path = Path(path).expanduser()
    payload = yaml.safe_load(profile_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise CalibrationProfileError(f"calibration profile must be a mapping: {profile_path}")
    profile = CalibrationProfile.from_mapping(payload, source_path=str(profile_path))
    validation = validate_calibration_profile(profile, allow_missing_gates=allow_missing_gates)
    if validation.errors:
        raise CalibrationProfileError("; ".join(validation.errors))
    return profile
