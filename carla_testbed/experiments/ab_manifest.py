from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

AB_MANIFEST_SCHEMA_VERSION = "ab_manifest.v1"

FIXED_VARIABLE_KEYS = [
    "carla_version",
    "apollo_version",
    "town_map",
    "map_hash",
    "route_id",
    "route_definition_hash",
    "spawn_pose",
    "ego_blueprint",
    "vehicle_physics_hash",
    "fixed_delta_seconds",
    "sim_duration_s",
    "random_seed",
    "traffic_actors",
    "sensor_rig",
    "localization_source",
    "chassis_source",
    "routing_source",
    "control_mode",
    "actuator_mapping_mode",
    "steer_scale",
    "guard_config_hash",
    "calibration_profile_id",
    "timeout_policy",
]

CANDIDATE_POSITIVE_INPUT_KEYS = [
    "route_completion",
    "control_available",
    "planning_available",
    "lateral_error",
    "heading_error",
    "failure_reason",
    "artifact_complete",
]


@dataclass(frozen=True)
class ABRunRecord:
    run_id: str
    route_id: str
    backend: str
    duration_s: float | None
    run_dir: str | None
    command: str | None
    config_path: str | None
    resolved_config_path: str | None
    summary_path: str | None
    route_health_path: str | None
    status: str
    control_health_path: str | None = None
    apollo_channel_health_path: str | None = None
    failure_reason: str | None = None
    return_code: int | None = None
    actual_run_dir: str | None = None
    route_health_error: str | None = None
    transport_mode: str | None = None
    bridge_mode: str | None = None
    backend_config_path: str | None = None
    direct_control_apply_mode: str | None = None
    direct_stale_world_frame_policy: str | None = None
    steering_percent_normalization: str | None = None
    command_stdout_path: str | None = None
    command_stderr_path: str | None = None

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "ABRunRecord":
        return cls(
            run_id=str(payload.get("run_id") or ""),
            route_id=str(payload.get("route_id") or ""),
            backend=str(payload.get("backend") or ""),
            duration_s=None if payload.get("duration_s") is None else float(payload["duration_s"]),
            run_dir=None if payload.get("run_dir") is None else str(payload.get("run_dir")),
            command=None if payload.get("command") is None else str(payload.get("command")),
            config_path=None if payload.get("config_path") is None else str(payload.get("config_path")),
            resolved_config_path=(
                None if payload.get("resolved_config_path") is None else str(payload.get("resolved_config_path"))
            ),
            summary_path=None if payload.get("summary_path") is None else str(payload.get("summary_path")),
            route_health_path=None if payload.get("route_health_path") is None else str(payload.get("route_health_path")),
            control_health_path=(
                None if payload.get("control_health_path") is None else str(payload.get("control_health_path"))
            ),
            apollo_channel_health_path=(
                None
                if payload.get("apollo_channel_health_path") is None
                else str(payload.get("apollo_channel_health_path"))
            ),
            status=str(payload.get("status") or ""),
            failure_reason=None if payload.get("failure_reason") is None else str(payload.get("failure_reason")),
            return_code=None if payload.get("return_code") is None else int(payload.get("return_code")),
            actual_run_dir=None if payload.get("actual_run_dir") is None else str(payload.get("actual_run_dir")),
            route_health_error=(
                None if payload.get("route_health_error") is None else str(payload.get("route_health_error"))
            ),
            transport_mode=None if payload.get("transport_mode") is None else str(payload.get("transport_mode")),
            bridge_mode=None if payload.get("bridge_mode") is None else str(payload.get("bridge_mode")),
            backend_config_path=(
                None if payload.get("backend_config_path") is None else str(payload.get("backend_config_path"))
            ),
            direct_control_apply_mode=(
                None if payload.get("direct_control_apply_mode") is None else str(payload.get("direct_control_apply_mode"))
            ),
            direct_stale_world_frame_policy=(
                None
                if payload.get("direct_stale_world_frame_policy") is None
                else str(payload.get("direct_stale_world_frame_policy"))
            ),
            steering_percent_normalization=(
                None
                if payload.get("steering_percent_normalization") is None
                else str(payload.get("steering_percent_normalization"))
            ),
            command_stdout_path=(
                None if payload.get("command_stdout_path") is None else str(payload.get("command_stdout_path"))
            ),
            command_stderr_path=(
                None if payload.get("command_stderr_path") is None else str(payload.get("command_stderr_path"))
            ),
        )


@dataclass
class ABManifest:
    batch_id: str
    created_at: str
    route_set: str
    baseline_backend: str
    candidate_backend: str
    durations_s: list[float] = field(default_factory=list)
    runs: list[ABRunRecord] = field(default_factory=list)
    fixed_variables: dict[str, Any] = field(default_factory=dict)
    allowed_differences: list[str] = field(default_factory=list)
    missing_fixed_variables: list[str] = field(default_factory=list)
    consistency_status: str = "unchecked"
    notes: str = ""
    candidate_positive_inputs: dict[str, Any] = field(default_factory=dict)
    analysis_commands: dict[str, str] = field(default_factory=dict)
    schema_version: str = AB_MANIFEST_SCHEMA_VERSION

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "ABManifest":
        return cls(
            schema_version=str(payload.get("schema_version") or ""),
            batch_id=str(payload.get("batch_id") or ""),
            created_at=str(payload.get("created_at") or ""),
            route_set=str(payload.get("route_set") or ""),
            baseline_backend=str(payload.get("baseline_backend") or ""),
            candidate_backend=str(payload.get("candidate_backend") or ""),
            durations_s=[float(item) for item in payload.get("durations_s") or []],
            runs=[ABRunRecord.from_mapping(item) for item in payload.get("runs") or [] if isinstance(item, dict)],
            fixed_variables=dict(payload.get("fixed_variables") or {}),
            allowed_differences=[str(item) for item in payload.get("allowed_differences") or []],
            missing_fixed_variables=[str(item) for item in payload.get("missing_fixed_variables") or []],
            consistency_status=str(payload.get("consistency_status") or "unchecked"),
            notes=str(payload.get("notes") or ""),
            candidate_positive_inputs=dict(payload.get("candidate_positive_inputs") or {}),
            analysis_commands={str(key): str(value) for key, value in (payload.get("analysis_commands") or {}).items()},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["runs"] = [asdict(run) for run in self.runs]
        return payload


def load_ab_manifest(path: str | Path) -> ABManifest:
    manifest_path = Path(path).expanduser()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"AB manifest root must be an object: {manifest_path}")
    return ABManifest.from_mapping(payload)
