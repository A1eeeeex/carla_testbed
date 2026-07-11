from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.carla_runtime import (
    _float_attr,
    _make_vector,
    _tick_world_if_available,
)


def materialize_ego_initial_pose(
    *,
    ego_actor: Any,
    initial_transform: Any,
    world: Any | None = None,
    artifact_dir: str | Path | None = None,
    enabled: bool = True,
    source: str = "runtime.fixed_scene_player.materialize_ego_initial_pose",
) -> dict[str, Any]:
    """Restore the setup pose once before a deferred fixed scene starts."""
    report: dict[str, Any] = {
        "schema_version": "ego_initial_pose_materialization.v1",
        "status": "disabled" if not enabled else "not_applicable",
        "source": source,
        "enabled": bool(enabled),
        "pose_before": _transform_dict(_safe_call(ego_actor, "get_transform")),
        "target_pose": _transform_dict(initial_transform),
        "pose_after": None,
        "position_tolerance_m": 1.5,
        "linear_velocity_reset": False,
        "angular_velocity_reset": False,
        "warnings": [],
        "errors": [],
        "claim_boundary": (
            "This artifact records a one-shot Phase 1 scenario setup reset. "
            "It is not ongoing ego control and not autonomy capability evidence."
        ),
    }
    if not enabled:
        return _write_named_report(report, artifact_dir, "ego_initial_pose_materialization.json")
    setter = getattr(ego_actor, "set_transform", None)
    if initial_transform is None:
        report["status"] = "fail"
        report["errors"].append("ego_initial_transform_missing")
    elif not callable(setter):
        report["status"] = "fail"
        report["errors"].append("ego_set_transform_missing")
    else:
        try:
            velocity_setter = getattr(ego_actor, "set_target_velocity", None)
            if callable(velocity_setter):
                velocity_setter(_make_vector(x=0.0, y=0.0, z=0.0))
                report["linear_velocity_reset"] = True
            angular_setter = getattr(ego_actor, "set_target_angular_velocity", None)
            if callable(angular_setter):
                angular_setter(_make_vector(x=0.0, y=0.0, z=0.0))
                report["angular_velocity_reset"] = True
            setter(initial_transform)
        except Exception as exc:  # pragma: no cover - defensive CARLA runtime path
            report["status"] = "fail"
            report["errors"].append(f"ego_set_transform_failed:{type(exc).__name__}")
        else:
            _tick_world_if_available(world)
            report["status"] = "pass"
            report["pose_after"] = _transform_dict(_safe_call(ego_actor, "get_transform"))
            error_m = _pose_distance_m(report["target_pose"], report["pose_after"])
            report["position_error_m"] = error_m
            if error_m is None or error_m > float(report["position_tolerance_m"]):
                report["status"] = "fail"
                report["errors"].append("ego_initial_pose_not_materialized")
    return _write_named_report(report, artifact_dir, "ego_initial_pose_materialization.json")


def materialize_ego_initial_speed(
    *,
    ego_actor: Any,
    storyboard: Mapping[str, Any],
    artifact_dir: str | Path | None = None,
    enabled: bool = True,
    source: str = "runtime.fixed_scene_player.materialize_ego_initial_speed",
) -> dict[str, Any]:
    """Apply a one-shot initial ego velocity for Phase 1 fixed-scene setup.

    This is scenario initial-state materialization, not ongoing ego control.
    The selected backend still owns subsequent throttle/brake/steer commands.
    """

    expected_speed = _ego_initial_speed(storyboard)
    report: dict[str, Any] = {
        "schema_version": "ego_initial_state_materialization.v1",
        "status": "not_applicable",
        "source": source,
        "enabled": bool(enabled),
        "expected_ego_initial_speed_mps": expected_speed,
        "applied_target_velocity": None,
        "observed_ego_speed_mps": _actor_speed_mps(ego_actor),
        "warnings": [],
        "errors": [],
        "claim_boundary": (
            "This artifact records a one-shot Phase 1 scenario setup action. "
            "It is not an ego controller, not an Apollo/Autoware capability "
            "claim, and not natural-driving evidence."
        ),
    }
    if not enabled:
        report["status"] = "disabled"
        return _write_report(report, artifact_dir)
    if expected_speed is None or expected_speed <= 0.0:
        return _write_report(report, artifact_dir)

    transform = _safe_call(ego_actor, "get_transform") or getattr(ego_actor, "transform", None)
    setter = getattr(ego_actor, "set_target_velocity", None)
    if transform is None:
        report["status"] = "fail"
        report["errors"].append("ego_transform_missing")
        return _write_report(report, artifact_dir)
    if not callable(setter):
        report["status"] = "fail"
        report["errors"].append("ego_set_target_velocity_missing")
        return _write_report(report, artifact_dir)

    yaw = math.radians(_float_attr(getattr(transform, "rotation", None), "yaw", 0.0) or 0.0)
    vector = _make_vector(
        x=float(expected_speed) * math.cos(yaw),
        y=float(expected_speed) * math.sin(yaw),
        z=0.0,
    )
    try:
        setter(vector)
    except Exception as exc:  # pragma: no cover - defensive CARLA runtime path
        report["status"] = "fail"
        report["errors"].append(f"ego_set_target_velocity_failed:{type(exc).__name__}")
        return _write_report(report, artifact_dir)

    report["status"] = "pass"
    report["applied_target_velocity"] = {
        "x": _float_attr(vector, "x", 0.0),
        "y": _float_attr(vector, "y", 0.0),
        "z": _float_attr(vector, "z", 0.0),
        "norm_mps": _vector_norm(vector),
    }
    report["observed_ego_speed_mps"] = _actor_speed_mps(ego_actor)
    report["warnings"].append("observed_speed_may_lag_until_next_carla_tick")
    return _write_report(report, artifact_dir)


def _ego_initial_speed(storyboard: Mapping[str, Any]) -> float | None:
    roles = storyboard.get("roles") if isinstance(storyboard.get("roles"), Mapping) else {}
    ego = roles.get("ego") if isinstance(roles.get("ego"), Mapping) else {}
    params = storyboard.get("params") if isinstance(storyboard.get("params"), Mapping) else {}
    for source in (ego, params):
        value = source.get("initial_speed_mps") or source.get("ego_initial_speed_mps")
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _actor_speed_mps(actor: Any) -> float | None:
    velocity = _safe_call(actor, "get_velocity") or getattr(actor, "velocity", None)
    if velocity is None:
        return None
    return _vector_norm(velocity)


def _vector_norm(vector: Any) -> float:
    x = _float_attr(vector, "x", 0.0) or 0.0
    y = _float_attr(vector, "y", 0.0) or 0.0
    z = _float_attr(vector, "z", 0.0) or 0.0
    return math.sqrt(float(x) * float(x) + float(y) * float(y) + float(z) * float(z))


def _safe_call(obj: Any, name: str) -> Any | None:
    method = getattr(obj, name, None)
    if not callable(method):
        return None
    try:
        return method()
    except Exception:
        return None


def _write_report(report: dict[str, Any], artifact_dir: str | Path | None) -> dict[str, Any]:
    return _write_named_report(report, artifact_dir, "ego_initial_state_materialization.json")


def _write_named_report(
    report: dict[str, Any], artifact_dir: str | Path | None, filename: str
) -> dict[str, Any]:
    if artifact_dir is None:
        return report
    root = Path(artifact_dir).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    path = root / filename
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report["artifact_path"] = str(path)
    return report


def _transform_dict(transform: Any) -> dict[str, Any] | None:
    if transform is None:
        return None
    location = getattr(transform, "location", None)
    rotation = getattr(transform, "rotation", None)
    return {
        "x": _float_attr(location, "x", None),
        "y": _float_attr(location, "y", None),
        "z": _float_attr(location, "z", None),
        "roll_deg": _float_attr(rotation, "roll", None),
        "pitch_deg": _float_attr(rotation, "pitch", None),
        "yaw_deg": _float_attr(rotation, "yaw", None),
    }


def _pose_distance_m(left: Any, right: Any) -> float | None:
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return None
    try:
        return math.sqrt(
            sum((float(left[key]) - float(right[key])) ** 2 for key in ("x", "y", "z"))
        )
    except (KeyError, TypeError, ValueError):
        return None
