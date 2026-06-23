from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.scenario_player.carla_runtime import _float_attr, _make_vector


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
    if artifact_dir is None:
        return report
    root = Path(artifact_dir).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    path = root / "ego_initial_state_materialization.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report["artifact_path"] = str(path)
    return report
