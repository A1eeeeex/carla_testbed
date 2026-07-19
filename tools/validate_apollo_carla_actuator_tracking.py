#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from tools.apollo10_cyber_bridge.actuator_mapping import ActuatorCalibration
    from tools.calibrate_carla_actuators import CarlaProbe


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_runtime_modules():
    try:
        from tools.apollo10_cyber_bridge.actuator_mapping import load_actuator_calibration
        from tools.calibrate_carla_actuators import CarlaProbe, _safe_float
    except Exception:
        from apollo10_cyber_bridge.actuator_mapping import load_actuator_calibration
        from calibrate_carla_actuators import CarlaProbe, _safe_float
    return load_actuator_calibration, CarlaProbe, _safe_float


def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _info(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _warn(msg: str) -> None:
    print(f"[{_ts()}][WARN] {msg}", flush=True)


def _median(values: Iterable[float], default: float = 0.0) -> float:
    buf = [float(v) for v in values]
    if not buf:
        return float(default)
    return float(statistics.median(buf))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
    except Exception:
        return float(default)
    if math.isnan(out) or math.isinf(out):
        return float(default)
    return out


def _optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _percentile(values: Iterable[float], q: float, default: float = 0.0) -> float:
    buf = sorted(float(v) for v in values)
    if not buf:
        return float(default)
    if len(buf) == 1:
        return float(buf[0])
    q = min(1.0, max(0.0, float(q)))
    pos = q * (len(buf) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(buf[lo])
    frac = pos - lo
    return float(buf[lo] * (1.0 - frac) + buf[hi] * frac)


def _parse_float_list(raw: str) -> List[float]:
    return [float(item.strip()) for item in str(raw).split(",") if item.strip()]


def _parse_axes(raw: str) -> Tuple[str, ...]:
    supported = ("steering", "throttle", "brake")
    requested = tuple(
        dict.fromkeys(item.strip().lower() for item in str(raw).split(",") if item.strip())
    )
    unknown = [item for item in requested if item not in supported]
    if unknown:
        raise ValueError(f"unsupported validation axes: {','.join(unknown)}")
    if not requested:
        raise ValueError("at least one validation axis is required")
    return requested


def _tail_text(path: Path, *, max_lines: int = 60) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(errors="replace").splitlines()
    except Exception:
        return ""
    return "\n".join(lines[-max_lines:])


def _find_existing_redirect(run_dir: Path) -> Optional[Path]:
    redirect_path = run_dir / "RUN_DIR_REDIRECT.txt"
    if redirect_path.exists():
        try:
            candidate = Path(redirect_path.read_text().strip())
            if candidate.exists():
                return candidate
        except Exception:
            pass
    latest_path = run_dir / "LATEST.txt"
    if latest_path.exists():
        try:
            candidate = Path(latest_path.read_text().strip())
            if candidate.exists():
                return candidate
        except Exception:
            pass
    siblings = sorted(run_dir.parent.glob(f"{run_dir.name}__*"))
    for candidate in reversed(siblings):
        if candidate.exists():
            return candidate
    return None


def _resolve_run_dir(run_dir: Path) -> Path:
    if run_dir.exists():
        redirect = _find_existing_redirect(run_dir)
        if redirect is not None:
            return redirect
        return run_dir
    redirect = _find_existing_redirect(run_dir)
    if redirect is not None:
        return redirect
    return run_dir


def _wait_for_metadata(run_dir: Path, *, timeout_sec: float) -> Tuple[Path, Dict[str, Any]]:
    deadline = time.monotonic() + max(1.0, float(timeout_sec))
    while time.monotonic() < deadline:
        resolved = _resolve_run_dir(run_dir)
        path = resolved / "artifacts" / "scenario_metadata.json"
        if path.exists():
            payload = json.loads(path.read_text())
            if isinstance(payload, dict):
                return resolved, payload
        time.sleep(1.0)
    raise RuntimeError(f"scenario metadata not ready under {run_dir}")


def _run_child(cmd: Sequence[str], *, cwd: Path, log_path: Path) -> subprocess.Popen[str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fp = log_path.open("w")
    proc = subprocess.Popen(
        list(cmd),
        cwd=str(cwd),
        stdout=fp,
        stderr=subprocess.STDOUT,
        text=True,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,  # type: ignore[name-defined]
    )
    proc._codex_log_fp = fp  # type: ignore[attr-defined]
    return proc


def _stop_child(proc: Optional[subprocess.Popen[str]], *, grace_sec: float = 10.0) -> None:
    if proc is None:
        return
    try:
        fp = getattr(proc, "_codex_log_fp", None)
    except Exception:
        fp = None
    if proc.poll() is None:
        try:
            if hasattr(os, "killpg") and hasattr(os, "getpgid"):  # type: ignore[name-defined]
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)  # type: ignore[name-defined]
            else:
                proc.send_signal(signal.SIGINT)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
        deadline = time.monotonic() + max(1.0, grace_sec)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            time.sleep(0.25)
        if proc.poll() is None:
            try:
                if hasattr(os, "killpg") and hasattr(os, "getpgid"):  # type: ignore[name-defined]
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)  # type: ignore[name-defined]
                else:
                    proc.kill()
            except Exception:
                pass
    if fp is not None:
        try:
            fp.close()
        except Exception:
            pass


def _sample_window(
    probe: CarlaProbe,
    *,
    throttle: float,
    brake: float,
    steer: float,
    sample_sec: float,
) -> List[Dict[str, float]]:
    return list(
        probe.hold_and_sample(
            throttle=float(throttle),
            brake=float(brake),
            steer=float(steer),
            settle_sec=0.0,
            sample_sec=float(sample_sec),
        )
    )


def _effective_throttle_accel(samples: Sequence[Dict[str, float]], *, window_sec: float) -> float:
    active = []
    for item in samples:
        elapsed = _safe_float(item.get("elapsed_sec"), 0.0)
        if elapsed > float(window_sec):
            continue
        speed = _safe_float(item.get("speed_mps"), 0.0)
        accel = _safe_float(item.get("forward_accel_mps2"), 0.0)
        if speed >= 0.2:
            active.append(accel)
    if not active:
        active = [_safe_float(item.get("forward_accel_mps2"), 0.0) for item in samples]
    return _percentile(active, 0.7, default=0.0)


def _effective_brake_decel(
    samples: Sequence[Dict[str, float]],
    *,
    start_sec: float,
    end_sec: float,
) -> float:
    if not samples or float(end_sec) <= float(start_sec):
        return 0.0
    speed_key = (
        "forward_speed_mps"
        if any("forward_speed_mps" in item for item in samples)
        else "speed_mps"
    )
    start_speed = _value_at_elapsed(samples, float(start_sec), speed_key, default=0.0)
    end_speed = _value_at_elapsed(samples, float(end_sec), speed_key, default=start_speed)
    return max(0.0, (start_speed - end_speed) / (float(end_sec) - float(start_sec)))


def _value_at_elapsed(samples: Sequence[Dict[str, float]], elapsed_sec: float, key: str, *, default: float = 0.0) -> float:
    if not samples:
        return float(default)
    target = float(elapsed_sec)
    for item in samples:
        if _safe_float(item.get("elapsed_sec"), 0.0) >= target:
            return _safe_float(item.get(key), default)
    return _safe_float(samples[-1].get(key), default)


def _motion_speed_mps(row: Dict[str, Any]) -> float:
    forward_speed = _optional_float(row.get("forward_speed_mps"))
    if forward_speed is not None:
        return abs(float(forward_speed))
    velocity_x = _optional_float(row.get("velocity_x_mps"))
    velocity_y = _optional_float(row.get("velocity_y_mps"))
    if velocity_x is not None and velocity_y is not None:
        return math.hypot(float(velocity_x), float(velocity_y))
    return abs(_safe_float(row.get("speed_mps"), 0.0))


def _motion_speed_at_elapsed(
    samples: Sequence[Dict[str, float]],
    elapsed_sec: float,
    *,
    default: float = 0.0,
) -> float:
    if not samples:
        return float(default)
    for item in samples:
        if _safe_float(item.get("elapsed_sec"), 0.0) >= float(elapsed_sec):
            return _motion_speed_mps(item)
    return _motion_speed_mps(samples[-1])


def _time_to_motion_threshold(
    samples: Sequence[Dict[str, float]],
    *,
    threshold_mps: float,
) -> Optional[float]:
    for item in samples:
        if _motion_speed_mps(item) <= float(threshold_mps):
            return _safe_float(item.get("elapsed_sec"), 0.0)
    return None


def _choose_steering_targets(cal: ActuatorCalibration, *, apollo_max_steer_angle_deg: float) -> List[float]:
    max_abs = float(apollo_max_steer_angle_deg)
    pairs = getattr(cal, "_steering_angle_table", None)
    if pairs and getattr(pairs, "pairs", None):
        max_table = max(abs(float(x)) for x, _ in pairs.pairs)
        max_abs = min(max_abs, max_table) if max_abs > 0.0 else max_table
    base = [0.0, 0.5, 1.0, 2.0, 3.0, 4.5, 6.0, 8.0]
    vals = sorted({min(max_abs, v) for v in base if min(max_abs, v) > 0.0})
    out = [-v for v in reversed(vals)] + [0.0] + vals
    return out


def validate_steering(probe: CarlaProbe, cal: ActuatorCalibration, args: argparse.Namespace) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []
    for target_deg in _choose_steering_targets(cal, apollo_max_steer_angle_deg=args.apollo_max_steer_angle_deg):
        probe.reset_to_reference_pose(settle_sec=args.reset_settle_sec)
        mapped_cmd = cal.steering_cmd_for_angle(target_deg)
        if mapped_cmd is None:
            continue
        samples = probe.hold_and_sample(
            throttle=float(args.validation_steering_probe_throttle),
            brake=0.0,
            steer=float(mapped_cmd),
            settle_sec=float(args.validation_steering_settle_sec),
            sample_sec=float(args.validation_steering_sample_sec),
        )
        if not samples:
            continue
        measured = _median(
            [_safe_float(item.get("measured_steer_deg"), 0.0) for item in samples],
            default=0.0,
        )
        yaw_rate = _median([_safe_float(item.get("yaw_rate_rps"), 0.0) for item in samples], default=0.0)
        curvature = _median([_safe_float(item.get("curvature"), 0.0) for item in samples], default=0.0)
        record = {
            "target_front_wheel_angle_deg": float(target_deg),
            "mapped_carla_steer_cmd": float(mapped_cmd),
            "measured_steer_deg": float(measured),
            "yaw_rate_rps": float(yaw_rate),
            "curvature": float(curvature),
            "abs_error_deg": abs(float(target_deg) - float(measured)),
            "sample_count": len(samples),
        }
        records.append(record)
        raw_rows.extend(
            {
                "axis": "steering",
                "target_front_wheel_angle_deg": float(target_deg),
                "mapped_carla_steer_cmd": float(mapped_cmd),
                **item,
            }
            for item in samples[: max(1, args.max_raw_series_per_axis // 20)]
        )
    errors = [float(item["abs_error_deg"]) for item in records]
    return {
        "measurements": records,
        "quality": {
            "mean_abs_error_deg": _median(errors, default=0.0) if not errors else float(sum(errors) / len(errors)),
            "median_abs_error_deg": _median(errors, default=0.0),
            "max_abs_error_deg": max(errors) if errors else 0.0,
            "sample_count": len(records),
        },
        "raw_series": raw_rows[: args.max_raw_series_per_axis],
    }


def validate_throttle(probe: CarlaProbe, cal: ActuatorCalibration, args: argparse.Namespace) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    entry_speeds = _parse_float_list(args.validation_throttle_entry_speeds)
    target_accels = _parse_float_list(args.validation_target_accels)
    for entry_speed in entry_speeds:
        for target_accel in target_accels:
            probe.reset_to_reference_pose(settle_sec=args.reset_settle_sec)
            if entry_speed > 0.05:
                probe.accelerate_to_speed(
                    target_speed_mps=float(entry_speed),
                    throttle=float(args.longitudinal_prep_throttle),
                    timeout_sec=float(args.longitudinal_prep_timeout_sec),
                )
            mapping = cal.throttle_mapping_for_accel(
                float(target_accel),
                speed_mps=float(entry_speed),
                target_accel_max_mps2=float(args.apollo_max_accel_mps2),
            )
            mapped_cmd = _optional_float(mapping.get("cmd"))
            if mapped_cmd is None:
                continue
            source = str(mapping.get("source", "") or "")
            source_counts[source] += 1
            samples = _sample_window(
                probe,
                throttle=float(mapped_cmd),
                brake=0.0,
                steer=0.0,
                sample_sec=float(args.validation_longitudinal_sample_sec),
            )
            if not samples:
                continue
            measured_accel = _effective_throttle_accel(samples, window_sec=float(args.validation_longitudinal_eval_sec))
            speed_at_eval = _value_at_elapsed(
                samples,
                float(args.validation_longitudinal_eval_sec),
                "speed_mps",
                default=0.0,
            )
            record = {
                "entry_speed_mps": float(entry_speed),
                "target_accel_mps2": float(target_accel),
                "mapped_throttle_cmd": float(mapped_cmd),
                "mapping_source": source,
                "measured_accel_mps2": float(measured_accel),
                "speed_at_eval_mps": float(speed_at_eval),
                "abs_error_mps2": abs(float(target_accel) - float(measured_accel)),
                "sample_count": len(samples),
            }
            records.append(record)
            raw_rows.extend(
                {
                    "axis": "throttle",
                    "entry_speed_mps": float(entry_speed),
                    "target_accel_mps2": float(target_accel),
                    "mapped_throttle_cmd": float(mapped_cmd),
                    "mapping_source": source,
                    **item,
                }
                for item in samples[: max(1, args.max_raw_series_per_axis // 30)]
            )
    errors = [float(item["abs_error_mps2"]) for item in records]
    return {
        "measurements": records,
        "mapping_source_counts": dict(source_counts),
        "quality": {
            "mean_abs_error_mps2": float(sum(errors) / len(errors)) if errors else 0.0,
            "median_abs_error_mps2": _median(errors, default=0.0),
            "max_abs_error_mps2": max(errors) if errors else 0.0,
            "sample_count": len(records),
        },
        "raw_series": raw_rows[: args.max_raw_series_per_axis],
    }


def validate_brake(probe: CarlaProbe, cal: ActuatorCalibration, args: argparse.Namespace) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    entry_speeds = _parse_float_list(args.validation_brake_entry_speeds)
    target_spec = str(args.validation_target_decels).strip().lower()
    fixed_target_decels = None if target_spec == "auto" else _parse_float_list(target_spec)
    requested_count = 0
    for entry_speed in entry_speeds:
        if fixed_target_decels is None:
            calibrated_range = cal.brake_decel_range(speed_mps=float(entry_speed))
            if calibrated_range is None:
                skipped.append(
                    {
                        "entry_speed_target_mps": float(entry_speed),
                        "entry_speed_actual_mps": None,
                        "target_decel_mps2": None,
                        "mapping_source": "missing_speed_bin",
                        "reason": "calibrated_target_range_unavailable",
                    }
                )
                continue
            lower, upper = calibrated_range
            target_decels = sorted(
                {
                    float(lower),
                    0.5 * (float(lower) + float(upper)),
                    float(upper),
                }
            )
        else:
            target_decels = fixed_target_decels
        for target_decel in target_decels:
            requested_count += 1
            probe.reset_to_reference_pose(settle_sec=args.reset_settle_sec)
            prep = probe.accelerate_to_speed(
                target_speed_mps=float(entry_speed),
                throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
                timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
            )
            pre_brake_state = probe.state()
            actual_entry_speed = _safe_float(
                pre_brake_state.get("speed_mps"),
                _safe_float(prep.get("final_speed_mps"), float(entry_speed)),
            )
            mapping = cal.brake_mapping_for_decel(
                float(target_decel),
                speed_mps=float(actual_entry_speed),
            )
            mapped_cmd = _optional_float(mapping.get("cmd"))
            if mapped_cmd is None:
                skipped.append(
                    {
                        "entry_speed_target_mps": float(entry_speed),
                        "entry_speed_actual_mps": float(actual_entry_speed),
                        "target_decel_mps2": float(target_decel),
                        "mapping_source": str(mapping.get("source", "") or ""),
                        "reason": "mapping_unavailable",
                    }
                )
                continue
            source = str(mapping.get("source", "") or "")
            source_counts[source] += 1
            samples = _sample_window(
                probe,
                throttle=0.0,
                brake=float(mapped_cmd),
                steer=0.0,
                sample_sec=float(args.validation_longitudinal_sample_sec),
            )
            if not samples:
                continue
            measured_decel = _effective_brake_decel(
                samples,
                start_sec=float(args.validation_brake_response_start_sec),
                end_sec=float(args.validation_brake_response_end_sec),
            )
            speed_drop = max(
                0.0,
                _value_at_elapsed(
                    samples,
                    float(args.validation_brake_response_start_sec),
                    "forward_speed_mps",
                    default=0.0,
                )
                - _value_at_elapsed(
                    samples,
                    float(args.validation_brake_response_end_sec),
                    "forward_speed_mps",
                    default=0.0,
                ),
            )
            record = {
                "entry_speed_target_mps": float(entry_speed),
                "entry_speed_actual_mps": float(actual_entry_speed),
                "entry_speed_reached": bool(prep.get("reached", False)),
                "target_decel_mps2": float(target_decel),
                "mapped_brake_cmd": float(mapped_cmd),
                "mapping_source": source,
                "mapping_calibrated_decel_mps2": mapping.get("calibrated_decel_mps2"),
                "measured_decel_mps2": float(measured_decel),
                "speed_drop_mps": float(speed_drop),
                "response_start_sec": float(args.validation_brake_response_start_sec),
                "response_end_sec": float(args.validation_brake_response_end_sec),
                "response_metric": "forward_speed_delta_over_fixed_window",
                "abs_error_mps2": abs(float(target_decel) - float(measured_decel)),
                "sample_count": len(samples),
            }
            records.append(record)
            raw_rows.extend(
                {
                    "axis": "brake",
                    "entry_speed_target_mps": float(entry_speed),
                    "entry_speed_actual_mps": float(actual_entry_speed),
                    "target_decel_mps2": float(target_decel),
                    "mapped_brake_cmd": float(mapped_cmd),
                    "mapping_source": source,
                    "mapping_calibrated_decel_mps2": mapping.get("calibrated_decel_mps2"),
                    **item,
                }
                for item in samples[: max(1, args.max_raw_series_per_axis // 30)]
            )
    errors = [float(item["abs_error_mps2"]) for item in records]
    return {
        "measurements": records,
        "skipped_requests": skipped,
        "mapping_source_counts": dict(source_counts),
        "quality": {
            "mean_abs_error_mps2": float(sum(errors) / len(errors)) if errors else 0.0,
            "median_abs_error_mps2": _median(errors, default=0.0),
            "max_abs_error_mps2": max(errors) if errors else 0.0,
            "sample_count": len(records),
            "requested_count": requested_count,
            "skipped_count": len(skipped),
            "covered_entry_speed_count": len(
                {float(item["entry_speed_target_mps"]) for item in records}
            ),
            "requested_entry_speed_count": len(entry_speeds),
        },
        "raw_series": raw_rows[: args.max_raw_series_per_axis],
    }


def validate_low_speed_brake_contract(
    probe: CarlaProbe,
    cal: ActuatorCalibration,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    sample_sec = float(args.validation_low_speed_sample_sec)
    eval_sec = float(args.validation_low_speed_eval_sec)
    stop_speed = float(args.validation_low_speed_stop_speed_mps)
    raw_rows: List[Dict[str, Any]] = []

    probe.reset_to_reference_pose(settle_sec=float(args.reset_settle_sec))
    hold_state = probe.state()
    hold_entry_speed = _motion_speed_mps(hold_state)
    hold_mapping = cal.brake_mapping_for_decel(
        float(args.validation_low_speed_hold_target_decel_mps2),
        speed_mps=float(hold_entry_speed),
    )
    hold_cmd = _optional_float(hold_mapping.get("cmd"))
    hold_samples = (
        _sample_window(
            probe,
            throttle=0.0,
            brake=float(hold_cmd),
            steer=0.0,
            sample_sec=sample_sec,
        )
        if hold_cmd is not None
        else []
    )
    hold_eval_rows = [
        item
        for item in hold_samples
        if _safe_float(item.get("elapsed_sec"), 0.0) <= eval_sec + 1e-9
    ]
    hold_max_speed = max(
        (_motion_speed_mps(item) for item in hold_eval_rows),
        default=float("inf"),
    )
    hold_pass = bool(
        str(hold_mapping.get("source", "")) == "physical_low_speed_hold"
        and hold_cmd is not None
        and hold_max_speed <= float(args.validation_low_speed_hold_max_speed_mps)
    )
    hold_record = {
        "state": "hold",
        "entry_speed_mps": float(hold_entry_speed),
        "target_decel_mps2": float(args.validation_low_speed_hold_target_decel_mps2),
        "mapped_brake_cmd": hold_cmd,
        "mapping_source": str(hold_mapping.get("source", "") or ""),
        "max_speed_mps": None if math.isinf(hold_max_speed) else float(hold_max_speed),
        "max_speed_threshold_mps": float(args.validation_low_speed_hold_max_speed_mps),
        "pass": hold_pass,
        "sample_count": len(hold_samples),
    }
    raw_rows.extend({"axis": "brake_low_speed", "state": "hold", **item} for item in hold_samples)

    probe.reset_to_reference_pose(settle_sec=float(args.reset_settle_sec))
    stop_prep = probe.accelerate_to_speed(
        target_speed_mps=float(args.validation_low_speed_stop_entry_speed_mps),
        throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
        timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
    )
    stop_state = probe.state()
    stop_entry_speed = _motion_speed_mps(stop_state)
    stop_mapping = cal.brake_mapping_for_decel(
        float(args.validation_low_speed_stop_target_decel_mps2),
        speed_mps=float(stop_entry_speed),
    )
    stop_cmd = _optional_float(stop_mapping.get("cmd"))
    stop_samples = (
        _sample_window(
            probe,
            throttle=0.0,
            brake=float(stop_cmd),
            steer=0.0,
            sample_sec=sample_sec,
        )
        if stop_cmd is not None
        else []
    )
    stop_time = _time_to_motion_threshold(stop_samples, threshold_mps=stop_speed)
    stop_residual_speed = (
        _motion_speed_at_elapsed(stop_samples, eval_sec, default=0.0)
        if stop_samples
        else None
    )
    stop_pass = bool(
        str(stop_mapping.get("source", "")) == "physical_low_speed_stop"
        and stop_cmd is not None
        and stop_time is not None
        and float(stop_time) <= eval_sec
        and stop_residual_speed is not None
        and float(stop_residual_speed) <= stop_speed
    )
    stop_record = {
        "state": "stop",
        "entry_speed_target_mps": float(args.validation_low_speed_stop_entry_speed_mps),
        "entry_speed_actual_mps": float(stop_entry_speed),
        "entry_speed_reached": bool(stop_prep.get("reached", False)),
        "target_decel_mps2": float(args.validation_low_speed_stop_target_decel_mps2),
        "mapped_brake_cmd": stop_cmd,
        "mapping_source": str(stop_mapping.get("source", "") or ""),
        "stop_time_sec": stop_time,
        "residual_speed_mps": stop_residual_speed,
        "stop_speed_threshold_mps": stop_speed,
        "eval_sec": eval_sec,
        "pass": stop_pass,
        "sample_count": len(stop_samples),
    }
    raw_rows.extend({"axis": "brake_low_speed", "state": "stop", **item} for item in stop_samples)

    probe.reset_to_reference_pose(settle_sec=float(args.reset_settle_sec))
    rolling_prep = probe.accelerate_to_speed(
        target_speed_mps=float(args.validation_low_speed_rolling_entry_speed_mps),
        throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
        timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
    )
    rolling_state = probe.state()
    rolling_entry_speed = _motion_speed_mps(rolling_state)
    rolling_target_decel = float(args.validation_low_speed_rolling_target_decel_mps2)
    rolling_mapping = cal.brake_mapping_for_decel(
        rolling_target_decel,
        speed_mps=float(rolling_entry_speed),
    )
    rolling_cmd = _optional_float(rolling_mapping.get("cmd"))
    rolling_samples = (
        _sample_window(
            probe,
            throttle=0.0,
            brake=float(rolling_cmd),
            steer=0.0,
            sample_sec=sample_sec,
        )
        if rolling_cmd is not None
        else []
    )
    rolling_start_speed = (
        _motion_speed_mps(rolling_samples[0]) if rolling_samples else None
    )
    rolling_eval_speed = (
        _motion_speed_at_elapsed(
            rolling_samples,
            float(args.validation_low_speed_rolling_eval_sec),
            default=0.0,
        )
        if rolling_samples
        else None
    )
    rolling_drop = (
        max(0.0, float(rolling_start_speed) - float(rolling_eval_speed))
        if rolling_start_speed is not None and rolling_eval_speed is not None
        else None
    )
    rolling_target_drop = (
        rolling_target_decel * float(args.validation_low_speed_rolling_eval_sec)
    )
    rolling_drop_error = (
        abs(rolling_target_drop - float(rolling_drop))
        if rolling_drop is not None
        else None
    )
    rolling_stop_time = _time_to_motion_threshold(
        rolling_samples,
        threshold_mps=stop_speed,
    )
    rolling_pass = bool(
        str(rolling_mapping.get("source", "")).startswith("physical_low_speed_rolling_")
        and rolling_cmd is not None
        and rolling_eval_speed is not None
        and float(rolling_eval_speed) > stop_speed
        and (
            rolling_stop_time is None
            or float(rolling_stop_time) > float(args.validation_low_speed_rolling_eval_sec)
        )
        and rolling_drop_error is not None
        and float(rolling_drop_error)
        <= float(args.validation_low_speed_rolling_max_drop_error_mps)
    )
    rolling_record = {
        "state": "rolling",
        "entry_speed_target_mps": float(args.validation_low_speed_rolling_entry_speed_mps),
        "entry_speed_actual_mps": float(rolling_entry_speed),
        "entry_speed_reached": bool(rolling_prep.get("reached", False)),
        "target_decel_mps2": rolling_target_decel,
        "target_speed_drop_mps": float(rolling_target_drop),
        "measured_speed_drop_mps": rolling_drop,
        "speed_drop_error_mps": rolling_drop_error,
        "max_speed_drop_error_mps": float(args.validation_low_speed_rolling_max_drop_error_mps),
        "speed_at_eval_mps": rolling_eval_speed,
        "mapped_brake_cmd": rolling_cmd,
        "mapping_source": str(rolling_mapping.get("source", "") or ""),
        "stop_time_sec": rolling_stop_time,
        "eval_sec": float(args.validation_low_speed_rolling_eval_sec),
        "pass": rolling_pass,
        "sample_count": len(rolling_samples),
    }
    raw_rows.extend({"axis": "brake_low_speed", "state": "rolling", **item} for item in rolling_samples)

    measurements = [hold_record, stop_record, rolling_record]
    passed_count = sum(1 for item in measurements if item["pass"])
    return {
        "measurements": measurements,
        "quality": {
            "pass": passed_count == len(measurements),
            "passed_state_count": passed_count,
            "required_state_count": len(measurements),
        },
        "raw_series": raw_rows[: args.max_raw_series_per_axis],
    }


def _write_rows(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _axis_passes_quality(
    axis: str,
    quality: Dict[str, Any],
    threshold: Dict[str, Any],
) -> bool:
    passed = bool(
        int(quality.get("sample_count", 0) or 0) > 0
        and float(quality.get(str(threshold["metric"]), float("inf")))
        <= float(threshold["max"])
    )
    if axis == "brake":
        passed = bool(
            passed
            and float(quality.get(str(threshold["max_error_metric"]), float("inf")))
            <= float(threshold["max_error_max"])
            and int(quality.get("covered_entry_speed_count", 0) or 0)
            == int(quality.get("requested_entry_speed_count", 0) or 0)
        )
    return passed


def _write_markdown_report(path: Path, *, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    axes = tuple(str(item) for item in payload.get("axes_requested", []))
    steering = (payload.get("steering") or {}).get("quality", {}) or {}
    throttle = (payload.get("throttle") or {}).get("quality", {}) or {}
    brake = (payload.get("brake") or {}).get("quality", {}) or {}
    lines = [
        "# Apollo-CARLA 执行器跟踪验证报告",
        "",
        "这份报告验证的是固定场景下的执行器跟踪误差，不包含 Apollo 规划/定位/参考线因素。",
        "",
        "## 场景",
        "",
        f"- 地图: `{payload.get('map', '')}`",
        f"- ego actor_id: `{payload.get('ego_actor_id', '')}`",
        f"- calibration: `{payload.get('calibration_path', '')}`",
        f"- axes: `{','.join(axes)}`",
        "",
        "## 结果",
        "",
    ]
    if "steering" in axes:
        lines.extend(
            [
                f"- steering 平均绝对误差: `{steering.get('mean_abs_error_deg', 0.0):.4f} deg`",
                f"- steering 中位数绝对误差: `{steering.get('median_abs_error_deg', 0.0):.4f} deg`",
            ]
        )
    if "throttle" in axes:
        lines.append(
            f"- throttle 平均绝对误差: `{throttle.get('mean_abs_error_mps2', 0.0):.4f} m/s^2`"
        )
    if "brake" in axes:
        lines.extend(
            [
                f"- brake main tracking: `{(payload.get('brake') or {}).get('main_tracking_status', 'evaluated')}`",
            ]
        )
        if (payload.get("brake") or {}).get("main_tracking_status") != "not_applicable":
            lines.extend(
                [
                    f"- brake 平均绝对误差: `{brake.get('mean_abs_error_mps2', 0.0):.4f} m/s^2`",
                    f"- brake speed-bin 覆盖: `{brake.get('covered_entry_speed_count', 0)}/{brake.get('requested_entry_speed_count', 0)}`",
                ]
            )
        low_speed_quality = ((payload.get("brake") or {}).get("low_speed_contract") or {}).get("quality", {})
        if low_speed_quality:
            lines.append(
                "- brake low-speed state contract: "
                f"`{low_speed_quality.get('passed_state_count', 0)}/"
                f"{low_speed_quality.get('required_state_count', 0)}`"
            )
    lines.extend(
        [
            f"- overall pass: `{bool(payload.get('pass', False))}`",
            "",
            "## 说明",
            "",
            "- 这里的误差是 `target physical quantity - measured physical response`。",
            "- 本报告是 CARLA-direct 独立跟踪证据，Apollo 不在环，不能作为 Phase 1 或 mapping promotion 结论。",
        ]
    )
    if "steering" in axes:
        lines.append("- steering 比较的是 `target_front_wheel_angle_deg` 和 `measured_steer_deg`。")
    if "throttle" in axes:
        lines.append("- throttle 比较的是 `target_accel_mps2` 和短窗口内的实测前向加速度。")
    if "brake" in axes:
        lines.append(
            "- brake 使用固定响应窗口内的前向速度差比较 `target_decel_mps2` 与实测总减速度。"
        )
    lines.append("")
    path.write_text("\n".join(lines))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Validate Apollo↔CARLA physical actuator tracking in a fixed scene")
    ap.add_argument("--config", default="configs/io/examples/apollo_actuator_tracking_validation.yaml")
    ap.add_argument(
        "--calibration-file",
        default="artifacts/actuator_calibration_library/e969a34a5f43fd25/carla_actuator_calibration.json",
    )
    ap.add_argument("--output-dir", default="runs/apollo_carla_tracking_validation")
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--ego-role-name", default="hero")
    ap.add_argument("--timeout-sec", type=float, default=10.0)
    ap.add_argument("--ego-discovery-timeout-sec", type=float, default=30.0)
    ap.add_argument("--metadata-timeout-sec", type=float, default=240.0)
    ap.add_argument("--skip-start-scene", action="store_true")
    ap.add_argument(
        "--axes",
        default="steering,throttle,brake",
        help="comma-separated validation axes; supported: steering,throttle,brake",
    )
    ap.add_argument(
        "--spawn-vehicle",
        action="store_true",
        help="spawn and later destroy an isolated validation vehicle; refuses an occupied world",
    )
    ap.add_argument("--vehicle-blueprint", default="vehicle.lincoln.mkz_2020")
    ap.add_argument("--fixed-delta-seconds", type=float, default=0.05)
    ap.add_argument("--spawn-offset-m", type=float, default=5.0)
    ap.add_argument("--max-raw-series-per-axis", type=int, default=600)
    ap.add_argument("--apollo-max-steer-angle-deg", type=float, default=8.203)
    ap.add_argument("--apollo-max-accel-mps2", type=float, default=4.0)
    ap.add_argument("--validation-steering-probe-throttle", type=float, default=0.25)
    ap.add_argument("--validation-steering-settle-sec", type=float, default=1.0)
    ap.add_argument("--validation-steering-sample-sec", type=float, default=1.2)
    ap.add_argument("--validation-longitudinal-sample-sec", type=float, default=1.2)
    ap.add_argument("--validation-longitudinal-eval-sec", type=float, default=0.6)
    ap.add_argument("--validation-throttle-entry-speeds", default="0.0,2.0,4.0,6.0,10.0")
    ap.add_argument("--validation-target-accels", default="0.5,1.0,1.5,2.0,2.5,3.0,3.5")
    ap.add_argument("--validation-brake-entry-speeds", default="2.0,4.0,6.0,10.0")
    ap.add_argument(
        "--validation-target-decels",
        default="0.3,0.5,0.8,1.0,1.5,2.0,3.0",
        help="comma-separated targets or 'auto' for min/mid/max of each calibrated speed bin",
    )
    ap.add_argument("--validation-brake-response-start-sec", type=float, default=0.2)
    ap.add_argument("--validation-brake-response-end-sec", type=float, default=0.4)
    ap.add_argument("--include-low-speed-brake-contract", action="store_true")
    ap.add_argument("--low-speed-brake-contract-only", action="store_true")
    ap.add_argument("--validation-low-speed-sample-sec", type=float, default=1.2)
    ap.add_argument("--validation-low-speed-eval-sec", type=float, default=0.8)
    ap.add_argument("--validation-low-speed-stop-speed-mps", type=float, default=0.05)
    ap.add_argument("--validation-low-speed-hold-max-speed-mps", type=float, default=0.05)
    ap.add_argument("--validation-low-speed-hold-target-decel-mps2", type=float, default=1.0)
    ap.add_argument("--validation-low-speed-stop-entry-speed-mps", type=float, default=0.8)
    ap.add_argument("--validation-low-speed-stop-target-decel-mps2", type=float, default=1.0)
    ap.add_argument("--validation-low-speed-rolling-entry-speed-mps", type=float, default=1.5)
    ap.add_argument("--validation-low-speed-rolling-target-decel-mps2", type=float, default=1.0)
    ap.add_argument("--validation-low-speed-rolling-eval-sec", type=float, default=0.3)
    ap.add_argument("--validation-low-speed-rolling-max-drop-error-mps", type=float, default=0.25)
    ap.add_argument("--reset-settle-sec", type=float, default=0.8)
    ap.add_argument("--longitudinal-prep-throttle", type=float, default=0.75)
    ap.add_argument("--longitudinal-prep-timeout-sec", type=float, default=18.0)
    ap.add_argument("--brake-prep-throttle", type=float, default=0.45)
    ap.add_argument("--brake-prep-timeout-sec", type=float, default=8.0)
    return ap.parse_args(argv)


def main() -> int:
    args = parse_args()
    axes = _parse_axes(args.axes)
    if args.spawn_vehicle and not args.skip_start_scene:
        raise ValueError("--spawn-vehicle requires --skip-start-scene")
    if args.spawn_vehicle and float(args.fixed_delta_seconds) <= 0.0:
        raise ValueError("--fixed-delta-seconds must be positive")
    if "brake" in axes and not (
        0.0 <= float(args.validation_brake_response_start_sec)
        < float(args.validation_brake_response_end_sec)
        <= float(args.validation_longitudinal_sample_sec)
    ):
        raise ValueError(
            "brake response window must satisfy "
            "0 <= start < end <= validation_longitudinal_sample_sec"
        )
    if args.include_low_speed_brake_contract and "brake" not in axes:
        raise ValueError("--include-low-speed-brake-contract requires --axes brake")
    if args.low_speed_brake_contract_only and not args.include_low_speed_brake_contract:
        raise ValueError(
            "--low-speed-brake-contract-only requires --include-low-speed-brake-contract"
        )
    if args.include_low_speed_brake_contract and not (
        0.0 < float(args.validation_low_speed_rolling_eval_sec)
        <= float(args.validation_low_speed_sample_sec)
        and 0.0 < float(args.validation_low_speed_eval_sec)
        <= float(args.validation_low_speed_sample_sec)
    ):
        raise ValueError("low-speed eval windows must be positive and within sample_sec")
    load_actuator_calibration, CarlaProbe, _ = _load_runtime_modules()
    output_dir = Path(args.output_dir).expanduser()
    if not output_dir.is_absolute():
        output_dir = (Path.cwd() / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir = output_dir / "logs"
    scene_proc: Optional[subprocess.Popen[str]] = None
    scene_run_dir = output_dir / "scene_run"
    actor_id: Optional[int] = None
    metadata: Dict[str, Any] = {}
    spawned_actor: Any = None
    spawned_world: Any = None
    original_settings: Any = None
    try:
        if not args.skip_start_scene:
            config_path = Path(args.config).expanduser()
            if not config_path.is_absolute():
                config_path = (REPO_ROOT / config_path).resolve()
            cmd = [
                sys.executable,
                "-m",
                "carla_testbed",
                "run",
                "--config",
                str(config_path),
                "--run-dir",
                str(scene_run_dir),
                "--no-healthcheck",
            ]
            _info(f"[scene] start fixed validation scene: {config_path}")
            scene_proc = _run_child(cmd, cwd=REPO_ROOT, log_path=log_dir / "scene_run.log")
            resolved_run_dir, metadata = _wait_for_metadata(scene_run_dir, timeout_sec=float(args.metadata_timeout_sec))
            scene_run_dir = resolved_run_dir
            actor_id = int(metadata.get("ego_actor_id", 0) or 0) or None
            _info(f"[scene] ready actor_id={actor_id} run_dir={scene_run_dir}")
        cal_path = Path(args.calibration_file).expanduser()
        if not cal_path.is_absolute():
            cal_path = (REPO_ROOT / cal_path).resolve()
        calibration = load_actuator_calibration(cal_path)
        if args.spawn_vehicle:
            import carla

            client = carla.Client(args.carla_host, int(args.carla_port))
            client.set_timeout(float(args.timeout_sec))
            spawned_world = client.get_world()
            existing = list(spawned_world.get_actors().filter("vehicle.*"))
            if existing:
                raise RuntimeError(
                    "refusing to spawn actuator-tracking vehicle while CARLA vehicles exist: "
                    + ",".join(str(item.id) for item in existing)
                )
            original_settings = spawned_world.get_settings()
            settings = spawned_world.get_settings()
            settings.synchronous_mode = True
            settings.fixed_delta_seconds = float(args.fixed_delta_seconds)
            spawned_world.apply_settings(settings)
            blueprints = spawned_world.get_blueprint_library().filter(args.vehicle_blueprint)
            if not blueprints:
                raise RuntimeError(f"vehicle blueprint unavailable: {args.vehicle_blueprint}")
            blueprint = blueprints[0]
            role_name = "diagnostic_actuator_tracking"
            if blueprint.has_attribute("role_name"):
                blueprint.set_attribute("role_name", role_name)
            spawn_points = spawned_world.get_map().get_spawn_points()
            if not spawn_points:
                raise RuntimeError("current CARLA map has no spawn points")
            spawn_transform = spawn_points[0]
            spawn_forward = spawn_transform.get_forward_vector()
            spawn_transform.location.x += float(args.spawn_offset_m) * float(spawn_forward.x)
            spawn_transform.location.y += float(args.spawn_offset_m) * float(spawn_forward.y)
            spawn_transform.location.z += 0.1
            spawned_actor = spawned_world.try_spawn_actor(blueprint, spawn_transform)
            if spawned_actor is None:
                raise RuntimeError("failed to spawn diagnostic actuator-tracking vehicle")
            spawned_world.tick()
            actor_id = int(spawned_actor.id)
            metadata["map"] = str(spawned_world.get_map().name)
            probe = CarlaProbe(
                host=args.carla_host,
                port=args.carla_port,
                ego_role_name=role_name,
                timeout_sec=args.timeout_sec,
                actor_id=actor_id,
                ego_discovery_timeout_sec=2.0,
                ego_discovery_poll_sec=0.05,
            )
            probe.world = spawned_world
            probe.ego = spawned_actor
            probe.sync_mode = True
            probe.step_sec = float(args.fixed_delta_seconds)
            probe.reference_transform = spawned_actor.get_transform()
        else:
            probe = CarlaProbe(
                host=args.carla_host,
                port=args.carla_port,
                ego_role_name=args.ego_role_name,
                timeout_sec=args.timeout_sec,
                actor_id=actor_id,
                ego_discovery_timeout_sec=args.ego_discovery_timeout_sec,
                ego_discovery_poll_sec=1.0,
            )
        if actor_id is None:
            actor_id = int(getattr(probe.vehicle(), "id", 0) or 0) or None
        results: Dict[str, Dict[str, Any]] = {}
        if "steering" in axes:
            _info("[validate] steering")
            results["steering"] = validate_steering(probe, calibration, args)
        if "throttle" in axes:
            _info("[validate] throttle")
            results["throttle"] = validate_throttle(probe, calibration, args)
        if "brake" in axes:
            if args.low_speed_brake_contract_only:
                results["brake"] = {
                    "measurements": [],
                    "skipped_requests": [],
                    "mapping_source_counts": {},
                    "quality": {},
                    "raw_series": [],
                    "main_tracking_status": "not_applicable",
                }
            else:
                _info("[validate] brake")
                results["brake"] = validate_brake(probe, calibration, args)
                results["brake"]["main_tracking_status"] = "evaluated"
            if args.include_low_speed_brake_contract:
                _info("[validate] brake low-speed hold/stop/rolling contract")
                low_speed = validate_low_speed_brake_contract(
                    probe,
                    calibration,
                    args,
                )
                results["brake"]["low_speed_contract"] = low_speed
                results["brake"]["raw_series"].extend(low_speed["raw_series"])
        thresholds = {
            "steering": {"metric": "mean_abs_error_deg", "max": 1.2},
            "throttle": {"metric": "mean_abs_error_mps2", "max": 0.8},
            "brake": {
                "metric": "mean_abs_error_mps2",
                "max": 1.0,
                "max_error_metric": "max_abs_error_mps2",
                "max_error_max": 1.0,
            },
        }
        axis_pass: Dict[str, bool] = {}
        for axis in axes:
            if axis == "brake" and args.low_speed_brake_contract_only:
                axis_pass[axis] = True
            else:
                quality = (results.get(axis, {}).get("quality") or {})
                threshold = thresholds[axis]
                axis_pass[axis] = _axis_passes_quality(axis, quality, threshold)
        if args.include_low_speed_brake_contract:
            low_speed_quality = (
                results.get("brake", {}).get("low_speed_contract", {}).get("quality", {})
            )
            axis_pass["brake"] = bool(
                axis_pass.get("brake", False)
                and low_speed_quality.get("pass", False)
            )
        payload = {
            "schema_version": 1,
            "generated_at_unix_sec": time.time(),
            "map": str((metadata.get("map") or metadata.get("carla_map") or "Town01")),
            "ego_actor_id": int(actor_id or 0),
            "scene_run_dir": str(scene_run_dir),
            "calibration_path": str(cal_path),
            "calibration_status": calibration.status(),
            "axes_requested": list(axes),
            "vehicle": probe.vehicle_characteristics(),
            "quality": {
                "thresholds": thresholds,
                "axis_pass": axis_pass,
            },
            "claim_boundary": {
                "evidence_owner": "diagnostic_carla_direct",
                "apollo_in_loop": False,
                "independent_tracking_validation": True,
                "phase1_promotion_allowed": False,
                "mapping_promotion_allowed": False,
            },
            **results,
        }
        payload["pass"] = bool(all(axis_pass.values()))
        artifacts = output_dir / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "actuator_tracking_validation.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        _write_markdown_report(artifacts / "actuator_tracking_validation_report.md", payload=payload)
        for axis in axes:
            _write_rows(
                artifacts / f"{axis}_tracking_measurements.csv",
                results.get(axis, {}).get("measurements", []),
            )
        if args.include_low_speed_brake_contract:
            _write_rows(
                artifacts / "brake_low_speed_contract_measurements.csv",
                results.get("brake", {})
                .get("low_speed_contract", {})
                .get("measurements", []),
            )
        _write_rows(
            artifacts / "actuator_tracking_raw_series.csv",
            [
                row
                for axis in axes
                for row in list(results.get(axis, {}).get("raw_series", []))
            ],
        )
        _info(f"[done] wrote {artifacts / 'actuator_tracking_validation.json'}")
        return 0
    except Exception as exc:
        _warn(f"validation failed: {exc}")
        if scene_run_dir.exists():
            tail = _tail_text(scene_run_dir / "logs" / "carla_server.log")
            if tail:
                _warn("[carla_server.log tail]\n" + tail)
        return 1
    finally:
        if spawned_actor is not None and spawned_world is not None:
            try:
                import carla

                spawned_actor.apply_control(
                    carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0)
                )
                spawned_world.tick()
            except Exception:
                pass
            try:
                spawned_actor.destroy()
            except Exception:
                pass
        if spawned_world is not None and original_settings is not None:
            try:
                spawned_world.apply_settings(original_settings)
            except Exception:
                pass
        _stop_child(scene_proc)


if __name__ == "__main__":
    raise SystemExit(main())
