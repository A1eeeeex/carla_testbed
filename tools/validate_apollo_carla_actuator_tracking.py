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
    samples: List[Dict[str, float]] = []
    deadline = time.monotonic() + max(0.0, float(sample_sec))
    while time.monotonic() < deadline:
        probe.apply(throttle=throttle, brake=brake, steer=steer)
        if probe.sync_mode:
            probe.world.tick()
        else:
            time.sleep(probe.step_sec)
        item = probe.state()
        item["elapsed_sec"] = float(sample_sec - max(0.0, deadline - time.monotonic()))
        samples.append(item)
    return samples


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


def _effective_brake_decel(samples: Sequence[Dict[str, float]]) -> float:
    active = []
    for item in samples:
        speed = _safe_float(item.get("speed_mps"), 0.0)
        elapsed = _safe_float(item.get("elapsed_sec"), 0.0)
        decel = max(0.0, -_safe_float(item.get("forward_accel_mps2"), 0.0))
        if speed >= 0.5 or elapsed <= 1.0:
            active.append(decel)
    if not active:
        active = [max(0.0, -_safe_float(item.get("forward_accel_mps2"), 0.0)) for item in samples]
    return _percentile(active, 0.85, default=0.0)


def _value_at_elapsed(samples: Sequence[Dict[str, float]], elapsed_sec: float, key: str, *, default: float = 0.0) -> float:
    if not samples:
        return float(default)
    target = float(elapsed_sec)
    for item in samples:
        if _safe_float(item.get("elapsed_sec"), 0.0) >= target:
            return _safe_float(item.get(key), default)
    return _safe_float(samples[-1].get(key), default)


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
            mapped_cmd = _safe_float(mapping.get("cmd"), None)
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
    source_counts: Counter[str] = Counter()
    entry_speeds = _parse_float_list(args.validation_brake_entry_speeds)
    target_decels = _parse_float_list(args.validation_target_decels)
    for entry_speed in entry_speeds:
        for target_decel in target_decels:
            probe.reset_to_reference_pose(settle_sec=args.reset_settle_sec)
            probe.accelerate_to_speed(
                target_speed_mps=float(entry_speed),
                throttle=max(float(args.brake_prep_throttle), float(args.longitudinal_prep_throttle)),
                timeout_sec=max(float(args.brake_prep_timeout_sec), float(args.longitudinal_prep_timeout_sec)),
            )
            mapping = cal.brake_mapping_for_decel(float(target_decel), speed_mps=float(entry_speed))
            mapped_cmd = _safe_float(mapping.get("cmd"), None)
            if mapped_cmd is None:
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
            measured_decel = _effective_brake_decel(samples)
            speed_drop = max(
                0.0,
                _safe_float(samples[0].get("speed_mps"), 0.0)
                - _value_at_elapsed(samples, float(args.validation_longitudinal_eval_sec), "speed_mps", default=0.0),
            )
            record = {
                "entry_speed_mps": float(entry_speed),
                "target_decel_mps2": float(target_decel),
                "mapped_brake_cmd": float(mapped_cmd),
                "mapping_source": source,
                "measured_decel_mps2": float(measured_decel),
                "speed_drop_mps": float(speed_drop),
                "abs_error_mps2": abs(float(target_decel) - float(measured_decel)),
                "sample_count": len(samples),
            }
            records.append(record)
            raw_rows.extend(
                {
                    "axis": "brake",
                    "entry_speed_mps": float(entry_speed),
                    "target_decel_mps2": float(target_decel),
                    "mapped_brake_cmd": float(mapped_cmd),
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


def _write_markdown_report(path: Path, *, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
        "",
        "## 结果",
        "",
        f"- steering 平均绝对误差: `{steering.get('mean_abs_error_deg', 0.0):.4f} deg`",
        f"- steering 中位数绝对误差: `{steering.get('median_abs_error_deg', 0.0):.4f} deg`",
        f"- throttle 平均绝对误差: `{throttle.get('mean_abs_error_mps2', 0.0):.4f} m/s^2`",
        f"- brake 平均绝对误差: `{brake.get('mean_abs_error_mps2', 0.0):.4f} m/s^2`",
        "",
        "## 说明",
        "",
        "- 这里的误差是 `target physical quantity - measured physical response`。",
        "- steering 比较的是 `target_front_wheel_angle_deg` 和 `measured_steer_deg`。",
        "- throttle 比较的是 `target_accel_mps2` 和短窗口内的实测前向加速度。",
        "- brake 比较的是 `target_decel_mps2` 和短窗口内的实测有效减速度。",
        "",
    ]
    path.write_text("\n".join(lines))


def parse_args() -> argparse.Namespace:
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
    ap.add_argument("--validation-target-decels", default="0.3,0.5,0.8,1.0,1.5,2.0,3.0")
    ap.add_argument("--reset-settle-sec", type=float, default=0.8)
    ap.add_argument("--longitudinal-prep-throttle", type=float, default=0.75)
    ap.add_argument("--longitudinal-prep-timeout-sec", type=float, default=18.0)
    ap.add_argument("--brake-prep-throttle", type=float, default=0.45)
    ap.add_argument("--brake-prep-timeout-sec", type=float, default=8.0)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
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
        _info("[validate] steering")
        steering = validate_steering(probe, calibration, args)
        _info("[validate] throttle")
        throttle = validate_throttle(probe, calibration, args)
        _info("[validate] brake")
        brake = validate_brake(probe, calibration, args)
        payload = {
            "schema_version": 1,
            "generated_at_unix_sec": time.time(),
            "map": str((metadata.get("map") or metadata.get("carla_map") or "Town01")),
            "ego_actor_id": int(actor_id or 0),
            "scene_run_dir": str(scene_run_dir),
            "calibration_path": str(cal_path),
            "steering": steering,
            "throttle": throttle,
            "brake": brake,
        }
        payload["pass"] = bool(
            float(((steering.get("quality") or {}).get("mean_abs_error_deg", 0.0) or 0.0)) <= 1.2
            and float(((throttle.get("quality") or {}).get("mean_abs_error_mps2", 0.0) or 0.0)) <= 0.8
            and float(((brake.get("quality") or {}).get("mean_abs_error_mps2", 0.0) or 0.0)) <= 1.0
        )
        artifacts = output_dir / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "actuator_tracking_validation.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        _write_markdown_report(artifacts / "actuator_tracking_validation_report.md", payload=payload)
        _write_rows(artifacts / "steering_tracking_measurements.csv", steering.get("measurements", []))
        _write_rows(artifacts / "throttle_tracking_measurements.csv", throttle.get("measurements", []))
        _write_rows(artifacts / "brake_tracking_measurements.csv", brake.get("measurements", []))
        _write_rows(
            artifacts / "actuator_tracking_raw_series.csv",
            list(steering.get("raw_series", [])) + list(throttle.get("raw_series", [])) + list(brake.get("raw_series", [])),
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
        _stop_child(scene_proc)


if __name__ == "__main__":
    raise SystemExit(main())
