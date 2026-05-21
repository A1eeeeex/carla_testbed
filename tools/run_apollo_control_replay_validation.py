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
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if TYPE_CHECKING:
    from tools.calibrate_carla_actuators import CarlaProbe


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_runtime_modules():
    from tools.apollo10_cyber_bridge.actuator_mapping import load_actuator_calibration
    from tools.calibrate_carla_actuators import CarlaProbe
    from tools.calibration_pipeline_common import (
        coverage_meets_minimum,
        evaluate_capture_validity,
        load_policy_config,
        summarize_capture_validity,
        write_capture_validity_artifacts,
        write_policy_artifacts,
    )
    return (
        load_actuator_calibration,
        CarlaProbe,
        coverage_meets_minimum,
        evaluate_capture_validity,
        load_policy_config,
        summarize_capture_validity,
        write_capture_validity_artifacts,
        write_policy_artifacts,
    )


def _load_yaml(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text()) or {}
    return payload if isinstance(payload, dict) else {}


def _deep_update(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _materialize_config(config_path: Path, *, generated_dir: Path, output_name: str) -> Path:
    cfg = _load_yaml(config_path)
    suite_cfg = cfg.get("suite") if isinstance(cfg.get("suite"), dict) else {}
    base_config = str((suite_cfg or {}).get("base_config", "") or "").strip()
    if base_config:
        base_path = Path(base_config)
        if not base_path.is_absolute():
            base_path = (REPO_ROOT / base_path).resolve()
        merged = _load_yaml(base_path)
        merged = _deep_update(merged, cfg)
        cfg = merged
    generated_dir.mkdir(parents=True, exist_ok=True)
    out_path = generated_dir / output_name
    out_path.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return out_path


def _force_start_carla_for_validation(config_path: Path) -> None:
    cfg = _load_yaml(config_path)
    runtime = cfg.setdefault("runtime", {})
    if not isinstance(runtime, dict):
        runtime = {}
        cfg["runtime"] = runtime
    carla_cfg = runtime.setdefault("carla", {})
    if not isinstance(carla_cfg, dict):
        carla_cfg = {}
        runtime["carla"] = carla_cfg
    carla_cfg["start"] = True
    config_path.write_text(yaml.safe_dump(cfg, sort_keys=False))


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
    return float(out)


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


def _normalize_field_priority(raw: Any, *, default: Sequence[str], preferred: Any = None) -> List[str]:
    items: List[str] = []
    preferred_str = str(preferred or "").strip()
    if preferred_str:
        items.append(preferred_str)
    if isinstance(raw, (list, tuple)):
        items.extend(str(item).strip() for item in raw if str(item).strip())
    else:
        raw_str = str(raw or "").strip()
        if raw_str:
            items.extend(item.strip() for item in raw_str.split(",") if item.strip())
    items.extend(str(item).strip() for item in default if str(item).strip())
    out: List[str] = []
    seen = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _tail_text(path: Path, *, max_lines: int = 60) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(errors="replace").splitlines()
    except Exception:
        return ""
    return "\n".join(lines[-max_lines:])


def _find_existing_redirect(run_dir: Path) -> Optional[Path]:
    for name in ("RUN_DIR_REDIRECT.txt", "LATEST.txt"):
        path = run_dir / name
        if path.exists():
            try:
                candidate = Path(path.read_text().strip())
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


def _run_child(cmd: Sequence[str], *, cwd: Path, log_path: Path) -> subprocess.Popen[str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fp = log_path.open("w")
    proc = subprocess.Popen(
        list(cmd),
        cwd=str(cwd),
        stdout=fp,
        stderr=subprocess.STDOUT,
        text=True,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )
    proc._codex_log_fp = fp  # type: ignore[attr-defined]
    return proc


def _stop_child(proc: Optional[subprocess.Popen[str]], *, grace_sec: float = 10.0) -> None:
    if proc is None:
        return
    fp = getattr(proc, "_codex_log_fp", None)
    if proc.poll() is None:
        try:
            if hasattr(os, "killpg") and hasattr(os, "getpgid"):
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)
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
                if hasattr(os, "killpg") and hasattr(os, "getpgid"):
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                else:
                    proc.kill()
            except Exception:
                pass
    if fp is not None:
        try:
            fp.close()
        except Exception:
            pass


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


def _wait_for_capture_artifact(
    run_dir: Path,
    *,
    timeout_sec: float,
    launched_at_unix_sec: float,
    previous_resolved_dir: Optional[Path] = None,
) -> Tuple[Path, Path]:
    deadline = time.monotonic() + max(1.0, float(timeout_sec))
    while time.monotonic() < deadline:
        resolved = _resolve_run_dir(run_dir)
        path = resolved / "artifacts" / "apollo_control_raw.jsonl"
        if path.exists() and path.stat().st_size > 0:
            try:
                mtime = path.stat().st_mtime
            except OSError:
                mtime = 0.0
            is_new_dir = previous_resolved_dir is None or resolved != previous_resolved_dir
            is_fresh_file = mtime >= max(0.0, float(launched_at_unix_sec) - 1.0)
            if is_new_dir or is_fresh_file:
                return resolved, path
        time.sleep(1.0)
    raise RuntimeError(f"apollo_control_raw.jsonl not ready under {run_dir}")


def _select_steering_field(raw_fields: Dict[str, Any], *, field_order: Sequence[str]) -> Tuple[str, float]:
    for key in field_order:
        if key in raw_fields:
            return key, _safe_float(raw_fields.get(key), 0.0)
    return "missing", 0.0


def _derive_longitudinal_targets(
    *,
    raw_fields: Dict[str, Any],
    raw_throttle: float,
    raw_brake: float,
    acceleration_field_priority: Sequence[str],
    use_top_level_acceleration: bool,
    use_lon_debug: bool,
    apollo_max_accel_mps2: float,
    apollo_max_decel_mps2: float,
) -> Dict[str, Any]:
    accel_candidates = {
        "acceleration": _safe_float(raw_fields.get("acceleration"), float("nan")),
        "debug_simple_lon_acceleration_cmd": _safe_float(
            raw_fields.get("debug_simple_lon_acceleration_cmd"), float("nan")
        ),
        "debug_simple_lon_acceleration_lookup": _safe_float(
            raw_fields.get("debug_simple_lon_acceleration_lookup"), float("nan")
        ),
    }
    target_accel = 0.0
    target_decel = 0.0
    accel_source = "percentage_scaled"
    decel_source = "percentage_scaled"
    selected_signed_field = ""
    for key in acceleration_field_priority:
        value = accel_candidates.get(key, float("nan"))
        if key == "acceleration" and (not use_top_level_acceleration):
            continue
        if key != "acceleration" and (not use_lon_debug):
            continue
        if not math.isfinite(value):
            continue
        if value >= 0.0:
            target_accel = float(value)
            target_decel = 0.0
        else:
            target_accel = 0.0
            target_decel = abs(float(value))
        accel_source = "apollo.acceleration" if key == "acceleration" else key
        decel_source = accel_source
        selected_signed_field = key
        break
    if not selected_signed_field:
        target_accel = raw_throttle * apollo_max_accel_mps2
        target_decel = raw_brake * apollo_max_decel_mps2
    if raw_brake > raw_throttle and target_decel <= 1e-6:
        target_decel = raw_brake * apollo_max_decel_mps2
        decel_source = "percentage_scaled"
    if raw_throttle >= raw_brake and target_accel <= 1e-6:
        target_accel = raw_throttle * apollo_max_accel_mps2
        accel_source = "percentage_scaled"
    return {
        "target_accel_mps2": max(0.0, float(target_accel)),
        "target_decel_mps2": max(0.0, float(target_decel)),
        "target_accel_source": accel_source,
        "target_decel_source": decel_source,
        "selected_signed_acceleration_field": selected_signed_field or "",
    }


def _load_targets_from_apollo_raw(
    raw_path: Path,
    *,
    steering_field_priority: Sequence[str],
    acceleration_field_priority: Sequence[str],
    use_top_level_acceleration: bool,
    use_lon_debug: bool,
    apollo_max_steer_angle_deg: float,
    apollo_max_accel_mps2: float,
    apollo_max_decel_mps2: float,
    min_dt_sec: float,
    max_samples: int,
) -> List[Dict[str, Any]]:
    seq: List[Dict[str, Any]] = []
    last_ts: Optional[float] = None
    with raw_path.open() as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            raw_fields = obj.get("apollo_control_raw") or {}
            if not isinstance(raw_fields, dict):
                continue
            ts_sec = _safe_float(obj.get("ts_sec"), float("nan"))
            if not math.isfinite(ts_sec):
                continue
            if last_ts is not None and (ts_sec - last_ts) < float(min_dt_sec):
                continue
            last_ts = ts_sec
            steer_field, steer_pct = _select_steering_field(raw_fields, field_order=steering_field_priority)
            raw_throttle = max(0.0, min(1.0, _safe_float(raw_fields.get("throttle"), 0.0) / 100.0))
            raw_brake = max(0.0, min(1.0, _safe_float(raw_fields.get("brake"), 0.0) / 100.0))
            raw_steer = max(-1.0, min(1.0, float(steer_pct) / 100.0))
            targets = _derive_longitudinal_targets(
                raw_fields=raw_fields,
                raw_throttle=raw_throttle,
                raw_brake=raw_brake,
                acceleration_field_priority=acceleration_field_priority,
                use_top_level_acceleration=use_top_level_acceleration,
                use_lon_debug=use_lon_debug,
                apollo_max_accel_mps2=apollo_max_accel_mps2,
                apollo_max_decel_mps2=apollo_max_decel_mps2,
            )
            seq.append(
                {
                    "ts_sec": float(ts_sec),
                    "selected_steering_field": steer_field,
                    "raw_steer": float(raw_steer),
                    "raw_throttle": float(raw_throttle),
                    "raw_brake": float(raw_brake),
                    "target_front_wheel_angle_deg": float(raw_steer) * float(apollo_max_steer_angle_deg),
                    **targets,
                }
            )
    if not seq:
        return seq
    return _select_replay_subset(seq, max_samples=max_samples)


def _is_active_target(item: Dict[str, Any]) -> bool:
    if abs(float(item.get("target_front_wheel_angle_deg", 0.0) or 0.0)) > 0.05:
        return True
    if float(item.get("target_accel_mps2", 0.0) or 0.0) > 0.05:
        return True
    if float(item.get("target_decel_mps2", 0.0) or 0.0) > 1.0:
        return True
    return False


def _downsample_sequence(seq: Sequence[Dict[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
    if len(seq) <= limit:
        return list(seq)
    out: List[Dict[str, Any]] = []
    step = float(len(seq) - 1) / float(max(1, limit - 1))
    for idx in range(limit):
        pick = int(round(idx * step))
        pick = max(0, min(len(seq) - 1, pick))
        out.append(dict(seq[pick]))
    return out


def _select_replay_subset(seq: Sequence[Dict[str, Any]], *, max_samples: int) -> List[Dict[str, Any]]:
    limit = max(1, int(max_samples))
    active = [dict(item) for item in seq if _is_active_target(item)]
    inactive = [dict(item) for item in seq if not _is_active_target(item)]
    if not active:
        return _downsample_sequence(seq, limit=limit)
    active_budget = min(len(active), max(1, int(round(limit * 0.8))))
    inactive_budget = max(0, limit - active_budget)
    out = _downsample_sequence(active, limit=active_budget)
    if inactive_budget > 0 and inactive:
        out.extend(_downsample_sequence(inactive, limit=min(len(inactive), inactive_budget)))
    out.sort(key=lambda item: float(item.get("ts_sec", 0.0) or 0.0))
    if len(out) > limit:
        out = _downsample_sequence(out, limit=limit)
    return out


def _sequence_coverage(sequence: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    abs_steers = [abs(float(item.get("target_front_wheel_angle_deg", 0.0) or 0.0)) for item in sequence]
    accels = [float(item.get("target_accel_mps2", 0.0) or 0.0) for item in sequence]
    decels = [float(item.get("target_decel_mps2", 0.0) or 0.0) for item in sequence]
    return {
        "sample_count": len(sequence),
        "nonzero_steer_count": sum(1 for v in abs_steers if v > 1e-3),
        "positive_accel_count": sum(1 for v in accels if v > 1e-3),
        "positive_decel_count": sum(1 for v in decels if v > 1e-3),
        "max_abs_steer_deg": max(abs_steers) if abs_steers else 0.0,
        "max_target_accel_mps2": max(accels) if accels else 0.0,
        "max_target_decel_mps2": max(decels) if decels else 0.0,
    }


def _coverage_is_sufficient(coverage: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    if int(coverage.get("nonzero_steer_count", 0) or 0) <= 0:
        reasons.append("no_nonzero_steer_targets")
    if int(coverage.get("positive_accel_count", 0) or 0) <= 0:
        reasons.append("no_positive_accel_targets")
    if float(coverage.get("max_target_decel_mps2", 0.0) or 0.0) < 0.2:
        reasons.append("weak_decel_targets")
    return (not reasons), reasons


def _merge_coverages(items: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    merged = {
        "sample_count": 0,
        "nonzero_steer_count": 0,
        "positive_accel_count": 0,
        "positive_decel_count": 0,
        "max_abs_steer_deg": 0.0,
        "max_target_accel_mps2": 0.0,
        "max_target_decel_mps2": 0.0,
    }
    for item in items:
        merged["sample_count"] += int(item.get("sample_count", 0) or 0)
        merged["nonzero_steer_count"] += int(item.get("nonzero_steer_count", 0) or 0)
        merged["positive_accel_count"] += int(item.get("positive_accel_count", 0) or 0)
        merged["positive_decel_count"] += int(item.get("positive_decel_count", 0) or 0)
        merged["max_abs_steer_deg"] = max(
            float(merged["max_abs_steer_deg"]),
            float(item.get("max_abs_steer_deg", 0.0) or 0.0),
        )
        merged["max_target_accel_mps2"] = max(
            float(merged["max_target_accel_mps2"]),
            float(item.get("max_target_accel_mps2", 0.0) or 0.0),
        )
        merged["max_target_decel_mps2"] = max(
            float(merged["max_target_decel_mps2"]),
            float(item.get("max_target_decel_mps2", 0.0) or 0.0),
        )
    return merged


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


def _write_rows(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    q = payload.get("quality", {}) or {}
    lines = [
        "# Apollo 真实控制序列 Replay 验证报告",
        "",
        "这份报告不是手工目标网格，而是把 Apollo 真实发过的一段控制序列解码成物理目标后，在固定场景里 replay。",
        "",
        "## 结果",
        "",
        f"- steering 平均绝对误差: `{q.get('mean_abs_steer_error_deg', 0.0):.4f} deg`",
        f"- throttle 平均绝对误差: `{q.get('mean_abs_accel_error_mps2', 0.0):.4f} m/s^2`",
        f"- brake 平均绝对误差: `{q.get('mean_abs_decel_error_mps2', 0.0):.4f} m/s^2`",
        f"- replay 样本数: `{q.get('sample_count', 0)}`",
        "",
        "## 序列覆盖",
        "",
        f"- nonzero steer count: `{(payload.get('sequence_coverage') or {}).get('nonzero_steer_count', 0)}`",
        f"- positive accel count: `{(payload.get('sequence_coverage') or {}).get('positive_accel_count', 0)}`",
        f"- positive decel count: `{(payload.get('sequence_coverage') or {}).get('positive_decel_count', 0)}`",
        f"- max abs steer deg: `{(payload.get('sequence_coverage') or {}).get('max_abs_steer_deg', 0.0):.4f}`",
        f"- max target accel mps2: `{(payload.get('sequence_coverage') or {}).get('max_target_accel_mps2', 0.0):.4f}`",
        f"- max target decel mps2: `{(payload.get('sequence_coverage') or {}).get('max_target_decel_mps2', 0.0):.4f}`",
        "",
        "## 说明",
        "",
        "- capture 来源是 Apollo 真实输出的 `apollo_control_raw.jsonl`。",
        "- replay 时先用与 bridge 同语义的 decode 规则恢复物理目标，再在固定场景里只验证执行器误差。",
        "- 这一步仍然依赖当前字段语义假设，但比手工目标网格更接近 Apollo 真实在线输出。",
        "",
    ]
    path.write_text("\n".join(lines))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Capture Apollo control sequence and replay it in a fixed validation scene")
    ap.add_argument(
        "--capture-config",
        action="append",
        default=None,
    )
    ap.add_argument(
        "--capture-run-dir",
        action="append",
        default=None,
        help="reuse an existing capture run dir instead of launching a new capture",
    )
    ap.add_argument("--validation-config", default="configs/io/examples/apollo_actuator_tracking_validation.yaml")
    ap.add_argument(
        "--calibration-file",
        default="artifacts/actuator_calibration_library/e969a34a5f43fd25/carla_actuator_calibration.json",
    )
    ap.add_argument("--output-dir", default="runs/apollo_control_replay_validation")
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--ego-role-name", default="hero")
    ap.add_argument("--timeout-sec", type=float, default=10.0)
    ap.add_argument("--metadata-timeout-sec", type=float, default=240.0)
    ap.add_argument("--ego-discovery-timeout-sec", type=float, default=30.0)
    ap.add_argument("--capture-wait-timeout-sec", type=float, default=900.0)
    ap.add_argument("--capture-max-samples", type=int, default=800)
    ap.add_argument("--capture-min-dt-sec", type=float, default=0.08)
    ap.add_argument("--apollo-max-steer-angle-deg", type=float, default=8.203)
    ap.add_argument("--apollo-max-accel-mps2", type=float, default=4.0)
    ap.add_argument("--apollo-max-decel-mps2", type=float, default=6.0)
    ap.add_argument("--use-top-level-acceleration", action="store_true", default=True)
    ap.add_argument("--use-lon-debug", action="store_true", default=True)
    ap.add_argument("--replay-step-sec", type=float, default=0.10)
    ap.add_argument("--replay-window-sec", type=float, default=0.18)
    ap.add_argument(
        "--policy-config",
        default="configs/io/examples/unified_calibration_pipeline.yaml",
        help="capture validity / minimum coverage policy yaml",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if not args.capture_config and not args.capture_run_dir:
        args.capture_config = [
            "configs/io/examples/apollo_semantic_capture_longitudinal.yaml",
            "configs/io/examples/apollo_semantic_capture_lateral.yaml",
        ]
    (
        load_actuator_calibration,
        CarlaProbe,
        coverage_meets_minimum,
        evaluate_capture_validity,
        load_policy_config,
        summarize_capture_validity,
        write_capture_validity_artifacts,
        write_policy_artifacts,
    ) = _load_runtime_modules()
    output_dir = Path(args.output_dir).expanduser()
    if not output_dir.is_absolute():
        output_dir = (Path.cwd() / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = output_dir / "logs"
    captures_dir = output_dir / "captures"
    scene_run_dir = output_dir / "scene_run"
    generated_cfg_dir = output_dir / "generated_configs"
    artifacts_dir = output_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    policy_path = Path(args.policy_config).expanduser()
    if not policy_path.is_absolute():
        policy_path = (REPO_ROOT / policy_path).resolve()
    policy = load_policy_config(policy_path if policy_path.exists() else None)
    steering_field_priority = _normalize_field_priority(
        None,
        default=("steering_target", "steering_percentage", "steering", "steering_rate"),
    )
    acceleration_field_priority = _normalize_field_priority(
        None,
        default=("acceleration", "debug_simple_lon_acceleration_cmd", "debug_simple_lon_acceleration_lookup"),
    )
    capture_procs: List[subprocess.Popen[str]] = []
    validation_proc: Optional[subprocess.Popen[str]] = None
    try:
        capture_details: List[Dict[str, Any]] = []
        if args.capture_run_dir:
            for idx, run_dir_str in enumerate(args.capture_run_dir, start=1):
                resolved_dir = _resolve_run_dir(Path(run_dir_str).expanduser().resolve())
                raw_path = resolved_dir / "artifacts" / "apollo_control_raw.jsonl"
                detail = evaluate_capture_validity(
                    capture_id=f"capture_{idx:02d}",
                    raw_path=raw_path if raw_path.exists() else None,
                    exit_code=0,
                    policy=policy,
                    summary_path=resolved_dir / "summary.json",
                    metadata_path=resolved_dir / "artifacts" / "scenario_metadata.json",
                    log_path=logs_dir / f"capture_{idx:02d}.log",
                )
                detail.update(
                    {
                        "config_path": "",
                        "materialized_config_path": "",
                        "run_dir": str(resolved_dir),
                        "artifact_wait_error": "",
                        "reused_existing_capture": True,
                    }
                )
                capture_details.append(detail)
        else:
            for idx, cfg_str in enumerate(args.capture_config):
                cfg_path = Path(cfg_str).expanduser()
                if not cfg_path.is_absolute():
                    cfg_path = (REPO_ROOT / cfg_path).resolve()
                materialized_cfg = _materialize_config(
                    cfg_path,
                    generated_dir=generated_cfg_dir,
                    output_name=f"capture_{idx+1:02d}.yaml",
                )
                run_dir = captures_dir / f"capture_{idx+1:02d}"
                cmd = [
                    sys.executable,
                    "-m",
                    "carla_testbed",
                    "run",
                    "--config",
                    str(materialized_cfg),
                    "--run-dir",
                    str(run_dir),
                    "--no-healthcheck",
                ]
                _info(f"[capture {idx+1}] start {cfg_path} -> {materialized_cfg}")
                previous_resolved_dir = _resolve_run_dir(run_dir) if run_dir.exists() else None
                launched_at_unix_sec = time.time()
                proc = _run_child(cmd, cwd=REPO_ROOT, log_path=logs_dir / f"capture_{idx+1:02d}.log")
                capture_procs.append(proc)
                resolved_dir = _resolve_run_dir(run_dir)
                raw_path: Optional[Path] = None
                artifact_wait_error = ""
                try:
                    resolved_dir, raw_path = _wait_for_capture_artifact(
                        run_dir,
                        timeout_sec=float(args.capture_wait_timeout_sec),
                        launched_at_unix_sec=launched_at_unix_sec,
                        previous_resolved_dir=previous_resolved_dir,
                    )
                    _info(f"[capture {idx+1}] raw ready {raw_path}")
                except Exception as exc:
                    artifact_wait_error = str(exc)
                    _warn(f"[capture {idx+1}] raw artifact not ready: {exc}")
                proc.wait(timeout=max(30.0, float(args.capture_wait_timeout_sec)))
                if proc.returncode != 0:
                    _warn(
                        f"[capture {idx+1}] non-zero exit rc={proc.returncode} "
                        f"for {cfg_path}; will inspect excitation quality before deciding"
                    )
                detail = evaluate_capture_validity(
                    capture_id=f"capture_{idx+1:02d}",
                    raw_path=raw_path,
                    exit_code=int(proc.returncode or 0),
                    policy=policy,
                    summary_path=resolved_dir / "summary.json",
                    metadata_path=resolved_dir / "artifacts" / "scenario_metadata.json",
                    log_path=logs_dir / f"capture_{idx+1:02d}.log",
                )
                detail.update(
                    {
                        "config_path": str(cfg_path),
                        "materialized_config_path": str(materialized_cfg),
                        "run_dir": str(resolved_dir),
                        "artifact_wait_error": artifact_wait_error,
                        "reused_existing_capture": False,
                    }
                )
                capture_details.append(detail)
        capture_summary = summarize_capture_validity(capture_details, policy=policy)
        write_capture_validity_artifacts(artifacts_dir, capture_summary)
        write_policy_artifacts(artifacts_dir, capture_summary)
        if not bool(capture_summary.get("minimum_coverage_ok")):
            raise RuntimeError(
                "captured Apollo sequence lacks enough valid coverage: "
                + ", ".join(capture_summary.get("minimum_coverage_reasons", []))
            )
        valid_sequences: List[List[Dict[str, Any]]] = []
        valid_coverages: List[Dict[str, Any]] = []
        valid_capture_details = [item for item in capture_details if bool(item.get("capture_valid")) and item.get("raw_path")]
        per_file_limit = max(1, int(args.capture_max_samples / max(len(valid_capture_details), 1)))
        for detail in valid_capture_details:
            raw_path = Path(str(detail.get("raw_path")))
            seq = _load_targets_from_apollo_raw(
                raw_path,
                steering_field_priority=steering_field_priority,
                acceleration_field_priority=acceleration_field_priority,
                use_top_level_acceleration=bool(args.use_top_level_acceleration),
                use_lon_debug=bool(args.use_lon_debug),
                apollo_max_steer_angle_deg=float(args.apollo_max_steer_angle_deg),
                apollo_max_accel_mps2=float(args.apollo_max_accel_mps2),
                apollo_max_decel_mps2=float(args.apollo_max_decel_mps2),
                min_dt_sec=float(args.capture_min_dt_sec),
                max_samples=per_file_limit,
            )
            coverage = _sequence_coverage(seq)
            coverage_reasons: List[str] = []
            coverage_ok = bool(
                int(coverage.get("sample_count", 0) or 0) > 0
                and (
                    int(coverage.get("nonzero_steer_count", 0) or 0) > 0
                    or int(coverage.get("positive_accel_count", 0) or 0) > 0
                    or int(coverage.get("positive_decel_count", 0) or 0) > 0
                )
            )
            if not coverage_ok:
                coverage_reasons.append("no_replay_targets_extracted")
            detail["replay_target_sample_count"] = len(seq)
            detail["replay_target_coverage"] = coverage
            detail["replay_target_coverage_ok"] = coverage_ok
            detail["replay_target_coverage_reasons"] = coverage_reasons
            if coverage_ok:
                valid_sequences.append(seq)
                valid_coverages.append(coverage)
                _info(
                    "[capture] accepted "
                    f"{raw_path} steer={coverage['nonzero_steer_count']} "
                    f"accel={coverage['positive_accel_count']} decel={coverage['positive_decel_count']}"
                )
            else:
                _warn(
                    "[capture] skip insufficient excitation "
                    f"{raw_path}: {', '.join(coverage_reasons) or 'unknown'}"
                )
        sequence: List[Dict[str, Any]] = []
        for seq in valid_sequences:
            sequence.extend(seq)
        if not sequence:
            raise RuntimeError("no valid Apollo replay targets extracted from capture artifacts")
        coverage = _merge_coverages(valid_coverages)
        coverage_ok, coverage_reasons = coverage_meets_minimum(
            {
                "valid_capture_count": len(valid_sequences),
                "nonzero_steer_samples": int(coverage.get("nonzero_steer_count", 0) or 0),
                "positive_accel_samples": int(coverage.get("positive_accel_count", 0) or 0),
                "decel_samples": int(coverage.get("positive_decel_count", 0) or 0),
                "max_dead_sequence_ratio": float(capture_summary.get("coverage", {}).get("max_dead_sequence_ratio", 0.0) or 0.0),
                "min_duration_sec": float(capture_summary.get("coverage", {}).get("min_duration_sec", 0.0) or 0.0),
            },
            policy=policy,
        )
        _write_rows(artifacts_dir / "apollo_replay_sequence.csv", sequence)
        (artifacts_dir / "apollo_replay_sequence_summary.json").write_text(
            json.dumps(
                {
                    "captures": capture_details,
                    "capture_validity_summary": capture_summary,
                    "coverage": coverage,
                    "coverage_ok": coverage_ok,
                    "coverage_reasons": coverage_reasons,
                    "policy": policy,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        if not coverage_ok:
            raise RuntimeError(
                "captured Apollo sequence lacks enough excitation: "
                + ", ".join(coverage_reasons)
            )
        validation_cfg = Path(args.validation_config).expanduser()
        if not validation_cfg.is_absolute():
            validation_cfg = (REPO_ROOT / validation_cfg).resolve()
        materialized_validation_cfg = _materialize_config(
            validation_cfg,
            generated_dir=generated_cfg_dir,
            output_name="validation_scene.yaml",
        )
        _force_start_carla_for_validation(materialized_validation_cfg)
        validation_cmd = [
            sys.executable,
            "-m",
            "carla_testbed",
            "run",
            "--config",
            str(materialized_validation_cfg),
            "--run-dir",
            str(scene_run_dir),
            "--no-healthcheck",
        ]
        _info("[scene] start fixed replay validation scene")
        validation_proc = _run_child(validation_cmd, cwd=REPO_ROOT, log_path=logs_dir / "validation_scene.log")
        resolved_scene_dir, metadata = _wait_for_metadata(scene_run_dir, timeout_sec=float(args.metadata_timeout_sec))
        scene_run_dir = resolved_scene_dir
        actor_id = int(metadata.get("ego_actor_id", 0) or 0)
        _info(f"[scene] ready actor_id={actor_id}")
        calibration = load_actuator_calibration(Path(args.calibration_file).expanduser().resolve())
        probe = CarlaProbe(
            host=args.carla_host,
            port=args.carla_port,
            ego_role_name=args.ego_role_name,
            timeout_sec=args.timeout_sec,
            actor_id=actor_id,
            ego_discovery_timeout_sec=args.ego_discovery_timeout_sec,
            ego_discovery_poll_sec=1.0,
        )
        records: List[Dict[str, Any]] = []
        raw_rows: List[Dict[str, Any]] = []
        _info(f"[replay] samples={len(sequence)}")
        for i, item in enumerate(sequence, start=1):
            target_angle = float(item.get("target_front_wheel_angle_deg", 0.0) or 0.0)
            target_accel = float(item.get("target_accel_mps2", 0.0) or 0.0)
            target_decel = float(item.get("target_decel_mps2", 0.0) or 0.0)
            speed_mps = max(0.0, float(probe.state().get("speed_mps", 0.0)))
            steer_cmd = calibration.steering_cmd_for_angle(target_angle)
            throttle_mapping = calibration.throttle_mapping_for_accel(
                target_accel,
                speed_mps=speed_mps,
                target_accel_max_mps2=float(args.apollo_max_accel_mps2),
            )
            brake_mapping = calibration.brake_mapping_for_decel(target_decel, speed_mps=speed_mps)
            throttle_cmd = _safe_float(throttle_mapping.get("cmd"), 0.0)
            brake_cmd = _safe_float(brake_mapping.get("cmd"), 0.0)
            if brake_cmd > 0.0 and target_decel > target_accel:
                throttle_cmd = 0.0
            samples = _sample_window(
                probe,
                throttle=float(throttle_cmd),
                brake=float(brake_cmd),
                steer=float(_safe_float(steer_cmd, 0.0)),
                sample_sec=float(args.replay_window_sec),
            )
            if not samples:
                continue
            measured_steer = _median([_safe_float(s.get("measured_steer_deg"), 0.0) for s in samples], default=0.0)
            measured_accel = _effective_throttle_accel(samples, window_sec=float(args.replay_window_sec))
            measured_decel = _effective_brake_decel(samples)
            row = {
                "index": i,
                "selected_steering_field": item.get("selected_steering_field", ""),
                "selected_signed_acceleration_field": item.get("selected_signed_acceleration_field", ""),
                "target_front_wheel_angle_deg": target_angle,
                "measured_steer_deg": measured_steer,
                "abs_steer_error_deg": abs(target_angle - measured_steer),
                "target_accel_mps2": target_accel,
                "measured_accel_mps2": measured_accel,
                "abs_accel_error_mps2": abs(target_accel - measured_accel),
                "target_decel_mps2": target_decel,
                "measured_decel_mps2": measured_decel,
                "abs_decel_error_mps2": abs(target_decel - measured_decel),
                "mapped_carla_steer_cmd": _safe_float(steer_cmd, 0.0),
                "mapped_throttle_cmd": throttle_cmd,
                "mapped_brake_cmd": brake_cmd,
                "throttle_mapping_source": str(throttle_mapping.get("source", "") or ""),
                "brake_mapping_source": str(brake_mapping.get("source", "") or ""),
            }
            records.append(row)
            raw_rows.extend(
                {
                    "replay_index": i,
                    "target_front_wheel_angle_deg": target_angle,
                    "target_accel_mps2": target_accel,
                    "target_decel_mps2": target_decel,
                    **s,
                }
                for s in samples[:5]
            )
            if i % 50 == 0:
                _info(f"[replay] {i}/{len(sequence)}")
            probe._step(max(0.0, float(args.replay_step_sec) - float(args.replay_window_sec)))
        steer_errors = [float(r["abs_steer_error_deg"]) for r in records]
        accel_errors = [float(r["abs_accel_error_mps2"]) for r in records if float(r["target_accel_mps2"]) > 0.0]
        decel_errors = [float(r["abs_decel_error_mps2"]) for r in records if float(r["target_decel_mps2"]) > 0.0]
        payload = {
            "schema_version": 1,
            "generated_at_unix_sec": time.time(),
            "calibration_file": str(Path(args.calibration_file).expanduser().resolve()),
            "capture_configs": list(args.capture_config or []),
            "capture_run_dirs": [str(item.get("run_dir", "") or "") for item in capture_details],
            "capture_paths": [str(p) for p in capture_paths],
            "sequence_coverage": coverage,
            "scene_run_dir": str(scene_run_dir),
            "quality": {
                "sample_count": len(records),
                "mean_abs_steer_error_deg": float(sum(steer_errors) / len(steer_errors)) if steer_errors else 0.0,
                "median_abs_steer_error_deg": _median(steer_errors, default=0.0),
                "mean_abs_accel_error_mps2": float(sum(accel_errors) / len(accel_errors)) if accel_errors else 0.0,
                "median_abs_accel_error_mps2": _median(accel_errors, default=0.0),
                "mean_abs_decel_error_mps2": float(sum(decel_errors) / len(decel_errors)) if decel_errors else 0.0,
                "median_abs_decel_error_mps2": _median(decel_errors, default=0.0),
            },
            "records": records,
        }
        payload["pass"] = bool(coverage_ok and bool(records))
        (artifacts_dir / "apollo_control_replay_validation.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False)
        )
        _write_report(artifacts_dir / "apollo_control_replay_validation_report.md", payload)
        _write_rows(artifacts_dir / "apollo_control_replay_validation.csv", records)
        _write_rows(artifacts_dir / "apollo_control_replay_raw_series.csv", raw_rows)
        _info(f"[done] wrote {artifacts_dir / 'apollo_control_replay_validation.json'}")
        return 0
    except Exception as exc:
        _warn(f"replay validation failed: {exc}")
        if scene_run_dir.exists():
            tail = _tail_text(scene_run_dir / "logs" / "carla_server.log")
            if tail:
                _warn("[carla_server.log tail]\n" + tail)
        return 1
    finally:
        _stop_child(validation_proc)
        for proc in capture_procs:
            _stop_child(proc)


if __name__ == "__main__":
    raise SystemExit(main())
