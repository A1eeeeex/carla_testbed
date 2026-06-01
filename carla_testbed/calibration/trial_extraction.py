from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .control_actuation import CALIBRATION_TRIAL_FIELDS

DEFAULT_COMMAND_THRESHOLDS = {
    "throttle": 0.05,
    "brake": 0.05,
    "steer": 0.02,
}

COMMAND_FIELD_CANDIDATES = {
    "throttle": ("throttle_applied", "throttle_mapped", "throttle_raw", "cmd_throttle", "throttle"),
    "brake": ("brake_applied", "brake_mapped", "brake_raw", "cmd_brake", "brake"),
    "steer": ("carla_steer_applied", "bridge_steer_mapped", "apollo_steer_raw", "cmd_steer", "steer"),
}

TIME_FIELDS = ("sim_time", "time_s", "timestamp", "t")
SPEED_FIELDS = ("ego_speed", "speed_mps", "v_mps", "speed")
YAW_RATE_FIELDS = ("ego_yaw_rate", "yaw_rate_mean_rad_s", "yaw_rate_rad_s", "yaw_rate")
LATENCY_FIELDS = ("control_latency_ms", "latency_ms")


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _first_num(row: Mapping[str, Any], names: Sequence[str]) -> float | None:
    for name in names:
        value = _num(row.get(name))
        if value is not None:
            return value
    return None


def _first_text(row: Mapping[str, Any], names: Sequence[str]) -> str | None:
    for name in names:
        value = row.get(name)
        if value is not None and value != "":
            return str(value)
    return None


def _mean(values: Iterable[float]) -> float | None:
    cleaned = [value for value in values if math.isfinite(value)]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def _percentile(values: Iterable[float], q: float) -> float | None:
    cleaned = sorted(value for value in values if math.isfinite(value))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (len(cleaned) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return cleaned[lower]
    weight = rank - lower
    return cleaned[lower] * (1.0 - weight) + cleaned[upper] * weight


def _format(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.9g}"
    return value


def load_timeseries_rows(path: str | Path) -> list[dict[str, Any]]:
    ts_path = Path(path).expanduser()
    suffix = ts_path.suffix.lower()
    if suffix == ".csv":
        with ts_path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    if suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        with ts_path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    raise ValueError(f"timeseries JSONL row must be an object at {ts_path}:{line_number}")
                rows.append(payload)
        return rows
    if suffix == ".json":
        payload = json.loads(ts_path.read_text(encoding="utf-8"))
        if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
            return list(payload)
        raise ValueError(f"timeseries JSON must be a list of objects: {ts_path}")
    raise ValueError(f"unsupported timeseries format: {ts_path}")


def discover_timeseries_paths(run_dir: str | Path) -> list[Path]:
    root = Path(run_dir).expanduser()
    candidates = [
        root / "timeseries.csv",
        root / "timeseries.jsonl",
        root / "timeseries.json",
        root / "artifacts" / "timeseries.csv",
        root / "artifacts" / "timeseries.jsonl",
        root / "artifacts" / "timeseries.json",
    ]
    found = [path for path in candidates if path.exists()]
    if found:
        return found
    recursive: list[Path] = []
    for pattern in ("timeseries.csv", "timeseries.jsonl", "timeseries.json"):
        recursive.extend(sorted(root.rglob(pattern)))
    return recursive


def _annotated_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    fixed_dt_s: float | None = None,
) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        time_s = _first_num(row, TIME_FIELDS)
        if time_s is None and fixed_dt_s is not None:
            frame_id = _first_num(row, ("frame_id", "frame"))
            time_s = (frame_id if frame_id is not None else index) * fixed_dt_s
        payload = dict(row)
        payload["_row_index"] = index
        payload["_time_s"] = time_s
        payload["_speed_mps"] = _first_num(row, SPEED_FIELDS)
        payload["_yaw_rate_rad_s"] = _first_num(row, YAW_RATE_FIELDS)
        payload["_latency_ms"] = _first_num(row, LATENCY_FIELDS)
        annotated.append(payload)
    return annotated


def _command_value(row: Mapping[str, Any], command_type: str) -> float | None:
    return _first_num(row, COMMAND_FIELD_CANDIDATES[command_type])


def _segment_to_trial(
    segment: Sequence[Mapping[str, Any]],
    *,
    command_type: str,
    trial_index: int,
    route_id: str | None,
    backend: str | None,
) -> dict[str, Any] | None:
    if not segment:
        return None
    start_time = _num(segment[0].get("_time_s"))
    end_time = _num(segment[-1].get("_time_s"))
    if start_time is None or end_time is None:
        return None
    duration = max(0.0, end_time - start_time)
    commands = [_command_value(row, command_type) for row in segment]
    command_values = [value for value in commands if value is not None]
    speeds = [_num(row.get("_speed_mps")) for row in segment]
    speed_values = [value for value in speeds if value is not None]
    yaw_rates = [_num(row.get("_yaw_rate_rad_s")) for row in segment]
    yaw_values = [value for value in yaw_rates if value is not None]
    latencies = [_num(row.get("_latency_ms")) for row in segment]
    latency_values = [value for value in latencies if value is not None]

    speed_initial = speed_values[0] if speed_values else None
    speed_final = speed_values[-1] if speed_values else None
    accel = None
    decel = None
    if duration > 0 and speed_initial is not None and speed_final is not None:
        delta = speed_final - speed_initial
        if command_type == "throttle":
            accel = delta / duration
        if command_type == "brake":
            decel = max(0.0, -delta / duration)

    row_route_id = route_id or _first_text(segment[0], ("route_id",))
    row_backend = backend or _first_text(segment[0], ("backend", "transport_mode"))
    notes = ["extracted_from_timeseries"]
    if not latency_values:
        notes.append("latency_missing")
    if command_type == "steer" and not yaw_values:
        notes.append("yaw_rate_missing")
    return {
        "trial_id": f"{command_type}_{trial_index:03d}",
        "command_type": command_type,
        "command_value": _mean(command_values),
        "start_time_s": start_time,
        "duration_s": duration,
        "speed_initial_mps": speed_initial,
        "speed_final_mps": speed_final,
        "accel_mean_mps2": accel,
        "decel_mean_mps2": decel,
        "yaw_rate_mean_rad_s": _mean(yaw_values),
        "yaw_rate_p95_rad_s": _percentile([abs(value) for value in yaw_values], 0.95),
        "latency_ms": _percentile(latency_values, 0.95),
        "route_id": row_route_id,
        "backend": row_backend,
        "notes": ";".join(notes),
    }


def extract_control_actuation_trials(
    rows: Sequence[Mapping[str, Any]],
    *,
    route_id: str | None = None,
    backend: str | None = None,
    min_duration_s: float = 0.1,
    command_thresholds: Mapping[str, float] | None = None,
    fixed_dt_s: float | None = None,
) -> list[dict[str, Any]]:
    thresholds = dict(DEFAULT_COMMAND_THRESHOLDS)
    if command_thresholds:
        thresholds.update({key: float(value) for key, value in command_thresholds.items() if key in thresholds})
    annotated = _annotated_rows(rows, fixed_dt_s=fixed_dt_s)
    trials: list[dict[str, Any]] = []
    counters: dict[str, int] = defaultdict(int)

    for command_type in ("throttle", "brake", "steer"):
        active: list[Mapping[str, Any]] = []
        for row in annotated:
            time_s = _num(row.get("_time_s"))
            command = _command_value(row, command_type)
            is_active = time_s is not None and command is not None and abs(command) >= thresholds[command_type]
            if is_active:
                active.append(row)
                continue
            if active:
                counters[command_type] += 1
                trial = _segment_to_trial(
                    active,
                    command_type=command_type,
                    trial_index=counters[command_type],
                    route_id=route_id,
                    backend=backend,
                )
                if trial is not None and (_num(trial.get("duration_s")) or 0.0) >= float(min_duration_s):
                    trials.append(trial)
                active = []
        if active:
            counters[command_type] += 1
            trial = _segment_to_trial(
                active,
                command_type=command_type,
                trial_index=counters[command_type],
                route_id=route_id,
                backend=backend,
            )
            if trial is not None and (_num(trial.get("duration_s")) or 0.0) >= float(min_duration_s):
                trials.append(trial)

    return sorted(trials, key=lambda row: (_num(row.get("start_time_s")) or 0.0, str(row.get("command_type") or "")))


def write_calibration_trials_csv(path: str | Path, trials: Sequence[Mapping[str, Any]]) -> None:
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CALIBRATION_TRIAL_FIELDS)
        writer.writeheader()
        for row in trials:
            writer.writerow({field: _format(row.get(field)) for field in CALIBRATION_TRIAL_FIELDS})
