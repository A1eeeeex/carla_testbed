from __future__ import annotations

import json
import math
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from carla_testbed.analysis.apollo_hdmap_projection import OFFICIAL_SOURCE

MAP_XYSL_LINE_RE = re.compile(
    r"lane_id\[(?P<lane_id>[^\]]*)\],\s*"
    r"s\[(?P<s>[-+0-9.eE]+)\],\s*"
    r"l\[(?P<l>[-+0-9.eE]+)\],\s*"
    r"heading\[(?P<heading>[-+0-9.eE]+)\]"
)


@dataclass(frozen=True)
class LocalizationProjectionSample:
    timestamp: float | None
    x: float
    y: float
    heading: float | None
    source_artifact: str
    source_index: int


@dataclass(frozen=True)
class MapXyslConfig:
    map_dir: str = "/apollo/modules/map/data/carla_town01"
    base_map_filename: str = "base_map.txt"
    map_name: str = "Town01"
    docker_container: str | None = "apollo_neo_dev_10.0.0_pkg"
    map_xysl_bin: str = "/opt/apollo/neo/bin/map_xysl"
    timeout_s: float = 15.0


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def load_localization_projection_samples(
    *,
    run_dir: str | Path | None = None,
    input_path: str | Path | None = None,
    max_samples: int | None = 250,
) -> list[LocalizationProjectionSample]:
    """Load Apollo-map localization samples without depending on Apollo runtime.

    The preferred input is `artifacts/apollo_reference_line_contract.jsonl`
    because it records the actual localization x/y/heading published to Apollo.
    A single-sample fallback from `cyber_bridge_stats.last_pose_debug` is kept
    for readiness checks, but dense claim-grade evidence should use the JSONL.
    """

    candidates: list[Path] = []
    if input_path:
        candidates.append(Path(input_path).expanduser())
    if run_dir:
        root = Path(run_dir).expanduser()
        candidates.extend(
            [
                root / "artifacts" / "apollo_reference_line_contract.jsonl",
                root / "artifacts" / "cyber_bridge_stats.json",
            ]
        )

    seen: set[Path] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        if not path.exists():
            continue
        if path.suffix.lower() == ".jsonl":
            samples = _samples_from_jsonl(path)
        else:
            samples = _samples_from_json(path)
        if samples:
            return _limit_samples(samples, max_samples=max_samples)
    return []


def export_apollo_hdmap_projection_jsonl(
    *,
    run_dir: str | Path | None = None,
    input_path: str | Path | None = None,
    out_path: str | Path,
    config: MapXyslConfig | None = None,
    max_samples: int | None = 250,
    command_runner: CommandRunner = subprocess.run,
) -> dict[str, Any]:
    cfg = config or MapXyslConfig()
    samples = load_localization_projection_samples(
        run_dir=run_dir,
        input_path=input_path,
        max_samples=max_samples,
    )
    output_path = Path(out_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    for sample in samples:
        rows.append(
            project_sample_with_map_xysl(
                sample,
                cfg,
                command_runner=command_runner,
            )
        )

    output_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )

    ok_rows = [row for row in rows if row.get("status") == "ok"]
    non_ok_rows = [row for row in rows if row.get("status") != "ok"]
    status = "pass" if rows and not non_ok_rows else "warn" if rows else "insufficient_data"
    return {
        "schema_version": "apollo_hdmap_projection_export.v1",
        "status": status,
        "run_dir": str(Path(run_dir).expanduser()) if run_dir else None,
        "input_path": str(Path(input_path).expanduser()) if input_path else None,
        "out_path": str(output_path),
        "sample_count": len(samples),
        "row_count": len(rows),
        "ok_row_count": len(ok_rows),
        "non_ok_row_count": len(non_ok_rows),
        "map_name": cfg.map_name,
        "map_dir": cfg.map_dir,
        "base_map_filename": cfg.base_map_filename,
        "docker_container": cfg.docker_container,
        "map_xysl_bin": cfg.map_xysl_bin,
        "backend": "apollo_map_xysl",
        "source": OFFICIAL_SOURCE,
        "warnings": [] if rows else ["localization_samples_missing"],
        "blocking_reasons": [],
    }


def project_sample_with_map_xysl(
    sample: LocalizationProjectionSample,
    config: MapXyslConfig,
    *,
    command_runner: CommandRunner = subprocess.run,
) -> dict[str, Any]:
    command = build_map_xysl_command(sample, config)
    try:
        completed = command_runner(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=config.timeout_s,
        )
        returncode = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        timeout_error = False
    except subprocess.TimeoutExpired as exc:
        returncode = None
        stdout = _decode_output(exc.stdout)
        stderr = _decode_output(exc.stderr)
        timeout_error = True
    output = "\n".join(part for part in [stdout, stderr] if part)
    parsed = parse_map_xysl_xy_to_sl_output(output)
    status = "ok" if returncode == 0 and parsed else _status_from_map_xysl_output(output)
    if timeout_error:
        status = "error"

    lane_heading = parsed.get("lane_heading_at_s") if parsed else None
    lateral = parsed.get("projection_l") if parsed else None
    heading_error = (
        normalize_angle(float(sample.heading) - float(lane_heading))
        if sample.heading is not None and lane_heading is not None
        else None
    )
    return {
        "timestamp": sample.timestamp,
        "localization_x": sample.x,
        "localization_y": sample.y,
        "localization_heading": sample.heading,
        "nearest_lane_id": parsed.get("nearest_lane_id") if parsed else None,
        "projection_s": parsed.get("projection_s") if parsed else None,
        "projection_l": lateral,
        "lane_heading_at_s": lane_heading,
        "heading_error_rad": heading_error,
        "lateral_error_m": lateral,
        "road_id": None,
        "junction_id": None,
        "source": OFFICIAL_SOURCE,
        "map_name": config.map_name,
        "map_dir": config.map_dir,
        "status": status,
        "exporter_backend": "apollo_map_xysl",
        "source_artifact": sample.source_artifact,
        "source_index": sample.source_index,
        "command_exit_code": returncode,
        "command_timeout": timeout_error,
        "command": " ".join(shlex.quote(part) for part in command),
        "raw_output_excerpt": output[:1000],
    }


def build_map_xysl_command(sample: LocalizationProjectionSample, config: MapXyslConfig) -> list[str]:
    timeout_seconds = max(1, int(math.ceil(config.timeout_s)))
    inner = [
        "timeout",
        "--kill-after=2s",
        f"{timeout_seconds}s",
        config.map_xysl_bin,
        f"--map_dir={config.map_dir}",
        f"--base_map_filename={config.base_map_filename}",
        "--xy_to_sl",
        f"--x={sample.x:.12g}",
        f"--y={sample.y:.12g}",
        "--logtostderr=1",
    ]
    if config.docker_container:
        return [
            "docker",
            "exec",
            config.docker_container,
            "bash",
            "-lc",
            " ".join(shlex.quote(part) for part in inner),
        ]
    return inner


def _decode_output(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def parse_map_xysl_xy_to_sl_output(output: str) -> dict[str, Any] | None:
    match = MAP_XYSL_LINE_RE.search(output or "")
    if not match:
        return None
    return {
        "nearest_lane_id": match.group("lane_id"),
        "projection_s": _float(match.group("s")),
        "projection_l": _float(match.group("l")),
        "lane_heading_at_s": _float(match.group("heading")),
    }


def normalize_angle(angle_rad: float) -> float:
    value = math.fmod(angle_rad + math.pi, 2.0 * math.pi)
    if value < 0:
        value += 2.0 * math.pi
    return value - math.pi


def _samples_from_jsonl(path: Path) -> list[LocalizationProjectionSample]:
    samples: list[LocalizationProjectionSample] = []
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            sample = _sample_from_mapping(payload, source_artifact=str(path), source_index=index)
            if sample is not None:
                samples.append(sample)
    return samples


def _samples_from_json(path: Path) -> list[LocalizationProjectionSample]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, Mapping):
        return []
    last_pose = payload.get("last_pose_debug") if isinstance(payload.get("last_pose_debug"), Mapping) else payload
    sample = _sample_from_mapping(last_pose, source_artifact=str(path), source_index=0)
    return [sample] if sample is not None else []


def _sample_from_mapping(
    payload: Mapping[str, Any],
    *,
    source_artifact: str,
    source_index: int,
) -> LocalizationProjectionSample | None:
    loc = payload.get("localization") if isinstance(payload.get("localization"), Mapping) else payload
    x = _first_number(loc, ["x", "localization_x", "map_x"])
    y = _first_number(loc, ["y", "localization_y", "map_y"])
    heading = _first_number(loc, ["heading", "localization_heading", "map_heading"])
    timestamp = _first_number(payload, ["timestamp", "sim_time_sec", "measurement_time", "ts_sec"])
    if x is None or y is None:
        return None
    return LocalizationProjectionSample(
        timestamp=timestamp,
        x=x,
        y=y,
        heading=heading,
        source_artifact=source_artifact,
        source_index=source_index,
    )


def _limit_samples(
    samples: Sequence[LocalizationProjectionSample],
    *,
    max_samples: int | None,
) -> list[LocalizationProjectionSample]:
    selected = list(samples)
    if max_samples is None or max_samples <= 0 or len(selected) <= max_samples:
        return selected
    if max_samples == 1:
        return [selected[-1]]
    step = (len(selected) - 1) / float(max_samples - 1)
    indexes = sorted({round(i * step) for i in range(max_samples)})
    return [selected[index] for index in indexes]


def _status_from_map_xysl_output(output: str) -> str:
    lowered = (output or "").lower()
    if "get_nearest_lane failed" in lowered or "lane is null" in lowered:
        return "no_lane"
    if "outside" in lowered or "out_of_map" in lowered:
        return "out_of_map"
    return "error"


def _first_number(payload: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        value = payload.get(key)
        number = _float(value)
        if number is not None:
            return number
    return None


def _float(value: Any) -> float | None:
    if value in {None, "", "nan", "NaN"}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
