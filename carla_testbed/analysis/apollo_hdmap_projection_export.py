from __future__ import annotations

import json
import math
import re
import shlex
import subprocess
from collections.abc import Sequence as SequenceABC
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from carla_testbed.adapters.apollo.frame_transform import (
    ApolloFrameTransform,
    Vector3,
    carla_forward_to_heading,
    carla_point_to_apollo,
    load_frame_transform,
)
from carla_testbed.analysis.apollo_hdmap_projection import OFFICIAL_SOURCE

MAP_XYSL_LINE_RE = re.compile(
    r"lane_id\[(?P<lane_id>[^\]]*)\],\s*"
    r"s\[(?P<s>[-+0-9.eE]+)\],\s*"
    r"l\[(?P<l>[-+0-9.eE]+)\],\s*"
    r"heading\[(?P<heading>[-+0-9.eE]+)\]"
)
ROUTE_DENSIFICATION_MAX_HEADING_DELTA_RAD = 0.05


@dataclass(frozen=True)
class LocalizationProjectionSample:
    timestamp: float | None
    x: float
    y: float
    heading: float | None
    source_artifact: str
    source_index: int
    sample_type: str = "ego"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MapXyslConfig:
    map_dir: str = "/apollo/modules/map/data/carla_town01"
    base_map_filename: str = "base_map.txt"
    map_name: str = "Town01"
    docker_container: str | None = "apollo_neo_dev_10.0.0_pkg"
    map_xysl_bin: str = "/opt/apollo/neo/bin/map_xysl"
    timeout_s: float = 15.0


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def infer_map_xysl_config_from_run_dir(
    run_dir: str | Path | None,
    *,
    base_config: MapXyslConfig | None = None,
) -> MapXyslConfig:
    """Infer Apollo map_xysl config from run-local map contract artifacts.

    This keeps operator commands map-specific without hard-coding Town01 into
    Baguang or future custom-map projection exports. Explicit CLI/config values
    should still override this inferred config at the call site.
    """

    cfg = base_config or MapXyslConfig()
    if run_dir is None:
        return cfg
    root = Path(run_dir).expanduser()
    guard = _read_json_mapping(root / "artifacts" / "map_contract_guard.json")
    if not guard:
        return cfg

    probe = guard.get("container_runtime_probe") if isinstance(guard.get("container_runtime_probe"), Mapping) else {}
    component_paths = (
        guard.get("container_component_paths")
        if isinstance(guard.get("container_component_paths"), Mapping)
        else {}
    )
    probe_component_paths = (
        probe.get("component_paths")
        if isinstance(probe.get("component_paths"), Mapping)
        else {}
    )

    map_dir = _first_text(
        [
            probe.get("selected_runtime_map_dir"),
            guard.get("runtime_map_dir_container_actual"),
            Path(str(probe_component_paths.get("base_map"))).parent
            if probe_component_paths.get("base_map")
            else None,
            Path(str(component_paths.get("base_map"))).parent
            if component_paths.get("base_map")
            else None,
        ]
    )
    base_map_filename = _first_text(
        [
            Path(str(probe_component_paths.get("base_map"))).name
            if probe_component_paths.get("base_map")
            else None,
            Path(str(component_paths.get("base_map"))).name
            if component_paths.get("base_map")
            else None,
        ]
    )
    map_name = _first_text(
        [
            guard.get("dreamview_selected_map"),
            guard.get("map_name"),
            Path(str(map_dir)).name if map_dir else None,
        ]
    )
    return replace(
        cfg,
        map_dir=map_dir or cfg.map_dir,
        base_map_filename=base_map_filename or cfg.base_map_filename,
        map_name=map_name or cfg.map_name,
    )


def load_localization_projection_samples(
    *,
    run_dir: str | Path | None = None,
    input_path: str | Path | None = None,
    max_samples: int | None = 250,
    include_route_samples: bool = False,
    include_start_goal: bool = False,
    frame_transform_path: str | Path | None = None,
    route_sample_step_m: float | None = None,
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

    selected_samples: list[LocalizationProjectionSample] = []
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
            selected_samples.extend(samples)
            break
    if run_dir and (include_route_samples or include_start_goal):
        root = Path(run_dir).expanduser()
        frame_transform = _load_optional_frame_transform(frame_transform_path)
        route_samples = _samples_from_route_json(root / "route.json", frame_transform=frame_transform)
        if not route_samples:
            route_samples = _samples_from_manifest_route_trace(
                root / "manifest.json",
                frame_transform=frame_transform,
            )
        if not route_samples:
            route_samples = _samples_from_scenario_metadata(
                root / "artifacts" / "scenario_metadata.json",
                frame_transform=frame_transform,
                step_m=route_sample_step_m,
            )
        if not route_samples:
            route_samples = _samples_from_scenario_goal(
                root / "artifacts" / "scenario_goal.json",
                frame_transform=frame_transform,
                step_m=route_sample_step_m,
            )
        if include_route_samples:
            route_samples = _densify_route_samples(
                route_samples,
                step_m=route_sample_step_m,
            )
        if include_start_goal and route_samples:
            selected_samples.extend(
                [
                    replace(route_samples[0], sample_type="start", source_index=0),
                    replace(route_samples[-1], sample_type="goal", source_index=len(route_samples) - 1),
                ]
            )
        if include_route_samples:
            selected_samples.extend(route_samples)
    return _limit_samples(_dedupe_samples(selected_samples), max_samples=max_samples)


def export_apollo_hdmap_projection_jsonl(
    *,
    run_dir: str | Path | None = None,
    input_path: str | Path | None = None,
    out_path: str | Path,
    config: MapXyslConfig | None = None,
    max_samples: int | None = 250,
    include_route_samples: bool = False,
    include_start_goal: bool = False,
    frame_transform_path: str | Path | None = None,
    route_sample_step_m: float | None = None,
    min_route_s_coverage: float | None = None,
    command_runner: CommandRunner = subprocess.run,
) -> dict[str, Any]:
    cfg = config or MapXyslConfig()
    samples = load_localization_projection_samples(
        run_dir=run_dir,
        input_path=input_path,
        max_samples=max_samples,
        include_route_samples=include_route_samples,
        include_start_goal=include_start_goal,
        frame_transform_path=frame_transform_path,
        route_sample_step_m=route_sample_step_m,
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
    projection_s_values = [_float(row.get("projection_s")) for row in ok_rows]
    projection_s_values = [value for value in projection_s_values if value is not None]
    projection_s_coverage_m = (
        max(projection_s_values) - min(projection_s_values)
        if len(projection_s_values) >= 2
        else None
    )
    warnings: list[str] = []
    if not rows:
        warnings.append("localization_samples_missing")
    if non_ok_rows:
        warnings.append("apollo_hdmap_projection_non_ok_rows")
    if (
        min_route_s_coverage is not None
        and projection_s_coverage_m is not None
        and projection_s_coverage_m < min_route_s_coverage
    ):
        warnings.append("apollo_hdmap_projection_route_s_coverage_below_requested_min")
    status = "pass" if rows and not warnings else "warn" if rows else "insufficient_data"
    return {
        "schema_version": "apollo_hdmap_projection_export.v1",
        "status": status,
        "failure_reason": _export_failure_reason(
            rows=rows,
            non_ok_rows=non_ok_rows,
            projection_s_coverage_m=projection_s_coverage_m,
            min_route_s_coverage=min_route_s_coverage,
        ),
        "run_dir": str(Path(run_dir).expanduser()) if run_dir else None,
        "input_path": str(Path(input_path).expanduser()) if input_path else None,
        "out_path": str(output_path),
        "sample_count": len(samples),
        "row_count": len(rows),
        "ok_row_count": len(ok_rows),
        "non_ok_row_count": len(non_ok_rows),
        "non_ok_status_counts": _status_counts(non_ok_rows),
        "include_route_samples": include_route_samples,
        "include_start_goal": include_start_goal,
        "route_sample_step_m": route_sample_step_m,
        "frame_transform_path": str(Path(frame_transform_path).expanduser()) if frame_transform_path else None,
        "min_route_s_coverage_m": min_route_s_coverage,
        "projection_s_coverage_m": projection_s_coverage_m,
        "map_name": cfg.map_name,
        "map_dir": cfg.map_dir,
        "base_map_filename": cfg.base_map_filename,
        "docker_container": cfg.docker_container,
        "map_xysl_bin": cfg.map_xysl_bin,
        "backend": "apollo_map_xysl",
        "source": OFFICIAL_SOURCE,
        "warnings": warnings,
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
    route_trace_heading_error = _heading_error_from_metadata(
        sample.metadata,
        "route_trace_heading_apollo",
        lane_heading,
    )
    route_chord_heading_error = _heading_error_from_metadata(
        sample.metadata,
        "route_chord_heading_apollo",
        lane_heading,
    )
    row = {
        "sample_type": sample.sample_type,
        "sample_index": sample.source_index,
        "sim_time": sample.timestamp,
        "apollo_time": None,
        "x_apollo": sample.x,
        "y_apollo": sample.y,
        "heading_apollo": sample.heading,
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
        "projection_status": status,
        "exporter_backend": "apollo_map_xysl",
        "source_artifact": sample.source_artifact,
        "source_index": sample.source_index,
        "command_exit_code": returncode,
        "command_timeout": timeout_error,
        "command": " ".join(shlex.quote(part) for part in command),
        "raw_output_excerpt": output[:1000],
    }
    if route_trace_heading_error is not None:
        row["route_trace_heading_error_rad"] = route_trace_heading_error
    if route_chord_heading_error is not None:
        row["route_chord_heading_error_rad"] = route_chord_heading_error
    for key, value in sample.metadata.items():
        if key not in row:
            row[str(key)] = value
    return row


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


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _first_text(values: Iterable[Any]) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


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


def _samples_from_route_json(
    path: Path,
    *,
    frame_transform: ApolloFrameTransform | None = None,
) -> list[LocalizationProjectionSample]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, Mapping):
        return []
    points = payload.get("points")
    if not isinstance(points, SequenceABC) or isinstance(points, (str, bytes)):
        points = payload.get("route_trace")
    if not isinstance(points, SequenceABC) or isinstance(points, (str, bytes)):
        return []
    points_are_carla_frame = _route_json_points_are_carla_frame(payload)
    if points_are_carla_frame and frame_transform is None:
        return []
    chord_headings = _route_chord_headings(
        points,
        frame_transform=frame_transform,
        points_are_carla_frame=points_are_carla_frame,
    )
    samples: list[LocalizationProjectionSample] = []
    for index, item in enumerate(points):
        if not isinstance(item, Mapping):
            continue
        x = _first_number(item, ["x", "localization_x", "map_x"])
        y = _first_number(item, ["y", "localization_y", "map_y"])
        z = _first_number(item, ["z", "localization_z", "map_z"]) or 0.0
        heading = _first_number(item, ["heading", "yaw_rad", "yaw", "localization_heading"])
        timestamp = _first_number(item, ["sim_time_sec", "timestamp", "time_sec"])
        if x is None or y is None:
            continue
        sample_metadata = _route_sample_metadata(item, index=index)
        if points_are_carla_frame and frame_transform is not None:
            sample_metadata.setdefault("carla_x", x)
            sample_metadata.setdefault("carla_y", y)
            sample_metadata.setdefault("carla_z", z)
            sample_metadata.setdefault("carla_heading", heading)
            apollo_point = carla_point_to_apollo(Vector3(x=x, y=y, z=z), frame_transform)
            x = apollo_point.x
            y = apollo_point.y
            trace_heading = _carla_heading_to_apollo(heading, frame_transform)
            chord_heading = chord_headings.get(index)
            if trace_heading is not None:
                sample_metadata["route_trace_heading_apollo"] = trace_heading
            if chord_heading is not None:
                sample_metadata["route_chord_heading_apollo"] = chord_heading
            sample_metadata["route_heading_source"] = (
                "route_chord" if chord_heading is not None else "route_trace"
            )
            heading = chord_heading if chord_heading is not None else trace_heading
        samples.append(
            LocalizationProjectionSample(
                timestamp=timestamp,
                x=x,
                y=y,
                heading=heading,
                source_artifact=str(path),
                source_index=index,
                sample_type="route",
                metadata=sample_metadata,
            )
        )
    return samples


def _route_json_points_are_carla_frame(payload: Mapping[str, Any]) -> bool:
    frame_keys = (
        payload.get("coordinate_frame"),
        payload.get("source_frame"),
        payload.get("points_frame"),
        payload.get("frame"),
    )
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    frame_keys = (*frame_keys, metadata.get("coordinate_frame"), metadata.get("route_trace_frame"))
    if any(str(value or "").strip().lower() == "carla_world" for value in frame_keys):
        return True
    schema_version = str(payload.get("schema_version") or "")
    source = str(payload.get("source") or "")
    source_lower = source.strip().lower()
    if source_lower in {
        "town01_forward_waypoint_trace",
        "baguang_forward_waypoint_trace",
    }:
        return True
    if "scenario_metadata" in source_lower or "carla" in source_lower:
        return True
    return schema_version == "runtime_route_trace.v1" and "scenario_metadata" in source


def _samples_from_manifest_route_trace(
    path: Path,
    *,
    frame_transform: ApolloFrameTransform | None,
) -> list[LocalizationProjectionSample]:
    if frame_transform is None or not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, Mapping):
        return []
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    scenario = metadata.get("scenario_metadata") if isinstance(metadata.get("scenario_metadata"), Mapping) else {}
    points = scenario.get("route_trace")
    if not isinstance(points, SequenceABC) or isinstance(points, (str, bytes)):
        return []
    chord_headings = _route_chord_headings(
        points,
        frame_transform=frame_transform,
        points_are_carla_frame=True,
    )
    samples: list[LocalizationProjectionSample] = []
    for index, item in enumerate(points):
        if not isinstance(item, Mapping):
            continue
        x = _first_number(item, ["x", "carla_x"])
        y = _first_number(item, ["y", "carla_y"])
        z = _first_number(item, ["z", "carla_z"]) or 0.0
        if x is None or y is None:
            continue
        sample_metadata = _route_sample_metadata(item, index=index)
        sample_metadata.setdefault("carla_x", x)
        sample_metadata.setdefault("carla_y", y)
        sample_metadata.setdefault("carla_z", z)
        sample_metadata.setdefault("carla_heading", item.get("heading"))
        apollo_point = carla_point_to_apollo(Vector3(x=x, y=y, z=z), frame_transform)
        trace_heading = _carla_heading_to_apollo(item.get("heading"), frame_transform)
        chord_heading = chord_headings.get(index)
        if trace_heading is not None:
            sample_metadata["route_trace_heading_apollo"] = trace_heading
        if chord_heading is not None:
            sample_metadata["route_chord_heading_apollo"] = chord_heading
        sample_metadata["route_heading_source"] = (
            "route_chord" if chord_heading is not None else "route_trace"
        )
        heading = chord_heading if chord_heading is not None else trace_heading
        samples.append(
            LocalizationProjectionSample(
                timestamp=_first_number(item, ["sim_time_sec", "timestamp", "time_sec"]),
                x=apollo_point.x,
                y=apollo_point.y,
                heading=heading,
                source_artifact=f"{path}:metadata.scenario_metadata.route_trace",
                source_index=index,
                sample_type="route",
                metadata=sample_metadata,
            )
        )
    return samples


def _load_optional_frame_transform(path: str | Path | None) -> ApolloFrameTransform | None:
    if not path:
        return None
    return load_frame_transform(Path(path).expanduser())


def _samples_from_scenario_goal(
    path: Path,
    *,
    frame_transform: ApolloFrameTransform | None,
    step_m: float | None,
) -> list[LocalizationProjectionSample]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, Mapping):
        return []
    start = payload.get("start_at_write_time")
    if not isinstance(start, Mapping):
        return []
    start_x = _first_number(start, ["x", "localization_x", "map_x"])
    start_y = _first_number(start, ["y", "localization_y", "map_y"])
    start_z = _first_number(start, ["z", "localization_z", "map_z"]) or 0.0
    if start_x is None or start_y is None:
        return []

    goal_payload = payload.get("goal")
    goal_source = "goal"
    frame = str(payload.get("frame") or "").strip().lower()
    if isinstance(payload.get("goal_raw_carla"), Mapping):
        goal_payload = payload.get("goal_raw_carla")
        goal_source = "goal_raw_carla"
    if not isinstance(goal_payload, Mapping):
        return []
    goal_x = _first_number(goal_payload, ["x", "localization_x", "map_x"])
    goal_y = _first_number(goal_payload, ["y", "localization_y", "map_y"])
    goal_z = _first_number(goal_payload, ["z", "localization_z", "map_z"]) or start_z
    if goal_x is None or goal_y is None:
        return []
    if goal_source == "goal_raw_carla" or frame in {"relative", "carla_raw", "carla_world"}:
        if frame_transform is None:
            return []
        apollo_goal = carla_point_to_apollo(
            Vector3(x=float(goal_x), y=float(goal_y), z=float(goal_z)),
            frame_transform,
        )
        goal_x, goal_y, goal_z = apollo_goal.x, apollo_goal.y, apollo_goal.z
    dx = float(goal_x) - float(start_x)
    dy = float(goal_y) - float(start_y)
    length = math.hypot(dx, dy)
    if length <= 1e-6:
        return []
    heading = math.atan2(dy, dx)
    step = _float(step_m)
    if step is None or step <= 0.0:
        sample_count = 2
    else:
        sample_count = max(2, int(math.ceil(length / step)) + 1)
    samples: list[LocalizationProjectionSample] = []
    for index in range(sample_count):
        fraction = index / float(sample_count - 1) if sample_count > 1 else 0.0
        route_s = length * fraction
        metadata = {
            "route_index": index,
            "route_s": route_s,
            "route_length_m": length,
            "expected_route_distance_m": length,
            "scenario_goal_source": str(payload.get("source") or ""),
            "scenario_goal_frame": str(payload.get("frame") or ""),
            "scenario_goal_sample_source": goal_source,
            "route_heading_source": "scenario_goal_chord",
        }
        samples.append(
            LocalizationProjectionSample(
                timestamp=None,
                x=float(start_x) + dx * fraction,
                y=float(start_y) + dy * fraction,
                heading=heading,
                source_artifact=str(path),
                source_index=index,
                sample_type="route",
                metadata=metadata,
            )
        )
    return samples


def _samples_from_scenario_metadata(
    path: Path,
    *,
    frame_transform: ApolloFrameTransform | None,
    step_m: float | None,
) -> list[LocalizationProjectionSample]:
    if frame_transform is None or not path.exists():
        return []
    payload = _read_json_mapping(path)
    if not payload:
        return []
    route_trace = payload.get("route_trace")
    if isinstance(route_trace, SequenceABC) and not isinstance(route_trace, (str, bytes)):
        return _samples_from_route_trace_points(
            route_trace,
            source_artifact=f"{path}:route_trace",
            frame_transform=frame_transform,
        )

    alignment = payload.get("front_alignment") if isinstance(payload.get("front_alignment"), Mapping) else {}
    if alignment and alignment.get("aligned") is not True:
        return []
    spawn = payload.get("spawn") if isinstance(payload.get("spawn"), Mapping) else {}
    front_spawn = payload.get("front_spawn") if isinstance(payload.get("front_spawn"), Mapping) else {}
    start_x = _first_number(spawn, ["x", "carla_x"])
    start_y = _first_number(spawn, ["y", "carla_y"])
    start_z = _first_number(spawn, ["z", "carla_z"]) or 0.0
    goal_x = _first_number(front_spawn, ["x", "carla_x"])
    goal_y = _first_number(front_spawn, ["y", "carla_y"])
    goal_z = _first_number(front_spawn, ["z", "carla_z"]) or start_z
    if start_x is None or start_y is None or goal_x is None or goal_y is None:
        return []

    start_apollo = carla_point_to_apollo(Vector3(x=start_x, y=start_y, z=start_z), frame_transform)
    goal_apollo = carla_point_to_apollo(Vector3(x=goal_x, y=goal_y, z=goal_z), frame_transform)
    dx = goal_apollo.x - start_apollo.x
    dy = goal_apollo.y - start_apollo.y
    chord_length = math.hypot(dx, dy)
    length = _first_number(
        alignment,
        ["longitudinal_m", "euclidean_m", "distance_m"],
    )
    if length is None:
        length = _first_number(payload, ["front_waypoint_ahead_m", "target_ahead_m"])
    if length is None:
        length = chord_length
    if chord_length <= 1e-6 or length <= 0.0:
        return []
    chord_heading = math.atan2(dy, dx)
    trace_heading = _carla_heading_to_apollo(
        _first_number(spawn, ["yaw_rad", "heading", "yaw"])
        if _first_number(spawn, ["yaw_rad", "heading", "yaw"]) is not None
        else _yaw_deg_to_rad(spawn.get("yaw_deg")),
        frame_transform,
    )
    step = _float(step_m)
    sample_count = max(2, int(math.ceil(length / step)) + 1) if step and step > 0.0 else 2
    samples: list[LocalizationProjectionSample] = []
    for index in range(sample_count):
        fraction = index / float(sample_count - 1) if sample_count > 1 else 0.0
        route_s = float(length) * fraction
        carla_x = float(start_x) + (float(goal_x) - float(start_x)) * fraction
        carla_y = float(start_y) + (float(goal_y) - float(start_y)) * fraction
        carla_z = float(start_z) + (float(goal_z) - float(start_z)) * fraction
        metadata = {
            "route_index": index,
            "route_s": route_s,
            "route_length_m": float(length),
            "expected_route_distance_m": float(length),
            "carla_x": carla_x,
            "carla_y": carla_y,
            "carla_z": carla_z,
            "route_trace_heading_apollo": trace_heading,
            "route_chord_heading_apollo": chord_heading,
            "route_heading_source": "fixed_scene_front_alignment_chord",
            "scenario_metadata_source": "fixed_scene_front_alignment",
            "front_alignment_longitudinal_m": alignment.get("longitudinal_m"),
            "front_alignment_lateral_m": alignment.get("lateral_m"),
        }
        samples.append(
            LocalizationProjectionSample(
                timestamp=None,
                x=start_apollo.x + dx * fraction,
                y=start_apollo.y + dy * fraction,
                heading=chord_heading,
                source_artifact=str(path),
                source_index=index,
                sample_type="route",
                metadata={key: value for key, value in metadata.items() if value is not None},
            )
        )
    return samples


def _samples_from_route_trace_points(
    points: SequenceABC,
    *,
    source_artifact: str,
    frame_transform: ApolloFrameTransform,
) -> list[LocalizationProjectionSample]:
    chord_headings = _route_chord_headings(
        points,
        frame_transform=frame_transform,
        points_are_carla_frame=True,
    )
    samples: list[LocalizationProjectionSample] = []
    for index, item in enumerate(points):
        if not isinstance(item, Mapping):
            continue
        x = _first_number(item, ["x", "carla_x"])
        y = _first_number(item, ["y", "carla_y"])
        z = _first_number(item, ["z", "carla_z"]) or 0.0
        if x is None or y is None:
            continue
        apollo_point = carla_point_to_apollo(Vector3(x=x, y=y, z=z), frame_transform)
        trace_heading = _carla_heading_to_apollo(item.get("heading"), frame_transform)
        chord_heading = chord_headings.get(index)
        metadata = _route_sample_metadata(item, index=index)
        metadata.update(
            {
                "carla_x": x,
                "carla_y": y,
                "carla_z": z,
                "route_trace_heading_apollo": trace_heading,
                "route_chord_heading_apollo": chord_heading,
                "route_heading_source": "route_chord" if chord_heading is not None else "route_trace",
            }
        )
        samples.append(
            LocalizationProjectionSample(
                timestamp=_first_number(item, ["sim_time_sec", "timestamp", "time_sec"]),
                x=apollo_point.x,
                y=apollo_point.y,
                heading=chord_heading if chord_heading is not None else trace_heading,
                source_artifact=source_artifact,
                source_index=index,
                sample_type="route",
                metadata={key: value for key, value in metadata.items() if value is not None},
            )
        )
    return samples


def _carla_heading_to_apollo(value: Any, frame_transform: ApolloFrameTransform) -> float | None:
    heading = _float(value)
    if heading is None:
        return None
    return carla_forward_to_heading(
        Vector3(x=math.cos(heading), y=math.sin(heading), z=0.0),
        frame_transform,
    )


def _yaw_deg_to_rad(value: Any) -> float | None:
    number = _float(value)
    return math.radians(number) if number is not None else None


def _heading_error_from_metadata(
    metadata: Mapping[str, Any],
    key: str,
    lane_heading: Any,
) -> float | None:
    heading = _float(metadata.get(key))
    lane = _float(lane_heading)
    if heading is None or lane is None:
        return None
    return normalize_angle(float(heading) - float(lane))


def _route_chord_headings(
    points: SequenceABC,
    *,
    frame_transform: ApolloFrameTransform | None,
    points_are_carla_frame: bool,
) -> dict[int, float]:
    coords: dict[int, tuple[float, float]] = {}
    for index, item in enumerate(points):
        if not isinstance(item, Mapping):
            continue
        x = _first_number(item, ["x", "localization_x", "map_x", "carla_x"])
        y = _first_number(item, ["y", "localization_y", "map_y", "carla_y"])
        z = _first_number(item, ["z", "localization_z", "map_z", "carla_z"]) or 0.0
        if x is None or y is None:
            continue
        if points_are_carla_frame:
            if frame_transform is None:
                continue
            point = carla_point_to_apollo(Vector3(x=x, y=y, z=z), frame_transform)
            coords[index] = (point.x, point.y)
        else:
            coords[index] = (float(x), float(y))
    if len(coords) < 2:
        return {}
    indexes = sorted(coords)
    headings: dict[int, float] = {}
    for pos, index in enumerate(indexes):
        prev_index = indexes[pos - 1] if pos > 0 else None
        next_index = indexes[pos + 1] if pos + 1 < len(indexes) else None
        if prev_index is None and next_index is None:
            continue
        start_index = prev_index if prev_index is not None else index
        end_index = next_index if next_index is not None else index
        start = coords[start_index]
        end = coords[end_index]
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        if abs(dx) < 1e-9 and abs(dy) < 1e-9:
            continue
        headings[index] = math.atan2(dy, dx)
    return headings


def _densify_route_samples(
    samples: Sequence[LocalizationProjectionSample],
    *,
    step_m: float | None,
) -> list[LocalizationProjectionSample]:
    step = _float(step_m)
    if step is None or step <= 0.0 or len(samples) < 2:
        return list(samples)
    densified: list[LocalizationProjectionSample] = []
    for index, sample in enumerate(samples):
        densified.append(sample)
        if index + 1 >= len(samples):
            continue
        next_sample = samples[index + 1]
        if sample.sample_type != "route" or next_sample.sample_type != "route":
            continue
        lane_key = str(sample.metadata.get("carla_lane_key") or "").strip()
        next_lane_key = str(next_sample.metadata.get("carla_lane_key") or "").strip()
        if not lane_key or lane_key != next_lane_key:
            continue
        heading_delta = _route_segment_heading_delta(sample, next_sample)
        if (
            heading_delta is None
            or heading_delta > ROUTE_DENSIFICATION_MAX_HEADING_DELTA_RAD
        ):
            continue
        distance = _route_segment_distance(sample, next_sample)
        if distance is None or distance <= step:
            continue
        insert_count = max(0, int(math.floor(distance / step)) - 1)
        if insert_count <= 0:
            continue
        segment_heading = math.atan2(next_sample.y - sample.y, next_sample.x - sample.x)
        for offset in range(1, insert_count + 1):
            fraction = offset / float(insert_count + 1)
            metadata = dict(sample.metadata)
            metadata.update(
                {
                    "route_index": _interpolate_optional_number(
                        sample.metadata.get("route_index"),
                        next_sample.metadata.get("route_index"),
                        fraction,
                    ),
                    "route_s": _interpolate_optional_number(
                        sample.metadata.get("route_s"),
                        next_sample.metadata.get("route_s"),
                        fraction,
                    ),
                    "route_chord_heading_apollo": segment_heading,
                    "route_heading_source": "densified_route_chord",
                    "route_sample_step_m": step,
                    "densified_route_sample": True,
                    "densified_from_route_indices": [
                        sample.metadata.get("route_index", sample.source_index),
                        next_sample.metadata.get("route_index", next_sample.source_index),
                    ],
                }
            )
            for axis in ("x", "y", "z"):
                key = f"carla_{axis}"
                value = _interpolate_optional_number(
                    sample.metadata.get(key),
                    next_sample.metadata.get(key),
                    fraction,
                )
                if value is not None:
                    metadata[key] = value
            trace_heading = _interpolate_optional_angle(
                sample.metadata.get("route_trace_heading_apollo"),
                next_sample.metadata.get("route_trace_heading_apollo"),
                fraction,
            )
            if trace_heading is not None:
                metadata["route_trace_heading_apollo"] = trace_heading
            carla_heading = _interpolate_optional_angle(
                sample.metadata.get("carla_heading"),
                next_sample.metadata.get("carla_heading"),
                fraction,
            )
            if carla_heading is not None:
                metadata["carla_heading"] = carla_heading
            source_index = int(sample.source_index * 1000 + offset)
            densified.append(
                LocalizationProjectionSample(
                    timestamp=_interpolate_optional_number(
                        sample.timestamp,
                        next_sample.timestamp,
                        fraction,
                    ),
                    x=sample.x + (next_sample.x - sample.x) * fraction,
                    y=sample.y + (next_sample.y - sample.y) * fraction,
                    heading=segment_heading,
                    source_artifact=sample.source_artifact,
                    source_index=source_index,
                    sample_type="route",
                    metadata={key: value for key, value in metadata.items() if value is not None},
                )
            )
    return densified


def _route_segment_distance(
    sample: LocalizationProjectionSample,
    next_sample: LocalizationProjectionSample,
) -> float | None:
    route_s = _float(sample.metadata.get("route_s"))
    next_route_s = _float(next_sample.metadata.get("route_s"))
    if route_s is not None and next_route_s is not None and next_route_s > route_s:
        return next_route_s - route_s
    distance = math.hypot(next_sample.x - sample.x, next_sample.y - sample.y)
    return distance if distance > 0.0 else None


def _route_segment_heading_delta(
    sample: LocalizationProjectionSample,
    next_sample: LocalizationProjectionSample,
) -> float | None:
    first = _float(sample.metadata.get("route_trace_heading_apollo"))
    second = _float(next_sample.metadata.get("route_trace_heading_apollo"))
    if first is None or second is None:
        first = _float(sample.heading)
        second = _float(next_sample.heading)
    if first is None or second is None:
        return None
    return abs(normalize_angle(second - first))


def _interpolate_optional_number(first: Any, second: Any, fraction: float) -> float | None:
    a = _float(first)
    b = _float(second)
    if a is None or b is None:
        return None
    return a + (b - a) * fraction


def _interpolate_optional_angle(first: Any, second: Any, fraction: float) -> float | None:
    a = _float(first)
    b = _float(second)
    if a is None or b is None:
        return None
    return normalize_angle(a + normalize_angle(b - a) * fraction)


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
    # Prefer simulation time when both wall-time `timestamp` and `sim_time_sec`
    # are present in bridge debug rows. Claim-grade projection evidence should
    # stay on the same time base as localization/chassis/planning contracts.
    timestamp = _first_number(payload, ["sim_time_sec", "measurement_time", "ts_sec", "timestamp"])
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


def _route_sample_metadata(item: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    lane_id = _route_sample_lane_id(item)
    metadata: dict[str, Any] = {
        "route_index": index,
        "route_s": _first_number(item, ["s", "route_s", "distance_s"]),
        "carla_lane_id": lane_id,
        "carla_lane_key": _carla_lane_key(lane_id),
    }
    return {key: value for key, value in metadata.items() if value is not None}


def _route_sample_lane_id(item: Mapping[str, Any]) -> str | None:
    lane_id = item.get("lane_id")
    if lane_id is not None and str(lane_id).strip():
        return str(lane_id)
    road = item.get("road_id")
    section = item.get("section_id", 0)
    lane = item.get("lane")
    if lane is None:
        lane = item.get("lane_id_value")
    if road is None or lane is None:
        return None
    return f"{road}:{section}:{lane}"


def _carla_lane_key(lane_id: str | None) -> str | None:
    if not lane_id:
        return None
    text = str(lane_id).strip()
    if not text:
        return None
    sep = ":" if ":" in text else "_"
    parts = text.split(sep)
    if len(parts) < 3:
        return text
    return f"{parts[0]}:{parts[-1]}"


def _dedupe_samples(samples: Sequence[LocalizationProjectionSample]) -> list[LocalizationProjectionSample]:
    deduped: list[LocalizationProjectionSample] = []
    seen: set[tuple[float, float, float | None]] = set()
    for sample in samples:
        key = (
            round(float(sample.x), 6),
            round(float(sample.y), 6),
            round(float(sample.heading), 6) if sample.heading is not None else None,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(sample)
    return deduped


def _limit_samples(
    samples: Sequence[LocalizationProjectionSample],
    *,
    max_samples: int | None,
) -> list[LocalizationProjectionSample]:
    selected = list(samples)
    if max_samples is None or max_samples <= 0 or len(selected) <= max_samples:
        return selected
    route_indexes = [index for index, sample in enumerate(selected) if sample.sample_type == "route"]
    pinned_indexes = [
        index for index, sample in enumerate(selected) if sample.sample_type in {"start", "goal"}
    ]
    if route_indexes:
        return _limit_samples_with_route_priority(
            selected,
            route_indexes=route_indexes,
            pinned_indexes=pinned_indexes,
            max_samples=max_samples,
        )
    if max_samples == 1:
        return [selected[-1]]
    step = (len(selected) - 1) / float(max_samples - 1)
    indexes = sorted({round(i * step) for i in range(max_samples)})
    return [selected[index] for index in indexes]


def _limit_samples_with_route_priority(
    samples: Sequence[LocalizationProjectionSample],
    *,
    route_indexes: Sequence[int],
    pinned_indexes: Sequence[int],
    max_samples: int,
) -> list[LocalizationProjectionSample]:
    pinned = sorted(set(pinned_indexes))
    if len(pinned) >= max_samples:
        return [samples[index] for index in _limit_indexes(pinned, max_samples)]
    remaining = max_samples - len(pinned)
    other_indexes = [
        index
        for index, sample in enumerate(samples)
        if index not in set(pinned) and sample.sample_type != "route"
    ]
    route_budget = min(len(route_indexes), max(1, int(math.ceil(remaining * 0.8))))
    other_budget = max(0, remaining - route_budget)
    if len(other_indexes) < other_budget:
        route_budget = min(len(route_indexes), route_budget + other_budget - len(other_indexes))
        other_budget = len(other_indexes)
    elif len(route_indexes) < route_budget:
        other_budget = min(len(other_indexes), other_budget + route_budget - len(route_indexes))
        route_budget = len(route_indexes)
    selected_indexes = set(pinned)
    selected_indexes.update(_limit_indexes(route_indexes, route_budget))
    selected_indexes.update(_limit_indexes(other_indexes, other_budget))
    if len(selected_indexes) < max_samples:
        remaining_indexes = [
            index for index in range(len(samples)) if index not in selected_indexes
        ]
        selected_indexes.update(
            _limit_indexes(remaining_indexes, max_samples - len(selected_indexes))
        )
    return [samples[index] for index in sorted(selected_indexes)]


def _limit_indexes(indexes: Sequence[int], max_count: int) -> list[int]:
    selected = list(indexes)
    if max_count <= 0:
        return []
    if len(selected) <= max_count:
        return selected
    if max_count == 1:
        return [selected[-1]]
    step = (len(selected) - 1) / float(max_count - 1)
    return [selected[index] for index in sorted({round(i * step) for i in range(max_count)})]


def _status_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _export_failure_reason(
    *,
    rows: Sequence[Mapping[str, Any]],
    non_ok_rows: Sequence[Mapping[str, Any]],
    projection_s_coverage_m: float | None,
    min_route_s_coverage: float | None,
) -> str | None:
    if not rows:
        return "localization_samples_missing"
    if non_ok_rows:
        counts = _status_counts(non_ok_rows)
        return "non_ok_projection_rows:" + ",".join(f"{key}={value}" for key, value in sorted(counts.items()))
    if (
        min_route_s_coverage is not None
        and projection_s_coverage_m is not None
        and projection_s_coverage_m < min_route_s_coverage
    ):
        return "route_s_coverage_below_requested_min"
    return None


def _status_from_map_xysl_output(output: str) -> str:
    lowered = (output or "").lower()
    if (
        "container" in lowered
        and (
            "is not running" in lowered
            or "no such container" in lowered
            or "is paused" in lowered
        )
    ) or "cannot connect to the docker daemon" in lowered:
        return "environment_unavailable"
    if "no such file or directory" in lowered or "command not found" in lowered:
        return "environment_unavailable"
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
