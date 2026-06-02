from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

AUTOWARE_EVIDENCE_SCHEMA_VERSION = "autoware_evidence.v1"

TRAFFIC_LIGHT_SCENARIO_CLASSES = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}

DEFAULT_REQUIRED_ROSBAG_TOPICS = (
    "/clock",
    "/tf",
    "/tf_static",
)


def analyze_autoware_evidence_run_dir(
    run_dir: str | Path,
    *,
    required_rosbag_topics: Sequence[str] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    summary = _read_json(root / "summary.json")
    manifest = _read_json(root / "manifest.json")
    topics = tuple(required_rosbag_topics or _required_rosbag_topics(manifest))

    rviz_video = root / "video" / "rviz" / "autoware_rviz.mp4"
    carla_video = root / "video" / "dual_cam" / "demo_third_person.mp4"
    rosbag_dir = root / "rosbag2" / "autoware_demo"
    rosbag_metadata = rosbag_dir / "metadata.yaml"
    control_log = root / "artifacts" / "autoware_control.jsonl"
    summary_path = root / "summary.json"
    timeseries_path = _find_first(root, ("timeseries.csv", "timeseries.jsonl"))
    route_health_path = _find_first(root, ("analysis/route_health/route_health.json", "route_health.json"))
    channel_health_path = _find_first(
        root,
        (
            "analysis/autoware_channel_health/autoware_channel_health_report.json",
            "analysis/apollo_channel_health/apollo_channel_health_report.json",
            "autoware_channel_health_report.json",
            "apollo_channel_health_report.json",
        ),
    )
    control_health_path = _find_first(
        root,
        ("analysis/control_health/control_health_report.json", "control_health_report.json"),
    )
    control_attribution_path = _find_first(
        root,
        ("analysis/control_attribution/control_attribution_report.json", "control_attribution_report.json"),
    )
    natural_driving_path = _find_first(
        root,
        (
            "analysis/natural_driving/natural_driving_report.json",
            "natural_driving_report.json",
        ),
    )
    traffic_light_contract_path = _find_first(
        root,
        (
            "analysis/traffic_light/traffic_light_contract_report.json",
            "traffic_light_contract_report.json",
        ),
    )

    rosbag_present = _dir_has_bag(rosbag_dir)
    rosbag_inspected, rosbag_required_topics_present, missing_topics = _inspect_rosbag_topics(
        rosbag_metadata,
        topics,
        rosbag_present=rosbag_present,
    )
    scenario_class = _first_text(summary, "scenario_class", manifest, "scenario_class")
    route_id = _first_text(summary, "route_id", manifest, "route_id")
    scenario_id = _first_text(summary, "scenario_id", manifest, "scenario_id")
    duration_s = _first_value(summary, "duration_s", manifest, "duration_s")

    missing_artifacts = _missing_artifacts(
        {
            "rviz_video": _file_ok(rviz_video),
            "rosbag": rosbag_present,
            "carla_video": _file_ok(carla_video),
            "control_log": _file_ok(control_log),
            "summary": _file_ok(summary_path),
            "timeseries": timeseries_path is not None and _file_ok(timeseries_path),
            "route_health": route_health_path is not None,
            "channel_health": channel_health_path is not None,
            "control_health": control_health_path is not None,
            "control_attribution": control_attribution_path is not None,
            "natural_driving_report": natural_driving_path is not None,
        },
        scenario_class=scenario_class,
        traffic_light_contract_present=traffic_light_contract_path is not None,
    )
    warnings: list[str] = []
    if rosbag_present and not rosbag_inspected:
        warnings.append("rosbag_present_but_topics_not_inspected")
    if missing_topics:
        warnings.append("rosbag_required_topics_missing")
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and traffic_light_contract_path is None:
        warnings.append("traffic_light_scenario_missing_traffic_light_contract_report")

    recording_ready = _file_ok(rviz_video) and rosbag_present and _file_ok(carla_video)
    gate_artifacts_ready = all(
        path is not None
        for path in (
            route_health_path,
            channel_health_path,
            control_health_path,
            control_attribution_path,
            natural_driving_path,
        )
    )
    metadata_ready = bool(scenario_id and route_id and duration_s not in {None, ""})
    traffic_light_ready = (
        scenario_class not in TRAFFIC_LIGHT_SCENARIO_CLASSES
        or traffic_light_contract_path is not None
    )
    can_compare = bool(
        gate_artifacts_ready
        and metadata_ready
        and traffic_light_ready
        and _file_ok(summary_path)
        and timeseries_path is not None
    )
    status = _artifact_status(
        recording_ready=recording_ready,
        gate_artifacts_ready=gate_artifacts_ready,
        can_compare=can_compare,
    )

    return {
        "schema_version": AUTOWARE_EVIDENCE_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id", default=root.name),
        "scenario_id": scenario_id,
        "scenario_class": scenario_class,
        "route_id": route_id,
        "duration_s": _num(duration_s),
        "backend": "autoware",
        "rviz_video_present": _file_ok(rviz_video),
        "rosbag_present": rosbag_present,
        "rosbag_inspected": rosbag_inspected,
        "rosbag_required_topics_present": rosbag_required_topics_present,
        "carla_third_person_video_present": _file_ok(carla_video),
        "control_log_present": _file_ok(control_log),
        "summary_present": _file_ok(summary_path),
        "timeseries_present": timeseries_path is not None and _file_ok(timeseries_path),
        "route_health_present": route_health_path is not None,
        "channel_health_present": channel_health_path is not None,
        "control_health_present": control_health_path is not None,
        "control_attribution_present": control_attribution_path is not None,
        "natural_driving_report_present": natural_driving_path is not None,
        "traffic_light_contract_present": traffic_light_contract_path is not None,
        "artifact_completeness_status": status,
        "recording_artifacts_ready": recording_ready,
        "gate_artifacts_ready": gate_artifacts_ready,
        "can_compare_with_apollo": can_compare,
        "missing_artifacts": missing_artifacts,
        "missing_rosbag_topics": missing_topics,
        "warnings": warnings,
        "artifacts": {
            "rviz_video": str(rviz_video),
            "rosbag_dir": str(rosbag_dir),
            "carla_third_person_video": str(carla_video),
            "control_log": str(control_log),
            "summary": str(summary_path),
            "timeseries": None if timeseries_path is None else str(timeseries_path),
            "route_health": None if route_health_path is None else str(route_health_path),
            "channel_health": None if channel_health_path is None else str(channel_health_path),
            "control_health": None if control_health_path is None else str(control_health_path),
            "control_attribution": None if control_attribution_path is None else str(control_attribution_path),
            "natural_driving_report": None if natural_driving_path is None else str(natural_driving_path),
            "traffic_light_contract": None if traffic_light_contract_path is None else str(traffic_light_contract_path),
        },
        "interpretation_boundary": (
            "RViz, rosbag2, and CARLA third-person recordings prove operator/demo evidence only. "
            "Autoware can be compared with Apollo only when the same route/channel/control/"
            "natural-driving gate artifacts are present."
        ),
    }


def write_autoware_evidence_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "autoware_evidence_report.json"
    md_path = output / "autoware_evidence_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {
        "autoware_evidence_report": str(json_path),
        "autoware_evidence_summary": str(md_path),
    }


def _summary_markdown(report: Mapping[str, Any]) -> str:
    missing = [str(item) for item in report.get("missing_artifacts") or []]
    warnings = [str(item) for item in report.get("warnings") or []]
    lines = [
        "# Autoware Evidence Summary",
        "",
        f"- run_id: `{report.get('run_id')}`",
        f"- scenario_id: `{report.get('scenario_id')}`",
        f"- route_id: `{report.get('route_id')}`",
        f"- artifact_completeness_status: `{report.get('artifact_completeness_status')}`",
        f"- recording_artifacts_ready: `{report.get('recording_artifacts_ready')}`",
        f"- gate_artifacts_ready: `{report.get('gate_artifacts_ready')}`",
        f"- can_compare_with_apollo: `{report.get('can_compare_with_apollo')}`",
        "",
        "## Missing Artifacts",
        "",
    ]
    lines.extend(f"- `{item}`" for item in missing) if missing else lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- `{item}`" for item in warnings) if warnings else lines.append("- none")
    lines.extend(
        [
            "",
            "RViz/rosbag/CARLA video evidence is not a natural-driving pass by itself.",
            "",
        ]
    )
    return "\n".join(lines)


def _required_rosbag_topics(manifest: Mapping[str, Any]) -> tuple[str, ...]:
    record = manifest.get("record")
    rosbag = record.get("rosbag") if isinstance(record, Mapping) else None
    topics: list[str] = []
    if isinstance(rosbag, Mapping):
        for key in ("topics", "extra_topics", "probe_topics", "required_topics"):
            values = rosbag.get(key)
            if isinstance(values, str):
                topics.append(values)
            elif isinstance(values, Sequence):
                topics.extend(str(item) for item in values if item not in {None, ""})
    if not topics:
        topics.extend(DEFAULT_REQUIRED_ROSBAG_TOPICS)
    return tuple(dict.fromkeys(topics))


def _inspect_rosbag_topics(
    metadata_path: Path,
    required_topics: Sequence[str],
    *,
    rosbag_present: bool,
) -> tuple[bool, bool, list[str]]:
    if not rosbag_present or not metadata_path.exists():
        return False, False, list(required_topics)
    text = metadata_path.read_text(encoding="utf-8", errors="ignore")
    if not text.strip():
        return False, False, list(required_topics)
    missing = [topic for topic in required_topics if topic and topic not in text]
    return True, not missing, missing


def _missing_artifacts(
    present: Mapping[str, bool],
    *,
    scenario_class: str | None,
    traffic_light_contract_present: bool,
) -> list[str]:
    names = {
        "rviz_video": "video/rviz/autoware_rviz.mp4",
        "rosbag": "rosbag2/autoware_demo",
        "carla_video": "video/dual_cam/demo_third_person.mp4",
        "control_log": "artifacts/autoware_control.jsonl",
        "summary": "summary.json",
        "timeseries": "timeseries.csv or timeseries.jsonl",
        "route_health": "route_health.json",
        "channel_health": "channel_health_report.json",
        "control_health": "control_health_report.json",
        "control_attribution": "control_attribution_report.json",
        "natural_driving_report": "natural_driving_report.json",
    }
    missing = [name for key, name in names.items() if not present.get(key)]
    if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES and not traffic_light_contract_present:
        missing.append("traffic_light_contract_report.json")
    return missing


def _artifact_status(*, recording_ready: bool, gate_artifacts_ready: bool, can_compare: bool) -> str:
    if can_compare:
        return "ready_for_comparison"
    if recording_ready and not gate_artifacts_ready:
        return "recording_only"
    if gate_artifacts_ready and not recording_ready:
        return "gate_artifacts_only"
    return "incomplete"


def _file_ok(path: Path) -> bool:
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _dir_has_bag(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    if (path / "metadata.yaml").exists():
        return True
    return any(path.rglob("*.db3")) or any(path.rglob("*.mcap"))


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relative_paths}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _first_text(
    first: Mapping[str, Any],
    first_key: str,
    second: Mapping[str, Any] | None = None,
    second_key: str | None = None,
    *,
    default: str | None = None,
) -> str | None:
    value = _first_value(first, first_key, second or {}, second_key or first_key)
    if value in {None, ""}:
        return default
    return str(value)


def _first_value(
    first: Mapping[str, Any],
    first_key: str,
    second: Mapping[str, Any] | None = None,
    second_key: str | None = None,
) -> Any:
    value = first.get(first_key)
    if value not in {None, ""}:
        return value
    if second is None:
        return None
    return second.get(second_key or first_key)


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
