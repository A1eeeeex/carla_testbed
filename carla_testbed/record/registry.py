from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from carla_testbed.platform.plan import RunPlan

from .base import RecorderSpec, RecordingProfileError, recording_artifacts_for_plan


@dataclass(frozen=True)
class RecordingResolution:
    profile: str
    recorders: list[RecorderSpec]
    expected_artifacts: list[str]
    record_manager_modes: list[str]
    external_recorders: list[str]
    warnings: list[str]

    def to_dict(self) -> dict:
        return {
            "profile": self.profile,
            "recorders": [spec.to_dict() for spec in self.recorders],
            "expected_artifacts": list(self.expected_artifacts),
            "record_manager_modes": list(self.record_manager_modes),
            "external_recorders": list(self.external_recorders),
            "warnings": list(self.warnings),
        }


class RecorderRegistry:
    """Metadata registry for recording intent.

    This complements the runtime `RecordManager`; it does not instantiate CARLA
    sensors, ffmpeg, RViz, Dreamview, ROS2 bagging, or CyberRT recording.
    """

    def __init__(self, specs: Mapping[str, RecorderSpec] | None = None) -> None:
        self._specs = dict(specs or default_recorder_specs())

    def get(self, name: str) -> RecorderSpec:
        key = _key(name)
        try:
            return self._specs[key]
        except KeyError as exc:
            available = ", ".join(self.names())
            raise RecordingProfileError(f"unknown recorder '{name}'. Available: {available}") from exc

    def resolve_plan(self, plan: RunPlan) -> RecordingResolution:
        specs: list[RecorderSpec] = []
        warnings: list[str] = []
        for name in plan.recording.recorders:
            try:
                specs.append(self.get(name))
            except RecordingProfileError as exc:
                warnings.append(str(exc))
        artifacts = recording_artifacts_for_plan(plan, self._specs)
        modes = _record_manager_modes(spec.name for spec in specs)
        external = [
            spec.name
            for spec in specs
            if spec.name not in RECORDER_TO_RECORD_MANAGER_MODE and spec.starts_runtime_process
        ]
        return RecordingResolution(
            profile=plan.recording.profile,
            recorders=specs,
            expected_artifacts=artifacts,
            record_manager_modes=modes,
            external_recorders=external,
            warnings=warnings,
        )

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(spec.name for spec in self._specs.values()))


def default_recorder_registry() -> RecorderRegistry:
    return RecorderRegistry()


RECORDER_TO_RECORD_MANAGER_MODE = {
    "carla_dual_cam": "dual_cam",
    "carla_hud": "hud",
}


def record_manager_modes_for_plan(plan: RunPlan) -> list[str]:
    return default_recorder_registry().resolve_plan(plan).record_manager_modes


def external_recorders_for_plan(plan: RunPlan) -> list[str]:
    return default_recorder_registry().resolve_plan(plan).external_recorders


def default_recorder_specs() -> dict[str, RecorderSpec]:
    specs = [
        RecorderSpec("neutral_manifest", "manifest", neutral=True, expected_artifacts=["manifest.json"]),
        RecorderSpec("neutral_events", "events", neutral=True, expected_artifacts=["events.jsonl"]),
        RecorderSpec("neutral_timeseries", "timeseries", neutral=True, expected_artifacts=["timeseries.csv"]),
        RecorderSpec(
            "control_apply_trace",
            "control_trace",
            neutral=True,
            expected_artifacts=["artifacts/control_apply_trace.jsonl"],
        ),
        RecorderSpec(
            "topic_publish_stats",
            "topic_stats",
            neutral=True,
            expected_artifacts=["artifacts/topic_publish_stats.jsonl"],
        ),
        RecorderSpec(
            "publish_gap_trace",
            "topic_gap_trace",
            neutral=True,
            expected_artifacts=["artifacts/publish_gap_trace.jsonl"],
        ),
        RecorderSpec("stack_logs", "logs", neutral=True, expected_artifacts=["logs/"]),
        RecorderSpec(
            "carla_dual_cam",
            "video",
            starts_runtime_process=True,
            expected_artifacts=["video/dual_cam/demo_third_person.mp4"],
            claim_boundary={"video_is_operator_evidence_only": True},
        ),
        RecorderSpec(
            "carla_hud",
            "video",
            starts_runtime_process=True,
            expected_artifacts=["video/hud/"],
            claim_boundary={"video_is_operator_evidence_only": True},
        ),
        RecorderSpec(
            "apollo_dreamview_capture",
            "operator_view",
            platform_specific=True,
            starts_runtime_process=True,
            expected_artifacts=["video/dreamview/apollo_dreamview.mp4"],
            claim_boundary={"operator_view_is_not_behavior_pass_evidence": True},
        ),
        RecorderSpec(
            "apollo_cyber_record",
            "cyber_record",
            platform_specific=True,
            starts_runtime_process=True,
            expected_artifacts=["cyber_record/"],
            claim_boundary={"record_archive_is_not_behavior_pass_evidence": True},
        ),
        RecorderSpec(
            "autoware_rviz_capture",
            "operator_view",
            platform_specific=True,
            starts_runtime_process=True,
            expected_artifacts=["video/rviz/autoware_rviz.mp4"],
            claim_boundary={"operator_view_is_not_behavior_pass_evidence": True},
        ),
        RecorderSpec(
            "autoware_rosbag2",
            "rosbag2",
            platform_specific=True,
            starts_runtime_process=True,
            expected_artifacts=["rosbag2/autoware_demo/"],
            claim_boundary={"rosbag_archive_is_not_behavior_pass_evidence": True},
        ),
    ]
    return {_key(spec.name): spec for spec in specs}


def _key(value: str) -> str:
    return str(value).strip().lower().replace("-", "_")


def _record_manager_modes(names) -> list[str]:
    modes: list[str] = []
    for name in names:
        mode = RECORDER_TO_RECORD_MANAGER_MODE.get(str(name))
        if mode and mode not in modes:
            modes.append(mode)
    return modes
