from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from carla_testbed.platform.plan import RunPlan


@dataclass(frozen=True)
class RecorderSpec:
    name: str
    recorder_type: str
    neutral: bool = False
    platform_specific: bool = False
    starts_runtime_process: bool = False
    expected_artifacts: list[str] = field(default_factory=list)
    claim_boundary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "recorder_type": self.recorder_type,
            "neutral": self.neutral,
            "platform_specific": self.platform_specific,
            "starts_runtime_process": self.starts_runtime_process,
            "expected_artifacts": list(self.expected_artifacts),
            "claim_boundary": dict(self.claim_boundary),
        }


class RecordingProfileError(ValueError):
    pass


def recording_artifacts_for_plan(plan: RunPlan, specs: Mapping[str, RecorderSpec]) -> list[str]:
    artifacts: list[str] = []
    for name in plan.recording.recorders:
        spec = specs.get(name)
        if spec is None:
            continue
        for artifact in spec.expected_artifacts:
            if artifact not in artifacts:
                artifacts.append(artifact)
    return artifacts
