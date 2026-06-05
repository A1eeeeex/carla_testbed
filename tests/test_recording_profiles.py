from __future__ import annotations

from carla_testbed.platform.compiler import compile_run_plan
from carla_testbed.platform.registry import PlatformRegistry
from carla_testbed.record.registry import default_recorder_registry


def test_demo_recording_profile_adds_platform_specific_operator_view() -> None:
    registry = PlatformRegistry(repo_root=".")

    apollo = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="demo",
        gate="diagnostic",
        registry=registry,
    )
    autoware = compile_run_plan(
        platform="autoware_ros2",
        algorithm="autoware/universe_gt_localization",
        scenario="town01/lane_keep_097",
        recording="demo",
        gate="diagnostic",
        registry=registry,
    )

    assert "carla_dual_cam" in apollo.recording.recorders
    assert "apollo_dreamview_capture" in apollo.recording.recorders
    assert "autoware_rviz_capture" not in apollo.recording.recorders
    assert "autoware_rviz_capture" in autoware.recording.recorders
    assert "autoware_rosbag2" in autoware.recording.recorders
    assert autoware.recording.claim_boundary["recording_alone_is_not_evidence_of_natural_driving"] is True


def test_none_recording_profile_does_not_request_operator_video() -> None:
    plan = compile_run_plan(
        platform="apollo_cyberrt",
        algorithm="apollo/apollo10_carla_gt",
        scenario="town01/lane_keep_097",
        recording="none",
        gate="smoke",
        registry=PlatformRegistry(repo_root="."),
    )

    assert plan.recording.recorders == ["neutral_manifest", "neutral_events"]
    assert "apollo_dreamview_capture" not in plan.recording.recorders


def test_recording_registry_resolves_expected_artifacts_without_runtime() -> None:
    plan = compile_run_plan(
        platform="autoware_ros2",
        algorithm="autoware/universe_gt_localization",
        scenario="town01/lane_keep_097",
        recording="demo",
        gate="diagnostic",
        registry=PlatformRegistry(repo_root="."),
    )

    resolved = default_recorder_registry().resolve_plan(plan)

    assert "video/rviz/autoware_rviz.mp4" in resolved.expected_artifacts
    assert "rosbag2/autoware_demo/" in resolved.expected_artifacts
    assert resolved.warnings == []
