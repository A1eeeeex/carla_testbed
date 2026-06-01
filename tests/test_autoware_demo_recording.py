import json
from pathlib import Path

from carla_testbed.record.autoware_operator_view import (
    AutowareOperatorViewRecorder,
    parse_capture_region,
)


class FakeProcess:
    def __init__(self):
        self.terminated = False
        self.killed = False

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        return 0

    def kill(self):
        self.killed = True


def test_operator_view_builds_rviz_and_ffmpeg_commands(tmp_path: Path) -> None:
    recorder = AutowareOperatorViewRecorder(
        run_dir=tmp_path,
        artifacts_dir=tmp_path / "artifacts",
        config={
            "enabled": True,
            "display": ":0",
            "capture_region": "1280x720+10,20",
            "fps": 12,
            "docker_image": "autoware:test",
        },
        ros_domain_id=7,
        rmw_implementation="rmw_cyclonedds_cpp",
        which=lambda name: f"/usr/bin/{name}",
    )

    assert parse_capture_region("1280x720+10,20") == (1280, 720, 10, 20)
    rviz_cmd = recorder.build_rviz_command()
    ffmpeg_cmd = recorder.build_ffmpeg_command()

    assert rviz_cmd[:3] == ["docker", "run", "--rm"]
    assert "autoware:test" in rviz_cmd
    assert "ROS_DOMAIN_ID=7" in rviz_cmd
    assert "rviz2 -d /rviz/autoware_operator_view.rviz" in rviz_cmd[-1]
    assert ffmpeg_cmd[:2] == ["ffmpeg", "-y"]
    assert "x11grab" in ffmpeg_cmd
    assert ":0+10,20" in ffmpeg_cmd
    assert str(tmp_path / "video" / "rviz" / "autoware_rviz.mp4") in ffmpeg_cmd


def test_operator_view_missing_display_gracefully_writes_failed_status(tmp_path: Path) -> None:
    recorder = AutowareOperatorViewRecorder(
        run_dir=tmp_path,
        artifacts_dir=tmp_path / "artifacts",
        config={"enabled": True, "display": ""},
        env={"DISPLAY": ""},
        which=lambda name: f"/usr/bin/{name}",
    )

    recorder.start()

    status = json.loads((tmp_path / "artifacts" / "autoware_operator_view_status.json").read_text())
    assert status["status"] == "failed"
    assert status["recording_success"] is False
    assert "DISPLAY is not set" in status["failure_messages"]


def test_operator_view_stop_marks_success_when_video_exists(tmp_path: Path) -> None:
    processes = []

    def fake_popen(*args, **kwargs):
        proc = FakeProcess()
        processes.append(proc)
        return proc

    recorder = AutowareOperatorViewRecorder(
        run_dir=tmp_path,
        artifacts_dir=tmp_path / "artifacts",
        config={"enabled": True, "display": ":0", "startup_delay_s": 0.0},
        env={"DISPLAY": ":0"},
        which=lambda name: f"/usr/bin/{name}",
        popen=fake_popen,
    )

    recorder.start()
    recorder.video_path.write_bytes(b"mp4")
    recorder.stop()

    status = json.loads((tmp_path / "artifacts" / "autoware_operator_view_status.json").read_text())
    assert len(processes) == 2
    assert status["status"] == "success"
    assert status["recording_success"] is True
