import json
import subprocess
from pathlib import Path

from carla_testbed.record.autoware_rosbag import (
    AutowareRosbagRecorder,
    build_autoware_demo_rosbag_topics,
)


class FakeProcess:
    def __init__(self):
        self.terminated = False

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def send_signal(self, signum):
        self.terminated = True

    def wait(self, timeout=None):
        return 0

    def kill(self):
        pass


def _profile(tmp_path: Path):
    return {
        "record": {
            "rosbag": {
                "enable": True,
                "out": "rosbag2/autoware_demo",
                "include_tf": True,
                "include_clock": True,
                "auto_topics": True,
                "storage": "sqlite3",
            },
            "control_log": {
                "topic": "/control/command/control_cmd",
                "extra_topics": ["/planning/trajectory", "/planning/trajectory"],
            },
            "probe": {"topics": ["/clock", "/tf"]},
        }
    }


def test_autoware_rosbag_topics_are_deduped(tmp_path: Path) -> None:
    topics = build_autoware_demo_rosbag_topics(_profile(tmp_path))

    assert topics == [
        "/tf",
        "/tf_static",
        "/clock",
        "/control/command/control_cmd",
        "/planning/trajectory",
    ]


def test_autoware_rosbag_builds_docker_compose_command(tmp_path: Path) -> None:
    recorder = AutowareRosbagRecorder(
        compose_path=Path("compose.yaml"),
        run_dir=tmp_path,
        artifacts_dir=tmp_path / "artifacts",
        repo_root=tmp_path,
        profile=_profile(tmp_path),
        which=lambda name: f"/usr/bin/{name}",
    )

    command = recorder.build_command()
    joined = " ".join(command)

    assert command[:5] == ["docker", "compose", "-f", "compose.yaml", "exec"]
    assert "-T autoware" in joined
    assert "ros2 bag record" in joined
    assert "/planning/trajectory" in joined
    assert "-o /work/rosbag2/autoware_demo" in joined


def test_autoware_rosbag_stop_marks_success_when_bag_exists(tmp_path: Path) -> None:
    recorder = AutowareRosbagRecorder(
        compose_path=Path("compose.yaml"),
        run_dir=tmp_path,
        artifacts_dir=tmp_path / "artifacts",
        repo_root=tmp_path,
        profile=_profile(tmp_path),
        which=lambda name: f"/usr/bin/{name}",
        popen=lambda *args, **kwargs: FakeProcess(),
        run_cmd=lambda *args, **kwargs: subprocess.CompletedProcess(args, 0),
    )

    recorder.start()
    recorder.out_path.mkdir(parents=True)
    (recorder.out_path / "metadata.yaml").write_text("rosbag2_bagfile_information: {}\n")
    recorder.stop()

    status = json.loads((tmp_path / "artifacts" / "autoware_rosbag_status.json").read_text())
    assert status["status"] == "success"
    assert status["recording_success"] is True
    assert "/control/command/control_cmd" in status["topics"]
