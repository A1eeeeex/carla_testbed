from pathlib import Path

from tools.inspect_autoware_demo_recording import build_inspection


def _write_ready_run(run_dir: Path) -> None:
    (run_dir / "video" / "dual_cam").mkdir(parents=True)
    (run_dir / "video" / "dual_cam" / "demo_third_person.mp4").write_bytes(b"mp4")
    (run_dir / "video" / "rviz").mkdir(parents=True)
    (run_dir / "video" / "rviz" / "autoware_rviz.mp4").write_bytes(b"mp4")
    (run_dir / "rosbag2" / "autoware_demo").mkdir(parents=True)
    (run_dir / "rosbag2" / "autoware_demo" / "metadata.yaml").write_text("bag: {}\n")
    (run_dir / "artifacts").mkdir(parents=True)
    (run_dir / "artifacts" / "autoware_control.jsonl").write_text("{}\n")
    (run_dir / "summary.json").write_text("{}\n")
    (run_dir / "timeseries.csv").write_text("frame_id\n1\n")


def test_inspect_autoware_demo_recording_ready(tmp_path: Path) -> None:
    _write_ready_run(tmp_path)

    payload = build_inspection(tmp_path)

    assert payload["status"] == "ready"
    assert payload["ready_count"] == 1


def test_inspect_autoware_demo_recording_missing_rviz(tmp_path: Path) -> None:
    _write_ready_run(tmp_path)
    (tmp_path / "video" / "rviz" / "autoware_rviz.mp4").unlink()

    payload = build_inspection(tmp_path)

    assert payload["status"] == "missing_rviz_recording"
    assert payload["runs"][0]["checks"]["rviz_recording"] is False


def test_inspect_autoware_demo_recording_missing_rosbag(tmp_path: Path) -> None:
    _write_ready_run(tmp_path)
    (tmp_path / "rosbag2" / "autoware_demo" / "metadata.yaml").unlink()

    payload = build_inspection(tmp_path)

    assert payload["status"] == "missing_rosbag"
    assert payload["runs"][0]["checks"]["rosbag"] is False
