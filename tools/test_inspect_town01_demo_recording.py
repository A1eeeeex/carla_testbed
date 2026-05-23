from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_town01_demo_recording import build_inspection, write_json, write_markdown


def _write_route_run(root: Path, route_id: str, *, carla_frames: int, dreamview_ok: bool = False) -> Path:
    run_dir = root / f"{route_id}_run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(json.dumps({"route_id": route_id}), encoding="utf-8")
    raw_tp = run_dir / "video" / "dual_cam" / "raw_tp"
    raw_tp.mkdir(parents=True, exist_ok=True)
    for index in range(carla_frames):
        (raw_tp / f"{index:06d}.png").write_bytes(b"png")
    if dreamview_ok:
        video = run_dir / "video" / "dreamview" / "dreamview_capture.mp4"
        video.parent.mkdir(parents=True, exist_ok=True)
        video.write_bytes(b"mp4")
        manifest = {
            "recording_success": True,
            "recording_status": "success",
            "output_video_generated": True,
            "output_video_path": str(video),
            "frame_count": 12,
        }
    else:
        manifest = {
            "recording_success": False,
            "recording_status": "disabled",
            "output_video_generated": False,
            "frame_count": 0,
        }
    (artifacts / "dreamview_capture_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (artifacts / "dreamview_recording_status.json").write_text(json.dumps(manifest), encoding="utf-8")
    return run_dir


class InspectTown01DemoRecordingTests(unittest.TestCase):
    def test_ready_when_carla_required_and_dreamview_optional(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_route_run(root, "town01_rh_spawn097_goal046", carla_frames=15, dreamview_ok=False)

            payload = build_inspection(root, require_carla=True, require_dreamview=False, min_carla_frames=10)

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload["carla_ok_count"], 1)
            self.assertEqual(payload["dreamview_ok_count"], 0)

    def test_requires_dreamview_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_route_run(root, "town01_rh_spawn097_goal046", carla_frames=15, dreamview_ok=False)

            payload = build_inspection(root, require_carla=True, require_dreamview=True)

            self.assertEqual(payload["status"], "missing_dreamview_recording")

    def test_ready_with_dreamview_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_route_run(root, "town01_rh_spawn097_goal046", carla_frames=15, dreamview_ok=True)

            payload = build_inspection(root, require_carla=True, require_dreamview=True)

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload["dreamview_ok_count"], 1)

    def test_missing_carla_recording(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_route_run(root, "town01_rh_spawn097_goal046", carla_frames=2, dreamview_ok=True)

            payload = build_inspection(root, require_carla=True, require_dreamview=False, min_carla_frames=10)

            self.assertEqual(payload["status"], "missing_carla_recording")

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_route_run(root, "town01_rh_spawn097_goal046", carla_frames=15, dreamview_ok=True)
            payload = build_inspection(root, require_carla=True, require_dreamview=True)
            json_out = root / "inspection.json"
            md_out = root / "inspection.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "ready")
            self.assertIn("Town01 Demo Recording Inspection", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
