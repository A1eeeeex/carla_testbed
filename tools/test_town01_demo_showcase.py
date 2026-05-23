from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools import run_town01_demo_showcase as demo


def _which_chrome(name: str) -> str | None:
    return "/usr/bin/google-chrome" if name == "google-chrome" else None


def _which_missing(name: str) -> str | None:
    return None


def _which_npx(name: str) -> str | None:
    return "/usr/bin/npx" if name == "npx" else None


class Town01DemoShowcaseTests(unittest.TestCase):
    def test_dreamview_recording_overrides_are_opt_in(self) -> None:
        parser = demo._build_parser()
        args = parser.parse_args(["--mode", "short"])

        cmd = demo._build_command(args)

        self.assertNotIn("--override", cmd)
        self.assertNotIn("algo.apollo.dreamview.record.enabled=true", cmd)

    def test_dreamview_recording_overrides_include_auto_open_and_region(self) -> None:
        parser = demo._build_parser()
        args = parser.parse_args(
            [
                "--mode",
                "short",
                "--record-dreamview",
                "--dreamview-auto-open",
                "--dreamview-browser-cmd",
                "google-chrome --new-window --app={url}",
                "--dreamview-capture-mode",
                "tick_snapshot",
                "--dreamview-window-title",
                "Apollo Dreamview",
                "--dreamview-capture-region",
                "1280x720+10,20",
                "--dreamview-use-fixed-region",
            ]
        )

        cmd = demo._build_command(args)
        joined = "\n".join(cmd)

        self.assertIn("algo.apollo.dreamview.enabled=true", cmd)
        self.assertIn("algo.apollo.dreamview.auto_open=true", cmd)
        self.assertIn("algo.apollo.dreamview.open_wait_page_on_unreachable=true", cmd)
        self.assertIn("algo.apollo.dreamview.record.enabled=true", cmd)
        self.assertIn("algo.apollo.dreamview.record.capture_mode=tick_snapshot", cmd)
        self.assertIn("algo.apollo.dreamview.record.use_fixed_region=true", cmd)
        self.assertIn("algo.apollo.dreamview.record.capture_region.width=1280", cmd)
        self.assertIn("algo.apollo.dreamview.record.capture_region.height=720", cmd)
        self.assertIn("algo.apollo.dreamview.record.capture_region.offset_x=10", cmd)
        self.assertIn("algo.apollo.dreamview.record.capture_region.offset_y=20", cmd)
        self.assertIn("algo.apollo.dreamview.browser_cmd=google-chrome --new-window --app={url}", joined)

    def test_dreamview_wait_page_can_be_disabled(self) -> None:
        parser = demo._build_parser()
        args = parser.parse_args(
            [
                "--mode",
                "short",
                "--dreamview-auto-open",
                "--no-dreamview-open-wait-page",
            ]
        )

        cmd = demo._build_command(args)

        self.assertIn("algo.apollo.dreamview.open_wait_page_on_unreachable=false", cmd)

    def test_invalid_dreamview_capture_region_fails_fast(self) -> None:
        with self.assertRaisesRegex(ValueError, "WIDTHxHEIGHT"):
            demo._parse_capture_region("1280x720")

    def test_showcase_manifest_collects_dreamview_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "some_run"
            (run_dir / "video" / "dual_cam" / "raw_tp").mkdir(parents=True)
            dreamview_video = run_dir / "video" / "dreamview" / "dreamview_capture.mp4"
            dreamview_video.parent.mkdir(parents=True)
            dreamview_video.write_bytes(b"fake")
            manifest = run_dir / "artifacts" / "dreamview_capture_manifest.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text("{}", encoding="utf-8")

            demo._write_showcase_manifest(
                root,
                mode="short",
                steps=["lane_keep:town01_rh_spawn097_goal046"],
                dreamview_record_requested=True,
            )

            payload = (root / "artifacts" / "town01_demo_showcase_manifest.json").read_text(
                encoding="utf-8"
            )
            self.assertIn("dreamview_record_requested", payload)
            self.assertIn("dreamview_capture.mp4", payload)
        self.assertIn("dreamview_capture_manifest.json", payload)

    def test_auto_browser_prefers_installed_app_command(self) -> None:
        command, source = demo.resolve_dreamview_browser_cmd("auto", which=_which_chrome)

        self.assertEqual(
            command,
            "google-chrome --new-window --app={url} --window-size=1280,720 --window-position=0,0",
        )
        self.assertEqual(source, "auto:google-chrome")

    def test_auto_browser_empty_command_uses_backend_system_opener_fallback(self) -> None:
        command, source = demo.resolve_dreamview_browser_cmd("auto", which=_which_missing)

        self.assertEqual(command, "")
        self.assertEqual(source, "auto:system-opener")

    def test_playwright_browser_alias_uses_skill_wrapper_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            wrapper = Path(tmpdir) / "playwright_cli.sh"
            wrapper.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

            command, source = demo.resolve_dreamview_browser_cmd(
                "playwright",
                which=_which_npx,
                playwright_wrapper=wrapper,
            )

            self.assertEqual(command, f"{wrapper} open {{url}} --headed")
            self.assertEqual(source, "playwright:skill-wrapper")

    def test_playwright_browser_alias_reports_unavailable(self) -> None:
        command, source = demo.resolve_dreamview_browser_cmd(
            "playwright",
            which=_which_missing,
            playwright_wrapper=Path("/missing/playwright_cli.sh"),
        )

        self.assertEqual(command, "")
        self.assertEqual(source, "playwright:unavailable")

    def test_recording_inspection_is_written_for_demo_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "lane097_run"
            artifacts = run_dir / "artifacts"
            raw_tp = run_dir / "video" / "dual_cam" / "raw_tp"
            dreamview_video = run_dir / "video" / "dreamview" / "dreamview_capture.mp4"
            artifacts.mkdir(parents=True)
            raw_tp.mkdir(parents=True)
            dreamview_video.parent.mkdir(parents=True)
            (run_dir / "summary.json").write_text('{"route_id":"town01_rh_spawn097_goal046"}', encoding="utf-8")
            for index in range(12):
                (raw_tp / f"{index:06d}.png").write_bytes(b"png")
            dreamview_video.write_bytes(b"mp4")
            manifest = {
                "recording_success": True,
                "recording_status": "success",
                "output_video_generated": True,
                "output_video_path": str(dreamview_video),
                "frame_count": 12,
            }
            (artifacts / "dreamview_capture_manifest.json").write_text(
                json.dumps(manifest),
                encoding="utf-8",
            )
            (artifacts / "dreamview_recording_status.json").write_text(
                json.dumps(manifest),
                encoding="utf-8",
            )

            payload = demo._write_recording_inspection(root, require_dreamview=True)

            self.assertEqual(payload["status"], "ready")
            self.assertTrue((root / "artifacts" / "town01_demo_recording_inspection.json").exists())
            self.assertTrue((root / "artifacts" / "town01_demo_recording_inspection.md").exists())


if __name__ == "__main__":
    unittest.main()
