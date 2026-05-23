from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_town01_demo_readiness import build_readiness_payload, write_json, write_markdown


def _which_all(name: str) -> str | None:
    return f"/usr/bin/{name}"


def _which_missing(name: str) -> str | None:
    return None


def _which_system_opener_only(name: str) -> str | None:
    if name == "xdg-open":
        return "/usr/bin/xdg-open"
    if name == "ffmpeg":
        return "/usr/bin/ffmpeg"
    return None


def _import_ok(name: str) -> object:
    return object()


def _import_fail(name: str) -> object:
    raise ImportError(name)


class InspectTown01DemoReadinessTests(unittest.TestCase):
    def test_readiness_ready_with_all_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = root / "demo.yaml"
            chain = root / "chain.py"
            config.write_text("demo: true\n", encoding="utf-8")
            chain.write_text("print('ok')\n", encoding="utf-8")

            payload = build_readiness_payload(
                config_path=config,
                chain_path=chain,
                browser_cmd="google-chrome --new-window --app={url}",
                capture_region="1280x720+0,0",
                which=_which_all,
                importer=_import_ok,
                env={"DISPLAY": ":0"},
            )

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload["failed_count"], 0)
            self.assertIn("--dreamview-auto-open", payload["recommended_demo_command"])
            self.assertIn("--dreamview-open-wait-page", payload["recommended_demo_command"])

    def test_readiness_fails_on_missing_required_browser(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = root / "demo.yaml"
            chain = root / "chain.py"
            config.write_text("demo: true\n", encoding="utf-8")
            chain.write_text("print('ok')\n", encoding="utf-8")

            payload = build_readiness_payload(
                config_path=config,
                chain_path=chain,
                browser_cmd="missing-browser --app={url}",
                capture_region="1280x720+0,0",
                which=_which_missing,
                importer=_import_ok,
                env={"DISPLAY": ":0"},
            )

            self.assertEqual(payload["status"], "failed")
            self.assertGreaterEqual(payload["failed_count"], 1)

    def test_readiness_auto_browser_accepts_system_opener_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = root / "demo.yaml"
            chain = root / "chain.py"
            config.write_text("demo: true\n", encoding="utf-8")
            chain.write_text("print('ok')\n", encoding="utf-8")

            payload = build_readiness_payload(
                config_path=config,
                chain_path=chain,
                browser_cmd="auto",
                capture_region="1280x720+0,0",
                which=_which_system_opener_only,
                importer=_import_ok,
                env={"DISPLAY": ":0"},
            )

            self.assertEqual(payload["status"], "ready")
            browser_check = [item for item in payload["checks"] if item["name"] == "dreamview_browser_command"][0]
            self.assertEqual(browser_check["data"]["browser_source"], "auto:system-opener")

    def test_readiness_fails_when_playwright_alias_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = root / "demo.yaml"
            chain = root / "chain.py"
            config.write_text("demo: true\n", encoding="utf-8")
            chain.write_text("print('ok')\n", encoding="utf-8")

            payload = build_readiness_payload(
                config_path=config,
                chain_path=chain,
                browser_cmd="playwright",
                capture_region="1280x720+0,0",
                which=_which_system_opener_only,
                importer=_import_ok,
                env={"DISPLAY": ":0"},
            )

            self.assertEqual(payload["status"], "failed")
            browser_check = [item for item in payload["checks"] if item["name"] == "dreamview_browser_command"][0]
            self.assertEqual(browser_check["data"]["browser_source"], "playwright:unavailable")

    def test_readiness_fails_fast_on_invalid_region(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = root / "demo.yaml"
            chain = root / "chain.py"
            config.write_text("demo: true\n", encoding="utf-8")
            chain.write_text("print('ok')\n", encoding="utf-8")

            payload = build_readiness_payload(
                config_path=config,
                chain_path=chain,
                capture_region="1280x720",
                which=_which_all,
                importer=_import_ok,
                env={"DISPLAY": ":0"},
            )

            self.assertEqual(payload["status"], "failed")
            self.assertTrue(any(item["name"] == "dreamview_capture_region" for item in payload["checks"]))

    def test_readiness_detects_missing_pil_for_tick_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = root / "demo.yaml"
            chain = root / "chain.py"
            config.write_text("demo: true\n", encoding="utf-8")
            chain.write_text("print('ok')\n", encoding="utf-8")

            payload = build_readiness_payload(
                config_path=config,
                chain_path=chain,
                capture_mode="tick_snapshot",
                capture_region="1280x720+0,0",
                which=_which_all,
                importer=_import_fail,
                env={"DISPLAY": ":0"},
            )

            self.assertEqual(payload["status"], "failed")
            self.assertTrue(any(item["name"] == "dreamview_tick_snapshot_dependency" for item in payload["checks"]))

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "status": "ready",
                "failed_count": 0,
                "warning_count": 0,
                "checks": [],
                "recommended_demo_command": "python demo.py",
            }
            json_out = root / "readiness.json"
            md_out = root / "readiness.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "ready")
            self.assertIn("Town01 Demo Readiness", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
