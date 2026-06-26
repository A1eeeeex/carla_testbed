from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tbio.backends.cyberrt import CyberRTBackend


class DreamviewOpenUrlTests(unittest.TestCase):
    def test_browser_cmd_url_placeholder_is_replaced_without_extra_url_arg(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "dreamview": {
                                "browser_cmd": "google-chrome --new-window --app={url}",
                            }
                        }
                    }
                }
            )
            captured_cmds = []

            class DummyProc:
                pass

            def fake_popen(cmd, **_kwargs):
                captured_cmds.append(list(cmd))
                return DummyProc()

            with mock.patch("tbio.backends.cyberrt.shutil.which", return_value=None), mock.patch(
                "tbio.backends.cyberrt.subprocess.Popen",
                side_effect=fake_popen,
            ):
                backend._open_url("http://127.0.0.1:8888", artifacts)

            self.assertEqual(
                captured_cmds[0],
                ["google-chrome", "--new-window", "--app=http://127.0.0.1:8888"],
            )
            self.assertIn("--app=http://127.0.0.1:8888", (artifacts / "dreamview_open.log").read_text())

    def test_browser_cmd_without_placeholder_appends_url_for_compatibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend({"algo": {"apollo": {"dreamview": {"browser_cmd": "firefox"}}}})
            captured_cmds = []

            class DummyProc:
                pass

            def fake_popen(cmd, **_kwargs):
                captured_cmds.append(list(cmd))
                return DummyProc()

            with mock.patch("tbio.backends.cyberrt.shutil.which", return_value=None), mock.patch(
                "tbio.backends.cyberrt.subprocess.Popen",
                side_effect=fake_popen,
            ):
                backend._open_url("http://127.0.0.1:8888", artifacts)

            self.assertEqual(captured_cmds[0], ["firefox", "http://127.0.0.1:8888"])

    def test_unreachable_dreamview_can_open_auto_refresh_wait_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "dreamview": {
                                "enabled": True,
                                "auto_start": True,
                                "auto_open": True,
                                "open_wait_page_on_unreachable": True,
                                "ready_timeout_sec": 0.0,
                                "browser_cmd": "browser --app={url}",
                            }
                        }
                    }
                }
            )
            captured_cmds = []

            class DummyProc:
                pass

            def fake_popen(cmd, **_kwargs):
                captured_cmds.append(list(cmd))
                return DummyProc()

            with mock.patch.object(backend, "_wait_for_tcp", return_value=False), mock.patch.object(
                backend,
                "_docker_enabled",
                return_value=False,
            ), mock.patch("tbio.backends.cyberrt.shutil.which", return_value=None), mock.patch(
                "tbio.backends.cyberrt.subprocess.Popen",
                side_effect=fake_popen,
            ):
                backend._maybe_start_dreamview(artifacts)

            wait_page = artifacts / "dreamview_wait_page.html"
            self.assertTrue(wait_page.exists())
            self.assertIn("Waiting for Apollo Dreamview", wait_page.read_text(encoding="utf-8"))
            self.assertEqual(
                captured_cmds[0][0:2],
                ["browser", "--app=file://" + str(wait_page.resolve())],
            )
            self.assertIn("file://", (artifacts / "dreamview_wait_page_open.log").read_text())
            self.assertEqual(
                (artifacts / "dreamview_wait_page_target.txt").read_text(encoding="utf-8").strip(),
                "http://localhost:8888",
            )

    def test_docker_dreamview_start_timeout_is_logged_and_nonfatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = Path(tmpdir)
            backend = CyberRTBackend(
                {
                    "algo": {
                        "apollo": {
                            "dreamview": {
                                "enabled": True,
                                "auto_start": True,
                                "auto_open": False,
                                "ready_timeout_sec": 0.0,
                                "start_timeout_sec": 0.25,
                            }
                        }
                    }
                }
            )
            backend._docker_container_name = "apollo_test"

            def fake_docker_exec(*_args, **kwargs):
                assert kwargs.get("timeout") == 0.25
                raise subprocess.TimeoutExpired(cmd="dreamview_start", timeout=0.25)

            with mock.patch.object(backend, "_wait_for_tcp", return_value=False), mock.patch.object(
                backend,
                "_docker_enabled",
                return_value=True,
            ), mock.patch.object(backend, "_docker_container", return_value="apollo_test"), mock.patch.object(
                backend,
                "_docker_ensure_dreamview_runtime_deps",
            ), mock.patch.object(
                backend,
                "_docker_modules_prefix",
                return_value="source apollo_env",
            ), mock.patch.object(
                backend,
                "_dreamview_start_cmd",
                return_value="dreamview_start",
            ), mock.patch.object(
                backend,
                "_docker_exec",
                side_effect=fake_docker_exec,
            ):
                backend._maybe_start_dreamview(artifacts)

            launch_log = (artifacts / "dreamview_launch.log").read_text(encoding="utf-8")
            self.assertIn("start_cmd=dreamview_start", launch_log)
            self.assertIn("start_timeout_sec=0.25", launch_log)
            self.assertIn("start_failed=", launch_log)
            self.assertFalse(backend._dreamview_started_by_backend)


if __name__ == "__main__":
    unittest.main()
