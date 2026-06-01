from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "tools" / "run_baguang_apollo_followstop_80kph_demo.sh"
BRIDGE = REPO_ROOT / "tools" / "apollo10_cyber_bridge" / "bridge.py"


class BaguangFollowStopDemoWrapperTest(unittest.TestCase):
    def test_shell_syntax_is_valid(self) -> None:
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)

    def test_records_third_person_carla_and_dreamview_when_enabled(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("--record-dual-cam-third-person-only", text)
        self.assertIn("RECORD_DREAMVIEW", text)
        self.assertIn('RECORD_DREAMVIEW="${RECORD_DREAMVIEW:-${RECORD_DEMO:-0}}"', text)
        self.assertIn("algo.apollo.dreamview.auto_open=", text)
        self.assertIn("algo.apollo.dreamview.record.enabled=true", text)
        self.assertIn("algo.apollo.dreamview.record.capture_mode=", text)
        self.assertIn("algo.apollo.dreamview.record.use_fixed_region=", text)
        self.assertIn("algo.apollo.dreamview.record.capture_region.width=", text)
        self.assertIn("open_dreamview_chrome_auto.sh {url}", text)
        self.assertIn("DREAMVIEW_CHROME_WIDTH", text)
        self.assertIn("DREAMVIEW_CHROME_HEIGHT", text)
        self.assertIn('if [[ -z "${DREAMVIEW_BROWSER_CMD:-}" ]]; then', text)
        self.assertNotIn("DREAMVIEW_BROWSER_CMD=\"${DREAMVIEW_BROWSER_CMD:-google-chrome", text)
        self.assertIn("acceptance.low_speed_creep.require_reached_speed_mps=5.0", text)

    def test_carla_direct_control_path_does_not_require_ros2_float32_message(self) -> None:
        text = BRIDGE.read_text(encoding="utf-8")
        self.assertIn("types.SimpleNamespace(data=[])", text)
        self.assertIn("control_publish_errors.jsonl", text)
        self.assertIn("control_publish_exception_count", text)
        self.assertIn("last_control_publish_error", text)


if __name__ == "__main__":
    unittest.main()
