from __future__ import annotations

from pathlib import Path

from tools import run_town01_demo_showcase as demo


def test_demo_showcase_dry_run_keeps_dreamview_automation_contract(tmp_path: Path) -> None:
    args = demo._build_parser().parse_args(
        [
            "--mode",
            "short",
            "--record-dreamview",
            "--dreamview-auto-open",
            "--dreamview-open-wait-page",
            "--dreamview-browser-cmd",
            "google-chrome --new-window --app={url}",
            "--dreamview-capture-mode",
            "tick_snapshot",
            "--dreamview-capture-region",
            "1280x720+0,0",
            "--dreamview-use-fixed-region",
            "--require-recording-ready",
            "--batch-root-parent",
            str(tmp_path),
            "--dry-run",
        ]
    )

    command = demo._build_command(args)
    joined = " ".join(command)

    assert "--dry-run" in command
    assert "--ticks" in command
    assert "420" in command
    assert "algo.apollo.dreamview.enabled=true" in command
    assert "algo.apollo.dreamview.auto_open=true" in command
    assert "algo.apollo.dreamview.open_wait_page_on_unreachable=true" in command
    assert "algo.apollo.dreamview.record.enabled=true" in command
    assert "algo.apollo.dreamview.record.capture_mode=tick_snapshot" in command
    assert "algo.apollo.dreamview.record.use_fixed_region=true" in command
    assert "algo.apollo.dreamview.record.capture_region.width=1280" in command
    assert "algo.apollo.dreamview.record.capture_region.height=720" in command
    assert "algo.apollo.dreamview.record.capture_region.offset_x=0" in command
    assert "algo.apollo.dreamview.record.capture_region.offset_y=0" in command
    assert "algo.apollo.dreamview.browser_cmd=google-chrome --new-window --app={url}" in joined


def test_demo_showcase_auto_browser_resolution_has_no_hard_chrome_dependency() -> None:
    def _missing(_: str) -> None:
        return None

    command, source = demo.resolve_dreamview_browser_cmd("auto", which=_missing)

    assert command == ""
    assert source == "auto:system-opener"
