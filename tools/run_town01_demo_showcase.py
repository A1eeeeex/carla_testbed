#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import shutil
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, List


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = (
    REPO_ROOT / "configs" / "io" / "examples" / "town01_apollo_route_health_behavior_recovery_stitcher_v1_demo.yaml"
)
DEFAULT_CHAIN = REPO_ROOT / "tools" / "run_town01_capability_online_chain.py"
DEFAULT_PYTHON = Path("/home/ubuntu/miniconda3/envs/carla16/bin/python3")
AUTO_BROWSER_CMD = "auto"
PLAYWRIGHT_BROWSER_CMD = "playwright"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.recording_manifest import (
    build_recording_manifest,
    render_recording_manifest_markdown,
    write_recording_manifest,
)
from carla_testbed.utils.town01_route_health import canonical_demo_steps
from tools.inspect_town01_demo_recording import (
    build_inspection as build_demo_recording_inspection,
    write_json as write_demo_inspection_json,
    write_markdown as write_demo_inspection_markdown,
)


def _default_python_exec() -> str:
    if DEFAULT_PYTHON.exists():
        return str(DEFAULT_PYTHON)
    return sys.executable


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record a Town01 CARLA demo batch using the strongest Town01 demo profile. "
            "Defaults to the canonical Town01 showcase core set: lane_keep 097 / lane_keep 217 / junction 031."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Town01 demo config to forward to the online chain.",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "short"),
        default="full",
        help="full keeps config ticks; short uses 420 ticks + 120 post-fail for faster smoke/demo checks.",
    )
    parser.add_argument(
        "--include-curve-diagnostic",
        action="store_true",
        help=(
            "Append the current curve diagnostic pair. These are useful for engineering demos, "
            "but they are not current capability-promotion evidence."
        ),
    )
    parser.add_argument(
        "--startup-profile",
        choices=(
            "default",
            "render_offscreen_no_ros2",
            "lowres_low_quality",
            "render_offscreen",
            "lowres_no_ros",
            "adaptive",
        ),
        default="render_offscreen_no_ros2",
    )
    parser.add_argument("--carla-world-ready-timeout-sec", type=float, default=180.0)
    parser.add_argument("--carla-launch-attempts", type=int, default=1)
    parser.add_argument(
        "--python-exec",
        default=_default_python_exec(),
        help="Python executable used to launch the Town01 online chain.",
    )
    parser.add_argument(
        "--batch-root-parent",
        type=Path,
        default=None,
        help="Optional parent directory for the generated chain batch.",
    )
    parser.add_argument(
        "--comparison-label-suffix",
        default="demo_showcase",
        help="Label suffix forwarded to the online chain for easier run discovery.",
    )
    parser.add_argument("--progress-update-sec", type=float, default=2.5)
    parser.add_argument("--no-prewarm-carla", action="store_true")
    parser.add_argument("--keep-carla-alive-at-end", action="store_true")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument(
        "--record-dreamview",
        action="store_true",
        help="Enable Apollo Dreamview recording for each route run. Default is off.",
    )
    parser.add_argument(
        "--dreamview-auto-open",
        action="store_true",
        help=(
            "Open Dreamview automatically before recording. Works with browser_cmd or xdg-open/gio. "
            "If Dreamview is still booting, a local auto-refresh wait page can be opened instead."
        ),
    )
    parser.add_argument(
        "--dreamview-open-wait-page",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "When --dreamview-auto-open is set and Dreamview is not reachable yet, open a local "
            "auto-refresh wait page instead of requiring manual browser navigation."
        ),
    )
    parser.add_argument(
        "--dreamview-browser-cmd",
        default=AUTO_BROWSER_CMD,
        help=(
            "Browser command used for auto-opening Dreamview. Use 'auto' to prefer an installed "
            "Chrome/Chromium/Firefox app-style command and otherwise fall back to xdg-open/gio. "
            "Use 'playwright' to open through the local Playwright CLI wrapper when available. "
            "Use {url} as a placeholder; without {url}, the URL is appended."
        ),
    )
    parser.add_argument(
        "--dreamview-capture-mode",
        choices=("tick_snapshot", "ffmpeg_realtime"),
        default="tick_snapshot",
        help="Dreamview recording mode. tick_snapshot is usually more deterministic for regression demos.",
    )
    parser.add_argument(
        "--dreamview-window-title",
        default="Dreamview",
        help="Window title used by the Dreamview recorder when mode=window.",
    )
    parser.add_argument(
        "--dreamview-capture-region",
        default="",
        help="Optional fixed capture region in WIDTHxHEIGHT+X,Y form, for example 1280x720+0,0.",
    )
    parser.add_argument(
        "--dreamview-use-fixed-region",
        action="store_true",
        help="Use --dreamview-capture-region directly instead of locating a Dreamview window.",
    )
    parser.add_argument(
        "--require-recording-ready",
        action="store_true",
        help="After a real run, exit non-zero unless the CARLA/Dreamview recording inspection is ready.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _selected_steps(include_curve_diagnostic: bool) -> List[str]:
    return list(canonical_demo_steps(include_curve_diagnostic=include_curve_diagnostic))


def _append_steps(cmd: List[str], steps: Iterable[str]) -> None:
    for item in steps:
        cmd.extend(["--step", item])


def _candidate_batch_root_parent(raw_parent: Path | None) -> Path:
    if raw_parent is not None:
        return raw_parent.expanduser().resolve()
    return (REPO_ROOT / "runs").resolve()


def _discover_batch_root(batch_root_parent: Path, started_at_ts: float) -> Path | None:
    candidates = [
        path
        for path in batch_root_parent.glob("town01_capability_online_chain_*")
        if path.is_dir() and path.stat().st_mtime >= (started_at_ts - 1.0)
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: path.stat().st_mtime)[-1]


def _expected_recording_outputs(batch_root: Path, steps: Iterable[str]) -> List[Path]:
    expected: List[Path] = []
    for item in steps:
        _, route_id = item.split(":", 1)
        route_token = route_id.strip()
        expected.extend(batch_root.glob(f"**/*{route_token}*/video/dual_cam/raw_tp"))
    if expected:
        return sorted(set(path.resolve() for path in expected))
    return [batch_root / "video" / "dual_cam" / "raw_tp"]


def _actual_recording_outputs(batch_root: Path) -> List[Path]:
    return sorted({path.resolve() for path in batch_root.glob("**/video/dual_cam/raw_tp")})


def _actual_dreamview_outputs(batch_root: Path) -> List[Path]:
    patterns = (
        "**/video/dreamview/*.mp4",
        "**/video/dreamview/frames",
        "**/artifacts/dreamview_capture_manifest.json",
        "**/artifacts/dreamview_recording_status.json",
    )
    outputs = []
    for pattern in patterns:
        outputs.extend(path.resolve() for path in batch_root.glob(pattern))
    return sorted(set(outputs))


def _write_showcase_manifest(
    batch_root: Path,
    *,
    mode: str,
    steps: Iterable[str],
    dreamview_record_requested: bool,
) -> None:
    actual_outputs = _actual_recording_outputs(batch_root)
    expected_outputs = actual_outputs or _expected_recording_outputs(batch_root, steps)
    dreamview_outputs = _actual_dreamview_outputs(batch_root)
    status = "completed" if actual_outputs else "missing_outputs"
    payload = build_recording_manifest(
        mode=mode,
        expected_outputs=expected_outputs,
        actual_outputs=actual_outputs,
        required_for_acceptance=False,
        status=status,
        extra={
            "batch_root": str(batch_root),
            "step_count": len(list(steps)),
            "steps": list(steps),
            "dreamview_record_requested": bool(dreamview_record_requested),
            "dreamview_outputs": [str(path) for path in dreamview_outputs],
        },
    )
    manifest_path = batch_root / "artifacts" / "town01_demo_showcase_manifest.json"
    markdown_path = batch_root / "artifacts" / "town01_demo_showcase_manifest.md"
    write_recording_manifest(manifest_path, payload)
    markdown_path.write_text(render_recording_manifest_markdown(payload), encoding="utf-8")


def _write_recording_inspection(
    batch_root: Path,
    *,
    require_dreamview: bool,
    min_carla_frames: int = 10,
    min_dreamview_frames: int = 10,
) -> dict:
    payload = build_demo_recording_inspection(
        batch_root,
        require_carla=True,
        require_dreamview=require_dreamview,
        min_carla_frames=min_carla_frames,
        min_dreamview_frames=min_dreamview_frames,
    )
    artifacts = batch_root / "artifacts"
    write_demo_inspection_json(artifacts / "town01_demo_recording_inspection.json", payload)
    write_demo_inspection_markdown(artifacts / "town01_demo_recording_inspection.md", payload)
    return payload


def _parse_capture_region(text: str) -> tuple[int, int, int, int] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    match = re.fullmatch(r"(\d+)x(\d+)\+(-?\d+),(-?\d+)", raw)
    if not match:
        raise ValueError("invalid --dreamview-capture-region, expected WIDTHxHEIGHT+X,Y")
    width, height, offset_x, offset_y = (int(part) for part in match.groups())
    if width <= 0 or height <= 0:
        raise ValueError("invalid --dreamview-capture-region, width/height must be positive")
    return width, height, offset_x, offset_y


def resolve_dreamview_browser_cmd(
    raw_cmd: str | None,
    *,
    which=shutil.which,
    playwright_wrapper: Path | None = None,
) -> tuple[str, str]:
    """Resolve the browser command without making Chrome a hard dependency."""
    raw = str(raw_cmd or "").strip()
    raw_lower = raw.lower()
    if raw_lower in {PLAYWRIGHT_BROWSER_CMD, "playwright-cli"}:
        wrapper = playwright_wrapper
        if wrapper is None:
            codex_home = Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex"))
            wrapper = codex_home / "skills" / "playwright" / "scripts" / "playwright_cli.sh"
        if wrapper.exists() and which("npx"):
            return f"{wrapper} open {{url}} --headed", "playwright:skill-wrapper"
        if which("playwright-cli"):
            return "playwright-cli open {url} --headed", "playwright:global-cli"
        return "", "playwright:unavailable"
    if raw and raw_lower != AUTO_BROWSER_CMD:
        return raw, "explicit"
    preferred = (
        ("google-chrome", "google-chrome --new-window --app={url} --window-size=1280,720 --window-position=0,0"),
        (
            "google-chrome-stable",
            "google-chrome-stable --new-window --app={url} --window-size=1280,720 --window-position=0,0",
        ),
        ("chromium-browser", "chromium-browser --new-window --app={url} --window-size=1280,720 --window-position=0,0"),
        ("chromium", "chromium --new-window --app={url} --window-size=1280,720 --window-position=0,0"),
        ("firefox", "firefox --new-window {url}"),
    )
    for binary, command in preferred:
        if which(binary):
            return command, f"auto:{binary}"
    # Empty command intentionally lets CyberRTBackend fall back to xdg-open/gio.
    return "", "auto:system-opener"


def _append_dreamview_overrides(cmd: List[str], args: argparse.Namespace) -> None:
    if not args.record_dreamview and not args.dreamview_auto_open:
        return
    browser_cmd, browser_source = resolve_dreamview_browser_cmd(args.dreamview_browser_cmd)
    overrides = [
        "algo.apollo.dreamview.enabled=true",
        "algo.apollo.dreamview.auto_start=true",
        f"algo.apollo.dreamview.auto_open={str(bool(args.dreamview_auto_open)).lower()}",
        f"algo.apollo.dreamview.open_wait_page_on_unreachable={str(bool(args.dreamview_open_wait_page)).lower()}",
        f"algo.apollo.dreamview.browser_cmd_source={browser_source}",
    ]
    if browser_cmd:
        overrides.append(f"algo.apollo.dreamview.browser_cmd={browser_cmd}")
    if args.record_dreamview:
        overrides.extend(
            [
                "algo.apollo.dreamview.record.enabled=true",
                f"algo.apollo.dreamview.record.capture_mode={args.dreamview_capture_mode}",
                "algo.apollo.dreamview.record.mode=window",
                f"algo.apollo.dreamview.record.window_title={args.dreamview_window_title}",
                "algo.apollo.dreamview.record.fallback_to_screen=true",
            ]
        )
        region = _parse_capture_region(args.dreamview_capture_region)
        if args.dreamview_use_fixed_region:
            overrides.append("algo.apollo.dreamview.record.use_fixed_region=true")
        if region is not None:
            width, height, offset_x, offset_y = region
            overrides.extend(
                [
                    f"algo.apollo.dreamview.record.capture_region.width={width}",
                    f"algo.apollo.dreamview.record.capture_region.height={height}",
                    f"algo.apollo.dreamview.record.capture_region.offset_x={offset_x}",
                    f"algo.apollo.dreamview.record.capture_region.offset_y={offset_y}",
                ]
            )
    for override in overrides:
        cmd.extend(["--override", override])


def _build_command(args: argparse.Namespace) -> List[str]:
    cmd: List[str] = [
        str(Path(args.python_exec).expanduser()),
        str(DEFAULT_CHAIN),
        "--enable-lateral",
        "--config",
        str(args.config.expanduser().resolve()),
        "--startup-profile",
        args.startup_profile,
        "--carla-world-ready-timeout-sec",
        str(args.carla_world_ready_timeout_sec),
        "--carla-launch-attempts",
        str(args.carla_launch_attempts),
        "--comparison-label-suffix",
        args.comparison_label_suffix,
        "--progress-update-sec",
        str(args.progress_update_sec),
    ]
    if args.batch_root_parent is not None:
        cmd.extend(["--batch-root-parent", str(args.batch_root_parent.expanduser().resolve())])
    if args.no_prewarm_carla:
        cmd.append("--no-prewarm-carla")
    if args.keep_carla_alive_at_end:
        cmd.append("--keep-carla-alive-at-end")
    if args.continue_on_failure:
        cmd.append("--continue-on-failure")
    if args.mode == "short":
        cmd.extend(["--ticks", "420", "--post-fail-steps", "120"])
    _append_dreamview_overrides(cmd, args)
    _append_steps(cmd, _selected_steps(args.include_curve_diagnostic))
    if args.dry_run:
        cmd.append("--dry-run")
    return cmd


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    cmd = _build_command(args)
    steps = _selected_steps(args.include_curve_diagnostic)
    print("[town01-demo] steps:")
    for index, item in enumerate(steps, start=1):
        print(f"  {index}. {item}")
    print("[town01-demo] command:")
    print("  " + " ".join(shlex.quote(part) for part in cmd))
    if args.dry_run:
        subprocess.run(cmd, check=True, cwd=str(REPO_ROOT))
        return
    started_at_ts = time.time()
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT))
    batch_root = _discover_batch_root(
        _candidate_batch_root_parent(args.batch_root_parent),
        started_at_ts,
    )
    if batch_root is not None:
        _write_showcase_manifest(
            batch_root,
            mode=args.mode,
            steps=steps,
            dreamview_record_requested=bool(args.record_dreamview),
        )
        print(f"[town01-demo] manifest: {batch_root / 'artifacts' / 'town01_demo_showcase_manifest.json'}")
        inspection = _write_recording_inspection(
            batch_root,
            require_dreamview=bool(args.record_dreamview),
        )
        print(f"[town01-demo] recording inspection: {batch_root / 'artifacts' / 'town01_demo_recording_inspection.json'}")
        print(f"[town01-demo] recording status: {inspection.get('status')}")
        if args.require_recording_ready and inspection.get("status") != "ready":
            raise SystemExit(2)


if __name__ == "__main__":
    main()
