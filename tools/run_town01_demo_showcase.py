#!/usr/bin/env python3
from __future__ import annotations

import argparse
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

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.recording_manifest import (
    build_recording_manifest,
    render_recording_manifest_markdown,
    write_recording_manifest,
)
from carla_testbed.utils.town01_route_health import canonical_demo_steps


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


def _write_showcase_manifest(batch_root: Path, *, mode: str, steps: Iterable[str]) -> None:
    actual_outputs = _actual_recording_outputs(batch_root)
    expected_outputs = actual_outputs or _expected_recording_outputs(batch_root, steps)
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
        },
    )
    manifest_path = batch_root / "artifacts" / "town01_demo_showcase_manifest.json"
    markdown_path = batch_root / "artifacts" / "town01_demo_showcase_manifest.md"
    write_recording_manifest(manifest_path, payload)
    markdown_path.write_text(render_recording_manifest_markdown(payload), encoding="utf-8")


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
        _write_showcase_manifest(batch_root, mode=args.mode, steps=steps)
        print(f"[town01-demo] manifest: {batch_root / 'artifacts' / 'town01_demo_showcase_manifest.json'}")


if __name__ == "__main__":
    main()
