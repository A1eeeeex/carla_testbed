#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
_ROS_SOURCED_ENV: Dict[str, str] | None = None
PROFILE_TO_CONFIG: Dict[str, Path] = {
    "minimal": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_minimal.yaml",
    "relaxed": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_relaxed.yaml",
    "strict": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_strict.yaml",
}


def _ros_sourced_env(base_env: Dict[str, str]) -> Dict[str, str]:
    global _ROS_SOURCED_ENV
    if _ROS_SOURCED_ENV is not None:
        env = base_env.copy()
        env.update(_ROS_SOURCED_ENV)
        return env
    ros_setup = Path("/opt/ros/humble/setup.bash")
    if not ros_setup.exists():
        return base_env.copy()
    probe = subprocess.run(
        [
            "bash",
            "-lc",
            f"source {shlex.quote(str(ros_setup))} >/dev/null 2>&1 && env -0",
        ],
        cwd=str(REPO_ROOT),
        env=base_env.copy(),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if probe.returncode != 0:
        return base_env.copy()
    sourced: Dict[str, str] = {}
    for item in probe.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        try:
            sourced[key.decode("utf-8")] = value.decode("utf-8")
        except UnicodeDecodeError:
            continue
    _ROS_SOURCED_ENV = sourced
    env = base_env.copy()
    env.update(sourced)
    return env


def _build_overrides(args: argparse.Namespace) -> List[str]:
    overrides: List[str] = []
    apollo_root = (args.apollo_root or os.environ.get("APOLLO_ROOT") or "").strip()
    docker_container = (args.docker_container or os.environ.get("APOLLO_DOCKER_CONTAINER") or "").strip()
    carla_root = (args.carla_root or os.environ.get("CARLA_ROOT") or "").strip()
    if apollo_root:
        overrides.extend(
            [
                f"algo.apollo.apollo_root={apollo_root}",
                f"paths.apollo_root={apollo_root}",
            ]
        )
    if docker_container:
        overrides.append(f"algo.apollo.docker.container={docker_container}")
    if carla_root:
        overrides.extend(
            [
                f"paths.carla_root={carla_root}",
                f"carla.root={carla_root}",
                f"runtime.carla.root={carla_root}",
                "runtime.carla.start=true",
            ]
        )
    overrides.extend(args.override)
    return overrides


def _run_profile(profile: str, run_dir: Path, overrides: List[str]) -> None:
    cmd = [
        sys.executable,
        "-m",
        "carla_testbed",
        "run",
        "--config",
        str(PROFILE_TO_CONFIG[profile]),
        "--run-dir",
        str(run_dir),
    ]
    for item in overrides:
        cmd.extend(["--override", item])
    print(f"[mainline] running {profile} -> {run_dir}")
    env = _ros_sourced_env(os.environ.copy())
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT), env=env)


def _maybe_restart_carla(args: argparse.Namespace) -> None:
    if not args.fresh_carla:
        return
    pattern = f"CarlaUE4-Linux-Shipping CarlaUE4 -carla-rpc-port={args.carla_port}"
    result = subprocess.run(
        ["bash", "-lc", f"pkill -f {pattern!r} || true"],
        check=False,
        cwd=str(REPO_ROOT),
        env=_ros_sourced_env(os.environ.copy()),
    )
    if result.returncode not in (0, -15, 143):
        raise subprocess.CalledProcessError(result.returncode, result.args)
    time.sleep(2.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Apollo mainline profiles: minimal / relaxed / strict")
    parser.add_argument("--profile", choices=["minimal", "relaxed", "strict", "all"], default="relaxed")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--batch-root", default="")
    parser.add_argument("--apollo-root", default="")
    parser.add_argument("--docker-container", default="")
    parser.add_argument("--carla-root", default="")
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument(
        "--fresh-carla",
        action="store_true",
        help="Kill existing CARLA on the target port before each run",
    )
    parser.add_argument("--override", action="append", default=[], help="Extra config override, repeatable")
    args = parser.parse_args()

    overrides = _build_overrides(args)

    if args.profile == "all":
        batch_root = Path(args.batch_root or (REPO_ROOT / "runs" / "apollo_mainline_profiles")).expanduser().resolve()
        batch_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "profiles": {name: str(path) for name, path in PROFILE_TO_CONFIG.items()},
            "overrides": overrides,
        }
        artifacts_dir = batch_root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        (artifacts_dir / "apollo_mainline_profiles.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        for profile in ("minimal", "relaxed", "strict"):
            _maybe_restart_carla(args)
            _run_profile(profile, batch_root / f"case_{profile}", overrides)
        return

    if args.batch_root:
        raise SystemExit("--batch-root is only valid with --profile all")

    default_run_dir = REPO_ROOT / "runs" / f"apollo_mainline_{args.profile}"
    run_dir = Path(args.run_dir or default_run_dir).expanduser().resolve()
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    _maybe_restart_carla(args)
    _run_profile(args.profile, run_dir, overrides)


if __name__ == "__main__":
    main()
