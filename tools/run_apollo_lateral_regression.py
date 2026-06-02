#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Apollo lateral regression runs.
# Do not add new platform logic here; move reusable code into carla_testbed.analysis.
# Migration target: carla_testbed.analysis.apollo_lateral_semantics.
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
_ROS_SOURCED_ENV: Dict[str, str] | None = None
PROFILE_TO_CONFIG: Dict[str, Path] = {
    "current_relaxed": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_relaxed.yaml",
    "lateral_enabled_raw": REPO_ROOT
    / "configs"
    / "io"
    / "examples"
    / "followstop_apollo_gt_lateral_enabled_raw.yaml",
    "lateral_enabled_guarded": REPO_ROOT
    / "configs"
    / "io"
    / "examples"
    / "followstop_apollo_gt_lateral_enabled_guarded.yaml",
    "lateral_enabled": REPO_ROOT
    / "configs"
    / "io"
    / "examples"
    / "followstop_apollo_gt_lateral_enabled.yaml",
    "lateral_enabled_strict": REPO_ROOT
    / "configs"
    / "io"
    / "examples"
    / "followstop_apollo_gt_lateral_enabled_strict.yaml",
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


def _build_base_overrides(args: argparse.Namespace) -> List[str]:
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


def _maybe_restart_carla(args: argparse.Namespace) -> None:
    if not args.fresh_carla:
        return
    pattern = f"CarlaUE4-Linux-Shipping CarlaUE4 -carla-rpc-port={args.carla_port}"
    result = subprocess.run(
        ["bash", "-lc", f"pkill -f {pattern!r} || true"],
        check=False,
        cwd=str(REPO_ROOT),
        env=os.environ.copy(),
    )
    if result.returncode not in (0, -15, 143):
        raise subprocess.CalledProcessError(result.returncode, result.args)
    time.sleep(2.0)


def _write_manifest(batch_root: Path, manifest: Dict[str, object]) -> None:
    artifacts_dir = batch_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifacts_dir / "apollo_lateral_regression_manifest.json"
    md_path = artifacts_dir / "apollo_lateral_regression_manifest.md"
    json_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    lines = [
        "# Apollo Lateral Regression Manifest",
        "",
        f"- batch_root: `{batch_root}`",
        f"- created_at: `{manifest.get('created_at')}`",
        f"- profiles_run: `{manifest.get('profiles_run')}`",
        f"- seeds: `{manifest.get('seeds')}`",
        f"- repeats: `{manifest.get('repeats')}`",
        f"- acceptance_policy_version: `{manifest.get('acceptance_policy_version')}`",
        "",
        "## Runs",
        "",
    ]
    for item in manifest.get("runs", []):
        lines.append(
            "- `{profile}` seed=`{seed}` repeat=`{repeat_id}` status=`{status}` run_dir=`{run_dir}`".format(
                **item
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_one(
    profile: str,
    seed: int,
    repeat_id: int,
    run_dir: Path,
    base_overrides: List[str],
    args: argparse.Namespace,
) -> int:
    cmd = [
        sys.executable,
        "-m",
        "carla_testbed",
        "run",
        "--config",
        str(PROFILE_TO_CONFIG[profile]),
        "--run-dir",
        str(run_dir),
        "--override",
        f"run.seed={seed}",
        "--override",
        f"run.ticks={args.ticks}",
        "--override",
        f"run.profile_name={profile}_seed{seed:02d}_repeat{repeat_id:02d}",
    ]
    for item in base_overrides:
        cmd.extend(["--override", item])
    print(f"[lateral-regression] running profile={profile} seed={seed} repeat={repeat_id} -> {run_dir}")
    env = _ros_sourced_env(os.environ.copy())
    result = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, check=False)
    return int(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Apollo lateral regression matrix")
    parser.add_argument(
        "--profiles",
        default="current_relaxed,lateral_enabled_raw,lateral_enabled_guarded,lateral_enabled_strict",
        help="Comma-separated profile names",
    )
    parser.add_argument("--seeds", default="1,2,3", help="Comma-separated integer seeds")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--ticks", type=int, default=700)
    parser.add_argument("--batch-root", default="")
    parser.add_argument("--apollo-root", default="")
    parser.add_argument("--docker-container", default="")
    parser.add_argument("--carla-root", default="")
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument("--fresh-carla", action="store_true")
    parser.add_argument("--skip-run", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    parser.add_argument("--override", action="append", default=[], help="Extra config override, repeatable")
    args = parser.parse_args()

    profiles = [item.strip() for item in args.profiles.split(",") if item.strip()]
    invalid = [item for item in profiles if item not in PROFILE_TO_CONFIG]
    if invalid:
        raise SystemExit(f"Unknown profiles: {invalid}")
    seeds = [int(item.strip()) for item in args.seeds.split(",") if item.strip()]
    if not seeds:
        raise SystemExit("No seeds provided")

    batch_root = Path(
        args.batch_root
        or (REPO_ROOT / "runs" / f"apollo_lateral_regression_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    ).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)

    base_overrides = _build_base_overrides(args)
    manifest: Dict[str, object] = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "batch_root": str(batch_root),
        "profiles_run": profiles,
        "seeds": seeds,
        "repeats": args.repeats,
        "ticks": args.ticks,
        "acceptance_policy_version": "apollo_lateral_acceptance_policy_v1",
        "runs": [],
        "artifact_paths": {},
        "comparison_report_paths": {},
    }
    _write_manifest(batch_root, manifest)

    if not args.analyze_only:
        for profile in profiles:
            for seed in seeds:
                for repeat_id in range(1, args.repeats + 1):
                    run_dir = batch_root / f"{profile}_seed{seed:02d}_repeat{repeat_id:02d}"
                    record = {
                        "profile": profile,
                        "seed": seed,
                        "repeat_id": repeat_id,
                        "run_dir": str(run_dir),
                        "status": "pending",
                        "returncode": None,
                    }
                    manifest["runs"].append(record)
                    _write_manifest(batch_root, manifest)
                    if args.skip_run:
                        record["status"] = "skipped"
                        continue
                    _maybe_restart_carla(args)
                    rc = _run_one(profile, seed, repeat_id, run_dir, base_overrides, args)
                    record["returncode"] = rc
                    record["status"] = "completed" if rc == 0 else "failed"
                    _write_manifest(batch_root, manifest)

    analyzer = [
        sys.executable,
        str(REPO_ROOT / "tools" / "analyze_apollo_lateral_regression.py"),
        "--batch-root",
        str(batch_root),
    ]
    subprocess.run(analyzer, cwd=str(REPO_ROOT), env=os.environ.copy(), check=True)


if __name__ == "__main__":
    main()
