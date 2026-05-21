#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
CONDA_PREFIX = ["conda", "run", "-n", "carla16"]

CASES: List[Dict[str, Any]] = [
    {
        "id": "case_a_profile_a_locked",
        "label": "Case A",
        "description": "Profile A locked conservative baseline",
        "config": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_case2_locked.yaml",
    },
    {
        "id": "case_b_profile_b_locked",
        "label": "Case B",
        "description": "Profile B locked trusted lane-centerline snap",
        "config": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_case3_locked.yaml",
    },
]


def _run_case(case: Dict[str, Any], run_dir: Path, overrides: List[str]) -> None:
    cmd = CONDA_PREFIX + [
        "python",
        "-m",
        "carla_testbed",
        "run",
        "--config",
        str(case["config"]),
        "--run-dir",
        str(run_dir),
    ]
    for item in overrides:
        cmd.extend(["--override", item])
    print(f"[planning-truth] running {case['label']} -> {run_dir}")
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT), env=os.environ.copy())


def _run_analyzer(batch_root: Path) -> None:
    cmd = CONDA_PREFIX + [
        "python",
        str(REPO_ROOT / "tools" / "analyze_planning_truth_validation.py"),
        "--batch-root",
        str(batch_root),
    ]
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT), env=os.environ.copy())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apollo-root", default="", help="Explicit APOLLO_ROOT override")
    parser.add_argument("--docker-container", default="", help="Explicit APOLLO_DOCKER_CONTAINER override")
    parser.add_argument("--carla-root", default="", help="Explicit CARLA_ROOT override")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--cases", default="A,B")
    parser.add_argument("--skip-run", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)
    batch_artifacts = batch_root / "artifacts"
    batch_artifacts.mkdir(parents=True, exist_ok=True)

    if args.apollo_root:
        os.environ["APOLLO_ROOT"] = args.apollo_root
    if args.docker_container:
        os.environ["APOLLO_DOCKER_CONTAINER"] = args.docker_container
    if args.carla_root:
        os.environ["CARLA_ROOT"] = args.carla_root

    selected = {f"Case {part.strip().upper()}" for part in args.cases.split(",") if part.strip()}
    selected_cases = [case for case in CASES if case["label"] in selected]
    if not selected_cases:
        raise SystemExit("no cases selected")
    selected_cases_payload = [
        {
            **case,
            "config": str(case["config"]),
        }
        for case in selected_cases
    ]

    overrides: List[str] = []
    apollo_root = str(os.environ.get("APOLLO_ROOT") or "").strip()
    carla_root = str(os.environ.get("CARLA_ROOT") or "").strip()
    if apollo_root:
        overrides.extend(
            [
                f"algo.apollo.apollo_root={apollo_root}",
                f"paths.apollo_root={apollo_root}",
            ]
        )
    if carla_root:
        overrides.extend(
            [
                f"paths.carla_root={carla_root}",
                f"carla.root={carla_root}",
                f"runtime.carla.root={carla_root}",
                "runtime.carla.start=true",
            ]
        )

    commands: List[str] = []
    for case in selected_cases:
        run_dir = batch_root / case["id"]
        cmd = CONDA_PREFIX + [
            "python",
            "-m",
            "carla_testbed",
            "run",
            "--config",
            str(case["config"]),
            "--run-dir",
            str(run_dir),
        ]
        for item in overrides:
            cmd.extend(["--override", item])
        commands.append(" ".join(cmd))

    (batch_artifacts / "planning_truth_validation_cases.json").write_text(
        json.dumps(selected_cases_payload, indent=2)
    )
    (batch_artifacts / "planning_truth_validation_commands.txt").write_text("\n".join(commands) + "\n")

    if args.skip_run:
        print(
            "[planning-truth] commands written to "
            f"{batch_artifacts / 'planning_truth_validation_commands.txt'}"
        )
        return

    if not args.analyze_only:
        for case in selected_cases:
            _run_case(case, batch_root / case["id"], overrides)
    _run_analyzer(batch_root)


if __name__ == "__main__":
    main()
