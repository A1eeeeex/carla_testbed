#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical reference-line provider validation.
# Do not add new platform logic here; move reusable code into carla_testbed.analysis.
# Migration target: carla_testbed.analysis.apollo_lateral_semantics.
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
CONDA_PREFIX = ["conda", "run", "-n", "carla16"]

CASE_CONFIGS: List[Dict[str, Any]] = [
    {
        "id": "case_a_profile_a_locked",
        "label": "Case A",
        "base_config": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_case2_locked.yaml",
        "stage6_clear_cache": False,
    },
    {
        "id": "case_b_profile_b_locked",
        "label": "Case B",
        "base_config": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_case3_locked.yaml",
        "stage6_clear_cache": False,
    },
    {
        "id": "case_b_freeze_profile_b_locked",
        "label": "Case B-freeze",
        "base_config": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_case3_freeze_locked.yaml",
        "stage6_clear_cache": False,
    },
    {
        "id": "case_b_tightened_profile_b_locked",
        "label": "Case B-tightened",
        "base_config": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_case3_tightened_locked.yaml",
        "stage6_clear_cache": False,
    },
    {
        "id": "case_b_reflinefix_profile_b_locked",
        "label": "Case B-reflinefix",
        "base_config": REPO_ROOT / "configs" / "io" / "examples" / "followstop_apollo_gt_case3_tightened_locked.yaml",
        "stage6_clear_cache": True,
    },
]

APOLLO_STAGE6_RELATIVE_FILES = [
    "modules/map/pnc_map/pnc_map_base.h",
    "modules/planning/planning_base/reference_line/reference_line_provider.h",
    "modules/planning/planning_base/reference_line/reference_line_provider.cc",
    "modules/planning/pnc_map/lane_follow_map/lane_follow_map.h",
    "modules/planning/pnc_map/lane_follow_map/lane_follow_map.cc",
]


def _prepare_generated_config(case: Dict[str, Any], generated_dir: Path) -> Path:
    payload = yaml.safe_load(case["base_config"].read_text()) or {}
    apollo_cfg = payload.setdefault("algo", {}).setdefault("apollo", {})
    stage6_cfg = apollo_cfg.setdefault("stage6_reference_line", {})
    stage6_cfg["enabled"] = True
    stage6_cfg["clear_lane_follow_cache_on_new_command"] = bool(case["stage6_clear_cache"])
    stage6_cfg["reference_line_generation_guard"] = False
    out = generated_dir / f"{case['id']}.yaml"
    out.write_text(yaml.safe_dump(payload, sort_keys=False))
    return out


def _sync_apollo_sources(apollo_root: Path, container: str) -> None:
    for rel in APOLLO_STAGE6_RELATIVE_FILES:
        host_path = apollo_root / rel
        if not host_path.exists():
            raise FileNotFoundError(f"stage6 Apollo source missing: {host_path}")
        target = f"{container}:/opt/apollo/neo/src/{rel}"
        subprocess.run(["docker", "cp", str(host_path), target], check=True, cwd=str(REPO_ROOT))
    header_sync_cmd = (
        "set -eo pipefail; "
        "install -D -m 0644 "
        "/opt/apollo/neo/src/modules/map/pnc_map/pnc_map_base.h "
        "/opt/apollo/neo/include/modules/map/pnc_map/pnc_map_base.h; "
        "install -D -m 0644 "
        "/opt/apollo/neo/src/modules/planning/planning_base/reference_line/reference_line_provider.h "
        "/opt/apollo/neo/include/modules/planning/planning_base/reference_line/reference_line_provider.h; "
        "install -D -m 0644 "
        "/opt/apollo/neo/src/modules/planning/pnc_map/lane_follow_map/lane_follow_map.h "
        "/opt/apollo/neo/include/modules/planning/pnc_map/lane_follow_map/lane_follow_map.h; "
        "install -D -m 0644 "
        "/opt/apollo/neo/src/modules/map/pnc_map/pnc_map_base.h "
        "/apollo_workspace/.aem/envroot/opt/apollo/neo/include/modules/map/pnc_map/pnc_map_base.h; "
        "install -D -m 0644 "
        "/opt/apollo/neo/src/modules/planning/planning_base/reference_line/reference_line_provider.h "
        "/apollo_workspace/.aem/envroot/opt/apollo/neo/include/modules/planning/planning_base/reference_line/reference_line_provider.h; "
        "install -D -m 0644 "
        "/opt/apollo/neo/src/modules/planning/pnc_map/lane_follow_map/lane_follow_map.h "
        "/apollo_workspace/.aem/envroot/opt/apollo/neo/include/modules/planning/pnc_map/lane_follow_map/lane_follow_map.h"
    )
    subprocess.run(
        ["docker", "exec", "-i", container, "bash", "-lc", header_sync_cmd],
        check=True,
        cwd=str(REPO_ROOT),
    )


def _build_and_deploy_apollo(container: str, build_log: Path, deploy_log: Path) -> None:
    prepare_linker_cmd = (
        "set -eo pipefail; "
        "mkdir -p /usr/lib/x86_64-linux-gnu; "
        "ln -sf /usr/local/lib/libbvar.so /usr/lib/x86_64-linux-gnu/libbvar.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-proj/9.0.0-alpha3-r1/lib/libproj.so /usr/lib/x86_64-linux-gnu/libproj.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-osqp/9.0.0-alpha3-r1/lib/libosqp.so /usr/lib/x86_64-linux-gnu/libosqp.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-boost/9.0.0-alpha3-r1/lib/libboost_filesystem.so /usr/lib/x86_64-linux-gnu/libboost_filesystem.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-boost/9.0.0-alpha3-r1/lib/libboost_program_options.so /usr/lib/x86_64-linux-gnu/libboost_program_options.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-boost/9.0.0-alpha3-r1/lib/libboost_regex.so /usr/lib/x86_64-linux-gnu/libboost_regex.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-boost/9.0.0-alpha3-r1/lib/libboost_system.so /usr/lib/x86_64-linux-gnu/libboost_system.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-boost/9.0.0-alpha3-r1/lib/libboost_thread.so /usr/lib/x86_64-linux-gnu/libboost_thread.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-opencv/10.0.0-beta-r1/lib/libopencv_calib3d.so /usr/lib/x86_64-linux-gnu/libopencv_calib3d.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-opencv/10.0.0-beta-r1/lib/libopencv_highgui.so /usr/lib/x86_64-linux-gnu/libopencv_highgui.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-opencv/10.0.0-beta-r1/lib/libopencv_imgcodecs.so /usr/lib/x86_64-linux-gnu/libopencv_imgcodecs.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-opencv/10.0.0-beta-r1/lib/libopencv_imgproc.so /usr/lib/x86_64-linux-gnu/libopencv_imgproc.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-opencv/10.0.0-beta-r1/lib/libopencv_core.so /usr/lib/x86_64-linux-gnu/libopencv_core.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-adv-plat/9.0.0-alpha3-r1/lib/libadv_trigger.so /usr/lib/x86_64-linux-gnu/libadv_trigger.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-adv-plat/9.0.0-alpha3-r1/lib/libadv_bcan.so /usr/lib/x86_64-linux-gnu/libadv_bcan.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-libtorch-gpu/10.0.0-beta-r1/lib/libc10.so /usr/lib/x86_64-linux-gnu/libc10.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-libtorch-gpu/10.0.0-beta-r1/lib/libc10_cuda.so /usr/lib/x86_64-linux-gnu/libc10_cuda.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-libtorch-gpu/10.0.0-beta-r1/lib/libtorch.so /usr/lib/x86_64-linux-gnu/libtorch.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-libtorch-gpu/10.0.0-beta-r1/lib/libtorch_cpu.so /usr/lib/x86_64-linux-gnu/libtorch_cpu.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-libtorch-gpu/10.0.0-beta-r1/lib/libtorch_cuda.so /usr/lib/x86_64-linux-gnu/libtorch_cuda.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-fastrtps/9.2.0-beta-r1/lib/libapollo_fastrtps.so /usr/lib/x86_64-linux-gnu/libapollo_fastrtps.so; "
        "ln -sf /opt/apollo/neo/packages/3rd-fastrtps/9.2.0-beta-r1/lib/libapollo_fastcdr.so /usr/lib/x86_64-linux-gnu/libapollo_fastcdr.so; "
        "ldconfig"
    )
    prepare_linker = subprocess.run(
        ["docker", "exec", "-i", container, "bash", "-lc", prepare_linker_cmd],
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    if prepare_linker.returncode != 0:
        raise RuntimeError(f"Apollo stage6 linker prep failed; see {build_log}")

    build_cmd = (
        "set -eo pipefail; "
        "cd /apollo_workspace; "
        "bazel --output_user_root=/tmp/bazelroot_stage6 build --config=cpu "
        "@apollo_src//modules/map:apollo_map "
        "@apollo_src//modules/planning/planning_base:apollo_planning_planning_base "
        "@apollo_src//modules/planning/pnc_map/lane_follow_map:liblane_follow_map.so"
    )
    build = subprocess.run(
        ["docker", "exec", "-i", container, "bash", "-lc", build_cmd],
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    build_log.write_text(
        f"prepare_cmd: {prepare_linker_cmd}\nprepare_returncode: {prepare_linker.returncode}\n"
        f"--- prepare stdout ---\n{prepare_linker.stdout}\n--- prepare stderr ---\n{prepare_linker.stderr}\n"
        f"cmd: {build_cmd}\nreturncode: {build.returncode}\n--- stdout ---\n{build.stdout}\n--- stderr ---\n{build.stderr}\n"
    )
    if build.returncode != 0:
        raise RuntimeError(f"Apollo stage6 build failed; see {build_log}")

    deploy_cmd = (
        "set -eo pipefail; "
        "MAP_SO=$(find /tmp/bazelroot_stage6 -path '*bin/external/apollo_src/modules/map/libapollo_map.so' | head -1); "
        "PLANNING_SO=$(find /tmp/bazelroot_stage6 -path '*bin/external/apollo_src/modules/planning/planning_base/libapollo_planning_planning_base.so' | head -1); "
        "LANEFOLLOW_SO=$(find /tmp/bazelroot_stage6 -path '*bin/external/apollo_src/modules/planning/pnc_map/lane_follow_map/liblane_follow_map.so' | head -1); "
        "[ -n \"$MAP_SO\" ] || { echo 'map so not found'; exit 1; }; "
        "[ -n \"$PLANNING_SO\" ] || { echo 'planning so not found'; exit 2; }; "
        "[ -n \"$LANEFOLLOW_SO\" ] || { echo 'lane_follow_map so not found'; exit 3; }; "
        "for dst in $(find /apollo /opt/apollo /apollo_workspace/.aem/envroot "
        "-path '*modules/map/libapollo_map.so' 2>/dev/null); do cp -f \"$MAP_SO\" \"$dst\"; echo map:$dst; done; "
        "for dst in $(find /apollo /opt/apollo /apollo_workspace/.aem/envroot "
        "-path '*planning_base/libapollo_planning_planning_base.so' 2>/dev/null); do cp -f \"$PLANNING_SO\" \"$dst\"; echo planning:$dst; done; "
        "for dst in $(find /apollo /opt/apollo /apollo_workspace/.aem/envroot "
        "-path '*/lib/modules/planning/pnc_map/lane_follow_map/liblane_follow_map.so' "
        "-o -path '*/lib/modules/planning/pnc_map/lane_follow_map/lane_follow_map/liblane_follow_map.so' 2>/dev/null); "
        "do cp -f \"$LANEFOLLOW_SO\" \"$dst\"; echo lane_follow_map:$dst; done; "
        "for idx in "
        "/opt/apollo/neo/share/cyber_plugin_index/modules__planning__pnc_map__lane_follow_map__liblane_follow_map.so "
        "/apollo_workspace/.aem/envroot/opt/apollo/neo/share/cyber_plugin_index/modules__planning__pnc_map__lane_follow_map__liblane_follow_map.so; "
        "do if [ -e \"$idx\" ]; then printf 'share/modules/planning/pnc_map/lane_follow_map/plugins.xml' > \"$idx\"; echo lane_follow_map_index:$idx; fi; done"
    )
    deploy = subprocess.run(
        ["docker", "exec", "-i", container, "bash", "-lc", deploy_cmd],
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    deploy_log.write_text(
        f"cmd: {deploy_cmd}\nreturncode: {deploy.returncode}\n--- stdout ---\n{deploy.stdout}\n--- stderr ---\n{deploy.stderr}\n"
    )
    if deploy.returncode != 0:
        raise RuntimeError(f"Apollo stage6 deploy failed; see {deploy_log}")


def _run_case(case: Dict[str, Any], run_dir: Path, config_path: Path, overrides: List[str]) -> None:
    cmd = CONDA_PREFIX + [
        "python",
        "-m",
        "carla_testbed",
        "run",
        "--config",
        str(config_path),
        "--run-dir",
        str(run_dir),
    ]
    for item in overrides:
        cmd.extend(["--override", item])
    print(f"[stage6] running {case['label']} -> {run_dir}")
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT), env=os.environ.copy())


def _run_analyzer(batch_root: Path) -> None:
    cmd = CONDA_PREFIX + [
        "python",
        str(REPO_ROOT / "tools" / "analyze_stage6_reference_line_provider.py"),
        "--batch-root",
        str(batch_root),
    ]
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT), env=os.environ.copy())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apollo-root", default="")
    parser.add_argument("--docker-container", default="")
    parser.add_argument("--carla-root", default="")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--cases", default="A,B,BFREEZE,BTIGHTENED,BREFLINEFIX")
    parser.add_argument("--reuse-running-carla", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-run", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    args = parser.parse_args()

    batch_root = Path(args.batch_root).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)
    batch_artifacts = batch_root / "artifacts"
    batch_artifacts.mkdir(parents=True, exist_ok=True)
    generated_dir = batch_artifacts / "generated_configs"
    generated_dir.mkdir(parents=True, exist_ok=True)

    if args.apollo_root:
        os.environ["APOLLO_ROOT"] = args.apollo_root
    if args.docker_container:
        os.environ["APOLLO_DOCKER_CONTAINER"] = args.docker_container
    if args.carla_root:
        os.environ["CARLA_ROOT"] = args.carla_root

    apollo_root_raw = str(os.environ.get("APOLLO_ROOT") or "").strip()
    docker_container = str(os.environ.get("APOLLO_DOCKER_CONTAINER") or "").strip()

    token_map = {
        "A": "Case A",
        "B": "Case B",
        "BFREEZE": "Case B-freeze",
        "B-FREEZE": "Case B-freeze",
        "BTIGHTENED": "Case B-tightened",
        "B-TIGHTENED": "Case B-tightened",
        "BREFLINEFIX": "Case B-reflinefix",
        "B-REFLINEFIX": "Case B-reflinefix",
    }
    selected_tokens = {part.strip().upper() for part in args.cases.split(",") if part.strip()}
    selected_labels = {token_map[token] for token in selected_tokens if token in token_map}
    selected_cases = [case for case in CASE_CONFIGS if case["label"] in selected_labels]
    if not selected_cases:
        raise SystemExit("no cases selected")

    generated_payload: List[Dict[str, Any]] = []
    for case in selected_cases:
        config_path = _prepare_generated_config(case, generated_dir)
        generated_payload.append(
            {
                **case,
                "base_config": str(case["base_config"]),
                "generated_config": str(config_path),
            }
        )
    (batch_artifacts / "stage6_cases.json").write_text(json.dumps(generated_payload, indent=2))

    overrides: List[str] = []
    if apollo_root_raw:
        overrides.extend(
            [
                f"algo.apollo.apollo_root={apollo_root_raw}",
                f"paths.apollo_root={apollo_root_raw}",
            ]
        )
    carla_root = str(os.environ.get("CARLA_ROOT") or "").strip()
    if carla_root:
        overrides.extend(
            [
                f"paths.carla_root={carla_root}",
                f"carla.root={carla_root}",
                f"runtime.carla.root={carla_root}",
            ]
        )
    overrides.append(f"runtime.carla.start={'false' if args.reuse_running_carla else 'true'}")

    commands: List[str] = []
    for case in selected_cases:
        config_path = generated_dir / f"{case['id']}.yaml"
        run_dir = batch_root / case["id"]
        cmd = CONDA_PREFIX + [
            "python",
            "-m",
            "carla_testbed",
            "run",
            "--config",
            str(config_path),
            "--run-dir",
            str(run_dir),
        ]
        for item in overrides:
            cmd.extend(["--override", item])
        commands.append(" ".join(cmd))
    (batch_artifacts / "stage6_commands.txt").write_text("\n".join(commands) + "\n")

    if args.skip_run:
        print(f"[stage6] commands written to {batch_artifacts / 'stage6_commands.txt'}")
        return

    if not args.analyze_only:
        if not args.skip_build:
            if not apollo_root_raw:
                raise SystemExit("APOLLO_ROOT is required for stage6 build")
            if not docker_container:
                raise SystemExit("APOLLO_DOCKER_CONTAINER is required for stage6 build")
            apollo_root = Path(apollo_root_raw).expanduser().resolve()
            _sync_apollo_sources(apollo_root, docker_container)
            _build_and_deploy_apollo(
                docker_container,
                batch_artifacts / "stage6_apollo_build.log",
                batch_artifacts / "stage6_apollo_deploy.log",
            )
        for case in selected_cases:
            _run_case(
                case,
                batch_root / case["id"],
                generated_dir / f"{case['id']}.yaml",
                overrides,
            )
    _run_analyzer(batch_root)


if __name__ == "__main__":
    main()
