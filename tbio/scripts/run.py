from __future__ import annotations

import argparse
import datetime
import json
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Dict

import yaml

from tbio.contract.generate_artifacts import generate_all
from tbio.carla.launch_policy import append_followstop_launch_args, resolve_carla_launch_policy
from tbio.scripts.healthcheck_ros2 import healthcheck
from algo.registry import get_adapter
from algo.adapters.autoware import AutowareAdapter
from carla_testbed.utils.env import (
    list_local_config_files,
    load_local_config,
    load_project_config,
    resolve_repo_root,
)
from carla_testbed.doctor import doctor_main
from carla_testbed.utils.run_naming import build_run_name, next_available_run_dir, update_latest_pointer

DEFAULTS = {
    "io": {"ros": {"domain_id": 0, "use_sim_time": True}},
    "logging": {"level": "INFO"},
}


def deep_update(base: Dict[str, Any], patch: Dict[str, Any]):
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_update(base[k], v)
        else:
            base[k] = v
    return base


def parse_overrides(pairs):
    out = {}
    for item in pairs or []:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        cursor = out
        parts = k.split(".")
        for p in parts[:-1]:
            cursor = cursor.setdefault(p, {})
        cursor[parts[-1]] = yaml.safe_load(v)
    return out


def _to_path_str(repo_root: Path, raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    p = Path(text).expanduser()
    if not p.is_absolute():
        p = (repo_root / p).resolve()
    return str(p)


def _apply_host_path_defaults(cfg: Dict[str, Any], repo_root: Path) -> Dict[str, Any]:
    paths = (cfg.get("paths", {}) or {}) if isinstance(cfg.get("paths"), dict) else {}
    if not isinstance(paths, dict):
        return cfg

    normalized_paths: Dict[str, Any] = dict(paths)
    for key in [
        "carla_root",
        "apollo_root",
        "ros_workspace",
        "data_root",
        "log_root",
        "map_root",
        "model_root",
        "output_root",
    ]:
        if key in normalized_paths and str(normalized_paths.get(key, "")).strip():
            normalized_paths[key] = _to_path_str(repo_root, normalized_paths[key])
    cfg["paths"] = normalized_paths

    carla_root = str(normalized_paths.get("carla_root") or "").strip()
    if carla_root:
        cfg.setdefault("carla", {}).setdefault("root", carla_root)
        cfg.setdefault("runtime", {}).setdefault("carla", {}).setdefault("root", carla_root)

    apollo_root = str(normalized_paths.get("apollo_root") or "").strip()
    if apollo_root:
        cfg.setdefault("algo", {}).setdefault("apollo", {}).setdefault("apollo_root", apollo_root)

    map_root = str(normalized_paths.get("map_root") or "").strip()
    if map_root:
        cfg.setdefault("algo", {}).setdefault("autoware", {}).setdefault("map_root", map_root)

    data_root = str(normalized_paths.get("data_root") or "").strip()
    if data_root:
        cfg.setdefault("algo", {}).setdefault("autoware", {}).setdefault("data_path", data_root)

    log_root = str(normalized_paths.get("log_root") or "").strip()
    if log_root:
        cfg.setdefault("logging", {}).setdefault("dir", log_root)

    output_root = str(normalized_paths.get("output_root") or "").strip()
    if output_root:
        cfg.setdefault("run", {}).setdefault("output_root", output_root)

    ros_workspace = str(normalized_paths.get("ros_workspace") or "").strip()
    if ros_workspace:
        cfg.setdefault("io", {}).setdefault("ros", {}).setdefault("workspace", ros_workspace)

    return cfg


def load_config(cfg_path: Path, overrides: Dict[str, Any]) -> Dict[str, Any]:
    repo_root = resolve_repo_root()
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    project_cfg = load_project_config(repo_root)
    local_cfg = load_local_config(repo_root)
    merged = deep_update(DEFAULTS.copy(), project_cfg)
    merged = deep_update(merged, cfg)
    merged = deep_update(merged, local_cfg)
    merged = _apply_host_path_defaults(merged, repo_root)
    merged = deep_update(merged, overrides)
    merged = _apply_host_path_defaults(merged, repo_root)
    return merged


def ensure_run_dir(run_dir: Path) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def save_effective(cfg: Dict[str, Any], run_dir: Path):
    eff = run_dir / "effective.yaml"
    eff.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return eff


def _resolve_runs_root(cfg: Dict[str, Any], repo_root: Path) -> Path:
    run_cfg = (cfg.get("run", {}) or {}) if isinstance(cfg.get("run"), dict) else {}
    paths_cfg = (cfg.get("paths", {}) or {}) if isinstance(cfg.get("paths"), dict) else {}
    raw = str(run_cfg.get("output_root") or paths_cfg.get("output_root") or "runs").strip()
    root = Path(raw).expanduser()
    if not root.is_absolute():
        root = (repo_root / root).resolve()
    return root


def _default_run_dir(cfg: Dict[str, Any], ts: str, repo_root: Path) -> Path:
    run_name = build_run_name(
        ts,
        [
            (cfg.get("scenario", {}) or {}).get("driver", "scenario"),
            (cfg.get("algo", {}) or {}).get("stack", "algo"),
            (cfg.get("io", {}) or {}).get("mode", "io"),
            (cfg.get("run", {}) or {}).get("map", "map"),
        ],
    )
    runs_root = _resolve_runs_root(cfg, repo_root)
    return next_available_run_dir(runs_root, run_name)


def _start_followstop_via_unified_entry(
    effective_config_path: Path, run_dir: Path, cfg: Dict[str, Any]
) -> subprocess.Popen:
    repo_root = Path(__file__).resolve().parents[2]
    cmd = [
        sys.executable,
        "-m",
        "examples.run_followstop",
        "--config",
        str(effective_config_path),
        "--run-dir",
        str(run_dir),
    ]
    policy = resolve_carla_launch_policy(cfg)
    cmd = append_followstop_launch_args(cmd, policy)
    print(
        f"[run] carla launch policy: start={policy.start} host={policy.host}:{policy.port} "
        f"town={policy.town} need_ros2_native={policy.need_ros2_native} extra_args='{policy.extra_args}'"
    )
    if policy.need_ros2_native and (not policy.start):
        print("[run] ROS2 native enabled: expecting external CARLA already started with --ros2")
    print(f"[run] dispatch followstop: {' '.join(cmd)}")
    return subprocess.Popen(cmd, cwd=repo_root, start_new_session=True)


def main(args=None):
    ap = argparse.ArgumentParser(description="Unified IO/Algo runner")
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--override", action="append", help="key=value overrides", default=[])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--print-effective-config", action="store_true")
    ap.add_argument("--run-dir", type=Path, default=None)
    ap.add_argument("--log-level", default=None)
    ap.add_argument("--no-healthcheck", action="store_true")
    ap.add_argument("--follow-logs", action="store_true")
    ap.add_argument("--compose-clean", action="store_true", help="先执行 docker compose down 再 up")
    if args is None:
        args = ap.parse_args()

    repo_root = resolve_repo_root()
    overrides = parse_overrides(args.override)
    cfg = load_config(args.config, overrides)
    if args.log_level:
        cfg.setdefault("logging", {})["level"] = args.log_level

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.run_dir or _default_run_dir(cfg, ts, repo_root)
    ensure_run_dir(run_dir)
    update_latest_pointer(run_dir)
    artifacts_dir = run_dir / "artifacts"
    doctor_main(run_dir)

    run_meta = {
        "run_dir": str(run_dir),
        "config": str(args.config),
        "timestamp": ts,
        "scenario_driver": cfg.get("scenario", {}).get("driver"),
        "local_config_files": [str(p) for p in list_local_config_files(repo_root)],
        "paths": cfg.get("paths", {}),
    }
    if (cfg.get("scenario", {}) or {}).get("driver") == "carla_followstop":
        policy = resolve_carla_launch_policy(cfg)
        run_meta["carla_launch_policy"] = {
            "start": policy.start,
            "host": policy.host,
            "port": policy.port,
            "town": policy.town,
            "extra_args": policy.extra_args,
            "foreground": policy.foreground,
            "carla_root": policy.carla_root,
            "need_ros2_native": policy.need_ros2_native,
        }
    (artifacts_dir).mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "run_meta.json").write_text(json.dumps(run_meta, indent=2))

    # generate artifacts
    gen_cfg = cfg.get("io", {}).get("generate", {})
    contract_paths = cfg.get("io", {}).get("contract", {})
    if any(gen_cfg.get(k, False) for k in ["sensor_mapping", "sensor_kit_calibration", "qos_overrides", "frames"]):
        generate_all(
            rig_path=Path(contract_paths.get("sensor_minimal", "configs/rigs/minimal.yaml")),
            contract_path=Path(contract_paths.get("canon_ros2", "io/contract/canon_ros2.yaml")),
            frames_path=Path("io/contract/frames.yaml"),
            out_dir=artifacts_dir,
        )
    cfg.setdefault("artifacts", {}).update(
        {
            "dir": str(artifacts_dir),
            "sensor_mapping": str(artifacts_dir / "sensor_mapping.yaml"),
            "sensor_kit_calibration": str(artifacts_dir / "sensor_kit_calibration.yaml"),
            "qos_overrides": str(artifacts_dir / "qos_overrides.yaml"),
            "frames": str(artifacts_dir / "frames.yaml"),
        }
    )
    # runtime flags
    cfg.setdefault("runtime", {})["compose_clean"] = args.compose_clean
    eff_path = save_effective(cfg, run_dir)

    cleanup_state: Dict[str, Any] = {
        "done": False,
        "child_proc": None,
        "adapter": None,
        "adapter_started": False,
        "normal_exit": False,
    }
    cleanup_lock = threading.Lock()
    signal_state = {"handling": False}

    def _stop_child(proc: subprocess.Popen) -> None:
        if proc.poll() is not None:
            return
        try:
            pgid = os.getpgid(proc.pid)
        except Exception:
            pgid = None
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGINT)
            else:
                proc.send_signal(signal.SIGINT)
            proc.wait(timeout=10.0)
            return
        except Exception:
            pass
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                proc.terminate()
            proc.wait(timeout=3.0)
            return
        except Exception:
            pass
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
            else:
                proc.kill()
        except Exception:
            pass

    def _cleanup_once(reason: str) -> None:
        with cleanup_lock:
            if cleanup_state["done"]:
                return
            cleanup_state["done"] = True
        print(f"[run] cleanup begin ({reason})")
        proc = cleanup_state.get("child_proc")
        if proc is not None:
            _stop_child(proc)
        adapter = cleanup_state.get("adapter")
        if adapter is not None and cleanup_state.get("adapter_started"):
            try:
                adapter.stop(cfg, run_dir)
            except Exception as exc:
                print(f"[run][WARN] adapter stop failed: {exc}")
        print(f"[run] cleanup end ({reason})")

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)

    def _handle_interrupt(signum, _frame):
        if signal_state["handling"]:
            os._exit(128 + signum)
        signal_state["handling"] = True
        try:
            print(f"[run][WARN] received signal {signum}, cleaning up")
            _cleanup_once(f"signal_{signum}")
        finally:
            signal_state["handling"] = False
        if signum == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGINT, _handle_interrupt)
    signal.signal(signal.SIGTERM, _handle_interrupt)

    try:
        if args.print_effective_config:
            print(eff_path.read_text())
        if args.dry_run:
            cleanup_state["normal_exit"] = True
            print(f"[dry-run] artifacts at {artifacts_dir}, config at {eff_path}")
            return

        driver = (cfg.get("scenario", {}) or {}).get("driver")
        if driver == "carla_followstop":
            proc = _start_followstop_via_unified_entry(eff_path, run_dir, cfg)
            cleanup_state["child_proc"] = proc
            rc = proc.wait()
            cleanup_state["child_proc"] = None
            if rc != 0:
                raise subprocess.CalledProcessError(rc, proc.args)
            cleanup_state["normal_exit"] = True
            return

        adapter = get_adapter(cfg.get("algo", {}).get("stack"))
        cleanup_state["adapter"] = adapter
        adapter.prepare(cfg, run_dir)
        started_result = adapter.start(cfg, run_dir)
        cleanup_state["adapter_started"] = True if started_result is None else bool(started_result)

        if not args.no_healthcheck:
            ok = healthcheck(eff_path, timeout=5.0)
            if not ok:
                raise SystemExit("healthcheck failed")

        if args.follow_logs and isinstance(adapter, AutowareAdapter):
            compose = cfg.get("algo", {}).get("autoware", {}).get("compose")
            if compose:
                os.execvp("docker", ["docker", "compose", "-f", str(Path(compose).resolve()), "logs", "-f"])
        cleanup_state["normal_exit"] = True
    finally:
        if not cleanup_state.get("normal_exit"):
            _cleanup_once("finally")
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)


if __name__ == "__main__":
    main()
