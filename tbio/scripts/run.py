from __future__ import annotations

import argparse
import datetime
import os
from pathlib import Path
from typing import Any, Dict

import yaml

from tbio.contract.generate_artifacts import generate_all
from tbio.scripts.healthcheck_ros2 import healthcheck
from algo.registry import get_adapter
from algo.adapters.autoware import AutowareAdapter
from carla_testbed.utils.env import resolve_repo_root, load_local_config
from carla_testbed.doctor import doctor_main

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


def load_config(cfg_path: Path, overrides: Dict[str, Any]) -> Dict[str, Any]:
    repo_root = resolve_repo_root()
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    local_cfg = load_local_config(repo_root)
    merged = deep_update(DEFAULTS.copy(), cfg)
    merged = deep_update(merged, local_cfg)
    merged = deep_update(merged, overrides)
    return merged


def ensure_run_dir(run_dir: Path) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def save_effective(cfg: Dict[str, Any], run_dir: Path):
    eff = run_dir / "effective.yaml"
    eff.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return eff


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

    overrides = parse_overrides(args.override)
    cfg = load_config(args.config, overrides)
    if args.log_level:
        cfg.setdefault("logging", {})["level"] = args.log_level

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.run_dir or Path("runs") / ts
    ensure_run_dir(run_dir)
    artifacts_dir = run_dir / "artifacts"
    doctor_main(run_dir)

    run_meta = {
        "run_dir": str(run_dir),
        "config": str(args.config),
        "timestamp": ts,
    }
    (artifacts_dir).mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "run_meta.json").write_text(yaml.safe_dump(run_meta))

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

    if args.print_effective_config:
        print(eff_path.read_text())
    if args.dry_run:
        print(f"[dry-run] artifacts at {artifacts_dir}, config at {eff_path}")
        return

    adapter = get_adapter(cfg.get("algo", {}).get("stack"))
    adapter.prepare(cfg, run_dir)
    adapter.start(cfg, run_dir)

    if not args.no_healthcheck:
        ok = healthcheck(eff_path, timeout=5.0)
        if not ok:
            adapter.stop(cfg, run_dir)
            raise SystemExit("healthcheck failed")

    if args.follow_logs and isinstance(adapter, AutowareAdapter):
        compose = cfg.get("algo", {}).get("autoware", {}).get("compose")
        if compose:
            os.execvp("docker", ["docker", "compose", "-f", str(Path(compose).resolve()), "logs", "-f"])


if __name__ == "__main__":
    main()
