from __future__ import annotations

import argparse
import dataclasses
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.config import ConfigError, TestbedConfig, load_config
from carla_testbed.contracts import EgoState, FrameStamp, SceneTruth
from carla_testbed.control import DummyController
from carla_testbed.doctor import doctor_main
from carla_testbed.record import RunArtifactStore, build_manifest, build_summary
from carla_testbed.utils.env import resolve_repo_root

try:
    from dotenv import load_dotenv as _load_dotenv
except Exception:  # python-dotenv is optional
    _load_dotenv = None


def _load_env_file(env_path: Path, *, protected_keys: set[str], override_loaded: bool = False) -> None:
    if not env_path.exists():
        return
    if _load_dotenv is not None:
        _load_dotenv(env_path, override=override_loaded)
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        if key in protected_keys:
            continue
        if (not override_loaded) and key in os.environ:
            continue
        os.environ[key] = value


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="carla_testbed", description="CARLA testbed CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="run a configured scenario or dispatch a legacy run config")
    run_p.add_argument("--config", required=True, type=Path)
    run_p.add_argument("--override", action="append", default=[])
    run_p.add_argument("--dry-run", action="store_true")
    run_p.add_argument("--print-effective-config", action="store_true")
    run_p.add_argument("--run-dir", type=Path)
    run_p.add_argument("--log-level", default=None)
    run_p.add_argument("--no-healthcheck", action="store_true")
    run_p.add_argument("--follow-logs", action="store_true")
    run_p.add_argument("--compose-clean", action="store_true")

    smoke_p = sub.add_parser("smoke", help="CI-friendly config smoke; does not start CARLA/Apollo")
    smoke_p.add_argument("--config", required=True, type=Path)
    smoke_p.add_argument("--run-dir", type=Path)

    validate_p = sub.add_parser("config-validate", help="validate a typed v0 config")
    validate_p.add_argument("path", type=Path)

    inspect_p = sub.add_parser("inspect-run", help="inspect manifest.json and summary.json in a run dir")
    inspect_p.add_argument("output_dir", type=Path)

    sub.add_parser("doctor", help="环境检查")
    return ap


def _parse_override_pairs(pairs: list[str] | None) -> dict[str, Any]:
    import yaml

    out: dict[str, Any] = {}
    for item in pairs or []:
        if "=" not in item:
            raise ConfigError(f"override must use key=value form: {item}")
        key, value = item.split("=", 1)
        cursor = out
        parts = [part for part in key.split(".") if part]
        if not parts:
            raise ConfigError(f"override key is empty: {item}")
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = yaml.safe_load(value)
    return out


def _config_to_dict(cfg: TestbedConfig) -> dict[str, Any]:
    payload = dataclasses.asdict(cfg)
    if cfg.source_path is not None:
        payload["source_path"] = str(cfg.source_path)
    return payload


def _run_dir_for_config(cfg: TestbedConfig, explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit
    return Path(cfg.run.output_root) / cfg.run.id


def _print_config_loaded(cfg: TestbedConfig, *, prefix: str = "config") -> None:
    print(
        f"[{prefix}] ok path={cfg.source_path} run_id={cfg.run.id} "
        f"scenario={cfg.scenario.name} backend={cfg.backend.name} "
        f"town={cfg.sim.town} max_ticks={cfg.run.max_ticks}"
    )


def _cmd_config_validate(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.path)
    except ConfigError as exc:
        print(f"[config-validate] failed: {exc}", file=sys.stderr)
        return 2
    _print_config_loaded(cfg, prefix="config-validate")
    return 0


def _cmd_smoke(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"[smoke] config failed: {exc}", file=sys.stderr)
        return 2

    start_wall_time_s = time.time()
    run_dir = _run_dir_for_config(cfg, args.run_dir)
    store = RunArtifactStore(run_dir).ensure()
    store.write_manifest(
        build_manifest(
            run_id=cfg.run.id,
            start_time_wall_s=start_wall_time_s,
            config_path=args.config,
            carla_host=cfg.sim.host,
            carla_port=cfg.sim.port,
            carla_town=cfg.sim.town,
            scenario_name=cfg.scenario.name,
            backend_name=cfg.backend.name,
            metadata={"mode": "smoke-config", "starts_carla": False, "starts_apollo": False},
        )
    )
    store.write_resolved_config(_config_to_dict(cfg))
    events = store.open_events()
    events.append({"event_type": "smoke_start", "run_id": cfg.run.id, "wall_time_s": start_wall_time_s})

    frame = FrameStamp(frame_id=0, sim_time_s=0.0)
    controller = DummyController()
    command = controller.step(frame, EgoState(stamp=frame), SceneTruth(stamp=frame))
    command.validate()

    end_wall_time_s = time.time()
    summary = build_summary(
        success=True,
        exit_reason="smoke_config_ok",
        frames=0,
        sim_duration_s=0.0,
        wall_duration_s=end_wall_time_s - start_wall_time_s,
        cleanup_errors_count=0,
        metadata={
            "mode": "smoke-config",
            "dummy_control": command.to_dict(),
            "starts_carla": False,
            "starts_apollo": False,
        },
    )
    events.append({"event_type": "smoke_end", "success": True, "wall_time_s": end_wall_time_s})
    events.close()
    store.update_manifest({"end_time_wall_s": end_wall_time_s})
    store.write_summary(summary)
    print(f"[smoke] ok run_dir={run_dir} scenario={cfg.scenario.name} backend={cfg.backend.name}")
    print("[smoke] no CARLA/Apollo runtime was started")
    return 0


def _cmd_inspect_run(args: argparse.Namespace) -> int:
    run_dir = args.output_dir
    manifest_path = run_dir / "manifest.json"
    summary_path = run_dir / "summary.json"
    manifest = _read_json_optional(manifest_path)
    summary = _read_json_optional(summary_path)
    if manifest is None and summary is None:
        print(f"[inspect-run] no manifest.json or summary.json found in {run_dir}", file=sys.stderr)
        return 2

    run_id = (manifest or {}).get("run_id") or run_dir.name
    scenario = (manifest or {}).get("scenario_name")
    backend = (manifest or {}).get("backend_name")
    success = (summary or {}).get("success")
    exit_reason = (summary or {}).get("exit_reason") or (summary or {}).get("fail_reason")
    frames = (summary or {}).get("frames")
    metrics = (summary or {}).get("metrics") or {}

    print(f"[inspect-run] run_id={run_id} path={run_dir}")
    if scenario or backend:
        print(f"[inspect-run] scenario={scenario or 'unknown'} backend={backend or 'unknown'}")
    if summary is not None:
        print(f"[inspect-run] success={success} exit_reason={exit_reason} frames={frames}")
        if metrics:
            print(
                "[inspect-run] metrics "
                f"avg_speed_mps={metrics.get('avg_speed_mps')} "
                f"max_speed_mps={metrics.get('max_speed_mps')} "
                f"collision_count={metrics.get('collision_count')}"
            )
    return 0


def _read_json_optional(path: Path) -> Mapping[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(data, Mapping):
        raise SystemExit(f"{path}: JSON root must be an object")
    return data


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config, overrides=_parse_override_pairs(args.override))
    except ConfigError as exc:
        print(f"[run] typed config load failed, falling back to legacy runner: {exc}", file=sys.stderr)
        from tbio.scripts.run import main as legacy_run_main

        legacy_run_main(args)
        return 0

    _print_config_loaded(cfg, prefix="run")
    if args.print_effective_config:
        import yaml

        print(yaml.safe_dump(_config_to_dict(cfg), sort_keys=False))
    if args.dry_run:
        print("[run] dry-run ok; typed runner not executed")
        return 0

    print(
        "[run] typed v0 runner is not wired to CARLA yet. "
        "Use `python -m carla_testbed smoke --config ...` for a no-runtime check, "
        "or use a legacy configs/io config for the existing runner.",
        file=sys.stderr,
    )
    return 2


def main(argv=None) -> int:
    # load env files with a clear priority:
    # process env > .env.<hostname> > .env.local > .env
    repo_root = resolve_repo_root()
    protected = set(os.environ.keys())
    hostname = socket.gethostname().strip()
    candidates = [
        (repo_root / ".env", False),
        (repo_root / ".env.local", True),
    ]
    if hostname:
        candidates.append((repo_root / f".env.{hostname}", True))
    for path, override_loaded in candidates:
        _load_env_file(path, protected_keys=protected, override_loaded=override_loaded)
    argv = argv or sys.argv[1:]
    ap = build_parser()
    args = ap.parse_args(argv)
    if args.cmd == "run":
        return _cmd_run(args)
    elif args.cmd == "smoke":
        return _cmd_smoke(args)
    elif args.cmd == "config-validate":
        return _cmd_config_validate(args)
    elif args.cmd == "inspect-run":
        return _cmd_inspect_run(args)
    elif args.cmd == "doctor":
        doctor_main()
        return 0
    else:
        ap.print_help()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
