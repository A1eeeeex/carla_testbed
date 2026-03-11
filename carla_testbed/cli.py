from __future__ import annotations

import argparse
import os
import socket
import sys
from pathlib import Path

from tbio.scripts.run import main as run_main
from tbio.scripts.smoke_test import main as smoke_main
from carla_testbed.doctor import doctor_main
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

    run_p = sub.add_parser("run", help="运行任务")
    run_p.add_argument("--config", required=True, type=Path)
    run_p.add_argument("--override", action="append", default=[])
    run_p.add_argument("--dry-run", action="store_true")
    run_p.add_argument("--print-effective-config", action="store_true")
    run_p.add_argument("--run-dir", type=Path)
    run_p.add_argument("--log-level", default=None)
    run_p.add_argument("--no-healthcheck", action="store_true")
    run_p.add_argument("--follow-logs", action="store_true")
    run_p.add_argument("--compose-clean", action="store_true")

    smoke_p = sub.add_parser("smoke", help="运行 smoke test")
    smoke_p.add_argument("--config", required=True, type=Path)

    sub.add_parser("doctor", help="环境检查")
    return ap


def main(argv=None):
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
        run_main(args)
    elif args.cmd == "smoke":
        smoke_main(args)
    elif args.cmd == "doctor":
        doctor_main()
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
