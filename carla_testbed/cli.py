from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tbio.scripts.run import main as run_main
from tbio.scripts.smoke_test import main as smoke_main
from carla_testbed.doctor import doctor_main
from carla_testbed.utils.env import resolve_repo_root
from dotenv import load_dotenv


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
    # load .env if present
    repo_root = resolve_repo_root()
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
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
