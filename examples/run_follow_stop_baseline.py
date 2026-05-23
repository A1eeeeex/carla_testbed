#!/usr/bin/env python3
"""Lightweight follow-stop baseline wrapper.

This wrapper demonstrates the canonical CLI path. By default it performs a
typed dry-run. Pass `--legacy-run` to dispatch an existing legacy config through
the compatibility runner.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from carla_testbed.cli import main as cli_main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run or dry-run the follow-stop baseline through carla_testbed CLI")
    parser.add_argument("--config", type=Path, default=Path("configs/examples/smoke.yaml"))
    parser.add_argument("--run-dir", type=Path, default=None)
    parser.add_argument(
        "--legacy-run",
        action="store_true",
        help="dispatch the config through the existing legacy runner instead of typed dry-run",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cli_args = ["run", "--config", str(args.config)]
    if args.run_dir is not None:
        cli_args.extend(["--run-dir", str(args.run_dir)])
    if not args.legacy_run:
        cli_args.append("--dry-run")
    return cli_main(cli_args)


if __name__ == "__main__":
    sys.exit(main())
