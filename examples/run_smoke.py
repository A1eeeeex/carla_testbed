#!/usr/bin/env python3
"""Lightweight no-runtime smoke example.

This example calls the canonical CLI. It does not start CARLA, Apollo, CyberRT,
or ROS2.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from carla_testbed.cli import main as cli_main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the CI-friendly carla_testbed smoke example")
    parser.add_argument("--config", type=Path, default=Path("configs/examples/smoke.yaml"))
    parser.add_argument("--run-dir", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cli_args = ["smoke", "--config", str(args.config)]
    if args.run_dir is not None:
        cli_args.extend(["--run-dir", str(args.run_dir)])
    return cli_main(cli_args)


if __name__ == "__main__":
    sys.exit(main())
