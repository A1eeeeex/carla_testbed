#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Deprecated compatibility wrapper for Town01 route health analysis")
    parser.add_argument("--batch-root", required=True)
    args = parser.parse_args()
    print("[town01-route-health][WARN] tools/analyze_town01_route_health.py is deprecated; forwarding to unified entry")
    cmd = [
        sys.executable,
        str(REPO_ROOT / "tools" / "run_town01_route_health.py"),
        "analyze",
        "--batch-root",
        str(Path(args.batch_root).expanduser().resolve()),
    ]
    raise SystemExit(subprocess.run(cmd, cwd=str(REPO_ROOT), check=False).returncode)


if __name__ == "__main__":
    main()
