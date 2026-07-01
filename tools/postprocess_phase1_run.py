#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_postprocess import run_phase1_postprocess  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Regenerate Phase 1 run-local comparison artifacts without starting runtime."
    )
    parser.add_argument("--run-dir", required=True, help="ScenarioRun artifact directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir).expanduser()
    report = run_phase1_postprocess(run_dir)
    out_dir = run_dir / "analysis" / "phase1_postprocess"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "phase1_postprocess_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
