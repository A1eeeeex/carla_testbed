#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.algorithms.replay_digest import (
    compare_replay_digest,
    load_replay_digest,
    write_replay_comparison_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare synthetic Apollo replay planning/control digest JSON files."
    )
    parser.add_argument("--golden", type=Path, required=True, help="Golden replay digest JSON.")
    parser.add_argument("--candidate", type=Path, required=True, help="Candidate replay digest JSON.")
    parser.add_argument("--out", type=Path, required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    golden = load_replay_digest(args.golden)
    candidate = load_replay_digest(args.candidate)
    comparison = compare_replay_digest(golden, candidate, None)
    outputs = write_replay_comparison_report(
        args.out,
        comparison,
        golden_path=args.golden,
        candidate_path=args.candidate,
    )
    print(
        json.dumps(
            {
                "outputs": outputs,
                "status": comparison.status,
                "missing_metrics": comparison.report.get("missing_metrics"),
                "tolerance_failures": comparison.report.get("tolerance_failures"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if comparison.status in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
