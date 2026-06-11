#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.hdmap_projection import (  # noqa: E402
    analyze_hdmap_projection_file,
    write_apollo_hdmap_projection_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze canonical HDMap projection evidence.")
    parser.add_argument("--projection", required=True, help="Input artifacts/apollo_hdmap_projection.jsonl or JSON export.")
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_hdmap_projection_file(args.projection)
    outputs = write_apollo_hdmap_projection_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "claim_grade": report.get("claim_grade"),
                "blocking_reasons": report.get("blocking_reasons") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
