#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_hdmap_projection import (  # noqa: E402
    analyze_apollo_hdmap_projection_file,
    write_apollo_hdmap_projection_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze claim-grade Apollo HDMap API projection evidence."
    )
    parser.add_argument(
        "--projection",
        required=True,
        help="Input artifacts/apollo_hdmap_projection.jsonl or JSON export.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for apollo_hdmap_projection_report.json/md.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = analyze_apollo_hdmap_projection_file(args.projection)
    outputs = write_apollo_hdmap_projection_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report["status"],
                "claim_grade": report["claim_grade"],
                "blocking_reasons": report.get("blocking_reasons") or [],
                "insufficient_reasons": report.get("insufficient_reasons") or [],
                "warnings": report.get("warnings") or [],
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["status"] in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
