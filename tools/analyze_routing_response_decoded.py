#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.routing_response_decoded import (  # noqa: E402
    read_routing_response_decoded,
    write_routing_response_decoded_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize a decoded /apollo/routing_response JSON/JSONL artifact."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="routing_response_decoded.json or routing_response_decoded.jsonl.",
    )
    parser.add_argument("--out", required=True, help="Output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = read_routing_response_decoded(args.input)
    outputs = write_routing_response_decoded_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "total_length_m": report.get("total_length_m"),
                "lane_segment_count": report.get("lane_segment_count"),
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
