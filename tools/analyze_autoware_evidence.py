#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.autoware_evidence import (
    analyze_autoware_evidence_run_dir,
    write_autoware_evidence_report,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Autoware demo/operator evidence for acceptance gates.")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--required-rosbag-topic",
        action="append",
        default=None,
        help="Required rosbag topic. Can be repeated. Defaults to /clock,/tf,/tf_static or manifest record.rosbag topics.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = analyze_autoware_evidence_run_dir(
        args.run_dir,
        required_rosbag_topics=args.required_rosbag_topic,
    )
    outputs = write_autoware_evidence_report(report, args.out)
    print(json.dumps({"outputs": outputs, "report": report}, indent=2, sort_keys=True))
    return 0 if report.get("artifact_completeness_status") != "incomplete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
