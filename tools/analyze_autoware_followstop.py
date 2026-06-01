#!/usr/bin/env python3
"""Analyze whether an Autoware/CARLA run is valid follow-stop evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.autoware_followstop import (  # noqa: E402
    analyze_followstop_run,
    write_followstop_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--out", default=None)
    parser.add_argument("--min-ahead-m", type=float, default=20.0)
    parser.add_argument("--max-ahead-m", type=float, default=320.0)
    parser.add_argument("--max-lateral-m", type=float, default=4.0)
    parser.add_argument("--max-heading-diff-deg", type=float, default=35.0)
    parser.add_argument("--stop-zone-m", type=float, default=15.0)
    args = parser.parse_args(argv)

    report = analyze_followstop_run(
        args.run_dir,
        min_ahead_m=args.min_ahead_m,
        max_ahead_m=args.max_ahead_m,
        max_lateral_m=args.max_lateral_m,
        max_heading_diff_deg=args.max_heading_diff_deg,
        stop_zone_m=args.stop_zone_m,
    )
    out = args.out or str(Path(args.run_dir) / "analysis" / "autoware_followstop")
    paths = write_followstop_report(report, out)
    print(
        "autoware_followstop_diagnostics "
        f"status={report['status']} reasons={','.join(report.get('failure_reasons') or [])} "
        f"out={paths['json']}"
    )
    return 0 if report["status"] in {"pass", "warn", "fail"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
