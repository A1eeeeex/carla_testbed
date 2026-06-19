#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_control_runtime_provenance import (  # noqa: E402
    DEFAULT_RECORD_NAME,
    analyze_apollo_control_runtime_overlay_set_provenance,
    analyze_apollo_control_runtime_provenance,
    write_apollo_control_runtime_overlay_set_provenance,
    write_apollo_control_runtime_provenance,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collect read-only provenance for Apollo control runtime overlay files."
    )
    parser.add_argument("--overlay-manifest", help="apollo_control_runtime_overlay_manifest.json")
    parser.add_argument(
        "--all-records",
        action="store_true",
        help="Probe every record in the overlay manifest instead of one record.",
    )
    parser.add_argument("--record-name", default=DEFAULT_RECORD_NAME)
    parser.add_argument("--source-path", help="Explicit source path; overrides manifest record source.")
    parser.add_argument("--target-path", help="Explicit target path; overrides manifest record target.")
    parser.add_argument("--container", help="Apollo Docker container name. If omitted, paths are probed locally.")
    parser.add_argument("--source-run", help="Optional source/candidate run path recorded in the report.")
    parser.add_argument("--baseline-run", help="Optional baseline run path recorded in the report.")
    parser.add_argument("--online-diff-report", help="Optional runtime diff report path recorded in the report.")
    parser.add_argument("--out", required=True, help="Output directory.")
    args = parser.parse_args(argv)

    if args.all_records:
        if not args.overlay_manifest:
            parser.error("--all-records requires --overlay-manifest")
        report = analyze_apollo_control_runtime_overlay_set_provenance(
            overlay_manifest=args.overlay_manifest,
            container=args.container,
            source_run=args.source_run,
            baseline_run=args.baseline_run,
            online_diff_report=args.online_diff_report,
        )
        outputs = write_apollo_control_runtime_overlay_set_provenance(report, args.out)
        status = report["diagnostic_interpretation"]["status"]
        finding = report["diagnostic_interpretation"]["finding"]
    else:
        report = analyze_apollo_control_runtime_provenance(
            source_path=args.source_path,
            target_path=args.target_path,
            overlay_manifest=args.overlay_manifest,
            record_name=args.record_name,
            container=args.container,
            source_run=args.source_run,
            baseline_run=args.baseline_run,
            online_diff_report=args.online_diff_report,
        )
        outputs = write_apollo_control_runtime_provenance(report, args.out)
        status = report["diagnostic_interpretation"]["status"]
        finding = report["diagnostic_interpretation"]["finding"]
    print(
        json.dumps(
            {
                "status": status,
                "finding": finding,
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
