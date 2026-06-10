#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.apollo_module_consumption import (  # noqa: E402
    analyze_apollo_module_consumption_run_dir,
    write_apollo_module_consumption_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze Apollo module input consumption evidence.")
    parser.add_argument("--run-dir", required=True, help="Run directory with Apollo debug artifacts.")
    parser.add_argument("--out", required=True, help="Output directory.")
    args = parser.parse_args(argv)

    report = analyze_apollo_module_consumption_run_dir(args.run_dir)
    outputs = write_apollo_module_consumption_report(report, args.out)
    print(
        json.dumps(
            {
                "status": report["status"],
                "blocking_reasons": report.get("blocking_reasons") or [],
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
