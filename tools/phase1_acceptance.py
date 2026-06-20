#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_acceptance import (  # noqa: E402
    analyze_phase1_acceptance,
    write_phase1_acceptance,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create an atomic Phase 1 acceptance bundle from one ScenarioComparison."
    )
    parser.add_argument("--comparison-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    report = analyze_phase1_acceptance(args.comparison_dir)
    outputs = write_phase1_acceptance(report, args.out)
    final_report = json.loads(Path(outputs["report"]).read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "status": final_report.get("status"),
                "blocking_reasons": final_report.get("blocking_reasons"),
                "bundle_self_contained": (
                    final_report.get("gates", {}).get("bundle_self_contained")
                    if isinstance(final_report.get("gates"), dict)
                    else None
                ),
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
