#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.phase1_review_pack import (  # noqa: E402
    build_phase1_review_pack,
    write_phase1_review_archive,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Package exact Phase 1 accepted comparison bundles for external review."
    )
    parser.add_argument("--catalog", required=True, help="phase1_scenario_catalog.json")
    parser.add_argument("--out", required=True, help="output review-pack directory")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--archive", action="store_true", help="also write <out>.tar.gz and sha256")
    args = parser.parse_args(argv)

    manifest = build_phase1_review_pack(args.catalog, args.out, repo_root=args.repo_root)
    payload = {
        "status": manifest.get("status"),
        "scenario_count": manifest.get("scenario_count"),
        "done_scenario_count": manifest.get("done_scenario_count"),
        "partial_scenario_count": manifest.get("partial_scenario_count"),
        "manifest": str(Path(args.out) / "phase1_review_pack_manifest.json"),
        "summary": str(Path(args.out) / "phase1_review_pack_summary.md"),
    }
    if args.archive:
        payload["archive"] = write_phase1_review_archive(args.out)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
