#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.analysis.town01_goal_audit import (
    build_goal_audit,
    find_goal_ab_report_paths,
    find_latest_file,
    render_goal_audit_markdown,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit current evidence for the Town01 Apollo closed-loop goal.")
    parser.add_argument(
        "--ab-report",
        type=Path,
        action="append",
        help="Explicit ab_report.json. Can be provided multiple times for hard-gate plus curve batches.",
    )
    parser.add_argument("--ab-root", type=Path, default=Path("runs/ab"), help="Root used to find latest ab_report.json.")
    parser.add_argument(
        "--latest-ab-only",
        action="store_true",
        help="Use only the latest ab_report.json instead of collecting latest reports that cover goal routes.",
    )
    parser.add_argument("--calibration-report", type=Path, help="Explicit calibration_report.json.")
    parser.add_argument("--calibration-root", type=Path, default=Path("runs"), help="Root used to find latest calibration_report.json.")
    parser.add_argument("--natural-driving-report", type=Path, help="Explicit natural_driving_report.json.")
    parser.add_argument(
        "--natural-driving-root",
        type=Path,
        default=Path("runs"),
        help="Root used to find latest natural_driving_report.json.",
    )
    parser.add_argument("--demo-recording", type=Path, help="Explicit town01_demo_recording_inspection.json.")
    parser.add_argument("--demo-root", type=Path, default=Path("runs"), help="Root used to find latest demo recording inspection.")
    parser.add_argument(
        "--no-refresh-ab-from-manifest",
        action="store_true",
        help="Use stored ab_report.json as-is instead of reanalyzing source_manifest with current analyzer code.",
    )
    parser.add_argument("--cadence-ratio-min", type=float, default=0.8)
    parser.add_argument("--out", type=Path, help="Directory to write town01_goal_audit.json/md.")
    parser.add_argument(
        "--fail-on-status",
        default="",
        help=(
            "Comma-separated audit statuses that should return non-zero. "
            "Use incomplete for the final strict goal gate."
        ),
    )
    return parser


def _resolve_latest(explicit: Path | None, root: Path, pattern: str) -> Path | None:
    if explicit is not None:
        return explicit
    return find_latest_file(root, pattern)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ab_reports = args.ab_report or []
    if ab_reports:
        ab_report_paths = ab_reports
        ab_report = None
    elif args.latest_ab_only:
        ab_report_paths = None
        ab_report = _resolve_latest(None, args.ab_root, "ab_report.json")
    else:
        discovered = find_goal_ab_report_paths(args.ab_root)
        ab_report_paths = discovered or None
        ab_report = None if discovered else _resolve_latest(None, args.ab_root, "ab_report.json")
    calibration_report = _resolve_latest(args.calibration_report, args.calibration_root, "calibration_report.json")
    natural_driving_report = _resolve_latest(
        args.natural_driving_report,
        args.natural_driving_root,
        "natural_driving_report.json",
    )
    demo_recording = _resolve_latest(args.demo_recording, args.demo_root, "town01_demo_recording_inspection.json")
    audit = build_goal_audit(
        ab_report_path=ab_report,
        ab_report_paths=ab_report_paths,
        calibration_report_path=calibration_report,
        natural_driving_report_path=natural_driving_report,
        demo_recording_path=demo_recording,
        cadence_ratio_min=args.cadence_ratio_min,
        refresh_ab_from_manifest=not args.no_refresh_ab_from_manifest,
    )
    outputs: dict[str, str] = {}
    if args.out is not None:
        args.out.mkdir(parents=True, exist_ok=True)
        json_path = args.out / "town01_goal_audit.json"
        md_path = args.out / "town01_goal_audit.md"
        json_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
        md_path.write_text(render_goal_audit_markdown(audit), encoding="utf-8")
        outputs = {
            "town01_goal_audit_json": str(json_path),
            "town01_goal_audit_md": str(md_path),
        }
    print(
        json.dumps(
            {
                "audit": audit,
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )
    fail_statuses = {item.strip() for item in str(args.fail_on_status).split(",") if item.strip()}
    return 2 if audit.get("status") in fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main())
