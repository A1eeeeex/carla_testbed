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
from carla_testbed.analysis.apollo_hdmap_projection_export import (  # noqa: E402
    MapXyslConfig,
    export_apollo_hdmap_projection_jsonl,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export claim-grade Apollo HDMap projection evidence by invoking "
            "Apollo's map_xysl HDMap API tool."
        )
    )
    parser.add_argument("--run-dir", help="Run directory containing artifacts/apollo_reference_line_contract.jsonl.")
    parser.add_argument("--input", help="Optional JSONL/JSON localization sample input.")
    parser.add_argument(
        "--out",
        help=(
            "Output JSONL path. Defaults to <run-dir>/artifacts/"
            "apollo_hdmap_projection.jsonl."
        ),
    )
    parser.add_argument("--container", default="apollo_neo_dev_10.0.0_pkg", help="Apollo Docker container name.")
    parser.add_argument("--map-dir", default="/apollo/modules/map/data/carla_town01", help="Apollo map directory inside container.")
    parser.add_argument("--base-map-filename", default="base_map.txt", help="Apollo base map filename.")
    parser.add_argument("--map-name", default="Town01", help="Map name recorded in output rows.")
    parser.add_argument("--map-xysl-bin", default="/opt/apollo/neo/bin/map_xysl", help="Apollo map_xysl binary path.")
    parser.add_argument(
        "--frame-transform",
        help=(
            "Optional CARLA world -> Apollo map transform YAML. Required if "
            "--include-route-samples should fall back to manifest route_trace."
        ),
    )
    parser.add_argument("--timeout-s", type=float, default=15.0, help="Per-sample command timeout.")
    parser.add_argument(
        "--max-samples",
        type=int,
        default=250,
        help=(
            "Maximum samples to project, evenly spread across the source window. "
            "<=0 means all samples and can be slow for online run artifacts."
        ),
    )
    parser.add_argument(
        "--include-route-samples",
        action="store_true",
        help="Also project route.json points so route-s coverage can become claim-grade.",
    )
    parser.add_argument(
        "--include-start-goal",
        action="store_true",
        help="Also project route start and goal points when route.json is available.",
    )
    parser.add_argument(
        "--min-route-s-coverage",
        type=float,
        default=None,
        help="Warn when exported projection_s coverage is below this many meters.",
    )
    parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run map_xysl directly on the host instead of docker exec.",
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Also write analysis/apollo_hdmap_projection report next to the run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.run_dir and not args.input:
        raise SystemExit("--run-dir or --input is required")
    run_dir = Path(args.run_dir).expanduser() if args.run_dir else None
    if args.out:
        out_path = Path(args.out).expanduser()
    elif run_dir:
        out_path = run_dir / "artifacts" / "apollo_hdmap_projection.jsonl"
    else:
        raise SystemExit("--out is required when --run-dir is not set")

    cfg = MapXyslConfig(
        map_dir=args.map_dir,
        base_map_filename=args.base_map_filename,
        map_name=args.map_name,
        docker_container=None if args.no_docker else args.container,
        map_xysl_bin=args.map_xysl_bin,
        timeout_s=args.timeout_s,
    )
    export_status = export_apollo_hdmap_projection_jsonl(
        run_dir=run_dir,
        input_path=args.input,
        out_path=out_path,
        config=cfg,
        max_samples=args.max_samples,
        include_route_samples=args.include_route_samples,
        include_start_goal=args.include_start_goal,
        frame_transform_path=args.frame_transform,
        min_route_s_coverage=args.min_route_s_coverage,
    )
    payload: dict[str, object] = {"export": export_status}
    exit_ok = export_status["status"] in {"pass", "warn"}

    if args.analyze:
        report = analyze_apollo_hdmap_projection_file(out_path)
        out_dir = (run_dir / "analysis" / "apollo_hdmap_projection") if run_dir else out_path.parent / "analysis"
        outputs = write_apollo_hdmap_projection_report(report, out_dir)
        payload["analysis"] = {
            "status": report["status"],
            "claim_grade": report["claim_grade"],
            "blocking_reasons": report.get("blocking_reasons") or [],
            "insufficient_reasons": report.get("insufficient_reasons") or [],
            "warnings": report.get("warnings") or [],
            "outputs": outputs,
        }
        exit_ok = exit_ok and report["status"] in {"pass", "warn"}

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if exit_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
