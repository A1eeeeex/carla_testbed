#!/usr/bin/env python3
"""Build the Apollo HDMap for straight_road_for_baguang from RoadRunner XML."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from pathlib import Path

from carla_testbed.apollo.roadrunner_hdmap import (
    LaneYLine,
    convert_roadrunner_apollo_xml_to_metric,
    summarize_text_map,
)


DEFAULT_HOST_MAP_DIR = Path(
    "/home/ubuntu/Apollo10.0/application-core/data/map_data/straight_road_for_baguang"
)
DEFAULT_CONTAINER_MAP_DIR = Path("/apollo/modules/map/data/straight_road_for_baguang")
DEFAULT_APOLLO_FRAME_CARLA_0916_LANE_Y_LINES = {
    # Measured against CARLA 0.9.16 package waypoints for straight_road_for_baguang,
    # then expressed in the Apollo frame used by the current bridge
    # (x ~= CARLA x, y ~= -CARLA y).  Apollo routing snaps localization in this
    # transformed frame, not in raw CARLA coordinates.
    #
    # Lane ids preserve the RoadRunner/Apollo lane point direction:
    #   0_0_2 / 0_0_1   drive west with CARLA lanes -2 / -1
    #   0_0_-1 / 0_0_-2 drive east with CARLA lanes  1 /  2
    "0_0_2": LaneYLine(299.697021484375, 5.25499963760376, -99.30290985107422, 5.045356750488281),
    "0_0_1": LaneYLine(299.6990051269531, 1.5049997568130493, -99.30094146728516, 1.29535710811615),
    "0_0_-1": LaneYLine(-99.97899627685547, -2.4549996852874756, 299.02093505859375, -2.245357036590576),
    "0_0_-2": LaneYLine(-99.97705841064453, -6.2049994468688965, 299.0229187011719, -5.995357036590576),
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-map-dir", type=Path, default=DEFAULT_HOST_MAP_DIR)
    parser.add_argument("--container-map-dir", type=Path, default=DEFAULT_CONTAINER_MAP_DIR)
    parser.add_argument("--container", default="apollo_neo_dev_10.0.0_pkg")
    parser.add_argument("--source-xml", type=Path)
    parser.add_argument("--reference-xml", type=Path)
    parser.add_argument("--zone-id", default="31")
    parser.add_argument(
        "--no-reverse-driving-lanes",
        action="store_true",
        help="Do not reverse RoadRunner driving lane point order before Apollo map generation.",
    )
    parser.add_argument(
        "--extend-driving-lane-ends-m",
        type=float,
        default=5.0,
        help="Extend driving lane point sets at both ends after metric conversion.",
    )
    parser.add_argument(
        "--no-carla-lane-y-alignment",
        action="store_true",
        help=(
            "Disable the CARLA 0.9.16 Apollo-frame lane-y alignment shim for straight_road_for_baguang. "
            "Use only when the RoadRunner Apollo XML has already been re-exported with "
            "bridge-frame CARLA waypoint-equivalent lane centers."
        ),
    )
    parser.add_argument(
        "--lane-y-line",
        action="append",
        default=[],
        metavar="LANE_UID:X0,Y0,X1,Y1",
        help=(
            "Override/add an Apollo-frame target lane-center line. Example: "
            "0_0_2:299.7,5.25,-99.3,5.04. Can be repeated."
        ),
    )
    parser.add_argument(
        "--lane-y-shift",
        action="append",
        default=[],
        metavar="LANE_UID:DELTA_Y",
        help=(
            "Override/add an Apollo lane y shift in meters, e.g. 0_0_2:0.7385. "
            "Can be repeated. Defaults are measured from CARLA 0.9.16 for this map."
        ),
    )
    parser.add_argument(
        "--speed-limit-mps",
        type=float,
        default=23.61,
        help=(
            "Target Apollo lane speed in m/s. The converter writes km/h into "
            "RoadRunner XML because Apollo's XML parser converts that field to m/s."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    host_map_dir = args.host_map_dir
    source_xml = args.source_xml or host_map_dir / "base_map.roadrunner_apollo_longlat.xml"
    reference_xml = args.reference_xml or host_map_dir / "base_map.xml"
    converted_xml = host_map_dir / "base_map.apollo10_metric.xml"

    _require(source_xml, "RoadRunner Apollo XML")
    _require(reference_xml, "reference metric OpenDRIVE XML")

    print(f"[hdmap] source_xml={source_xml}")
    print(f"[hdmap] reference_xml={reference_xml}")
    print(f"[hdmap] converted_xml={converted_xml}")
    if args.dry_run:
        return 0

    lane_y_lines = (
        {}
        if args.no_carla_lane_y_alignment
        else dict(DEFAULT_APOLLO_FRAME_CARLA_0916_LANE_Y_LINES)
    )
    lane_y_lines.update(_parse_lane_y_lines(args.lane_y_line))
    lane_y_shifts = {}
    lane_y_shifts.update(_parse_lane_y_shifts(args.lane_y_shift))

    scale = convert_roadrunner_apollo_xml_to_metric(
        source_xml,
        reference_xml,
        converted_xml,
        zone_id=args.zone_id,
        reverse_driving_lane_points=not args.no_reverse_driving_lanes,
        extend_driving_lane_ends_m=float(args.extend_driving_lane_ends_m),
        lane_y_alignment_lines=lane_y_lines,
        lane_y_shifts=lane_y_shifts,
        lane_speed_limit_mps=float(args.speed_limit_mps),
    )
    print(f"[hdmap] converted coordinate scale: x={scale.x_scale:.9f}, y={scale.y_scale:.9f}")
    print(f"[hdmap] reverse_driving_lane_points={not args.no_reverse_driving_lanes}")
    print(f"[hdmap] extend_driving_lane_ends_m={float(args.extend_driving_lane_ends_m):.3f}")
    print(f"[hdmap] carla_lane_y_alignment_enabled={not args.no_carla_lane_y_alignment}")
    print(f"[hdmap] lane_y_lines={json.dumps(_lane_y_lines_json(lane_y_lines), sort_keys=True)}")
    print(f"[hdmap] lane_y_shifts={json.dumps(lane_y_shifts, sort_keys=True)}")
    print(f"[hdmap] speed_limit_mps={float(args.speed_limit_mps):.6f}")

    container_tmp = Path(f"/tmp/carla_testbed_hdmap_{int(time.time())}")
    _run(
        [
            "docker",
            "exec",
            args.container,
            "bash",
            "-lc",
            f"rm -rf {container_tmp} && mkdir -p {container_tmp}/in {container_tmp}/out",
        ]
    )
    _run(["docker", "cp", str(converted_xml), f"{args.container}:{container_tmp}/in/base_map.xml"])

    _run(
        [
            "docker",
            "exec",
            args.container,
            "bash",
            "-lc",
            (
                f"set -euo pipefail; "
                f"proto_map_generator --map_dir={container_tmp}/in --output_dir={container_tmp}/out "
                f"--alsologtostderr=1; "
                f"cp {container_tmp}/in/base_map.xml {container_tmp}/out/base_map.xml; "
                f"sim_map_generator --map_dir={container_tmp}/out --output_dir={container_tmp}/out "
                f"--alsologtostderr=1; "
                f"cd /apollo; "
                f"topo_creator --flagfile=/opt/apollo/neo/share/modules/routing/conf/routing.conf "
                f"--map_dir={container_tmp}/out --alsologtostderr=1"
            ),
        ]
    )

    backup_dir = host_map_dir / f"backup_single_lane_{time.strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(parents=True, exist_ok=False)
    active_map_files = (
        "base_map.xml",
        "base_map.txt",
        "base_map.bin",
        "sim_map.txt",
        "sim_map.bin",
        "routing_map.txt",
        "routing_map.bin",
    )
    legacy_single_lane_files = (
        "single_lane_xy.csv",
        "single_lane_xy.pre_extend_backup.csv",
        "single_lane_xy_manifest.json",
    )
    for name in active_map_files + legacy_single_lane_files:
        src = host_map_dir / name
        if src.exists():
            shutil.copy2(src, backup_dir / name)

    _run(
        [
            "docker",
            "exec",
            args.container,
            "bash",
            "-lc",
            (
                f"set -euo pipefail; "
                f"mkdir -p {args.container_map_dir}; "
                f"cp {container_tmp}/out/base_map.xml {args.container_map_dir}/base_map.xml; "
                f"cp {container_tmp}/out/base_map.txt {args.container_map_dir}/base_map.txt; "
                f"cp {container_tmp}/out/base_map.bin {args.container_map_dir}/base_map.bin; "
                f"cp {container_tmp}/out/sim_map.txt {args.container_map_dir}/sim_map.txt; "
                f"cp {container_tmp}/out/sim_map.bin {args.container_map_dir}/sim_map.bin; "
                f"cp {container_tmp}/out/routing_map.txt {args.container_map_dir}/routing_map.txt; "
                f"cp {container_tmp}/out/routing_map.bin {args.container_map_dir}/routing_map.bin"
            ),
        ]
    )

    for name in legacy_single_lane_files:
        src = host_map_dir / name
        if src.exists():
            src.unlink()

    base_summary = summarize_text_map(host_map_dir / "base_map.txt")
    sim_summary = summarize_text_map(host_map_dir / "sim_map.txt")
    expected_speed_limits = [float(args.speed_limit_mps)]
    if base_summary["driving_lane_speed_limits_mps"] != expected_speed_limits:
        raise RuntimeError(
            "generated base_map driving-lane speed limits do not match target: "
            f"{base_summary['driving_lane_speed_limits_mps']} != {expected_speed_limits}"
        )
    if sim_summary["driving_lane_speed_limits_mps"] != expected_speed_limits:
        raise RuntimeError(
            "generated sim_map driving-lane speed limits do not match target: "
            f"{sim_summary['driving_lane_speed_limits_mps']} != {expected_speed_limits}"
        )

    report = {
        "schema_version": "baguang_apollo_hdmap_build.v2",
        "host_map_dir": str(host_map_dir),
        "container_map_dir": str(args.container_map_dir),
        "source_xml": str(source_xml),
        "converted_xml": str(converted_xml),
        "backup_dir": str(backup_dir),
        "reverse_driving_lane_points": not args.no_reverse_driving_lanes,
        "extend_driving_lane_ends_m": float(args.extend_driving_lane_ends_m),
        "carla_lane_y_alignment_enabled": not args.no_carla_lane_y_alignment,
        "lane_y_alignment_lines": _lane_y_lines_json(lane_y_lines),
        "lane_y_shifts": lane_y_shifts,
        "speed_limit_mps": float(args.speed_limit_mps),
        "base_map": base_summary,
        "sim_map": sim_summary,
        "routing_map": summarize_text_map(host_map_dir / "routing_map.txt"),
    }
    report_path = host_map_dir / "baguang_apollo_hdmap_build_report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"[hdmap] backup_dir={backup_dir}")
    print(f"[hdmap] report={report_path}")
    print(json.dumps(report, indent=2))
    return 0


def _require(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"missing {label}: {path}")


def _run(cmd: list[str]) -> None:
    print("[hdmap][cmd]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _parse_lane_y_shifts(items: list[str]) -> dict[str, float]:
    shifts: dict[str, float] = {}
    for item in items:
        if ":" not in str(item):
            raise ValueError(f"invalid --lane-y-shift {item!r}; expected LANE_UID:DELTA_Y")
        lane_id, value = str(item).split(":", 1)
        lane_id = lane_id.strip()
        if not lane_id:
            raise ValueError(f"invalid --lane-y-shift {item!r}; lane id is empty")
        shifts[lane_id] = float(value)
    return shifts


def _parse_lane_y_lines(items: list[str]) -> dict[str, LaneYLine]:
    lines: dict[str, LaneYLine] = {}
    for item in items:
        if ":" not in str(item):
            raise ValueError(f"invalid --lane-y-line {item!r}; expected LANE_UID:X0,Y0,X1,Y1")
        lane_id, coords_text = str(item).split(":", 1)
        lane_id = lane_id.strip()
        coords = [float(part.strip()) for part in coords_text.split(",") if part.strip()]
        if not lane_id or len(coords) != 4:
            raise ValueError(f"invalid --lane-y-line {item!r}; expected LANE_UID:X0,Y0,X1,Y1")
        lines[lane_id] = LaneYLine(coords[0], coords[1], coords[2], coords[3])
    return lines


def _lane_y_lines_json(lines: dict[str, LaneYLine]) -> dict[str, dict[str, float]]:
    return {
        lane_id: {"x0": line.x0, "y0": line.y0, "x1": line.x1, "y1": line.y1}
        for lane_id, line in lines.items()
    }


if __name__ == "__main__":
    raise SystemExit(main())
