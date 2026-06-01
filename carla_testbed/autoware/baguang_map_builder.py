"""Build a minimal Autoware map for ``straight_road_for_baguang``.

This is a pragmatic bridge artifact generator for the current RoadRunner
straight-road map. It converts the CARLA OpenDRIVE lane geometry into a small
Lanelet2 OSM file plus an ASCII PCD placeholder so Autoware map loading can be
tested before richer map conversion is available.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import xml.etree.ElementTree as ET

BAGUANG_AUTOWARE_MAP_BUILD_SCHEMA_VERSION = "baguang_autoware_map_build.v1"
DEFAULT_XODR = Path(
    "/home/ubuntu/CARLA_0.9.16/CarlaUE4/Content/Carla/Maps/"
    "straight_road_for_baguang/OpenDrive/straight_road_for_baguang.xodr"
)
DEFAULT_AUTOWARE_MAP_ROOT = Path("/home/ubuntu/autoware-contents/maps")
DEFAULT_MAP_NAME = "straight_road_for_baguang"


@dataclass(frozen=True)
class LaneSpec:
    road_id: str
    lane_id: int
    travel_dir: str
    inner_t: float
    outer_t: float
    width_m: float


@dataclass(frozen=True)
class RoadGeometry:
    road_id: str
    length_m: float
    x0: float
    y0: float
    heading_rad: float
    source_speed_mph: float | None
    lanes: tuple[LaneSpec, ...]


@dataclass(frozen=True)
class LaneCenterYLine:
    y0: float
    y1: float

    def at_s(self, s: float, length_m: float) -> float:
        if float(length_m) <= 1e-6:
            return self.y0
        ratio = max(0.0, min(1.0, float(s) / float(length_m)))
        return self.y0 + ((self.y1 - self.y0) * ratio)


DEFAULT_AUTOWARE_FRAME_CARLA_0916_LANE_CENTER_Y = {
    # Measured from CARLA 0.9.16 waypoints for straight_road_for_baguang and
    # expressed in the current Autoware frame (x ~= CARLA x, y ~= -CARLA y).
    -2: LaneCenterYLine(5.25499963760376, 5.045356750488281),
    -1: LaneCenterYLine(1.5049997568130493, 1.29535710811615),
    1: LaneCenterYLine(-2.245357036590576, -2.4549996852874756),
    2: LaneCenterYLine(-5.995357036590576, -6.2049994468688965),
}


def parse_baguang_xodr(xodr_path: str | Path) -> RoadGeometry:
    path = Path(xodr_path).expanduser()
    root = ET.parse(path).getroot()
    road = root.find("road")
    if road is None:
        raise ValueError(f"no road element found in {path}")
    road_id = str(road.attrib.get("id") or "0")
    length_m = float(road.attrib["length"])
    geometry = road.find("./planView/geometry")
    if geometry is None or geometry.find("line") is None:
        raise ValueError("only single line road geometry is supported for Baguang")
    source_speed_mph = None
    speed = road.find("./type/speed")
    if speed is not None and speed.attrib.get("max"):
        try:
            source_speed_mph = float(speed.attrib["max"])
        except ValueError:
            source_speed_mph = None
    lanes: list[LaneSpec] = []
    lane_section = road.find("./lanes/laneSection")
    if lane_section is None:
        raise ValueError("OpenDRIVE road missing laneSection")
    for side_name in ("left", "right"):
        side = lane_section.find(side_name)
        if side is None:
            continue
        running_t = 0.0
        lane_elems = [
            lane for lane in side.findall("lane") if lane.attrib.get("type") == "driving"
        ]
        lane_elems.sort(key=lambda elem: abs(int(elem.attrib["id"])))
        sign = 1.0 if side_name == "left" else -1.0
        for lane in lane_elems:
            width_elem = lane.find("width")
            width = float(width_elem.attrib.get("a", "0.0")) if width_elem is not None else 0.0
            inner_t = sign * running_t
            outer_t = sign * (running_t + width)
            travel_dir = "unknown"
            vector_lane = lane.find("./userData/vectorLane")
            if vector_lane is not None and vector_lane.attrib.get("travelDir"):
                travel_dir = str(vector_lane.attrib["travelDir"])
            lanes.append(
                LaneSpec(
                    road_id=road_id,
                    lane_id=int(lane.attrib["id"]),
                    travel_dir=travel_dir,
                    inner_t=inner_t,
                    outer_t=outer_t,
                    width_m=width,
                )
            )
            running_t += width
    if not lanes:
        raise ValueError("no driving lanes found in Baguang OpenDRIVE")
    return RoadGeometry(
        road_id=road_id,
        length_m=length_m,
        x0=float(geometry.attrib["x"]),
        y0=float(geometry.attrib["y"]),
        heading_rad=float(geometry.attrib["hdg"]),
        source_speed_mph=source_speed_mph,
        lanes=tuple(lanes),
    )


def build_baguang_autoware_map(
    *,
    xodr_path: str | Path = DEFAULT_XODR,
    out_root: str | Path = DEFAULT_AUTOWARE_MAP_ROOT,
    map_name: str = DEFAULT_MAP_NAME,
    speed_limit_kph: float = 80.0,
    sample_step_m: float = 10.0,
) -> dict:
    geometry = parse_baguang_xodr(xodr_path)
    out = Path(out_root).expanduser()
    lanelet_path = out / "vector_maps" / "lanelet2" / f"{map_name}.osm"
    pcd_path = out / "point_cloud_maps" / f"{map_name}.pcd"
    projector_path = out / "map_projector_info" / f"{map_name}.yaml"
    lanelet_path.parent.mkdir(parents=True, exist_ok=True)
    pcd_path.parent.mkdir(parents=True, exist_ok=True)
    projector_path.parent.mkdir(parents=True, exist_ok=True)
    lanelet_count = _write_lanelet_osm(
        lanelet_path,
        geometry,
        speed_limit_kph=float(speed_limit_kph),
        sample_step_m=float(sample_step_m),
    )
    point_count = _write_ascii_pcd(pcd_path, geometry, sample_step_m=max(float(sample_step_m), 20.0))
    projector_path.write_text("projector_type: Local\n", encoding="utf-8")
    warnings: list[str] = []
    if geometry.source_speed_mph is not None:
        source_kph = geometry.source_speed_mph * 1.609344
        if abs(source_kph - float(speed_limit_kph)) > 2.0:
            warnings.append(
                "output_speed_limit_differs_from_xodr_speed:"
                f"{source_kph:.3f}kph->{float(speed_limit_kph):.3f}kph"
            )
    report = {
        "schema_version": BAGUANG_AUTOWARE_MAP_BUILD_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "map_name": map_name,
        "source_xodr_path": str(Path(xodr_path).expanduser()),
        "out_root": str(out),
        "lanelet2_map_path": str(lanelet_path),
        "pointcloud_map_path": str(pcd_path),
        "map_projector_info_path": str(projector_path),
        "road": {
            "road_id": geometry.road_id,
            "length_m": geometry.length_m,
            "source_speed_mph": geometry.source_speed_mph,
        },
        "lanelet_count": lanelet_count,
        "pointcloud_point_count": point_count,
        "speed_limit_kph": float(speed_limit_kph),
        "lane_y_alignment": {
            "applied": True,
            "source": "carla_0.9.16_waypoint_measurement",
            "lane_ids": sorted(DEFAULT_AUTOWARE_FRAME_CARLA_0916_LANE_CENTER_Y),
        },
        "explicit_centerline": {
            "applied": True,
            "reason": "Autoware path generation should not have to infer the driving centerline from sparse RoadRunner lane boundaries.",
        },
        "warnings": warnings,
        "status": "pass",
    }
    report_path = out / "vector_maps" / "lanelet2" / f"{map_name}.build_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report["report_path"] = str(report_path)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _write_lanelet_osm(
    path: Path,
    geometry: RoadGeometry,
    *,
    speed_limit_kph: float,
    sample_step_m: float,
) -> int:
    osm = ET.Element("osm", {"version": "0.6", "generator": "carla_testbed_baguang_map_builder"})
    next_node_id = 1000
    next_way_id = 2000
    next_rel_id = 3000
    lanelet_count = 0
    samples = _samples(geometry.length_m, sample_step_m)
    for lane in sorted(geometry.lanes, key=lambda item: item.lane_id):
        if lane.travel_dir == "backward":
            lane_samples = list(reversed(samples))
        else:
            lane_samples = list(samples)
        left_nodes: list[str] = []
        right_nodes: list[str] = []
        center_nodes: list[str] = []
        center_t = (float(lane.inner_t) + float(lane.outer_t)) / 2.0
        for s in lane_samples:
            left = _lane_point_to_autoware_local_xy(geometry, lane, s, lane.inner_t)
            right = _lane_point_to_autoware_local_xy(geometry, lane, s, lane.outer_t)
            center = _lane_point_to_autoware_local_xy(geometry, lane, s, center_t)
            left_nodes.append(str(next_node_id))
            _append_node(osm, next_node_id, left[0], left[1])
            next_node_id += 1
            right_nodes.append(str(next_node_id))
            _append_node(osm, next_node_id, right[0], right[1])
            next_node_id += 1
            center_nodes.append(str(next_node_id))
            _append_node(osm, next_node_id, center[0], center[1])
            next_node_id += 1
        left_way_id = next_way_id
        _append_way(osm, left_way_id, left_nodes)
        next_way_id += 1
        right_way_id = next_way_id
        _append_way(osm, right_way_id, right_nodes)
        next_way_id += 1
        center_way_id = next_way_id
        _append_way(osm, center_way_id, center_nodes)
        next_way_id += 1
        relation = ET.SubElement(
            osm,
            "relation",
            {"id": str(next_rel_id), "visible": "true", "version": "1"},
        )
        next_rel_id += 1
        ET.SubElement(relation, "member", {"type": "way", "ref": str(left_way_id), "role": "left"})
        ET.SubElement(relation, "member", {"type": "way", "ref": str(right_way_id), "role": "right"})
        ET.SubElement(relation, "member", {"type": "way", "ref": str(center_way_id), "role": "centerline"})
        ET.SubElement(relation, "tag", {"k": "type", "v": "lanelet"})
        ET.SubElement(relation, "tag", {"k": "subtype", "v": "road"})
        ET.SubElement(relation, "tag", {"k": "speed_limit", "v": _format_speed(speed_limit_kph)})
        ET.SubElement(relation, "tag", {"k": "location", "v": "urban"})
        ET.SubElement(relation, "tag", {"k": "one_way", "v": "yes"})
        ET.SubElement(relation, "tag", {"k": "carla_road_id", "v": lane.road_id})
        ET.SubElement(relation, "tag", {"k": "carla_lane_id", "v": str(lane.lane_id)})
        lanelet_count += 1
    _indent(osm)
    ET.ElementTree(osm).write(path, encoding="utf-8", xml_declaration=True)
    return lanelet_count


def _write_ascii_pcd(path: Path, geometry: RoadGeometry, *, sample_step_m: float) -> int:
    samples = _samples(geometry.length_m, sample_step_m)
    points: list[tuple[float, float, float]] = []
    lateral_offsets = sorted({lane.inner_t for lane in geometry.lanes} | {lane.outer_t for lane in geometry.lanes})
    for s in samples:
        for t in lateral_offsets:
            x, y = _carla_to_autoware_local_xy(geometry, s, t)
            points.append((x, y, 0.0))
    lines = [
        "# .PCD v0.7 - Point Cloud Data file format",
        "VERSION 0.7",
        "FIELDS x y z",
        "SIZE 4 4 4",
        "TYPE F F F",
        "COUNT 1 1 1",
        f"WIDTH {len(points)}",
        "HEIGHT 1",
        "VIEWPOINT 0 0 0 1 0 0 0",
        f"POINTS {len(points)}",
        "DATA ascii",
    ]
    lines.extend(f"{x:.6f} {y:.6f} {z:.6f}" for x, y, z in points)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return len(points)


def _samples(length_m: float, step_m: float) -> list[float]:
    step = max(float(step_m), 1.0)
    values = [0.0]
    cur = step
    while cur < float(length_m):
        values.append(cur)
        cur += step
    if not math.isclose(values[-1], float(length_m)):
        values.append(float(length_m))
    return values


def _carla_to_autoware_local_xy(geometry: RoadGeometry, s: float, t: float) -> tuple[float, float]:
    carla_x = geometry.x0 + (math.cos(geometry.heading_rad) * s) - (math.sin(geometry.heading_rad) * t)
    # RoadRunner's generated CARLA OpenDRIVE for straight_road_for_baguang has
    # driving lane ids that are mirrored relative to CARLA's loaded waypoint
    # coordinates. The minus sign keeps lane -2 aligned with the CARLA spawn
    # used by the follow-stop scenario after the CARLA->Autoware y inversion.
    carla_y = geometry.y0 + (math.sin(geometry.heading_rad) * s) - (math.cos(geometry.heading_rad) * t)
    return carla_x, -carla_y


def _lane_point_to_autoware_local_xy(
    geometry: RoadGeometry,
    lane: LaneSpec,
    s: float,
    t: float,
) -> tuple[float, float]:
    x, y = _carla_to_autoware_local_xy(geometry, s, t)
    center_t = (float(lane.inner_t) + float(lane.outer_t)) / 2.0
    _, raw_center_y = _carla_to_autoware_local_xy(geometry, s, center_t)
    target_line = DEFAULT_AUTOWARE_FRAME_CARLA_0916_LANE_CENTER_Y.get(int(lane.lane_id))
    if target_line is None:
        return x, y
    return x, y + (target_line.at_s(float(s), geometry.length_m) - raw_center_y)


def _append_node(parent: ET.Element, node_id: int, local_x: float, local_y: float) -> None:
    node = ET.SubElement(
        parent,
        "node",
        {
            "id": str(node_id),
            "visible": "true",
            "version": "1",
            "lat": f"{local_y / 111111.0:.12f}",
            "lon": f"{local_x / 111111.0:.12f}",
        },
    )
    ET.SubElement(node, "tag", {"k": "ele", "v": "0.000000"})
    ET.SubElement(node, "tag", {"k": "local_x", "v": f"{local_x:.6f}"})
    ET.SubElement(node, "tag", {"k": "local_y", "v": f"{local_y:.6f}"})


def _append_way(parent: ET.Element, way_id: int, node_refs: list[str]) -> None:
    way = ET.SubElement(parent, "way", {"id": str(way_id), "visible": "true", "version": "1"})
    for ref in node_refs:
        ET.SubElement(way, "nd", {"ref": ref})
    ET.SubElement(way, "tag", {"k": "type", "v": "line_thin"})
    ET.SubElement(way, "tag", {"k": "subtype", "v": "solid"})


def _format_speed(speed_limit_kph: float) -> str:
    if float(speed_limit_kph).is_integer():
        return str(int(speed_limit_kph))
    return f"{float(speed_limit_kph):.3f}".rstrip("0").rstrip(".")


def _indent(elem: ET.Element, level: int = 0) -> None:
    indent = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "  "
        for child in elem:
            _indent(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    if level and (not elem.tail or not elem.tail.strip()):
        elem.tail = indent
