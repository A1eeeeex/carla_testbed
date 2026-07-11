"""RoadRunner Apollo XML helpers for Apollo 10 HDMap generation.

RoadRunner's Apollo 5 export can contain Apollo-specific lane fields that
Apollo 10's OpenDRIVE adapter expects, but its coordinates may be WGS84-like
degrees. For CARLA truth-input runs we need those coordinates in the same local
meter frame used by CARLA.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Mapping
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class HeaderScale:
    x_scale: float
    y_scale: float


@dataclass(frozen=True)
class LaneYLine:
    x0: float
    y0: float
    x1: float
    y1: float


def derive_header_scale(source_xml: str | Path, reference_xml: str | Path) -> HeaderScale:
    """Derive degree-to-meter scale from matching source/reference headers."""

    src_header = _load_header(source_xml)
    ref_header = _load_header(reference_xml)
    src_east = float(src_header.attrib["east"])
    src_north = float(src_header.attrib["north"])
    if src_east == 0.0 or src_north == 0.0:
        raise ValueError("source header east/north cannot be zero for scale derivation")
    return HeaderScale(
        x_scale=float(ref_header.attrib["east"]) / src_east,
        y_scale=float(ref_header.attrib["north"]) / src_north,
    )


def convert_roadrunner_apollo_xml_to_metric(
    source_xml: str | Path,
    reference_xml: str | Path,
    output_xml: str | Path,
    *,
    zone_id: str = "31",
    reverse_driving_lane_points: bool = False,
    extend_driving_lane_ends_m: float = 0.0,
    lane_y_alignment_lines: dict[str, LaneYLine] | None = None,
    lane_y_shifts: dict[str, float] | None = None,
    lane_speed_limit_mps: float | None = None,
) -> HeaderScale:
    """Convert RoadRunner Apollo XML coordinates to CARLA local meters.

    The input is expected to be RoadRunner's Apollo-style OpenDRIVE export
    containing Apollo lane fields such as ``uid``, ``centerLine`` and
    ``sampleAssociates``. The reference XML is the CARLA/RoadRunner metric
    OpenDRIVE export from the same map. Its header bounds provide the local
    meter scale.
    """

    source_xml = Path(source_xml)
    output_xml = Path(output_xml)
    scale = derive_header_scale(source_xml, reference_xml)

    tree = ET.parse(source_xml)
    root = tree.getroot()
    header = root.find("header")
    if header is None:
        raise ValueError(f"{source_xml} is missing OpenDRIVE header")

    if reverse_driving_lane_points:
        _reverse_driving_lane_point_order(root)

    for elem in root.iter():
        if "x" in elem.attrib:
            elem.set("x", _format_float(float(elem.attrib["x"]) * scale.x_scale))
        if "y" in elem.attrib:
            elem.set("y", _format_float(float(elem.attrib["y"]) * scale.y_scale))

    for attr, attr_scale in (
        ("east", scale.x_scale),
        ("west", scale.x_scale),
        ("north", scale.y_scale),
        ("south", scale.y_scale),
    ):
        if attr in header.attrib:
            header.set(attr, _format_float(float(header.attrib[attr]) * attr_scale))

    _set_metric_projection(header, zone_id)
    if float(extend_driving_lane_ends_m) > 0.0:
        _extend_driving_lane_point_order(root, float(extend_driving_lane_ends_m))
    if lane_y_alignment_lines:
        apply_lane_y_alignment_lines(root, lane_y_alignment_lines)
    if lane_y_shifts:
        apply_lane_y_shifts(root, lane_y_shifts)
    if lane_speed_limit_mps is not None:
        set_apollo_xml_lane_speed_limits(root, lane_speed_limit_mps)

    output_xml.parent.mkdir(parents=True, exist_ok=True)
    ET.indent(root, space="    ")
    tree.write(output_xml, encoding="UTF-8", xml_declaration=True)
    return scale


def set_apollo_xml_lane_speed_limits(root: ET.Element, speed_limit_mps: float) -> int:
    """Write lane speed values in the km/h units expected by Apollo's XML parser."""

    speed_mps = float(speed_limit_mps)
    if speed_mps <= 0.0:
        raise ValueError("lane_speed_limit_mps must be positive")
    speed_kph = _format_float(speed_mps * 3.6)
    updated = 0
    for lane in root.findall(".//lane"):
        if lane.attrib.get("type") != "driving":
            continue
        speed = lane.find("speed")
        if speed is None:
            speed = ET.SubElement(lane, "speed")
        speed.set("min", speed_kph)
        speed.set("max", speed_kph)
        updated += 1
    if updated == 0:
        raise ValueError("Apollo XML contains no driving lanes for speed-limit assignment")
    return updated


def apply_lane_y_alignment_lines(
    root: ET.Element,
    lane_y_alignment_lines: dict[str, LaneYLine],
) -> dict[str, dict[str, float]]:
    """Warp selected lanes so their centerline follows target CARLA y(x) lines.

    A constant y shift fixes the spawn point but not a slope mismatch between
    RoadRunner's Apollo XML export and CARLA's OpenDRIVE parser. For this
    straight-road custom map, CARLA's waypoint API is the runtime truth. This
    helper computes a per-point y delta from the lane's current centerline to a
    target ``LaneYLine`` and applies the same local delta to all lane geometry
    at that x coordinate, preserving lane width while matching CARLA's lane
    centerline along the route.
    """

    applied: dict[str, dict[str, float]] = {}
    targets = {str(lane_id): line for lane_id, line in lane_y_alignment_lines.items()}
    for lane in root.findall(".//lane"):
        lane_uid = str(lane.attrib.get("uid") or lane.attrib.get("id") or "")
        target = targets.get(lane_uid)
        if target is None:
            continue
        centerline_points = _lane_centerline_points(lane)
        if len(centerline_points) < 2:
            continue
        deltas: list[float] = []
        for elem in lane.iter():
            if "x" not in elem.attrib or "y" not in elem.attrib:
                continue
            x = float(elem.attrib["x"])
            source_y = _interpolate_y_by_x(centerline_points, x)
            target_y = _line_y_at_x(target, x)
            delta_y = target_y - source_y
            elem.set("y", _format_float(float(elem.attrib["y"]) + delta_y))
            deltas.append(delta_y)
        if deltas:
            applied[lane_uid] = {
                "min_delta_y": min(deltas),
                "max_delta_y": max(deltas),
                "mean_delta_y": sum(deltas) / len(deltas),
            }
    return applied


def apply_lane_y_shifts(root: ET.Element, lane_y_shifts: dict[str, float]) -> dict[str, float]:
    """Shift selected Apollo lane geometries in-place by lane ``uid``.

    RoadRunner's Apollo export and CARLA's OpenDRIVE parser can disagree about
    the lane-center y coordinate even when they describe the same visual road.
    This helper is intentionally narrow: it only shifts x/y-bearing geometry
    elements under explicitly named lanes and returns the lane ids that were
    actually changed. It does not infer new topology or touch Apollo runtime
    parameters.
    """

    applied: dict[str, float] = {}
    wanted = {str(lane_id): float(delta_y) for lane_id, delta_y in lane_y_shifts.items()}
    for lane in root.findall(".//lane"):
        lane_uid = str(lane.attrib.get("uid") or lane.attrib.get("id") or "")
        if lane_uid not in wanted:
            continue
        delta_y = wanted[lane_uid]
        for elem in lane.iter():
            if "y" not in elem.attrib:
                continue
            elem.set("y", _format_float(float(elem.attrib["y"]) + delta_y))
        applied[lane_uid] = delta_y
    return applied


def _lane_centerline_points(lane: ET.Element) -> list[tuple[float, float]]:
    return [
        (float(point.attrib["x"]), float(point.attrib["y"]))
        for point in lane.findall("./centerLine/geometry/pointSet/point")
        if "x" in point.attrib and "y" in point.attrib
    ]


def _interpolate_y_by_x(points: list[tuple[float, float]], x: float) -> float:
    best: tuple[float, float] | None = None
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        dx = x1 - x0
        if abs(dx) <= 1e-12:
            candidate = y0
            distance = abs(x - x0)
        else:
            t = max(0.0, min(1.0, (x - x0) / dx))
            candidate = y0 + ((y1 - y0) * t)
            projected_x = x0 + dx * t
            distance = abs(x - projected_x)
        if best is None or distance < best[0]:
            best = (distance, candidate)
    return best[1] if best is not None else points[0][1]


def _line_y_at_x(line: LaneYLine, x: float) -> float:
    dx = float(line.x1) - float(line.x0)
    if abs(dx) <= 1e-12:
        return float(line.y0)
    t = (float(x) - float(line.x0)) / dx
    return float(line.y0) + ((float(line.y1) - float(line.y0)) * t)


def _reverse_driving_lane_point_order(root: ET.Element) -> None:
    """Reverse RoadRunner driving lane geometry when export order opposes CARLA.

    Some RoadRunner Apollo exports keep Apollo lane metadata but serialize lane
    centerlines opposite to CARLA's generated spawn-point yaw. Apollo routing
    uses the centerline point order as lane heading, so the mismatch makes the
    ego look like it starts against traffic. Keep this as an explicit conversion
    option instead of changing runtime bridge semantics.
    """

    for lane in root.findall(".//lane"):
        if lane.attrib.get("type") != "driving":
            continue
        for container_name in ("centerLine", "border"):
            container = lane.find(container_name)
            if container is None:
                continue
            geometries = list(container.findall("geometry"))
            if len(geometries) > 1:
                for geometry in geometries:
                    container.remove(geometry)
                for geometry in reversed(geometries):
                    container.append(geometry)
            for geometry in container.findall("geometry"):
                point_set = geometry.find("pointSet")
                if point_set is None:
                    continue
                points = list(point_set.findall("point"))
                if len(points) <= 1:
                    continue
                for point in points:
                    point_set.remove(point)
                for point in reversed(points):
                    point_set.append(point)
                first = point_set.find("point")
                if first is not None:
                    geometry.set("x", first.attrib["x"])
                    geometry.set("y", first.attrib["y"])


def _extend_driving_lane_point_order(root: ET.Element, extend_m: float) -> None:
    """Extend driving lane point sets at both ends in metric coordinates."""

    for lane in root.findall(".//lane"):
        if lane.attrib.get("type") != "driving":
            continue
        for container_name in ("centerLine", "border"):
            container = lane.find(container_name)
            if container is None:
                continue
            for geometry in container.findall("geometry"):
                point_set = geometry.find("pointSet")
                if point_set is None:
                    continue
                points = list(point_set.findall("point"))
                if len(points) <= 1:
                    continue
                _extend_point_set(points, extend_m)
                first = points[0]
                geometry.set("x", first.attrib["x"])
                geometry.set("y", first.attrib["y"])
                if "length" in geometry.attrib:
                    try:
                        geometry.set(
                            "length",
                            _format_float(float(geometry.attrib["length"]) + (2.0 * extend_m)),
                        )
                    except ValueError:
                        pass


def _extend_point_set(points: list[ET.Element], extend_m: float) -> None:
    first_x = float(points[0].attrib["x"])
    first_y = float(points[0].attrib["y"])
    second_x = float(points[1].attrib["x"])
    second_y = float(points[1].attrib["y"])
    last_x = float(points[-1].attrib["x"])
    last_y = float(points[-1].attrib["y"])
    prev_x = float(points[-2].attrib["x"])
    prev_y = float(points[-2].attrib["y"])

    start_dx = first_x - second_x
    start_dy = first_y - second_y
    end_dx = last_x - prev_x
    end_dy = last_y - prev_y

    start_norm = (start_dx * start_dx + start_dy * start_dy) ** 0.5
    end_norm = (end_dx * end_dx + end_dy * end_dy) ** 0.5
    if start_norm > 1e-9:
        points[0].set("x", _format_float(first_x + (start_dx / start_norm) * extend_m))
        points[0].set("y", _format_float(first_y + (start_dy / start_norm) * extend_m))
    if end_norm > 1e-9:
        points[-1].set("x", _format_float(last_x + (end_dx / end_norm) * extend_m))
        points[-1].set("y", _format_float(last_y + (end_dy / end_norm) * extend_m))


def summarize_text_map(map_txt: str | Path) -> dict[str, object]:
    text = Path(map_txt).read_text(errors="ignore")
    speed_limits = sorted(
        {float(value) for value in re.findall(r"(?m)^\s*speed_limit:\s*([0-9.eE+-]+)\s*$", text)}
    )
    driving_speed_limits: set[float] = set()
    for lane_match in re.finditer(r"(?ms)^lane \{(.*?)(?=^lane \{|^road \{|\Z)", text):
        lane_block = lane_match.group(1)
        if not re.search(r"(?m)^\s*type:\s*CITY_DRIVING\s*$", lane_block):
            continue
        speed_match = re.search(
            r"(?m)^\s*speed_limit:\s*([0-9.eE+-]+)\s*$",
            lane_block,
        )
        if speed_match:
            driving_speed_limits.add(float(speed_match.group(1)))
    return {
        "lane_blocks": text.count("\nlane {"),
        "road_blocks": text.count("\nroad {"),
        "signal_blocks": text.count("\nsignal {"),
        "overlap_blocks": text.count("\noverlap {"),
        "routing_nodes": text.count("\nnode {"),
        "routing_edges": text.count("\nedge {"),
        "left_neighbor_forward": text.count("left_neighbor_forward_lane_id"),
        "right_neighbor_forward": text.count("right_neighbor_forward_lane_id"),
        "left_neighbor_reverse": text.count("left_neighbor_reverse_lane_id"),
        "right_neighbor_reverse": text.count("right_neighbor_reverse_lane_id"),
        "predecessors": text.count("predecessor_id"),
        "successors": text.count("successor_id"),
        "speed_limits_mps": speed_limits,
        "driving_lane_speed_limits_mps": sorted(driving_speed_limits),
    }


def build_roadrunner_conversion_metadata(
    *,
    source_map_path: str | Path | None = None,
    generated_base_map_path: str | Path | None = None,
    scale: HeaderScale | Mapping[str, float] | None = None,
    lane_count: int | None = None,
    signal_count: int | None = None,
    stop_line_count: int | None = None,
    warnings: list[str] | None = None,
) -> dict[str, object]:
    """Build optional RoadRunner conversion evidence for contract reports.

    This is metadata only: it records what was converted and hashed, but it does
    not by itself make an Apollo HDMap/reference-line/signal contract verified.
    """

    source = Path(source_map_path).expanduser() if source_map_path else None
    generated = Path(generated_base_map_path).expanduser() if generated_base_map_path else None
    source_counts = _count_xml_map_entities(source) if source and source.exists() else {}
    return {
        "schema_version": "roadrunner_conversion_metadata.v1",
        "source_map_path": None if source is None else str(source),
        "source_map_hash": _sha256_file(source),
        "generated_base_map_path": None if generated is None else str(generated),
        "generated_base_map_hash": _sha256_file(generated),
        "scale": _scale_payload(scale),
        "lane_count": lane_count if lane_count is not None else source_counts.get("lane_count"),
        "signal_count": signal_count if signal_count is not None else source_counts.get("signal_count"),
        "stop_line_count": stop_line_count if stop_line_count is not None else source_counts.get("stop_line_count"),
        "warnings": list(warnings or []),
    }


def _sha256_file(path: Path | None) -> str | None:
    if path is None or not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _scale_payload(scale: HeaderScale | Mapping[str, float] | None) -> dict[str, float] | None:
    if scale is None:
        return None
    if isinstance(scale, HeaderScale):
        return {"x_scale": scale.x_scale, "y_scale": scale.y_scale}
    return {str(key): float(value) for key, value in scale.items()}


def _count_xml_map_entities(path: Path) -> dict[str, int]:
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return {}
    text = path.read_text(errors="ignore")
    return {
        "lane_count": len(root.findall(".//lane")),
        "signal_count": len(root.findall(".//signal")),
        "stop_line_count": text.count("stopLine") + text.count("stop_line"),
    }


def _load_header(xml_path: str | Path) -> ET.Element:
    root = ET.parse(xml_path).getroot()
    header = root.find("header")
    if header is None:
        raise ValueError(f"{xml_path} is missing OpenDRIVE header")
    return header


def _set_metric_projection(header: ET.Element, zone_id: str) -> None:
    geo = header.find("geoReference")
    if geo is None:
        geo = ET.Element("geoReference")
        header.insert(0, geo)
    geo.text = f"+proj=utm +zone={zone_id} +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

    projection = header.find("projection")
    if projection is None:
        projection = ET.Element("projection")
        children = list(header)
        try:
            index = children.index(geo)
        except ValueError:
            index = -1
        header.insert(index + 1, projection)
    else:
        projection.clear()
    utm = ET.SubElement(projection, "utm")
    utm.set("zoneID", zone_id)


def _format_float(value: float) -> str:
    return f"{value:.12f}"
