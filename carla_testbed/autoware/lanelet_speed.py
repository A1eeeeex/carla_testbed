"""Lanelet2 speed metadata checks for Autoware map diagnostics.

The checker is intentionally lightweight: it reads OSM XML exported for
Autoware maps and reports whether speed-limit metadata is explicit enough for
follow-stop / natural-driving probes. It does not require Autoware, ROS2, or
CARLA runtime imports.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from statistics import mean
import xml.etree.ElementTree as ET


SCHEMA_VERSION = "autoware_lanelet_speed_report.v1"
DEFAULT_URBAN_SPEED_MPS = 50.0 / 3.6
DEFAULT_URBAN_SPEED_TOLERANCE_MPS = 0.05
REQUESTED_SPEED_GAP_WARN_MPS = 2.0
SPEED_TAG_KEYS = {
    "maxspeed",
    "max_speed",
    "maximum_speed",
    "speed",
    "speed_limit",
    "velocity",
    "velocity_limit",
}
LANELET2_TRAFFIC_RULE_SPEED_TAG_KEYS = {"speed_limit"}


@dataclass(frozen=True)
class ParsedSpeed:
    raw: str
    mps: float | None
    assumed_unit: str | None


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _is_speed_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    return normalized in SPEED_TAG_KEYS or "speed" in normalized or "velocity" in normalized


def parse_speed_value(raw: str) -> ParsedSpeed:
    """Parse a common OSM/Lanelet speed value into m/s.

    Bare numeric values are treated as km/h because OSM maxspeed-style tags
    commonly use that convention. This is a diagnostic assumption and is
    reported in `assumed_unit`.
    """

    value = str(raw).strip().lower()
    match = re.search(r"[-+]?\d+(?:\.\d+)?", value)
    if not match:
        return ParsedSpeed(raw=str(raw), mps=None, assumed_unit=None)
    number = float(match.group(0))
    if "mph" in value:
        return ParsedSpeed(raw=str(raw), mps=number * 0.44704, assumed_unit="mph")
    if "m/s" in value or "mps" in value:
        return ParsedSpeed(raw=str(raw), mps=number, assumed_unit="mps")
    return ParsedSpeed(raw=str(raw), mps=number / 3.6, assumed_unit="kmh")


def _stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "max": None, "mean": None}
    return {"min": min(values), "max": max(values), "mean": mean(values)}


def analyze_lanelet_speed_metadata(
    osm_path: str | Path,
    *,
    requested_max_vel_mps: float | None = None,
    map_name: str | None = None,
) -> dict:
    """Analyze speed-limit metadata in a Lanelet2 OSM file."""

    path = Path(osm_path)
    report: dict = {
        "schema_version": SCHEMA_VERSION,
        "osm_path": str(path),
        "map_name": map_name or path.stem,
        "exists": path.exists(),
        "requested_max_vel_mps": requested_max_vel_mps,
        "default_urban_speed_candidate_mps": DEFAULT_URBAN_SPEED_MPS,
        "element_counts": {"node": 0, "way": 0, "relation": 0, "tag": 0},
        "speed_tag_count": 0,
        "speed_tags": [],
        "lanelet2_speed_limit_tag_count": 0,
        "speed_values_mps": [],
        "speed_value_stats_mps": _stats([]),
        "speed_limit_regulatory_element_count": 0,
        "missing_speed_metadata": True,
        "missing_lanelet2_speed_limit_metadata": True,
        "warnings": [],
        "status": "insufficient_data",
    }
    if not path.exists():
        report["warnings"].append("osm_missing")
        return report

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        report["warnings"].append(f"osm_parse_error:{exc}")
        return report

    relation_tags: dict[str, str] = {}
    for elem in root.iter():
        name = _local_name(elem.tag)
        if name in report["element_counts"]:
            report["element_counts"][name] += 1
        if name == "tag":
            report["element_counts"]["tag"] += 1
            key = elem.attrib.get("k", "")
            value = elem.attrib.get("v", "")
            if _is_speed_key(key):
                parsed = parse_speed_value(value)
                normalized_key = key.strip().lower().replace("-", "_")
                item = {
                    "key": key,
                    "raw": value,
                    "mps": parsed.mps,
                    "assumed_unit": parsed.assumed_unit,
                    "lanelet2_traffic_rules_candidate": normalized_key
                    in LANELET2_TRAFFIC_RULE_SPEED_TAG_KEYS,
                }
                report["speed_tags"].append(item)
                if normalized_key in LANELET2_TRAFFIC_RULE_SPEED_TAG_KEYS:
                    report["lanelet2_speed_limit_tag_count"] += 1
                if parsed.mps is not None:
                    report["speed_values_mps"].append(parsed.mps)
        elif name == "relation":
            relation_tags = {
                child.attrib.get("k", ""): child.attrib.get("v", "")
                for child in elem
                if _local_name(child.tag) == "tag"
            }
            if (
                relation_tags.get("type") == "regulatory_element"
                and relation_tags.get("subtype") == "speed_limit"
            ):
                report["speed_limit_regulatory_element_count"] += 1

    values = [float(v) for v in report["speed_values_mps"] if v is not None]
    report["speed_tag_count"] = len(report["speed_tags"])
    report["speed_value_stats_mps"] = _stats(values)
    report["missing_speed_metadata"] = (
        report["speed_tag_count"] == 0
        and report["speed_limit_regulatory_element_count"] == 0
    )
    report["missing_lanelet2_speed_limit_metadata"] = (
        report["lanelet2_speed_limit_tag_count"] == 0
        and report["speed_limit_regulatory_element_count"] == 0
    )

    if report["missing_speed_metadata"]:
        report["status"] = "warn"
        report["warnings"].append("speed_metadata_missing")
        if (
            requested_max_vel_mps is not None
            and requested_max_vel_mps - DEFAULT_URBAN_SPEED_MPS >= REQUESTED_SPEED_GAP_WARN_MPS
        ):
            report["warnings"].append("missing_speed_tags_with_default_urban_speed_candidate")
    else:
        report["status"] = "pass"
        if report["missing_lanelet2_speed_limit_metadata"]:
            report["status"] = "warn"
            report["warnings"].append("lanelet2_speed_limit_metadata_missing")
            if any(
                str(item.get("key", "")).lower().replace("-", "_") == "maxspeed"
                for item in report["speed_tags"]
            ):
                report["warnings"].append(
                    "maxspeed_tag_may_not_affect_lanelet2_german_vehicle_speed_limit"
                )
        max_speed = report["speed_value_stats_mps"]["max"]
        if (
            max_speed is not None
            and requested_max_vel_mps is not None
            and requested_max_vel_mps - float(max_speed) >= REQUESTED_SPEED_GAP_WARN_MPS
        ):
            report["status"] = "warn"
            report["warnings"].append("map_speed_below_requested_max_candidate")

    return report


def write_lanelet_speed_limit_variant(
    osm_path: str | Path,
    out_path: str | Path,
    *,
    speed_limit: str = "80",
    subtype: str = "road",
    overwrite: bool = False,
) -> dict:
    """Write a copy of a Lanelet2 OSM with explicit lanelet `speed_limit` tags.

    Lanelet2 GermanVehicle traffic rules consume `speed_limit` on lanelets;
    common OSM `maxspeed` tags are useful documentation but did not change
    `TrafficRules::speedLimit()` in the local Autoware/Lanelet2 probe.
    """

    src = Path(osm_path)
    dst = Path(out_path)
    report = {
        "schema_version": "autoware_lanelet_speed_patch.v1",
        "source_osm_path": str(src),
        "output_osm_path": str(dst),
        "speed_limit": str(speed_limit),
        "subtype": subtype,
        "overwrite": bool(overwrite),
        "lanelet_relations_seen": 0,
        "lanelet_relations_modified": 0,
        "existing_speed_limit_tags": 0,
        "status": "insufficient_data",
        "warnings": [],
    }
    if not src.exists():
        report["warnings"].append("source_osm_missing")
        return report

    try:
        tree = ET.parse(src)
    except ET.ParseError as exc:
        report["warnings"].append(f"osm_parse_error:{exc}")
        return report

    root = tree.getroot()
    for relation in root.iter():
        if _local_name(relation.tag) != "relation":
            continue
        tags = [
            child
            for child in relation
            if _local_name(child.tag) == "tag" and "k" in child.attrib
        ]
        tag_values = {tag.attrib.get("k", ""): tag.attrib.get("v", "") for tag in tags}
        if tag_values.get("type") != "lanelet" or tag_values.get("subtype") != subtype:
            continue

        report["lanelet_relations_seen"] += 1
        speed_tag = next((tag for tag in tags if tag.attrib.get("k") == "speed_limit"), None)
        if speed_tag is not None:
            report["existing_speed_limit_tags"] += 1
            if not overwrite:
                continue
            if speed_tag.attrib.get("v") == str(speed_limit):
                continue
            speed_tag.set("v", str(speed_limit))
        else:
            ET.SubElement(relation, "tag", {"k": "speed_limit", "v": str(speed_limit)})
        report["lanelet_relations_modified"] += 1

    if report["lanelet_relations_seen"] <= 0:
        report["status"] = "warn"
        report["warnings"].append("no_matching_lanelet_relations")
    elif report["lanelet_relations_modified"] <= 0:
        report["status"] = "warn"
        report["warnings"].append("no_lanelet_relations_modified")
    else:
        report["status"] = "pass"

    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(dst, encoding="utf-8", xml_declaration=True)
    return report


def write_lanelet_speed_report(report: dict, out_dir: str | Path) -> dict[str, str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "autoware_lanelet_speed_report.json"
    md_path = out / "autoware_lanelet_speed_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    md_path.write_text(_render_markdown(report))
    return {"json": str(json_path), "markdown": str(md_path)}


def _render_markdown(report: dict) -> str:
    warnings = report.get("warnings") or []
    stats = report.get("speed_value_stats_mps") or {}
    return "\n".join(
        [
            "# Autoware Lanelet Speed Metadata Report",
            "",
            f"- status: `{report.get('status')}`",
            f"- osm_path: `{report.get('osm_path')}`",
            f"- map_name: `{report.get('map_name')}`",
            f"- speed_tag_count: `{report.get('speed_tag_count')}`",
            f"- lanelet2_speed_limit_tag_count: `{report.get('lanelet2_speed_limit_tag_count')}`",
            f"- speed_limit_regulatory_element_count: `{report.get('speed_limit_regulatory_element_count')}`",
            f"- speed_value_max_mps: `{stats.get('max')}`",
            f"- requested_max_vel_mps: `{report.get('requested_max_vel_mps')}`",
            f"- default_urban_speed_candidate_mps: `{report.get('default_urban_speed_candidate_mps')}`",
            f"- missing_speed_metadata: `{report.get('missing_speed_metadata')}`",
            f"- missing_lanelet2_speed_limit_metadata: `{report.get('missing_lanelet2_speed_limit_metadata')}`",
            f"- warnings: `{warnings}`",
            "",
        ]
    )
