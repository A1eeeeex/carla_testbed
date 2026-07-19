#!/usr/bin/env python3
"""Build an isolated longer Baguang OpenDRIVE source for dynamic CARLA worlds."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CARLA_ROOT = Path(
    os.environ.get("CARLA_ROOT", str(Path.home() / "CARLA_0.9.16"))
)
DEFAULT_SOURCE = (
    DEFAULT_CARLA_ROOT
    / "CarlaUE4/Content/Carla/Maps/straight_road_for_baguang/OpenDrive/straight_road_for_baguang.xodr"
)
DEFAULT_OUTPUT = (
    REPO_ROOT
    / "configs/io/maps/straight_road_for_baguang_extended_120m/straight_road_for_baguang_extended_120m.xodr"
)


def build_extended_opendrive(
    source_path: str | Path,
    output_path: str | Path,
    *,
    extension_each_end_m: float = 120.0,
) -> dict[str, object]:
    source = Path(source_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    extension = float(extension_each_end_m)
    if extension <= 0.0:
        raise ValueError("extension_each_end_m must be positive")

    tree = ET.parse(source)
    root = tree.getroot()
    roads = root.findall("road")
    if len(roads) != 1:
        raise ValueError(f"expected exactly one Baguang road, found {len(roads)}")
    road = roads[0]
    geometries = road.findall("./planView/geometry")
    if len(geometries) != 1 or geometries[0].find("line") is None:
        raise ValueError("expected one straight planView geometry")
    geometry = geometries[0]

    original_length = float(road.attrib["length"])
    original_x = float(geometry.attrib["x"])
    original_y = float(geometry.attrib["y"])
    heading = float(geometry.attrib["hdg"])
    new_length = original_length + (2.0 * extension)
    new_x = original_x - (math.cos(heading) * extension)
    new_y = original_y - (math.sin(heading) * extension)
    road.set("length", _format_float(new_length))
    geometry.set("s", "0")
    geometry.set("x", _format_float(new_x))
    geometry.set("y", _format_float(new_y))
    geometry.set("length", _format_float(new_length))

    header = root.find("header")
    if header is None:
        raise ValueError("OpenDRIVE header is missing")
    header.set("name", "straight_road_for_baguang_extended_120m")
    x_margin = abs(math.cos(heading) * extension)
    y_margin = abs(math.sin(heading) * extension)
    if "east" in header.attrib:
        header.set("east", _format_float(float(header.attrib["east"]) + x_margin))
    if "west" in header.attrib:
        header.set("west", _format_float(float(header.attrib["west"]) - x_margin))
    if "north" in header.attrib:
        header.set("north", _format_float(float(header.attrib["north"]) + y_margin))
    if "south" in header.attrib:
        header.set("south", _format_float(float(header.attrib["south"]) - y_margin))

    removed_object_count = 0
    objects = road.find("objects")
    if objects is not None:
        removed_object_count = len(objects.findall("object"))
        road.remove(objects)

    output.parent.mkdir(parents=True, exist_ok=True)
    ET.indent(root, space="    ")
    tree.write(output, encoding="UTF-8", xml_declaration=True)
    output_text = output.read_text(encoding="utf-8")
    source_bytes = source.read_bytes()
    report = {
        "schema_version": "baguang_extended_opendrive_build.v1",
        "source_path": str(source),
        "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "output_path": str(output),
        "output_sha256": hashlib.sha256(output_text.encode("utf-8")).hexdigest(),
        "extension_each_end_m": extension,
        "original_road_length_m": original_length,
        "generated_road_length_m": new_length,
        "original_reference_start": {"x": original_x, "y": original_y},
        "generated_reference_start": {"x": new_x, "y": new_y},
        "generated_reference_end": {
            "x": new_x + (math.cos(heading) * new_length),
            "y": new_y + (math.sin(heading) * new_length),
        },
        "removed_static_object_count": removed_object_count,
        "claim_boundary": (
            "Generated CARLA map source only; Apollo HDMap must be extended and verified separately."
        ),
    }
    return report


def _format_float(value: float) -> str:
    return f"{value:.15e}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--extension-each-end-m", type=float, default=120.0)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    report = build_extended_opendrive(
        args.source,
        args.output,
        extension_each_end_m=args.extension_each_end_m,
    )
    report_path = args.report or args.output.with_suffix(".build_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
