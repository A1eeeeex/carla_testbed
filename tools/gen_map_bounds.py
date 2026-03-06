#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable, List, Tuple


def _extract_from_text(text: str) -> List[Tuple[float, float]]:
    pairs: List[Tuple[float, float]] = []
    yaml_like = re.compile(
        r"x:\s*(-?\d+(?:\.\d+)?)\s+"
        r"(?:z:\s*-?\d+(?:\.\d+)?\s+)?"
        r"y:\s*(-?\d+(?:\.\d+)?)",
        re.MULTILINE,
    )
    for match in yaml_like.finditer(text):
        pairs.append((float(match.group(1)), float(match.group(2))))
    if pairs:
        return pairs

    xml_like = re.compile(
        r"\bx\s*=\s*\"(-?\d+(?:\.\d+)?)\"[^>]*\by\s*=\s*\"(-?\d+(?:\.\d+)?)\""
    )
    for match in xml_like.finditer(text):
        pairs.append((float(match.group(1)), float(match.group(2))))
    if pairs:
        return pairs

    x_vals = [float(m.group(1)) for m in re.finditer(r"\bx:\s*(-?\d+(?:\.\d+)?)", text)]
    y_vals = [float(m.group(1)) for m in re.finditer(r"\by:\s*(-?\d+(?:\.\d+)?)", text)]
    return list(zip(x_vals, y_vals))


def _candidate_files(map_dir: Path) -> Iterable[Path]:
    preferred = (
        "base_map.txt",
        "base_map.xml",
        "sim_map.txt",
        "sim_map.xml",
    )
    for name in preferred:
        path = map_dir / name
        if path.exists():
            yield path
    for path in sorted(map_dir.glob("*.txt")):
        if path.name not in preferred:
            yield path
    for path in sorted(map_dir.glob("*.xml")):
        if path.name not in preferred:
            yield path


def _build_bounds(points: List[Tuple[float, float]]) -> dict:
    x_vals = [p[0] for p in points]
    y_vals = [p[1] for p in points]
    return {
        "min_x": min(x_vals),
        "max_x": max(x_vals),
        "min_y": min(y_vals),
        "max_y": max(y_vals),
        "point_count": len(points),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate a simple map_bounds.json from an Apollo HDMap directory")
    ap.add_argument("--map_dir", required=True, help="Apollo map directory containing base_map/sim_map files")
    ap.add_argument(
        "--output",
        default="map_bounds.json",
        help="Output JSON path (default: <map_dir>/map_bounds.json when relative)",
    )
    args = ap.parse_args()

    map_dir = Path(args.map_dir).expanduser().resolve()
    if not map_dir.exists():
        print(f"[gen_map_bounds] map_dir missing: {map_dir}", file=sys.stderr)
        return 2

    points: List[Tuple[float, float]] = []
    source_file: Path | None = None
    for candidate in _candidate_files(map_dir):
        try:
            parsed = _extract_from_text(candidate.read_text(errors="ignore"))
        except Exception:
            continue
        if parsed:
            points = parsed
            source_file = candidate
            break

    if not points:
        print(
            "[gen_map_bounds] unable to extract x/y points from HDMap text assets; "
            "provide base_map.txt/base_map.xml or generate bounds from Dreamview manually",
            file=sys.stderr,
        )
        return 3

    bounds = _build_bounds(points)
    bounds["source_file"] = str(source_file) if source_file is not None else ""

    output = Path(args.output)
    if not output.is_absolute():
        output = (map_dir / output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(bounds, indent=2))
    print(f"[gen_map_bounds] wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
